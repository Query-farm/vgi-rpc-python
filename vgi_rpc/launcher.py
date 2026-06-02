# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""Generic Unix-socket worker launcher.

Spawns and coordinates long-running worker processes that serve RPC over
``AF_UNIX`` sockets.  Designed for clients (e.g. the VGI DuckDB extension)
that want a warm worker without managing its lifecycle themselves.

Architecture:

* The launcher derives a deterministic socket path from a hash of the worker
  command tuple (``cmd`` + ``args`` + ``cwd`` + ``VGI_RPC_*`` env), so the
  same worker is reused across unrelated callers.
* Concurrent first-callers serialise on a per-hash lockfile (``filelock``,
  cross-platform, kernel-managed auto-release on process death).
* Each spawned worker self-terminates after ``idle_timeout`` seconds with
  zero connected clients (see :func:`vgi_rpc.rpc.serve_unix`).

Cross-language contract for workers:

* Accept ``--unix PATH`` and ``--idle-timeout SEC``.
* Emit exactly one line ``UNIX:<absolute-path>\n`` on **stdout** (flushed)
  once bind+listen succeed.
* Write nothing further to stdout.
* Tolerate (or suppress) third-party noise on stdout *before* the bind line —
  the launcher skips non-``UNIX:`` prefix lines for resilience.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Final

from filelock import FileLock, Timeout

_logger = logging.getLogger("vgi_rpc.launcher")

# Maximum number of stale entries the opportunistic GC scans per launch.
_DEFAULT_GC_LIMIT: Final[int] = 16

# Hash truncation — 16 hex chars (64 bits) is plenty of collision space for
# the per-user worker tuple namespace and keeps the socket path comfortably
# under the AF_UNIX path length limits (104 macOS / 108 Linux+Windows).
_HASH_LEN: Final[int] = 16

# Probe timeout for the connect() liveness check on existing sockets.  Short
# enough that a hung worker doesn't make the launcher hang.
_PROBE_TIMEOUT_S: Final[float] = 2.0


@dataclasses.dataclass(frozen=True)
class LaunchConfig:
    """Inputs to :func:`launch`."""

    worker_argv: tuple[str, ...]
    """The worker command and arguments.  Must be non-empty."""

    socket_path: str | None = None
    """Explicit socket path; when ``None``, derived from the hash of the tuple."""

    idle_timeout: float = 300.0
    """Worker self-shutdown after this many seconds idle.  Forwarded as ``--idle-timeout``."""

    connect_timeout: float = 30.0
    """Maximum seconds to block waiting for the per-hash flock."""

    worker_startup_timeout: float = 60.0
    """Maximum seconds to wait for the worker to print ``UNIX:<path>``."""

    worker_stderr: str | None = None
    """If set, worker stderr is appended to this file; otherwise discarded."""

    state_dir: str | None = None
    """Override the default state directory."""


def default_state_dir() -> Path:
    """Resolve the per-user state directory used for lockfiles + sockets.

    Linux uses ``$XDG_RUNTIME_DIR/vgi-rpc/`` when set (systemd-managed,
    auto-cleaned on logout).  macOS and other POSIX use
    ``$TMPDIR/vgi-rpc-$UID/`` (per-user namespacing).  Windows uses
    ``$TMP/vgi-rpc/``.  The directory is created mode 0700 if missing.
    """
    if sys.platform == "win32":
        base = Path(tempfile.gettempdir()) / "vgi-rpc"
    else:
        xdg = os.environ.get("XDG_RUNTIME_DIR")
        base = Path(xdg) / "vgi-rpc" if xdg else Path(tempfile.gettempdir()) / f"vgi-rpc-{os.geteuid()}"
    base.mkdir(parents=True, exist_ok=True)
    if sys.platform != "win32":
        # Tighten mode on every call — cheap and idempotent.
        with contextlib.suppress(OSError):
            os.chmod(base, 0o700)
        # Refuse to operate on a hijacked directory.
        if base.stat().st_uid != os.geteuid():
            raise RuntimeError(f"state directory {base} is not owned by current user")
    return base


def compute_hash(worker_argv: Iterable[str], cwd: str | None = None) -> str:
    """Hash the worker tuple to a deterministic 16-hex-char identifier.

    The hash domain is ``(argv, cwd, VGI_RPC_*-env)``: workers with different
    cwd or different ``VGI_RPC_*`` configuration are intentionally treated as
    distinct so a launcher invocation never silently shares a worker that was
    started with conflicting settings.
    """
    canonical: dict[str, object] = {
        "cmd": list(worker_argv),
        "cwd": cwd if cwd is not None else os.getcwd(),
        "env": {k: v for k, v in sorted(os.environ.items()) if k.startswith("VGI_RPC_")},
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:_HASH_LEN]


def _socket_paths(state_dir: Path, hash_id: str) -> tuple[Path, Path, Path]:
    """Return the ``(lock, sock, meta)`` paths for a given hash."""
    return (
        state_dir / f"{hash_id}.lock",
        state_dir / f"{hash_id}.sock",
        state_dir / f"{hash_id}.meta",
    )


def _probe(path: str | Path) -> bool:
    """Return True if a worker is currently accepting on *path*."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(_PROBE_TIMEOUT_S)
    try:
        s.connect(str(path))
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError):
        return False
    finally:
        s.close()
    return True


def _write_meta(meta_path: Path, worker_argv: Iterable[str], cwd: str, sock_path: str) -> None:
    """Best-effort write of human-readable launch metadata for ``--status``."""
    payload = {
        "cmd": list(worker_argv),
        "cwd": cwd,
        "socket": sock_path,
        "started_at": time.time(),
        "launcher_pid": os.getpid(),
    }
    try:
        meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError:
        _logger.debug("failed to write meta file %s", meta_path, exc_info=True)


def _drain_pipe(pipe: object) -> threading.Thread:
    """Spawn a daemon thread that consumes (and discards) lines from *pipe*.

    Defends against contract-violating workers that write to stdout after the
    ``UNIX:`` line — without this, a full pipe buffer (~64 KB) would
    eventually deadlock the worker once the launcher exits.
    """

    def _drain() -> None:
        try:
            for _ in pipe:  # type: ignore[attr-defined]  # ty: ignore[not-iterable]
                pass
        except (OSError, ValueError):
            pass

    t = threading.Thread(target=_drain, daemon=True, name="vgi-launcher-drain")
    t.start()
    return t


def _spawn_worker(
    worker_argv: list[str],
    sock_path: str,
    idle_timeout: float,
    worker_stderr: str | None,
    startup_timeout: float,
) -> subprocess.Popen[bytes]:
    """Spawn the worker, wait for ``UNIX:<path>`` on stdout, return the live process.

    Skips non-matching prefix lines (third-party stdout noise during import).
    Raises ``RuntimeError`` on worker exit before readiness or timeout.
    """
    full_argv = [*worker_argv, "--unix", sock_path, "--idle-timeout", str(idle_timeout)]
    stderr_file: IO[bytes] | None = None
    stderr_dest: int | IO[bytes]
    if worker_stderr is None:
        stderr_dest = subprocess.DEVNULL
    else:
        # Append mode so multiple worker generations share one log; subprocess owns the dup'd fd.
        stderr_file = open(worker_stderr, "ab")  # noqa: SIM115
        stderr_dest = stderr_file
    try:
        proc = subprocess.Popen(
            full_argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=stderr_dest,
            # Inherit session/pgrp so terminal SIGINT/SIGHUP propagate; reaping
            # is via idle timeout, not deathwatch.
        )
    finally:
        if stderr_file is not None:
            stderr_file.close()
    deadline = time.monotonic() + startup_timeout
    expected_prefix = f"UNIX:{sock_path}"
    assert proc.stdout is not None
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            # Worker exited or closed stdout without emitting the line.
            proc.wait(timeout=1)
            raise RuntimeError(f"worker exited before readiness (rc={proc.returncode})")
        decoded = line.decode("utf-8", errors="replace").rstrip("\r\n")
        if decoded.startswith("UNIX:"):
            if decoded != expected_prefix:
                # Worker bound to a different path than we asked for — refuse.
                proc.terminate()
                proc.wait(timeout=5)
                raise RuntimeError(f"worker bound to unexpected path: {decoded!r} (expected {expected_prefix!r})")
            _drain_pipe(proc.stdout)
            return proc
        # Non-matching prefix — third-party noise; log at DEBUG and keep reading.
        _logger.debug("worker prefix line (not UNIX:): %r", decoded)
    # Timeout — worker is still alive but never emitted the line.
    proc.terminate()
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=5)
    raise RuntimeError(f"worker did not emit UNIX:<path> within {startup_timeout}s")


def launch(config: LaunchConfig) -> str:
    """Ensure a worker is running and return its socket path.

    Returns the absolute socket path the caller should connect to.  Either the
    existing worker for this hash is reused (probe succeeds) or a fresh one is
    spawned under flock.

    Raises ``RuntimeError`` on any failure to bring up a worker.
    """
    if not config.worker_argv:
        raise ValueError("worker_argv must be non-empty")
    state_dir = Path(config.state_dir) if config.state_dir is not None else default_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)

    if config.socket_path is not None:
        sock_path = Path(config.socket_path).absolute()
        # Explicit paths get a sibling lock, no .meta, skipped by --status/--gc.
        lock_path = sock_path.with_suffix(sock_path.suffix + ".lock")
        meta_path: Path | None = None
        hash_id: str | None = None
    else:
        hash_id = compute_hash(config.worker_argv)
        lock_path, sock_path_p, meta_path = _socket_paths(state_dir, hash_id)
        sock_path = sock_path_p

    # Guard launcher from SIGPIPE so caller closing the popen() read end doesn't
    # kill us mid-print.  POSIX only (Windows lacks SIGPIPE), and only safe from
    # the main thread — under tests the launcher may run inside a thread pool.
    if hasattr(signal, "SIGPIPE") and threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    lock = FileLock(str(lock_path), timeout=config.connect_timeout)
    try:
        lock.acquire()
    except Timeout as exc:
        raise RuntimeError(f"failed to acquire {lock_path} within {config.connect_timeout}s") from exc

    try:
        # Probe — maybe a worker is already serving.
        if _probe(sock_path):
            return str(sock_path)
        # Stale socket cleanup; broaden OSError for Windows ERROR_SHARING_VIOLATION.
        with contextlib.suppress(OSError):
            os.unlink(sock_path)
        if meta_path is not None:
            _write_meta(meta_path, config.worker_argv, os.getcwd(), str(sock_path))
        proc = _spawn_worker(
            list(config.worker_argv),
            str(sock_path),
            config.idle_timeout,
            config.worker_stderr,
            config.worker_startup_timeout,
        )
        _logger.debug("spawned worker pid=%d on %s", proc.pid, sock_path)
        return str(sock_path)
    finally:
        lock.release()
        # Opportunistic GC after release — bounded so it can't dominate runtime.
        if hash_id is not None:
            with contextlib.suppress(Exception):
                gc_state_dir(state_dir, limit=_DEFAULT_GC_LIMIT, exclude_hash=hash_id)


@dataclasses.dataclass(frozen=True)
class GcResult:
    """Summary of a GC pass."""

    cleaned: list[str]
    """Hash IDs of stale entries that were removed."""

    skipped_in_use: list[str]
    """Hash IDs whose lockfile is currently held (worker may be running or
    another launcher is mid-spawn)."""


def gc_state_dir(state_dir: Path, *, limit: int | None = None, exclude_hash: str | None = None) -> GcResult:
    """Remove ``<hash>.lock``/``.sock``/``.meta`` triples whose worker is gone.

    For each lockfile we can grab non-blocking, probe the sibling socket; if
    the probe also fails the entry is stale and we unlink all three files.
    Entries whose lock is held are left alone (a launch is in flight or the
    worker is alive).

    Args:
        state_dir: Directory to scan.
        limit: Stop after considering this many candidate entries.  Used by
            the opportunistic in-launch GC to keep work bounded.
        exclude_hash: Skip this hash (the launcher's own entry).

    """
    cleaned: list[str] = []
    skipped: list[str] = []
    seen = 0
    for lock_path in sorted(state_dir.glob("*.lock")):
        if limit is not None and seen >= limit:
            break
        seen += 1
        hash_id = lock_path.stem
        if exclude_hash is not None and hash_id == exclude_hash:
            continue
        sock_path = state_dir / f"{hash_id}.sock"
        meta_path = state_dir / f"{hash_id}.meta"
        try:
            probe_lock = FileLock(str(lock_path), timeout=0.0)
            probe_lock.acquire()
        except Timeout:
            skipped.append(hash_id)
            continue
        try:
            if _probe(sock_path):
                # Worker is alive but didn't hold its own lock — odd but
                # leave it alone.
                continue
            for p in (sock_path, meta_path, lock_path):
                with contextlib.suppress(OSError):
                    os.unlink(p)
            cleaned.append(hash_id)
        finally:
            with contextlib.suppress(Exception):
                probe_lock.release()
    return GcResult(cleaned=cleaned, skipped_in_use=skipped)


@dataclasses.dataclass(frozen=True)
class StatusRow:
    """One row in a ``--status`` listing."""

    hash_id: str
    cmd: list[str]
    cwd: str
    socket: str
    started_at: float | None
    alive: bool


def status_rows(state_dir: Path) -> list[StatusRow]:
    """Return one row per ``<hash>.lock`` in *state_dir*, with liveness probe.

    Read-only; takes no locks.  Best-effort parsing of ``.meta`` — entries
    with missing or corrupt metadata still appear with whatever fields can
    be recovered.
    """
    rows: list[StatusRow] = []
    for lock_path in sorted(state_dir.glob("*.lock")):
        hash_id = lock_path.stem
        sock_path = state_dir / f"{hash_id}.sock"
        meta_path = state_dir / f"{hash_id}.meta"
        cmd: list[str] = []
        cwd = ""
        started_at: float | None = None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            meta = {}
        if isinstance(meta.get("cmd"), list):
            cmd = [str(x) for x in meta["cmd"]]
        if isinstance(meta.get("cwd"), str):
            cwd = meta["cwd"]
        if isinstance(meta.get("started_at"), int | float):
            started_at = float(meta["started_at"])
        rows.append(
            StatusRow(
                hash_id=hash_id,
                cmd=cmd,
                cwd=cwd,
                socket=str(sock_path),
                started_at=started_at,
                alive=_probe(sock_path),
            )
        )
    return rows
