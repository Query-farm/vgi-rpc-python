# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the generic Unix-socket worker launcher (``vgi_rpc.launcher``).

Coverage:

* Smoke: launch → discover ``UNIX:<path>`` → connect → introspect.
* Single-instance: concurrent launchers serialise on the per-hash flock; only
  one worker is ever spawned for the same tuple.
* Defensive bind: ``serve_unix`` refuses to clobber a path with an existing
  listener (defense in depth even if the launcher coordinator misbehaves).
* Stale-socket recovery: a dangling ``.sock`` file with no listener gets
  unlinked and a fresh worker is spawned.
* Idle self-shutdown: worker exits after the configured idle period and the
  socket file is removed.
* Hash differentiation: same command from different cwds → different sockets.
* GC: ``vgi-rpc launch --gc`` cleans stale entries.
* ``--status``: lists tracked workers with liveness probe.

Tests skip on Windows because the launcher relies on POSIX-specific bits
(``os.geteuid``, ``XDG_RUNTIME_DIR``) that aren't available; the wire format
itself is portable but separate Windows coverage is a v0.2 task.

Each test caps its idle/startup timings tight to keep the file under the
project-wide 50-second pytest budget.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from vgi_rpc.launcher import (
    GcResult,
    LaunchConfig,
    compute_hash,
    default_state_dir,
    gc_state_dir,
    launch,
    status_rows,
)

_SKIP_WIN = pytest.mark.skipif(sys.platform == "win32", reason="launcher relies on POSIX state-dir semantics")

_FIXTURE_RUNNER = str(Path(__file__).parent / "_launcher_worker_run_server.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def state_dir() -> Iterator[Path]:
    """Per-test state dir under ``/tmp`` to stay under macOS's 104-byte AF_UNIX limit.

    ``tmp_path`` resolves to a deeply nested ``/private/var/folders/...`` path
    (~95 chars), which when concatenated with ``<hash>.sock`` (21 chars) blows
    past the 104-byte cap and breaks ``connect()``.
    """
    d = Path(tempfile.gettempdir()) / f"vgi-launcher-test-{uuid.uuid4().hex[:8]}"
    d.mkdir(mode=0o700)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _worker_argv() -> list[str]:
    """Return argv that runs the test fixture service through ``run_server()``.

    The shim file calls ``run_server(RpcFixtureService, RpcFixtureServiceImpl())`` —
    a thin wrapper over the existing test fixtures so we exercise the new
    ``run_server --unix`` code path end-to-end via the launcher.
    """
    return [sys.executable, _FIXTURE_RUNNER]


def _connect_and_close(path: str) -> None:
    """Probe a Unix socket; raise if not accepting."""
    if sys.platform == "win32":  # pragma: no cover - tests skip on Windows
        raise RuntimeError("AF_UNIX is not available on Windows")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(path)
    finally:
        s.close()


def _wait_for_path_gone(path: str, timeout: float) -> bool:
    """Poll until *path* no longer exists; return True on success."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not Path(path).exists():
            return True
        time.sleep(0.05)
    return False


def _wait_for_no_listener(path: str, timeout: float) -> bool:
    """Poll until *path* is no longer accepting; return True on success."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _connect_and_close(path)
        except (FileNotFoundError, ConnectionRefusedError):
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@_SKIP_WIN
def test_smoke_launch_and_connect(state_dir: Path) -> None:
    """Launch a worker, connect over the returned socket, then let it idle out."""
    stderr_path = state_dir / "worker.stderr"
    config = LaunchConfig(
        worker_argv=tuple(_worker_argv()),
        idle_timeout=2.0,
        connect_timeout=10.0,
        worker_startup_timeout=15.0,
        state_dir=str(state_dir),
        worker_stderr=str(stderr_path),
    )
    path = launch(config)
    assert Path(path).exists()
    _connect_and_close(path)
    # Disconnected; idle timer should fire within idle_timeout + slack.
    if not _wait_for_path_gone(path, timeout=15.0):
        # Give faulthandler a moment to flush its periodic stack dumps.
        time.sleep(1.0)
        stderr_text = stderr_path.read_text(errors="replace") if stderr_path.exists() else "(no stderr file)"
        raise AssertionError(f"worker did not idle-shutdown\n--- worker stderr ---\n{stderr_text}")


@_SKIP_WIN
def test_single_instance_under_race(state_dir: Path) -> None:
    """Eight concurrent launchers → one worker, all eight clients connect."""
    config = LaunchConfig(
        worker_argv=tuple(_worker_argv()),
        idle_timeout=10.0,
        connect_timeout=10.0,
        worker_startup_timeout=15.0,
        state_dir=str(state_dir),
    )
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(launch, config) for _ in range(8)]
        paths: list[str] = [f.result() for f in as_completed(futures)]

    assert len(set(paths)) == 1, f"expected a single shared socket, got {set(paths)}"
    shared = paths[0]
    # Hash is deterministic from cmd+cwd+env, so we can inspect the state dir.
    sock_files = list(state_dir.glob("*.sock"))
    assert len(sock_files) == 1, f"expected exactly one .sock file, got {sock_files}"
    _connect_and_close(shared)


@_SKIP_WIN
def test_defensive_bind_refuses_existing_listener() -> None:
    """``serve_unix`` rejects a path that already has a listener on it."""
    from vgi_rpc.rpc._transport import _check_no_existing_listener

    base = Path(tempfile.gettempdir()) / f"vgi-bind-{uuid.uuid4().hex[:8]}"
    base.mkdir(mode=0o700)
    path = str(base / "occupied.sock")
    if sys.platform == "win32":  # pragma: no cover - test is skipped on Windows
        pytest.skip("AF_UNIX is not available on Windows")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(path)
        listener.listen(1)
        with pytest.raises(RuntimeError, match="already listening"):
            _check_no_existing_listener(path)
    finally:
        listener.close()
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        shutil.rmtree(base, ignore_errors=True)


@_SKIP_WIN
def test_stale_socket_file_cleaned_up(state_dir: Path) -> None:
    """A dangling socket file with no listener is unlinked and a fresh worker spawns."""
    hash_id = compute_hash(_worker_argv())
    stale_sock = state_dir / f"{hash_id}.sock"
    stale_sock.touch()
    assert stale_sock.exists()

    config = LaunchConfig(
        worker_argv=tuple(_worker_argv()),
        idle_timeout=2.0,
        connect_timeout=10.0,
        worker_startup_timeout=15.0,
        state_dir=str(state_dir),
    )
    path = launch(config)
    # Same path we expect, and now actually accepting.
    assert path == str(stale_sock)
    _connect_and_close(path)


@_SKIP_WIN
def test_hash_differentiates_cwd(state_dir: Path) -> None:
    """Same argv from different cwds yields different hashes (no false sharing)."""
    argv = _worker_argv()
    h1 = compute_hash(argv, cwd="/tmp/a")
    h2 = compute_hash(argv, cwd="/tmp/b")
    assert h1 != h2


@_SKIP_WIN
def test_hash_differentiates_vgi_rpc_env(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``VGI_RPC_*`` env vars are part of the hash domain."""
    argv = _worker_argv()
    monkeypatch.delenv("VGI_RPC_TEST_TOKEN", raising=False)
    h1 = compute_hash(argv)
    monkeypatch.setenv("VGI_RPC_TEST_TOKEN", "x")
    h2 = compute_hash(argv)
    assert h1 != h2


@_SKIP_WIN
def test_status_lists_known_workers(state_dir: Path) -> None:
    """``status_rows`` returns one row per ``.lock`` with a liveness probe."""
    config = LaunchConfig(
        worker_argv=tuple(_worker_argv()),
        idle_timeout=10.0,
        connect_timeout=10.0,
        worker_startup_timeout=15.0,
        state_dir=str(state_dir),
    )
    path = launch(config)
    rows = status_rows(state_dir)
    assert len(rows) == 1
    row = rows[0]
    assert row.alive is True
    assert row.socket == path
    assert row.cmd == _worker_argv()


@_SKIP_WIN
def test_gc_cleans_stale_entries(state_dir: Path) -> None:
    """``gc_state_dir`` removes lock/sock/meta triples whose worker is gone."""
    # Manufacture two stale entries (no real workers).
    for hash_id in ("aaaa111122223333", "bbbb444455556666"):
        (state_dir / f"{hash_id}.lock").touch()
        (state_dir / f"{hash_id}.sock").touch()
        (state_dir / f"{hash_id}.meta").write_text("{}", encoding="utf-8")

    result = gc_state_dir(state_dir)
    assert isinstance(result, GcResult)
    assert sorted(result.cleaned) == ["aaaa111122223333", "bbbb444455556666"]
    assert result.skipped_in_use == []
    # Files actually gone.
    assert list(state_dir.glob("*.lock")) == []
    assert list(state_dir.glob("*.sock")) == []
    assert list(state_dir.glob("*.meta")) == []


@_SKIP_WIN
def test_gc_skips_held_lock(state_dir: Path) -> None:
    """``gc_state_dir`` doesn't touch entries whose lock is currently held."""
    from filelock import FileLock

    hash_id = "cccc777788889999"
    (state_dir / f"{hash_id}.lock").touch()
    (state_dir / f"{hash_id}.sock").touch()  # stale, no listener
    held = FileLock(str(state_dir / f"{hash_id}.lock"))
    held.acquire()
    try:
        result = gc_state_dir(state_dir)
    finally:
        held.release()
    assert result.cleaned == []
    assert result.skipped_in_use == [hash_id]
    # Files preserved for the lock holder.
    assert (state_dir / f"{hash_id}.lock").exists()


@_SKIP_WIN
def test_explicit_socket_path_skips_hash() -> None:
    """``--socket`` skips the per-hash machinery and uses a sibling lockfile."""
    base = Path(tempfile.gettempdir()) / f"vgi-launcher-explicit-{uuid.uuid4().hex[:8]}"
    base.mkdir(mode=0o700)
    try:
        sock_path = str(base / "explicit.sock")
        config = LaunchConfig(
            worker_argv=tuple(_worker_argv()),
            socket_path=sock_path,
            idle_timeout=2.0,
            connect_timeout=10.0,
            worker_startup_timeout=15.0,
        )
        path = launch(config)
        assert path == sock_path
        _connect_and_close(path)
        # Sibling lockfile next to the socket.
        assert Path(sock_path + ".lock").exists()
    finally:
        # Wait for idle-shutdown to remove the socket then clean up.
        _wait_for_path_gone(sock_path, timeout=15.0)
        shutil.rmtree(base, ignore_errors=True)


@_SKIP_WIN
def test_run_server_unix_emits_discovery_line() -> None:
    """The ``--unix`` branch of ``run_server`` prints ``UNIX:<path>`` after bind."""
    base = Path(tempfile.gettempdir()) / f"vgi-rs-{uuid.uuid4().hex[:8]}"
    base.mkdir(mode=0o700)
    try:
        sock_path = str(base / "rs.sock")
        proc = subprocess.Popen(
            [sys.executable, _FIXTURE_RUNNER, "--unix", sock_path, "--idle-timeout", "2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line == f"UNIX:{sock_path}", f"unexpected discovery line: {line!r}"
            _connect_and_close(sock_path)
        finally:
            proc.terminate()
            proc.wait(timeout=5)
    finally:
        shutil.rmtree(base, ignore_errors=True)


@_SKIP_WIN
def test_run_server_unix_rejects_http_only_flags() -> None:
    """``--unix`` combined with HTTP-only flags exits non-zero with a clear message."""
    proc = subprocess.run(
        [
            sys.executable,
            _FIXTURE_RUNNER,
            "--unix",
            "/tmp/should-not-bind.sock",
            "--max-response-bytes",
            "1024",
        ],
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode != 0
    assert b"HTTP-only" in proc.stderr or b"HTTP-only" in proc.stdout


@_SKIP_WIN
def test_default_state_dir_is_per_user() -> None:
    """``default_state_dir`` returns an existing, owned directory."""
    d = default_state_dir()
    assert d.is_dir()
    if sys.platform == "win32":  # pragma: no cover - test is skipped on Windows
        pytest.skip("os.geteuid is not available on Windows")
    assert d.stat().st_uid == os.geteuid()
