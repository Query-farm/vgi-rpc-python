# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport protocol and implementations."""

from __future__ import annotations

import contextlib
import errno
import logging
import os
import socket
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from enum import Enum
from io import BufferedReader, IOBase, RawIOBase
from typing import TYPE_CHECKING, Any, BinaryIO, Protocol, cast, runtime_checkable

from vgi_rpc.rpc._common import _logger
from vgi_rpc.rpc._debug import wire_transport_logger
from vgi_rpc.shm import ShmSegment

if TYPE_CHECKING:
    from vgi_rpc.rpc._server import RpcServer


def _stderr_open() -> bool:
    """Return True if stderr is still writable (guards against Windows shutdown)."""
    try:
        return sys.stderr is not None and not sys.stderr.closed
    except ValueError:
        return False


# Largest byte count handed to a single underlying ``write()`` or ``read()``.
# Chosen to sit well under ``INT_MAX`` on every platform; ``memoryview``
# slicing is free, so the only cost of chunking is one extra syscall per
# gigabyte.
_MAX_WRITE_CHUNK = 1 << 30  # 1 GiB
_MAX_READ_CHUNK = _MAX_WRITE_CHUNK

# ``accept(2)`` can fail temporarily when a burst of parallel scan/window
# connections reaches the process or system fd budget. The listening socket is
# still valid in that case; existing handlers closing their connections will
# make room. Treating it like EBADF permanently shuts down and unlinks a Unix
# listener, stranding every subsequent client.
_TRANSIENT_ACCEPT_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM})
_ACCEPT_RESOURCE_RETRY_DELAY = 0.05


def _accept_resource_exhausted(exc: OSError) -> bool:
    """Whether *exc* is temporary listener resource pressure."""
    return exc.errno in _TRANSIENT_ACCEPT_ERRNOS


class _ExactWriter(RawIOBase):
    """Wrap an unbuffered binary writer so a large payload is written in full.

    Every writer here is deliberately unbuffered so IPC data reaches the
    peer immediately, which means ``write()`` maps onto a single
    ``write(2)`` / ``send(2)``.  That syscall is not obliged to accept the
    whole buffer, and above 2 GiB on macOS it refuses to — in one of two
    different ways depending on what is underneath:

    * **pipes** return a short count of exactly ``INT_MAX`` with *no
      error*, so the tail is silently dropped.  Nothing raises; the peer
      simply blocks forever waiting for bytes the Arrow IPC header
      promised, and the RPC deadlocks.
    * **sockets** (Unix domain and TCP) fail outright with ``EINVAL``.

    So looping on the return value alone is not enough — the per-call size
    has to be clamped as well, which is what ``_MAX_WRITE_CHUNK`` does.
    Arrow's ``PythonFile`` sink does not re-offer a remainder, so both
    behaviours have to be absorbed here.  See :class:`_ClampedReader` for the
    read-side counterpart.
    """

    def __init__(self, raw: IOBase) -> None:
        super().__init__()
        self._raw = raw

    def write(self, b: Any, /) -> int:
        view = memoryview(b).cast("B")
        total = 0
        size = len(view)
        while total < size:
            end = min(total + _MAX_WRITE_CHUNK, size)
            written = self._raw.write(view[total:end])
            if written is None:
                # Non-blocking fd with nothing accepted. These transports are
                # always blocking; treat it as a programming error rather than
                # spinning.
                raise BlockingIOError("transport writer is non-blocking; refusing to spin")
            if written == 0:
                raise OSError("transport write accepted 0 bytes; peer is not consuming")
            total += written
        return total

    def writable(self) -> bool:
        return True

    def fileno(self) -> int:
        return self._raw.fileno()

    def flush(self) -> None:
        if not self._raw.closed:
            self._raw.flush()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._raw.close()
            super().close()


def _exact(raw: IOBase) -> IOBase:
    """Wrap ``raw`` so partial writes are retried. See :class:`_ExactWriter`."""
    return cast("IOBase", _ExactWriter(raw))


class _ClampedRaw(RawIOBase):
    """Cap each underlying read below ``INT_MAX``; buffering above refills.

    The mirror image of :class:`_ExactWriter`, and the half that was missed
    when the write side was fixed.  Arrow asks for a whole message body in
    one call, so a >2 GiB batch produces a single read of that size, and on
    macOS ``recv_into`` refuses it with ``EINVAL`` exactly as ``send`` does
    -- the read-side twin of the write bug, on the same two socket
    transports.

    It presented as a flake rather than a failure.  ``BufferedReader``
    bypasses its buffer only for requests larger than the buffer, so whether
    any single request crossed ``INT_MAX`` depended on how much of the body
    happened to be buffered already: roughly one connection in two died
    mid-request, the serve loop logged it at DEBUG and carried on, and the
    peer saw a bare broken pipe with no explanation.

    This sits *under* the buffering rather than over it, which is what keeps
    it free.  Clamping alone is not sufficient -- pyarrow does not retry a
    short read, it raises ``Expected to be able to read N bytes for message
    body, got M`` -- but ``BufferedReader.read`` already loops over its raw
    until the request is satisfied, in C.  So the refill comes from the layer
    that was always there, and this class is called once per buffer fill
    instead of once per Arrow read.  Wrapping the buffered stream from the
    outside instead measured 2-3x slower on small calls, for no extra safety.
    """

    def __init__(self, raw: RawIOBase, owner: IOBase | None = None) -> None:
        super().__init__()
        self._raw = raw
        # The buffered wrapper we took ``raw`` out of, kept so closing this
        # object still releases whatever it owns (a socket's io refcount).
        self._owner = owner

    def readinto(self, b: Any, /) -> int:
        view = memoryview(b).cast("B")
        if len(view) > _MAX_READ_CHUNK:
            view = view[:_MAX_READ_CHUNK]
        got = self._raw.readinto(view)
        if got is None:
            # Non-blocking fd with nothing available. These transports are
            # always blocking; treat it as a programming error rather than
            # spinning, exactly as the writer does.
            raise BlockingIOError("transport reader is non-blocking; refusing to spin")
        return got

    def readable(self) -> bool:
        return True

    def fileno(self) -> int:
        return self._raw.fileno()

    def close(self) -> None:
        try:
            self._raw.close()
            if self._owner is not None and not self._owner.closed:
                self._owner.close()
        finally:
            super().close()


def _clamped(buffered: IOBase) -> IOBase:
    """Re-buffer ``buffered`` over a read-clamping raw. See :class:`_ClampedRaw`.

    Args:
        buffered: A buffered reader, as returned by ``open``/``makefile``.

    Returns:
        An equivalent buffered reader whose underlying reads are capped.
        If the stream exposes no ``raw`` to clamp, it is returned unchanged.

    """
    raw = getattr(buffered, "raw", None)
    if raw is None:
        return buffered
    return cast("IOBase", BufferedReader(_ClampedRaw(raw, buffered)))


# ---------------------------------------------------------------------------
# RpcTransport protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RpcTransport(Protocol):
    """Bidirectional byte stream transport."""

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        ...

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        ...

    def close(self) -> None:
        """Close the transport."""
        ...


# ---------------------------------------------------------------------------
# PipeTransport + make_pipe_pair
# ---------------------------------------------------------------------------


class PipeTransport:
    """Transport backed by file-like IO streams (e.g. from os.pipe())."""

    __slots__ = ("_reader", "_writer")

    def __init__(self, reader: IOBase, writer: IOBase) -> None:
        """Initialize with reader and writer streams."""
        self._reader = reader
        self._writer = writer

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._writer

    def close(self) -> None:
        """Close both streams."""
        self._reader.close()
        self._writer.close()


def make_pipe_pair() -> tuple[PipeTransport, PipeTransport]:
    """Create connected client/server transports using os.pipe().

    Returns (client_transport, server_transport).
    """
    c2s_r, c2s_w = os.pipe()
    s2c_r, s2c_w = os.pipe()
    if wire_transport_logger.isEnabledFor(logging.DEBUG):
        wire_transport_logger.debug(
            "make_pipe_pair: c2s=(%d,%d), s2c=(%d,%d)",
            c2s_r,
            c2s_w,
            s2c_r,
            s2c_w,
        )
    client = PipeTransport(
        _clamped(os.fdopen(s2c_r, "rb")),
        _exact(os.fdopen(c2s_w, "wb", buffering=0)),
    )
    server = PipeTransport(
        _clamped(os.fdopen(c2s_r, "rb")),
        _exact(os.fdopen(s2c_w, "wb", buffering=0)),
    )
    return client, server


class ShmPipeTransport:
    """Pipe transport with shared memory side-channel for batch data.

    Does NOT own the ``ShmSegment`` — caller manages segment lifecycle.
    Closing the transport closes the pipe only.
    """

    __slots__ = ("_pipe", "_shm")

    def __init__(self, pipe: PipeTransport, shm: ShmSegment) -> None:
        """Initialize with a pipe transport and a shared memory segment."""
        self._pipe = pipe
        self._shm = shm

    @property
    def reader(self) -> IOBase:
        """Readable binary stream (delegated to pipe)."""
        return self._pipe.reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream (delegated to pipe)."""
        return self._pipe.writer

    @property
    def shm(self) -> ShmSegment:
        """The shared memory segment."""
        return self._shm

    def close(self) -> None:
        """Close the pipe transport (does NOT close/unlink shm)."""
        self._pipe.close()


class StderrMode(Enum):
    """How to handle child process stderr in SubprocessTransport.

    Members:
        INHERIT: Child stderr goes to parent's stderr (default).
        PIPE: Parent drains child stderr via a daemon thread and
            forwards each line to a ``logging.Logger``.
        DEVNULL: Child stderr discarded at OS level.
    """

    INHERIT = "inherit"
    PIPE = "pipe"
    DEVNULL = "devnull"


def _drain_stderr(pipe: BinaryIO, logger: logging.Logger) -> None:
    """Drain child stderr line-by-line. Runs in parent as daemon thread."""
    try:
        for raw_line in pipe:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if line:
                logger.info(line)
    except (OSError, ValueError):
        pass
    except Exception:
        _logger.debug("Unexpected error in stderr drain", exc_info=True)
    with contextlib.suppress(OSError, ValueError):
        pipe.close()


class SubprocessTransport:
    """Transport that communicates with a child process over stdin/stdout.

    Spawns a command via ``subprocess.Popen`` with ``stdin=PIPE``,
    ``stdout=PIPE``, and configurable stderr handling via :class:`StderrMode`.

    The writer (child's stdin) is kept unbuffered (``bufsize=0``) so IPC
    data is flushed immediately.  The reader (child's stdout) is wrapped
    in a ``BufferedReader`` because Arrow IPC expects ``read(n)`` to
    return exactly *n* bytes, but raw ``FileIO.read(n)`` on a pipe may
    return fewer (POSIX short-read semantics).
    """

    __slots__ = ("_closed", "_proc", "_reader", "_stderr_thread", "_writer")

    def __init__(
        self,
        cmd: list[str],
        *,
        stderr: StderrMode = StderrMode.INHERIT,
        stderr_logger: logging.Logger | None = None,
    ) -> None:
        """Spawn the subprocess and wire up stdin/stdout as the transport.

        Args:
            cmd: Command to spawn.
            stderr: How to handle the child's stderr stream.
            stderr_logger: Logger for ``StderrMode.PIPE`` output.
                Defaults to ``logging.getLogger("vgi_rpc.subprocess.stderr")``.

        """
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug(
                "SubprocessTransport init: cmd=%s, stderr=%s",
                cmd,
                stderr.value,
            )

        if stderr == StderrMode.DEVNULL:
            stderr_arg: int | None = subprocess.DEVNULL
        elif stderr == StderrMode.PIPE:
            stderr_arg = subprocess.PIPE
        else:
            stderr_arg = None

        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_arg,
            bufsize=0,
        )
        assert self._proc.stdout is not None
        assert self._proc.stdin is not None
        self._reader: IOBase = _clamped(os.fdopen(self._proc.stdout.fileno(), "rb", closefd=False))
        self._writer: IOBase = _exact(cast("IOBase", self._proc.stdin))
        self._closed = False
        self._stderr_thread: threading.Thread | None = None
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug(
                "SubprocessTransport spawned: pid=%d, stdin_fd=%d, stdout_fd=%d",
                self._proc.pid,
                self._proc.stdin.fileno(),
                self._proc.stdout.fileno(),
            )

        if stderr == StderrMode.PIPE:
            assert self._proc.stderr is not None
            if stderr_logger is None:
                stderr_logger = logging.getLogger("vgi_rpc.subprocess.stderr")
            self._stderr_thread = threading.Thread(
                target=_drain_stderr,
                args=(self._proc.stderr, stderr_logger),
                daemon=True,
            )
            self._stderr_thread.start()

    @property
    def proc(self) -> subprocess.Popen[bytes]:
        """The underlying Popen process."""
        return self._proc

    @property
    def reader(self) -> IOBase:
        """Readable binary stream (child's stdout, buffered)."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream (child's stdin, unbuffered)."""
        return self._writer

    def close(self) -> None:
        """Close stdin (sends EOF), wait for exit, close stdout."""
        if self._closed:
            return
        if _stderr_open() and wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug("SubprocessTransport closing: pid=%d", self._proc.pid)
        self._closed = True
        if self._proc.stdin:
            self._proc.stdin.close()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=5)
        self._reader.close()
        if _stderr_open() and wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug(
                "SubprocessTransport closed: pid=%d, exit_code=%s",
                self._proc.pid,
                self._proc.returncode,
            )


def serve_stdio(server: RpcServer) -> None:
    """Serve RPC requests over stdin/stdout.

    This is the server-side entry point for subprocess mode.  The reader
    uses default buffering so that ``read(n)`` returns exactly *n* bytes
    (Arrow IPC requires this; raw ``FileIO.read(n)`` may short-read on
    pipes).  The writer is unbuffered (``buffering=0``) so IPC data is
    flushed immediately.  Uses ``closefd=False`` so the original stdio
    descriptors are not closed on exit.

    Emits a diagnostic warning to stderr when stdin or stdout is connected
    to a terminal, since the process expects binary Arrow IPC data.
    """
    if sys.stdin.isatty() or sys.stdout.isatty():
        sys.stderr.write(
            "WARNING: This process communicates via Arrow IPC on stdin/stdout "
            "and is not intended to be run interactively.\n"
            "It should be launched as a subprocess by an RPC client "
            "(e.g. vgi_rpc.connect()).\n"
        )
    reader = _clamped(os.fdopen(sys.stdin.fileno(), "rb", closefd=False))
    writer = _exact(os.fdopen(sys.stdout.fileno(), "wb", buffering=0, closefd=False))
    if wire_transport_logger.isEnabledFor(logging.DEBUG):
        wire_transport_logger.debug(
            "serve_stdio: server_id=%s, protocol=%s",
            server.server_id,
            server.protocol_name,
        )
    transport = PipeTransport(reader, writer)
    server.serve(transport)


# ---------------------------------------------------------------------------
# UnixTransport + make_unix_pair + serve_unix
# ---------------------------------------------------------------------------

#: Socket buffer asked for on a Unix domain socket, both ends.  macOS defaults
#: one to 8192 bytes (``net.local.stream.sendspace``) against ~64 KiB for a
#: pipe, so a megabyte of Arrow crosses the kernel in 128 trips instead of a
#: handful; Linux is more generous but still below this.  Best effort — the
#: kernel clamps to its own maximum, and a refusal is not worth failing a
#: connection over.
_UNIX_SOCKET_BUFFER_BYTES = 1 << 20


def _widen_socket_buffers(sock: socket.socket) -> None:
    """Request ``_UNIX_SOCKET_BUFFER_BYTES`` of send and receive buffer."""
    for option in (socket.SO_SNDBUF, socket.SO_RCVBUF):
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, option, _UNIX_SOCKET_BUFFER_BYTES)


class UnixTransport:
    """Transport backed by a connected Unix domain socket.

    The reader is buffered (default ``makefile`` buffering) so that
    ``read(n)`` returns exactly *n* bytes — required by Arrow IPC.
    The writer is unbuffered (``buffering=0``) so data is flushed
    immediately, matching the pattern used by ``PipeTransport``.
    """

    __slots__ = ("_reader", "_sock", "_writer")

    def __init__(self, sock: socket.socket) -> None:
        """Initialize from a connected AF_UNIX socket."""
        _widen_socket_buffers(sock)
        self._sock = sock
        self._reader: IOBase = _clamped(cast("IOBase", sock.makefile("rb")))
        self._writer: IOBase = _exact(cast("IOBase", sock.makefile("wb", buffering=0)))

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._writer

    def close(self) -> None:
        """Close the reader, writer, and underlying socket."""
        self._reader.close()
        self._writer.close()
        self._sock.close()


def make_unix_pair() -> tuple[UnixTransport, UnixTransport]:
    """Create connected client/server transports using ``socketpair()``.

    Returns ``(client_transport, server_transport)``.
    """
    s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    if wire_transport_logger.isEnabledFor(logging.DEBUG):
        wire_transport_logger.debug(
            "make_unix_pair: fd1=%d, fd2=%d",
            s1.fileno(),
            s2.fileno(),
        )
    return UnixTransport(s1), UnixTransport(s2)


def _check_no_existing_listener(path: str) -> None:
    """Refuse non-sockets and raise if another process listens on *path*.

    Best-effort defense-in-depth — between this probe and the caller's bind,
    another process could still claim the socket.  The launcher coordinates
    via flock at a higher layer; this guards against accidental misuse where
    two workers are pointed at the same path. Stable regular files and
    symlinks are preserved. Portable pathname APIs cannot close the final
    check/unlink race against a malicious same-UID process, so callers needing
    isolation should use a private (0700) parent directory.
    """
    try:
        entry = os.lstat(path)
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(entry.st_mode):
        raise RuntimeError(f"refusing to replace pre-existing non-socket path: {path}")

    test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        test_sock.connect(path)
    except (FileNotFoundError, ConnectionRefusedError):
        return
    finally:
        test_sock.close()
    raise RuntimeError(f"another process is already listening on {path}")


def _unlink_stale_unix_socket(path: str) -> None:
    """Best-effort unlink of a stable stale socket entry.

    This protects accidental collisions, not same-UID adversarial path swaps;
    use a private (0700) parent directory for isolation.
    """
    try:
        entry = os.lstat(path)
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(entry.st_mode):
        raise RuntimeError(f"refusing to replace pre-existing non-socket path: {path}")
    os.unlink(path)


def _unlink_bound_unix_socket(path: str, identity: tuple[int, int]) -> None:
    """Best-effort cleanup when the stable dirent still names our socket.

    The identity check is not atomic with unlink and therefore is not a
    same-UID adversarial-swap guarantee.
    """
    try:
        entry = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISSOCK(entry.st_mode) and (entry.st_dev, entry.st_ino) == identity:
        with contextlib.suppress(OSError):
            os.unlink(path)


def serve_unix(
    server: RpcServer,
    path: str,
    *,
    threaded: bool = False,
    max_connections: int | None = None,
    idle_timeout: float | None = None,
    on_bound: Callable[[str], None] | None = None,
) -> None:
    """Serve RPC on a Unix domain socket, accepting connections in a loop.

    Binds to *path*, listens, and accepts connections.  By default connections
    are handled sequentially (one at a time).  With ``threaded=True`` each
    accepted connection is served in its own daemon thread, allowing multiple
    clients to use the same socket concurrently.

    .. note::

       When ``threaded=True`` the *implementation* object passed to
       :class:`RpcServer` is shared across threads.  If it carries mutable
       state the caller must ensure thread-safety (e.g. via locks).  Per-
       connection stream state (:class:`StreamState`) is always isolated.

    Args:
        server: The RPC server to dispatch requests.
        path: Filesystem path for the Unix domain socket.
        threaded: When ``True``, serve each connection in a separate thread.
        max_connections: Maximum number of connections served simultaneously.
            Only meaningful when *threaded* is ``True``; ignored otherwise.
            Excess connections are accepted but queued until a slot is free.
            ``None`` means unlimited.
        idle_timeout: When set, the worker self-terminates after this many
            seconds with zero active connections.  Only meaningful when
            *threaded* is ``True``; raises ``ValueError`` otherwise.  A
            startup-grace timer of ``max(idle_timeout, 60)`` seconds protects
            the worker from shutting down before the first client arrives
            (e.g. during slow JVM cold-start).  ``None`` (default) keeps the
            accept loop running indefinitely.
        on_bound: Optional callback invoked once the socket is bound and
            listening, before the accept loop runs.  Used by ``run_server``
            to emit the ``UNIX:<path>`` discovery line on stdout only after
            bind has succeeded.  Exceptions raised by the callback propagate
            and abort the serve.

    Raises:
        RuntimeError: If another process is already listening on *path*.
        ValueError: If *idle_timeout* is set but *threaded* is ``False``.

    """
    if idle_timeout is not None and not threaded:
        raise ValueError("idle_timeout requires threaded=True")
    _check_no_existing_listener(path)
    _unlink_stale_unix_socket(path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    bound_identity: tuple[int, int] | None = None
    try:
        # Bind under a restrictive umask so the socket dirent is owner-only.
        saved_umask = os.umask(0o077)
        try:
            sock.bind(path)
        finally:
            os.umask(saved_umask)
        entry = os.lstat(path)
        bound_identity = (entry.st_dev, entry.st_ino)
        with contextlib.suppress(OSError):
            os.chmod(path, 0o600)
        # Even in sequential mode the listen backlog only governs the kernel's
        # pending-connection queue; it does *not* affect how many connections
        # we service at once.  A margin avoids brief macOS ECONNREFUSED windows.
        sock.listen(128 if threaded else 16)
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug(
                "serve_unix: server_id=%s, protocol=%s, path=%s, threaded=%s, idle_timeout=%s",
                server.server_id,
                server.protocol_name,
                path,
                threaded,
                idle_timeout,
            )
        if on_bound is not None:
            on_bound(path)
        if threaded:
            _serve_socket_threaded(server, sock, max_connections, idle_timeout, UnixTransport, "vgi-unix")
        else:
            _serve_socket_sequential(server, sock, UnixTransport)
    finally:
        sock.close()
        if bound_identity is not None:
            _unlink_bound_unix_socket(path, bound_identity)


def _serve_socket_sequential(
    server: RpcServer,
    sock: socket.socket,
    transport_factory: Callable[[socket.socket], RpcTransport],
) -> None:
    """Accept and serve connections one at a time.

    Args:
        server: The RPC server to dispatch requests.
        sock: A bound, listening stream socket (AF_UNIX or AF_INET).
        transport_factory: Builds a transport from an accepted connection
            socket — :class:`UnixTransport` or :class:`TcpTransport`.

    """
    while True:
        try:
            conn, _ = sock.accept()
        except OSError as exc:
            if _accept_resource_exhausted(exc):
                time.sleep(_ACCEPT_RESOURCE_RETRY_DELAY)
                continue
            break
        transport = transport_factory(conn)
        try:
            server.serve(transport)
        except Exception:
            _logger.debug("Error serving socket connection", exc_info=True)
        finally:
            transport.close()


def _serve_socket_threaded(
    server: RpcServer,
    sock: socket.socket,
    max_connections: int | None,
    idle_timeout: float | None,
    transport_factory: Callable[[socket.socket], RpcTransport],
    thread_name_prefix: str,
) -> None:
    """Accept connections in daemon threads, optionally idle-shutdown.

    With ``idle_timeout`` set, a startup-grace timer of ``max(idle_timeout, 60)``
    seconds is armed at bind; once the first client connects, subsequent idle
    periods (zero active connections) re-arm the idle timer.  When the timer
    fires it re-checks the connection count under the state lock before closing
    the listening socket — closing causes the accept loop's ``OSError`` branch
    to unwind cleanly so the surrounding ``finally`` can unlink the path.

    Args:
        server: The RPC server to dispatch requests.
        sock: A bound, listening stream socket (AF_UNIX or AF_INET).
        max_connections: Maximum simultaneous connections, or ``None`` for
            unlimited.
        idle_timeout: Seconds of zero-connection idle before self-terminating,
            or ``None`` to run indefinitely.
        transport_factory: Builds a transport from an accepted connection
            socket — :class:`UnixTransport` or :class:`TcpTransport`.
        thread_name_prefix: Prefix for the per-connection daemon thread names.

    """
    semaphore: threading.Semaphore | None = None
    if max_connections is not None:
        semaphore = threading.Semaphore(max_connections)
    active: set[threading.Thread] = set()
    state_lock = threading.Lock()
    conn_count = 0
    timer: threading.Timer | None = None
    shutdown_requested = False

    # Linux does not wake a blocked accept() when another thread closes the
    # socket, so the timer cannot reliably tear down the listener directly.
    # Drive accept on a short timeout and check a shutdown flag instead.
    sock.settimeout(0.5)

    def _close_listener_if_idle() -> None:
        nonlocal timer, shutdown_requested
        with state_lock:
            timer = None
            if conn_count != 0:
                return
            shutdown_requested = True

    def _arm_timer_locked(seconds: float) -> None:
        nonlocal timer
        if timer is not None:
            timer.cancel()
        timer = threading.Timer(seconds, _close_listener_if_idle)
        timer.daemon = True
        timer.start()

    def _cancel_timer_locked() -> None:
        nonlocal timer
        if timer is not None:
            timer.cancel()
            timer = None

    if idle_timeout is not None:
        with state_lock:
            _arm_timer_locked(max(idle_timeout, 60.0))

    def _handle(conn: socket.socket) -> None:
        nonlocal conn_count
        transport = transport_factory(conn)
        try:
            server.serve(transport)
        except Exception:
            _logger.debug("Error serving socket connection", exc_info=True)
        finally:
            transport.close()
            if semaphore is not None:
                semaphore.release()
            with state_lock:
                conn_count -= 1
                if conn_count == 0 and idle_timeout is not None:
                    _arm_timer_locked(idle_timeout)
                active.discard(threading.current_thread())

    try:
        while True:
            # Reserve a handler slot *before* accepting. Acquiring inside the
            # handler still lets the accept loop open an unbounded number of
            # sockets and threads that merely wait on the semaphore, defeating
            # the fd/memory cap it is meant to provide.
            if semaphore is not None:
                semaphore.acquire()
            try:
                conn, _ = sock.accept()
            except TimeoutError:
                if semaphore is not None:
                    semaphore.release()
                with state_lock:
                    if shutdown_requested:
                        break
                continue
            except OSError as exc:
                if semaphore is not None:
                    semaphore.release()
                if _accept_resource_exhausted(exc):
                    time.sleep(_ACCEPT_RESOURCE_RETRY_DELAY)
                    continue
                break
            conn.settimeout(None)  # accepted connections must be blocking
            with state_lock:
                conn_count += 1
                _cancel_timer_locked()
            t = threading.Thread(
                target=_handle,
                args=(conn,),
                daemon=True,
                name=f"{thread_name_prefix}-{conn.fileno()}",
            )
            with state_lock:
                active.add(t)
            t.start()
    finally:
        with state_lock:
            _cancel_timer_locked()
            snapshot = list(active)
        for t in snapshot:
            t.join(timeout=10)


# ---------------------------------------------------------------------------
# TcpTransport + make_tcp_pair + serve_tcp
#
# Raw Arrow-IPC framing over a bare TCP (AF_INET) socket — the network analog of
# UnixTransport.  There is NO authentication or TLS on this transport; bind it to
# a trusted network only (the default host is loopback-only).  For untrusted
# networks use the HTTP transport, which carries auth middleware and TLS via the
# fronting server.
# ---------------------------------------------------------------------------


class TcpTransport:
    """Transport backed by a connected TCP (AF_INET) socket.

    The reader is buffered (default ``makefile`` buffering) so that
    ``read(n)`` returns exactly *n* bytes — required by Arrow IPC.
    The writer is unbuffered (``buffering=0``) so data is flushed
    immediately, matching the pattern used by :class:`UnixTransport`.

    Nagle's algorithm is disabled (``TCP_NODELAY``) so the lockstep
    request/response framing is not delayed waiting to coalesce writes.
    """

    __slots__ = ("_reader", "_sock", "_writer")

    def __init__(self, sock: socket.socket) -> None:
        """Initialize from a connected AF_INET socket."""
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Deliberately no SO_SNDBUF/SO_RCVBUF here, unlike UnixTransport: TCP
        # already starts at 128 KiB and grows, an explicit SO_RCVBUF *disables*
        # Linux's receive-window auto-tuning and pins the window at whatever we
        # guessed, and measuring it on loopback showed no gain either way.
        self._sock = sock
        self._reader: IOBase = _clamped(cast("IOBase", sock.makefile("rb")))
        self._writer: IOBase = _exact(cast("IOBase", sock.makefile("wb", buffering=0)))

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._writer

    def close(self) -> None:
        """Close the reader, writer, and underlying socket."""
        self._reader.close()
        self._writer.close()
        self._sock.close()


def make_tcp_pair() -> tuple[TcpTransport, TcpTransport]:
    """Create connected client/server transports over a loopback TCP socket.

    ``socket.socketpair`` only supports ``AF_UNIX``, so for AF_INET we bind a
    throwaway listener on ``127.0.0.1:0``, connect a client, and accept the
    server side.  Returns ``(client_transport, server_transport)``.

    Returns:
        A ``(client_transport, server_transport)`` pair of connected
        :class:`TcpTransport` instances.

    """
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        client = socket.create_connection(listener.getsockname())
        server_conn, _ = listener.accept()
    finally:
        listener.close()
    if wire_transport_logger.isEnabledFor(logging.DEBUG):
        wire_transport_logger.debug(
            "make_tcp_pair: client_fd=%d, server_fd=%d",
            client.fileno(),
            server_conn.fileno(),
        )
    return TcpTransport(client), TcpTransport(server_conn)


def serve_tcp(
    server: RpcServer,
    host: str = "127.0.0.1",
    port: int = 0,
    *,
    threaded: bool = False,
    max_connections: int | None = None,
    idle_timeout: float | None = None,
    on_bound: Callable[[str, int], None] | None = None,
) -> None:
    """Serve RPC on a TCP socket, accepting connections in a loop.

    Binds to ``(host, port)``, listens, and accepts connections.  By default
    connections are handled sequentially (one at a time).  With
    ``threaded=True`` each accepted connection is served in its own daemon
    thread, allowing multiple clients to use the same socket concurrently.

    The default *host* is loopback-only (``127.0.0.1``).  Binding a routable
    address (e.g. ``0.0.0.0``) is explicit opt-in and exposes the **unauthenticated,
    unencrypted** raw framing protocol on the network — only do so on a trusted
    network; use the HTTP transport otherwise.

    .. note::

       When ``threaded=True`` the *implementation* object passed to
       :class:`RpcServer` is shared across threads.  If it carries mutable
       state the caller must ensure thread-safety (e.g. via locks).  Per-
       connection stream state (:class:`StreamState`) is always isolated.

    Args:
        server: The RPC server to dispatch requests.
        host: Interface to bind.  Defaults to ``127.0.0.1`` (loopback only).
        port: TCP port to bind.  ``0`` (default) lets the OS choose a free
            port, reported to *on_bound*.
        threaded: When ``True``, serve each connection in a separate thread.
        max_connections: Maximum number of connections served simultaneously.
            Only meaningful when *threaded* is ``True``; ignored otherwise.
            Excess connections are accepted but queued until a slot is free.
            ``None`` means unlimited.
        idle_timeout: When set, the worker self-terminates after this many
            seconds with zero active connections.  Only meaningful when
            *threaded* is ``True``; raises ``ValueError`` otherwise.  A
            startup-grace timer of ``max(idle_timeout, 60)`` seconds protects
            the worker from shutting down before the first client arrives.
            ``None`` (default) keeps the accept loop running indefinitely.
        on_bound: Optional callback invoked with ``(host, port)`` once the
            socket is bound and listening, before the accept loop runs.  The
            *port* is the actual bound port (resolved when ``port=0``).  Used
            by ``run_server`` to emit the ``TCP:<host>:<port>`` discovery line
            on stdout only after bind has succeeded.  Exceptions raised by the
            callback propagate and abort the serve.

    Raises:
        ValueError: If *idle_timeout* is set but *threaded* is ``False``.

    """
    if idle_timeout is not None and not threaded:
        raise ValueError("idle_timeout requires threaded=True")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    bound_port = int(sock.getsockname()[1])
    sock.listen(128 if threaded else 16)
    if wire_transport_logger.isEnabledFor(logging.DEBUG):
        wire_transport_logger.debug(
            "serve_tcp: server_id=%s, protocol=%s, host=%s, port=%d, threaded=%s, idle_timeout=%s",
            server.server_id,
            server.protocol_name,
            host,
            bound_port,
            threaded,
            idle_timeout,
        )
    if on_bound is not None:
        on_bound(host, bound_port)
    try:
        if threaded:
            _serve_socket_threaded(server, sock, max_connections, idle_timeout, TcpTransport, "vgi-tcp")
        else:
            _serve_socket_sequential(server, sock, TcpTransport)
    finally:
        sock.close()


# ---------------------------------------------------------------------------
# Windows named-pipe transport
#
# CPython does not expose ``socket.AF_UNIX`` on Windows, so the launcher's local
# rendezvous uses a Windows named pipe (``\\.\pipe\...``) there instead of an
# AF_UNIX socket. ``serve_named_pipe`` mirrors ``serve_unix``'s threaded /
# idle-timeout / on_bound semantics. Requires pywin32 (Windows-only dependency).
# ---------------------------------------------------------------------------


class NamedPipeTransport:
    """Transport over a connected Windows named-pipe handle.

    The connected ``PyHANDLE`` is converted to a CRT file descriptor via
    ``msvcrt.open_osfhandle`` and wrapped in standard buffered file objects, so
    the rest of the wire code sees the same reader/writer interface as
    :class:`UnixTransport`. The handle's ownership is transferred to the fd
    (``Detach``), so it is closed exactly once when the file objects close.
    """

    __slots__ = ("_reader", "_writer")

    def __init__(self, handle: object) -> None:
        if sys.platform != "win32":  # pragma: no cover - Windows-only
            raise RuntimeError("NamedPipeTransport is Windows-only")
        import msvcrt  # type: ignore[unreachable]  # reachable on win32; mypy runs on Linux

        raw = handle.Detach()  # take ownership from pywin32 (PyHANDLE)
        fd = msvcrt.open_osfhandle(raw, os.O_BINARY)
        self._reader: IOBase = _clamped(cast("IOBase", os.fdopen(fd, "rb")))
        self._writer: IOBase = _exact(cast("IOBase", os.fdopen(os.dup(fd), "wb", buffering=0)))

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._writer

    def close(self) -> None:
        """Close the reader and writer (and thus the underlying pipe handle)."""
        with contextlib.suppress(Exception):
            self._reader.close()
        with contextlib.suppress(Exception):
            self._writer.close()


def serve_named_pipe(
    server: RpcServer,
    pipe_name: str,
    *,
    threaded: bool = False,
    max_connections: int | None = None,
    idle_timeout: float | None = None,
    on_bound: Callable[[str], None] | None = None,
) -> None:
    r"""Serve RPC over a Windows named pipe — the Windows analog of ``serve_unix``.

    ``pipe_name`` is a full named-pipe name (``\\.\pipe\vgi-rpc-<hash>``). The
    pipe is created with ``PIPE_UNLIMITED_INSTANCES`` so concurrent clients each
    get their own instance. Semantics mirror :func:`serve_unix`: sequential by
    default; ``threaded=True`` serves each connection in a daemon thread with an
    optional ``idle_timeout`` self-shutdown (startup grace ``max(idle_timeout,
    60)``). ``on_bound`` is invoked once after the first pipe instance is created.

    The pipe instances are *synchronous* (no overlapped I/O) so per-connection
    ``ReadFile``/``WriteFile`` in the handler threads are simple blocking calls.
    To unblock the accept loop's blocking ``ConnectNamedPipe`` at idle shutdown,
    the idle timer briefly self-connects a throwaway client to the pipe.
    """
    if idle_timeout is not None and not threaded:
        raise ValueError("idle_timeout requires threaded=True")
    if sys.platform != "win32":  # pragma: no cover - Windows-only
        raise RuntimeError("serve_named_pipe is Windows-only")

    import pywintypes  # type: ignore[unreachable]  # reachable on win32; mypy runs on Linux
    import win32file
    import win32pipe
    import winerror

    state_lock = threading.Lock()
    conn_count = 0
    timer: threading.Timer | None = None
    shutdown_requested = False
    semaphore = threading.Semaphore(max_connections) if max_connections is not None else None
    active: set[threading.Thread] = set()

    def _self_connect_to_unblock() -> None:
        # Open (and immediately close) a client handle to wake a pending
        # ConnectNamedPipe so the accept loop can observe shutdown_requested.
        with contextlib.suppress(Exception):
            h = win32file.CreateFile(
                pipe_name,
                win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                0,
                None,
                win32file.OPEN_EXISTING,
                0,
                None,
            )
            win32file.CloseHandle(h)

    def _close_if_idle() -> None:
        nonlocal timer, shutdown_requested
        with state_lock:
            timer = None
            if conn_count != 0:
                return
            shutdown_requested = True
        _self_connect_to_unblock()

    def _arm_timer_locked(seconds: float) -> None:
        nonlocal timer
        if timer is not None:
            timer.cancel()
        timer = threading.Timer(seconds, _close_if_idle)
        timer.daemon = True
        timer.start()

    def _cancel_timer_locked() -> None:
        nonlocal timer
        if timer is not None:
            timer.cancel()
            timer = None

    if idle_timeout is not None:
        with state_lock:
            _arm_timer_locked(max(idle_timeout, 60.0))

    def _handle(handle: object) -> None:
        # The semaphore slot for this connection was already reserved by the
        # accept loop *before* the pipe instance was created (see the loop
        # below) — acquiring here instead would let the loop create an
        # unbounded number of pipe instances and threads that merely wait on
        # the semaphore, defeating the connection cap it is meant to provide.
        nonlocal conn_count
        transport = NamedPipeTransport(handle)
        try:
            server.serve(transport)
        except Exception:
            _logger.debug("Error serving named-pipe connection", exc_info=True)
        finally:
            transport.close()
            if semaphore is not None:
                semaphore.release()
            with state_lock:
                conn_count -= 1
                if conn_count == 0 and idle_timeout is not None:
                    _arm_timer_locked(idle_timeout)
                active.discard(threading.current_thread())

    bound = False
    try:
        while True:
            # Reserve a handler slot *before* creating the next pipe instance,
            # mirroring _serve_socket_threaded: acquiring inside the handler
            # would let this loop open an unbounded number of pipe instances
            # and threads that merely wait on the semaphore, defeating the
            # cap max_connections is meant to provide.
            if semaphore is not None:
                semaphore.acquire()
            handle = win32pipe.CreateNamedPipe(
                pipe_name,
                win32pipe.PIPE_ACCESS_DUPLEX,
                win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT,
                win32pipe.PIPE_UNLIMITED_INSTANCES,
                65536,
                65536,
                0,
                None,
            )
            if not bound:
                bound = True
                if on_bound is not None:
                    on_bound(pipe_name)
            try:
                win32pipe.ConnectNamedPipe(handle, None)  # blocks until a client connects
            except pywintypes.error as exc:
                # ERROR_PIPE_CONNECTED: a client connected between Create and Connect.
                if exc.winerror != winerror.ERROR_PIPE_CONNECTED:
                    win32file.CloseHandle(handle)
                    if semaphore is not None:
                        semaphore.release()
                    break
            with state_lock:
                if shutdown_requested:
                    win32file.CloseHandle(handle)  # this was the self-connect probe
                    if semaphore is not None:
                        semaphore.release()
                    break
                conn_count += 1
                _cancel_timer_locked()
            if not threaded:
                _handle(handle)
                with state_lock:
                    if shutdown_requested:
                        break
                continue
            t = threading.Thread(target=_handle, args=(handle,), daemon=True, name="vgi-pipe")
            with state_lock:
                active.add(t)
            t.start()
    finally:
        with state_lock:
            _cancel_timer_locked()
            snapshot = list(active)
        for t in snapshot:
            t.join(timeout=10)
