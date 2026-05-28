# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport protocol and implementations."""

from __future__ import annotations

import contextlib
import logging
import os
import socket
import subprocess
import sys
import threading
from collections.abc import Callable
from enum import Enum
from io import IOBase
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast, runtime_checkable

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
        os.fdopen(s2c_r, "rb"),
        os.fdopen(c2s_w, "wb", buffering=0),
    )
    server = PipeTransport(
        os.fdopen(c2s_r, "rb"),
        os.fdopen(s2c_w, "wb", buffering=0),
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
        self._reader: IOBase = os.fdopen(self._proc.stdout.fileno(), "rb", closefd=False)
        self._writer: IOBase = cast("IOBase", self._proc.stdin)
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
    reader = os.fdopen(sys.stdin.fileno(), "rb", closefd=False)
    writer = os.fdopen(sys.stdout.fileno(), "wb", buffering=0, closefd=False)
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
        self._sock = sock
        self._reader: IOBase = cast("IOBase", sock.makefile("rb"))
        self._writer: IOBase = cast("IOBase", sock.makefile("wb", buffering=0))

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
    """Raise ``RuntimeError`` if another process is already listening on *path*.

    Best-effort defense-in-depth — between this probe and the caller's bind,
    another process could still claim the socket.  The launcher coordinates
    via flock at a higher layer; this guards against accidental misuse where
    two workers are pointed at the same path.
    """
    test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        test_sock.connect(path)
    except (FileNotFoundError, ConnectionRefusedError):
        return
    finally:
        test_sock.close()
    raise RuntimeError(f"another process is already listening on {path}")


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
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    # Best-effort cleanup of any stale socket file.  Broad ``OSError`` catch
    # because Windows can raise ``ERROR_SHARING_VIOLATION`` (a ``PermissionError``
    # subclass) if the kernel still considers the file in use.
    with contextlib.suppress(OSError):
        os.unlink(path)
    # Bind under a restrictive umask so the socket dirent is owner-only.  On
    # Windows ``os.umask`` is a no-op for AF_UNIX permissions, so we follow
    # up with an explicit chmod (also a belt-and-suspenders on POSIX in case
    # the caller had a relaxed umask in scope before us).
    saved_umask = os.umask(0o077)
    try:
        sock.bind(path)
    finally:
        os.umask(saved_umask)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)
    # Even in sequential mode the listen backlog only governs the kernel's
    # pending-connection queue; it does *not* affect how many connections we
    # service at once.  ``listen(1)`` is fragile on macOS, where the backlog
    # is enforced strictly and the brief window between ``serve()`` returning
    # and the next ``accept()`` can drop incoming connects with
    # ECONNREFUSED.  16 leaves margin without changing serving semantics.
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
    try:
        if threaded:
            _serve_unix_threaded(server, sock, max_connections, idle_timeout)
        else:
            _serve_unix_sequential(server, sock)
    finally:
        sock.close()
        with contextlib.suppress(OSError):
            os.unlink(path)


def _serve_unix_sequential(server: RpcServer, sock: socket.socket) -> None:
    """Accept and serve connections one at a time."""
    while True:
        try:
            conn, _ = sock.accept()
        except OSError:
            break
        transport = UnixTransport(conn)
        try:
            server.serve(transport)
        except Exception:
            _logger.debug("Error serving Unix connection", exc_info=True)
        finally:
            transport.close()


def _serve_unix_threaded(
    server: RpcServer,
    sock: socket.socket,
    max_connections: int | None,
    idle_timeout: float | None,
) -> None:
    """Accept connections in daemon threads, optionally idle-shutdown.

    With ``idle_timeout`` set, a startup-grace timer of ``max(idle_timeout, 60)``
    seconds is armed at bind; once the first client connects, subsequent idle
    periods (zero active connections) re-arm the idle timer.  When the timer
    fires it re-checks the connection count under the state lock before closing
    the listening socket — closing causes the accept loop's ``OSError`` branch
    to unwind cleanly so the surrounding ``finally`` can unlink the path.
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
        if semaphore is not None:
            semaphore.acquire()
        transport = UnixTransport(conn)
        try:
            server.serve(transport)
        except Exception:
            _logger.debug("Error serving Unix connection", exc_info=True)
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
            try:
                conn, _ = sock.accept()
            except TimeoutError:
                with state_lock:
                    if shutdown_requested:
                        break
                continue
            except OSError:
                break
            conn.settimeout(None)  # accepted connections must be blocking
            with state_lock:
                conn_count += 1
                _cancel_timer_locked()
            t = threading.Thread(
                target=_handle,
                args=(conn,),
                daemon=True,
                name=f"vgi-unix-{conn.fileno()}",
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
        self._reader: IOBase = cast("IOBase", os.fdopen(fd, "rb"))
        self._writer: IOBase = cast("IOBase", os.fdopen(os.dup(fd), "wb", buffering=0))

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
        nonlocal conn_count
        if semaphore is not None:
            semaphore.acquire()
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
                    break
            with state_lock:
                if shutdown_requested:
                    win32file.CloseHandle(handle)  # this was the self-connect probe
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
