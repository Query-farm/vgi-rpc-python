"""Transport protocol and implementations."""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import sys
import threading
from enum import Enum
from io import IOBase
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast, runtime_checkable

from vgi_rpc.rpc._common import _logger
from vgi_rpc.rpc._debug import wire_transport_logger
from vgi_rpc.shm import ShmSegment

if TYPE_CHECKING:
    from vgi_rpc.rpc._server import RpcServer


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
        self._writer: IOBase = cast(IOBase, self._proc.stdin)
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
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
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
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
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
