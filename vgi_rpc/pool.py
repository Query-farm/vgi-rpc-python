"""Subprocess process pool with shared memory support.

Keeps idle worker subprocesses alive between ``connect()`` calls, avoiding
repeated process spawn/teardown overhead for session-oriented workloads.

Each ``connect()`` call borrows a worker (or spawns a new one), yields a
typed RPC proxy, and returns the worker to the pool on context exit.  When
``shm_size`` is set on the pool, each borrow gets its own isolated
:class:`~vgi_rpc.shm.ShmSegment` that is automatically destroyed on exit.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from io import IOBase

from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.log import Message
from vgi_rpc.rpc._client import RpcConnection, StreamSession
from vgi_rpc.rpc._transport import StderrMode, SubprocessTransport
from vgi_rpc.shm import ShmSegment
from vgi_rpc.utils import IpcValidation

__all__ = [
    "PoolMetrics",
    "WorkerPool",
]

_logger = logging.getLogger("vgi_rpc.pool")


# ---------------------------------------------------------------------------
# PoolMetrics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PoolMetrics:
    """Snapshot of pool counters and current state.

    Attributes:
        borrows: Total ``connect()`` calls.
        spawns: New subprocess spawns.
        reuses: Borrowed from idle pool.
        returns: Returned to idle pool.
        discards: Discarded (unhealthy, stream abandoned, or pool closed).
        evictions_idle: Evicted due to idle timeout.
        evictions_max: Evicted due to ``max_idle`` overflow.
        idle: Current idle count.
        active: Current active count.

    """

    borrows: int
    spawns: int
    reuses: int
    returns: int
    discards: int
    evictions_idle: int
    evictions_max: int
    idle: int
    active: int


# ---------------------------------------------------------------------------
# _IdleEntry
# ---------------------------------------------------------------------------


@dataclass
class _IdleEntry:
    """An idle worker awaiting reuse."""

    key: tuple[str, ...]
    transport: SubprocessTransport
    returned_at: float


# ---------------------------------------------------------------------------
# _PooledTransport — wraps SubprocessTransport for pool lifecycle
# ---------------------------------------------------------------------------


class _PooledTransport:
    """Transport wrapper that returns the subprocess to the pool on close.

    Implements the same duck-typed interface as :class:`RpcTransport` plus
    pool-specific attributes (``shm``, ``_stream_opened``).
    """

    __slots__ = ("_inner", "_last_stream_session", "_pool", "_returned", "_shm", "_stream_opened")

    def __init__(self, inner: SubprocessTransport, pool: WorkerPool, shm: ShmSegment | None = None) -> None:
        """Initialize wrapping *inner* transport, owned by *pool*."""
        self._inner = inner
        self._pool = pool
        self._returned = False
        self._shm = shm
        self._stream_opened = False
        self._last_stream_session: StreamSession | None = None

    @property
    def reader(self) -> IOBase:
        """Readable binary stream (delegated to inner transport)."""
        return self._inner.reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream (delegated to inner transport)."""
        return self._inner.writer

    @property
    def shm(self) -> ShmSegment | None:
        """The shared memory segment for this borrow, or ``None``."""
        return self._shm

    @property
    def proc(self) -> object:
        """The underlying Popen process (for diagnostics)."""
        return self._inner.proc

    def close(self) -> None:
        """Return to pool or discard — idempotent."""
        if self._returned:
            return
        self._returned = True
        self._shm = None
        # A stream is "abandoned" if it was opened but not cleanly closed
        stream_abandoned = self._stream_opened and (
            self._last_stream_session is None or not self._last_stream_session._closed
        )
        self._last_stream_session = None
        try:
            self._pool._return_worker(self._inner, stream_abandoned)
        except Exception:
            _logger.debug("Unexpected error returning worker to pool", exc_info=True)
            with contextlib.suppress(Exception):
                self._inner.close()


# ---------------------------------------------------------------------------
# WorkerPool
# ---------------------------------------------------------------------------


class WorkerPool:
    """Subprocess process pool with optional shared memory support.

    Workers are keyed by their command tuple.  Idle workers are cached up
    to *max_idle* globally.  A daemon reaper thread evicts workers that
    exceed *idle_timeout*.

    Args:
        max_idle: Global cap on idle workers across all commands.
        idle_timeout: Seconds before an idle worker is evicted.
        stderr: How to handle child process stderr.
        stderr_logger: Logger for ``StderrMode.PIPE`` output.
        shm_size: If set, each ``connect()`` call creates an isolated
            :class:`~vgi_rpc.shm.ShmSegment` of this size (in bytes)
            for zero-copy data transfer.  The segment is destroyed
            automatically when the borrow ends.

    """

    def __init__(
        self,
        max_idle: int = 4,
        idle_timeout: float = 60.0,
        stderr: StderrMode = StderrMode.INHERIT,
        stderr_logger: logging.Logger | None = None,
        shm_size: int | None = None,
    ) -> None:
        """Initialize the pool with the given configuration."""
        if max_idle < 0:
            raise ValueError(f"max_idle must be non-negative, got {max_idle}")
        if idle_timeout <= 0:
            raise ValueError(f"idle_timeout must be positive, got {idle_timeout}")

        self._max_idle = max_idle
        self._idle_timeout = idle_timeout
        self._stderr = stderr
        self._stderr_logger = stderr_logger
        self._shm_size = shm_size

        self._lock = threading.Lock()
        # Keyed idle deques: cmd tuple → deque of _IdleEntry (LIFO)
        self._idle: dict[tuple[str, ...], deque[_IdleEntry]] = {}
        self._closed = False

        # Counters
        self._borrows = 0
        self._spawns = 0
        self._reuses = 0
        self._returns = 0
        self._discards = 0
        self._evictions_idle = 0
        self._evictions_max = 0
        self._active = 0

        # Reaper thread
        self._stop_event = threading.Event()
        self._reaper = threading.Thread(target=self._reaper_loop, daemon=True, name="vgi_rpc.pool.reaper")
        self._reaper.start()

        atexit.register(self.close)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    @contextlib.contextmanager
    def connect[P](
        self,
        protocol: type[P],
        cmd: list[str],
        *,
        on_log: Callable[[Message], None] | None = None,
        external_location: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Iterator[P]:
        """Borrow a worker, yield a typed proxy, return the worker on exit.

        If the pool was created with ``shm_size``, each borrow gets its own
        isolated :class:`~vgi_rpc.shm.ShmSegment` that is destroyed on exit.

        Args:
            protocol: The Protocol class defining the RPC interface.
            cmd: Command to spawn the subprocess worker.
            on_log: Optional callback for log messages from the server.
            external_location: Optional ExternalLocation configuration.
            ipc_validation: Validation level for incoming IPC batches.

        Yields:
            A typed RPC proxy supporting all methods defined on *protocol*.

        Raises:
            RuntimeError: If the pool has been closed.

        """
        if self._closed:
            raise RuntimeError("WorkerPool is closed")

        shm: ShmSegment | None = None
        pooled: _PooledTransport | None = None
        try:
            shm = ShmSegment.create(self._shm_size) if self._shm_size is not None else None
            key = tuple(cmd)
            inner = self._borrow(key)
            pooled = _PooledTransport(inner, self, shm=shm)
            with RpcConnection(
                protocol,
                pooled,
                on_log=on_log,
                external_location=external_location,
                ipc_validation=ipc_validation,
            ) as proxy:
                yield proxy
        finally:
            if pooled is not None:
                pooled.close()
            if shm is not None:
                with contextlib.suppress(FileNotFoundError):
                    shm.unlink()
                with contextlib.suppress(BufferError):
                    shm.close()

    @property
    def idle_count(self) -> int:
        """Current number of idle workers."""
        with self._lock:
            return sum(len(d) for d in self._idle.values())

    @property
    def active_count(self) -> int:
        """Current number of actively borrowed workers."""
        with self._lock:
            return self._active

    @property
    def metrics(self) -> PoolMetrics:
        """Snapshot of pool counters and current state."""
        with self._lock:
            return PoolMetrics(
                borrows=self._borrows,
                spawns=self._spawns,
                reuses=self._reuses,
                returns=self._returns,
                discards=self._discards,
                evictions_idle=self._evictions_idle,
                evictions_max=self._evictions_max,
                idle=sum(len(d) for d in self._idle.values()),
                active=self._active,
            )

    def close(self) -> None:
        """Terminate all idle workers and stop the reaper thread.

        Idempotent — safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True
        atexit.unregister(self.close)

        # Stop reaper
        self._stop_event.set()
        self._reaper.join(timeout=5)

        # Collect all idle workers under lock, then close outside lock
        with self._lock:
            active = self._active
            all_idle: list[SubprocessTransport] = []
            for dq in self._idle.values():
                all_idle.extend(entry.transport for entry in dq)
                dq.clear()
            self._idle.clear()

        if active > 0:
            _logger.warning("WorkerPool closing with %d active connections", active)
        _logger.info("WorkerPool closed: terminated %d idle workers", len(all_idle))

        for transport in all_idle:
            self._discards += 1
            with contextlib.suppress(Exception):
                transport.close()

    def __enter__(self) -> WorkerPool:
        """Enter context manager."""
        return self

    def __exit__(self, *args: object) -> None:
        """Close the pool on context exit."""
        self.close()

    # -----------------------------------------------------------------------
    # Internal: borrow / return
    # -----------------------------------------------------------------------

    def _borrow(self, key: tuple[str, ...]) -> SubprocessTransport:
        """Pop an idle worker for *key* or spawn a new one."""
        with self._lock:
            self._borrows += 1
            self._active += 1
            dq = self._idle.get(key)
            if dq:
                # LIFO pop for cache warmth
                entry = dq.pop()
                if not dq:
                    del self._idle[key]
                self._reuses += 1
                transport = entry.transport
                _logger.debug("Borrowed idle worker: pid=%d, cmd=%s", transport.proc.pid, key)
                # Health check — process still alive?
                if transport.proc.poll() is not None:
                    _logger.warning(
                        "Idle worker died unexpectedly: pid=%d, exit_code=%s",
                        transport.proc.pid,
                        transport.proc.returncode,
                    )
                    self._discards += 1
                    self._reuses -= 1
                    # Fall through to spawn
                else:
                    return transport

        # Spawn new
        transport = SubprocessTransport(list(key), stderr=self._stderr, stderr_logger=self._stderr_logger)
        with self._lock:
            self._spawns += 1
        _logger.info("Spawned new worker: pid=%d, cmd=%s", transport.proc.pid, key)
        return transport

    def _return_worker(self, transport: SubprocessTransport, stream_opened: bool) -> None:
        """Return a worker to the pool or discard it."""
        # Check process health
        if transport.proc.poll() is not None:
            _logger.warning(
                "Worker died during use: pid=%d, exit_code=%s",
                transport.proc.pid,
                transport.proc.returncode,
            )
            with self._lock:
                self._active -= 1
                self._discards += 1
            return

        # Abandoned stream — transport has stale data, must discard
        if stream_opened:
            _logger.warning("Discarding worker with abandoned stream: pid=%d", transport.proc.pid)
            with self._lock:
                self._active -= 1
                self._discards += 1
            transport.close()
            return

        evicted: SubprocessTransport | None = None
        with self._lock:
            self._active -= 1

            if self._closed:
                self._discards += 1
                transport.close()
                return

            key = (
                tuple(str(a) for a in transport.proc.args)
                if isinstance(transport.proc.args, (list, tuple))
                else (str(transport.proc.args),)
            )
            total_idle = sum(len(d) for d in self._idle.values())

            # If at capacity, evict the globally oldest idle worker
            if total_idle >= self._max_idle:
                evicted = self._evict_oldest_locked()

            dq = self._idle.setdefault(key, deque())
            dq.append(_IdleEntry(key=key, transport=transport, returned_at=time.monotonic()))
            self._returns += 1
            _logger.debug("Returned worker to pool: pid=%d", transport.proc.pid)

        # Close evicted transport outside the lock
        if evicted is not None:
            _logger.info("Evicted worker (max_idle): pid=%d", evicted.proc.pid)
            with contextlib.suppress(Exception):
                evicted.close()

    def _evict_oldest_locked(self) -> SubprocessTransport | None:
        """Evict the globally oldest idle worker. Caller MUST hold self._lock."""
        oldest_key: tuple[str, ...] | None = None
        oldest_time = float("inf")
        for key, dq in self._idle.items():
            if dq and dq[0].returned_at < oldest_time:
                oldest_time = dq[0].returned_at
                oldest_key = key
        if oldest_key is not None:
            dq = self._idle[oldest_key]
            entry = dq.popleft()
            if not dq:
                del self._idle[oldest_key]
            self._evictions_max += 1
            return entry.transport
        return None

    # -----------------------------------------------------------------------
    # Reaper thread
    # -----------------------------------------------------------------------

    def _reaper_loop(self) -> None:
        """Daemon thread: periodically evict idle-timeout workers."""
        while not self._stop_event.wait(timeout=min(self._idle_timeout, 5.0)):
            self._reap_expired()

    def _reap_expired(self) -> None:
        """Collect and terminate expired idle workers."""
        now = time.monotonic()
        expired: list[SubprocessTransport] = []
        with self._lock:
            for key in list(self._idle.keys()):
                dq = self._idle[key]
                while dq and (now - dq[0].returned_at) >= self._idle_timeout:
                    entry = dq.popleft()
                    expired.append(entry.transport)
                    self._evictions_idle += 1
                if not dq:
                    del self._idle[key]

        for transport in expired:
            _logger.info("Evicted worker (idle timeout): pid=%d", transport.proc.pid)
            with contextlib.suppress(Exception):
                transport.close()
