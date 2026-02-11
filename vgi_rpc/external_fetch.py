"""Parallel range-request fetching for external location URLs.

Centralizes all URL fetching with a HEAD-first probing strategy:

1. A **HEAD** request is issued to learn ``Content-Length`` and
   ``Accept-Ranges`` without downloading the body.
2. If the HEAD succeeds and indicates parallel Range support above the
   threshold, **parallel Range GETs** are issued with time-based
   multi-chunk speculative hedging for stragglers.
3. Otherwise (below threshold, no Range support, or HEAD unsupported)
   a single **GET** request downloads the body directly.

If the server does not support HEAD (returns 405 or similar), the
module transparently falls back to a single GET.

The module maintains a persistent ``aiohttp.ClientSession`` per
``FetchConfig`` instance, backed by a daemon thread running
``loop.run_forever()``.  Connections are pooled and reused across calls.
Stale connections (``ServerDisconnectedError``) trigger automatic session
recreation.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import threading
import time
from dataclasses import dataclass, field
from types import TracebackType

import aiohttp
import zstandard

__all__ = [
    "FetchConfig",
    "fetch_url",
]

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pool state container (mutable interior for frozen FetchConfig)
# ---------------------------------------------------------------------------


@dataclass
class _FetchPool:
    """Mutable connection-pool state owned by a ``FetchConfig``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    loop: asyncio.AbstractEventLoop | None = None
    thread: threading.Thread | None = None
    session: aiohttp.ClientSession | None = None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchConfig:
    """Configuration for parallel range-request fetching.

    Maintains a persistent ``aiohttp.ClientSession`` backed by a daemon
    thread.  Use as a context manager or call ``close()`` to release
    resources.

    Attributes:
        parallel_threshold_bytes: Below this size, use a single GET.
        chunk_size_bytes: Size of each Range request chunk.
        max_parallel_requests: Semaphore limit on concurrent requests.
        timeout_seconds: Overall deadline for the fetch.
        max_fetch_bytes: Hard cap on total download size.
        speculative_retry_multiplier: Launch a hedge request for chunks
            taking longer than ``multiplier * median``.  Set to ``0`` to
            disable hedging.

    """

    parallel_threshold_bytes: int = 64 * 1024 * 1024
    chunk_size_bytes: int = 8 * 1024 * 1024
    max_parallel_requests: int = 8
    timeout_seconds: float = 60.0
    max_fetch_bytes: int = 256 * 1024 * 1024
    speculative_retry_multiplier: float = 2.0

    _pool: _FetchPool = field(default_factory=_FetchPool, init=False, repr=False, compare=False, hash=False)

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the pooled session, stop the event loop, and join the thread."""
        pool = self._pool
        with pool.lock:
            if pool.session is not None and pool.loop is not None and not pool.loop.is_closed():
                asyncio.run_coroutine_threadsafe(pool.session.close(), pool.loop).result(timeout=5)
            pool.session = None
            if pool.loop is not None and not pool.loop.is_closed():
                pool.loop.call_soon_threadsafe(pool.loop.stop)
            if pool.thread is not None:
                pool.thread.join(timeout=5)
            pool.loop = None
            pool.thread = None

    def __del__(self) -> None:
        """Safety net: close pool on garbage collection."""
        with contextlib.suppress(Exception):
            self.close()

    def __enter__(self) -> FetchConfig:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager, closing the pool."""
        self.close()


# ---------------------------------------------------------------------------
# Pool helpers
# ---------------------------------------------------------------------------


def _ensure_pool(config: FetchConfig) -> _FetchPool:
    """Lazily initialize the background loop, thread, and session.

    Thread-safe: uses ``_FetchPool.lock`` to serialize initialization.
    Auto-recovers after ``close()`` (loop is ``None`` or closed).
    """
    pool = config._pool
    with pool.lock:
        if pool.loop is None or pool.loop.is_closed():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=loop.run_forever, daemon=True)
            thread.start()
            pool.loop = loop
            pool.thread = thread
            pool.session = None  # will be created below

        if pool.session is None:
            if pool.loop is None:  # pragma: no cover — unreachable after block above
                raise RuntimeError("FetchPool event loop not initialized")
            timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
            future = asyncio.run_coroutine_threadsafe(
                _create_session(timeout),
                pool.loop,
            )
            pool.session = future.result(timeout=5)
    return pool


async def _create_session(timeout: aiohttp.ClientTimeout) -> aiohttp.ClientSession:
    """Create a new ``aiohttp.ClientSession`` (must run on the pool loop)."""
    return aiohttp.ClientSession(timeout=timeout)


def _reset_session(config: FetchConfig, *, url: str = "") -> None:
    """Close the current session and create a fresh one on the same loop.

    Used after ``ServerDisconnectedError`` to recover from stale pooled
    connections.
    """
    _logger.warning(
        "Resetting aiohttp session due to stale connection",
        extra={"url": url},
    )
    pool = config._pool
    with pool.lock:
        if pool.loop is None or pool.loop.is_closed():
            return
        if pool.session is not None:
            asyncio.run_coroutine_threadsafe(pool.session.close(), pool.loop).result(timeout=5)
            pool.session = None
        timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
        future = asyncio.run_coroutine_threadsafe(
            _create_session(timeout),
            pool.loop,
        )
        pool.session = future.result(timeout=5)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_url(url: str, config: FetchConfig) -> bytes:
    """Fetch URL contents, using parallel Range requests when beneficial.

    Uses a persistent connection pool owned by *config*.  Stale
    connections (``ServerDisconnectedError``, ``ConnectionResetError``)
    trigger automatic session recreation and a single retry.

    Args:
        url: The URL to fetch.
        config: Fetch configuration controlling parallelism and limits.

    Returns:
        The complete response body as bytes.

    Raises:
        RuntimeError: If the content exceeds ``max_fetch_bytes`` or the
            overall timeout is exceeded.
        aiohttp.ClientResponseError: On HTTP error responses.

    """
    t0 = time.monotonic()
    pool = _ensure_pool(config)
    if pool.loop is None or pool.session is None:  # pragma: no cover
        raise RuntimeError("FetchPool not properly initialized")
    future = asyncio.run_coroutine_threadsafe(
        _fetch_with_probe(url, config, pool.session),
        pool.loop,
    )
    try:
        data = future.result()
    except (aiohttp.ServerDisconnectedError, ConnectionResetError):
        _reset_session(config, url=url)
        new_pool = config._pool
        if new_pool.loop is None or new_pool.session is None:  # pragma: no cover
            raise RuntimeError("FetchPool not properly initialized after reset") from None
        retry_future = asyncio.run_coroutine_threadsafe(
            _fetch_with_probe(url, config, new_pool.session),
            new_pool.loop,
        )
        data = retry_future.result()
    duration_ms = (time.monotonic() - t0) * 1000
    _logger.debug(
        "Fetch completed: %s (%d bytes, %.1fms)",
        url,
        len(data),
        duration_ms,
        extra={"url": url, "size_bytes": len(data), "duration_ms": round(duration_ms, 2)},
    )
    return data


# ---------------------------------------------------------------------------
# Core async implementation
# ---------------------------------------------------------------------------


async def _read_response_body(resp: aiohttp.ClientResponse, config: FetchConfig) -> bytes:
    """Read the full body from an already-open GET response with size limit."""
    chunks: list[bytes] = []
    total = 0
    async for chunk in resp.content.iter_chunked(65536):
        total += len(chunk)
        if total > config.max_fetch_bytes:
            raise RuntimeError(f"ExternalLocation fetch exceeded max_fetch_bytes ({config.max_fetch_bytes} bytes)")
        chunks.append(chunk)
    return b"".join(chunks)


async def _fetch_with_probe(url: str, config: FetchConfig, client: aiohttp.ClientSession) -> bytes:
    """Core async fetch: HEAD probe, then parallel Range GETs or simple GET.

    Issues a HEAD request first to learn Content-Length and Accept-Ranges
    without downloading the body.  Falls back to a plain GET when HEAD is
    unsupported (405 / 501) or the response lacks the headers needed for
    parallel fetching.

    If the HEAD response includes ``Content-Encoding: zstd``, the
    downloaded bytes are automatically decompressed before returning.
    """
    content_length, accept_ranges, content_encoding = await _head_probe(url, client)

    # Guard: reject before downloading
    if content_length is not None and content_length > config.max_fetch_bytes:
        raise RuntimeError(
            f"ExternalLocation fetch rejected: Content-Length {content_length} "
            f"exceeds max_fetch_bytes ({config.max_fetch_bytes})"
        )

    # Decide path
    use_parallel = (
        content_length is not None
        and "bytes" in accept_ranges.lower()
        and content_length >= config.parallel_threshold_bytes
    )

    if use_parallel:
        if content_length is None:  # pragma: no cover — guarded by use_parallel check
            raise RuntimeError("content_length unexpectedly None on parallel path")
        data = await _fetch_chunks_with_hedging(client, url, content_length, config)
    else:
        # Simple path: single GET
        async with client.get(url) as resp:
            resp.raise_for_status()
            data = await _read_response_body(resp, config)

    # Decompress if the server indicates zstd Content-Encoding
    if "zstd" in content_encoding.lower():
        try:
            data = zstandard.ZstdDecompressor().decompress(data)
        except zstandard.ZstdError as exc:
            raise RuntimeError(f"Failed to decompress zstd data ({len(data)} compressed bytes) from {url}") from exc

    return data


async def _head_probe(url: str, client: aiohttp.ClientSession) -> tuple[int | None, str, str]:
    """Issue a HEAD request and return (content_length, accept_ranges, content_encoding).

    Returns ``(None, "", "")`` if the server does not support HEAD (405, 501)
    or returns an error, allowing the caller to fall back to a plain GET.
    """
    try:
        async with client.head(url) as resp:
            if resp.status in (405, 501):
                return None, "", ""
            resp.raise_for_status()
            content_length_str = resp.headers.get("Content-Length")
            accept_ranges = resp.headers.get("Accept-Ranges", "")
            content_encoding = resp.headers.get("Content-Encoding", "")

            content_length: int | None = None
            if content_length_str is not None:
                try:
                    content_length = int(content_length_str)
                except ValueError:
                    content_length = None

            return content_length, accept_ranges, content_encoding
    except aiohttp.ClientResponseError as exc:
        if exc.status in (405, 501):
            return None, "", ""
        raise


# ---------------------------------------------------------------------------
# Parallel (Range GET) path with speculative hedging
# ---------------------------------------------------------------------------


def _compute_ranges(content_length: int, chunk_size: int) -> list[tuple[int, int]]:
    """Compute (start, end) byte ranges for parallel fetch.

    Each tuple is ``(start_inclusive, end_inclusive)`` matching HTTP Range
    header semantics.
    """
    num_chunks = math.ceil(content_length / chunk_size)
    ranges: list[tuple[int, int]] = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = min(start + chunk_size - 1, content_length - 1)
        ranges.append((start, end))
    return ranges


async def _fetch_one_chunk(
    client: aiohttp.ClientSession,
    url: str,
    start: int,
    end: int,
    semaphore: asyncio.Semaphore,
) -> bytes:
    """Fetch a single byte range.

    Validates that the server returns HTTP 206 Partial Content. If the
    server ignores the Range header and returns 200, it would silently
    deliver the full body for every chunk, corrupting the reassembled
    result.
    """
    expected_size = end - start + 1
    async with semaphore:
        headers = {"Range": f"bytes={start}-{end}"}
        async with client.get(url, headers=headers) as resp:
            resp.raise_for_status()
            if resp.status != 206:
                raise RuntimeError(
                    f"Expected HTTP 206 for Range request, got {resp.status} (bytes={start}-{end} of {url})"
                )
            data = await resp.read()
            if len(data) != expected_size:
                raise RuntimeError(
                    f"Range chunk size mismatch: expected {expected_size} bytes, "
                    f"got {len(data)} (bytes={start}-{end} of {url})"
                )
            return data


async def _fetch_chunks_with_hedging(
    client: aiohttp.ClientSession,
    url: str,
    content_length: int,
    config: FetchConfig,
) -> bytes:
    """Parallel chunk fetching with time-based multi-chunk speculative hedging."""
    ranges = _compute_ranges(content_length, config.chunk_size_bytes)
    semaphore = asyncio.Semaphore(config.max_parallel_requests)
    results: dict[int, bytes] = {}
    completion_times: list[float] = []
    hedging_enabled = config.speculative_retry_multiplier > 0

    # Track per-task timing and mapping
    task_start_times: dict[asyncio.Task[tuple[int, bytes]], float] = {}
    task_to_chunk: dict[asyncio.Task[tuple[int, bytes]], int] = {}
    hedged_chunks: set[int] = set()

    def _create_chunk_task(idx: int, start: int, end: int) -> asyncio.Task[tuple[int, bytes]]:
        """Create a timed chunk fetch task with closure-captured t0."""
        t0 = time.monotonic()

        async def _timed_fetch() -> tuple[int, bytes]:
            data = await _fetch_one_chunk(client, url, start, end, semaphore)
            elapsed = time.monotonic() - t0
            completion_times.append(elapsed)
            return idx, data

        task = asyncio.create_task(_timed_fetch())
        task_start_times[task] = t0
        task_to_chunk[task] = idx
        return task

    # Launch initial tasks
    pending: set[asyncio.Task[tuple[int, bytes]]] = set()
    for idx, (start, end) in enumerate(ranges):
        task = _create_chunk_task(idx, start, end)
        pending.add(task)

    # Collect results, launching hedges for slow chunks
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            first_exc: BaseException | None = None
            for task in done:
                # Clean up tracking for completed tasks
                task_start_times.pop(task, None)
                task_to_chunk.pop(task, None)
                try:
                    idx, data = task.result()
                except BaseException as exc:
                    if first_exc is None:
                        first_exc = exc
                    continue
                if idx not in results:
                    results[idx] = data

            if first_exc is not None:
                raise first_exc

            # Early exit if all chunks collected
            if len(results) == len(ranges):
                break

            # Time-based hedging: after >= 2 completions, hedge any pending
            # chunk whose elapsed time exceeds median * multiplier
            if hedging_enabled and len(completion_times) >= 2:
                sorted_times = sorted(completion_times)
                median = sorted_times[len(sorted_times) // 2]
                threshold = median * config.speculative_retry_multiplier
                now = time.monotonic()

                for task in list(pending):
                    chunk_idx = task_to_chunk.get(task)
                    if chunk_idx is None or chunk_idx in hedged_chunks or chunk_idx in results:
                        continue
                    start_time = task_start_times.get(task)
                    if start_time is not None and (now - start_time) > threshold:
                        start, end = ranges[chunk_idx]
                        hedge_task = _create_chunk_task(chunk_idx, start, end)
                        pending.add(hedge_task)
                        hedged_chunks.add(chunk_idx)
    finally:
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # Reassemble in order
    ordered = b"".join(results[i] for i in range(len(ranges)))

    # Defense-in-depth: verify total size after reassembly
    if len(ordered) > config.max_fetch_bytes:
        raise RuntimeError(
            f"ExternalLocation fetch exceeded max_fetch_bytes after reassembly "
            f"({len(ordered)} > {config.max_fetch_bytes} bytes)"
        )

    return ordered
