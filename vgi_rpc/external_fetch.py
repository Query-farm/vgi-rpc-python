# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

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
import re
import threading
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse

from vgi_rpc._codec import Encoding as _CodecEncoding

if TYPE_CHECKING:
    import aiohttp

__all__ = [
    "FetchConfig",
    "fetch_url",
    "redact_url",
]

_logger = logging.getLogger(__name__)

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


def redact_url(url: str) -> str:
    """Return a diagnostic-safe URL with credentials and bearer data removed.

    Query strings on external-location URLs commonly contain S3/GCS signing
    credentials. Userinfo and fragments can carry credentials too. Keep only
    the scheme, host, port, and path so errors remain actionable without
    turning logs or remote tracebacks into credential stores.
    """
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return "<invalid-url>"
        host = parsed.hostname
        if host is None:
            return "<invalid-url>"
        rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
        if parsed.port is not None:
            rendered_host = f"{rendered_host}:{parsed.port}"
        return urlunparse((parsed.scheme, rendered_host, parsed.path, "", "", ""))
    except (TypeError, ValueError):
        return "<invalid-url>"


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
        max_fetch_bytes: Hard cap on encoded/on-wire download size.
        max_decompressed_bytes: Hard cap after content decoding. ``None``
            preserves the historical effective limit of
            ``16 * max_fetch_bytes``.
        max_redirects: Maximum redirects followed by each probe or data
            request. Every target is passed through the caller's URL validator
            before it is requested. Set to ``0`` to reject redirects.
        speculative_retry_multiplier: Launch a hedge request for chunks
            taking longer than ``multiplier * median``.  Set to ``0`` to
            disable hedging.
        max_speculative_hedges: Maximum number of hedge requests per
            fetch.  Prevents runaway request amplification under
            adversarial or slow-backend conditions.  ``0`` means no
            limit (bounded only by chunk count).

    """

    parallel_threshold_bytes: int = 64 * 1024 * 1024
    chunk_size_bytes: int = 8 * 1024 * 1024
    max_parallel_requests: int = 8
    timeout_seconds: float = 60.0
    max_fetch_bytes: int = 256 * 1024 * 1024
    max_decompressed_bytes: int | None = None
    max_redirects: int = 5
    speculative_retry_multiplier: float = 2.0
    max_speculative_hedges: int = 4

    _pool: _FetchPool = field(default_factory=_FetchPool, init=False, repr=False, compare=False, hash=False)

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the pooled session, stop the event loop, and join the thread."""
        pool = self._pool
        with pool.lock:
            loop = pool.loop
            thread = pool.thread
            if pool.session is not None and loop is not None and not loop.is_closed():
                asyncio.run_coroutine_threadsafe(pool.session.close(), loop).result(timeout=5)
            pool.session = None
            if loop is not None and not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop)
            if thread is not None:
                thread.join(timeout=5)
                if thread.is_alive():
                    raise RuntimeError("external fetch event-loop thread did not stop within 5s")
            # ``loop.stop()`` and joining its thread do not release the event
            # loop's selector or self-pipe descriptors. Close it explicitly
            # before dropping the last owned reference; repeated short-lived
            # client configs otherwise exhaust macOS's common 256-FD limit.
            if loop is not None and not loop.is_closed():
                loop.close()
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
            import aiohttp

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
    import aiohttp as _aiohttp

    # Keep decompression behavior inside vgi-rpc so we don't double-decompress
    # when handling Content-Encoding ourselves after probe/read.
    return _aiohttp.ClientSession(timeout=timeout, auto_decompress=False)


def _reset_session(config: FetchConfig, *, url: str = "") -> None:
    """Close the current session and create a fresh one on the same loop.

    Used after ``ServerDisconnectedError`` to recover from stale pooled
    connections.
    """
    import aiohttp

    _logger.warning(
        "Resetting aiohttp session due to stale connection",
        extra={"url": redact_url(url)},
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


def fetch_url(
    url: str,
    config: FetchConfig,
    *,
    url_validator: Callable[[str], None] | None = None,
) -> bytes:
    """Fetch URL contents, using parallel Range requests when beneficial.

    Uses a persistent connection pool owned by *config*.  Stale
    connections (``ServerDisconnectedError``, ``ConnectionResetError``)
    trigger automatic session recreation and a single retry.

    Args:
        url: The URL to fetch.
        config: Fetch configuration controlling parallelism and limits.
        url_validator: Optional validator applied to the initial URL and every
            redirect target before issuing a request.

    Returns:
        The complete response body as bytes.

    Raises:
        RuntimeError: If the content exceeds ``max_fetch_bytes`` or the
            overall timeout is exceeded.
        aiohttp.ClientResponseError: On HTTP error responses.

    """
    import aiohttp

    t0 = time.monotonic()
    pool = _ensure_pool(config)
    if pool.loop is None or pool.session is None:  # pragma: no cover
        raise RuntimeError("FetchPool not properly initialized")
    fetch = (
        _fetch_with_probe(url, config, pool.session)
        if url_validator is None
        else _fetch_with_probe(url, config, pool.session, url_validator)
    )
    future = asyncio.run_coroutine_threadsafe(fetch, pool.loop)
    try:
        data = future.result()
    except (aiohttp.ServerDisconnectedError, ConnectionResetError):
        _reset_session(config, url=url)
        new_pool = config._pool
        if new_pool.loop is None or new_pool.session is None:  # pragma: no cover
            raise RuntimeError("FetchPool not properly initialized after reset") from None
        retry_fetch = (
            _fetch_with_probe(url, config, new_pool.session)
            if url_validator is None
            else _fetch_with_probe(url, config, new_pool.session, url_validator)
        )
        retry_future = asyncio.run_coroutine_threadsafe(retry_fetch, new_pool.loop)
        data = retry_future.result()
    duration_ms = (time.monotonic() - t0) * 1000
    _logger.debug(
        "Fetch completed: %s (%d bytes, %.1fms)",
        redact_url(url),
        len(data),
        duration_ms,
        extra={"url": redact_url(url), "size_bytes": len(data), "duration_ms": round(duration_ms, 2)},
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


async def _read_range_response_body(
    resp: aiohttp.ClientResponse,
    expected_size: int,
    config: FetchConfig,
) -> bytes:
    """Read one 206 body without buffering beyond its range or global cap."""
    chunks: list[bytes] = []
    total = 0
    while True:
        # Once either boundary is reached, read one byte only: EOF proves the
        # response is exact, while a byte is a bounded sentinel proving the
        # origin exceeded the range/global contract.
        sentinel = min(expected_size - total + 1, config.max_fetch_bytes - total + 1)
        chunk = await resp.content.read(min(65536, max(1, sentinel)))
        if not chunk:
            break
        total += len(chunk)
        if total > config.max_fetch_bytes:
            raise RuntimeError(
                f"ExternalLocation range fetch exceeded max_fetch_bytes ({total} > {config.max_fetch_bytes} bytes)"
            )
        if total > expected_size:
            raise RuntimeError(f"Range chunk size mismatch: expected {expected_size} bytes, got at least {total}")
        chunks.append(chunk)
    if total != expected_size:
        raise RuntimeError(f"Range chunk size mismatch: expected {expected_size} bytes, got {total}")
    return b"".join(chunks)


def _validate_url(url: str, validator: Callable[[str], None] | None) -> None:
    """Validate one request target while keeping validator diagnostics secret-safe."""
    if validator is None:
        return
    try:
        validator(url)
    except Exception as exc:
        # Preserve useful validator diagnostics, but suppress the original
        # exception so its args cannot survive in ``__context__``.
        message = str(exc).replace(url, redact_url(url))
        parsed = urlparse(url)
        secrets = [parsed.username, parsed.password, *(value for _, value in parse_qsl(parsed.query))]
        for secret in secrets:
            if secret:
                message = message.replace(secret, "<redacted>")
        raise ValueError(f"ExternalLocation URL rejected: {message}") from None


@asynccontextmanager
async def _request_following_redirects(
    client: aiohttp.ClientSession,
    method: str,
    url: str,
    config: FetchConfig,
    url_validator: Callable[[str], None] | None,
    *,
    headers: Mapping[str, str] | None = None,
) -> AsyncIterator[aiohttp.ClientResponse]:
    """Issue one request, manually validating and bounding every redirect."""
    current_url = url
    response: aiohttp.ClientResponse | None = None
    try:
        for redirect_count in range(config.max_redirects + 1):
            _validate_url(current_url, url_validator)
            try:
                if method == "HEAD":
                    response = await client.head(current_url, headers=headers, allow_redirects=False)
                else:
                    response = await client.get(current_url, headers=headers, allow_redirects=False)
            except (TimeoutError, ConnectionResetError):
                raise
            except Exception as exc:
                import aiohttp as _aiohttp

                if isinstance(exc, _aiohttp.ServerDisconnectedError):
                    raise
                raise _aiohttp.ClientConnectionError(
                    f"ExternalLocation {method} failed for {redact_url(current_url)}"
                ) from None

            if response.status not in _REDIRECT_STATUSES:
                yield response
                return

            location = response.headers.get("Location")
            response.release()
            response = None
            if redirect_count >= config.max_redirects:
                raise RuntimeError(
                    f"ExternalLocation redirect limit exceeded (max_redirects={config.max_redirects}) "
                    f"at {redact_url(current_url)}"
                )
            if not location:
                raise RuntimeError(f"ExternalLocation redirect had no Location header at {redact_url(current_url)}")
            next_url = urljoin(current_url, location)
            if not urlparse(next_url).scheme or not urlparse(next_url).netloc:
                raise RuntimeError(f"ExternalLocation redirect target is invalid at {redact_url(current_url)}")
            current_url = next_url
    finally:
        if response is not None:
            response.release()


def _raise_for_status_redacted(resp: aiohttp.ClientResponse, url: str) -> None:
    """Raise an aiohttp error without retaining its signed request URL."""
    if 200 <= resp.status < 300:
        return
    import aiohttp as _aiohttp
    from aiohttp.client_reqrep import RequestInfo
    from yarl import URL

    safe_url = URL(redact_url(url))
    request_info = RequestInfo(
        safe_url,
        resp.method,
        resp.request_info.headers,
        real_url=safe_url,
    )
    raise _aiohttp.ClientResponseError(
        request_info,
        (),
        status=resp.status,
        message=resp.reason or "",
        headers=resp.headers,
    ) from None


async def _fetch_with_probe(
    url: str,
    config: FetchConfig,
    client: aiohttp.ClientSession,
    url_validator: Callable[[str], None] | None = None,
) -> bytes:
    """Core async fetch: probe, then parallel Range GETs or simple GET.

    For normal URLs this issues a HEAD request first to learn
    Content-Length and Accept-Ranges without downloading the body.
    For pre-signed URLs (S3/GCS), where HEAD may be rejected by
    method-bound signatures, this issues a probe GET with
    ``Range: bytes=0-0`` instead.

    Falls back to a plain GET when probe metadata is unavailable
    (or HEAD is unsupported by the origin).

    When the delivering response names a codec in ``Content-Encoding``
    (e.g. ``zstd``), the downloaded bytes are decompressed before
    returning.  The header is read from the response that actually
    carried the body, falling back to the probe's: a probe can be
    rejected outright (a method-bound signature answering ``403`` to
    HEAD), and trusting only the probe would then silently return
    still-compressed bytes — which fail SHA-256 verification upstream.
    """
    if _is_presigned_url(url):
        content_length, accept_ranges, content_encoding = await _range_probe(url, client, config, url_validator)
    else:
        content_length, accept_ranges, content_encoding = await _head_probe(url, client, config, url_validator)

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
        data = await _fetch_chunks_with_hedging(client, url, content_length, config, url_validator)
    else:
        # Simple path: single GET
        async with _request_following_redirects(client, "GET", url, config, url_validator) as resp:
            _raise_for_status_redacted(resp, url)
            # The delivering response is authoritative for the codec: an
            # origin that rejected the probe still names its encoding here.
            content_encoding = resp.headers.get("Content-Encoding", "") or content_encoding
            data = await _read_response_body(resp, config)

    # Decompress if the server names a codec we know. The explicit decoded
    # cap defaults to 16 * max_fetch_bytes so a small, highly compressible
    # response (or a deliberately crafted bomb from a malicious / compromised
    # storage backend) cannot OOM the client the way an unbounded
    # ``decompress(data)`` would by trusting the frame header's declared
    # content_size (zstd) or footer ISIZE (gzip).
    ce = content_encoding.strip().lower()
    if ce:
        # Strip any q= weight / parameters — Content-Encoding doesn't
        # typically carry them but generic proxies sometimes do.
        ce = ce.split(";", 1)[0].strip()
    codec: _CodecEncoding | None = None
    for _enc in _CodecEncoding:
        if _enc.value == ce:
            codec = _enc
            break
    if codec is not None:
        from vgi_rpc._codec import decompress as _codec_decompress

        max_decompressed = (
            config.max_fetch_bytes * 16 if config.max_decompressed_bytes is None else config.max_decompressed_bytes
        )
        try:
            data = _codec_decompress(codec, data, max_output_size=max_decompressed)
        except Exception:
            raise RuntimeError(
                f"Failed to decompress {codec.value} data ({len(data)} encoded bytes) "
                f"without exceeding max_decompressed_bytes={max_decompressed} from {redact_url(url)}"
            ) from None

    max_decompressed = (
        config.max_fetch_bytes * 16 if config.max_decompressed_bytes is None else config.max_decompressed_bytes
    )
    if len(data) > max_decompressed:
        raise RuntimeError(
            f"ExternalLocation decoded body exceeded max_decompressed_bytes "
            f"({len(data)} > {max_decompressed}) from {redact_url(url)}"
        )

    return data


async def _head_probe(
    url: str,
    client: aiohttp.ClientSession,
    config: FetchConfig | None = None,
    url_validator: Callable[[str], None] | None = None,
) -> tuple[int | None, str, str]:
    """Issue a HEAD request and return (content_length, accept_ranges, content_encoding).

    Returns ``(None, "", "")`` if the server does not support HEAD (405, 501),
    rejects HEAD but allows GET (commonly 403 for signed URLs), or otherwise
    returns an error that should fall back to a plain GET.
    """
    import aiohttp as _aiohttp

    try:
        effective_config = config or FetchConfig()
        async with _request_following_redirects(client, "HEAD", url, effective_config, url_validator) as resp:
            if resp.status in (403, 405, 501):
                return None, "", ""
            _raise_for_status_redacted(resp, url)
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
    except _aiohttp.ClientResponseError as exc:
        if exc.status in (403, 405, 501):
            return None, "", ""
        raise


def _is_presigned_url(url: str) -> bool:
    """Return True when URL query looks like S3/GCS pre-signed auth.

    Covers SigV4 (``X-Amz-Signature``/``X-Amz-Credential``, and the GCS
    equivalents) and the older SigV2 query form
    (``AWSAccessKeyId``/``Signature``/``Expires``), which boto3 still emits
    against S3-compatible endpoints (MinIO, older gateways) depending on the
    client's signature version.  All of them are signed per HTTP method, so a
    HEAD probe would be rejected; they must take the Range-GET probe path.
    """
    query_pairs = parse_qsl(urlparse(url).query, keep_blank_values=True)
    query_keys = {k for k, _ in query_pairs}
    sigv4 = ("X-Amz-Signature" in query_keys and "X-Amz-Credential" in query_keys) or (
        "X-Goog-Signature" in query_keys and "X-Goog-Credential" in query_keys
    )
    sigv2 = "Signature" in query_keys and ("AWSAccessKeyId" in query_keys or "GoogleAccessId" in query_keys)
    return sigv4 or sigv2


def _content_length_from_content_range(content_range: str) -> int | None:
    """Parse total object size from Content-Range header."""
    # Example: "bytes 0-0/12345"
    match = re.match(r"^\s*bytes\s+\d+-\d+/(\d+)\s*$", content_range)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


async def _range_probe(
    url: str,
    client: aiohttp.ClientSession,
    config: FetchConfig,
    url_validator: Callable[[str], None] | None,
) -> tuple[int | None, str, str]:
    """Issue a tiny probe GET (Range: bytes=0-0) for pre-signed URLs."""
    import aiohttp as _aiohttp

    headers = {"Range": "bytes=0-0"}
    try:
        async with _request_following_redirects(client, "GET", url, config, url_validator, headers=headers) as resp:
            content_encoding = resp.headers.get("Content-Encoding", "")

            if resp.status == 206:
                content_range = resp.headers.get("Content-Range", "")
                content_length = _content_length_from_content_range(content_range)
                # A 206 response implies byte-range support even if the header is omitted.
                accept_ranges = resp.headers.get("Accept-Ranges", "bytes")
                # The probe requested exactly one byte.  Origins are not
                # trusted to honour that range, so retain only the expected
                # byte and read at most one extra sentinel before rejecting.
                await _read_range_response_body(resp, 1, config)
                return content_length, accept_ranges, content_encoding

            # Range unsupported/ignored by origin; caller falls back to plain GET.
            if resp.status == 200:
                return None, "", content_encoding

            # Some signed URLs are method/header constrained and may reject
            # the probe even though a normal GET is valid.
            if resp.status in (403, 405, 501):
                return None, "", ""

            _raise_for_status_redacted(resp, url)
            return None, "", content_encoding  # pragma: no cover
    except _aiohttp.ClientResponseError as exc:
        if exc.status in (403, 405, 501):
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
    config: FetchConfig,
    url_validator: Callable[[str], None] | None,
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
        async with _request_following_redirects(client, "GET", url, config, url_validator, headers=headers) as resp:
            _raise_for_status_redacted(resp, url)
            if resp.status != 206:
                raise RuntimeError(
                    f"Expected HTTP 206 for Range request, got {resp.status} (bytes={start}-{end} of {redact_url(url)})"
                )
            try:
                return await _read_range_response_body(resp, expected_size, config)
            except RuntimeError as exc:
                raise RuntimeError(f"{exc} (bytes={start}-{end} of {redact_url(url)})") from None


async def _fetch_chunks_with_hedging(
    client: aiohttp.ClientSession,
    url: str,
    content_length: int,
    config: FetchConfig,
    url_validator: Callable[[str], None] | None,
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
            data = await _fetch_one_chunk(client, url, start, end, semaphore, config, url_validator)
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
                # Clean up tracking for completed tasks.  Capture chunk_idx
                # before .result() so we can tell whether a failed task was
                # for a chunk we already have data for.
                task_start_times.pop(task, None)
                chunk_idx = task_to_chunk.pop(task, None)
                try:
                    idx, data = task.result()
                except BaseException as exc:
                    # Suppress hedge failures whose original (or earlier hedge)
                    # already succeeded — the speculative duplicate was lost,
                    # but the chunk's data is safely in `results`.
                    if chunk_idx is not None and chunk_idx in results:
                        continue
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
            # chunk whose elapsed time exceeds median * multiplier.
            # Capped by max_speculative_hedges (0 = unlimited).
            hedge_budget_exhausted = (
                config.max_speculative_hedges > 0 and len(hedged_chunks) >= config.max_speculative_hedges
            )
            if hedging_enabled and not hedge_budget_exhausted and len(completion_times) >= 2:
                sorted_times = sorted(completion_times)
                median = sorted_times[len(sorted_times) // 2]
                threshold = median * config.speculative_retry_multiplier
                now = time.monotonic()

                for task in list(pending):
                    if config.max_speculative_hedges > 0 and len(hedged_chunks) >= config.max_speculative_hedges:
                        break
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
