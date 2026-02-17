"""Tests for parallel range-request fetching in external_fetch.py."""

from __future__ import annotations

import asyncio
import re
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
import zstandard
from aioresponses import CallbackResult, aioresponses

from vgi_rpc.external_fetch import FetchConfig, _compute_ranges, _ensure_pool, _head_probe, _reset_session, fetch_url

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _register_head(mock: aioresponses, url: str, data: bytes, *, accept_ranges: bool = True) -> None:
    """Register a HEAD handler that returns Content-Length and optionally Accept-Ranges."""
    head_headers: dict[str, str] = {"Content-Length": str(len(data))}
    if accept_ranges:
        head_headers["Accept-Ranges"] = "bytes"
    mock.head(url, headers=head_headers)


def _register_range_url(mock: aioresponses, url: str, data: bytes, *, accept_ranges: bool = True) -> None:
    """Register HEAD + GET (simple and Range) handlers for a URL.

    The HEAD response includes ``Content-Length`` and optionally
    ``Accept-Ranges`` headers for the probe.  GET handlers serve the
    full body or individual Range chunks.

    Args:
        mock: The aioresponses mock instance.
        url: The URL to register.
        data: The complete data the URL serves.
        accept_ranges: Whether to advertise Range support.

    """
    _register_head(mock, url, data, accept_ranges=accept_ranges)

    base_headers: dict[str, str] = {"Content-Length": str(len(data))}
    if accept_ranges:
        base_headers["Accept-Ranges"] = "bytes"

    def _range_callback(url_: Any, **kwargs: Any) -> CallbackResult:
        headers = kwargs.get("headers", {})
        range_header = headers.get("Range", "")
        if range_header:
            match = re.match(r"bytes=(\d+)-(\d+)", range_header)
            if match:
                start, end = int(match.group(1)), int(match.group(2))
                chunk = data[start : end + 1]
                return CallbackResult(
                    status=206,
                    body=chunk,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{len(data)}",
                        "Content-Length": str(len(chunk)),
                    },
                )
        return CallbackResult(status=200, body=data, headers=base_headers)

    # Register enough GET responses for parallel chunks + potential hedges
    for _ in range(50):
        mock.get(url, callback=_range_callback)


# ===========================================================================
# Unit tests — _compute_ranges
# ===========================================================================


class TestComputeRanges:
    """Tests for _compute_ranges helper."""

    def test_exact_division(self) -> None:
        """Content length evenly divisible by chunk size."""
        ranges = _compute_ranges(100, 50)
        assert ranges == [(0, 49), (50, 99)]

    def test_remainder(self) -> None:
        """Content length not evenly divisible."""
        ranges = _compute_ranges(110, 50)
        assert ranges == [(0, 49), (50, 99), (100, 109)]

    def test_single_chunk(self) -> None:
        """Content smaller than chunk size → single range."""
        ranges = _compute_ranges(30, 50)
        assert ranges == [(0, 29)]

    def test_exact_one_byte(self) -> None:
        """Single-byte content."""
        ranges = _compute_ranges(1, 1)
        assert ranges == [(0, 0)]


# ===========================================================================
# Unit tests — fetch_url
# ===========================================================================


class TestFetchSimpleBelowThreshold:
    """Tests for small fetches below the parallel threshold."""

    def test_small_below_threshold(self) -> None:
        """Small content uses single GET, not Range requests."""
        data = b"hello world"
        url = "https://example.com/small"
        with FetchConfig(parallel_threshold_bytes=1024) as config:
            with aioresponses() as mock:
                _register_head(mock, url, data)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})

                result = fetch_url(url, config)

            assert result == data

    def test_no_accept_ranges_fallback(self) -> None:
        """Server omits Accept-Ranges header → single GET."""
        data = b"x" * 100
        url = "https://example.com/no-range"
        with FetchConfig(parallel_threshold_bytes=50) as config:
            with aioresponses() as mock:
                _register_head(mock, url, data, accept_ranges=False)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})

                result = fetch_url(url, config)

            assert result == data

    def test_no_content_length_fallback(self) -> None:
        """No Content-Length header from HEAD → single GET."""
        data = b"streaming data"
        url = "https://example.com/no-cl"
        with FetchConfig(parallel_threshold_bytes=10) as config:
            with aioresponses() as mock:
                # HEAD returns no Content-Length
                mock.head(url, headers={"Accept-Ranges": "bytes"})
                mock.get(url, body=data)

                result = fetch_url(url, config)

            assert result == data


class TestHeadProbeFallback:
    """Tests for HEAD probe fallback when server doesn't support HEAD."""

    def test_head_405_falls_back_to_get(self) -> None:
        """Server returning 405 for HEAD falls back to a single GET."""
        data = b"head not allowed"
        url = "https://example.com/no-head"
        with FetchConfig(parallel_threshold_bytes=10000) as config:
            with aioresponses() as mock:
                mock.head(url, status=405)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})

                result = fetch_url(url, config)

            assert result == data

    def test_head_501_falls_back_to_get(self) -> None:
        """Server returning 501 for HEAD falls back to a single GET."""
        data = b"head not implemented"
        url = "https://example.com/no-head-501"
        with FetchConfig(parallel_threshold_bytes=10000) as config:
            with aioresponses() as mock:
                mock.head(url, status=501)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})

                result = fetch_url(url, config)

            assert result == data

    def test_head_success_enables_parallel(self) -> None:
        """HEAD succeeds with Range support → parallel path used."""
        data = bytes(range(256)) * 8  # 2048 bytes
        url = "https://example.com/head-ok"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=512,
            max_parallel_requests=4,
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data


class TestFetchParallelChunks:
    """Tests for parallel range-request fetching."""

    def test_large_parallel_chunks(self) -> None:
        """Above threshold, data is fetched via parallel Range requests."""
        data = bytes(range(256)) * 40  # 10240 bytes
        url = "https://example.com/large"
        with FetchConfig(
            parallel_threshold_bytes=1000,
            chunk_size_bytes=2048,
            max_parallel_requests=4,
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data

    def test_chunk_reassembly_order(self) -> None:
        """Chunks reassembled in correct order regardless of arrival order."""
        # Create data with distinct per-chunk content
        chunk_size = 100
        num_chunks = 5
        data = b""
        for i in range(num_chunks):
            data += bytes([i]) * chunk_size
        url = "https://example.com/order"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=chunk_size,
            max_parallel_requests=8,
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data
            # Verify each chunk has the right content
            for i in range(num_chunks):
                chunk = result[i * chunk_size : (i + 1) * chunk_size]
                assert chunk == bytes([i]) * chunk_size


class TestFetchMaxBytes:
    """Tests for max_fetch_bytes enforcement."""

    def test_max_fetch_bytes_rejected_at_probe(self) -> None:
        """Content-Length exceeds limit → RuntimeError before download."""
        url = "https://example.com/huge"
        with FetchConfig(max_fetch_bytes=100) as config, aioresponses() as mock:
            mock.head(
                url,
                headers={"Content-Length": "99999", "Accept-Ranges": "bytes"},
            )

            with pytest.raises(RuntimeError, match="exceeds max_fetch_bytes"):
                fetch_url(url, config)

    def test_oversized_chunk_rejected(self) -> None:
        """Server returning oversized range chunks is caught at chunk level."""
        url = "https://example.com/lying-server"
        # Advertised content-length is 200 (below max_fetch_bytes=300)
        # but each range chunk returns 200 bytes instead of 100.
        with FetchConfig(
            max_fetch_bytes=300,
            parallel_threshold_bytes=100,
            chunk_size_bytes=100,
            max_parallel_requests=8,
        ) as config:

            def _lying_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                headers = kwargs.get("headers", {})
                range_header = headers.get("Range", "")
                if range_header:
                    # Return 200 bytes per chunk regardless of what was asked
                    oversized_chunk = b"x" * 200
                    return CallbackResult(
                        status=206,
                        body=oversized_chunk,
                        headers={
                            "Content-Length": str(len(oversized_chunk)),
                        },
                    )
                return CallbackResult(status=200, body=b"x" * 200)

            with aioresponses() as mock:
                mock.head(url, headers={"Content-Length": "200", "Accept-Ranges": "bytes"})
                for _ in range(50):
                    mock.get(url, callback=_lying_callback)

                with pytest.raises(RuntimeError, match="Range chunk size mismatch"):
                    fetch_url(url, config)


class TestFetchChunkValidation:
    """Tests for Range chunk validation in _fetch_one_chunk."""

    def test_non_206_status_rejected(self) -> None:
        """Server returning 200 instead of 206 for Range request raises RuntimeError."""
        url = "https://example.com/no-range-support"
        data = b"x" * 500
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=250,
            max_parallel_requests=4,
        ) as config:

            def _ignore_range_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                # Server ignores Range and returns full body with 200
                return CallbackResult(status=200, body=data, headers={"Content-Length": str(len(data))})

            with aioresponses() as mock:
                _register_head(mock, url, data)
                for _ in range(10):
                    mock.get(url, callback=_ignore_range_callback)

                with pytest.raises(RuntimeError, match="Expected HTTP 206"):
                    fetch_url(url, config)


class TestFetchHedging:
    """Tests for speculative retry (hedging) behavior."""

    def test_hedging_disabled(self) -> None:
        """multiplier=0 disables hedging — still works correctly."""
        data = b"x" * 1000
        url = "https://example.com/no-hedge"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=500,
            max_parallel_requests=4,
            speculative_retry_multiplier=0,
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data

    def test_multiple_chunks_hedged(self) -> None:
        """Multiple slow chunks can be hedged; data integrity preserved."""
        data = b"y" * 2000
        url = "https://example.com/multi-hedge"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=500,
            max_parallel_requests=8,
            speculative_retry_multiplier=1.0,  # aggressive — hedge anything above median
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data

    def test_slow_chunks_trigger_hedging(self) -> None:
        """Slow responses trigger time-based hedging; result is correct.

        Uses ``repeat=True`` on a single mock to avoid an aioresponses
        ``KeyError`` race where two concurrent async callbacks (original +
        hedge) try to delete the same non-repeating mock entry.  The sleep
        is generous (0.5 s) so that even on Windows — where asyncio timer
        resolution is ~15 ms — the slow chunk clearly exceeds the
        median-based hedging threshold.
        """
        chunk_size = 500
        data = b"z" * 2000  # 4 chunks
        url = "https://example.com/slow-hedge"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=chunk_size,
            max_parallel_requests=8,
            speculative_retry_multiplier=1.5,
        ) as config:
            first_attempt_for_chunk: dict[int, bool] = {}

            async def _slow_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                headers = kwargs.get("headers", {})
                range_header = headers.get("Range", "")
                if range_header:
                    match = re.match(r"bytes=(\d+)-(\d+)", range_header)
                    if match:
                        start, end = int(match.group(1)), int(match.group(2))
                        chunk_idx = start // chunk_size
                        is_first = first_attempt_for_chunk.get(chunk_idx) is None
                        if is_first:
                            first_attempt_for_chunk[chunk_idx] = True
                        # Slow the first attempt for chunk 2 so it gets hedged
                        if chunk_idx == 2 and is_first:
                            await asyncio.sleep(0.5)
                        chunk = data[start : end + 1]
                        return CallbackResult(
                            status=206,
                            body=chunk,
                            headers={
                                "Content-Range": f"bytes {start}-{end}/{len(data)}",
                                "Content-Length": str(len(chunk)),
                            },
                        )
                return CallbackResult(status=200, body=data)

            with aioresponses() as mock:
                _register_head(mock, url, data)
                mock.get(url, callback=_slow_callback, repeat=True)
                result = fetch_url(url, config)

            assert result == data

    def test_hedge_cap_limits_extra_requests(self) -> None:
        """max_speculative_hedges caps the number of hedge requests launched."""
        chunk_size = 500
        data = b"h" * 4000  # 8 chunks — enough that many could be hedged
        url = "https://example.com/hedge-cap"
        hedge_count = 0

        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=chunk_size,
            max_parallel_requests=16,
            speculative_retry_multiplier=1.0,  # aggressive — hedge above median
            max_speculative_hedges=2,
        ) as config:
            first_attempt_for_chunk: dict[int, bool] = {}

            async def _slow_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                nonlocal hedge_count
                headers = kwargs.get("headers", {})
                range_header = headers.get("Range", "")
                if range_header:
                    match = re.match(r"bytes=(\d+)-(\d+)", range_header)
                    if match:
                        start, end = int(match.group(1)), int(match.group(2))
                        chunk_idx = start // chunk_size
                        is_first = first_attempt_for_chunk.get(chunk_idx) is None
                        if is_first:
                            first_attempt_for_chunk[chunk_idx] = True
                        else:
                            hedge_count += 1
                        # Slow down chunks 2-5 so they all exceed the threshold
                        if chunk_idx in (2, 3, 4, 5) and is_first:
                            await asyncio.sleep(0.5)
                        chunk = data[start : end + 1]
                        return CallbackResult(
                            status=206,
                            body=chunk,
                            headers={
                                "Content-Range": f"bytes {start}-{end}/{len(data)}",
                                "Content-Length": str(len(chunk)),
                            },
                        )
                return CallbackResult(status=200, body=data)

            with aioresponses() as mock:
                _register_head(mock, url, data)
                mock.get(url, callback=_slow_callback, repeat=True)
                result = fetch_url(url, config)

            assert result == data
            # 4 chunks are slow enough to trigger hedging, but only 2 allowed
            assert hedge_count <= 2

    def test_hedge_cap_zero_means_unlimited(self) -> None:
        """max_speculative_hedges=0 disables the cap (backward-compatible)."""
        data = b"u" * 2000  # 4 chunks
        url = "https://example.com/hedge-unlimited"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=500,
            max_parallel_requests=8,
            speculative_retry_multiplier=1.0,
            max_speculative_hedges=0,
        ) as config:
            with aioresponses() as mock:
                _register_range_url(mock, url, data)
                result = fetch_url(url, config)

            assert result == data


class TestFetchErrors:
    """Tests for error propagation."""

    def test_server_error_propagates(self) -> None:
        """500 on HEAD raises aiohttp.ClientResponseError."""
        url = "https://example.com/error"
        with FetchConfig(parallel_threshold_bytes=10000) as config, aioresponses() as mock:
            mock.head(url, status=500)

            with pytest.raises(aiohttp.ClientResponseError):
                fetch_url(url, config)

    def test_overall_timeout(self) -> None:
        """Overall deadline exceeded raises."""
        url = "https://example.com/slow"
        with FetchConfig(timeout_seconds=0.1, parallel_threshold_bytes=10000) as config, aioresponses() as mock:
            mock.head(url, exception=TimeoutError())

            with pytest.raises((TimeoutError, aiohttp.ClientError)):
                fetch_url(url, config)

    def test_simple_path_exceeds_max_fetch_bytes(self) -> None:
        """Body larger than max_fetch_bytes on simple path raises RuntimeError."""
        url = "https://example.com/big-simple"
        data = b"x" * 200
        # HEAD returns no Content-Length so the header guard doesn't trigger — forces
        # the streaming _read_response_body path to catch the oversize body.
        with FetchConfig(max_fetch_bytes=100, parallel_threshold_bytes=10000) as config, aioresponses() as mock:
            mock.head(url, headers={})
            mock.get(url, body=data)

            with pytest.raises(RuntimeError, match="exceeded max_fetch_bytes"):
                fetch_url(url, config)

    def test_invalid_content_length_header(self) -> None:
        """Non-numeric Content-Length from HEAD falls back to simple GET."""
        data = b"some data"
        url = "https://example.com/bad-cl"
        with FetchConfig(parallel_threshold_bytes=5) as config:
            with aioresponses() as mock:
                mock.head(url, headers={"Content-Length": "not-a-number"})
                mock.get(url, body=data)

                result = fetch_url(url, config)

            assert result == data

    def test_chunk_error_cancels_pending(self) -> None:
        """Error in a Range chunk cancels remaining tasks and propagates."""
        url = "https://example.com/chunk-fail"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=100,
            max_parallel_requests=8,
        ) as config:
            call_count = 0

            def _fail_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                nonlocal call_count
                headers = kwargs.get("headers", {})
                range_header = headers.get("Range", "")
                if range_header:
                    call_count += 1
                    if call_count == 2:
                        # Second range chunk raises a transport error
                        raise aiohttp.ServerConnectionError("connection reset")
                    chunk = b"x" * 100
                    return CallbackResult(
                        status=206,
                        body=chunk,
                        headers={"Content-Length": "100"},
                    )
                return CallbackResult(status=200, body=b"")

            with aioresponses() as mock:
                mock.head(url, headers={"Content-Length": "300", "Accept-Ranges": "bytes"})
                for _ in range(50):
                    mock.get(url, callback=_fail_callback)

                with pytest.raises(aiohttp.ClientError):
                    fetch_url(url, config)


class TestNestedEventLoop:
    """Tests for fetch_url called from within a running event loop."""

    def test_nested_event_loop(self) -> None:
        """Verify run_coroutine_threadsafe handles nested loops."""
        data = b"nested data"
        url = "https://example.com/nested"
        with FetchConfig(parallel_threshold_bytes=10000) as config:

            async def run_in_loop() -> bytes:
                with aioresponses() as mock:
                    _register_head(mock, url, data, accept_ranges=False)
                    mock.get(url, body=data, headers={"Content-Length": str(len(data))})
                    return fetch_url(url, config)

            result = asyncio.run(run_in_loop())
            assert result == data


# ===========================================================================
# Connection pool tests
# ===========================================================================


class TestFetchPool:
    """Tests for persistent connection pooling."""

    def test_session_reused_across_calls(self) -> None:
        """Two fetch_url calls on the same config reuse the same pool."""
        data = b"reuse me"
        url = "https://example.com/pool-reuse"
        with FetchConfig(parallel_threshold_bytes=10000) as config:
            with aioresponses() as mock:
                _register_head(mock, url, data, accept_ranges=False)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})
                _register_head(mock, url, data, accept_ranges=False)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})

                result1 = fetch_url(url, config)
                pool_after_first = config._pool
                session_after_first = pool_after_first.session
                loop_after_first = pool_after_first.loop

                result2 = fetch_url(url, config)
                pool_after_second = config._pool
                session_after_second = pool_after_second.session
                loop_after_second = pool_after_second.loop

            assert result1 == data
            assert result2 == data
            # Same session and loop objects
            assert session_after_first is session_after_second
            assert loop_after_first is loop_after_second

    def test_close_cleans_up(self) -> None:
        """After close(), pool state is reset to None."""
        config = FetchConfig()
        # Force pool initialization
        pool = _ensure_pool(config)
        assert pool.session is not None
        assert pool.loop is not None
        assert pool.thread is not None

        config.close()
        assert pool.session is None
        assert pool.loop is None
        assert pool.thread is None

    def test_context_manager(self) -> None:
        """Context manager calls close() on exit."""
        with FetchConfig() as config:
            pool = _ensure_pool(config)
            assert pool.session is not None

        # After exiting context, pool is cleaned up
        assert config._pool.session is None
        assert config._pool.loop is None
        assert config._pool.thread is None

    def test_pool_recovers_after_close(self) -> None:
        """Pool auto-recovers when used after close()."""
        data = b"recovered"
        url = "https://example.com/recover"
        config = FetchConfig(parallel_threshold_bytes=10000)
        try:
            # Initialize and close
            _ensure_pool(config)
            config.close()
            assert config._pool.session is None

            # Use again — should re-initialize
            with aioresponses() as mock:
                _register_head(mock, url, data, accept_ranges=False)
                mock.get(url, body=data, headers={"Content-Length": str(len(data))})
                result = fetch_url(url, config)

            assert result == data
            assert config._pool.session is not None
        finally:
            config.close()

    def test_reset_session_on_closed_pool(self) -> None:
        """_reset_session is a no-op when the pool is already closed."""
        config = FetchConfig()
        _ensure_pool(config)
        config.close()
        assert config._pool.loop is None

        # Should not raise — just returns early
        _reset_session(config)
        assert config._pool.session is None

    def test_reset_session_without_existing_session(self) -> None:
        """_reset_session creates a new session even if the old one is already None."""
        config = FetchConfig()
        try:
            pool = _ensure_pool(config)
            assert pool.session is not None

            # Manually clear session to simulate the None branch
            pool.session = None
            _reset_session(config)
            assert config._pool.session is not None
        finally:
            config.close()

    def test_reconnects_on_server_disconnected(self) -> None:
        """ServerDisconnectedError triggers session recreation and retry."""
        data = b"retry success"
        url = "https://example.com/disconnect"
        with FetchConfig(parallel_threshold_bytes=10000) as config:
            # Force pool init
            pool = _ensure_pool(config)
            old_session = pool.session

            # Patch _fetch_with_probe: first call raises ServerDisconnectedError,
            # second call succeeds
            call_count = 0
            original_fetch = AsyncMock()

            async def _side_effect(url_arg: str, config_arg: FetchConfig, client: aiohttp.ClientSession) -> bytes:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise aiohttp.ServerDisconnectedError()
                return data

            original_fetch.side_effect = _side_effect

            with patch("vgi_rpc.external_fetch._fetch_with_probe", original_fetch):
                result = fetch_url(url, config)

            assert result == data
            assert call_count == 2
            # Session was recreated
            assert config._pool.session is not old_session


# ===========================================================================
# Fetch decompression tests
# ===========================================================================


class TestFetchDecompression:
    """Tests for zstd decompression in fetch_url via Content-Encoding header."""

    def test_fetch_zstd_simple_path(self) -> None:
        """Single GET with Content-Encoding: zstd → returns decompressed bytes."""
        raw_data = b"hello world uncompressed payload"
        compressed = zstandard.ZstdCompressor().compress(raw_data)
        url = "https://example.com/zstd-simple"
        with FetchConfig(parallel_threshold_bytes=10_000_000) as config:
            with aioresponses() as mock:
                mock.head(
                    url,
                    headers={
                        "Content-Length": str(len(compressed)),
                        "Content-Encoding": "zstd",
                    },
                )
                mock.get(
                    url,
                    body=compressed,
                    headers={
                        "Content-Length": str(len(compressed)),
                        "Content-Encoding": "zstd",
                    },
                )

                result = fetch_url(url, config)

            assert result == raw_data

    def test_fetch_zstd_parallel_path(self) -> None:
        """Range requests on compressed blob → reassemble → decompress."""
        raw_data = bytes(range(256)) * 40  # 10240 bytes uncompressed
        compressed = zstandard.ZstdCompressor().compress(raw_data)
        url = "https://example.com/zstd-parallel"
        with FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=512,
            max_parallel_requests=4,
        ) as config:
            with aioresponses() as mock:
                mock.head(
                    url,
                    headers={
                        "Content-Length": str(len(compressed)),
                        "Accept-Ranges": "bytes",
                        "Content-Encoding": "zstd",
                    },
                )

                def _range_callback(url_: Any, **kwargs: Any) -> CallbackResult:
                    headers = kwargs.get("headers", {})
                    range_header = headers.get("Range", "")
                    if range_header:
                        match = re.match(r"bytes=(\d+)-(\d+)", range_header)
                        if match:
                            start, end = int(match.group(1)), int(match.group(2))
                            chunk = compressed[start : end + 1]
                            return CallbackResult(
                                status=206,
                                body=chunk,
                                headers={
                                    "Content-Range": f"bytes {start}-{end}/{len(compressed)}",
                                    "Content-Length": str(len(chunk)),
                                },
                            )
                    return CallbackResult(status=200, body=compressed)

                for _ in range(50):
                    mock.get(url, callback=_range_callback)

                result = fetch_url(url, config)

            assert result == raw_data

    def test_fetch_no_encoding_passthrough(self) -> None:
        """No Content-Encoding header → raw bytes returned unchanged."""
        data = b"raw bytes no compression"
        url = "https://example.com/no-encoding"
        with FetchConfig(parallel_threshold_bytes=10_000_000) as config:
            with aioresponses() as mock:
                mock.head(
                    url,
                    headers={"Content-Length": str(len(data))},
                )
                mock.get(
                    url,
                    body=data,
                    headers={"Content-Length": str(len(data))},
                )

                result = fetch_url(url, config)

            assert result == data

    def test_head_probe_returns_content_encoding(self) -> None:
        """Verify _head_probe extracts Content-Encoding."""
        url = "https://example.com/probe-encoding"
        with FetchConfig() as config:
            pool = _ensure_pool(config)
            assert pool.loop is not None
            assert pool.session is not None

            with aioresponses() as mock:
                mock.head(
                    url,
                    headers={
                        "Content-Length": "1000",
                        "Accept-Ranges": "bytes",
                        "Content-Encoding": "zstd",
                    },
                )

                import asyncio

                future = asyncio.run_coroutine_threadsafe(
                    _head_probe(url, pool.session),
                    pool.loop,
                )
                content_length, accept_ranges, content_encoding = future.result(timeout=5)

            assert content_length == 1000
            assert accept_ranges == "bytes"
            assert content_encoding == "zstd"

    def test_corrupt_zstd_raises_runtime_error(self) -> None:
        """Corrupted zstd data with Content-Encoding: zstd raises RuntimeError."""
        corrupt_data = b"this is not valid zstd data"
        url = "https://example.com/corrupt-zstd"
        with FetchConfig(parallel_threshold_bytes=10_000_000) as config, aioresponses() as mock:
            mock.head(
                url,
                headers={
                    "Content-Length": str(len(corrupt_data)),
                    "Content-Encoding": "zstd",
                },
            )
            mock.get(
                url,
                body=corrupt_data,
                headers={
                    "Content-Length": str(len(corrupt_data)),
                    "Content-Encoding": "zstd",
                },
            )

            with pytest.raises(RuntimeError, match="Failed to decompress zstd data"):
                fetch_url(url, config)
