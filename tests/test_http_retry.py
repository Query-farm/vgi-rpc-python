"""Tests for HTTP retry logic (HttpRetryConfig, HttpTransientError, retry helpers).

Tests use ``_TransientFailureClient`` wrapper and ``_sleep=lambda _: None`` or
``backoff_base=0.001`` to stay fast.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from io import BytesIO

import httpx
import pyarrow as pa
import pytest

from vgi_rpc.http import (
    _SyncTestClient,
    _SyncTestResponse,
    http_capabilities,
    http_connect,
    http_introspect,
    make_sync_client,
    request_upload_urls,
)
from vgi_rpc.http._retry import (
    HttpRetryConfig,
    HttpTransientError,
    _compute_delay,
    _get_retry_after,
    _options_with_retry,
    _parse_retry_after,
    _post_with_retry,
)
from vgi_rpc.rpc import AnnotatedBatch, RpcError, RpcServer, _send_request, rpc_methods

from .test_rpc import (
    RpcFixtureService,
    RpcFixtureServiceImpl,
)

# ---------------------------------------------------------------------------
# Test infrastructure: transient-failure wrapper around _SyncTestClient
# ---------------------------------------------------------------------------


class _TransientFailureClient:
    """Wraps a ``_SyncTestClient``, injecting transient failure responses.

    The first ``failures`` requests return a configurable HTTP error status.
    Subsequent requests delegate to the real client.
    """

    def __init__(
        self,
        real: _SyncTestClient,
        *,
        failure_status: int = 502,
        failures: int = 1,
        retry_after: str | None = None,
    ) -> None:
        self._real = real
        self._failure_status = failure_status
        self._failures_remaining = failures
        self._retry_after = retry_after
        self.call_count = 0

    def _maybe_fail(self) -> _SyncTestResponse | None:
        self.call_count += 1
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            headers: dict[str, str] = {}
            if self._retry_after is not None:
                headers["Retry-After"] = self._retry_after
            return _SyncTestResponse(
                self._failure_status,
                b"<html>Bad Gateway</html>",
                headers=headers,
            )
        return None

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Intercept POST requests, optionally returning a failure."""
        fail = self._maybe_fail()
        if fail is not None:
            return fail
        return self._real.post(url, content=content, headers=headers)

    def options(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Intercept OPTIONS requests, optionally returning a failure."""
        fail = self._maybe_fail()
        if fail is not None:
            return fail
        return self._real.options(url, headers=headers)

    def close(self) -> None:
        """Close the real client."""
        self._real.close()


class _ConnectionErrorClient:
    """Raises ``httpx.ConnectError`` for the first N calls, then delegates."""

    def __init__(self, real: _SyncTestClient, *, failures: int = 1) -> None:
        self._real = real
        self._failures_remaining = failures
        self.call_count = 0

    def _maybe_fail(self) -> None:
        self.call_count += 1
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            raise httpx.ConnectError("Connection refused")

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Intercept POST requests, optionally raising ConnectError."""
        self._maybe_fail()
        return self._real.post(url, content=content, headers=headers)

    def options(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Intercept OPTIONS requests, optionally raising ConnectError."""
        self._maybe_fail()
        return self._real.options(url, headers=headers)

    def close(self) -> None:
        """Close the real client."""
        self._real.close()


class _TimeoutErrorClient:
    """Raises ``httpx.ReadTimeout`` for the first N calls, then delegates."""

    def __init__(self, real: _SyncTestClient, *, failures: int = 1) -> None:
        self._real = real
        self._failures_remaining = failures
        self.call_count = 0

    def _maybe_fail(self) -> None:
        self.call_count += 1
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            raise httpx.ReadTimeout("Read timed out")

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Intercept POST requests, optionally raising ReadTimeout."""
        self._maybe_fail()
        return self._real.post(url, content=content, headers=headers)

    def close(self) -> None:
        """Close the real client."""
        self._real.close()


def _make_add_request() -> bytes:
    """Build a valid IPC request for the ``add(a=1.0, b=2.0)`` method."""
    methods = rpc_methods(RpcFixtureService)
    req_buf = BytesIO()
    _send_request(req_buf, methods["add"], {"a": 1.0, "b": 2.0})
    return req_buf.getvalue()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def real_client() -> Iterator[_SyncTestClient]:
    """Create a sync Falcon test client."""
    c = make_sync_client(
        RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True),
        signing_key=b"test-key",
    )
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Unit tests: HttpRetryConfig
# ---------------------------------------------------------------------------


class TestHttpRetryConfig:
    """Tests for HttpRetryConfig dataclass."""

    def test_defaults(self) -> None:
        """Default config has sensible values."""
        cfg = HttpRetryConfig()
        assert cfg.max_retries == 3
        assert cfg.backoff_base == 0.5
        assert cfg.backoff_max == 30.0
        assert cfg.retryable_status_codes == frozenset({429, 502, 503, 504})
        assert cfg.retry_on_connection_error is True
        assert cfg.respect_retry_after is True

    def test_frozen(self) -> None:
        """Config is immutable."""
        cfg = HttpRetryConfig()
        with pytest.raises(AttributeError):
            cfg.max_retries = 5  # type: ignore[misc]

    def test_custom_status_codes(self) -> None:
        """Can override retryable status codes."""
        cfg = HttpRetryConfig(retryable_status_codes=frozenset({500, 503}))
        assert cfg.retryable_status_codes == frozenset({500, 503})

    def test_negative_max_retries_rejected(self) -> None:
        """max_retries < 0 raises ValueError at construction."""
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            HttpRetryConfig(max_retries=-1)

    def test_negative_backoff_base_rejected(self) -> None:
        """backoff_base < 0 raises ValueError at construction."""
        with pytest.raises(ValueError, match="backoff_base must be >= 0"):
            HttpRetryConfig(backoff_base=-0.5)

    def test_negative_backoff_max_rejected(self) -> None:
        """backoff_max < 0 raises ValueError at construction."""
        with pytest.raises(ValueError, match="backoff_max must be >= 0"):
            HttpRetryConfig(backoff_max=-1.0)

    def test_zero_values_accepted(self) -> None:
        """Zero is a valid value for all numeric fields."""
        cfg = HttpRetryConfig(max_retries=0, backoff_base=0.0, backoff_max=0.0)
        assert cfg.max_retries == 0
        assert cfg.backoff_base == 0.0
        assert cfg.backoff_max == 0.0


# ---------------------------------------------------------------------------
# Unit tests: HttpTransientError
# ---------------------------------------------------------------------------


class TestHttpTransientError:
    """Tests for HttpTransientError exception."""

    def test_is_rpc_error_subclass(self) -> None:
        """HttpTransientError is caught by except RpcError."""
        err = HttpTransientError(502, "Bad Gateway")
        assert isinstance(err, RpcError)

    def test_attributes(self) -> None:
        """Status code and retry_after are preserved."""
        err = HttpTransientError(429, "Too Many", retry_after=5.0)
        assert err.status_code == 429
        assert err.retry_after == 5.0
        assert err.error_type == "HttpTransientError"
        assert "429" in str(err)

    def test_no_retry_after(self) -> None:
        """retry_after defaults to None."""
        err = HttpTransientError(503, "Service Unavailable")
        assert err.retry_after is None


# ---------------------------------------------------------------------------
# Unit tests: _parse_retry_after
# ---------------------------------------------------------------------------


class TestParseRetryAfter:
    """Tests for Retry-After header parsing."""

    def test_delta_seconds_int(self) -> None:
        """Parse integer delta-seconds."""
        assert _parse_retry_after("120") == 120.0

    def test_delta_seconds_float(self) -> None:
        """Parse float delta-seconds."""
        assert _parse_retry_after("1.5") == 1.5

    def test_http_date(self) -> None:
        """Parse HTTP-date format (RFC 9110)."""
        from email.utils import format_datetime

        # Create a date far in the future to guarantee a positive result
        future = datetime(2099, 1, 1, 0, 0, 30, tzinfo=UTC)
        header = format_datetime(future, usegmt=True)
        result = _parse_retry_after(header)
        assert result is not None
        assert result > 0

    def test_http_date_in_past_returns_zero(self) -> None:
        """HTTP-date in the past returns 0."""
        result = _parse_retry_after("Mon, 01 Jan 2001 00:00:00 GMT")
        assert result == 0.0

    def test_unparseable(self) -> None:
        """Unparseable values return None."""
        assert _parse_retry_after("not-a-number-or-date") is None


# ---------------------------------------------------------------------------
# Unit tests: _get_retry_after
# ---------------------------------------------------------------------------


class TestGetRetryAfter:
    """Tests for _get_retry_after header extraction."""

    def test_canonical_casing(self) -> None:
        """Extracts Retry-After with canonical casing."""
        assert _get_retry_after({"Retry-After": "5"}) == 5.0

    def test_lowercase_casing(self) -> None:
        """Extracts retry-after with lowercase casing."""
        assert _get_retry_after({"retry-after": "10"}) == 10.0

    def test_uppercase_casing(self) -> None:
        """Extracts RETRY-AFTER with uppercase casing."""
        assert _get_retry_after({"RETRY-AFTER": "15"}) == 15.0

    def test_missing_header(self) -> None:
        """Returns None when no Retry-After header is present."""
        assert _get_retry_after({"Content-Type": "text/html"}) is None

    def test_empty_headers(self) -> None:
        """Returns None for empty headers."""
        assert _get_retry_after({}) is None


# ---------------------------------------------------------------------------
# Unit tests: _compute_delay
# ---------------------------------------------------------------------------


class TestComputeDelay:
    """Tests for backoff delay computation."""

    def test_exponential_progression(self) -> None:
        """Delay upper bound grows exponentially."""
        cfg = HttpRetryConfig(backoff_base=1.0, backoff_max=100.0)
        for attempt in range(4):
            upper = cfg.backoff_base * (2**attempt)
            for _ in range(20):
                d = _compute_delay(attempt, cfg, None)
                assert 0 <= d <= upper + 0.001

    def test_clamped_to_max(self) -> None:
        """Delay never exceeds backoff_max."""
        cfg = HttpRetryConfig(backoff_base=100.0, backoff_max=2.0)
        for _ in range(50):
            assert _compute_delay(5, cfg, None) <= 2.0 + 0.001

    def test_retry_after_honored(self) -> None:
        """When retry_after > computed delay, retry_after is used."""
        cfg = HttpRetryConfig(backoff_base=0.001, backoff_max=100.0)
        for _ in range(20):
            d = _compute_delay(0, cfg, 10.0)
            assert d >= 10.0

    def test_retry_after_clamped(self) -> None:
        """retry_after is clamped to backoff_max."""
        cfg = HttpRetryConfig(backoff_base=0.001, backoff_max=5.0)
        d = _compute_delay(0, cfg, 100.0)
        assert d <= 5.0 + 0.001

    def test_respect_retry_after_false(self) -> None:
        """When respect_retry_after=False, retry_after is ignored."""
        cfg = HttpRetryConfig(backoff_base=0.001, backoff_max=100.0, respect_retry_after=False)
        for _ in range(50):
            d = _compute_delay(0, cfg, 999.0)
            # With backoff_base=0.001, max jittered delay is 0.001
            assert d <= 0.001 + 0.001

    def test_always_non_negative(self) -> None:
        """Delay is always >= 0."""
        cfg = HttpRetryConfig(backoff_base=0.0, backoff_max=0.0)
        for attempt in range(5):
            assert _compute_delay(attempt, cfg, None) >= 0


# ---------------------------------------------------------------------------
# Unit tests: _post_with_retry / _options_with_retry
# ---------------------------------------------------------------------------


class TestPostWithRetry:
    """Tests for _post_with_retry."""

    def test_no_config_no_retry(self, real_client: _SyncTestClient) -> None:
        """When config is None, behaves like plain client.post."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=0)
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=b"",
            headers={"Content-Type": "application/octet-stream"},
            config=None,
        )
        assert resp.status_code is not None

    def test_retries_on_502(self, real_client: _SyncTestClient) -> None:
        """Retries on 502 and succeeds after transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=2)
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=_make_add_request(),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 200
        assert wrapper.call_count == 3  # 2 failures + 1 success

    def test_exhausted_raises_transient_error(self, real_client: _SyncTestClient) -> None:
        """Raises HttpTransientError when retries exhausted."""
        wrapper = _TransientFailureClient(real_client, failure_status=503, failures=10)
        with pytest.raises(HttpTransientError) as exc_info:
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=2, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert exc_info.value.status_code == 503
        assert wrapper.call_count == 3  # 1 initial + 2 retries

    def test_no_retry_on_non_transient(self, real_client: _SyncTestClient) -> None:
        """Non-retryable status codes are returned immediately."""
        wrapper = _TransientFailureClient(real_client, failure_status=400, failures=5)
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=b"data",
            headers={"Content-Type": "application/octet-stream"},
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 400
        assert wrapper.call_count == 1

    def test_max_retries_zero(self, real_client: _SyncTestClient) -> None:
        """max_retries=0 raises immediately on retryable status."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=5)
        with pytest.raises(HttpTransientError) as exc_info:
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=0),
                _sleep=lambda _: None,
            )
        assert exc_info.value.status_code == 502
        assert wrapper.call_count == 1

    def test_retry_after_preserved_in_error(self, real_client: _SyncTestClient) -> None:
        """HttpTransientError includes retry_after from last response."""
        wrapper = _TransientFailureClient(real_client, failure_status=429, failures=5, retry_after="60")
        with pytest.raises(HttpTransientError) as exc_info:
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=1, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert exc_info.value.status_code == 429
        assert exc_info.value.retry_after == 60.0

    def test_custom_retryable_codes(self, real_client: _SyncTestClient) -> None:
        """Custom retryable_status_codes are respected."""
        wrapper = _TransientFailureClient(real_client, failure_status=500, failures=5)
        # 500 is not in default retryable set — should NOT retry
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=b"data",
            headers={"Content-Type": "application/octet-stream"},
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 500
        assert wrapper.call_count == 1

        # Now with 500 in the retryable set
        wrapper2 = _TransientFailureClient(real_client, failure_status=500, failures=5)
        with pytest.raises(HttpTransientError):
            _post_with_retry(
                wrapper2,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=2, retryable_status_codes=frozenset({500}), backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert wrapper2.call_count == 3

    def test_each_retryable_code(self, real_client: _SyncTestClient) -> None:
        """All default retryable codes (429, 502, 503, 504) trigger retry."""
        for code in (429, 502, 503, 504):
            wrapper = _TransientFailureClient(real_client, failure_status=code, failures=1)
            resp = _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=_make_add_request(),
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
                _sleep=lambda _: None,
            )
            assert resp.status_code == 200, f"Failed for status code {code}"
            assert wrapper.call_count == 2

    def test_backoff_delay_called(self, real_client: _SyncTestClient) -> None:
        """Sleep is called with non-negative delays between retries."""
        delays: list[float] = []
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=3)
        with pytest.raises(HttpTransientError):
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=2, backoff_base=1.0, backoff_max=100.0),
                _sleep=lambda d: delays.append(d),
            )
        assert len(delays) == 2  # 2 retries = 2 sleeps
        assert all(d >= 0 for d in delays)


class TestConnectionErrorRetry:
    """Tests for retry on connection errors and timeouts."""

    def test_connect_error_retried_and_succeeds(self, real_client: _SyncTestClient) -> None:
        """ConnectError is retried and succeeds on next attempt."""
        wrapper = _ConnectionErrorClient(real_client, failures=2)
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=_make_add_request(),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 200
        assert wrapper.call_count == 3  # 2 errors + 1 success

    def test_connect_error_exhausted_raises(self, real_client: _SyncTestClient) -> None:
        """ConnectError is re-raised when retries are exhausted."""
        wrapper = _ConnectionErrorClient(real_client, failures=10)
        with pytest.raises(httpx.ConnectError):
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=2, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert wrapper.call_count == 3

    def test_connect_error_no_retry_when_disabled(self, real_client: _SyncTestClient) -> None:
        """ConnectError is re-raised immediately when retry_on_connection_error=False."""
        wrapper = _ConnectionErrorClient(real_client, failures=1)
        with pytest.raises(httpx.ConnectError):
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=3, retry_on_connection_error=False, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert wrapper.call_count == 1  # No retry

    def test_timeout_error_retried_and_succeeds(self, real_client: _SyncTestClient) -> None:
        """TimeoutException is retried and succeeds on next attempt."""
        wrapper = _TimeoutErrorClient(real_client, failures=1)
        resp = _post_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/add",
            content=_make_add_request(),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 200
        assert wrapper.call_count == 2

    def test_timeout_no_retry_when_disabled(self, real_client: _SyncTestClient) -> None:
        """TimeoutException is re-raised immediately when retry_on_connection_error=False."""
        wrapper = _TimeoutErrorClient(real_client, failures=1)
        with pytest.raises(httpx.ReadTimeout):
            _post_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/add",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
                config=HttpRetryConfig(max_retries=3, retry_on_connection_error=False, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert wrapper.call_count == 1


class TestOptionsWithRetry:
    """Tests for _options_with_retry."""

    def test_retries_on_503(self, real_client: _SyncTestClient) -> None:
        """OPTIONS retries on transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=503, failures=1)
        resp = _options_with_retry(
            wrapper,  # type: ignore[arg-type]
            "/vgi/__capabilities__",
            config=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            _sleep=lambda _: None,
        )
        assert resp.status_code == 200
        assert wrapper.call_count == 2

    def test_exhausted_raises(self, real_client: _SyncTestClient) -> None:
        """OPTIONS raises HttpTransientError when exhausted."""
        wrapper = _TransientFailureClient(real_client, failure_status=504, failures=10)
        with pytest.raises(HttpTransientError) as exc_info:
            _options_with_retry(
                wrapper,  # type: ignore[arg-type]
                "/vgi/__capabilities__",
                config=HttpRetryConfig(max_retries=1, backoff_base=0.001),
                _sleep=lambda _: None,
            )
        assert exc_info.value.status_code == 504


# ---------------------------------------------------------------------------
# Integration tests through http_connect
# ---------------------------------------------------------------------------


class TestHttpConnectWithRetry:
    """Integration tests: retry through http_connect proxy."""

    def test_unary_recovers(self, real_client: _SyncTestClient) -> None:
        """Unary call succeeds after transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=1)
        with http_connect(
            RpcFixtureService,
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        ) as proxy:
            result = proxy.add(a=3.0, b=4.0)
        assert result == 7.0
        assert wrapper.call_count == 2

    def test_stream_init_recovers(self, real_client: _SyncTestClient) -> None:
        """Stream init succeeds after transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=503, failures=1)
        with http_connect(
            RpcFixtureService,
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        ) as proxy:
            session = proxy.generate(count=2)
            batches = list(session)
        assert len(batches) > 0
        assert wrapper.call_count >= 2

    def test_stream_exchange_does_not_retry(self, real_client: _SyncTestClient) -> None:
        """Stream exchange does NOT retry — exchange calls may not be idempotent."""

        class _ExchangeFailWrapper:
            """Returns 502 on the first exchange POST."""

            def __init__(self, real: _SyncTestClient) -> None:
                self._real = real
                self._exchange_count = 0

            def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
                if "/exchange" in url:
                    self._exchange_count += 1
                    if self._exchange_count == 1:
                        return _SyncTestResponse(502, b"<html>Bad Gateway</html>")
                return self._real.post(url, content=content, headers=headers)

            def close(self) -> None:
                pass

        wrapper = _ExchangeFailWrapper(real_client)
        with http_connect(
            RpcFixtureService,
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        ) as proxy:
            session = proxy.transform(factor=2.0)
            input_batch = pa.record_batch({"value": [1.0, 2.0, 3.0]})
            # Exchange should NOT retry — 502 becomes RpcError immediately
            with pytest.raises(RpcError, match="HttpError"):
                session.exchange(AnnotatedBatch(batch=input_batch))

    def test_retry_exhaustion_raises(self, real_client: _SyncTestClient) -> None:
        """HttpTransientError raised when retries exhausted through proxy."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=10)
        with (
            http_connect(
                RpcFixtureService,
                client=wrapper,  # type: ignore[arg-type]
                retry=HttpRetryConfig(max_retries=2, backoff_base=0.001),
            ) as proxy,
            pytest.raises(HttpTransientError) as exc_info,
        ):
            proxy.add(a=1.0, b=2.0)
        assert exc_info.value.status_code == 502

    def test_no_retry_default(self, real_client: _SyncTestClient) -> None:
        """Without retry config, 503 raises plain RpcError (not HttpTransientError)."""
        wrapper = _TransientFailureClient(real_client, failure_status=503, failures=1)
        with (
            http_connect(
                RpcFixtureService,
                client=wrapper,  # type: ignore[arg-type]
            ) as proxy,
            pytest.raises(RpcError) as exc_info,
        ):
            proxy.add(a=1.0, b=2.0)
        assert not isinstance(exc_info.value, HttpTransientError)
        assert exc_info.value.error_type == "HttpError"

    def test_429_with_retry_after(self, real_client: _SyncTestClient) -> None:
        """429 with Retry-After header: retry succeeds."""
        wrapper = _TransientFailureClient(real_client, failure_status=429, failures=1, retry_after="1")
        with http_connect(
            RpcFixtureService,
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        ) as proxy:
            result = proxy.add(a=5.0, b=3.0)
        assert result == 8.0


class TestHttpIntrospectWithRetry:
    """Integration tests: retry through http_introspect."""

    def test_introspect_recovers(self, real_client: _SyncTestClient) -> None:
        """http_introspect succeeds after transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=1)
        desc = http_introspect(
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        )
        assert "add" in desc.methods
        assert wrapper.call_count == 2


class TestHttpCapabilitiesWithRetry:
    """Integration tests: retry through http_capabilities."""

    def test_capabilities_recovers(self, real_client: _SyncTestClient) -> None:
        """http_capabilities succeeds after transient failure."""
        wrapper = _TransientFailureClient(real_client, failure_status=504, failures=1)
        caps = http_capabilities(
            client=wrapper,  # type: ignore[arg-type]
            retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
        )
        assert caps is not None
        assert wrapper.call_count == 2


class TestRequestUploadUrlsWithRetry:
    """Integration tests: retry through request_upload_urls."""

    def test_upload_urls_recovers(self, real_client: _SyncTestClient) -> None:
        """request_upload_urls retries on transient failure (but server returns 404 without provider)."""
        wrapper = _TransientFailureClient(real_client, failure_status=502, failures=1)
        # Server has no upload_url_provider, so after retry succeeds we get NotSupported
        with pytest.raises(RpcError, match="NotSupported"):
            request_upload_urls(
                client=wrapper,  # type: ignore[arg-type]
                retry=HttpRetryConfig(max_retries=3, backoff_base=0.001),
            )
        # Verify retry happened: 1 failure + 1 success (which returns 404)
        assert wrapper.call_count == 2
