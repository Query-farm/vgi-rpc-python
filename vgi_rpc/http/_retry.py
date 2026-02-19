"""HTTP retry logic for transient failures.

Provides ``HttpRetryConfig`` for opt-in retry of transient HTTP errors
(429, 502, 503, 504) and connection failures, and ``HttpTransientError``
raised when retries are exhausted.

Logger: ``vgi_rpc.http.retry`` — retry attempts are logged at DEBUG level.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

import httpx

from vgi_rpc.rpc import RpcError

if TYPE_CHECKING:
    from collections.abc import Callable

    from vgi_rpc.http._testing import _SyncTestClient, _SyncTestResponse

_logger = logging.getLogger("vgi_rpc.http.retry")

# Default status codes produced by reverse proxies (nginx, ALB, etc.)
# that indicate transient failures safe to retry.
_DEFAULT_RETRYABLE: frozenset[int] = frozenset({429, 502, 503, 504})


@dataclass(frozen=True)
class HttpRetryConfig:
    """Configuration for retrying transient HTTP failures.

    Attributes:
        max_retries: Number of retry attempts (total calls = max_retries + 1).
        backoff_base: Exponential backoff base in seconds
            (delay = base * 2^attempt).
        backoff_max: Maximum backoff delay in seconds.
        retryable_status_codes: HTTP status codes eligible for retry.
        retry_on_connection_error: Whether to retry on ``httpx.ConnectError``
            and ``httpx.TimeoutException``.
        respect_retry_after: Whether to honor the ``Retry-After`` header
            on 429/503 responses.

    Raises:
        ValueError: If *max_retries* < 0, *backoff_base* < 0, or
            *backoff_max* < 0.

    """

    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_max: float = 30.0
    retryable_status_codes: frozenset[int] = field(default_factory=lambda: _DEFAULT_RETRYABLE)
    retry_on_connection_error: bool = True
    respect_retry_after: bool = True

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.backoff_base < 0:
            raise ValueError(f"backoff_base must be >= 0, got {self.backoff_base}")
        if self.backoff_max < 0:
            raise ValueError(f"backoff_max must be >= 0, got {self.backoff_max}")


class HttpTransientError(RpcError):
    """Raised when retries are exhausted on a transient HTTP error.

    Subclasses ``RpcError`` so existing ``except RpcError`` handlers still
    catch it.

    Attributes:
        status_code: The HTTP status code that caused the failure.
        retry_after: Parsed ``Retry-After`` value in seconds, or ``None``.

    """

    def __init__(self, status_code: int, body_preview: str, retry_after: float | None = None) -> None:
        """Initialize with HTTP status code and response body preview."""
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(
            "HttpTransientError",
            f"HTTP {status_code} after retries exhausted (body: {body_preview!r})",
            "",
        )


def _parse_retry_after(header_value: str) -> float | None:
    """Parse a ``Retry-After`` header value (delta-seconds or HTTP-date).

    Args:
        header_value: Raw header value.

    Returns:
        Delay in seconds, or ``None`` if unparseable.

    """
    # Try delta-seconds first (most common)
    try:
        return float(header_value)
    except ValueError:
        pass

    # Try HTTP-date (RFC 9110 section 10.2.3)
    try:
        dt = parsedate_to_datetime(header_value)
        delay = (dt - datetime.now(tz=UTC)).total_seconds()
        return max(0.0, delay)
    except (ValueError, TypeError):
        return None


def _compute_delay(
    attempt: int,
    config: HttpRetryConfig,
    retry_after: float | None,
) -> float:
    """Compute the backoff delay for a retry attempt.

    Uses exponential backoff with full jitter, clamped to ``backoff_max``.
    If ``retry_after`` is set and ``respect_retry_after`` is enabled, uses
    the larger of the computed delay and the server-requested delay.

    Args:
        attempt: Zero-based retry attempt number.
        config: Retry configuration.
        retry_after: Parsed Retry-After value, or ``None``.

    Returns:
        Delay in seconds before the next attempt.

    """
    exp_delay = config.backoff_base * (2**attempt)
    jittered = random.uniform(0, exp_delay)
    delay = min(jittered, config.backoff_max)

    if config.respect_retry_after and retry_after is not None:
        delay = max(delay, min(retry_after, config.backoff_max))

    return delay


def _get_retry_after(headers: object) -> float | None:
    """Extract and parse ``Retry-After`` from response headers.

    Handles both ``httpx.Headers`` (case-insensitive) and plain ``dict``
    (case-sensitive, checked with canonical and lowercase keys).

    Args:
        headers: Response headers (``httpx.Headers`` or ``dict``).

    Returns:
        Parsed delay in seconds, or ``None`` if absent or unparseable.

    """
    get = getattr(headers, "get", None)
    if get is None:
        return None
    raw: str | None = get("Retry-After")
    if raw is None:
        raw = get("retry-after")
    if raw is None:
        raw = get("RETRY-AFTER")
    if raw is None:
        return None
    return _parse_retry_after(raw)


def _body_preview(content: bytes) -> str:
    """Return a truncated, decoded preview of response body."""
    return content[:200].decode(errors="replace") if content else ""


def _request_with_retry(
    make_request: Callable[[], httpx.Response | _SyncTestResponse],
    *,
    config: HttpRetryConfig,
    method_label: str,
    url: str,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx.Response | _SyncTestResponse:
    """Execute an HTTP request with retry on transient failures.

    This is the core retry loop shared by ``_post_with_retry`` and
    ``_options_with_retry``.

    Args:
        make_request: Callable that performs the HTTP request.
        config: Retry configuration.
        method_label: HTTP method name for log messages (e.g. ``"POST"``).
        url: Request URL (for log messages only).
        _sleep: Sleep function (injectable for tests).

    Returns:
        The HTTP response.

    Raises:
        HttpTransientError: If retries are exhausted on a retryable status.
        httpx.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx.TimeoutException: If timeouts exhaust retries.

    """
    last_resp: httpx.Response | _SyncTestResponse | None = None
    last_retry_after: float | None = None

    for attempt in range(config.max_retries + 1):
        try:
            resp = make_request()
        except (httpx.ConnectError, httpx.TimeoutException):
            if not config.retry_on_connection_error or attempt >= config.max_retries:
                raise
            delay = _compute_delay(attempt, config, None)
            _logger.debug(
                "Connection error on %s %s (attempt %d/%d), retrying in %.2fs",
                method_label,
                url,
                attempt + 1,
                config.max_retries + 1,
                delay,
            )
            _sleep(delay)
            continue

        if resp.status_code not in config.retryable_status_codes:
            return resp

        # Retryable status — check if we have retries left
        last_resp = resp
        last_retry_after = _get_retry_after(resp.headers)

        if attempt >= config.max_retries:
            break

        delay = _compute_delay(attempt, config, last_retry_after)
        _logger.debug(
            "HTTP %d on %s %s (attempt %d/%d), retrying in %.2fs",
            resp.status_code,
            method_label,
            url,
            attempt + 1,
            config.max_retries + 1,
            delay,
        )
        _sleep(delay)

    # Retries exhausted — last_resp is guaranteed non-None because
    # max_retries >= 0 means at least one iteration executed.
    if last_resp is None:  # pragma: no cover — defensive, unreachable with validated config
        raise HttpTransientError(0, "no response received")
    raise HttpTransientError(last_resp.status_code, _body_preview(last_resp.content), last_retry_after)


def _post_with_retry(
    client: httpx.Client | _SyncTestClient,
    url: str,
    *,
    content: bytes,
    headers: dict[str, str],
    config: HttpRetryConfig | None,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx.Response | _SyncTestResponse:
    """Execute ``client.post()`` with optional retry on transient failures.

    Args:
        client: HTTP client (httpx or test client).
        url: Request URL.
        content: Request body bytes.
        headers: Request headers.
        config: Retry config, or ``None`` to disable retry.
        _sleep: Sleep function (injectable for tests).

    Returns:
        The HTTP response.

    Raises:
        HttpTransientError: If retries are exhausted on a retryable status.
        httpx.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx.TimeoutException: If timeouts exhaust retries.

    """
    if config is None:
        return client.post(url, content=content, headers=headers)

    return _request_with_retry(
        lambda: client.post(url, content=content, headers=headers),
        config=config,
        method_label="POST",
        url=url,
        _sleep=_sleep,
    )


def _options_with_retry(
    client: httpx.Client | _SyncTestClient,
    url: str,
    *,
    config: HttpRetryConfig | None,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx.Response | _SyncTestResponse:
    """Execute ``client.options()`` with optional retry on transient failures.

    Args:
        client: HTTP client (httpx or test client).
        url: Request URL.
        config: Retry config, or ``None`` to disable retry.
        _sleep: Sleep function (injectable for tests).

    Returns:
        The HTTP response.

    Raises:
        HttpTransientError: If retries are exhausted on a retryable status.
        httpx.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx.TimeoutException: If timeouts exhaust retries.

    """
    if config is None:
        return client.options(url)

    return _request_with_retry(
        lambda: client.options(url),
        config=config,
        method_label="OPTIONS",
        url=url,
        _sleep=_sleep,
    )
