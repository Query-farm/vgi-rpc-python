# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP retry logic for replay-safe transient failures.

Provides ``HttpRetryConfig`` for opt-in retry of transient HTTP errors
(429, 502, 503, 504) and connection failures, and ``HttpTransientError``
raised when retries are exhausted.

RPC dispatches do not use these helpers: a proxy may return a transient
status after the server committed a side effect, so replay requires an
application-level idempotency contract. The HTTP client uses this policy for
replay-safe discovery and control requests only.

Logger: ``vgi_rpc.http.retry`` — retry attempts are logged at DEBUG level.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, cast

import httpx2

from vgi_rpc.rpc import RpcError

from ._common import ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER

if TYPE_CHECKING:
    from collections.abc import Callable

    from vgi_rpc.http._testing import _SyncTestClient, _SyncTestResponse

_logger = logging.getLogger("vgi_rpc.http.retry")

# Default status codes handled by replay-safe requests.
_DEFAULT_RETRYABLE: frozenset[int] = frozenset({429, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class _BufferedResponse:
    """Closed streaming response with only the fields consumed by the RPC client."""

    status_code: int
    content: bytes
    headers: httpx2.Headers


def _response_too_large(actual: int, limit: int) -> RpcError:
    """Build the canonical client-side decoded-response overflow error."""
    return RpcError(
        "ResponseTooLargeError",
        f"Decoded HTTP response exceeds accepted_max_response_bytes ({actual} > {limit})",
        "",
    )


def _require_response_budget_support_header(headers: object) -> None:
    """Require one exact response-budget support field with value ``true``."""
    get_list = getattr(headers, "get_list", None)
    if callable(get_list):
        values = list(get_list(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER))
    else:
        get = getattr(headers, "get", None)
        raw = None if get is None else get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER)
        if raw is None and get is not None:
            raw = get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower())
        values = [] if raw is None else [raw]
    if values != ["true"]:
        raise RpcError(
            "ProtocolError",
            "Every capped RPC response must contain exactly one "
            f"{ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER}: true field",
            "",
        )


def _post_bounded(
    client: httpx2.Client | _SyncTestClient,
    url: str,
    *,
    content: bytes,
    headers: dict[str, str],
    response_limit_bytes: int | None,
) -> httpx2.Response | _SyncTestResponse | _BufferedResponse:
    """POST while bounding decoded response buffering to ``limit + 1`` bytes."""
    stream_request = getattr(client, "stream", None)
    supports_streaming = getattr(client, "_supports_bounded_stream", True)
    if response_limit_bytes is None or not callable(stream_request) or not supports_streaming:
        response = client.post(url, content=content, headers=headers)
        if response_limit_bytes is not None:
            _require_response_budget_support_header(response.headers)
            if len(response.content) > response_limit_bytes:
                raise _response_too_large(len(response.content), response_limit_bytes)
        return response

    buffered = bytearray()
    with stream_request("POST", url, content=content, headers=headers) as response:
        _require_response_budget_support_header(response.headers)
        for chunk in response.iter_bytes(chunk_size=64 * 1024):
            remaining = response_limit_bytes + 1 - len(buffered)
            if remaining <= 0:
                raise _response_too_large(len(buffered), response_limit_bytes)
            buffered.extend(chunk[:remaining])
            if len(buffered) > response_limit_bytes:
                raise _response_too_large(len(buffered), response_limit_bytes)
        return _BufferedResponse(response.status_code, bytes(buffered), response.headers)


@dataclass(frozen=True)
class HttpRetryConfig:
    """Configuration for retrying replay-safe transient HTTP failures.

    Attributes:
        max_retries: Number of retry attempts (total calls = max_retries + 1).
        backoff_base: Exponential backoff base in seconds
            (delay = base * 2^attempt).
        backoff_max: Maximum backoff delay in seconds.
        retryable_status_codes: HTTP status codes eligible for retry.
        retry_on_connection_error: Whether to retry on ``httpx2.ConnectError``
            and ``httpx2.TimeoutException``.
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

    status_code: int
    retry_after: float | None

    def __init__(self, status_code: int, body_preview: str, retry_after: float | None = None) -> None:
        """Initialize with HTTP status code and response body preview.

        Args:
            status_code: The HTTP status code that caused the failure.
            body_preview: Truncated response body, included in the error message.
            retry_after: Parsed ``Retry-After`` value in seconds, or ``None``.

        """
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

    Handles both ``httpx2.Headers`` (case-insensitive) and plain ``dict``
    (case-sensitive, checked with canonical and lowercase keys).

    Args:
        headers: Response headers (``httpx2.Headers`` or ``dict``).

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
    make_request: Callable[[], httpx2.Response | _SyncTestResponse | _BufferedResponse],
    *,
    config: HttpRetryConfig,
    method_label: str,
    url: str,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx2.Response | _SyncTestResponse | _BufferedResponse:
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
        httpx2.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx2.TimeoutException: If timeouts exhaust retries.

    """
    last_resp: httpx2.Response | _SyncTestResponse | _BufferedResponse | None = None
    last_retry_after: float | None = None

    for attempt in range(config.max_retries + 1):
        try:
            resp = make_request()
        except httpx2.RemoteProtocolError as exc:
            # A pooled keep-alive connection the peer had already closed.
            # httpx2 raises this with "Server disconnected without sending a
            # response" when the socket dies before ANY response byte arrives,
            # which is the one disconnect we can retry safely: the request
            # provably was not answered, so replaying it cannot duplicate an
            # effect the caller already observed. RFC 9110 6.3 requires a
            # client to cope with this -- a server or load balancer is free to
            # reap an idle persistent connection at any moment, and it races
            # the next request no matter how the client is written.
            #
            # Any other RemoteProtocolError means bytes were already flowing,
            # so the server may well have applied a non-idempotent POST; those
            # propagate rather than risk a double-apply.
            if "without sending a response" not in str(exc):
                raise
            if not config.retry_on_connection_error or attempt >= config.max_retries:
                raise
            delay = _compute_delay(attempt, config, None)
            _logger.debug(
                "Stale pooled connection on %s %s (attempt %d/%d), retrying in %.2fs",
                method_label,
                url,
                attempt + 1,
                config.max_retries + 1,
                delay,
            )
            _sleep(delay)
            continue
        except (httpx2.ConnectError, httpx2.TimeoutException):
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
    client: httpx2.Client | _SyncTestClient,
    url: str,
    *,
    content: bytes,
    headers: dict[str, str],
    config: HttpRetryConfig | None,
    response_limit_bytes: int | None = None,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx2.Response | _SyncTestResponse | _BufferedResponse:
    """Execute ``client.post()`` with optional retry on transient failures.

    Args:
        client: HTTP client (httpx2 or test client).
        url: Request URL.
        content: Request body bytes.
        headers: Request headers.
        config: Retry config, or ``None`` to disable retry.
        response_limit_bytes: Optional decoded response cap. Real HTTP clients
            stream into a buffer bounded to this value plus one byte.
        _sleep: Sleep function (injectable for tests).

    Returns:
        The HTTP response.

    Raises:
        HttpTransientError: If retries are exhausted on a retryable status.
        httpx2.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx2.TimeoutException: If timeouts exhaust retries.

    """
    if config is None:
        return _post_bounded(
            client,
            url,
            content=content,
            headers=headers,
            response_limit_bytes=response_limit_bytes,
        )

    return _request_with_retry(
        lambda: _post_bounded(
            client,
            url,
            content=content,
            headers=headers,
            response_limit_bytes=response_limit_bytes,
        ),
        config=config,
        method_label="POST",
        url=url,
        _sleep=_sleep,
    )


def _options_with_retry(
    client: httpx2.Client | _SyncTestClient,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    config: HttpRetryConfig | None,
    _sleep: Callable[[float], object] = time.sleep,
) -> httpx2.Response | _SyncTestResponse:
    """Execute ``client.options()`` with optional retry on transient failures.

    Args:
        client: HTTP client (httpx2 or test client).
        url: Request URL.
        headers: Optional request headers sent on every attempt.
        config: Retry config, or ``None`` to disable retry.
        _sleep: Sleep function (injectable for tests).

    Returns:
        The HTTP response.

    Raises:
        HttpTransientError: If retries are exhausted on a retryable status.
        httpx2.ConnectError: If connection errors exhaust retries (when
            ``retry_on_connection_error`` is enabled).
        httpx2.TimeoutException: If timeouts exhaust retries.

    """
    if config is None:
        return client.options(url, headers=headers)

    return cast(
        "httpx2.Response | _SyncTestResponse",
        _request_with_retry(
            lambda: client.options(url, headers=headers),
            config=config,
            method_label="OPTIONS",
            url=url,
            _sleep=_sleep,
        ),
    )
