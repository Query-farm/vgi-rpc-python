# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Small response/error helpers shared across the HTTP server."""

from __future__ import annotations

import importlib.metadata
import re
from contextvars import ContextVar
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import NamedTuple

import falcon
import pyarrow as pa

from vgi_rpc.rpc import _EMPTY_SCHEMA, _write_error_batch
from vgi_rpc.rpc._common import _current_request_batch
from vgi_rpc.utils import new_ipc_stream

from .._common import _ARROW_CONTENT_TYPE, ACCEPT_MAX_RESPONSE_BYTES_HEADER, RPC_ERROR_HEADER, _RpcHttpError

# Set by stream-dispatch paths that emit an in-band EXCEPTION batch instead
# of raising ``_RpcHttpError`` (cap-overshoot for stream-exchange and the
# producer external-channel cap).  The resource layer reads this on each
# request to translate a 500 into 200 + ``X-VGI-RPC-Error: true`` so the
# response shape matches the documented contract for hard caps.
_current_response_status: ContextVar[HTTPStatus] = ContextVar("vgi_rpc_response_status", default=HTTPStatus.OK)

_POSITIVE_DECIMAL = re.compile(r"[1-9][0-9]*\Z", re.ASCII)
_MIN_BUDGET_BYTES = 65536
_MAX_BUDGET_BYTES = (1 << 53) - 1


class _ResponseBudget(NamedTuple):
    """Hard and preferred byte budgets for the active HTTP response."""

    response_limit_bytes: int | None
    preferred_response_bytes: int | None


_current_response_budget: ContextVar[_ResponseBudget] = ContextVar(
    "vgi_rpc_response_budget",
    default=_ResponseBudget(None, None),  # noqa: B039 - NamedTuple is immutable
)


class ResponseTooLargeError(RuntimeError):
    """A successful RPC result could not fit the negotiated HTTP limit."""


def _minimum_present(*values: int | None) -> int | None:
    """Return the minimum configured value, or ``None`` when all are absent."""
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _parse_accepted_response_bytes(req: falcon.Request) -> int | None:
    """Parse the client's strict positive-decimal response limit."""
    raw = req.get_header(ACCEPT_MAX_RESPONSE_BYTES_HEADER)
    if raw is None:
        return None
    # WSGI combines duplicates with a comma, which intentionally fails this
    # grammar along with signs, whitespace, and leading zeroes.
    if _POSITIVE_DECIMAL.fullmatch(raw) is None:
        raise _RpcHttpError(
            ValueError(f"{ACCEPT_MAX_RESPONSE_BYTES_HEADER} must be one canonical positive decimal integer"),
            status_code=HTTPStatus.BAD_REQUEST,
        )
    value = int(raw)
    if value < _MIN_BUDGET_BYTES:
        raise _RpcHttpError(
            ValueError(f"{ACCEPT_MAX_RESPONSE_BYTES_HEADER} must be at least {_MIN_BUDGET_BYTES}"),
            status_code=HTTPStatus.BAD_REQUEST,
        )
    if value > _MAX_BUDGET_BYTES:
        raise _RpcHttpError(
            ValueError(f"{ACCEPT_MAX_RESPONSE_BYTES_HEADER} exceeds {_MAX_BUDGET_BYTES}"),
            status_code=HTTPStatus.BAD_REQUEST,
        )
    return value


def _resolve_response_budget(
    req: falcon.Request,
    *,
    server_limit_bytes: int | None,
    server_preferred_bytes: int | None,
) -> _ResponseBudget:
    """Resolve one request's hard response limit and preferred target."""
    client_limit = _parse_accepted_response_bytes(req)
    effective = _minimum_present(server_limit_bytes, client_limit)
    preferred = _minimum_present(server_preferred_bytes, effective)
    return _ResponseBudget(effective, preferred)


def _tighten_response_budget(sealed_limit_bytes: int | None) -> _ResponseBudget:
    """Tighten the active request budget by a stream's sealed init limit."""
    current = _current_response_budget.get()
    effective = _minimum_present(current.response_limit_bytes, sealed_limit_bytes)
    preferred = _minimum_present(current.preferred_response_bytes, effective)
    tightened = _ResponseBudget(effective, preferred)
    _current_response_budget.set(tightened)
    return tightened


def _vgi_version() -> str:
    """Return the installed vgi-rpc package version."""
    try:
        return importlib.metadata.version("vgi-rpc")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


def _enforce_response_budgets(
    *,
    method_name: str,
    wire_bytes: int,
    external_bytes: int,
    wire_cap: int | None,
    external_cap: int | None,
) -> None:
    """Raise ``RuntimeError`` if a response overshoots either configured cap.

    Called *after* a response has been flushed.  Both caps are independent:

    - ``wire_cap`` (``max_response_bytes``) governs decoded Arrow IPC body
      bytes before HTTP content coding.
      Externalised payloads do not count toward this — they leave only
      tiny pointer batches on the wire.
    - ``external_cap`` (``max_externalized_response_bytes``) governs the
      total bytes uploaded to external storage during one HTTP response.
      Bounds how much data the client will end up fetching for one RPC,
      regardless of how the framework chose to deliver it.

    The transport layer surfaces the failure as 200 + EXCEPTION-batch via
    the existing ``_set_http_status`` (unary: 500 → 200 + ``X-VGI-RPC-Error``)
    or by replacing a stream response with an error envelope. The RPC client
    sees a normal ``RpcError``.

    Args:
        method_name: For diagnostic messages.
        wire_bytes: ``resp_buf.tell()`` after flushing the response body.
        external_bytes: Cumulative bytes uploaded to external storage
            during this response.
        wire_cap: ``max_response_bytes`` or ``None`` for unbounded.
        external_cap: ``max_externalized_response_bytes`` or ``None``.

    Raises:
        RuntimeError: When either cap is exceeded.

    """
    if wire_cap is not None and wire_bytes > wire_cap:
        raise ResponseTooLargeError(
            f"HTTP body exceeds max_response_bytes ({wire_bytes} > {wire_cap}) for method {method_name!r}"
        )
    if external_cap is not None and external_bytes > external_cap:
        raise RuntimeError(
            f"Externalised payload exceeds max_externalized_response_bytes "
            f"({external_bytes} > {external_cap}) for method {method_name!r}"
        )


def _check_content_type(req: falcon.Request) -> None:
    """Raise ``_RpcHttpError`` if Content-Type is not Arrow IPC stream."""
    content_type = req.content_type or ""
    if content_type != _ARROW_CONTENT_TYPE:
        raise _RpcHttpError(
            TypeError(
                f"Expected Content-Type: '{_ARROW_CONTENT_TYPE}', got {content_type!r}. "
                f"All vgi-rpc HTTP requests must use Content-Type: {_ARROW_CONTENT_TYPE}"
            ),
            status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
        )


def _error_response_stream(
    exc: BaseException, schema: pa.Schema = _EMPTY_SCHEMA, server_id: str | None = None
) -> BytesIO:
    """Serialize an exception as a complete Arrow IPC error stream.

    Args:
        exc: The exception to serialize.
        schema: Arrow schema for the error stream (default empty).
        server_id: Optional server identifier injected into error metadata.

    Returns:
        A ``BytesIO`` positioned at the start, containing the IPC stream.

    """
    buf = BytesIO()
    with new_ipc_stream(buf, schema) as writer:
        _write_error_batch(writer, schema, exc, server_id=server_id)
    buf.seek(0)
    return buf


def _set_http_status(resp: falcon.Response, status_code: HTTPStatus) -> None:
    """Set HTTP status, translating 500 to 200 with error header.

    Server errors are sent as HTTP 200 with ``X-VGI-RPC-Error: true``
    so clients that discard response bodies on 5xx still receive the
    Arrow IPC error metadata.

    Args:
        resp: Falcon response object.
        status_code: Intended HTTP status code.

    """
    if status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        resp.status = "200"
        resp.set_header(RPC_ERROR_HEADER, "true")
    else:
        resp.status = str(status_code.value)


def _get_request_stream(req: falcon.Request) -> IOBase | pa.NativeFile:
    """Return the request body stream, using the decompressed stream if available.

    When ``_CompressionMiddleware`` is active and the request body was
    compressed, the decompressed bytes are stored in ``req.context.decompressed_stream``.
    This helper returns that stream when present, falling back to Falcon's
    ``req.bounded_stream``.

    Args:
        req: The Falcon request.

    Returns:
        A readable binary stream for the request body.

    """
    stream: IOBase | pa.NativeFile | None = getattr(req.context, "decompressed_stream", None)
    if stream is not None:
        return stream
    # Uncompressed: read the bounded body once and hand Arrow a native
    # buffer. Arrow consumes the whole stream regardless -- an IPC message
    # cannot be parsed incrementally here -- so this trades nothing for
    # keeping the reads in C++ instead of calling back into Python.
    body = getattr(req.context, "capped_request_body", None)
    if body is None:
        body = req.bounded_stream.read()
    _current_request_batch.set(body)
    return pa.BufferReader(body)


def _set_error_response(
    resp: falcon.Response,
    exc: BaseException,
    *,
    status_code: HTTPStatus = HTTPStatus.BAD_REQUEST,
    schema: pa.Schema = _EMPTY_SCHEMA,
    server_id: str | None = None,
) -> None:
    """Set a Falcon response to an Arrow IPC error stream."""
    resp.content_type = _ARROW_CONTENT_TYPE
    resp.stream = _error_response_stream(exc, schema, server_id=server_id)
    _set_http_status(resp, status_code)
