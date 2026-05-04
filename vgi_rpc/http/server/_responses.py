# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Small response/error helpers shared across the HTTP server."""

from __future__ import annotations

import importlib.metadata
from contextvars import ContextVar
from http import HTTPStatus
from io import BytesIO, IOBase

import falcon
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.rpc import _EMPTY_SCHEMA, _write_error_batch

from .._common import _ARROW_CONTENT_TYPE, RPC_ERROR_HEADER, _RpcHttpError

# Set by stream-dispatch paths that emit an in-band EXCEPTION batch instead
# of raising ``_RpcHttpError`` (cap-overshoot for stream-exchange and the
# producer external-channel cap).  The resource layer reads this on each
# request to translate a 500 into 200 + ``X-VGI-RPC-Error: true`` so the
# response shape matches the documented contract for hard caps.
_current_response_status: ContextVar[HTTPStatus] = ContextVar("vgi_rpc_response_status", default=HTTPStatus.OK)


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

    - ``wire_cap`` (``max_response_bytes``) governs the HTTP body size.
      Externalised payloads do not count toward this — they leave only
      tiny pointer batches on the wire.
    - ``external_cap`` (``max_externalized_response_bytes``) governs the
      total bytes uploaded to external storage during one HTTP response.
      Bounds how much data the client will end up fetching for one RPC,
      regardless of how the framework chose to deliver it.

    The transport layer surfaces the failure as 200 + EXCEPTION-batch via
    the existing ``_set_http_status`` (unary: 500 → 200 + ``X-VGI-RPC-Error``)
    or by appending a zero-row error batch to the in-progress IPC stream
    (producer/exchange).  The RPC client sees a normal ``RpcError``.

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
        raise RuntimeError(
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
    with ipc.new_stream(buf, schema) as writer:
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


def _get_request_stream(req: falcon.Request) -> IOBase:
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
    stream: IOBase | None = getattr(req.context, "decompressed_stream", None)
    if stream is not None:
        return stream
    return req.bounded_stream


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
