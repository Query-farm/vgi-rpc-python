# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Falcon resources for HTTP endpoints: health, unary, stream, upload-url."""

from __future__ import annotations

import json
import time
from http import HTTPStatus
from io import BytesIO
from typing import TYPE_CHECKING, Literal

import falcon
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.rpc import (
    MethodType,
    RpcError,
    VersionError,
    _ClientLogSink,
    _emit_access_log,
    _get_auth_and_metadata,
    _log_method_error,
    _read_request,
    _truncate_error_message,
    _write_error_batch,
)
from vgi_rpc.rpc._common import CookieSpec, _current_response_cookies

from .._common import (
    _ARROW_CONTENT_TYPE,
    _MAX_UPLOAD_URL_COUNT,
    _UPLOAD_URL_METHOD,
    _UPLOAD_URL_SCHEMA,
    _RpcHttpError,
)
from ._responses import (
    _check_content_type,
    _current_response_status,
    _get_request_stream,
    _set_error_response,
    _set_http_status,
)

if TYPE_CHECKING:
    from ._app import _HttpRpcApp


class _HealthResource:
    """Falcon resource for the health check at ``GET {prefix}/health``.

    Returns a lightweight JSON response with the server status, ID, and
    protocol name.  The response body is pre-serialized at construction
    time so the handler is allocation-free.
    """

    __slots__ = ("_body",)

    def __init__(self, server_id: str, protocol_name: str) -> None:
        self._body = json.dumps(
            {"status": "ok", "server_id": server_id, "protocol": protocol_name},
            separators=(",", ":"),
        ).encode()

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the health check JSON response."""
        resp.content_type = falcon.MEDIA_JSON
        resp.data = self._body


def _apply_cookies_to_response(resp: falcon.Response, cookies: list[CookieSpec]) -> None:
    """Apply queued ``CookieSpec`` entries to the Falcon response."""
    for c in cookies:
        if c.delete:
            resp.unset_cookie(c.name, path=c.path, domain=c.domain)
        else:
            resp.set_cookie(
                c.name,
                c.value,
                expires=c.expires,
                max_age=c.max_age,
                domain=c.domain,
                path=c.path,
                secure=c.secure,
                http_only=c.http_only,
                same_site=c.same_site,
                partitioned=c.partitioned,
            )


class _RpcResource:
    """Falcon resource for unary calls: ``POST {prefix}/{method}``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle unary and __describe__ RPC calls."""
        cookies: list[CookieSpec] = []
        cookie_token = _current_response_cookies.set(cookies)
        try:
            try:
                info = self._app._resolve_method(req, method)
                if info.method_type == MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Stream method '{method}' requires /init and /exchange endpoints"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )

                result_stream, http_status = self._app._unary_sync(method, info, _get_request_stream(req))
                resp.content_type = _ARROW_CONTENT_TYPE
                resp.stream = result_stream
                _set_http_status(resp, http_status)
            except _RpcHttpError as e:
                _set_error_response(
                    resp,
                    e.cause,
                    status_code=e.status_code,
                    schema=e.schema,
                    server_id=self._app._server.server_id,
                )
            _apply_cookies_to_response(resp, cookies)
        finally:
            _current_response_cookies.reset(cookie_token)


class _StreamInitResource:
    """Falcon resource for stream init: ``POST {prefix}/{method}/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle stream initialization (both producer and exchange)."""
        status_token = _current_response_status.set(HTTPStatus.OK)
        try:
            try:
                info = self._app._resolve_method(req, method)
                if info.method_type != MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Method '{method}' is not a stream"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                result_stream = self._app._stream_init_sync(method, info, _get_request_stream(req))
            except _RpcHttpError as e:
                _set_error_response(
                    resp,
                    e.cause,
                    status_code=e.status_code,
                    schema=e.schema,
                    server_id=self._app._server.server_id,
                )
                return
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
            # In-band errors (cap overshoots) flip the contextvar; translate
            # to 200 + X-VGI-RPC-Error: true to match the documented contract.
            _set_http_status(resp, _current_response_status.get())
        finally:
            _current_response_status.reset(status_token)


class _ExchangeResource:
    """Falcon resource for state exchange: ``POST {prefix}/{method}/exchange``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle stream exchange or producer continuation."""
        status_token = _current_response_status.set(HTTPStatus.OK)
        try:
            try:
                info = self._app._resolve_method(req, method)
                if info.method_type != MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Method '{method}' does not support /exchange"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                result_stream = self._app._stream_exchange_sync(method, _get_request_stream(req))
            except _RpcHttpError as e:
                _set_error_response(
                    resp,
                    e.cause,
                    status_code=e.status_code,
                    schema=e.schema,
                    server_id=self._app._server.server_id,
                )
                return
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
            # In-band errors (cap overshoots) flip the contextvar; translate
            # to 200 + X-VGI-RPC-Error: true to match the documented contract.
            _set_http_status(resp, _current_response_status.get())
        finally:
            _current_response_status.reset(status_token)


class _UploadUrlResource:
    """Falcon resource for upload URL generation: ``POST {prefix}/__upload_url__/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Handle upload URL generation requests."""
        try:
            _check_content_type(req)
            # Route is only registered when upload_url_provider is set.
            provider = self._app._upload_url_provider
            assert provider is not None

            # Read request using standard wire protocol
            try:
                ipc_method, kwargs = _read_request(_get_request_stream(req), self._app._server.ipc_validation)
                if ipc_method != _UPLOAD_URL_METHOD:
                    raise TypeError(f"Method mismatch: expected '{_UPLOAD_URL_METHOD}', got '{ipc_method}'")
            except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

            count = kwargs.get("count", 1)
            if not isinstance(count, int):
                count = 1
            count = max(1, min(count, _MAX_UPLOAD_URL_COUNT))
        except _RpcHttpError as e:
            _set_error_response(resp, e.cause, status_code=e.status_code, server_id=self._app._server.server_id)
            return

        # Execute: follows the same response pattern as _unary_sync —
        # log sink, inline error batches, access logging.
        server_id = self._app._server.server_id
        protocol_name = self._app._server.protocol_name
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()

        resp_buf = BytesIO()
        http_status = HTTPStatus.OK
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        _upload_exc: BaseException | None = None
        try:
            with ipc.new_stream(resp_buf, _UPLOAD_URL_SCHEMA) as writer:
                sink.flush_contents(writer, _UPLOAD_URL_SCHEMA)
                try:
                    urls = [provider.generate_upload_url(pa.schema([])) for _ in range(count)]
                    result_batch = pa.RecordBatch.from_pydict(
                        {
                            "upload_url": [u.upload_url for u in urls],
                            "download_url": [u.download_url for u in urls],
                            "expires_at": [u.expires_at for u in urls],
                        },
                        schema=_UPLOAD_URL_SCHEMA,
                    )
                    writer.write_batch(result_batch)
                except Exception as exc:
                    _upload_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, _UPLOAD_URL_METHOD, server_id, exc)
                    _write_error_batch(writer, _UPLOAD_URL_SCHEMA, exc, server_id=server_id)
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                _UPLOAD_URL_METHOD,
                "unary",
                server_id,
                auth,
                transport_metadata,
                duration_ms,
                status,
                error_type,
                http_status=http_status.value,
                server_version=self._app._server.server_version,
                protocol_hash=self._app._server.protocol_hash,
                protocol_version=self._app._server.protocol_version,
                error_message=_truncate_error_message(_upload_exc),
            )

        resp_buf.seek(0)
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.stream = resp_buf
        _set_http_status(resp, http_status)
