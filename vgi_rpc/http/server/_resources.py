# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Falcon resources for HTTP endpoints: health, unary, stream, upload-url."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from http import HTTPStatus
from io import BytesIO
from typing import TYPE_CHECKING, Literal

import falcon
import pyarrow as pa

from vgi_rpc.errors import error_code_of
from vgi_rpc.external import is_sha256_hex
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
from vgi_rpc.rpc._common import CookieSpec, ProtocolError, _current_body_precompressed, _current_response_cookies
from vgi_rpc.rpc._types import PROTOCOL_NAME_RE
from vgi_rpc.state_ref import mint_argument_ref
from vgi_rpc.utils import new_ipc_stream

from .._common import (
    _ARROW_CONTENT_TYPE,
    _MAX_UPLOAD_URL_COUNT,
    _UPLOAD_URL_METHOD,
    _UPLOAD_URL_SCHEMA,
    MAX_UPLOAD_BYTES_HEADER,
    UPLOAD_URL_HEADER,
    UPLOAD_URL_PROTOCOL_VERSION,
    _RpcHttpError,
)
from ._responses import (
    ResponseTooLargeError,
    _check_content_type,
    _current_response_budget,
    _current_response_status,
    _error_response_stream,
    _get_request_stream,
    _resolve_response_budget,
    _set_error_response,
    _set_http_status,
)

if TYPE_CHECKING:
    from ._app import _HttpRpcApp


def _strict_response_stream(
    stream: BytesIO | pa.NativeFile,
    *,
    method: str,
    limit: int | None,
    server_id: str,
    include_traceback: bool,
) -> tuple[BytesIO, bool]:
    """Materialize and strict-cap one successful resource response."""
    body = stream.read()
    if limit is not None and len(body) > limit:
        exc = ResponseTooLargeError(
            f"HTTP body exceeds max_response_bytes ({len(body)} > {limit}) for method {method!r}"
        )
        # A producer continuation may have encoded its successful body into
        # the negotiated codec before this final strict check. The replacement
        # error is plaintext, so let response middleware encode it normally.
        _current_body_precompressed.set(False)
        return _error_response_stream(exc, server_id=server_id, include_traceback=include_traceback), True
    return BytesIO(body), False


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

    def on_head(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Answer ``HEAD {prefix}/health`` with GET's headers and no body.

        ``/health`` is the mandatory, auth-exempt capability-discovery endpoint,
        and the C++ client probes it with ``HEAD`` (the Python client uses
        ``OPTIONS``/``GET``). Falcon does not synthesize ``HEAD`` from ``on_get``
        here, so without this responder ``HEAD /health`` 405s and discovery
        silently degrades to defaults. Emit the same ``Content-Type`` +
        ``Content-Length`` (the capability headers are added by middleware on
        every response), with no body.
        """
        resp.content_type = falcon.MEDIA_JSON
        resp.set_header("Content-Length", str(len(self._body)))


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


#: Sentinel protocol for the flat reserved-method route. Reserved names
#: (``__describe__``) are server-level and owned by no protocol.
_RESERVED_PROTOCOL = "\x00reserved"


class _RpcResource:
    """Falcon resource for unary calls: ``POST {prefix}/{method}``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, protocol: str, method: str) -> None:
        """Handle unary and __describe__ RPC calls."""
        cookies: list[CookieSpec] = []
        cookie_token = _current_response_cookies.set(cookies)
        try:
            budget = _resolve_response_budget(
                req,
                server_limit_bytes=self._app._max_response_bytes,
                server_preferred_bytes=self._app._preferred_response_bytes,
            )
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
                include_traceback=self._app._tracebacks,
            )
            _current_response_cookies.reset(cookie_token)
            return
        budget_token = _current_response_budget.set(budget)
        try:
            try:
                info = self._app._resolve_method(req, protocol, method)
                if info.method_type == MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Stream method '{method}' requires /init and /exchange endpoints"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )

                result_stream, http_status = self._app._unary_sync(method, info, _get_request_stream(req))
                if http_status == HTTPStatus.OK:
                    result_stream, oversized = _strict_response_stream(
                        result_stream,
                        method=method,
                        limit=_current_response_budget.get().response_limit_bytes,
                        server_id=self._app._server.server_id,
                        include_traceback=self._app._tracebacks,
                    )
                    if oversized:
                        http_status = HTTPStatus.INTERNAL_SERVER_ERROR
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
                    include_traceback=self._app._tracebacks,
                )
            _apply_cookies_to_response(resp, cookies)
        finally:
            _current_response_budget.reset(budget_token)
            _current_response_cookies.reset(cookie_token)

    def on_get(self, req: falcon.Request, resp: falcon.Response, protocol: str, method: str = "") -> None:
        """RPC endpoints are POST-only; say 404 when the path was never one.

        Falcon would otherwise answer 405 for any two-segment GET that happens
        to match this route, including paths belonging to other features.
        """
        del req, resp, method
        if not PROTOCOL_NAME_RE.match(protocol) or protocol not in self._app._server._bindings:
            raise falcon.HTTPNotFound
        raise falcon.HTTPMethodNotAllowed(allowed_methods=["POST"])

    def on_post_reserved(self, req: falcon.Request, resp: falcon.Response, protocol: str) -> None:
        """Handle a flat, server-level reserved method such as ``__describe__``.

        Routed at ``{prefix}/{method}`` rather than ``{prefix}/{protocol}/{method}``
        because these belong to no protocol. Anything that is not a reserved
        name 404s here rather than falling through to a protocol lookup — the
        flat route is not a catch-all.
        """
        # Falcon hands us the segment under the shared field name `protocol`;
        # on this route it is a reserved *method* name.
        method = protocol
        if not (method.startswith("__") and method.endswith("__")):
            raise falcon.HTTPNotFound
        self.on_post(req, resp, _RESERVED_PROTOCOL, method)


class _StreamInitResource:
    """Falcon resource for stream init: ``POST {prefix}/{method}/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, protocol: str, method: str) -> None:
        """Handle stream initialization (both producer and exchange)."""
        try:
            budget = _resolve_response_budget(
                req,
                server_limit_bytes=self._app._max_response_bytes,
                server_preferred_bytes=self._app._preferred_response_bytes,
            )
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
                include_traceback=self._app._tracebacks,
            )
            return
        budget_token = _current_response_budget.set(budget)
        status_token = _current_response_status.set(HTTPStatus.OK)
        try:
            try:
                info = self._app._resolve_method(req, protocol, method)
                if info.method_type != MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Method '{method}' is not a stream"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                result_stream = self._app._stream_init_sync(method, info, _get_request_stream(req))
                if _current_response_status.get() == HTTPStatus.OK:
                    result_stream, oversized = _strict_response_stream(
                        result_stream,
                        method=method,
                        limit=budget.response_limit_bytes,
                        server_id=self._app._server.server_id,
                        include_traceback=self._app._tracebacks,
                    )
                    if oversized:
                        _current_response_status.set(HTTPStatus.INTERNAL_SERVER_ERROR)
            except _RpcHttpError as e:
                _set_error_response(
                    resp,
                    e.cause,
                    status_code=e.status_code,
                    schema=e.schema,
                    server_id=self._app._server.server_id,
                    include_traceback=self._app._tracebacks,
                )
                return
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
            # In-band errors (cap overshoots) flip the contextvar; translate
            # to 200 + X-VGI-RPC-Error: true to match the documented contract.
            _set_http_status(resp, _current_response_status.get())
        finally:
            _current_response_status.reset(status_token)
            _current_response_budget.reset(budget_token)


class _ExchangeResource:
    """Falcon resource for state exchange: ``POST {prefix}/{method}/exchange``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, protocol: str, method: str) -> None:
        """Handle stream exchange or producer continuation."""
        try:
            budget = _resolve_response_budget(
                req,
                server_limit_bytes=self._app._max_response_bytes,
                server_preferred_bytes=self._app._preferred_response_bytes,
            )
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
                include_traceback=self._app._tracebacks,
            )
            return
        budget_token = _current_response_budget.set(budget)
        status_token = _current_response_status.set(HTTPStatus.OK)
        try:
            try:
                info = self._app._resolve_method(req, protocol, method)
                if info.method_type != MethodType.STREAM:
                    raise _RpcHttpError(
                        TypeError(f"Method '{method}' does not support /exchange"),
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                result_stream = self._app._stream_exchange_sync(info, _get_request_stream(req))
                if _current_response_status.get() == HTTPStatus.OK:
                    result_stream, oversized = _strict_response_stream(
                        result_stream,
                        method=method,
                        limit=_current_response_budget.get().response_limit_bytes,
                        server_id=self._app._server.server_id,
                        include_traceback=self._app._tracebacks,
                    )
                    if oversized:
                        _current_response_status.set(HTTPStatus.INTERNAL_SERVER_ERROR)
            except _RpcHttpError as e:
                _set_error_response(
                    resp,
                    e.cause,
                    status_code=e.status_code,
                    schema=e.schema,
                    server_id=self._app._server.server_id,
                    include_traceback=self._app._tracebacks,
                )
                return
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
            # In-band errors (cap overshoots) flip the contextvar; translate
            # to 200 + X-VGI-RPC-Error: true to match the documented contract.
            _set_http_status(resp, _current_response_status.get())
        finally:
            _current_response_status.reset(status_token)
            _current_response_budget.reset(budget_token)


def _parse_upload_entries(kwargs: dict[str, object], *, max_upload_bytes: int | None) -> list[tuple[int, str]]:
    """Validate an ``__upload_url__`` v2 request and return its ``(size, sha256)`` entries.

    Args:
        kwargs: The decoded request parameters.
        max_upload_bytes: The advertised ``VGI-Max-Upload-Bytes``, or ``None``.

    Returns:
        One ``(size, sha256_hex)`` per requested upload, in request order.

    Raises:
        ValueError: On the retired v1 ``count`` shape, a missing ``uploads``
            parameter, 0 or more than 100 entries, a ``size`` below 1 or
            above *max_upload_bytes*, or a ``sha256`` that is not 64
            lowercase hex characters.  The server answers 400 with this
            message before any URL is minted.

    """
    if "count" in kwargs or "uploads" not in kwargs:
        raise ValueError(
            f"{_UPLOAD_URL_METHOD} request uses the retired count shape: this server requires "
            f"{UPLOAD_URL_HEADER}: {UPLOAD_URL_PROTOCOL_VERSION} and an 'uploads' parameter "
            "(list<struct<size: int64, sha256: utf8>>, one entry per upload)"
        )
    raw = kwargs["uploads"]
    if not isinstance(raw, list) or not 1 <= len(raw) <= _MAX_UPLOAD_URL_COUNT:
        got = len(raw) if isinstance(raw, list) else "null"
        raise ValueError(f"{_UPLOAD_URL_METHOD} 'uploads' must have 1-{_MAX_UPLOAD_URL_COUNT} entries, got {got}")
    entries: list[tuple[int, str]] = []
    for i, item in enumerate(raw):
        size = item.get("size") if isinstance(item, dict) else None
        sha256_hex = item.get("sha256") if isinstance(item, dict) else None
        if isinstance(size, bool) or not isinstance(size, int) or size < 1:
            raise ValueError(f"{_UPLOAD_URL_METHOD} uploads[{i}].size must be an integer >= 1, got {size!r}")
        if not is_sha256_hex(sha256_hex):
            raise ValueError(f"{_UPLOAD_URL_METHOD} uploads[{i}].sha256 must be 64 lowercase hex characters")
        assert isinstance(sha256_hex, str)
        if max_upload_bytes is not None and size > max_upload_bytes:
            raise ValueError(
                f"{_UPLOAD_URL_METHOD} uploads[{i}].size {size} exceeds {MAX_UPLOAD_BYTES_HEADER} ({max_upload_bytes})"
            )
        entries.append((size, sha256_hex))
    return entries


def _upload_headers(headers: Mapping[str, str]) -> list[tuple[str, str]]:
    """Return a provider's PUT headers as map entries, minus ``Content-Length``.

    The client sends exactly the declared size with a plain
    ``Content-Length`` of its own, so a provider that lists one (the
    vendor's ``required_headers`` does) must not hand the client a second.
    """
    return [(k, v) for k, v in headers.items() if k.lower() != "content-length"]


class _UploadUrlResource:
    """Falcon resource for upload URL generation: ``POST {prefix}/__upload_url__/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Handle upload URL generation requests."""
        try:
            budget = _resolve_response_budget(
                req,
                server_limit_bytes=self._app._max_response_bytes,
                server_preferred_bytes=self._app._preferred_response_bytes,
            )
            _check_content_type(req)
            # Route is only registered when upload_url_provider is set.
            provider = self._app._upload_url_provider
            assert provider is not None

            # Read request using standard wire protocol
            try:
                ipc_method, kwargs = _read_request(_get_request_stream(req), self._app._server.ipc_validation)
                if ipc_method != _UPLOAD_URL_METHOD:
                    raise TypeError(f"Method mismatch: expected '{_UPLOAD_URL_METHOD}', got '{ipc_method}'")
            except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError, ProtocolError) as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc
            except Exception as exc:
                # Same reasoning as the unary/stream guards: an unclassified
                # failure while reading the request must still leave as an RPC
                # error with a type and a message, not as Falcon's default page.
                raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

            try:
                uploads = _parse_upload_entries(kwargs, max_upload_bytes=self._app._max_upload_bytes)
            except ValueError as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                server_id=self._app._server.server_id,
                include_traceback=self._app._tracebacks,
            )
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
        error_code = ""
        _upload_exc: BaseException | None = None
        try:
            with new_ipc_stream(resp_buf, _UPLOAD_URL_SCHEMA) as writer:
                sink.flush_contents(writer, _UPLOAD_URL_SCHEMA)
                try:
                    urls = [provider.generate_upload_url(size, sha256_hex) for size, sha256_hex in uploads]
                    # One sealed argument reference per entry, minted from the
                    # *claimed* (size, sha256) under the same AuthContext this
                    # request was authenticated with -- the one a later call
                    # presenting the ref is authenticated with too.
                    refs: list[bytes] = []
                    for (size, sha256_hex), url in zip(uploads, urls, strict=True):
                        if not url.object_id:
                            raise RuntimeError(
                                f"{type(provider).__name__}.generate_upload_url returned no object_id "
                                "(required since vgi-rpc protocol 3.4)"
                            )
                        refs.append(
                            mint_argument_ref(
                                self._app._token_key, auth, size=size, sha256=sha256_hex, object_id=url.object_id
                            )
                        )
                    result_batch = pa.RecordBatch.from_pydict(
                        {
                            "upload_url": [u.upload_url for u in urls],
                            "download_url": [u.download_url for u in urls],
                            "expires_at": [u.expires_at for u in urls],
                            "upload_headers": [_upload_headers(u.headers) for u in urls],
                            "object_id": [u.object_id for u in urls],
                            "ref": refs,
                        },
                        schema=_UPLOAD_URL_SCHEMA,
                    )
                    writer.write_batch(result_batch)
                except Exception as exc:
                    _upload_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, _UPLOAD_URL_METHOD, server_id, exc)
                    error_code = error_code_of(exc).value
                    _write_error_batch(
                        writer, _UPLOAD_URL_SCHEMA, exc, server_id=server_id, include_traceback=self._app._tracebacks
                    )
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
                # A framework endpoint owned by no protocol: the spec says these
                # log the server's primary, so this one is correct as-is.
                protocol_hash=self._app._server.protocol_hash,
                error_code=error_code,
                error_message=_truncate_error_message(_upload_exc),
            )

        resp_buf.seek(0)
        if http_status == HTTPStatus.OK:
            resp_buf, oversized = _strict_response_stream(
                resp_buf,
                method=_UPLOAD_URL_METHOD,
                limit=budget.response_limit_bytes,
                server_id=server_id,
                include_traceback=self._app._tracebacks,
            )
            if oversized:
                http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.stream = resp_buf
        _set_http_status(resp, http_status)
