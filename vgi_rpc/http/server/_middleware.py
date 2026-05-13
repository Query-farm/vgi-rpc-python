# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Falcon middlewares used by the HTTP server.

Each middleware is a thin wrapper around Falcon's ``process_request`` /
``process_response`` hooks.  They are composed by
``make_wsgi_app`` to provide:

* request drain (``_DrainRequestMiddleware``)
* per-request correlation IDs (``_RequestIdMiddleware``)
* access-log contextvar reset (``_AccessLogContextMiddleware``)
* authentication + transport metadata (``_AuthMiddleware``)
* transparent zstd compression (``_CompressionMiddleware``)
* CORS ``Access-Control-Max-Age`` (``_CorsMaxAgeMiddleware``)
* capability headers (``_CapabilitiesMiddleware``)
"""

from __future__ import annotations

import contextlib
import logging
import types as _types
from collections.abc import Callable
from io import BytesIO, IOBase
from typing import Any

import falcon

from vgi_rpc.rpc import (
    AuthContext,
    RpcServer,
    _current_request_id,
    _current_transport,
    _generate_request_id,
    _TransportContext,
)
from vgi_rpc.rpc._common import _ANONYMOUS, TransportKind, _current_request_batch, _current_stream_id

from .._common import _ARROW_CONTENT_TYPE, _decompress_body

_logger = logging.getLogger("vgi_rpc.http")

_REQUEST_ID_HEADER = "X-Request-ID"


class _TransportNotifyMiddleware:
    """Fires :meth:`RpcServer._notify_transport` once per process, on the first request.

    HTTP transport binding is announced lazily rather than at
    ``make_wsgi_app`` time so that pre-fork servers (gunicorn, uwsgi)
    correctly fire the worker's ``on_serve_start`` hook in each child
    process — not in the master.  After the first request the check
    becomes a single attribute load.

    Thread-safety is delegated to ``_notify_transport`` (lock-protected).
    """

    __slots__ = ("_server",)

    def __init__(self, server: RpcServer) -> None:
        """Hold a reference to the server whose hook we fire."""
        self._server = server

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Bind the server to ``HTTP`` on the first request handled here."""
        if self._server.transport_kind is None:
            self._server._notify_transport(TransportKind.HTTP, frozenset())


class _MaxRequestBytesMiddleware:
    """Falcon middleware enforcing the advertised ``max_request_bytes`` cap.

    Returns ``413 Payload Too Large`` for any RPC route whose request
    body exceeds the configured limit.  Skipped for capability /
    upload-URL routes whose bodies are intrinsically small and not
    user-controlled.  This is the server-side enforcement counterpart
    to the ``VGI-Max-Request-Bytes`` capability header — when the
    client externalizes via ``__upload_url__/init`` instead of POSTing
    inline, the request body shrinks to a pointer batch (small) and
    sails through.
    """

    __slots__ = ("_exempt_prefixes", "_max_bytes")

    def __init__(self, max_bytes: int, exempt_prefixes: tuple[str, ...]) -> None:
        self._max_bytes = max_bytes
        self._exempt_prefixes = exempt_prefixes

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Reject oversized inline request bodies with HTTP 413."""
        path = req.path
        for prefix in self._exempt_prefixes:
            if path == prefix or path.startswith(prefix + "/"):
                return
        cl = req.content_length
        if cl is not None and cl > self._max_bytes:
            raise falcon.HTTPPayloadTooLarge(
                title="Request body exceeds max_request_bytes",
                description=(
                    f"Request body of {cl} bytes exceeds the server's advertised "
                    f"max_request_bytes={self._max_bytes}.  Use the upload-URL "
                    f"flow (__upload_url__/init) to externalize large inputs."
                ),
            )


class _DrainRequestMiddleware:
    """Falcon middleware that drains unconsumed request body data.

    Some HTTP servers — notably Cloudflare Workers — require the entire
    request body to be consumed before the response can be sent, raising
    ``TypeError("Can't read from request stream after response has been
    sent.")`` otherwise.  This middleware ensures any unread request data
    is discarded after the resource handler completes.
    """

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Drain any unread request body data."""
        with contextlib.suppress(Exception):
            req.bounded_stream.read()


class _RequestIdMiddleware:
    """Falcon middleware that sets a per-request correlation ID.

    Reads ``X-Request-ID`` from the incoming request header or generates a
    new 16-char hex ID.  The value is stored in ``req.context.request_id``,
    set on the ``_current_request_id`` contextvar, and echoed back on the
    response as the ``X-Request-ID`` header.
    """

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Set request ID from header or generate one; populate contextvar."""
        request_id = req.get_header(_REQUEST_ID_HEADER) or _generate_request_id()
        req.context.request_id = request_id
        req.context.request_id_token = _current_request_id.set(request_id)

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Echo request ID on response header and reset contextvar."""
        request_id = getattr(req.context, "request_id", None)
        if request_id is not None:
            resp.set_header(_REQUEST_ID_HEADER, request_id)
        token = getattr(req.context, "request_id_token", None)
        if token is not None:
            _current_request_id.reset(token)


class _AccessLogContextMiddleware:
    """Falcon middleware that resets access-log contextvars between requests.

    Prevents cross-request leakage of ``_current_request_batch`` and
    ``_current_stream_id`` when threads are reused by WSGI thread-pool
    servers (waitress, gunicorn).
    """

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Clear access-log contextvars at the start of each request."""
        req.context._rb_token = _current_request_batch.set(None)
        req.context._sid_token = _current_stream_id.set("")

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Reset access-log contextvars to their pre-request state."""
        token = getattr(req.context, "_rb_token", None)
        if token is not None:
            _current_request_batch.reset(token)
        token = getattr(req.context, "_sid_token", None)
        if token is not None:
            _current_stream_id.reset(token)


def _build_transport_metadata(req: falcon.Request) -> dict[str, Any]:
    """Extract per-request HTTP transport metadata (remote_addr, user_agent, cookies)."""
    md: dict[str, Any] = {}
    if req.remote_addr:
        md["remote_addr"] = req.remote_addr
    ua = req.user_agent
    if ua:
        md["user_agent"] = ua
    if req.cookies:
        md["cookies"] = _types.MappingProxyType(dict(req.cookies))
    return md


class _AuthMiddleware:
    """Falcon middleware: populates transport metadata and optionally authenticates.

    Always populates ``_TransportContext`` in ``_current_transport`` for
    the duration of the request so that ``CallContext`` picks up transport
    metadata (``remote_addr``, ``user_agent``, ``cookies``).  When an
    ``authenticate`` callback is provided, also populates ``AuthContext``
    on the same transport context.

    The ``authenticate`` callback is expected to raise ``ValueError`` (bad
    credentials) or ``PermissionError`` (forbidden) on failure.  Other
    exceptions propagate as 500s so that bugs in the callback are not
    silently swallowed as 401s.
    """

    __slots__ = ("_authenticate", "_exempt_prefixes", "_on_auth_failure", "_www_authenticate")

    def __init__(
        self,
        authenticate: Callable[[falcon.Request], AuthContext] | None,
        www_authenticate: str | None = None,
        on_auth_failure: Callable[[str | None, str], None] | None = None,
        exempt_prefixes: tuple[str, ...] = (),
    ) -> None:
        self._authenticate = authenticate
        self._www_authenticate = www_authenticate
        self._on_auth_failure = on_auth_failure
        self._exempt_prefixes = exempt_prefixes

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Authenticate (if configured) and populate the transport contextvar.

        Only ``ValueError`` and ``PermissionError`` from the ``authenticate``
        callback are caught and mapped to HTTP 401.  Other exceptions
        propagate as 500 so that bugs surface loudly rather than masquerade
        as auth failures.

        CORS preflight ``OPTIONS`` requests and well-known paths
        (``/.well-known/``) are exempt from authentication so that browsers
        can complete the preflight handshake without credentials and
        clients can discover OAuth metadata before authenticating.  Exempt
        paths still get transport metadata populated.
        """
        transport_metadata = _build_transport_metadata(req)
        exempt = (
            req.method == "OPTIONS"
            or req.path.startswith("/.well-known/")
            or any(req.path.startswith(pfx) for pfx in self._exempt_prefixes)
        )
        if self._authenticate is None or exempt:
            tc = _TransportContext(auth=_ANONYMOUS, transport_metadata=transport_metadata)
            req.context.transport_token = _current_transport.set(tc)
            return
        try:
            auth = self._authenticate(req)
        except (ValueError, PermissionError) as exc:
            _logger.warning(
                "Auth failure from %s: %s",
                req.remote_addr,
                exc,
                extra={
                    "remote_addr": req.remote_addr or "",
                    "error_type": type(exc).__name__,
                    "auth_error": str(exc),
                },
            )
            if self._on_auth_failure is not None:
                self._on_auth_failure(req.remote_addr, type(exc).__name__)
            challenges = [self._www_authenticate] if self._www_authenticate else None
            raise falcon.HTTPUnauthorized(description=str(exc), challenges=challenges) from exc
        tc = _TransportContext(auth=auth, transport_metadata=transport_metadata)
        req.context.transport_token = _current_transport.set(tc)

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Reset the transport contextvar after each request."""
        token = getattr(req.context, "transport_token", None)
        if token is not None:
            _current_transport.reset(token)


class _CompressionMiddleware:
    """Falcon middleware for transparent zstd request/response compression.

    On requests: if ``Content-Encoding: zstd`` is present, the request body
    is decompressed and stored in ``req.context.decompressed_stream`` so that
    resource handlers read uncompressed data.

    On responses: if the client sent ``Accept-Encoding`` containing ``zstd``
    and the response has Arrow content, the response body is compressed and
    ``Content-Encoding: zstd`` is set.

    The decompression cap (``max_decompressed_request_bytes``) is checked
    *before* allocation so a tiny compressed body claiming a huge
    decompressed size cannot OOM the server (decompression-bomb DoS).
    """

    __slots__ = ("_level", "_max_decompressed_bytes")

    def __init__(self, level: int, *, max_decompressed_bytes: int | None = None) -> None:
        self._level = level
        self._max_decompressed_bytes = max_decompressed_bytes

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Decompress zstd request bodies; record client Accept-Encoding."""
        # Check if the client accepts zstd responses (standard or custom header)
        accept_encoding = req.get_header("Accept-Encoding") or ""
        custom_accept = req.get_header("X-VGI-Accept-Encoding") or ""
        req.context.client_accepts_zstd = "zstd" in accept_encoding or "zstd" in custom_accept
        req.context.use_custom_encoding_header = "zstd" in custom_accept

        # Decompress request body if Content-Encoding: zstd
        content_encoding = req.get_header("Content-Encoding") or ""
        if "zstd" in content_encoding:
            try:
                compressed = req.bounded_stream.read()
                decompressed = _decompress_body(compressed, max_output_size=self._max_decompressed_bytes)
                req.context.decompressed_stream = BytesIO(decompressed)
            except Exception as exc:
                raise falcon.HTTPBadRequest(
                    title="Decompression Error",
                    description=f"Failed to decompress zstd request body: {exc}",
                ) from exc

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Compress Arrow IPC response bodies with zstd when client supports it.

        Uses zstd's streaming compressor to avoid materializing the full
        uncompressed body as an intermediate ``bytes`` object. The previous
        ``stream.read()`` + ``_compress_body(body)`` path held the response
        BytesIO's internal buffer, the ``body`` bytes copy returned by
        ``read()``, AND the compressed output simultaneously — peak ~2x the
        body size per concurrent compression. For an N-thread WSGI server
        emitting M-byte Arrow IPC responses, that's ~2 x N x M of avoidable
        peak; under typical kafka_consume tuning (64 MiB/tick, 16 threads)
        it's ~2 GiB on the 2 GiB Fly VM. The streaming path keeps only one
        copy in memory (the compressed output) plus a 64 KiB read buffer.
        """
        if not getattr(req.context, "client_accepts_zstd", False):
            return
        if resp.content_type != _ARROW_CONTENT_TYPE:
            return
        stream = resp.stream
        if stream is None:
            return
        if not isinstance(stream, IOBase):
            return

        # Determine remaining bytes without buffering: seek-to-end / tell /
        # seek-back. Required because zstd's streaming compressor needs the
        # content_size up front to write a self-describing frame header
        # (``ZstdDecompressor.decompress`` rejects frames without it). For
        # non-seekable streams we fall back to the buffer-then-compress
        # path so frame format stays identical to the historical behaviour.
        size: int | None = None
        if hasattr(stream, "seek") and hasattr(stream, "tell"):
            cur = stream.tell()
            try:
                stream.seek(0, 2)
                end = stream.tell()
                stream.seek(cur)
                size = end - cur
            except (OSError, ValueError):
                size = None
        if size == 0:
            return

        import zstandard

        cctx = zstandard.ZstdCompressor(level=self._level)
        if size is None:
            # Non-seekable: fall back to one-shot compression so the frame
            # carries content_size for ``ZstdDecompressor.decompress``.
            # This is the rare path; seekable streams (BytesIO from the
            # framework's response builders) hit the streaming path above.
            body = stream.read()
            if not body:
                return
            compressed = cctx.compress(body)
            del body
        else:
            # Streaming path: avoid the duplicate ``body = stream.read()``
            # bytes object. ``stream_writer(size=N)`` writes the content
            # size into the frame header so the eager decompressor still
            # accepts the frame. ``closefd=False`` keeps ``out`` open.
            out = BytesIO()
            with cctx.stream_writer(out, size=size, closefd=False) as writer:
                while True:
                    chunk = stream.read(65536)
                    if not chunk:
                        break
                    writer.write(chunk)
            compressed = out.getvalue()
            del out

        resp.data = compressed
        resp.stream = None
        if getattr(req.context, "use_custom_encoding_header", False):
            resp.set_header("X-VGI-Content-Encoding", "zstd")
        else:
            resp.set_header("Content-Encoding", "zstd")


class _CorsMaxAgeMiddleware:
    """Falcon middleware that sets ``Access-Control-Max-Age`` on OPTIONS responses."""

    __slots__ = ("_max_age",)

    def __init__(self, max_age: int) -> None:
        self._max_age = str(max_age)

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Set Access-Control-Max-Age on preflight OPTIONS responses."""
        if req.method == "OPTIONS":
            resp.set_header("Access-Control-Max-Age", self._max_age)


class _CapabilitiesMiddleware:
    """Falcon middleware that sets capability headers on every response.

    On OPTIONS responses (which clients use as the discovery target via
    ``OPTIONS /health``), also sets ``Cache-Control: max-age=...`` so
    well-behaved clients cache the values for the advertised TTL and
    refresh on expiry.  Non-OPTIONS responses still carry the headers
    (cheap) so clients picking them up from any response also work.
    """

    __slots__ = ("_cache_max_age_seconds", "_headers")

    def __init__(self, headers: dict[str, str], cache_max_age_seconds: int = 300) -> None:
        self._headers = headers
        self._cache_max_age_seconds = cache_max_age_seconds

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Set capability headers on every response, plus Cache-Control on OPTIONS."""
        for name, value in self._headers.items():
            resp.set_header(name, value)
        if req.method == "OPTIONS":
            resp.set_header("Cache-Control", f"public, max-age={self._cache_max_age_seconds}")
