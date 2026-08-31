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
* transparent content compression (zstd / gzip, ``_CompressionMiddleware``)
* CORS extras — ``Access-Control-Max-Age`` + ``Cross-Origin-Resource-Policy``
  (``_CorsExtrasMiddleware``)
* capability headers (``_CapabilitiesMiddleware``)
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
import types as _types
from collections.abc import Callable, Iterable
from io import BytesIO, IOBase
from typing import Any, NoReturn

import falcon
import pyarrow as pa

from vgi_rpc.external import _current_externalized_bytes
from vgi_rpc.rpc import (
    AuthContext,
    PeerAuthenticationPolicy,
    PeerEvidenceSet,
    PeerIdentityProvider,
    PeerIdentityResult,
    PeerIdentityStatus,
    PeerIdentityUnavailableError,
    PeerResolutionContext,
    RpcServer,
    _current_request_id,
    _current_transport,
    _generate_request_id,
    _TransportContext,
)
from vgi_rpc.rpc._common import (
    _ANONYMOUS,
    TransportKind,
    _access_logger,
    _current_access_sink,
    _current_body_precompressed,
    _current_request_batch,
    _current_request_wire_bytes,
    _current_response_codec,
    _current_stream_id,
)

from .._common import (
    _ARROW_CONTENT_TYPE,
    DecompressionLimitExceeded,
    Encoding,
    available_encodings,
    parse_encoding_list,
)
from .._common import (
    decompress as _decompress_with_encoding,
)
from .._unauthorized import AuthFailure, AuthReason, AuthUnavailableError, classify_auth_failure

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

    Returns ``413 Payload Too Large`` for any RPC route whose request body
    exceeds the configured limit, including the upload-URL control route.
    Requests with ``Content-Length`` are rejected before reading.  Chunked
    requests are read once with a one-byte sentinel and the bounded result is
    handed to the normal request/decompression path through request context.
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
            self._raise_too_large(cl)
        if cl is None:
            body = req.bounded_stream.read(self._max_bytes + 1)
            if len(body) > self._max_bytes:
                self._raise_too_large(len(body))
            req.context.capped_request_body = body

    def _raise_too_large(self, size: int) -> NoReturn:
        """Raise Falcon's standardized 413 response."""
        raise falcon.HTTPContentTooLarge(
            title="Request body exceeds max_request_bytes",
            description=(
                f"Request body of at least {size} bytes exceeds the server's advertised "
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


def _request_source_endpoint(req: falcon.Request) -> str | None:
    """Return the immediate HTTP socket endpoint without changing trust input.

    Reverse-proxy allowlists compare ``req.remote_addr`` as a normalized IP,
    while Tailscale LocalAPI WhoIs requires the source port as well. WSGI
    exposes that port separately in ``REMOTE_PORT``.
    """
    host = req.remote_addr
    raw_port = req.env.get("REMOTE_PORT")
    if not host or not isinstance(raw_port, str):
        return None
    try:
        port = int(raw_port)
    except ValueError:
        return None
    if not 1 <= port <= 65_535:
        return None
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


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

    A failure is classified into an :class:`AuthReason` and stashed on
    ``req.context`` for the error serializer, which turns it into the
    standardized 401 of ``docs/unauthorized-spec.md``.  Raising
    :class:`vgi_rpc.http.AuthFailure` picks the code explicitly; a bare
    ``ValueError`` lands on the unclassified fallback rather than being
    guessed at from its message text.
    """

    __slots__ = (
        "_authenticate",
        "_exempt_prefixes",
        "_on_auth_failure",
        "_peer_authentication_policy",
        "_peer_identity_providers",
        "_peer_provider_slots",
        "_peer_resolution_timeout",
        "_service_name",
        "_www_authenticate",
    )

    def __init__(
        self,
        authenticate: Callable[[falcon.Request], AuthContext] | None,
        www_authenticate: str | None = None,
        on_auth_failure: Callable[[str | None, str], None] | None = None,
        exempt_prefixes: tuple[str, ...] = (),
        peer_identity_providers: tuple[PeerIdentityProvider, ...] = (),
        peer_authentication_policy: PeerAuthenticationPolicy | None = None,
        service_name: str | None = None,
        peer_resolution_timeout: float = 5.0,
        peer_provider_concurrency: int = 64,
    ) -> None:
        self._authenticate = authenticate
        self._www_authenticate = www_authenticate
        self._on_auth_failure = on_auth_failure
        self._exempt_prefixes = exempt_prefixes
        self._peer_identity_providers = peer_identity_providers
        self._peer_authentication_policy = peer_authentication_policy
        self._service_name = service_name
        if peer_resolution_timeout <= 0:
            raise ValueError("peer_resolution_timeout must be positive")
        if peer_provider_concurrency < len(peer_identity_providers):
            raise ValueError("peer_provider_concurrency must accommodate every configured provider")
        provider_names = tuple(provider.provider for provider in peer_identity_providers)
        if any(not name for name in provider_names) or len(set(provider_names)) != len(provider_names):
            raise ValueError("peer identity providers must have unique non-empty names")
        self._peer_resolution_timeout = peer_resolution_timeout
        self._peer_provider_slots = threading.BoundedSemaphore(peer_provider_concurrency)

    def _resolve_peer_evidence(self, context: PeerResolutionContext) -> PeerEvidenceSet:
        """Resolve all providers concurrently within one hard request deadline.

        Provider callbacks are trusted extension code and Python cannot stop a
        running callback.  Daemon threads plus a bounded semaphore ensure that
        a callback which ignores its deadline cannot block process shutdown or
        cause unbounded thread creation.  Its request still returns at the
        configured deadline; the occupied slot is released only when the
        callback actually exits.
        """
        providers = self._peer_identity_providers
        if not providers:
            return PeerEvidenceSet.empty()

        outcomes: queue.Queue[tuple[int, object]] = queue.Queue(maxsize=len(providers))
        resolved: list[object | None] = [None] * len(providers)
        started = 0
        for index, provider in enumerate(providers):
            if not self._peer_provider_slots.acquire(blocking=False):
                resolved[index] = PeerIdentityResult(provider.provider, PeerIdentityStatus.UNAVAILABLE)
                continue

            def invoke(position: int = index, resolver: PeerIdentityProvider = provider) -> None:
                try:
                    outcome: object = resolver(context)
                except Exception as exc:  # delivered to the request thread
                    outcome = exc
                finally:
                    self._peer_provider_slots.release()
                outcomes.put((position, outcome))

            threading.Thread(
                target=invoke,
                name=f"vgi-peer-identity-{index}",
                daemon=True,
            ).start()
            started += 1

        def record_outcome(index: int, outcome: object) -> None:
            if isinstance(outcome, Exception):
                # Providers are application extension points. Preserve only
                # the failure class; provider-controlled text must not reach
                # responses, logs, or telemetry.
                if isinstance(outcome, PeerIdentityUnavailableError):
                    resolved[index] = PeerIdentityResult(providers[index].provider, PeerIdentityStatus.UNAVAILABLE)
                    return
                if isinstance(outcome, (ValueError, PermissionError)):
                    raise PermissionError("peer identity evidence rejected") from outcome
                raise RuntimeError("peer identity provider failed") from None
            provider = providers[index]
            if not isinstance(outcome, PeerIdentityResult) or outcome.provider != provider.provider:
                raise RuntimeError("peer identity provider result mismatch")
            resolved[index] = outcome

        for _ in range(started):
            remaining = (context.deadline or 0.0) - time.monotonic()
            if remaining <= 0:
                break
            try:
                index, outcome = outcomes.get(timeout=remaining)
            except queue.Empty:
                break
            record_outcome(index, outcome)

        # Do not downgrade a rejection that completed on the deadline edge.
        # Harvest every already-queued outcome before marking only genuinely
        # incomplete providers unavailable.
        while True:
            try:
                index, outcome = outcomes.get_nowait()
            except queue.Empty:
                break
            record_outcome(index, outcome)

        results = []
        for index, outcome in enumerate(resolved):
            if outcome is None:
                outcome = PeerIdentityResult(providers[index].provider, PeerIdentityStatus.UNAVAILABLE)
            assert isinstance(outcome, PeerIdentityResult)
            results.append(outcome)
        return PeerEvidenceSet.from_results(tuple(results))

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
        path = req.path
        exempt = (
            req.method == "OPTIONS"
            or path.startswith("/.well-known/")
            or any(path == pfx or path.startswith(pfx + "/") for pfx in self._exempt_prefixes)
        )
        if exempt:
            tc = _TransportContext(
                auth=_ANONYMOUS,
                transport_metadata=transport_metadata,
                peer_evidence=PeerEvidenceSet.empty(),
            )
            req.context.transport_token = _current_transport.set(tc)
            return
        try:
            missing_credential: AuthFailure | None = None
            try:
                auth = self._authenticate(req) if self._authenticate is not None else _ANONYMOUS
            except AuthFailure as exc:
                reason = classify_auth_failure(exc)
                if self._peer_authentication_policy is None or reason is not AuthReason.MISSING_CREDENTIAL:
                    raise
                missing_credential = exc
                auth = _ANONYMOUS
            resolution_context = PeerResolutionContext(
                transport="http",
                immediate_peer=req.remote_addr,
                source_endpoint=_request_source_endpoint(req),
                authority=req.host,
                service_name=self._service_name,
                headers={name: (value,) for name, value in req.headers.items()},
                metadata=transport_metadata,
                deadline=time.monotonic() + self._peer_resolution_timeout,
            )
            evidence = self._resolve_peer_evidence(resolution_context)
            transport_metadata["peer_identity_status"] = ",".join(
                f"{provider}:{status.value}" for provider, status in sorted(evidence.provider_status.items())
            )
            transport_metadata["peer_identity_sources"] = ",".join(
                sorted(
                    {
                        f"{identity.provider}:{identity.evidence_source}:{identity.assurance.value}"
                        for identity in evidence.identities
                    }
                )
            )
            if self._peer_authentication_policy is not None:
                auth = self._peer_authentication_policy(evidence, auth)
            if missing_credential is not None and not auth.authenticated:
                raise missing_credential
        except PeerIdentityUnavailableError as exc:
            _logger.warning(
                "Peer identity unavailable",
                extra={"remote_addr": req.remote_addr or "", "error_type": type(exc).__name__},
            )
            raise falcon.HTTPServiceUnavailable(description="peer identity unavailable", retry_after=5) from exc
        except AuthUnavailableError as exc:
            # Not a rejection: the authority could not be reached. A 401 here
            # tells every caller to re-authenticate against a service that is
            # simply down, and invites them to negative-cache an outage.
            _logger.warning(
                "Authentication unavailable",
                extra={"remote_addr": req.remote_addr or "", "error_type": type(exc).__name__},
            )
            raise falcon.HTTPServiceUnavailable(
                description="authentication authority unavailable",
                retry_after=exc.retry_after,
            ) from exc
        except (ValueError, PermissionError) as exc:
            # Classify before raising so the error serializer can put a code on
            # the response. It reads req.context rather than re-deriving from
            # the Falcon error, which by then has lost the original type.
            reason = classify_auth_failure(exc)
            req.context.vgi_auth_reason = reason
            _logger.warning(
                "Auth failure from %s",
                req.remote_addr,
                extra={
                    "remote_addr": req.remote_addr or "",
                    "error_type": type(exc).__name__,
                    "auth_error": "authentication rejected",
                    "auth_reason": reason.value,
                },
            )
            if self._on_auth_failure is not None:
                self._on_auth_failure(req.remote_addr, type(exc).__name__)
            challenges = [self._www_authenticate] if self._www_authenticate else None
            raise falcon.HTTPUnauthorized(description="authentication rejected", challenges=challenges) from exc
        tc = _TransportContext(auth=auth, transport_metadata=transport_metadata, peer_evidence=evidence)
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


class _AccessLogEgressMiddleware:
    """Measures what actually crossed the network, then emits the access log.

    Access-log records used to be written by the handler that produced the
    response, which is too early to know what the response weighed: response
    compression runs afterwards, in :class:`_CompressionMiddleware`. A record
    written at handler time can only report the uncompressed body, which is
    the wrong number for anything that costs money.

    So this installs a sink for the request's records, lets the handlers fill
    it, and emits once the final body exists — stamping ``response_bytes``
    on the way out. Registered first in the middleware list, which under
    Falcon means its ``process_response`` runs last, after compression has
    replaced the body.

    Three numbers, three different questions:

    * ``request_bytes`` / ``response_bytes`` — what crossed the wire, after
      compression. This is the egress figure.
    * ``input_bytes`` / ``output_bytes`` — logical Arrow buffers, i.e. what
      the worker processed. Unaffected by compression.
    * ``externalized_bytes`` — uploaded to external storage, which never
      touches the HTTP body at all and is routinely the largest of the three.

    A crash between handler and response loses that request's records. The
    alternative — emit early and under-report — trades a rare loss for a
    permanently wrong number.
    """

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Install the record sink and capture the request's on-wire size."""
        req.context._access_sink_token = _current_access_sink.set([])
        # content_length is the body as received: before decompression, and
        # therefore the number of bytes the peer actually sent.
        req.context._req_bytes_token = _current_request_wire_bytes.set(req.content_length or 0)
        req.context._ext_bytes_token = _current_externalized_bytes.set(0)

    @staticmethod
    def _response_bytes(resp: falcon.Response) -> int | None:
        """Return the final body size, or ``None`` when it cannot be known.

        A streamed response has no length until it has been consumed, and
        guessing one would be worse than omitting the field.
        """
        if resp.data is not None:
            return len(resp.data)
        if resp.text is not None:
            return len(resp.text.encode("utf-8"))
        if resp.content_length is not None:
            return int(resp.content_length)
        return None

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Stamp egress sizes onto the request's records and emit them."""
        sink = _current_access_sink.get()
        for token_attr, var in (
            ("_access_sink_token", _current_access_sink),
            ("_req_bytes_token", _current_request_wire_bytes),
            ("_ext_bytes_token", _current_externalized_bytes),
        ):
            token = getattr(req.context, token_attr, None)
            if token is not None:
                var.reset(token)
        if not sink:
            return
        response_bytes = self._response_bytes(resp)
        for message, extra in sink:
            if response_bytes is not None:
                extra["response_bytes"] = response_bytes
            try:
                _access_logger.info("%s", message, extra=extra)
            except Exception:  # pragma: no cover - a log must not fail a request
                _logger.debug("Access log emission failed", exc_info=True)


class _CompressionMiddleware:
    """Falcon middleware for transparent codec request/response compression.

    The two directions are **independent capabilities**, configured
    separately.  Decoding a request costs nothing to offer and is what a
    peer needs from us; encoding a response is a server policy an operator
    may want off (already-compressed transport, CPU budget, a proxy that
    re-compresses).  Conflating them meant a server with response
    compression disabled could not decode a compressed request at all —
    it read the compressed bytes as Arrow IPC and 500'd.

    On requests: if ``Content-Encoding`` names a codec in
    ``decode_encodings``, the request body is decompressed and stored in
    ``req.context.decompressed_stream`` so resource handlers read
    uncompressed data.  An unknown or non-accepted codec triggers 415.

    On responses: pick the first codec from the client's
    ``Accept-Encoding`` / ``X-VGI-Accept-Encoding`` that we can produce
    (``encode_levels`` keys), compress an Arrow IPC body with it, and stamp
    the chosen codec on ``Content-Encoding`` (or ``X-VGI-Content-Encoding``
    if the client used the custom header).  No overlap → uncompressed.  An
    empty ``encode_levels`` disables response compression outright while
    leaving request decoding untouched.

    The decompression cap (``max_decompressed_bytes``) bounds output to
    block decompression-bomb DoS regardless of codec.  zstd uses its
    frame-header pre-check; gzip uses a bounded streaming loop.
    """

    __slots__ = ("_decode", "_levels", "_max_decompressed_bytes")

    def __init__(
        self,
        encode_levels: dict[Encoding, int] | int | None = None,
        *,
        decode_encodings: Iterable[Encoding] | None = None,
        max_decompressed_bytes: int | None = None,
    ) -> None:
        if isinstance(encode_levels, int):
            # Back-compat: old factories passed a single int (zstd level).
            encode_levels = {Encoding.ZSTD: encode_levels}
        elif encode_levels is None:
            encode_levels = {}
        # Only keep codecs the runtime can actually produce/consume.
        runtime = set(available_encodings())
        self._levels: dict[Encoding, int] = {enc: lvl for enc, lvl in encode_levels.items() if enc in runtime}
        # Request decoding defaults to whatever we can encode, so callers
        # that only pass levels keep the historical behaviour.
        decodable = tuple(encode_levels) if decode_encodings is None else tuple(decode_encodings)
        self._decode: tuple[Encoding, ...] = tuple(enc for enc in decodable if enc in runtime)
        self._max_decompressed_bytes = max_decompressed_bytes

    def _pick_response_encoding(self, req: falcon.Request) -> tuple[Encoding | None, bool]:
        """Return (chosen, used_custom_header) for the response codec.

        Returns ``(None, _)`` when no overlap exists between what the
        client offered and what we can produce.
        """
        standard = parse_encoding_list(req.get_header("Accept-Encoding") or "")
        custom = parse_encoding_list(req.get_header("X-VGI-Accept-Encoding") or "")
        # Honour the client's preference order, with VGI's own header first:
        # X-VGI-Accept-Encoding wins over the generic Accept-Encoding both in
        # choosing the codec and in deciding which response header to stamp.
        # HTTP clients (e.g. cpp-httplib, which the DuckDB extension uses) inject
        # their own `Accept-Encoding: deflate, gzip, br, zstd` listing gzip before
        # zstd; walking that list first picks gzip and silently ignores the
        # zstd-first order VGI states in X-VGI-Accept-Encoding. gzip compression
        # dominated large HTTP responses (432ms vs ~40ms of zstd for 200MB of
        # Arrow bodies -- a 4.2x slower round-trip end to end).
        for enc in custom + [e for e in standard if e not in custom]:
            # ``identity`` is always producible, so reaching it first means the
            # client explicitly asked for an uncompressed body — honour that
            # rather than continuing on to a codec listed after it.  No
            # content-encoding header is stamped for identity (see
            # process_response): an untransformed body is just a body.
            if enc is Encoding.IDENTITY:
                return None, False
            if enc in self._levels:
                return enc, enc in custom and enc not in standard
        return None, bool(custom)

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Decompress codec request bodies; record client response codec choice."""
        chosen, used_custom = self._pick_response_encoding(req)
        req.context.response_encoding = chosen
        # Publish it so a streaming producer can compress INTO its IPC stream
        # rather than have this middleware squeeze the finished body — the
        # plaintext then never exists as a whole. Arrow names zstd/gzip the same
        # way this enum does; anything else stays a post-hoc compress here.
        _current_response_codec.set(chosen.value if chosen is not None and chosen.value in ("zstd", "gzip") else None)
        _current_body_precompressed.set(False)
        req.context.use_custom_encoding_header = used_custom

        content_encoding = (req.get_header("Content-Encoding") or "").strip().lower()
        if not content_encoding:
            return
        # Unknown codec → 415; do not fall through to identity, since the
        # body would be garbage to the resource handler.
        req_enc = next((e for e in Encoding if e.value == content_encoding), None)
        if req_enc is None:
            raise falcon.HTTPUnsupportedMediaType(
                title="Unsupported Content-Encoding",
                description=f"Content-Encoding {content_encoding!r} is not supported by this server",
            )
        if req_enc not in self._decode:
            raise falcon.HTTPUnsupportedMediaType(
                title="Unsupported Content-Encoding",
                description=(
                    f"Content-Encoding {content_encoding!r} is not enabled on this server; "
                    f"supported: {', '.join(e.value for e in self._decode) or 'none'}"
                ),
            )
        try:
            compressed = getattr(req.context, "capped_request_body", None)
            if compressed is None:
                compressed = req.bounded_stream.read()
            decompressed = _decompress_with_encoding(req_enc, compressed, max_output_size=self._max_decompressed_bytes)
            # pa.BufferReader rather than BytesIO: Arrow reads through this
            # from C++, and a BytesIO makes it cross back into Python for
            # every read. Measured 26-31% off open_stream+read_batch, with
            # the gap widening as the body grows.
            req.context.decompressed_stream = pa.BufferReader(decompressed)
            # The decompressed body *is* the self-contained IPC stream the
            # access log is specified to carry. Capturing it costs nothing
            # and is exactly what the client sent.
            _current_request_batch.set(decompressed)
        except DecompressionLimitExceeded as exc:
            raise falcon.HTTPContentTooLarge(
                title="Request body exceeds max_request_bytes after decompression",
                description=(
                    f"Decompressed {req_enc.value} request body exceeds the server's advertised "
                    f"max_request_bytes={self._max_decompressed_bytes}."
                ),
            ) from exc
        except Exception as exc:
            raise falcon.HTTPBadRequest(
                title="Decompression Error",
                description=f"Failed to decompress {req_enc.value} request body: {exc}",
            ) from exc

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Compress Arrow IPC response bodies with the negotiated codec.

        zstd uses its streaming ``stream_writer`` to keep the frame's
        content_size header (required by the eager
        ``ZstdDecompressor.decompress`` on the client).  gzip uses
        ``zlib.compressobj(wbits=31)`` — also streaming, no
        content_size analog (gzip's ISIZE footer is mod 2^32 anyway).
        Both paths keep ~64 KiB of read buffer + the compressed output
        in memory; the historical peak-2x problem is avoided.
        """
        encoding: Encoding | None = getattr(req.context, "response_encoding", None)
        if encoding is None:
            return
        if resp.content_type != _ARROW_CONTENT_TYPE:
            return
        if _current_body_precompressed.get():
            # The producer already compressed into its stream; only the headers
            # are left to set. Compressing again would double-encode the body.
            if getattr(req.context, "use_custom_encoding_header", False):
                resp.set_header("X-VGI-Content-Encoding", encoding.value)
            else:
                resp.set_header("Content-Encoding", encoding.value)
            return
        stream = resp.stream
        if stream is None:
            return
        if not isinstance(stream, IOBase):
            return

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

        level = self._levels[encoding]

        if encoding is Encoding.ZSTD:
            import zstandard

            cctx = zstandard.ZstdCompressor(level=level)
            if size is None:
                body = stream.read()
                if not body:
                    return
                compressed = cctx.compress(body)
                del body
            else:
                out = BytesIO()
                with cctx.stream_writer(out, size=size, closefd=False) as writer:
                    while True:
                        chunk = stream.read(65536)
                        if not chunk:
                            break
                        writer.write(chunk)
                compressed = out.getvalue()
                del out
        elif encoding is Encoding.GZIP:
            import zlib

            co = zlib.compressobj(level, zlib.DEFLATED, 31)
            if size is None:
                body = stream.read()
                if not body:
                    return
                compressed = co.compress(body) + co.flush(zlib.Z_FINISH)
                del body
            else:
                pieces: list[bytes] = []
                while True:
                    chunk = stream.read(65536)
                    if not chunk:
                        break
                    out_chunk = co.compress(chunk)
                    if out_chunk:
                        pieces.append(out_chunk)
                tail = co.flush(zlib.Z_FINISH)
                if tail:
                    pieces.append(tail)
                compressed = b"".join(pieces)
        else:  # pragma: no cover — _levels keys are filtered by available_encodings
            # Unreachable in practice: _levels only ever holds codecs from
            # available_encodings(), and IDENTITY is excluded from that set.
            # mypy can't see it, since IDENTITY is a member of the enum.
            return

        resp.data = compressed
        resp.stream = None
        if getattr(req.context, "use_custom_encoding_header", False):
            resp.set_header("X-VGI-Content-Encoding", encoding.value)
        else:
            resp.set_header("Content-Encoding", encoding.value)


class _CorsExtrasMiddleware:
    """Falcon middleware for the CORS headers Falcon's own does not set.

    Two headers, both only meaningful on a server that has opted into CORS,
    which is why this is installed alongside ``falcon.CORSMiddleware``
    rather than unconditionally:

    * ``Access-Control-Max-Age`` on preflight responses, so a browser stops
      re-preflighting every call.  Every RPC call is preflighted -- the
      Arrow content type is not CORS-safelisted -- so without this the
      request count doubles.
    * ``Cross-Origin-Resource-Policy`` on every response.  CORS alone is
      not sufficient for a browser that has opted into cross-origin
      isolation: a page sending ``Cross-Origin-Embedder-Policy:
      require-corp`` has its *own* fetches blocked unless each response
      also carries CORP, and the failure is invisible from the server side.
      A server configured for cross-origin access wants ``cross-origin``;
      the value is settable because a deployment fronted by a same-site
      proxy may legitimately want to narrow it.
    """

    __slots__ = ("_max_age", "_resource_policy")

    def __init__(self, max_age: int | None, resource_policy: str | None) -> None:
        self._max_age = str(max_age) if max_age is not None else None
        self._resource_policy = resource_policy

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Set the preflight cache lifetime and the resource policy."""
        if self._max_age is not None and req.method == "OPTIONS":
            resp.set_header("Access-Control-Max-Age", self._max_age)
        if self._resource_policy is not None:
            resp.set_header("Cross-Origin-Resource-Policy", self._resource_policy)


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
