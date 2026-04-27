# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``make_wsgi_app`` — the Falcon WSGI application factory."""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vgi_rpc.http._oauth import OAuthResourceMetadata

import falcon

from vgi_rpc.external import UploadUrlProvider
from vgi_rpc.rpc import AuthContext, RpcServer

from .._common import (
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    RPC_ERROR_HEADER,
    UPLOAD_URL_HEADER,
)
from ._app import _HttpRpcApp
from ._errors import _error_serializer, _make_not_found_sink
from ._middleware import (
    _REQUEST_ID_HEADER,
    _AccessLogContextMiddleware,
    _AuthMiddleware,
    _CapabilitiesMiddleware,
    _CompressionMiddleware,
    _CorsMaxAgeMiddleware,
    _DrainRequestMiddleware,
    _MaxRequestBytesMiddleware,
    _RequestIdMiddleware,
)
from ._pages import (
    _build_describe_html,
    _build_landing_html,
    _DescribePageResource,
    _LandingPageResource,
)
from ._resources import (
    _ExchangeResource,
    _HealthResource,
    _RpcResource,
    _StreamInitResource,
    _UploadUrlResource,
)

_logger = logging.getLogger("vgi_rpc.http")


def make_wsgi_app(
    server: RpcServer,
    *,
    prefix: str = "",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    max_stream_response_time: float | None = None,
    max_request_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    cors_origins: str | Iterable[str] | None = None,
    cors_max_age: int | None = 7200,
    upload_url_provider: UploadUrlProvider | None = None,
    max_upload_bytes: int | None = None,
    otel_config: object | None = None,
    sentry_config: object | None = None,
    token_ttl: int = 3600,
    compression_level: int | None = 3,
    enable_not_found_page: bool = True,
    enable_landing_page: bool = True,
    enable_describe_page: bool = True,
    enable_health_endpoint: bool = True,
    repo_url: str | None = None,
    oauth_resource_metadata: OAuthResourceMetadata | None = None,
) -> falcon.App[falcon.Request, falcon.Response]:
    """Create a Falcon WSGI app that serves RPC requests over HTTP.

    Args:
        server: The RpcServer instance to serve.
        prefix: URL prefix for all RPC endpoints (default ``""`` — root).
        signing_key: HMAC key for signing state tokens.  When ``None``
            (the default), a random 32-byte key is generated **per process**.
            This means state tokens issued by one worker are invalid in
            another — you **must** provide a shared key for multi-process
            deployments (e.g. gunicorn with multiple workers).
        max_stream_response_bytes: When set, producer stream responses may
            buffer multiple batches in a single HTTP response up to this
            size before emitting a continuation token.  The client
            transparently resumes via ``POST /{method}/exchange``.
            When ``None`` (the default) and ``max_stream_response_time``
            is also ``None``, each produce cycle emits one batch per HTTP
            response for incremental streaming.
        max_stream_response_time: When set, producer stream responses may
            buffer multiple batches up to this many seconds of wall time
            before emitting a continuation token.  Can be combined with
            ``max_stream_response_bytes`` — the response breaks on
            whichever limit is reached first.
        max_request_bytes: When set, the value is advertised via the
            ``VGI-Max-Request-Bytes`` response header on every response
            (including OPTIONS).  Clients can use ``http_capabilities()``
            to discover this limit and decide whether to use external
            storage for large payloads.  Advertisement only — no
            server-side enforcement.  ``None`` (default) omits the header.
        authenticate: Optional callback that extracts an :class:`AuthContext`
            from a Falcon ``Request``.  When provided, every request is
            authenticated before dispatch.  The callback should raise
            ``ValueError`` (bad credentials) or ``PermissionError``
            (forbidden) on failure — these are mapped to HTTP 401.
            Other exceptions propagate as 500.
        cors_origins: Allowed origins for CORS.  Pass ``"*"`` to allow all
            origins, a single origin string like ``"https://example.com"``,
            or an iterable of origin strings.  ``None`` (the default)
            disables CORS headers.  Uses Falcon's built-in
            ``CORSMiddleware`` which also handles preflight OPTIONS
            requests automatically.
        cors_max_age: Value for the ``Access-Control-Max-Age`` header on
            preflight OPTIONS responses, in seconds.  ``7200`` (2 hours)
            by default.  ``None`` omits the header.  Only effective when
            ``cors_origins`` is set.
        upload_url_provider: Optional provider for generating pre-signed
            upload URLs.  When set, the ``__upload_url__/init`` endpoint
            is enabled and ``VGI-Upload-URL-Support: true`` is advertised
            on every response.
        max_upload_bytes: When set (and ``upload_url_provider`` is set),
            advertised via the ``VGI-Max-Upload-Bytes`` header.  Informs
            clients of the maximum size they may upload to vended URLs.
            Advertisement only — no server-side enforcement.
        otel_config: Optional ``OtelConfig`` for OpenTelemetry instrumentation.
            When provided, ``instrument_server()`` is called and
            ``_OtelFalconMiddleware`` is prepended for W3C trace propagation.
            Requires ``pip install vgi-rpc[otel]``.
        sentry_config: Optional ``SentryConfig`` for Sentry error reporting.
            When provided, ``instrument_server_sentry()`` is called.
            Requires ``pip install vgi-rpc[sentry]``.
        token_ttl: Maximum age of stream state tokens in seconds.  Tokens
            older than this are rejected with HTTP 400.  Default is 3600
            (1 hour).  Set to ``0`` to disable expiry checking.
        compression_level: Zstandard compression level for HTTP request/
            response bodies.  ``3`` (the default) installs
            ``_CompressionMiddleware`` at level 3.  Valid range is 1-22.
            ``None`` disables compression entirely.
        enable_not_found_page: When ``True`` (the default), requests to
            paths that do not match any RPC route receive a friendly HTML
            404 page.  Set to ``False`` to use Falcon's default 404
            behaviour instead.
        enable_landing_page: When ``True`` (the default), ``GET {prefix}``
            returns a friendly HTML landing page showing the protocol name,
            server ID, and links.  Set to ``False`` to disable.
        enable_describe_page: When ``True`` (the default) **and** the server
            has ``enable_describe=True``, ``GET {prefix}/describe`` returns
            an HTML page listing all methods, parameters, and types.  The
            path ``{prefix}/describe`` is reserved when active — an RPC
            method named ``describe`` would need the page disabled.
        enable_health_endpoint: When ``True`` (the default),
            ``GET {prefix}/health`` returns a JSON health check response
            with the server's status, ID, and protocol name.  The endpoint
            bypasses authentication.  Set to ``False`` to disable.
        repo_url: Optional URL to the service's source repository (e.g. a
            GitHub URL).  When provided, a "Source repository" link appears
            on the landing page and describe page.
        oauth_resource_metadata: Optional ``OAuthResourceMetadata`` for
            RFC 9728 OAuth discovery.  When provided, serves
            ``/.well-known/oauth-protected-resource`` and adds
            ``WWW-Authenticate: Bearer resource_metadata="..."`` to 401
            responses.

    Returns:
        A Falcon application with routes for unary and stream RPC calls.

    """
    if signing_key is None:
        warnings.warn(
            "No signing_key provided; generating a random per-process key. "
            "State tokens will be invalid across workers — pass a shared key "
            "for multi-process deployments.",
            stacklevel=2,
        )
        signing_key = os.urandom(32)
    # OpenTelemetry instrumentation (optional)
    if otel_config is not None:
        from vgi_rpc.otel import OtelConfig, _OtelFalconMiddleware, instrument_server

        if not isinstance(otel_config, OtelConfig):
            raise TypeError(f"otel_config must be an OtelConfig instance, got {type(otel_config).__name__}")
        instrument_server(server, otel_config)

    # Sentry error reporting (optional)
    if sentry_config is not None:
        from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

        if not isinstance(sentry_config, SentryConfig):
            raise TypeError(f"sentry_config must be a SentryConfig instance, got {type(sentry_config).__name__}")
        instrument_server_sentry(server, sentry_config)

    app_handler = _HttpRpcApp(
        server,
        signing_key,
        max_stream_response_bytes,
        max_stream_response_time,
        max_request_bytes,
        upload_url_provider,
        max_upload_bytes,
        token_ttl,
    )
    middleware: list[Any] = [_DrainRequestMiddleware(), _RequestIdMiddleware(), _AccessLogContextMiddleware()]

    # Enforce the advertised max_request_bytes cap server-side.  The
    # __upload_url__/init route (and capability-discovery routes) are
    # exempt because their payloads are intrinsically tiny.
    if max_request_bytes is not None:
        middleware.append(
            _MaxRequestBytesMiddleware(
                max_request_bytes,
                exempt_prefixes=(
                    f"{prefix}/__upload_url__",
                    f"{prefix}/health",
                ),
            )
        )

    # Compression middleware decompresses request bodies and compresses
    # responses — must come before auth so handlers read plaintext bodies.
    if compression_level is not None:
        middleware.append(_CompressionMiddleware(compression_level))

    # OTel middleware must come before auth so spans cover the full request
    if otel_config is not None:
        middleware.append(_OtelFalconMiddleware())

    # Always expose auth and request-id headers; capability headers are
    # appended conditionally below.
    cors_expose: list[str] = ["WWW-Authenticate", _REQUEST_ID_HEADER, "X-VGI-Content-Encoding", RPC_ERROR_HEADER]

    # Build capability headers
    capability_headers: dict[str, str] = {}
    if max_request_bytes is not None:
        capability_headers[MAX_REQUEST_BYTES_HEADER] = str(max_request_bytes)
        cors_expose.append(MAX_REQUEST_BYTES_HEADER)
    if upload_url_provider is not None:
        capability_headers[UPLOAD_URL_HEADER] = "true"
        cors_expose.append(UPLOAD_URL_HEADER)
        if max_upload_bytes is not None:
            capability_headers[MAX_UPLOAD_BYTES_HEADER] = str(max_upload_bytes)
            cors_expose.append(MAX_UPLOAD_BYTES_HEADER)

    # OAuth resource metadata (RFC 9728)
    from vgi_rpc.http._oauth import OAuthResourceMetadata as _OAuthMeta
    from vgi_rpc.http._oauth import _build_www_authenticate

    www_authenticate: str | None = None
    _validated_oauth_metadata: _OAuthMeta | None = None
    if oauth_resource_metadata is not None:
        if not isinstance(oauth_resource_metadata, _OAuthMeta):
            raise TypeError(
                f"oauth_resource_metadata must be an OAuthResourceMetadata instance, "
                f"got {type(oauth_resource_metadata).__name__}"
            )
        _validated_oauth_metadata = oauth_resource_metadata
        www_authenticate = _build_www_authenticate(_validated_oauth_metadata, prefix)

    if cors_origins is not None:
        cors_kwargs: dict[str, Any] = {
            "allow_origins": cors_origins,
            "expose_headers": cors_expose,
        }
        middleware.append(falcon.CORSMiddleware(**cors_kwargs))
        if cors_max_age is not None:
            middleware.append(_CorsMaxAgeMiddleware(cors_max_age))
    # OAuth PKCE browser flow — only when authenticate + OAuth metadata + client_id
    _pkce_active = False
    _pkce_user_info_html: str | None = None
    _exempt_prefixes_list: list[str] = []
    if enable_health_endpoint:
        _exempt_prefixes_list.append(f"{prefix}/health")
    if (
        authenticate is not None
        and _validated_oauth_metadata is not None
        and _validated_oauth_metadata.client_id is not None
    ):
        from urllib.parse import urlparse as _urlparse

        from vgi_rpc.http._bearer import chain_authenticate
        from vgi_rpc.http._oauth_pkce import (
            _DEFAULT_ALLOWED_RETURN_ORIGINS,
            _create_oidc_discovery,
            _derive_session_key,
            _OAuthCallbackResource,
            _OAuthLogoutResource,
            _OAuthPkceMiddleware,
            _OAuthTokenProxyResource,
            build_user_info_html,
            make_cookie_authenticate,
        )

        _pkce_issuer = _validated_oauth_metadata.authorization_servers[0]
        _pkce_oidc_discovery = _create_oidc_discovery(_pkce_issuer)
        _pkce_session_key = _derive_session_key(signing_key)
        _pkce_resource_parsed = _urlparse(_validated_oauth_metadata.resource)
        _pkce_secure = _pkce_resource_parsed.scheme == "https"
        _pkce_redirect_uri = f"{_pkce_resource_parsed.scheme}://{_pkce_resource_parsed.netloc}{prefix}/_oauth/callback"

        if not _pkce_secure and _pkce_resource_parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            _logger.warning(
                "OAuth PKCE is configured without HTTPS (%s) — cookies will not be Secure. "
                "This is acceptable for local development but not for production.",
                _validated_oauth_metadata.resource,
            )

        # Wrap authenticate to also accept tokens from a cookie
        _pkce_cookie_auth = make_cookie_authenticate(authenticate)
        authenticate = chain_authenticate(authenticate, _pkce_cookie_auth)

        _pkce_client_id: str = _validated_oauth_metadata.client_id
        _pkce_client_secret = _validated_oauth_metadata.client_secret
        _pkce_use_id_token = _validated_oauth_metadata.use_id_token_as_bearer
        _pkce_scope = (
            " ".join(_validated_oauth_metadata.scopes_supported)
            if _validated_oauth_metadata.scopes_supported
            else "openid email"
        )
        _exempt_prefixes_list.append(f"{prefix}/_oauth/")
        _pkce_active = True
        _pkce_user_info_html = build_user_info_html(prefix)

    on_auth_failure: Callable[[str | None, str], None] | None = None
    if authenticate is not None and otel_config is not None:
        from vgi_rpc.otel import OtelConfig as _OtelCfg
        from vgi_rpc.otel import make_auth_failure_counter

        assert isinstance(otel_config, _OtelCfg)  # validated above
        on_auth_failure = make_auth_failure_counter(otel_config, server.protocol_name)
    middleware.append(
        _AuthMiddleware(
            authenticate,
            www_authenticate=www_authenticate,
            on_auth_failure=on_auth_failure,
            exempt_prefixes=tuple(_exempt_prefixes_list),
        )
    )
    if authenticate is not None and _pkce_active:
        middleware.append(
            _OAuthPkceMiddleware(
                session_key=_pkce_session_key,
                oidc_discovery=_pkce_oidc_discovery,
                client_id=_pkce_client_id,
                prefix=prefix,
                secure_cookie=_pkce_secure,
                redirect_uri=_pkce_redirect_uri,
                scope=_pkce_scope,
            )
        )
    if capability_headers:
        middleware.append(_CapabilitiesMiddleware(capability_headers))
    app: falcon.App[falcon.Request, falcon.Response] = falcon.App(middleware=middleware or None)
    app.set_error_serializer(_error_serializer)

    # OAuth well-known endpoint (must be before RPC routes)
    if _validated_oauth_metadata is not None:
        from vgi_rpc.http._oauth import _OAuthResourceMetadataResource

        # When PKCE is active and a server-side client_secret is configured,
        # advertise the proxy token endpoint so SPA clients can perform PKCE
        # token exchanges without holding the secret themselves.
        _advertised_token_endpoint: str | None = None
        if _pkce_active and _validated_oauth_metadata.client_secret is not None:
            _advertised_token_endpoint = (
                f"{_pkce_resource_parsed.scheme}://{_pkce_resource_parsed.netloc}{prefix}/_oauth/token"
            )
        well_known = _OAuthResourceMetadataResource(_validated_oauth_metadata, _advertised_token_endpoint)
        app.add_route("/.well-known/oauth-protected-resource", well_known)
        if prefix and prefix != "/":
            app.add_route(f"/.well-known/oauth-protected-resource{prefix}", well_known)

    app.add_route(f"{prefix}/{{method}}", _RpcResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/init", _StreamInitResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/exchange", _ExchangeResource(app_handler))
    if upload_url_provider is not None:
        app.add_route(f"{prefix}/__upload_url__/init", _UploadUrlResource(app_handler))

    # OAuth PKCE callback and logout routes (must be before not-found sink)
    if _pkce_active:
        app.add_route(
            f"{prefix}/_oauth/callback",
            _OAuthCallbackResource(
                session_key=_pkce_session_key,
                oidc_discovery=_pkce_oidc_discovery,
                client_id=_pkce_client_id,
                client_secret=_pkce_client_secret,
                use_id_token=_pkce_use_id_token,
                prefix=prefix,
                secure_cookie=_pkce_secure,
                redirect_uri=_pkce_redirect_uri,
            ),
        )
        app.add_route(f"{prefix}/_oauth/logout", _OAuthLogoutResource(prefix, _pkce_secure))
        # Token-exchange proxy: lets SPA PKCE clients (which cannot safely
        # hold a client_secret) complete authorization_code/refresh_token
        # exchanges against IdPs that require client_secret (e.g. Google).
        app.add_route(
            f"{prefix}/_oauth/token",
            _OAuthTokenProxyResource(
                client_id=_pkce_client_id,
                client_secret=_pkce_client_secret,
                oidc_discovery=_pkce_oidc_discovery,
                allowed_origins=_DEFAULT_ALLOWED_RETURN_ORIGINS,
            ),
        )

    # Describe page — GET {prefix}/describe (requires both flags and server support)
    describe_page_active = enable_describe_page and server.describe_enabled
    if describe_page_active:
        describe_html = _build_describe_html(server, prefix, repo_url)
        if _pkce_user_info_html:
            describe_html = describe_html.replace(b"</body>", _pkce_user_info_html.encode() + b"\n</body>")
        app.add_route(f"{prefix}/describe", _DescribePageResource(describe_html))

    # Health endpoint — GET {prefix}/health
    if enable_health_endpoint:
        app.add_route(f"{prefix}/health", _HealthResource(server.server_id, server.protocol_name))

    # Landing page — GET {prefix}
    if enable_landing_page:
        describe_path = f"{prefix}/describe" if describe_page_active else None
        landing_body = _build_landing_html(prefix, server.protocol_name, server.server_id, describe_path, repo_url)
        if _pkce_user_info_html:
            landing_body = landing_body.replace(b"</body>", _pkce_user_info_html.encode() + b"\n</body>")
        app.add_route(prefix or "/", _LandingPageResource(landing_body))

    if enable_not_found_page:
        app.add_sink(_make_not_found_sink(prefix, server.protocol_name))

    _logger.info(
        "WSGI app created for %s (server_id=%s, prefix=%s, auth=%s)",
        server.protocol_name,
        server.server_id,
        prefix,
        "enabled" if authenticate is not None else "disabled",
        extra={
            "server_id": server.server_id,
            "protocol": server.protocol_name,
            "prefix": prefix,
            "auth_enabled": authenticate is not None,
        },
    )

    return app
