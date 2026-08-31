# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Synchronous test client for the HTTP transport.

Provides ``_SyncTestClient`` and ``make_sync_client`` which use
``falcon.testing.TestClient`` internally — no real HTTP server needed.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import falcon
import falcon.testing

from vgi_rpc.external import UploadUrlProvider
from vgi_rpc.rpc import AuthContext, PeerAuthenticationPolicy, PeerIdentityProvider, RpcServer

from .server import make_wsgi_app
from .server._introspect import TokenResolver

if TYPE_CHECKING:
    from vgi_rpc.http._oauth import OAuthResourceMetadata


class _SyncTestResponse:
    """Minimal response object matching what _HttpProxy expects from httpx2.Response.

    Transparently decompresses ``Content-Encoding`` response bodies for
    any codec the runtime can handle (zstd, gzip), mirroring httpx2's
    built-in decoders so the test client and a real httpx2 client behave
    identically.
    """

    __slots__ = ("content", "headers", "status_code")

    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = headers or {}
        encoding = (self.headers.get("content-encoding", "") or "").strip().lower()
        if encoding:
            from vgi_rpc._codec import Encoding as _CodecEncoding
            from vgi_rpc._codec import decompress as _codec_decompress

            for enc in _CodecEncoding:
                if enc.value == encoding:
                    content = _codec_decompress(enc, content)
                    break
        self.content = content


class _SyncTestClient:
    """Sync HTTP client that calls a Falcon WSGI app directly via falcon.testing.TestClient."""

    __slots__ = ("_client", "_default_headers", "prefix")

    def __init__(
        self,
        app: falcon.App[falcon.Request, falcon.Response],
        default_headers: dict[str, str] | None = None,
        prefix: str = "",
    ) -> None:
        self._client = falcon.testing.TestClient(app)
        self._default_headers: dict[str, str] = default_headers or {}
        self.prefix = prefix

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Send a synchronous POST using the Falcon test client."""
        merged = {**self._default_headers, **headers}
        # Strip scheme+host if present (test_http.py passes full URLs)
        path = urlparse(url).path
        result = self._client.simulate_post(path, body=content, headers=merged)
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Send a synchronous GET using the Falcon test client."""
        merged = {**self._default_headers, **(headers or {})}
        path = urlparse(url).path
        result = self._client.simulate_get(path, headers=merged)
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def options(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Send a synchronous OPTIONS using the Falcon test client."""
        merged = {**self._default_headers, **(headers or {})}
        path = urlparse(url).path
        result = self._client.simulate_options(path, headers=merged)
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def delete(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Send a synchronous DELETE using the Falcon test client.

        Mirrors :meth:`post` / :meth:`get` / :meth:`options` so the in-process
        test client supports the framework-managed sticky-session teardown
        endpoint (``DELETE /vgi/__session__``) without requiring a real
        HTTP server.
        """
        merged = {**self._default_headers, **(headers or {})}
        path = urlparse(url).path
        result = self._client.simulate_delete(path, headers=merged)
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def put(self, url: str, **kwargs: object) -> _SyncTestResponse:
        """Send a synchronous PUT — used by externalised upload paths.

        Returns a 404 by default because the test client doesn't proxy
        out to external storage; callers needing real PUT semantics
        should use ``httpx2.Client`` directly. This stub exists so the
        sticky session-tracking client (which delegates PUT through
        unchanged) keeps a uniform interface with httpx2.
        """
        # No-op delegation — tests don't go through the test client for
        # external uploads; they hit the FakeStorageBackend HTTP endpoint
        # via real httpx2. Returning a 404 surfaces clearly if anything
        # accidentally routes here.
        return _SyncTestResponse(404, b"", headers={})

    def close(self) -> None:
        """Close the client (no-op for test client)."""


def make_sync_client(
    server: RpcServer,
    *,
    prefix: str = "",
    token_key: bytes | None = None,
    max_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    max_stream_response_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    peer_identity_providers: Sequence[PeerIdentityProvider] = (),
    peer_authentication_policy: PeerAuthenticationPolicy | None = None,
    peer_service_name: str | None = None,
    peer_resolution_timeout: float = 5.0,
    peer_provider_concurrency: int = 64,
    proxy_proof_required: bool = False,
    proxy_auth_headers: Sequence[str] | None = None,
    default_headers: dict[str, str] | None = None,
    upload_url_provider: UploadUrlProvider | None = None,
    max_upload_bytes: int | None = None,
    otel_config: object | None = None,
    sentry_config: object | None = None,
    token_ttl: int = 3600,
    compression_level: int | None = 1,
    enable_not_found_page: bool = True,
    enable_landing_page: bool = True,
    enable_describe_page: bool = True,
    enable_health_endpoint: bool = True,
    repo_url: str | None = None,
    oauth_resource_metadata: OAuthResourceMetadata | None = None,
    enable_sticky: bool = False,
    sticky_default_ttl: float = 300.0,
    sticky_echo_headers: Mapping[str, str] | None = None,
    call_state_cache_entries: int = 4096,
    introspect_resolver: TokenResolver | None = None,
    introspect_principals: Iterable[str] | None = None,
    introspect_rate_limit: int = 20,
) -> _SyncTestClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``falcon.testing.TestClient`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        prefix: URL prefix for RPC endpoints (default ``""`` — root).
        token_key: AEAD key for sealing stream state tokens (see
            ``make_wsgi_app`` for details).
        max_response_bytes: See ``make_wsgi_app``.
        max_externalized_response_bytes: See ``make_wsgi_app``.
        max_request_bytes: See ``make_wsgi_app``.
        max_stream_response_bytes: **Deprecated** alias for
            ``max_response_bytes``.
        authenticate: See ``make_wsgi_app``.
        peer_identity_providers: See ``make_wsgi_app``.
        peer_authentication_policy: See ``make_wsgi_app``.
        peer_service_name: See ``make_wsgi_app``.
        peer_resolution_timeout: See ``make_wsgi_app``.
        peer_provider_concurrency: See ``make_wsgi_app``.
        proxy_proof_required: See ``make_wsgi_app``.
        proxy_auth_headers: See ``make_wsgi_app``.
        default_headers: Headers merged into every request (e.g. auth tokens).
        upload_url_provider: See ``make_wsgi_app``.
        max_upload_bytes: See ``make_wsgi_app``.
        otel_config: See ``make_wsgi_app``.
        sentry_config: See ``make_wsgi_app``.
        token_ttl: See ``make_wsgi_app``.
        compression_level: See ``make_wsgi_app``.
        enable_not_found_page: See ``make_wsgi_app``.
        enable_landing_page: See ``make_wsgi_app``.
        enable_describe_page: See ``make_wsgi_app``.
        enable_health_endpoint: See ``make_wsgi_app``.
        repo_url: See ``make_wsgi_app``.
        oauth_resource_metadata: See ``make_wsgi_app``.
        enable_sticky: See ``make_wsgi_app``.
        sticky_default_ttl: See ``make_wsgi_app``.
        sticky_echo_headers: See ``make_wsgi_app``.
        call_state_cache_entries: See ``make_wsgi_app``.  Pass ``0`` to force
            every stream continuation down the call-token miss path.
        introspect_resolver: See ``make_wsgi_app``.
        introspect_principals: See ``make_wsgi_app``.
        introspect_rate_limit: See ``make_wsgi_app``.

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_wsgi_app(
        server,
        prefix=prefix,
        token_key=token_key,
        max_response_bytes=max_response_bytes,
        max_externalized_response_bytes=max_externalized_response_bytes,
        max_stream_response_bytes=max_stream_response_bytes,
        max_request_bytes=max_request_bytes,
        authenticate=authenticate,
        peer_identity_providers=peer_identity_providers,
        peer_authentication_policy=peer_authentication_policy,
        peer_service_name=peer_service_name,
        peer_resolution_timeout=peer_resolution_timeout,
        peer_provider_concurrency=peer_provider_concurrency,
        proxy_proof_required=proxy_proof_required,
        proxy_auth_headers=proxy_auth_headers,
        upload_url_provider=upload_url_provider,
        max_upload_bytes=max_upload_bytes,
        otel_config=otel_config,
        sentry_config=sentry_config,
        token_ttl=token_ttl,
        compression_level=compression_level,
        enable_not_found_page=enable_not_found_page,
        enable_landing_page=enable_landing_page,
        enable_describe_page=enable_describe_page,
        enable_health_endpoint=enable_health_endpoint,
        repo_url=repo_url,
        oauth_resource_metadata=oauth_resource_metadata,
        enable_sticky=enable_sticky,
        sticky_default_ttl=sticky_default_ttl,
        sticky_echo_headers=sticky_echo_headers,
        call_state_cache_entries=call_state_cache_entries,
        introspect_resolver=introspect_resolver,
        introspect_principals=introspect_principals,
        introspect_rate_limit=introspect_rate_limit,
    )
    return _SyncTestClient(app, default_headers=default_headers, prefix=prefix)
