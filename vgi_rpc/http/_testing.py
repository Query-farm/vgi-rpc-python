# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Synchronous test client for the HTTP transport.

Provides ``_SyncTestClient`` and ``make_sync_client`` which use
``falcon.testing.TestClient`` internally — no real HTTP server needed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import falcon
import falcon.testing

from vgi_rpc.external import UploadUrlProvider
from vgi_rpc.rpc import AuthContext, RpcServer

from .server import make_wsgi_app

if TYPE_CHECKING:
    from vgi_rpc.http._oauth import OAuthResourceMetadata


class _SyncTestResponse:
    """Minimal response object matching what _HttpProxy expects from httpx.Response.

    Transparently decompresses ``Content-Encoding: zstd`` response bodies,
    mirroring httpx's built-in ``ZStandardDecoder`` behaviour so that the
    test client and a real httpx client behave identically.
    """

    __slots__ = ("content", "headers", "status_code")

    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = headers or {}
        encoding = self.headers.get("content-encoding", "")
        if "zstd" in encoding:
            from vgi_rpc.http._common import _decompress_body

            content = _decompress_body(content)
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

    def close(self) -> None:
        """Close the client (no-op for test client)."""


def make_sync_client(
    server: RpcServer,
    *,
    prefix: str = "",
    signing_key: bytes | None = None,
    max_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    max_stream_response_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    default_headers: dict[str, str] | None = None,
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
) -> _SyncTestClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``falcon.testing.TestClient`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        prefix: URL prefix for RPC endpoints (default ``""`` — root).
        signing_key: HMAC key for signing state tokens (see
            ``make_wsgi_app`` for details).
        max_response_bytes: See ``make_wsgi_app``.
        max_externalized_response_bytes: See ``make_wsgi_app``.
        max_request_bytes: See ``make_wsgi_app``.
        max_stream_response_bytes: **Deprecated** alias for
            ``max_response_bytes``.
        authenticate: See ``make_wsgi_app``.
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

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_wsgi_app(
        server,
        prefix=prefix,
        signing_key=signing_key,
        max_response_bytes=max_response_bytes,
        max_externalized_response_bytes=max_externalized_response_bytes,
        max_stream_response_bytes=max_stream_response_bytes,
        max_request_bytes=max_request_bytes,
        authenticate=authenticate,
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
    )
    return _SyncTestClient(app, default_headers=default_headers, prefix=prefix)
