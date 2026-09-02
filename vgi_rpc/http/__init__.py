# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP transport for vgi-rpc using Falcon (server) and httpx2 (client).

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application, and ``http_connect`` to call it from Python with ``httpx2``.

HTTP Wire Protocol
------------------
All endpoints use ``Content-Type: application/vnd.apache.arrow.stream``.

- **Unary**: ``POST /vgi/{method}``
- **Stream Init**: ``POST /vgi/{method}/init``
- **Stream Exchange**: ``POST /vgi/{method}/exchange``

Streaming is implemented statelessly: each exchange is a separate HTTP POST
carrying serialized state in Arrow custom metadata (``vgi_rpc.stream_state#b64``).
Every producer POST performs exactly one state transition and returns at most
one data batch; unfinished streams resume via ``POST /vgi/{method}/exchange``.

Optional dependencies: ``pip install vgi-rpc[http]``
"""

import contextlib

from vgi_rpc.http._bearer import (
    PreconditionGate,
    bearer_authenticate,
    bearer_authenticate_static,
    chain_authenticate,
    require_all,
)
from vgi_rpc.http._client import (
    HttpServerCapabilities,
    HttpStreamSession,
    OAuthResourceMetadataResponse,
    OAuthServerMetadata,
    _init_http_stream_session,
    fetch_auth_server_metadata,
    fetch_oauth_metadata,
    http_capabilities,
    http_connect,
    http_introspect,
    http_oauth_metadata,
    parse_client_id,
    parse_client_secret,
    parse_device_code_client_id,
    parse_device_code_client_secret,
    parse_resource_metadata_url,
    parse_use_id_token_as_bearer,
    parse_www_authenticate_params,
    request_upload_urls,
)
from vgi_rpc.http._common import (
    _ARROW_CONTENT_TYPE,
    ACCEPT_MAX_RESPONSE_BYTES_HEADER,
    ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER,
    AUTH_PROXY_REQUIRED_HEADER,
    AUTH_REASON_HEADER,
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    RPC_ERROR_HEADER,
    UPLOAD_URL_HEADER,
    _RpcHttpError,
    decode_content_encoding,
)
from vgi_rpc.http._common import (
    _MAX_UPLOAD_URL_COUNT as MAX_UPLOAD_URL_COUNT,
)
from vgi_rpc.http._common import (
    _UPLOAD_URL_METHOD as UPLOAD_URL_METHOD,
)
from vgi_rpc.http._common import (
    _UPLOAD_URL_PARAMS_SCHEMA as UPLOAD_URL_PARAMS_SCHEMA,
)
from vgi_rpc.http._common import (
    _UPLOAD_URL_SCHEMA as UPLOAD_URL_RESPONSE_SCHEMA,
)
from vgi_rpc.http._iroh import IROH_ENDPOINT_HEADER, iroh_forwarded_header_provider
from vgi_rpc.http._mtls import XfccElement, mtls_authenticate_xfcc
from vgi_rpc.http._oauth import OAuthResourceMetadata
from vgi_rpc.http._oauth_client import (
    OAuthChallenge,
    OAuthFlow,
    OAuthIdentity,
    OAuthPrompt,
    OAuthRefreshContext,
    OAuthTokenError,
    OAuthTokenSet,
    VgiOAuthAuth,
    attempt_token_refresh,
    is_colab_environment,
    is_headless_environment,
    parse_oauth_challenge,
    perform_device_code_flow,
    select_device_code_client,
)
from vgi_rpc.http._proof import (
    PROOF_HEADER,
    PROOF_REQUIRED_HEADER,
    ProofError,
    ProxyProofConfig,
    derive_secret,
    mint_proof,
    parse_secrets,
    proxy_proof_gate,
    verify_proof,
)
from vgi_rpc.http._retry import HttpRetryConfig, HttpTransientError
from vgi_rpc.http._tailscale import tailscale_localapi_provider, tailscale_serve_header_provider
from vgi_rpc.http._testing import (
    _SyncTestClient,
    _SyncTestResponse,
    make_sync_client,
)
from vgi_rpc.http._unauthorized import (
    AuthenticationError,
    AuthFailure,
    AuthReason,
    AuthUnavailableError,
    build_proxy_hint,
    declare_proxy_headers,
    proxy_headers_of,
)
from vgi_rpc.http.server import TokenIdentity, TokenResolver, make_wsgi_app, serve_http
from vgi_rpc.http.server._sticky import DrainHandle, drain_handle

with contextlib.suppress(ImportError):
    from vgi_rpc.http._oauth_jwt import jwt_authenticate  # noqa: F401
with contextlib.suppress(ImportError):
    from vgi_rpc.http._oauth_pkce import make_cookie_authenticate  # noqa: F401
with contextlib.suppress(ImportError):
    from vgi_rpc.http._mtls import (  # noqa: F401
        mtls_authenticate,
        mtls_authenticate_fingerprint,
        mtls_authenticate_subject,
    )
with contextlib.suppress(ImportError):
    from vgi_rpc.http._spiffe import (  # noqa: F401
        aws_alb_spiffe_provider,
        azure_application_gateway_spiffe_provider,
        envoy_xfcc_spiffe_provider,
        gcp_load_balancer_spiffe_provider,
        nginx_spiffe_provider,
        spiffe_x509_header_provider,
        validate_spiffe_id,
    )

__all__ = [
    "AUTH_PROXY_REQUIRED_HEADER",
    "AUTH_REASON_HEADER",
    "ACCEPT_MAX_RESPONSE_BYTES_HEADER",
    "ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER",
    "AuthFailure",
    "AuthReason",
    "AuthUnavailableError",
    "AuthenticationError",
    "build_proxy_hint",
    "declare_proxy_headers",
    "proxy_headers_of",
    "bearer_authenticate",
    "bearer_authenticate_static",
    "chain_authenticate",
    "PreconditionGate",
    "require_all",
    "PROOF_HEADER",
    "PROOF_REQUIRED_HEADER",
    "ProofError",
    "ProxyProofConfig",
    "derive_secret",
    "mint_proof",
    "parse_secrets",
    "proxy_proof_gate",
    "verify_proof",
    "DrainHandle",
    "drain_handle",
    "HttpRetryConfig",
    "HttpServerCapabilities",
    "HttpStreamSession",
    "HttpTransientError",
    "IROH_ENDPOINT_HEADER",
    "MAX_REQUEST_BYTES_HEADER",
    "MAX_UPLOAD_BYTES_HEADER",
    "MAX_UPLOAD_URL_COUNT",
    "OAuthResourceMetadata",
    "RPC_ERROR_HEADER",
    "OAuthResourceMetadataResponse",
    "OAuthServerMetadata",
    "UPLOAD_URL_HEADER",
    "decode_content_encoding",
    "UPLOAD_URL_METHOD",
    "UPLOAD_URL_PARAMS_SCHEMA",
    "UPLOAD_URL_RESPONSE_SCHEMA",
    "_ARROW_CONTENT_TYPE",
    "_RpcHttpError",
    "_SyncTestClient",
    "_init_http_stream_session",
    "_SyncTestResponse",
    "fetch_auth_server_metadata",
    "fetch_oauth_metadata",
    "http_capabilities",
    "http_connect",
    "http_introspect",
    "http_oauth_metadata",
    "make_sync_client",
    "parse_client_id",
    "parse_client_secret",
    "parse_device_code_client_id",
    "parse_device_code_client_secret",
    "parse_resource_metadata_url",
    "parse_use_id_token_as_bearer",
    "parse_www_authenticate_params",
    # Client-driven OAuth login (device-code + PKCE) -- vgi_rpc.http._oauth_client.
    "OAuthChallenge",
    "OAuthFlow",
    "OAuthIdentity",
    "OAuthPrompt",
    "OAuthRefreshContext",
    "OAuthTokenError",
    "OAuthTokenSet",
    "VgiOAuthAuth",
    "attempt_token_refresh",
    "is_colab_environment",
    "is_headless_environment",
    "parse_oauth_challenge",
    "perform_device_code_flow",
    "select_device_code_client",
    "make_wsgi_app",
    "serve_http",
    "TokenIdentity",
    "TokenResolver",
    "request_upload_urls",
    "XfccElement",
    "mtls_authenticate_xfcc",
    "tailscale_localapi_provider",
    "tailscale_serve_header_provider",
    "iroh_forwarded_header_provider",
]

if "jwt_authenticate" in dir():
    __all__.append("jwt_authenticate")
if "make_cookie_authenticate" in dir():
    __all__.append("make_cookie_authenticate")
if "mtls_authenticate" in dir():
    __all__.extend(["mtls_authenticate", "mtls_authenticate_fingerprint", "mtls_authenticate_subject"])
if "spiffe_x509_header_provider" in dir():
    __all__.extend(
        [
            "aws_alb_spiffe_provider",
            "azure_application_gateway_spiffe_provider",
            "envoy_xfcc_spiffe_provider",
            "gcp_load_balancer_spiffe_provider",
            "nginx_spiffe_provider",
            "spiffe_x509_header_provider",
            "validate_spiffe_id",
        ]
    )
