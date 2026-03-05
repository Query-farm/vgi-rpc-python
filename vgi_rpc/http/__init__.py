# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP transport for vgi-rpc using Falcon (server) and httpx (client).

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application, and ``http_connect`` to call it from Python with ``httpx``.

HTTP Wire Protocol
------------------
All endpoints use ``Content-Type: application/vnd.apache.arrow.stream``.

- **Unary**: ``POST /vgi/{method}``
- **Stream Init**: ``POST /vgi/{method}/init``
- **Stream Exchange**: ``POST /vgi/{method}/exchange``

Streaming is implemented statelessly: each exchange is a separate HTTP POST
carrying serialized state in Arrow custom metadata (``vgi_rpc.stream_state#b64``).
When ``max_stream_response_bytes`` is set, producer-stream responses are
split across multiple exchanges; the client transparently resumes via
``POST /vgi/{method}/exchange``.

Optional dependencies: ``pip install vgi-rpc[http]``
"""

import contextlib

from vgi_rpc.http._bearer import bearer_authenticate, bearer_authenticate_static, chain_authenticate
from vgi_rpc.http._client import (
    HttpServerCapabilities,
    HttpStreamSession,
    OAuthResourceMetadataResponse,
    _init_http_stream_session,
    fetch_oauth_metadata,
    http_capabilities,
    http_connect,
    http_introspect,
    http_oauth_metadata,
    parse_client_id,
    parse_resource_metadata_url,
    request_upload_urls,
)
from vgi_rpc.http._common import (
    _ARROW_CONTENT_TYPE,
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    UPLOAD_URL_HEADER,
    _RpcHttpError,
)
from vgi_rpc.http._mtls import XfccElement, mtls_authenticate_xfcc
from vgi_rpc.http._oauth import OAuthResourceMetadata
from vgi_rpc.http._retry import HttpRetryConfig, HttpTransientError
from vgi_rpc.http._server import make_wsgi_app, serve_http
from vgi_rpc.http._testing import (
    _SyncTestClient,
    _SyncTestResponse,
    make_sync_client,
)

with contextlib.suppress(ImportError):
    from vgi_rpc.http._oauth_jwt import jwt_authenticate  # noqa: F401
with contextlib.suppress(ImportError):
    from vgi_rpc.http._mtls import (  # noqa: F401
        mtls_authenticate,
        mtls_authenticate_fingerprint,
        mtls_authenticate_subject,
    )

__all__ = [
    "bearer_authenticate",
    "bearer_authenticate_static",
    "chain_authenticate",
    "HttpRetryConfig",
    "HttpServerCapabilities",
    "HttpStreamSession",
    "HttpTransientError",
    "MAX_REQUEST_BYTES_HEADER",
    "MAX_UPLOAD_BYTES_HEADER",
    "OAuthResourceMetadata",
    "OAuthResourceMetadataResponse",
    "UPLOAD_URL_HEADER",
    "_ARROW_CONTENT_TYPE",
    "_RpcHttpError",
    "_SyncTestClient",
    "_init_http_stream_session",
    "_SyncTestResponse",
    "fetch_oauth_metadata",
    "http_capabilities",
    "http_connect",
    "http_introspect",
    "http_oauth_metadata",
    "make_sync_client",
    "parse_client_id",
    "parse_resource_metadata_url",
    "make_wsgi_app",
    "serve_http",
    "request_upload_urls",
    "XfccElement",
    "mtls_authenticate_xfcc",
]

if "jwt_authenticate" in dir():
    __all__.append("jwt_authenticate")
if "mtls_authenticate" in dir():
    __all__.extend(["mtls_authenticate", "mtls_authenticate_fingerprint", "mtls_authenticate_subject"])
