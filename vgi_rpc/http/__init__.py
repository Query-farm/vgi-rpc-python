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
carrying serialized state in Arrow custom metadata (``vgi_rpc.stream_state``).
When ``max_stream_response_bytes`` is set, producer-stream responses are
split across multiple exchanges; the client transparently resumes via
``POST /{method}/exchange``.

Optional dependencies: ``pip install vgi-rpc[http]``
"""

from vgi_rpc.http._client import (
    HttpServerCapabilities,
    HttpStreamSession,
    _init_http_stream_session,
    http_capabilities,
    http_connect,
    http_introspect,
    request_upload_urls,
)
from vgi_rpc.http._common import (
    _ARROW_CONTENT_TYPE,
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    UPLOAD_URL_HEADER,
    _RpcHttpError,
)
from vgi_rpc.http._server import make_wsgi_app
from vgi_rpc.http._testing import (
    _SyncTestClient,
    _SyncTestResponse,
    make_sync_client,
)

__all__ = [
    "HttpServerCapabilities",
    "HttpStreamSession",
    "MAX_REQUEST_BYTES_HEADER",
    "MAX_UPLOAD_BYTES_HEADER",
    "UPLOAD_URL_HEADER",
    "_ARROW_CONTENT_TYPE",
    "_RpcHttpError",
    "_SyncTestClient",
    "_init_http_stream_session",
    "_SyncTestResponse",
    "http_capabilities",
    "http_connect",
    "http_introspect",
    "make_sync_client",
    "make_wsgi_app",
    "request_upload_urls",
]
