# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Shared constants, schemas, and exception for the HTTP transport layer."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import pyarrow as pa

from vgi_rpc._codec import (
    DecompressionError,
    Encoding,
    available_encodings,
    compress,
    decompress,
    parse_encoding_list,
)
from vgi_rpc.rpc import _EMPTY_SCHEMA

__all__ = [
    "DecompressionError",
    "Encoding",
    "available_encodings",
    "compress",
    "decompress",
    "parse_encoding_list",
]

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes"
MAX_RESPONSE_BYTES_HEADER = "VGI-Max-Response-Bytes"
MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER = "VGI-Max-Externalized-Response-Bytes"
EXTERNALIZATION_ENABLED_HEADER = "VGI-Externalization-Enabled"
RPC_ERROR_HEADER = "X-VGI-RPC-Error"
UPLOAD_URL_HEADER = "VGI-Upload-URL-Support"
MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes"
SUPPORTED_ENCODINGS_HEADER = "VGI-Supported-Encodings"

# Sticky session header conventions (HTTP-only). The cookie equivalent
# (Set-Cookie / vgi-session) is intentionally out of scope for v1 — headers
# multiplex cleanly across concurrent sessions, cookies do not.
SESSION_HEADER = "VGI-Session"
SESSION_ACCEPT_HEADER = "VGI-Session-Accept"
SESSION_CLOSE_HEADER = "VGI-Session-Close"
STICKY_ENABLED_HEADER = "VGI-Sticky-Enabled"
STICKY_DEFAULT_TTL_HEADER = "VGI-Sticky-Default-TTL"
STICKY_ECHO_HEADERS_HEADER = "VGI-Sticky-Echo-Headers"
"""Capability header listing the comma-separated names of headers a client must echo back."""

# Prefix the server uses to tell the client "echo this header on subsequent
# requests in this session". The client strips the prefix; e.g. on Fly the
# server emits ``VGI-Echo-fly-force-instance-id: <machine-id>`` and the
# client sends back ``fly-force-instance-id: <machine-id>``.
ECHO_HEADER_PREFIX = "VGI-Echo-"

# Framework-managed sticky session teardown endpoint. Parallel to the
# synthetic __describe__ method but served by a dedicated Falcon resource;
# DELETE /vgi/__session__ idempotently closes the session referenced by
# the request's VGI-Session header.
_SESSION_ENDPOINT = "__session__"

_MAX_UPLOAD_URL_COUNT = 100

_UPLOAD_URL_METHOD = "__upload_url__"
_upload_url_params_fields: list[pa.Field[Any]] = [pa.field("count", pa.int64())]
_UPLOAD_URL_PARAMS_SCHEMA = pa.schema(_upload_url_params_fields)
_upload_url_fields: list[pa.Field[Any]] = [
    pa.field("upload_url", pa.utf8()),
    pa.field("download_url", pa.utf8()),
    pa.field("expires_at", pa.timestamp("us", tz="UTC")),
]
_UPLOAD_URL_SCHEMA = pa.schema(_upload_url_fields)


# Backwards-compatible aliases — older call sites import these names.
def _compress_body(data: bytes, level: int) -> bytes:
    """Compress *data* using zstd at the given level (legacy entry point)."""
    return compress(Encoding.ZSTD, data, level=level)


def _decompress_body(data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress zstd-compressed *data* with optional output cap (legacy entry point)."""
    return decompress(Encoding.ZSTD, data, max_output_size=max_output_size)


def decode_content_encoding(
    data: bytes,
    content_encoding: str | None,
    *,
    max_output_size: int | None = None,
) -> bytes:
    """Decode an HTTP body per its ``Content-Encoding``, or return it unchanged.

    Handles the codings vgi-rpc speaks (``zstd``, ``gzip``); the header may list
    several applied in order, which are decoded in reverse. Unknown/``identity``
    codings are left as-is. Intended for an intermediary (proxy/gateway) that
    must read a compressed request/response body to inspect or rewrite it.

    Args:
        data: The raw (possibly compressed) body bytes.
        content_encoding: The ``Content-Encoding`` header value, or ``None``.
        max_output_size: Optional decompression output cap (per coding).

    Returns:
        The decoded body bytes (the input unchanged when nothing applies).

    """
    if not content_encoding:
        return data
    result = data
    for name in reversed([c.strip().lower() for c in content_encoding.split(",") if c.strip()]):
        try:
            encoding = Encoding(name)
        except ValueError:
            continue  # identity / unknown coding — leave as-is
        result = decompress(encoding, result, max_output_size=max_output_size)
    return result


# ---------------------------------------------------------------------------
# Shared HTML styles for error and landing pages
# ---------------------------------------------------------------------------

_FONT_IMPORTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">'  # noqa: E501
)

_ERROR_PAGE_STYLE = """\
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
         background: #faf8f0; }}
  .logo {{ margin-bottom: 24px; }}
  .logo img {{ width: 120px; height: 120px; border-radius: 50%;
               box-shadow: 0 4px 24px rgba(0,0,0,0.12); }}
  h1 {{ color: #2d5016; margin-bottom: 8px; font-weight: 700; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  p {{ line-height: 1.7; color: #6b6b5a; }}
  .detail {{ margin-top: 12px; padding: 12px 16px; background: #f0ece0;
             border-radius: 6px; font-size: 0.9em; color: #6b6b5a; }}
  footer {{ margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
            color: #6b6b5a; font-size: 0.85em; line-height: 1.8; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>"""

_VGI_LOGO_HTML = """\
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>"""


class _RpcHttpError(Exception):
    """Internal exception for HTTP-layer errors with status codes."""

    __slots__ = ("cause", "schema", "status_code")

    def __init__(self, cause: BaseException, *, status_code: HTTPStatus, schema: pa.Schema = _EMPTY_SCHEMA) -> None:
        self.cause = cause
        self.status_code = status_code
        self.schema = schema
