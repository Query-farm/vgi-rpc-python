# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Shared constants, schemas, and exception for the HTTP transport layer."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import pyarrow as pa

from vgi_rpc.rpc import _EMPTY_SCHEMA

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes"
MAX_RESPONSE_BYTES_HEADER = "VGI-Max-Response-Bytes"
MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER = "VGI-Max-Externalized-Response-Bytes"
EXTERNALIZATION_ENABLED_HEADER = "VGI-Externalization-Enabled"
RPC_ERROR_HEADER = "X-VGI-RPC-Error"
UPLOAD_URL_HEADER = "VGI-Upload-URL-Support"
MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes"

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


def _compress_body(data: bytes, level: int) -> bytes:
    """Compress *data* using zstd at the given level.

    A new ``ZstdCompressor`` is created per call (cheap, thread-safe).

    Args:
        data: Raw bytes to compress.
        level: Zstandard compression level (1-22).

    Returns:
        Compressed bytes.

    """
    import zstandard

    return zstandard.ZstdCompressor(level=level).compress(data)


def _decompress_body(data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress zstd-compressed *data*.

    A new ``ZstdDecompressor`` is created per call (cheap, thread-safe).

    Args:
        data: Zstd-compressed bytes.
        max_output_size: Optional upper bound on the decompressed size.
            Zstd frames carry the decompressed size in their header and
            ``decompress()`` would otherwise trust it and allocate
            eagerly — so a 3 KB compressed body claiming 100 MB output
            would allocate 100 MB on the server (decompression-bomb
            DoS).  When set, the frame's declared size is checked
            against this cap *before* decompression begins; frames with
            unknown content size fall back to streaming decompression
            with the cap enforced as a hard ceiling.  ``None`` keeps
            the historical unbounded behaviour for callers that have
            already validated the source.

    Returns:
        Decompressed bytes.

    Raises:
        zstandard.ZstdError: If the frame's declared decompressed size
            exceeds ``max_output_size`` (when set), or if decompression
            otherwise fails.

    """
    import zstandard

    if max_output_size is None:
        return zstandard.ZstdDecompressor().decompress(data)

    # Refuse the frame up-front when the header claims more than allowed.
    # This catches the ``unbounded allocation`` decompression-bomb case
    # before any output buffer is allocated.
    params = zstandard.get_frame_parameters(data)
    if params.content_size != -1 and params.content_size > max_output_size:
        raise zstandard.ZstdError(
            f"Compressed frame declares decompressed size {params.content_size} bytes, "
            f"which exceeds max_output_size={max_output_size}"
        )

    if params.content_size != -1:
        # Header-known size and within the cap — safe to decompress in one shot.
        return zstandard.ZstdDecompressor().decompress(data)

    # Unknown size — stream and stop the moment we exceed the cap.
    decompressor = zstandard.ZstdDecompressor()
    chunks: list[bytes] = []
    total = 0
    with decompressor.stream_reader(data) as reader:
        while True:
            chunk = reader.read(min(65536, max_output_size - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > max_output_size:
                raise zstandard.ZstdError(f"Decompressed output exceeds max_output_size={max_output_size}")
            chunks.append(chunk)
    return b"".join(chunks)


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
