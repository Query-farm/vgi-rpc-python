"""Shared constants, schemas, and exception for the HTTP transport layer."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import pyarrow as pa

from vgi_rpc.rpc import _EMPTY_SCHEMA

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes"
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


class _RpcHttpError(Exception):
    """Internal exception for HTTP-layer errors with status codes."""

    __slots__ = ("cause", "schema", "status_code")

    def __init__(self, cause: BaseException, *, status_code: HTTPStatus, schema: pa.Schema = _EMPTY_SCHEMA) -> None:
        self.cause = cause
        self.status_code = status_code
        self.schema = schema
