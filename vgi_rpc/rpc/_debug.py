"""Debug logging infrastructure for wire protocol diagnostics.

Provides logger instances under the ``vgi_rpc.wire.*`` hierarchy and
formatting helpers for Arrow IPC objects.  Enabling
``logging.getLogger("vgi_rpc.wire").setLevel(logging.DEBUG)`` gives
full visibility into what flows over the wire — invaluable for
cross-language interop debugging.

All formatting helpers return ``str`` and never log directly.
They are designed to be called inside ``isEnabledFor`` guards so
there is zero overhead when debug logging is disabled.
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

# ---------------------------------------------------------------------------
# Logger hierarchy: vgi_rpc.wire.*
# ---------------------------------------------------------------------------

wire_request_logger = logging.getLogger("vgi_rpc.wire.request")
"""Request serialization / deserialization."""

wire_response_logger = logging.getLogger("vgi_rpc.wire.response")
"""Response serialization / deserialization."""

wire_batch_logger = logging.getLogger("vgi_rpc.wire.batch")
"""Batch classification (log / error / data dispatch)."""

wire_stream_logger = logging.getLogger("vgi_rpc.wire.stream")
"""Stream session lifecycle."""

wire_transport_logger = logging.getLogger("vgi_rpc.wire.transport")
"""Transport lifecycle (pipe, subprocess)."""

wire_http_logger = logging.getLogger("vgi_rpc.wire.http")
"""HTTP client requests / responses."""

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_MAX_VALUE_LEN = 80
"""Maximum repr length for individual values in fmt_metadata / fmt_kwargs."""


def fmt_schema(schema: pa.Schema) -> str:
    """Format an Arrow schema compactly.

    Returns:
        ``"(a: double, b: double)"`` or ``"(empty)"`` for zero-field schemas.

    """
    if len(schema) == 0:
        return "(empty)"
    fields = ", ".join(f"{f.name}: {f.type}" for f in schema)
    return f"({fields})"


def fmt_metadata(metadata: pa.KeyValueMetadata | None) -> str:
    """Format Arrow custom metadata compactly.

    Returns:
        ``"{vgi_rpc.method='add', vgi_rpc.request_version='1'}"``
        or ``"None"`` when metadata is absent.

    """
    if metadata is None:
        return "None"
    parts: list[str] = []
    for k, v in metadata.items():
        key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else k
        val = v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v
        if len(val) > _MAX_VALUE_LEN:
            val = val[:_MAX_VALUE_LEN] + "..."
        parts.append(f"{key}={val!r}")
    return "{" + ", ".join(parts) + "}"


def fmt_batch(batch: pa.RecordBatch) -> str:
    """Format a RecordBatch summary.

    Returns:
        ``"RecordBatch(rows=1, cols=2, schema=(a: double, b: double), bytes=128)"``

    """
    nbytes = batch.nbytes
    schema_str = fmt_schema(batch.schema)
    return f"RecordBatch(rows={batch.num_rows}, cols={batch.num_columns}, schema={schema_str}, bytes={nbytes})"


def fmt_kwargs(kwargs: dict[str, Any]) -> str:
    """Format keyword arguments compactly.

    Returns:
        ``"a=1.0, b=2.0"`` with long repr values truncated.

    """
    if not kwargs:
        return ""
    parts: list[str] = []
    for k, v in kwargs.items():
        r = repr(v)
        if len(r) > _MAX_VALUE_LEN:
            r = r[:_MAX_VALUE_LEN] + "..."
        parts.append(f"{k}={r}")
    return ", ".join(parts)
