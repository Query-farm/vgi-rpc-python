# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

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

import pyarrow as pa

from vgi_rpc.metadata import CALL_STATE_KEY, LOCATION_KEY, STATE_KEY

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
"""Maximum length for individual metadata values rendered by fmt_metadata."""

#: Metadata whose values are rendered as a size only. State tokens serialize
#: whatever the call was given (secrets included) and are replayable; an
#: external location is commonly a presigned URL whose query string is the
#: credential. An 80-character prefix of either is still too much to log.
_OPAQUE_METADATA_KEYS = frozenset({STATE_KEY, CALL_STATE_KEY, LOCATION_KEY})


def fmt_schema(schema: pa.Schema) -> str:
    """Format an Arrow schema compactly.

    Args:
        schema: The Arrow schema to render.

    Returns:
        ``"(a: double, b: double)"`` or ``"(empty)"`` for zero-field schemas.

    """
    if len(schema) == 0:
        return "(empty)"
    fields = ", ".join(f"{f.name}: {f.type}" for f in schema)
    return f"({fields})"


def fmt_metadata(metadata: pa.KeyValueMetadata | None) -> str:
    """Format Arrow custom metadata compactly.

    Args:
        metadata: The Arrow key-value metadata to render, or ``None``.

    Returns:
        ``"{vgi_rpc.method='add', vgi_rpc.request_version='1'}"``
        or ``"None"`` when metadata is absent.

    """
    if metadata is None:
        return "None"
    parts: list[str] = []
    for k, v in metadata.items():
        key = k.decode("utf-8", errors="replace")
        if k in _OPAQUE_METADATA_KEYS:
            parts.append(f"{key}=<{len(v)} bytes>")
            continue
        val = v.decode("utf-8", errors="replace")
        if len(val) > _MAX_VALUE_LEN:
            val = val[:_MAX_VALUE_LEN] + "..."
        parts.append(f"{key}={val!r}")
    return "{" + ", ".join(parts) + "}"


def fmt_batch(batch: pa.RecordBatch) -> str:
    """Format a RecordBatch summary.

    Args:
        batch: The Arrow record batch to summarize.

    Returns:
        ``"RecordBatch(rows=1, cols=2, schema=(a: double, b: double), bytes=128)"``

    """
    nbytes = batch.nbytes
    schema_str = fmt_schema(batch.schema)
    return f"RecordBatch(rows={batch.num_rows}, cols={batch.num_columns}, schema={schema_str}, bytes={nbytes})"


def fmt_kwargs(kwargs: dict[str, object]) -> str:
    """Format keyword arguments as names and Python types -- never values.

    This feeds the ``vgi_rpc.wire.request`` DEBUG lines on both client and
    server. It used to render ``repr(value)``, which put every parameter --
    passwords and API keys included -- into the log of anyone who turned on
    wire debugging. The framework cannot tell a secret parameter from any
    other, so no value is rendered at all.

    Args:
        kwargs: Mapping of argument name to value to describe.

    Returns:
        ``"a: float, b: str"``.

    """
    if not kwargs:
        return ""
    return ", ".join(f"{k}: {type(v).__name__}" for k, v in kwargs.items())
