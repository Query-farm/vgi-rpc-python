"""Shared helpers for ``pa.KeyValueMetadata`` used across the RPC framework.

Centralises well-known metadata keys (including the wire-protocol version
constant ``REQUEST_VERSION``), encoding/decoding, merging, and key-stripping
so that ``rpc/``, ``http/``, ``log.py``, ``external.py``, ``shm.py``,
and ``introspect.py`` share a single implementation instead of
duplicating these patterns.
"""

from __future__ import annotations

import pyarrow as pa

__all__ = [
    "DESCRIBE_VERSION_KEY",
    "LOCATION_FETCH_MS_KEY",
    "LOCATION_KEY",
    "LOCATION_SOURCE_KEY",
    "LOG_EXTRA_KEY",
    "LOG_LEVEL_KEY",
    "LOG_MESSAGE_KEY",
    "PROTOCOL_NAME_KEY",
    "REQUEST_ID_KEY",
    "REQUEST_VERSION",
    "REQUEST_VERSION_KEY",
    "RPC_METHOD_KEY",
    "SERVER_ID_KEY",
    "SHM_LENGTH_KEY",
    "SHM_OFFSET_KEY",
    "SHM_SEGMENT_NAME_KEY",
    "SHM_SEGMENT_SIZE_KEY",
    "SHM_SOURCE_KEY",
    "STATE_KEY",
    "TRACEPARENT_KEY",
    "TRACESTATE_KEY",
    "encode_metadata",
    "merge_metadata",
    "strip_keys",
]

# ---------------------------------------------------------------------------
# Well-known metadata keys (bytes, matching what appears on the wire)
# ---------------------------------------------------------------------------

RPC_METHOD_KEY = b"vgi_rpc.method"
STATE_KEY = b"vgi_rpc.stream_state"
LOG_LEVEL_KEY = b"vgi_rpc.log_level"
LOG_MESSAGE_KEY = b"vgi_rpc.log_message"
LOG_EXTRA_KEY = b"vgi_rpc.log_extra"
REQUEST_VERSION_KEY = b"vgi_rpc.request_version"
REQUEST_VERSION = b"1"

SERVER_ID_KEY = b"vgi_rpc.server_id"
REQUEST_ID_KEY = b"vgi_rpc.request_id"

LOCATION_KEY = b"vgi_rpc.location"
LOCATION_FETCH_MS_KEY = b"vgi_rpc.location.fetch_ms"
LOCATION_SOURCE_KEY = b"vgi_rpc.location.source"

# Shared memory pointer batch (on data batch metadata)
SHM_OFFSET_KEY = b"vgi_rpc.shm_offset"
SHM_LENGTH_KEY = b"vgi_rpc.shm_length"

# Shared memory provenance (on resolved batch metadata — mirrors LOCATION_SOURCE_KEY)
SHM_SOURCE_KEY = b"vgi_rpc.shm_source"

# Shared memory segment identity (on request batch custom metadata — identifies
# the segment itself at session level, distinct from per-batch SHM_OFFSET/LENGTH)
SHM_SEGMENT_NAME_KEY = b"vgi_rpc.shm_segment_name"
SHM_SEGMENT_SIZE_KEY = b"vgi_rpc.shm_segment_size"

# Introspection (__describe__ response batch metadata)
PROTOCOL_NAME_KEY = b"vgi_rpc.protocol_name"
DESCRIBE_VERSION_KEY = b"vgi_rpc.describe_version"

# W3C trace context propagation (on request batch custom metadata)
TRACEPARENT_KEY = b"traceparent"
TRACESTATE_KEY = b"tracestate"

# ---------------------------------------------------------------------------
# Encode / decode
# ---------------------------------------------------------------------------


def encode_metadata(metadata: dict[str, str]) -> pa.KeyValueMetadata:
    """Encode a plain ``dict[str, str]`` to ``pa.KeyValueMetadata`` with bytes keys/values."""
    return pa.KeyValueMetadata({k.encode(): v.encode() for k, v in metadata.items()})


# ---------------------------------------------------------------------------
# Merge / strip
# ---------------------------------------------------------------------------


def merge_metadata(
    *metadata_dicts: pa.KeyValueMetadata | dict[bytes, bytes] | dict[str, str] | None,
) -> pa.KeyValueMetadata | None:
    """Merge multiple metadata objects into one.

    Later arguments override earlier ones on key conflict.

    Args:
        *metadata_dicts: Metadata objects to merge. ``None`` values are skipped.

    Returns:
        Merged ``KeyValueMetadata``, or ``None`` if all inputs were ``None``/empty.

    """
    result: dict[bytes, bytes] = {}
    for md in metadata_dicts:
        if md is not None:
            for k, v in md.items():
                key = k if isinstance(k, bytes) else k.encode()
                val = v if isinstance(v, bytes) else v.encode()
                result[key] = val
    return pa.KeyValueMetadata(result) if result else None


def strip_keys(
    metadata: pa.KeyValueMetadata | None,
    *keys: bytes,
) -> pa.KeyValueMetadata | None:
    """Return a copy of *metadata* with the specified *keys* removed.

    Returns ``None`` when the result would be empty or *metadata* is ``None``.
    """
    if metadata is None:
        return None
    stripped: dict[bytes, bytes] = {}
    keys_set = set(keys)
    for k, v in metadata.items():
        k_bytes = k if isinstance(k, bytes) else k.encode()
        if k_bytes not in keys_set:
            v_bytes = v if isinstance(v, bytes) else v.encode()
            stripped[k_bytes] = v_bytes
    return pa.KeyValueMetadata(stripped) if stripped else None
