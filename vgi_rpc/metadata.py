"""Shared helpers for ``pa.KeyValueMetadata`` used across the RPC framework.

Centralises well-known metadata keys, encoding/decoding, merging, and
key-stripping so that ``rpc.py``, ``http.py``, and ``utils.py`` share a
single implementation instead of duplicating these patterns.
"""

from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Well-known metadata keys (bytes, matching what appears on the wire)
# ---------------------------------------------------------------------------

RPC_METHOD_KEY = b"vgi_rpc.method"
BIDI_STATE_KEY = b"vgi_rpc.bidi_state"
LOG_LEVEL_KEY = b"vgi_rpc.log_level"
LOG_MESSAGE_KEY = b"vgi_rpc.log_message"
LOG_EXTRA_KEY = b"vgi_rpc.log_extra"

# ---------------------------------------------------------------------------
# Encode / decode
# ---------------------------------------------------------------------------


def encode_metadata(metadata: dict[str, str]) -> pa.KeyValueMetadata:
    """Encode a plain ``dict[str, str]`` to ``pa.KeyValueMetadata`` with bytes keys/values."""
    return pa.KeyValueMetadata({k.encode(): v.encode() for k, v in metadata.items()})


def decode_metadata(
    metadata: pa.KeyValueMetadata | None,
) -> dict[str, str] | None:
    """Decode ``pa.KeyValueMetadata`` to a plain ``dict[str, str]``.

    Returns ``None`` when *metadata* is ``None``.
    """
    if metadata is None:
        return None
    return {
        k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
        for k, v in metadata.items()
    }


# ---------------------------------------------------------------------------
# Merge / strip
# ---------------------------------------------------------------------------


def merge_metadata(
    *metadata_dicts: pa.KeyValueMetadata | dict[bytes, bytes] | None,
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
