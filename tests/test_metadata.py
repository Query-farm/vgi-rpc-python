"""Tests for vgi_rpc.metadata — metadata helpers."""

from __future__ import annotations

import pyarrow as pa

from vgi_rpc.metadata import (
    decode_metadata,
    encode_metadata,
    merge_metadata,
    strip_keys,
)

# ---------------------------------------------------------------------------
# encode_metadata / decode_metadata
# ---------------------------------------------------------------------------


class TestEncodeDecodeMetadata:
    """Tests for encode_metadata and decode_metadata."""

    def test_round_trip(self) -> None:
        """Encode then decode returns original dict."""
        original = {"key1": "value1", "key2": "value2"}
        encoded = encode_metadata(original)
        decoded = decode_metadata(encoded)
        assert decoded == original

    def test_decode_none_returns_none(self) -> None:
        """decode_metadata(None) returns None."""
        assert decode_metadata(None) is None

    def test_decode_string_keys(self) -> None:
        """Metadata with string keys (not bytes) decodes correctly."""
        md = pa.KeyValueMetadata({"str_key": "str_value"})  # type: ignore[dict-item]
        decoded = decode_metadata(md)
        assert decoded is not None
        assert decoded["str_key"] == "str_value"


# ---------------------------------------------------------------------------
# merge_metadata
# ---------------------------------------------------------------------------


class TestMergeMetadata:
    """Tests for merge_metadata."""

    def test_merge_two(self) -> None:
        """Merge two metadata dicts, later wins on conflict."""
        a = pa.KeyValueMetadata({b"x": b"1"})
        b = pa.KeyValueMetadata({b"x": b"2", b"y": b"3"})
        merged = merge_metadata(a, b)
        assert merged is not None
        assert merged[b"x"] == b"2"
        assert merged[b"y"] == b"3"

    def test_merge_all_none(self) -> None:
        """All None inputs returns None."""
        assert merge_metadata(None, None) is None

    def test_merge_with_none(self) -> None:
        """None inputs are skipped."""
        a = pa.KeyValueMetadata({b"x": b"1"})
        merged = merge_metadata(None, a, None)
        assert merged is not None
        assert merged[b"x"] == b"1"

    def test_merge_string_keys(self) -> None:
        """Metadata with string keys is converted to bytes."""
        md = pa.KeyValueMetadata({"str_key": "str_value"})  # type: ignore[dict-item]
        merged = merge_metadata(md)
        assert merged is not None
        assert merged[b"str_key"] == b"str_value"


# ---------------------------------------------------------------------------
# strip_keys
# ---------------------------------------------------------------------------


class TestStripKeys:
    """Tests for strip_keys."""

    def test_strip_none_returns_none(self) -> None:
        """strip_keys(None, ...) returns None."""
        assert strip_keys(None, b"any") is None

    def test_strip_key(self) -> None:
        """Specified key is removed."""
        md = pa.KeyValueMetadata({b"keep": b"1", b"remove": b"2"})
        result = strip_keys(md, b"remove")
        assert result is not None
        assert b"keep" in dict(result.items())
        assert b"remove" not in dict(result.items())

    def test_strip_all_returns_none(self) -> None:
        """Stripping all keys returns None."""
        md = pa.KeyValueMetadata({b"a": b"1"})
        assert strip_keys(md, b"a") is None

    def test_strip_with_string_keys(self) -> None:
        """Metadata with string keys is handled correctly."""
        md = pa.KeyValueMetadata({"str_key": "str_value", "keep": "yes"})  # type: ignore[dict-item]
        result = strip_keys(md, b"str_key")
        assert result is not None
        # The remaining key should be preserved
        items = dict(result.items())
        assert len(items) == 1

    def test_strip_nonexistent_key(self) -> None:
        """Stripping a key that doesn't exist returns all entries."""
        md = pa.KeyValueMetadata({b"a": b"1", b"b": b"2"})
        result = strip_keys(md, b"nonexistent")
        assert result is not None
        items = dict(result.items())
        assert len(items) == 2
