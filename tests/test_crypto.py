# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the generic AEAD seal/open primitive (``vgi_rpc.crypto``)."""

from __future__ import annotations

import pytest

from vgi_rpc.crypto import SealError, normalize_key, open_bytes, seal_bytes


def test_seal_open_roundtrip() -> None:
    """A sealed payload opens back to the original plaintext."""
    key = b"\x01" * 32
    payload = b"the quick brown fox"
    token = seal_bytes(payload, key, aad=b"ctx")
    assert open_bytes(token, key, aad=b"ctx") == payload


def test_empty_payload_roundtrips() -> None:
    """An empty payload is a valid input."""
    key = b"k"
    token = seal_bytes(b"", key, aad=b"")
    assert open_bytes(token, key, aad=b"") == b""


def test_nonce_is_random_per_seal() -> None:
    """Two seals of the same payload produce different ciphertext (random nonce)."""
    key = b"\x02" * 32
    a = seal_bytes(b"same", key, aad=b"x")
    b = seal_bytes(b"same", key, aad=b"x")
    assert a != b
    assert open_bytes(a, key, aad=b"x") == open_bytes(b, key, aad=b"x") == b"same"


def test_wrong_key_fails() -> None:
    """Opening with a different key fails."""
    token = seal_bytes(b"secret", b"key-a", aad=b"x")
    with pytest.raises(SealError):
        open_bytes(token, b"key-b", aad=b"x")


def test_wrong_aad_fails() -> None:
    """Opening with different associated data fails — this is the replay guard."""
    key = b"\x03" * 32
    token = seal_bytes(b"secret", key, aad=b"principal-alice")
    with pytest.raises(SealError):
        open_bytes(token, key, aad=b"principal-bob")


def test_tampered_ciphertext_fails() -> None:
    """Flipping any byte of the envelope fails the tag check."""
    key = b"\x04" * 32
    token = bytearray(seal_bytes(b"secret", key, aad=b"x"))
    token[-1] ^= 0x01
    with pytest.raises(SealError):
        open_bytes(bytes(token), key, aad=b"x")


def test_wrong_version_fails() -> None:
    """A token sealed under one version byte does not open under another."""
    key = b"\x05" * 32
    token = seal_bytes(b"secret", key, aad=b"x", version=7)
    assert open_bytes(token, key, aad=b"x", version=7) == b"secret"
    with pytest.raises(SealError):
        open_bytes(token, key, aad=b"x", version=8)


def test_too_short_token_fails() -> None:
    """A token shorter than the minimum envelope size fails cleanly."""
    with pytest.raises(SealError):
        open_bytes(b"\x01short", b"key", aad=b"x")


def test_invalid_version_rejected_on_seal() -> None:
    """A version that does not fit in one byte is rejected at seal time."""
    with pytest.raises(ValueError, match="one byte"):
        seal_bytes(b"x", b"key", aad=b"x", version=256)


def test_normalize_key_passthrough_and_stretch() -> None:
    """A 32-byte key passes through; any other length is SHA-256 stretched to 32."""
    exact = b"\x09" * 32
    assert normalize_key(exact) is exact
    short = normalize_key(b"short")
    assert len(short) == 32
    assert normalize_key(b"short") == short  # deterministic


def test_normalized_and_raw_32byte_keys_interoperate() -> None:
    """A non-32-byte key seals/opens consistently via the SHA-256 stretch."""
    token = seal_bytes(b"secret", b"operator-supplied-key", aad=b"x")
    assert open_bytes(token, b"operator-supplied-key", aad=b"x") == b"secret"
