# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the generic AEAD seal/open primitive (``vgi_rpc.crypto``)."""

from __future__ import annotations

import os

import pytest

from vgi_rpc import crypto
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


# ---------------------------------------------------------------------------
# Backend interoperability
#
# vgi_rpc.crypto picks its AEAD implementation per platform (see the BACKENDS
# section of that module). The whole design rests on the two producing the
# same construction: tokens must stay portable across a fleet whose nodes may
# not have made the same choice, and an operator must be able to flip
# VGI_RPC_AEAD_BACKEND without invalidating tokens already in flight. If these
# ever diverge the symptom is not a test failure but intermittent 400s under
# load, so it is worth asserting directly.
# ---------------------------------------------------------------------------


def _pynacl_available() -> bool:
    try:
        import nacl.bindings  # noqa: F401
    except ImportError:
        return False
    return True


@pytest.mark.skipif(not _pynacl_available(), reason="PyNaCl not installed on this platform")
@pytest.mark.parametrize("size", [0, 1, 731, 20_000])
def test_backends_produce_identical_envelopes(size: int) -> None:
    """Given the same nonce, both backends emit byte-identical output."""
    from Crypto.Cipher import ChaCha20_Poly1305
    from nacl.bindings import crypto_aead_xchacha20poly1305_ietf_encrypt

    key = b"\x11" * 32
    aad = b"identity-binding"
    nonce = b"\x22" * 24
    payload = os.urandom(size)

    cipher = ChaCha20_Poly1305.new(key=key, nonce=nonce)
    cipher.update(aad)
    ciphertext, tag = cipher.encrypt_and_digest(payload)

    assert ciphertext + tag == crypto_aead_xchacha20poly1305_ietf_encrypt(payload, aad, nonce, key)


@pytest.mark.skipif(not _pynacl_available(), reason="PyNaCl not installed on this platform")
@pytest.mark.parametrize("size", [0, 1, 731, 20_000])
def test_each_backend_opens_the_other_seal(size: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """A token sealed under one backend opens under the other, and vice versa."""
    import importlib

    key = os.urandom(32)
    aad = b"cross-backend"
    payload = os.urandom(size)

    sealed: dict[str, bytes] = {}
    for backend in ("pycryptodome", "pynacl"):
        monkeypatch.setenv("VGI_RPC_AEAD_BACKEND", backend)
        mod = importlib.reload(crypto)
        assert backend == mod.AEAD_BACKEND
        sealed[backend] = mod.seal_bytes(payload, key, aad=aad, version=7)

    for backend in ("pycryptodome", "pynacl"):
        monkeypatch.setenv("VGI_RPC_AEAD_BACKEND", backend)
        mod = importlib.reload(crypto)
        for producer, token in sealed.items():
            assert mod.open_bytes(token, key, aad=aad, version=7) == payload, (
                f"{backend} could not open a token sealed by {producer}"
            )
            # Tamper detection must survive the swap too.
            broken = bytearray(token)
            broken[-1] ^= 0x01
            with pytest.raises(crypto.SealError):
                mod.open_bytes(bytes(broken), key, aad=aad, version=7)

    monkeypatch.delenv("VGI_RPC_AEAD_BACKEND", raising=False)
    importlib.reload(crypto)


def test_unknown_backend_override_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo'd override fails loudly rather than silently using the slow path."""
    import importlib

    monkeypatch.setenv("VGI_RPC_AEAD_BACKEND", "libsodium")
    with pytest.raises(ValueError, match="must be 'pynacl' or 'pycryptodome'"):
        importlib.reload(crypto)
    monkeypatch.delenv("VGI_RPC_AEAD_BACKEND", raising=False)
    importlib.reload(crypto)
