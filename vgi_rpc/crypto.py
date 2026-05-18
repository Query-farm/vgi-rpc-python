# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Generic AEAD seal/open primitive (XChaCha20-Poly1305).

A small, audited envelope for any value that must be both confidential and
authenticated on the wire. Two consumers today:

- HTTP streaming state tokens (``vgi_rpc.http.server._state_token``).
- VGI catalog opaque-data envelopes (``attach_opaque_data`` /
  ``transaction_opaque_data`` — sealed so a value minted for one principal
  cannot be replayed by another).

Payload framing is the caller's concern; this module only handles the AEAD
envelope. Bind identity (or any context that must not be swapped) into the
``aad`` — associated data is authenticated but not encrypted, and a mismatch
on open fails the tag check.

Wire format::

    version (1 byte) || nonce (24 bytes) || ciphertext+tag

Requires ``pynacl`` (the ``http`` extra). Import this module lazily if your
code path also runs in environments without that extra installed.
"""

from __future__ import annotations

import hashlib
import struct

from nacl import bindings as _nacl
from nacl.exceptions import CryptoError

# XChaCha20-Poly1305 IETF algorithm constants.
_KEY_LEN = _nacl.crypto_aead_xchacha20poly1305_ietf_KEYBYTES  # 32
_NONCE_LEN = _nacl.crypto_aead_xchacha20poly1305_ietf_NPUBBYTES  # 24
_TAG_LEN = _nacl.crypto_aead_xchacha20poly1305_ietf_ABYTES  # 16
_VERSION_LEN = 1
_MIN_TOKEN_LEN = _VERSION_LEN + _NONCE_LEN + _TAG_LEN

__all__ = ["SealError", "normalize_key", "open_bytes", "seal_bytes"]


class SealError(Exception):
    """Raised by :func:`open_bytes` for any token it cannot open.

    Malformed, wrong-version, tampered, wrong-key, and wrong-AAD tokens all map
    to this single exception so callers cannot distinguish them (e.g. via
    exception type or message) — "wrong AAD" (cross-principal replay) is
    indistinguishable from "garbage input".
    """


def normalize_key(key: bytes) -> bytes:
    """Stretch or compress an operator-supplied key to the 32-byte AEAD key length.

    XChaCha20-Poly1305 requires exactly 32 bytes. Operators may supply keys of
    any length; hashing through SHA-256 yields a 32-byte pseudo-random key for
    any input — collision-resistant, deterministic, and indistinguishable from
    a directly-supplied 32-byte key to an attacker who never sees the input. A
    key already 32 bytes long is used as-is.
    """
    if len(key) == _KEY_LEN:
        return key
    return hashlib.sha256(key).digest()


def seal_bytes(payload: bytes, key: bytes, *, aad: bytes, version: int = 1) -> bytes:
    """Seal ``payload`` into an authenticated-encrypted envelope.

    Args:
        payload: Plaintext bytes to encrypt.
        key: Master key. Any length — normalized via :func:`normalize_key`.
        aad: Associated data: authenticated but not encrypted. The identical
            ``aad`` must be supplied to :func:`open_bytes`. Bind identity or
            any non-swappable context here.
        version: 1-byte format selector (0-255), echoed as the first output
            byte. Lets a caller version its own envelope format independently.

    Returns:
        The sealed envelope: ``version || nonce || ciphertext+tag``.

    """
    if not 0 <= version <= 255:
        msg = f"version must fit in one byte, got {version}"
        raise ValueError(msg)
    nonce = _nacl.randombytes(_NONCE_LEN)
    ciphertext: bytes = _nacl.crypto_aead_xchacha20poly1305_ietf_encrypt(payload, aad, nonce, normalize_key(key))
    return struct.pack("B", version) + nonce + ciphertext


def open_bytes(token: bytes, key: bytes, *, aad: bytes, version: int = 1) -> bytes:
    """Open and verify an envelope produced by :func:`seal_bytes`.

    Args:
        token: The sealed envelope.
        key: Master key — must match the key used to seal.
        aad: Associated data — must match the ``aad`` used to seal.
        version: Expected 1-byte format selector.

    Returns:
        The decrypted plaintext.

    Raises:
        SealError: On any malformed, wrong-version, tampered, wrong-key, or
            wrong-AAD token. All failure modes are indistinguishable.

    """
    if len(token) < _MIN_TOKEN_LEN or token[0] != version:
        msg = "malformed or wrong-version token"
        raise SealError(msg)
    nonce = token[_VERSION_LEN : _VERSION_LEN + _NONCE_LEN]
    ciphertext = token[_VERSION_LEN + _NONCE_LEN :]
    try:
        plaintext: bytes = _nacl.crypto_aead_xchacha20poly1305_ietf_decrypt(ciphertext, aad, nonce, normalize_key(key))
    except CryptoError as exc:
        msg = "token verification failed"
        raise SealError(msg) from exc
    return plaintext
