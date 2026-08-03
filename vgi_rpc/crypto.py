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

BACKENDS
--------
Two implementations of the same construction, chosen per platform because
neither wins everywhere. Both produce byte-identical envelopes, so a token
sealed by one opens with the other — the choice is a pure performance
decision and tokens stay portable across a mixed fleet.

The two differ in where their cost sits. PyCryptodome builds a cipher object
in Python per call (~17us on x86_64, ~7us on arm64 — over half a small
seal), then encrypts quickly. PyNaCl is a single C call, but wraps a
libsodium whose speed depends entirely on whether that wheel was built with
SIMD dispatch for the host. So fixed overhead dominates small payloads and
throughput dominates large ones, and which library wins flips with both
platform and payload size. Measured at our 731-byte token:

    macOS arm64     pycryptodome  14.5us   pynacl  17.0us
    Linux x86_64    pycryptodome  37.1us   pynacl   6.3us

At 20 KB the macOS ordering widens to 55us vs 330us (libsodium there has no
SIMD dispatch), which is why PyCryptodome remains right for that platform.

Selection is by install: ``pynacl`` is declared only for platforms where it
wins, so the resolver makes the choice and no runtime probing is needed.
``VGI_RPC_AEAD_BACKEND=pynacl|pycryptodome`` overrides for benchmarking or
to work around a bad wheel.

Requires ``pycryptodome`` (the ``http`` extra). Import this module lazily if your
code path also runs in environments without that extra installed.
"""

from __future__ import annotations

import hashlib
import os
import struct

# XChaCha20-Poly1305 algorithm constants. Fixed by the construction, so they are
# written out rather than read back from the backend.
_KEY_LEN = 32
_NONCE_LEN = 24  # 24 bytes selects XChaCha20 (12 would be IETF ChaCha20)
_TAG_LEN = 16
_VERSION_LEN = 1
_MIN_TOKEN_LEN = _VERSION_LEN + _NONCE_LEN + _TAG_LEN

__all__ = ["AEAD_BACKEND", "SealError", "normalize_key", "open_bytes", "seal_bytes"]


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


def _select_backend() -> str:
    """Resolve which AEAD backend to use for this process.

    Order: explicit override, then PyNaCl when it is installed. PyNaCl is
    declared as a dependency only on platforms where it measured faster (see
    this module's docstring), so "installed" already encodes the platform
    decision — the resolver made it, not a runtime probe.

    Returns:
        ``"pynacl"`` or ``"pycryptodome"``.

    Raises:
        ValueError: The override names an unknown or unavailable backend.
            Failing loudly beats silently running the slow path an operator
            explicitly tried to avoid.

    """
    override = (os.environ.get("VGI_RPC_AEAD_BACKEND") or "").strip().lower()
    if override:
        if override not in ("pynacl", "pycryptodome"):
            msg = f"VGI_RPC_AEAD_BACKEND={override!r} must be 'pynacl' or 'pycryptodome'"
            raise ValueError(msg)
        if override == "pynacl" and not _have_pynacl():
            msg = "VGI_RPC_AEAD_BACKEND=pynacl but PyNaCl is not installed"
            raise ValueError(msg)
        return override
    return "pynacl" if _have_pynacl() else "pycryptodome"


def _have_pynacl() -> bool:
    try:
        import nacl.bindings  # noqa: F401
    except ImportError:
        return False
    return True


AEAD_BACKEND: str = _select_backend()
"""Which AEAD implementation this process uses — see :func:`_select_backend`.

Exposed because it changes performance, never behaviour: both backends emit
byte-identical envelopes, so this is safe to log, assert on in benchmarks,
and vary across a fleet.
"""

if AEAD_BACKEND == "pynacl":
    from nacl.bindings import (
        crypto_aead_xchacha20poly1305_ietf_decrypt as _x_decrypt,
    )
    from nacl.bindings import (
        crypto_aead_xchacha20poly1305_ietf_encrypt as _x_encrypt,
    )

    def _seal(payload: bytes, key: bytes, aad: bytes, nonce: bytes) -> bytes:
        # libsodium returns ciphertext||tag as one buffer, which is already
        # the layout this envelope uses.
        return bytes(_x_encrypt(payload, aad, nonce, key))

    def _open(body: bytes, key: bytes, aad: bytes, nonce: bytes) -> bytes:
        try:
            return bytes(_x_decrypt(body, aad, nonce, key))
        except Exception as exc:
            msg = "token verification failed"
            raise SealError(msg) from exc
else:
    from Crypto.Cipher import ChaCha20_Poly1305

    def _seal(payload: bytes, key: bytes, aad: bytes, nonce: bytes) -> bytes:
        cipher = ChaCha20_Poly1305.new(key=key, nonce=nonce)
        cipher.update(aad)
        ciphertext, tag = cipher.encrypt_and_digest(payload)
        # PyCryptodome returns them separately; concatenating reproduces the
        # identical libsodium layout.
        return ciphertext + tag

    def _open(body: bytes, key: bytes, aad: bytes, nonce: bytes) -> bytes:
        cipher = ChaCha20_Poly1305.new(key=key, nonce=nonce)
        cipher.update(aad)
        try:
            plaintext: bytes = cipher.decrypt_and_verify(body[:-_TAG_LEN], body[-_TAG_LEN:])
        except ValueError as exc:
            msg = "token verification failed"
            raise SealError(msg) from exc
        return plaintext


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
        The sealed envelope: ``version || nonce || ciphertext+tag``. Identical
        whichever backend produced it.

    """
    if not 0 <= version <= 255:
        msg = f"version must fit in one byte, got {version}"
        raise ValueError(msg)
    nonce = os.urandom(_NONCE_LEN)
    return struct.pack("B", version) + nonce + _seal(payload, normalize_key(key), aad, nonce)


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
    body = token[_VERSION_LEN + _NONCE_LEN :]
    return _open(body, normalize_key(key), aad, nonce)
