# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""Sealed grants: the framework's own ``issue_grant`` credential, and its verifier.

``vgi_rpc.Identity.v1``'s ``issue_grant`` mints a standing delegation that
unattended automation later presents *as an ordinary bearer*.  Until this
module nothing accepted one: the loop was open in every port.  A sealed grant
closes it without storage and without author code.  When a deployment
configures a **grant key**, the framework mints grants itself (unless the
worker supplies its own ``mint_grant``) and accepts them back as bearer
credentials.  When it does not, nothing changes -- absent beats
hosted-and-refusing, so no worker grows a credential issuer by upgrading.

Normative: ``tools/cross-port/specs/IDENTITY_V1_SPEC.md`` §9 and
``docs/WIRE_PROTOCOL.md`` §16.  Every port mints and verifies byte-identically;
``vgi_rpc/conformance/grant_token_vectors.json`` pins it.

Token::

    "vgig1." || base64url_nopad( kid(8) || envelope )

    envelope = version(1)=0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad) (ciphertext || tag(16))
    kid      = SHA-256("vgi_rpc.grant.kid.v1\x00" || key)[0:8]
    aad      = "vgi_rpc.grant.v1\x00" || kid || UTF-8(audience)

    payload (little-endian) =
        issued_at   int64      seconds since the Unix epoch
        expires_at  int64
        grant_id    u16 len || UTF-8
        principal   u16 len || UTF-8
        purpose     u16 len || UTF-8
        scope_count u16
        scope       (u16 len || UTF-8) * scope_count

The envelope is the same XChaCha20-Poly1305 construction (and the same
``version || nonce || ciphertext+tag`` layout) every port already implements for
stream-state and call-state tokens, so no port adds a cipher.

**Not individually revocable.**  A sealed grant is valid until it expires; the
lever is a short maximum lifetime and re-issue.  Rotating every key revokes
every grant at once.  There is deliberately no revocation list.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import hashlib
import os
import re
import secrets
import struct
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vgi_rpc.rpc._token_identity import IssuedGrant

__all__ = [
    "DEFAULT_CLOCK_SKEW_SECONDS",
    "DEFAULT_MAX_TTL_SECONDS",
    "GRANT_AUDIENCE_ENV",
    "GRANT_KEYS_ENV",
    "GRANT_MAX_TTL_ENV",
    "GRANT_TOKEN_PREFIX",
    "GrantClaims",
    "GrantInvalidError",
    "GrantKeys",
    "grant_key_id",
    "mint_grant_token",
    "sealed_mint_grant",
    "verify_grant_token",
]

#: Token prefix.  The version is in the prefix so an incompatible format is a
#: different prefix -- routed elsewhere, never half-parsed.
GRANT_TOKEN_PREFIX = "vgig1."

#: Environment variables a worker reads its grant configuration from.  Named
#: like the framework's other ``VGI_RPC_*`` knobs.
GRANT_KEYS_ENV = "VGI_RPC_GRANT_KEYS"
GRANT_AUDIENCE_ENV = "VGI_RPC_GRANT_AUDIENCE"
GRANT_MAX_TTL_ENV = "VGI_RPC_GRANT_MAX_TTL_SECONDS"

#: Longest lifetime a minted grant may have unless the deployment says otherwise.
#: Short on purpose: expiry is the only revocation a sealed grant has.
DEFAULT_MAX_TTL_SECONDS = 7 * 24 * 3600

#: Allowance for clocks disagreeing between the minting and verifying worker.
DEFAULT_CLOCK_SKEW_SECONDS = 60

#: Longest token text considered at all -- the same cap ``introspect_token``
#: applies to a credential, so a verifier is never handed megabytes.
MAX_GRANT_TOKEN_CHARS = 4096

_KEY_LEN = 32
_KID_LEN = 8
_ENVELOPE_VERSION = 1
_KID_DOMAIN = b"vgi_rpc.grant.kid.v1\x00"
_AAD_DOMAIN = b"vgi_rpc.grant.v1\x00"
_B64URL = re.compile(r"[A-Za-z0-9_-]+")
_MAX_FIELD = 0xFFFF


class GrantInvalidError(ValueError):
    """A token carrying the grant prefix could not be accepted.

    One type for every cause on purpose -- malformed, wrong key, wrong
    audience, tampered, expired -- so a caller cannot tell a forged token from
    a stale one except by ``expired`` (set on the instance), which is only
    true once the token was proven authentic -- so it reveals nothing a forger
    could use.
    """

    def __init__(self, detail: str, *, expired: bool = False) -> None:
        """Build the error.

        Args:
            detail: Operator-facing text.  Never contains the token.
            expired: Authentic but outside its lifetime.

        """
        super().__init__(detail)
        self.expired = expired


def grant_key_id(key: bytes) -> bytes:
    r"""Return the 8-byte key id a token names its sealing key with.

    Args:
        key: A 32-byte grant key.

    Returns:
        ``SHA-256("vgi_rpc.grant.kid.v1\x00" || key)[0:8]``.

    """
    return hashlib.sha256(_KID_DOMAIN + key).digest()[:_KID_LEN]


@dataclasses.dataclass(frozen=True)
class GrantKeys:
    """A deployment's grant configuration.

    The first key mints; every key verifies.  Rotation is: add the new key
    first, keep the old one after it until every grant it minted has expired,
    then drop it.

    Attributes:
        keys: 32-byte keys, minting key first.
        audience: Bound into every token's associated data.  Distinct
            deployments that (against advice) share a key still cannot accept
            each other's grants if their audiences differ.
        max_ttl_seconds: Ceiling on a grant's lifetime, at minting and at
            verification.
        clock_skew_seconds: Tolerance applied to ``issued_at`` and ``expires_at``.

    """

    keys: tuple[bytes, ...]
    audience: str = ""
    max_ttl_seconds: int = DEFAULT_MAX_TTL_SECONDS
    clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS

    def __post_init__(self) -> None:
        """Refuse a configuration that could not mint or verify safely.

        Raises:
            ValueError: No key, a key that is not exactly 32 bytes, two keys
                with one id, or a non-positive lifetime.

        """
        if not self.keys:
            raise ValueError("grant configuration needs at least one key")
        for key in self.keys:
            if not isinstance(key, bytes) or len(key) != _KEY_LEN:
                raise ValueError(f"every grant key must be exactly {_KEY_LEN} bytes")
        if len({grant_key_id(k) for k in self.keys}) != len(self.keys):
            raise ValueError("grant keys must be distinct")
        if self.max_ttl_seconds <= 0:
            raise ValueError("max_ttl_seconds must be positive")
        if self.clock_skew_seconds < 0:
            raise ValueError("clock_skew_seconds must not be negative")
        if len(self.audience.encode("utf-8")) > _MAX_FIELD:
            raise ValueError("audience is too long")

    @classmethod
    def parse(
        cls,
        encoded_keys: Iterable[str],
        *,
        audience: str = "",
        max_ttl_seconds: int = DEFAULT_MAX_TTL_SECONDS,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
    ) -> GrantKeys:
        """Build from standard base64 key text, minting key first.

        Args:
            encoded_keys: Each key as standard base64 (padded or not) of
                exactly 32 bytes.
            audience: See :attr:`audience`.
            max_ttl_seconds: See :attr:`max_ttl_seconds`.
            clock_skew_seconds: See :attr:`clock_skew_seconds`.

        Returns:
            The configuration.

        Raises:
            ValueError: A key that is not base64 of exactly 32 bytes.  A worker
                refuses to start rather than run with a key it misread.

        """
        keys: list[bytes] = []
        for index, text in enumerate(encoded_keys):
            stripped = text.strip()
            try:
                key = base64.b64decode(stripped + "=" * (-len(stripped) % 4), validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError(f"grant key #{index + 1} is not valid base64") from exc
            if len(key) != _KEY_LEN:
                raise ValueError(f"grant key #{index + 1} decodes to {len(key)} bytes; exactly {_KEY_LEN} are required")
            keys.append(key)
        return cls(
            keys=tuple(keys),
            audience=audience,
            max_ttl_seconds=max_ttl_seconds,
            clock_skew_seconds=clock_skew_seconds,
        )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> GrantKeys | None:
        """Read the configuration from ``VGI_RPC_GRANT_*``, or ``None`` when no key is set.

        ``VGI_RPC_GRANT_KEYS`` is a comma-separated list of base64 keys,
        minting key first; ``VGI_RPC_GRANT_AUDIENCE`` and
        ``VGI_RPC_GRANT_MAX_TTL_SECONDS`` are optional.

        Args:
            environ: Mapping to read instead of ``os.environ``.

        Returns:
            The configuration, or ``None`` -- grants off.

        Raises:
            ValueError: A malformed key or lifetime.

        """
        env = os.environ if environ is None else environ
        raw = (env.get(GRANT_KEYS_ENV) or "").strip()
        if not raw:
            return None
        ttl_raw = (env.get(GRANT_MAX_TTL_ENV) or "").strip()
        try:
            max_ttl = int(ttl_raw) if ttl_raw else DEFAULT_MAX_TTL_SECONDS
        except ValueError as exc:
            raise ValueError(f"{GRANT_MAX_TTL_ENV}={ttl_raw!r} is not an integer") from exc
        return cls.parse(
            [part for part in raw.split(",") if part.strip()],
            audience=env.get(GRANT_AUDIENCE_ENV, ""),
            max_ttl_seconds=max_ttl,
        )

    def _aad(self, kid: bytes) -> bytes:
        return _AAD_DOMAIN + kid + self.audience.encode("utf-8")


@dataclasses.dataclass(frozen=True)
class GrantClaims:
    """What a verified grant says.

    Attributes:
        principal: Whose standing delegation this is -- the caller it was minted for.
        scopes: What it may do; the worker interprets them.
        purpose: Why it was minted, for the audit trail.
        grant_id: Correlation handle.
        issued_at: Seconds since the Unix epoch.
        expires_at: Seconds since the Unix epoch.

    """

    principal: str
    scopes: tuple[str, ...]
    purpose: str
    grant_id: str
    issued_at: int
    expires_at: int


def _pack_text(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > _MAX_FIELD:
        raise ValueError("grant field longer than 65535 bytes")
    return struct.pack("<H", len(raw)) + raw


def _encode_payload(claims: GrantClaims) -> bytes:
    if len(claims.scopes) > _MAX_FIELD:
        raise ValueError("too many scopes")
    parts = [
        struct.pack("<qq", claims.issued_at, claims.expires_at),
        _pack_text(claims.grant_id),
        _pack_text(claims.principal),
        _pack_text(claims.purpose),
        struct.pack("<H", len(claims.scopes)),
        *(_pack_text(scope) for scope in claims.scopes),
    ]
    return b"".join(parts)


def _decode_payload(payload: bytes) -> GrantClaims:
    """Parse a payload strictly: exact lengths, valid UTF-8, no trailing bytes.

    Args:
        payload: The opened plaintext.

    Returns:
        The claims it carries.

    Raises:
        GrantInvalidError: Anything else.

    """
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(payload):
            raise GrantInvalidError("grant payload is truncated")
        chunk = payload[pos : pos + n]
        pos += n
        return chunk

    def text() -> str:
        (length,) = struct.unpack("<H", take(2))
        try:
            return take(length).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise GrantInvalidError("grant payload is not UTF-8") from exc

    issued_at, expires_at = struct.unpack("<qq", take(16))
    grant_id = text()
    principal = text()
    purpose = text()
    (count,) = struct.unpack("<H", take(2))
    scopes = tuple(text() for _ in range(count))
    if pos != len(payload):
        raise GrantInvalidError("grant payload has trailing bytes")
    return GrantClaims(
        principal=principal,
        scopes=scopes,
        purpose=purpose,
        grant_id=grant_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_strict(text: str) -> bytes:
    """Decode unpadded base64url, rejecting any non-canonical spelling.

    Re-encoding and comparing rejects non-zero trailing bits, so one token has
    exactly one spelling -- no second string that verifies as the same grant.
    """
    if not _B64URL.fullmatch(text) or len(text) % 4 == 1:
        raise GrantInvalidError("grant token is not unpadded base64url")
    raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    if _b64url(raw) != text:
        raise GrantInvalidError("grant token is not canonical base64url")
    return raw


def mint_grant_token(
    keys: GrantKeys,
    *,
    principal: str,
    scopes: Sequence[str],
    purpose: str,
    ttl_seconds: int,
    now: int | None = None,
    grant_id: str | None = None,
    nonce: bytes | None = None,
) -> tuple[str, GrantClaims]:
    """Mint a sealed grant with the first configured key.

    Args:
        keys: The deployment's grant configuration.
        principal: The caller the grant is for.
        scopes: What it may do.
        purpose: Why it is being minted.
        ttl_seconds: Requested lifetime; capped at ``keys.max_ttl_seconds``.
        now: Override the clock (seconds), for tests and vectors.
        grant_id: Override the random grant id, for tests and vectors.
        nonce: Fixed 24-byte nonce, **for test vectors only**.

    Returns:
        The token and the claims it carries.

    Raises:
        ValueError: A non-positive lifetime, or a field too long to encode.

    """
    from vgi_rpc.crypto import seal_bytes

    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    issued_at = int(time.time()) if now is None else int(now)
    claims = GrantClaims(
        principal=principal,
        scopes=tuple(scopes),
        purpose=purpose,
        grant_id=secrets.token_hex(16) if grant_id is None else grant_id,
        issued_at=issued_at,
        expires_at=issued_at + min(int(ttl_seconds), keys.max_ttl_seconds),
    )
    key = keys.keys[0]
    kid = grant_key_id(key)
    envelope = seal_bytes(_encode_payload(claims), key, aad=keys._aad(kid), version=_ENVELOPE_VERSION, nonce=nonce)
    return GRANT_TOKEN_PREFIX + _b64url(kid + envelope), claims


def verify_grant_token(keys: GrantKeys, token: str, *, now: float | None = None) -> GrantClaims:
    """Verify a sealed grant and return its claims.

    Order, normative: prefix, length, base64url, key id, AEAD open, payload,
    then lifetime -- the lifetime is inside the ciphertext, so it is only
    trusted after the tag verified.

    Args:
        keys: The deployment's grant configuration.
        token: The bearer credential, exactly as presented.
        now: Override the clock (seconds), for tests.

    Returns:
        The verified claims.

    Raises:
        GrantInvalidError: For every cause; ``expired`` is set only for an
            authentic grant outside its lifetime.

    """
    from vgi_rpc.crypto import SealError, open_bytes

    if not token.startswith(GRANT_TOKEN_PREFIX):
        raise GrantInvalidError("not a sealed grant")
    if len(token) > MAX_GRANT_TOKEN_CHARS:
        raise GrantInvalidError("grant token is too long")
    raw = _b64url_strict(token[len(GRANT_TOKEN_PREFIX) :])
    kid, envelope = raw[:_KID_LEN], raw[_KID_LEN:]
    key = next((k for k in keys.keys if grant_key_id(k) == kid), None)
    if key is None:
        raise GrantInvalidError("grant was sealed with a key this deployment does not hold")
    try:
        payload = open_bytes(envelope, key, aad=keys._aad(kid), version=_ENVELOPE_VERSION)
    except SealError as exc:
        raise GrantInvalidError("grant failed verification") from exc
    claims = _decode_payload(payload)
    if not claims.principal:
        raise GrantInvalidError("grant names no principal")
    if claims.expires_at <= claims.issued_at or claims.expires_at - claims.issued_at > keys.max_ttl_seconds:
        raise GrantInvalidError("grant lifetime exceeds this deployment's maximum")
    current = time.time() if now is None else now
    skew = keys.clock_skew_seconds
    if claims.issued_at > current + skew:
        raise GrantInvalidError("grant is not yet valid", expired=True)
    if current >= claims.expires_at + skew:
        raise GrantInvalidError("grant has expired", expired=True)
    return claims


def sealed_mint_grant(keys: GrantKeys) -> Callable[[str, str, list[str], int], IssuedGrant]:
    """Return a ``mint_grant`` hook that issues sealed grants.

    What :class:`~vgi_rpc.rpc._token_identity.IdentityImpl` installs when a
    grant key is configured and the worker supplied no hook of its own.

    Args:
        keys: The deployment's grant configuration.

    Returns:
        ``(principal, purpose, scopes, ttl_seconds) -> IssuedGrant``.

    """
    from vgi_rpc.rpc._token_identity import GrantRefusedError, IssuedGrant

    def mint(principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
        if ttl_seconds <= 0:
            raise GrantRefusedError("ttl_seconds must be positive")
        try:
            token, claims = mint_grant_token(
                keys, principal=principal, scopes=scopes, purpose=purpose, ttl_seconds=ttl_seconds
            )
        except ValueError as exc:
            raise GrantRefusedError(str(exc)) from exc
        return IssuedGrant(token=token, expires_at=float(claims.expires_at), grant_id=claims.grant_id)

    return mint
