# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Proxy proof: HMAC evidence that a request arrived through a trusted proxy.

A proxy mints a per-request HMAC-SHA256 proof over a timestamp, a nonce and the
worker's own identifier, keyed by a secret shared only with that worker. The
worker recomputes and compares in constant time.

The proof establishes the **hop**, never the caller. It is ANDed with whatever
authenticates the user — see :func:`vgi_rpc.http.require_all` — and is not an
alternative credential.

Unlike schemes that forward an assertion about what happened at a TLS
terminator, a proof cannot be produced by someone who merely reaches the worker
directly: without the secret there is nothing to replay. That is the property
the feature exists for.

The normative cross-language contract is ``docs/proxy-proof-spec.md``; this
module is its reference implementation. No dependencies beyond the standard
library and ``falcon``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import re
import secrets as _secrets
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal

import falcon

from vgi_rpc.http._bearer import PreconditionGate
from vgi_rpc.http._common import PROOF_HEADER, PROOF_REQUIRED_HEADER
from vgi_rpc.http._replay import DEFAULT_CAPACITY, NonceCache

__all__ = [
    "PROOF_HEADER",
    "PROOF_REQUIRED_HEADER",
    "ProofError",
    "ProxyProofConfig",
    "derive_secret",
    "mint_proof",
    "proxy_proof_gate",
    "verify_proof",
]

_logger = logging.getLogger(__name__)

CLAIMS_KEY = "vgi_proxy_proof"
GATE_NAME = "vgi_proxy_proof"

_VERSION = "v1"
_DOMAIN_PREFIX = b"vgi.proxy.proof.v1"
_DERIVE_LABEL = b"vgi.proxy.proof.v1/"
_MAX_HEADER_BYTES = 512
_SECRET_LEN = 32
_NONCE_BYTES = 16

# Charsets are load-bearing, not cosmetic: the canonical string is
# NUL-separated, so framing is only unambiguous because no field can contain a
# NUL (and `kid` cannot contain the '.' that separates wire fields). Validation
# happens before any MAC is computed.
_KID_RE = re.compile(r"\A[A-Za-z0-9_-]{1,64}\Z")
_TS_RE = re.compile(r"\A[0-9]{1,20}\Z")
_NONCE_RE = re.compile(r"\A[A-Za-z0-9_-]{22}\Z")
_ORIGIN_RE = re.compile(r"\A[A-Za-z0-9._:/-]{1,255}\Z")
# The MAC is length- and charset-checked rather than left to the base64
# decoder. Decoders disagree: Python's silently drops non-alphabet bytes while
# Go, Rust and Java raise — so without this check the same malformed token
# would be reported as `bad_mac` by one port and `malformed` by another, and
# the reason code is part of the wire contract.
_MAC_RE = re.compile(r"\A[A-Za-z0-9_-]{43}\Z")

Mode = Literal["off", "allow", "require"]
MODES: tuple[Mode, ...] = ("off", "allow", "require")


class ProofError(PermissionError):
    """A proof was absent, malformed, or did not verify.

    Subclasses :class:`PermissionError` deliberately. ``chain_authenticate``
    swallows :class:`ValueError` to try the next credential; a precondition
    must not be swallowed that way, and ``PermissionError`` propagates.

    ``reason`` carries a code from the closed set in the specification. It is
    safe to log and to expose as a metric label, but never safe to return to
    the caller, since a request can steer it (``unknown_kid``).
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        """Create a proof failure carrying its reason code."""
        super().__init__(detail or reason)
        self.reason = reason


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def derive_secret(base_key: bytes, proxy_id: str, origin_id: str) -> bytes:
    """Derive the secret shared between one proxy and one worker.

    A worker is configured with its derived secret only, never ``base_key`` —
    otherwise it could mint proofs accepted by its siblings.

    Args:
        base_key: 32 raw bytes held only by the proxy.
        proxy_id: Identifier of the minting proxy deployment.
        origin_id: Identifier of the destination worker.

    Returns:
        A 32-byte secret.

    Raises:
        ValueError: If the key length or either identifier is invalid.

    """
    if len(base_key) != _SECRET_LEN:
        raise ValueError(f"base_key must be exactly {_SECRET_LEN} bytes, got {len(base_key)}")
    for name, value in (("proxy_id", proxy_id), ("origin_id", origin_id)):
        if not _ORIGIN_RE.match(value):
            raise ValueError(f"{name} must match {_ORIGIN_RE.pattern!r}, got {value!r}")
    # NUL-separated, and neither identifier can contain NUL — so
    # ("a", "b\x00c") cannot collide with ("a\x00b", "c").
    message = _DERIVE_LABEL + proxy_id.encode() + b"\x00" + origin_id.encode()
    return hmac.new(base_key, message, hashlib.sha256).digest()


def canonical_string(kid: str, ts: str, nonce: str, origin_id: str) -> bytes:
    """Build the MAC input.

    ``origin_id`` is folded in but never transmitted: the worker supplies its
    own. That is what makes a proof minted for one worker fail at every other
    worker even if secrets were misconfigured to overlap.
    """
    return b"\x00".join(
        (
            _DOMAIN_PREFIX,
            kid.encode(),
            ts.encode(),
            nonce.encode(),
            origin_id.encode(),
        ),
    )


def mint_proof(
    secret: bytes,
    kid: str,
    origin_id: str,
    *,
    now: int | None = None,
    nonce: str | None = None,
) -> str:
    """Mint a proof token.

    Args:
        secret: The 32-byte secret shared with ``origin_id``.
        kid: Key id, which also identifies the minting proxy to the worker.
        origin_id: Identifier of the destination worker.
        now: Unix seconds, injectable for tests. Wall clock by default — the
            value is compared across processes, so a monotonic clock (which
            measures uptime) would be meaningless to the receiver.
        nonce: Injectable for tests. A fresh CSPRNG value by default; reusing
            one across requests trips the receiver's replay cache.

    Returns:
        The ``VGI-Proxy-Proof`` header value.

    Raises:
        ValueError: If ``kid`` or ``origin_id`` is invalid.

    """
    if not _KID_RE.match(kid):
        raise ValueError(f"kid must match {_KID_RE.pattern!r}, got {kid!r}")
    if not _ORIGIN_RE.match(origin_id):
        raise ValueError(f"origin_id must match {_ORIGIN_RE.pattern!r}, got {origin_id!r}")
    ts = str(int(time.time()) if now is None else now)
    if nonce is None:
        nonce = _b64(_secrets.token_bytes(_NONCE_BYTES))
    mac = hmac.new(secret, canonical_string(kid, ts, nonce, origin_id), hashlib.sha256).digest()
    return f"{_VERSION}.{kid}.{ts}.{nonce}.{_b64(mac)}"


def verify_proof(
    token: str,
    *,
    secrets: Mapping[str, tuple[bytes, str]],
    origin_id: str,
    skew_seconds: int = 30,
    nonce_cache: NonceCache | None = None,
    now: int | None = None,
) -> dict[str, str]:
    """Verify a proof token, returning the claims to record.

    Cheap rejections run before any MAC is computed, so an unparseable header
    costs a few regex matches rather than a hash.

    Args:
        token: The raw header value.
        secrets: Maps ``kid`` to ``(secret, label)``. The label is the
            operator's name for the calling proxy.
        origin_id: This worker's identifier.
        skew_seconds: Half-width of the acceptance window.
        nonce_cache: Replay cache. When ``None``, replay within the window is
            possible and only the timestamp bounds it.
        now: Unix seconds, injectable for tests.

    Returns:
        Claims including the verified proxy label.

    Raises:
        ProofError: On any failure, carrying a reason code.

    """
    if len(token) > _MAX_HEADER_BYTES:
        raise ProofError("malformed", "proof header too long")

    parts = token.split(".")
    if len(parts) != 5:
        raise ProofError("malformed", f"expected 5 fields, got {len(parts)}")
    version, kid, ts_raw, nonce, mac_b64 = parts
    if version != _VERSION:
        raise ProofError("malformed", f"unsupported version {version!r}")
    if not _KID_RE.match(kid):
        raise ProofError("malformed", "kid charset")
    if not _TS_RE.match(ts_raw):
        raise ProofError("malformed", "ts charset")
    if not _NONCE_RE.match(nonce):
        raise ProofError("malformed", "nonce charset")
    if not _MAC_RE.match(mac_b64):
        raise ProofError("malformed", "mac charset")

    entry = secrets.get(kid)
    if entry is None:
        raise ProofError("unknown_kid", f"no secret for kid {kid!r}")
    secret, label = entry

    # Two-sided. Checking only the upper bound would let a far-future
    # timestamp pass forever.
    current = int(time.time()) if now is None else now
    age = current - int(ts_raw)
    if age > skew_seconds:
        raise ProofError("expired", f"age={age}s skew={skew_seconds}s")
    if -age > skew_seconds:
        raise ProofError("not_yet_valid", f"age={age}s skew={skew_seconds}s")

    expected = hmac.new(secret, canonical_string(kid, ts_raw, nonce, origin_id), hashlib.sha256).digest()
    received = _unb64(mac_b64)  # charset already validated above
    # `kid` is public, so selecting one candidate secret is a safe branch;
    # only the resulting MAC needs the constant-time compare.
    if not hmac.compare_digest(received, expected):
        raise ProofError("bad_mac", "signature mismatch")

    if nonce_cache is not None and not nonce_cache.check_and_add(nonce):
        raise ProofError("replayed", "nonce already seen")

    return {
        "verified": "true",
        "proxy": label,
        "kid": kid,
        "origin_id": origin_id,
        "reason": "ok",
    }


@dataclass(frozen=True)
class ProxyProofConfig:
    """Worker-side proxy-proof configuration.

    Attributes:
        mode: ``off``, ``allow`` or ``require``.
        origin_id: This worker's identifier, folded into every MAC.
        secrets: Maps ``kid`` to ``(secret, label)``.
        skew_seconds: Half-width of the timestamp acceptance window.
        replay_capacity: Hard cap on remembered nonces.
        enable_replay_cache: Whether to reject replayed nonces.

    """

    mode: Mode = "off"
    origin_id: str = ""
    secrets: Mapping[str, tuple[bytes, str]] = field(default_factory=dict)
    skew_seconds: int = 30
    replay_capacity: int = DEFAULT_CAPACITY
    enable_replay_cache: bool = True

    def __post_init__(self) -> None:
        """Validate eagerly so a misconfigured worker never starts.

        A shared secret spans two independently deployed processes. A lax
        parse means a typo silently yields different keys on each side and
        ``require`` becomes a total outage with no diagnostic.
        """
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {self.mode!r}")
        if self.mode == "off":
            return
        if not _ORIGIN_RE.match(self.origin_id):
            raise ValueError(
                f"origin_id is required in {self.mode!r} mode and must match {_ORIGIN_RE.pattern!r}, "
                f"got {self.origin_id!r}",
            )
        if not self.secrets:
            raise ValueError(f"at least one secret is required in {self.mode!r} mode")
        for kid, entry in self.secrets.items():
            if not _KID_RE.match(kid):
                raise ValueError(f"kid must match {_KID_RE.pattern!r}, got {kid!r}")
            secret, _label = entry
            if len(secret) != _SECRET_LEN:
                raise ValueError(f"secret for kid {kid!r} must be exactly {_SECRET_LEN} bytes, got {len(secret)}")
        if self.skew_seconds <= 0:
            raise ValueError(f"skew_seconds must be positive, got {self.skew_seconds}")


def parse_secrets(raw: str) -> dict[str, tuple[bytes, str]]:
    """Parse a ``kid:hex,kid:hex`` secret specification.

    The ``kid`` doubles as the proxy's label, so attribution needs no extra
    configuration.

    Args:
        raw: The specification string.

    Returns:
        Maps each key id to its secret and label.

    Raises:
        ValueError: On any malformed entry — never a partial parse, which
            would silently drop a proxy's access.

    """
    out: dict[str, tuple[bytes, str]] = {}
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        kid, sep, hex_secret = item.partition(":")
        if not sep:
            raise ValueError(f"expected 'kid:hex', got {item!r}")
        if not _KID_RE.match(kid):
            raise ValueError(f"kid must match {_KID_RE.pattern!r}, got {kid!r}")
        try:
            secret = bytes.fromhex(hex_secret)
        except ValueError as exc:
            raise ValueError(f"secret for kid {kid!r} is not valid hex") from exc
        if len(secret) != _SECRET_LEN:
            raise ValueError(
                f"secret for kid {kid!r} must be {_SECRET_LEN} bytes ({_SECRET_LEN * 2} hex chars), got {len(secret)}",
            )
        out[kid] = (secret, kid)
    if not out:
        raise ValueError("no secrets parsed")
    return out


def proxy_proof_gate(
    config: ProxyProofConfig,
    *,
    now: Callable[[], int] | None = None,
) -> PreconditionGate:
    """Build the request gate for a configured worker.

    Args:
        config: Worker-side configuration. ``mode`` must not be ``off`` —
            an off worker installs no gate at all, so there is no per-request
            cost rather than a gate that always passes.
        now: Injectable clock for tests.

    Returns:
        A gate for :func:`vgi_rpc.http.require_all`.

    """
    if config.mode == "off":
        raise ValueError("proxy_proof_gate called with mode='off'; install no gate instead")

    cache = (
        NonceCache(ttl_seconds=config.skew_seconds, capacity=config.replay_capacity)
        if config.enable_replay_cache
        else None
    )
    required = config.mode == "require"

    def gate(req: falcon.Request) -> Mapping[str, str]:
        # WSGI joins repeated headers with ", ". A proof contains no comma, so
        # this catches header injection without a separate multi-value API.
        raw = req.get_header(PROOF_HEADER)
        try:
            if not raw:
                raise ProofError("no_proof", "header absent")
            if "," in raw:
                raise ProofError("malformed", "multiple proof headers")
            return verify_proof(
                raw,
                secrets=config.secrets,
                origin_id=config.origin_id,
                skew_seconds=config.skew_seconds,
                nonce_cache=cache,
                now=None if now is None else now(),
            )
        except ProofError as exc:
            _logger.warning(
                "proxy proof rejected: %s",
                exc.reason,
                extra={
                    "remote_addr": req.remote_addr or "",
                    "proof_reason": exc.reason,
                    "proof_detail": str(exc),
                },
            )
            if required:
                # Uniform message: the caller controls `kid`, so echoing any
                # detail would reflect attacker-supplied text.
                raise ProofError(exc.reason, "proxy proof required") from exc
            return {
                "verified": "false",
                "proxy": "",
                "kid": "",
                "origin_id": config.origin_id,
                "reason": exc.reason,
            }

    return PreconditionGate(gate, name=GATE_NAME, claims_key=CLAIMS_KEY)
