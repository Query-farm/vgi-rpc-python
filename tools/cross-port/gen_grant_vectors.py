#!/usr/bin/env python3
"""Regenerate ``vgi_rpc/conformance/grant_token_vectors.json`` from the reference.

Every value is fixed -- key, nonce, clock, grant id -- so the output is
byte-for-byte reproducible and a port's minter must produce the same tokens.
Run: ``uv run python tools/cross-port/gen_grant_vectors.py``.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

from vgi_rpc.crypto import seal_bytes
from vgi_rpc.grants import GrantKeys, _encode_payload, grant_key_id, mint_grant_token

OUT = Path(__file__).resolve().parents[2] / "vgi_rpc" / "conformance" / "grant_token_vectors.json"

KEY_A = bytes(range(32))
KEY_B = bytes(range(32, 64))
KEY_OTHER = bytes([0xA5]) * 32
NONCE = bytes(range(0x40, 0x58))
ISSUED_AT = 1767225600  # 2026-01-01T00:00:00Z


def b64(key: bytes) -> str:
    return base64.b64encode(key).decode()


def mint(keys: GrantKeys, **kw: object) -> tuple[str, dict[str, object]]:
    token, claims = mint_grant_token(keys, now=ISSUED_AT, nonce=NONCE, **kw)  # type: ignore[arg-type]
    return token, {
        "principal": claims.principal,
        "scopes": list(claims.scopes),
        "purpose": claims.purpose,
        "grant_id": claims.grant_id,
        "issued_at": claims.issued_at,
        "expires_at": claims.expires_at,
    }


def main() -> None:
    valid = []
    cases = [
        (
            "basic",
            GrantKeys(keys=(KEY_A,)),
            {
                "principal": "alice@example.com",
                "scopes": ["read", "write"],
                "purpose": "nightly-report",
                "ttl_seconds": 3600,
                "grant_id": "g-0001",
            },
        ),
        (
            "audience_unicode_no_scopes",
            GrantKeys(keys=(KEY_A,), audience="conformance"),
            {"principal": "zoë", "scopes": [], "purpose": "", "ttl_seconds": 60, "grant_id": "g-0002"},
        ),
        (
            "ttl_capped_to_max",
            GrantKeys(keys=(KEY_A,), max_ttl_seconds=3600),
            {"principal": "bob", "scopes": ["admin"], "purpose": "p", "ttl_seconds": 999999, "grant_id": "g-0003"},
        ),
        (
            "minted_by_second_key",
            GrantKeys(keys=(KEY_B,)),
            {"principal": "carol", "scopes": ["read"], "purpose": "rotation", "ttl_seconds": 600, "grant_id": "g-0004"},
        ),
    ]
    for name, keys, kw in cases:
        token, claims = mint(keys, **kw)
        payload = _encode_payload(
            __import__("vgi_rpc.grants", fromlist=["GrantClaims"]).GrantClaims(
                principal=claims["principal"],
                scopes=tuple(claims["scopes"]),
                purpose=claims["purpose"],  # type: ignore[arg-type]
                grant_id=claims["grant_id"],
                issued_at=claims["issued_at"],
                expires_at=claims["expires_at"],
            )
        )  # type: ignore[arg-type]
        kid = grant_key_id(keys.keys[0])
        valid.append(
            {
                "name": name,
                "minting_key_b64": b64(keys.keys[0]),
                "audience": keys.audience,
                "max_ttl_seconds": keys.max_ttl_seconds,
                "nonce_hex": NONCE.hex(),
                "now": ISSUED_AT,
                "request": dict(kw),
                "claims": claims,
                "kid_hex": kid.hex(),
                "aad_hex": (b"vgi_rpc.grant.v1\x00" + kid + keys.audience.encode()).hex(),
                "payload_hex": payload.hex(),
                "token": token,
            }
        )

    basic = valid[0]["token"]
    keys_a = GrantKeys(keys=(KEY_A,))
    mid = len(basic) - 10  # inside the ciphertext/tag, so only the AEAD tag can catch it
    flipped = basic[:mid] + ("A" if basic[mid] != "A" else "B") + basic[mid + 1 :]
    # Trailing payload byte, sealed correctly: authentic but non-canonical.
    from vgi_rpc.grants import GrantClaims

    claims = GrantClaims("alice@example.com", ("read",), "x", "g", ISSUED_AT, ISSUED_AT + 60)
    kid = grant_key_id(KEY_A)
    env = seal_bytes(
        _encode_payload(claims) + b"\x00", KEY_A, aad=b"vgi_rpc.grant.v1\x00" + kid, version=1, nonce=NONCE
    )
    trailing = "vgig1." + base64.urlsafe_b64encode(kid + env).rstrip(b"=").decode()
    long_ttl, _ = mint(
        GrantKeys(keys=(KEY_A,), max_ttl_seconds=10**7),
        principal="alice",
        scopes=[],
        purpose="",
        ttl_seconds=10**6,
        grant_id="g-long",
    )
    # Non-canonical base64url: same bytes, a different last character with non-zero trailing bits.
    import string

    alphabet = string.ascii_uppercase + string.ascii_lowercase + string.digits + "-_"
    body = basic[len("vgig1.") :]
    idx = alphabet.index(body[-1])
    noncanon = None
    for d in range(1, 4):
        cand = "vgig1." + body[:-1] + alphabet[idx ^ d]
        raw = base64.urlsafe_b64decode(cand[6:] + "=" * (-len(cand[6:]) % 4))
        if base64.urlsafe_b64encode(raw).rstrip(b"=").decode() != cand[6:] and raw == base64.urlsafe_b64decode(
            body + "=" * (-len(body) % 4)
        ):
            noncanon = cand
            break
    rejects = [
        {"name": "tampered_ciphertext", "token": flipped, "expired": False},
        {"name": "wrong_key", "verify_keys_b64": [b64(KEY_OTHER)], "token": basic, "expired": False},
        {"name": "wrong_audience", "audience": "elsewhere", "token": basic, "expired": False},
        {"name": "expired_beyond_skew", "now": ISSUED_AT + 3600 + 60, "token": basic, "expired": True},
        {"name": "not_yet_valid_beyond_skew", "now": ISSUED_AT - 61, "token": basic, "expired": True},
        {"name": "wrong_prefix", "token": "vgig2." + body, "expired": False},
        {"name": "padded_base64", "token": basic + "=", "expired": False},
        {"name": "trailing_payload_byte", "token": trailing, "expired": False},
        {"name": "lifetime_over_max", "token": long_ttl, "expired": False},
    ]
    if noncanon is not None:
        rejects.append({"name": "non_canonical_base64url", "token": noncanon, "expired": False})
    accepts = [
        {"name": "within_skew_after_expiry", "now": ISSUED_AT + 3600 + 59, "token": basic},
        {"name": "rotation_old_key_verifies", "verify_keys_b64": [b64(KEY_B), b64(KEY_A)], "token": basic},
    ]
    doc = {
        "description": (
            "Sealed-grant test vectors (IDENTITY_V1_SPEC.md §9). A port's minter given minting_key, "
            "audience, max_ttl_seconds, nonce, now and request MUST produce exactly `token`; its verifier "
            "MUST accept every `accept` case and reject every `reject` case. Unless a case overrides "
            "them, verification uses verify_keys=[minting key of 'basic'], audience '', max_ttl_seconds "
            f"{GrantKeys(keys=(KEY_A,)).max_ttl_seconds}, clock_skew_seconds 60, now = issued_at + 60."
        ),
        "defaults": {
            "verify_keys_b64": [b64(KEY_A)],
            "audience": "",
            "max_ttl_seconds": keys_a.max_ttl_seconds,
            "clock_skew_seconds": 60,
            "now": ISSUED_AT + 60,
        },
        "mint": valid,
        "accept": accepts,
        "reject": rejects,
    }
    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
