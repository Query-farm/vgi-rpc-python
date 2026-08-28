# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""RFC 7636 PKCE primitives and unverified JWT payload decoding.

Shared by the server-side browser-redirect flow (``_oauth_pkce.py``) and the
client-driven flow (``_oauth_client.py``).

Extracted out of ``_oauth_pkce.py`` rather than duplicated: both call sites
need byte-for-byte identical math (a code_verifier one side generates must
produce the code_challenge the other side computed the same way), and both
need to read display claims (sub/email/name/iss) out of an id_token they
just obtained over TLS as part of their own OAuth exchange — trusted as an
identity hint, not re-verified, for exactly the reason ``vgi_oauth.cpp``'s
``ParseIdTokenClaims`` documents: the token didn't arrive as untrusted
caller input.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets

__all__ = [
    "decode_jwt_payload_unverified",
    "generate_code_challenge",
    "generate_code_verifier",
    "generate_state_nonce",
]


def decode_jwt_payload_unverified(token: str) -> dict[str, object] | None:
    """Best-effort decode of a JWT payload. No signature verification.

    Returns ``None`` on any malformed input (too few segments, bad base64,
    non-object JSON) rather than raising — callers treat a JWT they can't
    parse the same as one carrying no useful claims.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(payload))
        return decoded if isinstance(decoded, dict) else None
    except Exception:
        return None


def generate_code_verifier() -> str:
    """Generate a 43-character URL-safe random code verifier (RFC 7636 S4.1)."""
    return secrets.token_urlsafe(32)


def generate_code_challenge(code_verifier: str) -> str:
    """Compute the S256 code challenge from a code verifier (RFC 7636 S4.2)."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_state_nonce() -> str:
    """Generate a random ``state`` nonce for CSRF protection."""
    return secrets.token_urlsafe(24)
