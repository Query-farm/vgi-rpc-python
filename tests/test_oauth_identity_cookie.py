# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The ``_vgi_identity`` display-identity cookie derived from the OIDC id_token.

The shared VGI landing page reads this JS-readable cookie to render the signed-in
user. It must carry only display claims (never the raw token) and must surface the
email even when the bearer cookie holds an access token.
"""

from __future__ import annotations

import base64
import json

from vgi_rpc.http._oauth_pkce import (
    _IDENTITY_CLAIMS,
    _decode_jwt_payload,
    _identity_cookie_value,
)


def _make_id_token(claims: dict[str, object]) -> str:
    """Build an unsigned JWT (header.payload.sig) carrying ``claims``."""
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def _decode_cookie(value: str) -> dict[str, object]:
    """Decode the base64url(JSON) identity cookie as the browser page does."""
    decoded: dict[str, object] = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
    return decoded


def test_identity_cookie_surfaces_email_and_drops_non_display_claims() -> None:
    """Cookie carries display claims (email/name/sub) and no registered claims."""
    token = _make_id_token(
        {
            "sub": "IL5OHgxKCLFEMB_sqiN6p8uYmUKGKEGDtFyrYmkaNH8",
            "email": "rusty@luckydinosaur.com",
            "name": "Rusty Conover",
            "aud": "client-123",
            "exp": 9999999999,
            "iss": "https://idp.example.com",
        }
    )
    cookie = _identity_cookie_value(token)
    assert cookie is not None
    decoded = _decode_cookie(cookie)
    # Display claims present...
    assert decoded["email"] == "rusty@luckydinosaur.com"
    assert decoded["name"] == "Rusty Conover"
    assert str(decoded["sub"]).startswith("IL5OHgxKC")
    # ...security/registered claims excluded.
    for k in ("aud", "exp", "iss"):
        assert k not in decoded
    assert set(decoded) <= set(_IDENTITY_CLAIMS)


def test_identity_cookie_none_for_missing_or_bad_token() -> None:
    """No cookie for a missing, malformed, or claim-less token."""
    assert _identity_cookie_value(None) is None
    assert _identity_cookie_value("") is None
    assert _identity_cookie_value("not-a-jwt") is None
    # A JWT whose payload carries no display claims yields no cookie.
    assert _identity_cookie_value(_make_id_token({"aud": "x", "exp": 1})) is None


def test_decode_jwt_payload_tolerates_missing_padding() -> None:
    """The JWT payload decoder handles base64url without padding."""
    claims: dict[str, object] = {"sub": "abc", "email": "a@b.co"}
    assert _decode_jwt_payload(_make_id_token(claims)) == claims
    assert _decode_jwt_payload("only-one-part") is None
