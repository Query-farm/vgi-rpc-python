# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""JWT authenticate factory using Authlib for vgi-rpc HTTP transport.

Provides ``jwt_authenticate()`` which creates a callback compatible with
``make_wsgi_app(authenticate=...)``.  Validates Bearer JWTs against a
JWKS endpoint, with automatic key refresh on unknown ``kid``.

Requires ``pip install vgi-rpc[oauth]`` (Authlib).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

import falcon
import httpx

try:
    from authlib.jose import JsonWebKey, jwt
    from authlib.jose.errors import DecodeError, JoseError
except ImportError as _exc:
    raise ImportError("jwt_authenticate requires authlib: pip install vgi-rpc[oauth]") from _exc

from vgi_rpc.rpc import AuthContext

logger = logging.getLogger(__name__)


def jwt_authenticate(
    *,
    issuer: str,
    audience: str | tuple[str, ...],
    jwks_uri: str | None = None,
    claims_options: Mapping[str, Any] | None = None,
    principal_claim: str = "sub",
    domain: str = "jwt",
) -> Callable[[falcon.Request], AuthContext]:
    """Create a JWT-validating authenticate callback using Authlib.

    Returns a callback compatible with ``make_wsgi_app(authenticate=...)``.
    Fetches JWKS from the authorization server and validates Bearer tokens.
    Keys are cached in-process with automatic refresh on unknown ``kid``.

    Args:
        issuer: Expected ``iss`` claim in the JWT.
        audience: Expected ``aud`` claim(s) in the JWT.  A single string
            or a tuple of strings.  When multiple audiences are given, a
            token matching **any** of them is accepted.
        jwks_uri: URL to fetch the JWKS from.  When ``None``, discovered
            from ``{issuer}/.well-known/openid-configuration``.
        claims_options: Additional Authlib claim validation options.
        principal_claim: JWT claim to use as ``AuthContext.principal``
            (default ``"sub"``).
        domain: Domain string for the returned ``AuthContext``
            (default ``"jwt"``).

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` that validates
        Bearer tokens and returns an authenticated context.

    Raises:
        ValueError: From the returned callback when the token is missing,
            invalid, or fails validation.

    """
    # Authlib's KeySet type is not exported in public stubs; alias to contain Any.
    type _KeySet = Any

    resolved_jwks_uri = jwks_uri
    lock = threading.Lock()
    key_set: _KeySet = None

    def _fetch_jwks() -> _KeySet:
        nonlocal resolved_jwks_uri, key_set
        if resolved_jwks_uri is None:
            with httpx.Client() as client:
                oidc_resp = client.get(f"{issuer.rstrip('/')}/.well-known/openid-configuration")
                oidc_resp.raise_for_status()
                resolved_jwks_uri = oidc_resp.json()["jwks_uri"]

        with httpx.Client() as client:
            resp = client.get(resolved_jwks_uri)
            resp.raise_for_status()
            key_set = JsonWebKey.import_key_set(resp.json())
        return key_set

    def _get_key_set(force_refresh: bool = False) -> _KeySet:
        nonlocal key_set
        if key_set is None or force_refresh:
            with lock:
                if key_set is None or force_refresh:
                    return _fetch_jwks()
        return key_set

    audiences = (audience,) if isinstance(audience, str) else audience
    if not audiences:
        raise ValueError("audience must not be empty")

    base_claims_options: dict[str, Any] = {
        "iss": {"essential": True, "value": issuer},
        "aud": {"essential": True, "values": list(audiences)},
    }
    if claims_options:
        base_claims_options.update(claims_options)

    def _log_claim_mismatch(token: str, keys: _KeySet, exc: JoseError) -> None:
        """Log expected vs actual claims on JWT validation failure."""
        try:
            # Decode without validation to inspect the payload
            raw_claims = jwt.decode(token, keys)
            token_aud = raw_claims.get("aud")
            token_iss = raw_claims.get("iss")
            logger.warning(
                "JWT validation failed: %s\n"
                "  expected iss: %s\n"
                "  token   iss: %s\n"
                "  expected aud (any of): %s\n"
                "  token   aud: %s",
                exc,
                issuer,
                token_iss,
                list(audiences),
                token_aud,
            )
        except Exception:
            logger.warning("JWT validation failed: %s (could not decode token for diagnostics)", exc)

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")

        token = auth_header[7:]

        keys = _get_key_set()
        try:
            claims = jwt.decode(token, keys, claims_options=base_claims_options)
            claims.validate()
        except DecodeError:
            # DecodeError likely means unknown kid — refresh JWKS and retry
            keys = _get_key_set(force_refresh=True)
            try:
                claims = jwt.decode(token, keys, claims_options=base_claims_options)
                claims.validate()
            except JoseError as exc:
                _log_claim_mismatch(token, keys, exc)
                raise ValueError(f"Invalid JWT: {exc}") from exc
        except JoseError as exc:
            # Expired tokens, bad claims, etc. — no point refreshing keys
            _log_claim_mismatch(token, keys, exc)
            raise ValueError(f"Invalid JWT: {exc}") from exc

        principal = str(claims.get(principal_claim, ""))
        return AuthContext(
            domain=domain,
            authenticated=True,
            principal=principal,
            claims=dict(claims),
        )

    return authenticate
