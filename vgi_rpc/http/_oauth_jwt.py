# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""JWT authenticate factory using joserfc for vgi-rpc HTTP transport.

Provides ``jwt_authenticate()`` which creates a callback compatible with
``make_wsgi_app(authenticate=...)``.  Validates Bearer JWTs against a
JWKS endpoint, with automatic key refresh on unknown ``kid``.

Requires ``pip install vgi-rpc[oauth]`` (joserfc).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

import falcon
import httpx

try:
    from joserfc import jwt
    from joserfc.errors import InvalidKeyIdError, JoseError
    from joserfc.jwk import KeySet
except ImportError as _exc:
    raise ImportError("jwt_authenticate requires joserfc: pip install vgi-rpc[oauth]") from _exc

from vgi_rpc.rpc import AuthContext

logger = logging.getLogger(__name__)


def jwt_authenticate(
    *,
    issuer: str | tuple[str, ...],
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
        issuer: Expected ``iss`` claim(s) in the JWT.  A single string
            or a tuple of strings.  When multiple issuers are given, a
            token matching **any** of them is accepted.  When multiple
            issuers are provided and ``jwks_uri`` is not set, the first
            issuer is used for OIDC discovery.
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
    issuers = (issuer,) if isinstance(issuer, str) else issuer
    if not issuers:
        raise ValueError("issuer must not be empty")
    # Use the first issuer for OIDC discovery when jwks_uri is not provided.
    discovery_issuer = issuers[0]

    resolved_jwks_uri = jwks_uri
    lock = threading.Lock()
    key_set: KeySet | None = None

    def _fetch_jwks() -> KeySet:
        nonlocal resolved_jwks_uri, key_set
        if resolved_jwks_uri is None:
            with httpx.Client() as client:
                oidc_resp = client.get(f"{discovery_issuer.rstrip('/')}/.well-known/openid-configuration")
                oidc_resp.raise_for_status()
                resolved_jwks_uri = oidc_resp.json()["jwks_uri"]

        with httpx.Client() as client:
            resp = client.get(resolved_jwks_uri)
            resp.raise_for_status()
            new_key_set = KeySet.import_key_set(resp.json())
        key_set = new_key_set
        return new_key_set

    def _get_key_set(force_refresh: bool = False) -> KeySet:
        nonlocal key_set
        # Capture the cache version we observed *before* taking the lock.
        # If another thread refreshes while we wait, the cache identity
        # changes and we return the new value instead of stampeding the
        # JWKS endpoint with N concurrent re-fetches during key rotation.
        seen = key_set
        if seen is not None and not force_refresh:
            return seen
        with lock:
            if key_set is not None and key_set is not seen:
                return key_set
            return _fetch_jwks()

    audiences = (audience,) if isinstance(audience, str) else audience
    if not audiences:
        raise ValueError("audience must not be empty")

    base_claims_options: dict[str, Any] = {
        "iss": {"essential": True, "values": list(issuers)},
        "aud": {"essential": True, "values": list(audiences)},
    }
    if claims_options:
        base_claims_options.update(claims_options)

    def _log_claim_mismatch(claims: Mapping[str, Any] | None, exc: JoseError) -> None:
        """Log expected vs actual claims on JWT validation failure."""
        if claims is None:
            logger.warning("JWT validation failed: %s (could not decode token for diagnostics)", exc)
            return
        logger.warning(
            "JWT validation failed: %s\n"
            "  expected iss (any of): %s\n"
            "  token   iss: %s\n"
            "  expected aud (any of): %s\n"
            "  token   aud: %s",
            exc,
            list(issuers),
            claims.get("iss"),
            list(audiences),
            claims.get("aud"),
        )

    def _decode_and_validate(raw_token: str, keys: KeySet) -> dict[str, Any]:
        decoded = jwt.decode(raw_token, keys)
        registry = jwt.JWTClaimsRegistry(**base_claims_options)
        registry.validate(decoded.claims)
        return dict(decoded.claims)

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")

        token = auth_header[7:]

        keys = _get_key_set()
        try:
            claims = _decode_and_validate(token, keys)
        except InvalidKeyIdError:
            # Unknown kid — refresh JWKS and retry once
            keys = _get_key_set(force_refresh=True)
            try:
                claims = _decode_and_validate(token, keys)
            except JoseError as exc:
                diag = _peek_claims(token)
                _log_claim_mismatch(diag, exc)
                raise ValueError(f"Invalid JWT: {exc}") from exc
        except JoseError as exc:
            # Expired tokens, bad claims, etc. — no point refreshing keys
            diag = _peek_claims(token)
            _log_claim_mismatch(diag, exc)
            raise ValueError(f"Invalid JWT: {exc}") from exc

        principal = str(claims.get(principal_claim, ""))
        return AuthContext(
            domain=domain,
            authenticated=True,
            principal=principal,
            claims=claims,
        )

    return authenticate


def _peek_claims(token: str) -> dict[str, Any] | None:
    """Decode a JWT payload without signature verification for diagnostics."""
    import base64
    import json

    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        padding = "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64 + padding)
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
        return None
    except Exception:
        return None
