# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""JWT authenticate factory using joserfc for vgi-rpc HTTP transport.

Provides ``jwt_authenticate()`` which creates a callback compatible with
``make_wsgi_app(authenticate=...)``.  Validates Bearer JWTs against a
JWKS endpoint, with key refresh on unknown ``kid`` and opt-in hardening:
an algorithm allow-list, a required ``typ``, a required ``exp``, a hard
JWKS cache lifetime that fails closed, and a rate limit on unknown-``kid``
re-fetches.

Requires ``pip install vgi-rpc[oauth]`` (joserfc).
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Any

import falcon
import httpx2

try:
    from joserfc import jwt
    from joserfc.errors import (
        ExpiredTokenError,
        InvalidClaimError,
        InvalidHeaderValueError,
        InvalidKeyIdError,
        JoseError,
    )
    from joserfc.jwk import KeySet
except ImportError as _exc:
    raise ImportError("jwt_authenticate requires joserfc: pip install vgi-rpc[oauth]") from _exc

from vgi_rpc.errors import AuthUnavailableError
from vgi_rpc.http._unauthorized import AuthFailure, AuthReason
from vgi_rpc.rpc import AuthContext

logger = logging.getLogger(__name__)

#: ``alg`` values that may never appear in an ``algorithms`` allow-list.
#: ``none`` is unsigned, and an HMAC algorithm checked against a JWKS lets
#: anyone holding the *public* key mint tokens (key confusion).
_FORBIDDEN_ALGORITHM_PREFIXES = ("none", "hs")

_MAX_AGE_RE = re.compile(r"(?:^|,)\s*max-age\s*=\s*\"?(\d+)\"?\s*(?=,|$)", re.IGNORECASE)


def _validate_algorithms(algorithms: Collection[str]) -> tuple[str, ...]:
    """Return *algorithms* as a tuple, refusing ``none`` and HMAC algorithms."""
    if isinstance(algorithms, str):
        raise TypeError("algorithms must be a collection of algorithm names, not a single string")
    allowed = tuple(algorithms)
    if not allowed:
        raise ValueError("algorithms must not be empty")
    for alg in allowed:
        if not isinstance(alg, str) or not alg.strip():
            raise ValueError(f"invalid algorithm name: {alg!r}")
        if alg.strip().lower().startswith(_FORBIDDEN_ALGORITHM_PREFIXES):
            raise ValueError(f"algorithm {alg!r} is not allowed for JWKS-verified tokens")
    return allowed


def _normalize_typ(value: str) -> str:
    """Normalize a JOSE ``typ`` value for comparison.

    Media type names compare case-insensitively and the ``application/``
    prefix is optional (RFC 7515 §4.1.9, RFC 9068 §2.1).
    """
    return value.strip().lower().removeprefix("application/")


def _cache_control_max_age(value: str | None) -> float | None:
    """Return the ``max-age`` directive of a ``Cache-Control`` header, or ``None``."""
    if not value:
        return None
    match = _MAX_AGE_RE.search(value)
    if match is None:
        return None
    return float(match.group(1))


@dataclass(frozen=True)
class _CachedKeySet:
    """A fetched JWKS and the monotonic time it stops being usable (``None``: never)."""

    keys: KeySet
    expires_at: float | None


def jwt_authenticate(
    *,
    issuer: str | tuple[str, ...],
    audience: str | tuple[str, ...],
    jwks_uri: str | None = None,
    claims_options: Mapping[str, Any] | None = None,
    principal_claim: str = "sub",
    domain: str = "jwt",
    algorithms: Collection[str] | None = None,
    required_typ: str | None = None,
    require_exp: bool = False,
    single_audience: bool = False,
    leeway: int = 0,
    jwks_max_age: float | None = None,
    jwks_refresh_min_interval: float | None = None,
    jwks_fetch_timeout: float | None = None,
) -> Callable[[falcon.Request], AuthContext]:
    """Create a JWT-validating authenticate callback.

    Returns a callback compatible with ``make_wsgi_app(authenticate=...)``.
    Fetches JWKS from the authorization server and validates Bearer tokens.

    With the defaults, keys are cached for the life of the process and an
    unknown ``kid`` triggers an immediate re-fetch. The hardening options
    below are opt-in, so existing deployments behave as before.

    Claims reach ``AuthContext.claims`` exactly as decoded from the token's
    JSON payload: booleans stay booleans and numbers stay numbers.

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
        claims_options: Additional joserfc claim validation options.
        principal_claim: JWT claim to use as ``AuthContext.principal``
            (default ``"sub"``).
        domain: Domain string for the returned ``AuthContext``
            (default ``"jwt"``).
        algorithms: Allow-list of JWS ``alg`` values.  A token signed with
            any other algorithm is rejected.  ``none`` and ``HS*`` are
            refused at construction.  ``None`` keeps joserfc's default set
            of recommended algorithms.
        required_typ: When set (e.g. ``"at+jwt"`` for RFC 9068 access
            tokens), the JOSE header ``typ`` must be present and match,
            case-insensitively and with the ``application/`` prefix optional.
        require_exp: When ``True``, a token without ``exp`` is rejected.
            An ``exp`` that is present is always checked.
        single_audience: When ``True``, the token's ``aud`` must be exactly
            one value (a string, or a one-element list). A token naming
            several audiences is refused even if one of them matches.
        leeway: Clock skew in seconds allowed on ``exp`` and ``nbf``.
        jwks_max_age: Hard upper bound, in seconds, on how long fetched keys
            are used.  A shorter ``Cache-Control: max-age`` on the JWKS
            response wins.  When the bound passes the keys are discarded; if
            the re-fetch then fails, verification fails (fail closed) rather
            than using stale keys.  ``None`` caches for the process lifetime.
        jwks_refresh_min_interval: Minimum seconds between JWKS fetches
            triggered by an unknown ``kid``.  Within the interval a token
            whose ``kid`` is unknown is rejected without a fetch.  ``None``
            re-fetches on every unknown ``kid``.
        jwks_fetch_timeout: Timeout in seconds for each discovery or JWKS
            HTTP request. ``None`` keeps the HTTP client's default.

    Returns:
        A callback ``(falcon.Request) -> AuthContext``. It raises
        ``AuthFailure`` (a ``ValueError``, HTTP 401) for a missing, invalid
        or failing token, and ``AuthUnavailableError`` (HTTP 503) when the
        JWKS cannot be fetched and no key set within its maximum age exists.

    Raises:
        ValueError: When ``issuer``, ``audience`` or ``algorithms`` is empty,
            ``algorithms`` names ``none`` or an HMAC algorithm,
            ``required_typ`` is blank, or a duration is negative.

    """
    issuers = (issuer,) if isinstance(issuer, str) else issuer
    if not issuers:
        raise ValueError("issuer must not be empty")
    audiences = (audience,) if isinstance(audience, str) else audience
    if not audiences:
        raise ValueError("audience must not be empty")
    allowed_algorithms = _validate_algorithms(algorithms) if algorithms is not None else None
    expected_typ = _normalize_typ(required_typ) if required_typ is not None else None
    if expected_typ is not None and not expected_typ:
        raise ValueError("required_typ must not be empty")
    for name, value in (
        ("leeway", leeway),
        ("jwks_max_age", jwks_max_age),
        ("jwks_refresh_min_interval", jwks_refresh_min_interval),
    ):
        if value is not None and value < 0:
            raise ValueError(f"{name} must not be negative")

    # Use the first issuer for OIDC discovery when jwks_uri is not provided.
    discovery_issuer = issuers[0]

    resolved_jwks_uri = jwks_uri
    lock = threading.Lock()
    cache: _CachedKeySet | None = None
    last_fetch_attempt: float | None = None

    def _usable(entry: _CachedKeySet | None, now: float) -> bool:
        return entry is not None and (entry.expires_at is None or now < entry.expires_at)

    def _fetch_locked() -> KeySet:
        """Fetch the JWKS and replace the cache. The caller holds ``lock``."""
        nonlocal resolved_jwks_uri, cache, last_fetch_attempt
        started = time.monotonic()
        last_fetch_attempt = started
        try:
            timeout: dict[str, Any] = {} if jwks_fetch_timeout is None else {"timeout": jwks_fetch_timeout}
            with httpx2.Client() as client:
                if resolved_jwks_uri is None:
                    discovery_url = f"{discovery_issuer.rstrip('/')}/.well-known/openid-configuration"
                    oidc_resp = client.get(discovery_url, **timeout)
                    oidc_resp.raise_for_status()
                    resolved_jwks_uri = str(oidc_resp.json()["jwks_uri"])
                resp = client.get(resolved_jwks_uri, **timeout)
                resp.raise_for_status()
                new_key_set = KeySet.import_key_set(resp.json())
                cache_control = resp.headers.get("cache-control")
        except Exception as exc:
            # Never fall back to stale keys: drop an expired entry so nothing
            # can read it, and make the caller fail closed.
            if not _usable(cache, time.monotonic()):
                cache = None
            logger.warning("JWKS fetch failed (%s)", type(exc).__name__)
            raise AuthUnavailableError("JWKS endpoint unavailable") from exc

        expires_at: float | None = None
        if jwks_max_age is not None:
            ttl = jwks_max_age
            advertised = _cache_control_max_age(cache_control)
            if advertised is not None:
                ttl = min(ttl, advertised)
            # Measured from when the fetch started, so a slow response cannot
            # stretch the bound.
            expires_at = started + ttl
        cache = _CachedKeySet(new_key_set, expires_at)
        return new_key_set

    def _current_key_set() -> KeySet:
        """Return keys that are within their maximum age, fetching if needed."""
        entry = cache
        if entry is not None and _usable(entry, time.monotonic()):
            return entry.keys
        with lock:
            entry = cache
            if entry is not None and _usable(entry, time.monotonic()):
                return entry.keys
            return _fetch_locked()

    def _refresh_for_unknown_kid(seen: KeySet) -> KeySet | None:
        """Re-fetch after an unknown ``kid``; ``None`` when rate limited.

        *seen* is the key set the caller decoded against. If another thread
        refreshed while this one waited for the lock, its result is reused
        rather than stampeding the JWKS endpoint during a key rotation.
        """
        with lock:
            entry = cache
            now = time.monotonic()
            if entry is not None and entry.keys is not seen and _usable(entry, now):
                return entry.keys
            if (
                jwks_refresh_min_interval is not None
                and last_fetch_attempt is not None
                and now - last_fetch_attempt < jwks_refresh_min_interval
            ):
                return None
            return _fetch_locked()

    base_claims_options: dict[str, Any] = {
        "iss": {"essential": True, "values": list(issuers)},
        "aud": {"essential": True, "values": list(audiences)},
    }
    if require_exp:
        base_claims_options["exp"] = {"essential": True}
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
        decoded = jwt.decode(raw_token, keys, algorithms=allowed_algorithms)
        if expected_typ is not None:
            typ = decoded.header.get("typ")
            if not isinstance(typ, str) or _normalize_typ(typ) != expected_typ:
                raise InvalidHeaderValueError(f"JOSE header 'typ' must be {required_typ!r}")
        registry = jwt.JWTClaimsRegistry(leeway=leeway, **base_claims_options)
        registry.validate(decoded.claims)
        if single_audience:
            aud = decoded.claims.get("aud")
            if not (isinstance(aud, str) or (isinstance(aud, list) and len(aud) == 1)):
                raise InvalidClaimError("aud")
        return dict(decoded.claims)

    def _reject(token: str, exc: JoseError) -> AuthFailure:
        """Log the claim mismatch and build the failure to raise.

        An expired token is separated from a bad one because the two call for
        different actions: refresh and retry, versus stop and re-authenticate.
        """
        _log_claim_mismatch(_peek_claims(token), exc)
        reason = AuthReason.EXPIRED_CREDENTIAL if isinstance(exc, ExpiredTokenError) else AuthReason.INVALID_CREDENTIAL
        return AuthFailure(reason, f"Invalid JWT: {exc}")

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header:
            raise AuthFailure(AuthReason.MISSING_CREDENTIAL, "Missing Authorization header")
        if not auth_header.startswith("Bearer "):
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "Authorization header is not a Bearer credential")

        token = auth_header[7:]

        keys = _current_key_set()
        try:
            claims = _decode_and_validate(token, keys)
        except InvalidKeyIdError as kid_exc:
            # Unknown kid: refresh the JWKS (rate limited) and retry once.
            refreshed = _refresh_for_unknown_kid(keys)
            if refreshed is None:
                raise _reject(token, kid_exc) from kid_exc
            try:
                claims = _decode_and_validate(token, refreshed)
            except JoseError as exc:
                raise _reject(token, exc) from exc
        except JoseError as exc:
            # Expired tokens, bad claims, etc. — no point refreshing keys
            raise _reject(token, exc) from exc

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
