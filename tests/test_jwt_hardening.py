# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for jwt_authenticate's opt-in hardening and make_wsgi_app(strict_auth=True).

Covers the verifier options a hosted worker needs (RFC 9068 access tokens:
ES256 only, ``typ: at+jwt``, ``exp`` required, a JWKS cache with a hard
maximum age that fails closed, and a rate limit on unknown-``kid``
re-fetches), and the strict HTTP mode in which every request but
``HEAD /health`` and CORS preflights must authenticate.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import falcon
import falcon.testing
import falcon.testing.helpers
import pytest
from joserfc import jwt
from joserfc.jwk import ECKey, OctKey, RSAKey

from vgi_rpc.errors import AuthUnavailableError
from vgi_rpc.http import AuthFailure, AuthReason, jwt_authenticate, make_wsgi_app
from vgi_rpc.rpc import AuthContext, RpcServer

ISS = "https://auth.example.com"
AUD = "https://desk.example.com/d/dep_1"
JWKS_URI = "https://auth.example.com/jwks"

# ---------------------------------------------------------------------------
# Keys, tokens and a scriptable JWKS endpoint
# ---------------------------------------------------------------------------


def _ec_key(kid: str) -> ECKey:
    return ECKey.generate_key("P-256", parameters={"kid": kid}, private=True)


EC = _ec_key("ec-1")
EC_ROTATED = _ec_key("ec-2")
RSA = RSAKey.generate_key(2048, parameters={"kid": "rsa-1"}, private=True)


def _public(*keys: ECKey | RSAKey) -> dict[str, Any]:
    return {"keys": [k.as_dict(private=False) for k in keys]}


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": ISS,
        "aud": AUD,
        "sub": "user_01",
        "kind": "user",
        "email": "ada@example.com",
        "email_verified": True,
        "org_id": "org_1",
        "iat": now,
        "nbf": now,
        "exp": now + 900,
    }
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not None}


def _token(
    key: ECKey | RSAKey | OctKey = EC,
    *,
    alg: str = "ES256",
    typ: str | None = "at+jwt",
    kid: str | None = None,
    **claim_overrides: Any,
) -> str:
    header: dict[str, Any] = {"alg": alg, "kid": kid or key.kid}
    if typ is not None:
        header["typ"] = typ
    return jwt.encode(header, _claims(**claim_overrides), key, algorithms=[alg], default_type=None)


@dataclass
class _Jwks:
    """A JWKS endpoint whose answer, headers and availability tests control."""

    body: dict[str, Any] = field(default_factory=lambda: _public(EC))
    cache_control: str | None = None
    failing: bool = False
    fetches: int = 0


class _Resp:
    def __init__(self, jwks: _Jwks) -> None:
        self._jwks = jwks
        self.headers = {"cache-control": jwks.cache_control} if jwks.cache_control else {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._jwks.body


class _Clock:
    """Stands in for the ``time`` module inside ``_oauth_jwt``."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def jwks() -> Iterator[_Jwks]:
    """Patch the JWKS HTTP client with a scriptable endpoint."""
    state = _Jwks()

    class _Client:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def __enter__(self) -> _Client:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def get(self, url: str, *args: object, **kwargs: object) -> _Resp:
            state.fetches += 1
            if state.failing:
                raise ConnectionError("JWKS endpoint down")
            return _Resp(state)

    with patch("vgi_rpc.http._oauth_jwt.httpx2.Client", _Client):
        yield state


@pytest.fixture
def clock() -> Iterator[_Clock]:
    """Control the monotonic clock the JWKS cache uses."""
    fake = _Clock()
    with patch("vgi_rpc.http._oauth_jwt.time", fake):
        yield fake


def _hosted(**overrides: Any) -> Any:
    options: dict[str, Any] = {
        "issuer": ISS,
        "audience": AUD,
        "jwks_uri": JWKS_URI,
        "algorithms": ["ES256"],
        "required_typ": "at+jwt",
        "require_exp": True,
        "leeway": 60,
        "jwks_max_age": 600,
        "jwks_refresh_min_interval": 60,
    }
    options.update(overrides)
    return jwt_authenticate(**options)


def _req(token: str | None) -> falcon.Request:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    return falcon.testing.helpers.create_req(headers=headers)


def _rejected(auth: Any, token: str | None) -> AuthReason:
    with pytest.raises(AuthFailure) as info:
        auth(_req(token))
    return info.value.reason


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    """Options that are refused when the authenticator is built."""

    @pytest.mark.parametrize("alg", ["none", "NONE", "HS256", "hs512"])
    def test_forbidden_algorithms(self, alg: str) -> None:
        """``none`` and HMAC algorithms can never be allow-listed."""
        with pytest.raises(ValueError, match="not allowed"):
            jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI, algorithms=["ES256", alg])

    def test_empty_algorithms(self) -> None:
        """An empty allow-list would mean 'nothing verifies'; refuse it loudly."""
        with pytest.raises(ValueError, match="empty"):
            jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI, algorithms=[])

    def test_string_algorithms(self) -> None:
        """A bare string is a mistake (it would iterate characters)."""
        with pytest.raises(TypeError):
            jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI, algorithms="ES256")

    def test_negative_durations(self) -> None:
        """Negative cache lifetimes are refused."""
        with pytest.raises(ValueError, match="jwks_max_age"):
            jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI, jwks_max_age=-1)


# ---------------------------------------------------------------------------
# Token checks
# ---------------------------------------------------------------------------


class TestTokenChecks:
    """Each verifier option rejects what it should and accepts the rest."""

    def test_valid_pass_claims_keep_json_types(self, jwks: _Jwks) -> None:
        """Claims reach AuthContext.claims with their JSON types."""
        auth = _hosted()
        ctx = auth(_req(_token(nested={"a": [1, 2]}, score=1.5)))
        assert ctx.authenticated
        assert ctx.principal == "user_01"
        assert ctx.claims["email_verified"] is True
        assert isinstance(ctx.claims["exp"], int)
        assert ctx.claims["score"] == 1.5
        assert ctx.claims["nested"] == {"a": [1, 2]}
        assert ctx.claims["org_id"] == "org_1"

    @pytest.mark.parametrize("typ", ["at+jwt", "AT+JWT", "application/at+jwt", "Application/AT+JWT"])
    def test_typ_accepted_forms(self, jwks: _Jwks, typ: str) -> None:
        """The typ header compares case-insensitively with an optional application/ prefix."""
        assert _hosted()(_req(_token(typ=typ))).authenticated

    @pytest.mark.parametrize("typ", [None, "JWT", "at+jwt+x", "application/jwt"])
    def test_wrong_typ(self, jwks: _Jwks, typ: str | None) -> None:
        """A missing or different typ is rejected."""
        assert _rejected(_hosted(), _token(typ=typ)) is AuthReason.INVALID_CREDENTIAL

    def test_typ_not_required_by_default(self, jwks: _Jwks) -> None:
        """Without required_typ a plain JWT still verifies (existing behaviour)."""
        auth = jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI)
        assert auth(_req(_token(typ="JWT"))).authenticated

    def test_wrong_alg_rs256(self, jwks: _Jwks) -> None:
        """An RS256 token is rejected under an ES256-only allow-list, even with its key in the JWKS."""
        jwks.body = _public(EC, RSA)
        assert _rejected(_hosted(), _token(RSA, alg="RS256")) is AuthReason.INVALID_CREDENTIAL
        # ...and the same token verifies without an allow-list (default unchanged).
        default = jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI)
        assert default(_req(_token(RSA, alg="RS256"))).authenticated

    def test_wrong_alg_hs256(self, jwks: _Jwks) -> None:
        """An HS256 token naming the EC key's kid is rejected."""
        secret = OctKey.import_key(b"x" * 32, {"kid": "ec-1"})
        assert _rejected(_hosted(), _token(secret, alg="HS256")) is AuthReason.INVALID_CREDENTIAL

    def test_missing_exp(self, jwks: _Jwks) -> None:
        """require_exp rejects a token without exp."""
        assert _rejected(_hosted(), _token(exp=None)) is AuthReason.INVALID_CREDENTIAL
        assert _hosted(require_exp=False)(_req(_token(exp=None))).authenticated

    def test_expired(self, jwks: _Jwks) -> None:
        """An expired token beyond the leeway is an expired credential."""
        assert _rejected(_hosted(), _token(exp=int(time.time()) - 120)) is AuthReason.EXPIRED_CREDENTIAL

    def test_expired_within_leeway(self, jwks: _Jwks) -> None:
        """Expiry inside the 60 s skew still verifies."""
        assert _hosted()(_req(_token(exp=int(time.time()) - 30))).authenticated

    def test_not_yet_valid(self, jwks: _Jwks) -> None:
        """An nbf beyond the leeway is rejected."""
        assert _rejected(_hosted(), _token(nbf=int(time.time()) + 300)) is AuthReason.INVALID_CREDENTIAL

    def test_wrong_aud(self, jwks: _Jwks) -> None:
        """A token for another Desk Address is rejected."""
        assert _rejected(_hosted(), _token(aud="https://desk.example.com/d/other")) is AuthReason.INVALID_CREDENTIAL

    def test_wrong_iss(self, jwks: _Jwks) -> None:
        """A token from another issuer is rejected."""
        assert _rejected(_hosted(), _token(iss="https://evil.example.com")) is AuthReason.INVALID_CREDENTIAL

    def test_tampered_claim(self, jwks: _Jwks) -> None:
        """Changing a claim after signing breaks the signature."""
        import base64
        import json

        header, payload, sig = _token().split(".")
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        claims["org_id"] = "org_evil"
        forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
        assert _rejected(_hosted(), f"{header}.{forged}.{sig}") is AuthReason.INVALID_CREDENTIAL

    def test_missing_header(self, jwks: _Jwks) -> None:
        """No Authorization header is a missing credential, never anonymous."""
        assert _rejected(_hosted(), None) is AuthReason.MISSING_CREDENTIAL


# ---------------------------------------------------------------------------
# JWKS caching
# ---------------------------------------------------------------------------


class TestJwksCache:
    """Hard maximum age, Cache-Control, fail-closed expiry and the unknown-kid limit."""

    def test_cached_within_max_age(self, jwks: _Jwks, clock: _Clock) -> None:
        """Keys are reused until the maximum age."""
        auth = _hosted()
        auth(_req(_token()))
        clock.now += 599
        auth(_req(_token()))
        assert jwks.fetches == 1
        clock.now += 2
        auth(_req(_token()))
        assert jwks.fetches == 2

    def test_shorter_cache_control_wins(self, jwks: _Jwks, clock: _Clock) -> None:
        """A shorter Cache-Control max-age shortens the cache."""
        jwks.cache_control = "public, max-age=30"
        auth = _hosted()
        auth(_req(_token()))
        clock.now += 31
        auth(_req(_token()))
        assert jwks.fetches == 2

    def test_longer_cache_control_capped(self, jwks: _Jwks, clock: _Clock) -> None:
        """A longer Cache-Control max-age never extends past jwks_max_age."""
        jwks.cache_control = "max-age=86400"
        auth = _hosted()
        auth(_req(_token()))
        clock.now += 601
        auth(_req(_token()))
        assert jwks.fetches == 2

    def test_fail_closed_after_max_age(self, jwks: _Jwks, clock: _Clock) -> None:
        """Once the cache is past its maximum age and the refetch fails, nothing verifies."""
        auth = _hosted()
        assert auth(_req(_token())).authenticated
        jwks.failing = True
        clock.now += 601
        with pytest.raises(AuthUnavailableError):
            auth(_req(_token()))
        # Still closed on the next request: the stale keys were dropped, not kept.
        with pytest.raises(AuthUnavailableError):
            auth(_req(_token()))
        assert jwks.fetches == 3
        # Recovers once the endpoint does.
        jwks.failing = False
        assert auth(_req(_token())).authenticated

    def test_initial_fetch_failure_is_unavailable(self, jwks: _Jwks) -> None:
        """A JWKS outage is 'cannot verify' (503), not a rejection or a 500."""
        jwks.failing = True
        with pytest.raises(AuthUnavailableError):
            _hosted()(_req(_token()))

    def test_default_caches_forever(self, jwks: _Jwks, clock: _Clock) -> None:
        """Without jwks_max_age the existing cache-forever behaviour is kept."""
        auth = jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI)
        auth(_req(_token()))
        clock.now += 10**6
        auth(_req(_token()))
        assert jwks.fetches == 1

    def test_unknown_kid_refetch_rate_limited(self, jwks: _Jwks, clock: _Clock) -> None:
        """An unknown kid refetches at most once per interval; otherwise it is rejected."""
        auth = _hosted()
        auth(_req(_token()))
        assert jwks.fetches == 1
        rotated = _token(EC_ROTATED)
        # Within 60 s of the last fetch: rejected without contacting the endpoint.
        clock.now += 10
        assert _rejected(auth, rotated) is AuthReason.INVALID_CREDENTIAL
        assert jwks.fetches == 1
        # After the interval, one refetch; the key is still absent, so rejected.
        clock.now += 51
        assert _rejected(auth, rotated) is AuthReason.INVALID_CREDENTIAL
        assert jwks.fetches == 2
        for _ in range(5):
            _rejected(auth, rotated)
        assert jwks.fetches == 2
        # The issuer publishes the new key; after the interval it verifies.
        jwks.body = _public(EC, EC_ROTATED)
        clock.now += 61
        assert auth(_req(rotated)).authenticated
        assert jwks.fetches == 3

    def test_unknown_kid_without_limit_refetches(self, jwks: _Jwks, clock: _Clock) -> None:
        """Default (no interval) keeps refetching on every unknown kid."""
        auth = jwt_authenticate(issuer=ISS, audience=AUD, jwks_uri=JWKS_URI)
        auth(_req(_token()))
        for _ in range(3):
            _rejected(auth, _token(EC_ROTATED))
        assert jwks.fetches == 4


# ---------------------------------------------------------------------------
# make_wsgi_app(strict_auth=True)
# ---------------------------------------------------------------------------


def _strict_app(**kwargs: Any) -> falcon.testing.TestClient:
    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl

    def authenticate(req: falcon.Request) -> AuthContext:
        if req.get_header("Authorization") == "Bearer good":
            return AuthContext(domain="test", authenticated=True, principal="alice", claims={})
        raise AuthFailure(
            AuthReason.MISSING_CREDENTIAL if not req.get_header("Authorization") else AuthReason.INVALID_CREDENTIAL
        )

    options: dict[str, Any] = {
        "authenticate": authenticate,
        "strict_auth": True,
        "www_authenticate": 'Bearer error="invalid_token"',
        "cors_origins": "*",
        "token_key": b"k" * 32,
        "max_request_bytes": 1 << 20,
    }
    options.update(kwargs)
    app = make_wsgi_app(RpcServer(ConformanceService, ConformanceServiceImpl()), **options)
    return falcon.testing.TestClient(app)


class TestStrictAuth:
    """Every request but HEAD /health and CORS preflights authenticates."""

    def test_head_health_exempt(self) -> None:
        """HEAD /health answers 200 without a credential."""
        resp = _strict_app().simulate_head("/health")
        assert resp.status_code == 200

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("GET", "/health"),
            ("OPTIONS", "/health"),
            ("GET", "/"),
            ("GET", "/describe"),
            ("GET", "/.well-known/oauth-protected-resource"),
            ("GET", "/nope"),
            ("POST", "/conformance.v1/echo_string"),
        ],
    )
    def test_everything_else_401(self, method: str, path: str) -> None:
        """No Authorization header gets 401 with the configured challenge and capability headers."""
        resp = _strict_app().simulate_request(method, path, headers={"Origin": "https://app.example.com"})
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == 'Bearer error="invalid_token"'
        assert "resource_metadata" not in resp.headers["www-authenticate"]
        assert resp.headers["vgi-supported-encodings"] is not None
        assert resp.headers["access-control-allow-origin"] == "*"

    def test_bad_token_401(self) -> None:
        """A rejected credential is 401 too."""
        resp = _strict_app().simulate_get("/health", headers={"Authorization": "Bearer bad"})
        assert resp.status_code == 401

    def test_good_token_passes(self) -> None:
        """A valid credential reaches the handler."""
        resp = _strict_app().simulate_get("/health", headers={"Authorization": "Bearer good"})
        assert resp.status_code == 200

    def test_cors_preflight_exempt(self) -> None:
        """A CORS preflight is answered without credentials, with open CORS."""
        resp = _strict_app().simulate_options(
            "/conformance.v1/echo_string",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization, content-type",
            },
        )
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == "*"

    def test_body_not_read_before_auth(self) -> None:
        """An unauthenticated request is refused before its body is read or decompressed."""
        resp = _strict_app().simulate_post(
            "/conformance.v1/echo_string",
            body=b"not zstd at all",
            headers={"Content-Encoding": "zstd", "Content-Type": "application/vnd.apache.arrow.stream"},
        )
        # Not 400 (decompression error) and not 413: authentication ran first.
        assert resp.status_code == 401

    def test_oversized_body_401_before_413(self) -> None:
        """Size enforcement runs after authentication in strict mode."""
        resp = _strict_app(max_request_bytes=16).simulate_post("/conformance.v1/echo_string", body=b"x" * 1024)
        assert resp.status_code == 401

    def test_requires_authenticate(self) -> None:
        """Using strict_auth without an authenticator is a configuration error."""
        with pytest.raises(ValueError, match="authenticate"):
            _strict_app(authenticate=None)

    def test_incompatible_with_oauth_metadata(self) -> None:
        """Strict auth refuses resource-metadata discovery."""
        from vgi_rpc.http import OAuthResourceMetadata

        meta = OAuthResourceMetadata(resource="https://w.example.com", authorization_servers=(ISS,))
        with pytest.raises(ValueError):
            _strict_app(oauth_resource_metadata=meta, www_authenticate=None)

    def test_default_mode_unchanged(self) -> None:
        """Without strict_auth, GET /health stays exempt."""
        resp = _strict_app(strict_auth=False).simulate_get("/health")
        assert resp.status_code == 200
