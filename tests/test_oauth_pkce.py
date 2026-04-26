# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for OAuth PKCE browser flow in vgi-rpc HTTP transport."""

from __future__ import annotations

import base64
import hashlib
import time
from collections.abc import Callable
from typing import Protocol
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import falcon
import falcon.testing
import pytest
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey, import_key

from vgi_rpc import AuthContext, RpcServer
from vgi_rpc.http import OAuthResourceMetadata
from vgi_rpc.http._oauth_pkce import (
    _AUTH_COOKIE_NAME,
    _SESSION_COOKIE_NAME,
    _derive_session_key,
    _generate_code_challenge,
    _generate_code_verifier,
    _generate_state_nonce,
    _pack_oauth_cookie,
    _unpack_oauth_cookie,
    _validate_original_url,
    make_cookie_authenticate,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


class _EchoService(Protocol):
    def echo(self, message: str) -> str: ...


class _EchoImpl:
    def echo(self, message: str) -> str:
        return message


def _make_rsa_key() -> tuple[dict[str, str | list[str]], dict[str, str | list[str]]]:
    key = RSAKey.generate_key(2048)
    priv = key.as_dict(private=True)
    priv["kid"] = "test-kid"
    pub = import_key(priv).as_dict(private=False)
    return priv, pub


def _mint_jwt(
    private_key: dict[str, str | list[str]],
    *,
    sub: str = "testuser",
    email: str = "test@example.com",
    iss: str = "https://auth.example.com",
    aud: str = "my-client-id",
    exp_offset: int = 3600,
) -> str:
    now = int(time.time())
    header = {"alg": "RS256", "kid": private_key.get("kid", "test-kid")}
    payload = {
        "iss": iss,
        "aud": aud,
        "sub": sub,
        "email": email,
        "iat": now,
        "exp": now + exp_offset,
    }
    return jwt.encode(header, payload, import_key(private_key))


def _make_local_auth(
    public_key: dict[str, str | list[str]],
    *,
    issuer: str = "https://auth.example.com",
    audience: str | tuple[str, ...] = "my-client-id",
) -> Callable[[falcon.Request], AuthContext]:
    from joserfc.errors import JoseError

    audiences = (audience,) if isinstance(audience, str) else audience
    key_set = KeySet.import_key_set({"keys": [public_key]})

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")
        raw_token = auth_header[7:]
        try:
            decoded = jwt.decode(raw_token, key_set)
            registry = jwt.JWTClaimsRegistry(
                iss={"essential": True, "value": issuer},
                aud={"essential": True, "values": list(audiences)},
            )
            registry.validate(decoded.claims)
        except JoseError as exc:
            raise ValueError(f"Invalid JWT: {exc}") from exc
        claims = dict(decoded.claims)
        return AuthContext(
            domain="jwt",
            authenticated=True,
            principal=str(claims.get("sub", "")),
            claims=claims,
        )

    return authenticate


_SIGNING_KEY = b"test-signing-key-32-bytes-long!!"

_METADATA_PKCE = OAuthResourceMetadata(
    resource="http://localhost:8000/vgi",
    authorization_servers=("https://auth.example.com",),
    client_id="my-client-id",
    client_secret="my-client-secret",
    use_id_token_as_bearer=True,
    resource_name="Test PKCE Service",
)

_METADATA_NO_CLIENT_ID = OAuthResourceMetadata(
    resource="http://localhost:8000/vgi",
    authorization_servers=("https://auth.example.com",),
    resource_name="Test No ClientID",
)


# ---------------------------------------------------------------------------
# PKCE helpers
# ---------------------------------------------------------------------------


class TestPkceHelpers:
    """Test PKCE code verifier, challenge, and state nonce generation."""

    def test_code_verifier_length(self) -> None:
        """Generated code verifier has valid RFC 7636 length."""
        cv = _generate_code_verifier()
        assert 43 <= len(cv) <= 128

    def test_code_verifier_charset(self) -> None:
        """Generated code verifier uses only URL-safe characters."""
        import re

        cv = _generate_code_verifier()
        assert re.fullmatch(r"[A-Za-z0-9_-]+", cv)

    def test_code_verifier_uniqueness(self) -> None:
        """Each generated code verifier is unique."""
        verifiers = {_generate_code_verifier() for _ in range(100)}
        assert len(verifiers) == 100

    def test_code_challenge_s256(self) -> None:
        """S256 challenge matches RFC 7636 Appendix B test vector."""
        cv = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        expected = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        assert _generate_code_challenge(cv) == expected

    def test_state_nonce_uniqueness(self) -> None:
        """Each generated state nonce is unique."""
        nonces = {_generate_state_nonce() for _ in range(100)}
        assert len(nonces) == 100


# ---------------------------------------------------------------------------
# Signed session cookie
# ---------------------------------------------------------------------------


class TestSignedSessionCookie:
    """Test signed OAuth session cookie pack/unpack and tamper detection."""

    def setup_method(self) -> None:
        """Derive session key for each test."""
        self.session_key = _derive_session_key(_SIGNING_KEY)

    def test_roundtrip(self) -> None:
        """Pack and unpack preserves all fields with empty return_to."""
        cv = "test-code-verifier"
        state = "test-state"
        url = "/vgi/"
        cookie = _pack_oauth_cookie(cv, state, url, self.session_key)
        got_cv, got_state, got_url, got_rt = _unpack_oauth_cookie(cookie, self.session_key)
        assert got_cv == cv
        assert got_state == state
        assert got_url == url
        assert got_rt == ""

    def test_roundtrip_with_return_to(self) -> None:
        """Pack and unpack preserves return_to URL."""
        cv = "test-code-verifier"
        state = "test-state"
        url = "/vgi/"
        rt = "http://localhost:4321/?service=https://example.com"
        cookie = _pack_oauth_cookie(cv, state, url, self.session_key, return_to=rt)
        got_cv, got_state, got_url, got_rt = _unpack_oauth_cookie(cookie, self.session_key)
        assert got_cv == cv
        assert got_state == state
        assert got_url == url
        assert got_rt == rt

    def test_tampered_rejected(self) -> None:
        """Tampered cookie bytes are rejected by HMAC verification."""
        cookie = _pack_oauth_cookie("cv", "st", "/", self.session_key)
        # Flip a bit
        raw = base64.urlsafe_b64decode(cookie)
        tampered = raw[:5] + bytes([raw[5] ^ 0xFF]) + raw[6:]
        tampered_cookie = base64.urlsafe_b64encode(tampered).decode()
        with pytest.raises(ValueError, match="signature mismatch"):
            _unpack_oauth_cookie(tampered_cookie, self.session_key)

    def test_expired_rejected(self) -> None:
        """Expired cookies are rejected based on max_age."""
        old_time = int(time.time()) - 700  # 700s ago, max_age=600
        cookie = _pack_oauth_cookie("cv", "st", "/", self.session_key, created_at=old_time)
        with pytest.raises(ValueError, match="expired"):
            _unpack_oauth_cookie(cookie, self.session_key, max_age=600)

    def test_wrong_key_rejected(self) -> None:
        """Cookie signed with a different key is rejected."""
        cookie = _pack_oauth_cookie("cv", "st", "/", self.session_key)
        wrong_key = _derive_session_key(b"wrong-key-wrong-key-wrong-key!!")
        with pytest.raises(ValueError, match="signature mismatch"):
            _unpack_oauth_cookie(cookie, wrong_key)

    def test_truncated_rejected(self) -> None:
        """Truncated cookie data is rejected as too short."""
        with pytest.raises(ValueError, match="too short"):
            _unpack_oauth_cookie("dG9vc2hvcnQ=", self.session_key)

    def test_cross_protocol_rejected(self) -> None:
        """Stream state tokens (version 2) must not be accepted as session cookies."""
        # Build a fake token with version 2
        import struct

        payload = struct.pack("B", 2) + struct.pack("<Q", int(time.time()))
        # Add enough bytes to pass length check
        payload += b"\x00" * 20
        mac = __import__("hmac").new(self.session_key, payload, hashlib.sha256).digest()
        fake = base64.urlsafe_b64encode(payload + mac).decode()
        with pytest.raises(ValueError, match="version"):
            _unpack_oauth_cookie(fake, self.session_key)


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------


class TestValidateOriginalUrl:
    """Test original URL validation for redirect safety."""

    def test_relative_url_passes(self) -> None:
        """Relative URL within prefix is accepted."""
        assert _validate_original_url("/vgi/", "/vgi") == "/vgi/"

    def test_with_query_string(self) -> None:
        """URL with query string is preserved."""
        assert _validate_original_url("/vgi/?foo=bar", "/vgi") == "/vgi/?foo=bar"

    def test_absolute_url_rejected(self) -> None:
        """Absolute URL is rejected as potential open redirect."""
        assert _validate_original_url("https://evil.com/steal", "/vgi") == "/vgi"

    def test_wrong_prefix_rejected(self) -> None:
        """URL outside expected prefix falls back to prefix root."""
        assert _validate_original_url("/other/path", "/vgi") == "/vgi"

    def test_long_url_truncated(self) -> None:
        """URLs exceeding max length are truncated."""
        long_url = "/vgi/" + "a" * 3000
        result = _validate_original_url(long_url, "/vgi")
        assert len(result) <= 2048

    def test_empty_prefix(self) -> None:
        """Empty prefix allows any relative URL."""
        assert _validate_original_url("/anything", "") == "/anything"


# ---------------------------------------------------------------------------
# Cookie authenticate wrapper
# ---------------------------------------------------------------------------


def _make_request(
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> falcon.Request:
    """Create a Falcon request with optional headers and cookies."""
    environ = falcon.testing.create_environ(
        method="GET",
        path="/",
        headers=headers,
        cookies=cookies,
    )
    return falcon.Request(environ)


class TestCookieAuthenticate:
    """Test cookie-based authentication fallback via make_cookie_authenticate."""

    def setup_method(self) -> None:
        """Generate RSA keys and create local authenticator."""
        self.priv, self.pub = _make_rsa_key()
        self.inner = _make_local_auth(self.pub)

    def test_header_passthrough(self) -> None:
        """When Authorization header is present, inner is called directly."""
        token = _mint_jwt(self.priv)
        from vgi_rpc.http._bearer import chain_authenticate

        cookie_auth = make_cookie_authenticate(self.inner)
        combined = chain_authenticate(self.inner, cookie_auth)
        req = _make_request(headers={"Authorization": f"Bearer {token}"})
        ctx = combined(req)
        assert ctx.authenticated
        assert ctx.principal == "testuser"

    def test_cookie_fallback(self) -> None:
        """When no Authorization header, falls back to cookie."""
        token = _mint_jwt(self.priv)
        from vgi_rpc.http._bearer import chain_authenticate

        cookie_auth = make_cookie_authenticate(self.inner)
        combined = chain_authenticate(self.inner, cookie_auth)
        req = _make_request(cookies={_AUTH_COOKIE_NAME: token})
        ctx = combined(req)
        assert ctx.authenticated
        assert ctx.principal == "testuser"

    def test_neither_raises(self) -> None:
        """When neither header nor cookie, raises ValueError."""
        from vgi_rpc.http._bearer import chain_authenticate

        cookie_auth = make_cookie_authenticate(self.inner)
        combined = chain_authenticate(self.inner, cookie_auth)
        req = _make_request()
        with pytest.raises(ValueError):
            combined(req)

    def test_invalid_cookie_token_raises(self) -> None:
        """Invalid token in cookie still raises ValueError."""
        from vgi_rpc.http._bearer import chain_authenticate

        cookie_auth = make_cookie_authenticate(self.inner)
        combined = chain_authenticate(self.inner, cookie_auth)
        req = _make_request(cookies={_AUTH_COOKIE_NAME: "bad-token"})
        with pytest.raises(ValueError):
            combined(req)

    def test_header_restored_after_cookie_auth(self) -> None:
        """WSGI environ is restored even after cookie auth."""
        token = _mint_jwt(self.priv)
        cookie_auth = make_cookie_authenticate(self.inner)
        req = _make_request(cookies={_AUTH_COOKIE_NAME: token})
        cookie_auth(req)
        # Authorization header should not persist
        assert "HTTP_AUTHORIZATION" not in req.env


# ---------------------------------------------------------------------------
# Integration: PKCE redirect on browser GET
# ---------------------------------------------------------------------------


def _make_test_app(
    authenticate: Callable[..., AuthContext] | None = None,
    oauth_metadata: OAuthResourceMetadata | None = None,
    prefix: str = "/vgi",
) -> falcon.testing.TestClient:
    """Create a Falcon test client with optional PKCE wiring."""
    server = RpcServer(_EchoService, _EchoImpl(), enable_describe=True)
    from vgi_rpc.http import make_wsgi_app

    app = make_wsgi_app(
        server,
        prefix=prefix,
        signing_key=_SIGNING_KEY,
        authenticate=authenticate,
        oauth_resource_metadata=oauth_metadata,
        compression_level=None,
    )
    return falcon.testing.TestClient(app)


# Mock OIDC discovery response
_MOCK_OIDC_CONFIG = {
    "authorization_endpoint": "https://auth.example.com/authorize",
    "token_endpoint": "https://auth.example.com/token",
    "jwks_uri": "https://auth.example.com/.well-known/jwks.json",
    "issuer": "https://auth.example.com",
}


class TestPkceRedirect:
    """Test that browser GETs get redirected to OAuth when unauthenticated."""

    def setup_method(self) -> None:
        """Generate RSA keys and create local authenticator."""
        self.priv, self.pub = _make_rsa_key()
        self.auth = _make_local_auth(self.pub)

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_browser_get_redirects(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """Unauthenticated browser GET -> 302 to authorization endpoint."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get("/vgi", headers={"Accept": "text/html"})
        assert result.status_code == 302
        location = result.headers.get("location", "")
        parsed = urlparse(location)
        assert parsed.netloc == "auth.example.com"
        assert parsed.path == "/authorize"
        params = parse_qs(parsed.query)
        assert params["response_type"] == ["code"]
        assert params["client_id"] == ["my-client-id"]
        assert params["code_challenge_method"] == ["S256"]
        assert "code_challenge" in params
        assert "state" in params
        # Session cookie should be set
        assert _SESSION_COOKIE_NAME in result.cookies

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_non_browser_get_gets_401(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """Non-browser GET (no text/html Accept) -> 401, no redirect."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get("/vgi", headers={"Accept": "application/json"})
        assert result.status_code == 401

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_post_gets_401(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """POST requests -> 401, no redirect even with text/html Accept."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_post("/vgi/echo", headers={"Accept": "text/html"})
        # POST to RPC endpoint without auth -> 401 (not redirected)
        assert result.status_code == 401

    def test_authenticated_browser_get_succeeds(self) -> None:
        """Browser GET with valid cookie -> 200 (page served)."""
        token = _mint_jwt(self.priv)

        # Need to mock OIDC discovery since make_wsgi_app with PKCE creates the discovery callable
        with patch("vgi_rpc.http._oauth_pkce.httpx.Client") as mock_client_cls:
            mock_resp = type(
                "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
            )()
            mock_client = type(
                "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
            )()
            mock_client_cls.return_value = mock_client

            client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
            result = client.simulate_get(
                "/vgi",
                headers={
                    "Accept": "text/html",
                    "Cookie": f"{_AUTH_COOKIE_NAME}={token}",
                },
            )
            assert result.status_code == 200


# ---------------------------------------------------------------------------
# Integration: OAuth callback
# ---------------------------------------------------------------------------


class TestOAuthCallback:
    """Test OAuth callback endpoint handling."""

    def setup_method(self) -> None:
        """Generate RSA keys and derive session key."""
        self.priv, self.pub = _make_rsa_key()
        self.auth = _make_local_auth(self.pub)
        self.session_key = _derive_session_key(_SIGNING_KEY)

    def _make_session_cookie(self, state: str = "test-state", url: str = "/vgi/") -> str:
        """Create a signed session cookie for testing."""
        return _pack_oauth_cookie("test-verifier", state, url, self.session_key)

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_missing_code_returns_400(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """Missing authorization code in callback returns 400."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get("/vgi/_oauth/callback", params={"state": "test"})
        assert result.status_code == 400

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_google_error_returns_400(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """Authorization server error in callback returns 400 with message."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get(
            "/vgi/_oauth/callback",
            params={"error": "access_denied", "error_description": "User denied"},
        )
        assert result.status_code == 400
        assert b"authorization server returned an error" in result.content

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_state_mismatch_returns_400(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """State parameter mismatch (CSRF) returns 400."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        cookie = self._make_session_cookie(state="expected-state")
        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get(
            "/vgi/_oauth/callback",
            params={"code": "authcode", "state": "wrong-state"},
            headers={"Cookie": f"{_SESSION_COOKIE_NAME}={cookie}"},
        )
        assert result.status_code == 400
        assert b"State mismatch" in result.content

    @patch("vgi_rpc.http._oauth_pkce._exchange_code_for_token")
    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_successful_callback_redirects_with_cookie(self, mock_client_cls, mock_exchange) -> None:  # type: ignore[no-untyped-def]
        """Successful code exchange redirects with auth cookie set."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        token = _mint_jwt(self.priv)
        mock_exchange.return_value = (token, 3600, None)

        state = "test-state-123"
        cookie = self._make_session_cookie(state=state, url="/vgi/describe")
        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get(
            "/vgi/_oauth/callback",
            params={"code": "authcode123", "state": state},
            headers={"Cookie": f"{_SESSION_COOKIE_NAME}={cookie}"},
        )
        assert result.status_code == 302
        assert result.headers.get("location") == "/vgi/describe"
        # Auth cookie should be set
        assert _AUTH_COOKIE_NAME in result.cookies


# ---------------------------------------------------------------------------
# Integration: OAuth logout
# ---------------------------------------------------------------------------


class TestOAuthLogout:
    """Test OAuth logout endpoint clears cookies and redirects."""

    def setup_method(self) -> None:
        """Generate RSA keys and create local authenticator."""
        self.priv, self.pub = _make_rsa_key()
        self.auth = _make_local_auth(self.pub)

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_logout_clears_cookie_and_redirects(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """Logout clears auth cookie and redirects to prefix root."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get("/vgi/_oauth/logout")
        assert result.status_code == 302
        assert result.headers.get("location") == "/vgi"


# ---------------------------------------------------------------------------
# Non-OAuth auth schemes (PKCE must not interfere)
# ---------------------------------------------------------------------------


class TestNonOAuthAuthSchemes:
    """Test that PKCE does not interfere with non-OAuth auth schemes."""

    def setup_method(self) -> None:
        """Generate RSA keys and create local authenticator."""
        self.priv, self.pub = _make_rsa_key()
        self.auth = _make_local_auth(self.pub)

    def test_bearer_only_no_redirect(self) -> None:
        """Auth without oauth_resource_metadata -> plain 401, no redirect."""
        client = _make_test_app(authenticate=self.auth, oauth_metadata=None)
        result = client.simulate_get("/vgi", headers={"Accept": "text/html"})
        assert result.status_code == 401
        assert "location" not in result.headers

    def test_metadata_without_client_id_no_redirect(self) -> None:
        """OAuth metadata without client_id -> plain 401, no redirect."""
        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_NO_CLIENT_ID)
        result = client.simulate_get("/vgi", headers={"Accept": "text/html"})
        assert result.status_code == 401
        assert "location" not in result.headers

    def test_no_auth_pages_public(self) -> None:
        """No authenticate -> pages are public."""
        client = _make_test_app(authenticate=None, oauth_metadata=None)
        result = client.simulate_get("/vgi", headers={"Accept": "text/html"})
        assert result.status_code == 200


# ---------------------------------------------------------------------------
# Landing page user info injection
# ---------------------------------------------------------------------------


class TestUserInfoInjection:
    """Test user info JS injection on landing pages when PKCE is active."""

    def setup_method(self) -> None:
        """Generate RSA keys and create local authenticator."""
        self.priv, self.pub = _make_rsa_key()
        self.auth = _make_local_auth(self.pub)

    @patch("vgi_rpc.http._oauth_pkce.httpx.Client")
    def test_landing_page_has_user_info_script(self, mock_client_cls) -> None:  # type: ignore[no-untyped-def]
        """When PKCE is active, landing page includes user-info JS."""
        mock_resp = type(
            "R", (), {"status_code": 200, "raise_for_status": lambda s: None, "json": lambda s: _MOCK_OIDC_CONFIG}
        )()
        mock_client = type(
            "C", (), {"get": lambda s, *a, **k: mock_resp, "__enter__": lambda s: s, "__exit__": lambda *a: None}
        )()
        mock_client_cls.return_value = mock_client

        token = _mint_jwt(self.priv)
        client = _make_test_app(authenticate=self.auth, oauth_metadata=_METADATA_PKCE)
        result = client.simulate_get(
            "/vgi",
            headers={"Accept": "text/html", "Cookie": f"{_AUTH_COOKIE_NAME}={token}"},
        )
        assert result.status_code == 200
        assert b"vgi-user-info" in result.content
        assert b"_oauth/logout" in result.content

    def test_landing_page_no_user_info_without_pkce(self) -> None:
        """Without PKCE, landing page does not include user-info JS."""
        client = _make_test_app(authenticate=None, oauth_metadata=None)
        result = client.simulate_get("/vgi", headers={"Accept": "text/html"})
        assert result.status_code == 200
        assert b"vgi-user-info" not in result.content
