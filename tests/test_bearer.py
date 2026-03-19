# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for bearer token authentication and chain authenticate combinator."""

from __future__ import annotations

import time
from typing import Protocol

import falcon
import falcon.testing.helpers
import pytest
from authlib.jose import JsonWebKey, jwt

from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import (
    bearer_authenticate,
    bearer_authenticate_static,
    chain_authenticate,
    http_connect,
    make_sync_client,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _IdentityService(Protocol):
    def whoami(self) -> str: ...


class _IdentityImpl:
    def whoami(self, ctx: CallContext) -> str:
        ctx.auth.require_authenticated()
        return f"{ctx.auth.domain}:{ctx.auth.principal}"


_ALICE = AuthContext(domain="bearer", authenticated=True, principal="alice", claims={})
_BOB = AuthContext(domain="apikey", authenticated=True, principal="bob", claims={"role": "admin"})

_TOKENS: dict[str, AuthContext] = {
    "token-alice": _ALICE,
    "token-bob": _BOB,
}


def _make_req(*, authorization: str | None = None) -> falcon.Request:
    """Create a Falcon test request with optional Authorization header."""
    headers: dict[str, str] = {}
    if authorization is not None:
        headers["Authorization"] = authorization
    return falcon.testing.helpers.create_req(headers=headers)


# ---------------------------------------------------------------------------
# TestBearerAuthenticate
# ---------------------------------------------------------------------------


class TestBearerAuthenticate:
    """Tests for bearer_authenticate factory."""

    def test_valid_token(self) -> None:
        """A valid token calls validate and returns its AuthContext."""

        def validate(t: str) -> AuthContext:
            if t == "good":
                return _ALICE
            raise ValueError("bad")

        auth_fn = bearer_authenticate(validate=validate)
        req = _make_req(authorization="Bearer good")
        auth = auth_fn(req)
        assert auth.authenticated is True
        assert auth.principal == "alice"

    def test_invalid_token_raises(self) -> None:
        """An unknown token raises ValueError from validate."""

        def reject(_token: str) -> AuthContext:
            raise ValueError("invalid token")

        auth_fn = bearer_authenticate(validate=reject)
        req = _make_req(authorization="Bearer bad-token")
        with pytest.raises(ValueError, match="invalid token"):
            auth_fn(req)

    def test_missing_header_raises(self) -> None:
        """Missing Authorization header raises ValueError."""
        auth_fn = bearer_authenticate(validate=lambda t: _ALICE)
        req = _make_req()
        with pytest.raises(ValueError, match="Missing"):
            auth_fn(req)

    def test_non_bearer_scheme_raises(self) -> None:
        """Non-Bearer scheme raises ValueError."""
        auth_fn = bearer_authenticate(validate=lambda t: _ALICE)
        req = _make_req(authorization="Basic dXNlcjpwYXNz")
        with pytest.raises(ValueError, match="Missing"):
            auth_fn(req)

    def test_end_to_end_rpc(self) -> None:
        """Full round-trip: bearer auth -> RPC call -> identity returned."""

        def validate(token: str) -> AuthContext:
            if token == "secret-key":
                return _ALICE
            raise ValueError("bad token")

        auth_fn = bearer_authenticate(validate=validate)
        server = RpcServer(_IdentityService, _IdentityImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            default_headers={"Authorization": "Bearer secret-key"},
        )
        with http_connect(_IdentityService, client=client) as svc:
            result = svc.whoami()
            assert result == "bearer:alice"


# ---------------------------------------------------------------------------
# TestBearerAuthenticateStatic
# ---------------------------------------------------------------------------


class TestBearerAuthenticateStatic:
    """Tests for bearer_authenticate_static convenience wrapper."""

    def test_known_token(self) -> None:
        """A known token returns the mapped AuthContext."""
        auth_fn = bearer_authenticate_static(tokens=_TOKENS)
        req = _make_req(authorization="Bearer token-alice")
        auth = auth_fn(req)
        assert auth.principal == "alice"
        assert auth.domain == "bearer"

    def test_unknown_token_raises(self) -> None:
        """An unknown token raises ValueError."""
        auth_fn = bearer_authenticate_static(tokens=_TOKENS)
        req = _make_req(authorization="Bearer unknown")
        with pytest.raises(ValueError, match="Unknown bearer token"):
            auth_fn(req)


# ---------------------------------------------------------------------------
# TestChainAuthenticate
# ---------------------------------------------------------------------------


class TestChainAuthenticate:
    """Tests for chain_authenticate combinator."""

    def test_first_succeeds(self) -> None:
        """First authenticator succeeds — returned immediately."""
        first = bearer_authenticate_static(tokens={"t1": _ALICE})
        second = bearer_authenticate_static(tokens={"t2": _BOB})
        chain = chain_authenticate(first, second)
        req = _make_req(authorization="Bearer t1")
        auth = chain(req)
        assert auth.principal == "alice"

    def test_fallback_to_second(self) -> None:
        """First raises ValueError, second succeeds."""
        first = bearer_authenticate_static(tokens={"t1": _ALICE})
        second = bearer_authenticate_static(tokens={"t2": _BOB})
        chain = chain_authenticate(first, second)
        req = _make_req(authorization="Bearer t2")
        auth = chain(req)
        assert auth.principal == "bob"

    def test_all_fail(self) -> None:
        """All authenticators fail — raises ValueError."""
        first = bearer_authenticate_static(tokens={"t1": _ALICE})
        second = bearer_authenticate_static(tokens={"t2": _BOB})
        chain = chain_authenticate(first, second)
        req = _make_req(authorization="Bearer unknown")
        with pytest.raises(ValueError, match="No authenticator accepted"):
            chain(req)

    def test_permission_error_propagates(self) -> None:
        """PermissionError propagates immediately, skipping later authenticators."""

        def forbidden(_req: falcon.Request) -> AuthContext:
            raise PermissionError("access denied")

        never_called = bearer_authenticate_static(tokens={"t": _ALICE})
        chain = chain_authenticate(forbidden, never_called)
        req = _make_req(authorization="Bearer t")
        with pytest.raises(PermissionError, match="access denied"):
            chain(req)

    def test_non_auth_exception_propagates(self) -> None:
        """Non-ValueError/PermissionError exceptions propagate immediately."""

        def broken(_req: falcon.Request) -> AuthContext:
            raise RuntimeError("bug in authenticator")

        chain = chain_authenticate(broken)
        req = _make_req(authorization="Bearer x")
        with pytest.raises(RuntimeError, match="bug in authenticator"):
            chain(req)

    def test_empty_chain_raises(self) -> None:
        """Empty chain raises ValueError at construction time."""
        with pytest.raises(ValueError, match="at least one"):
            chain_authenticate()

    def test_jwt_plus_bearer_end_to_end(self) -> None:
        """Integration: chain JWT + bearer, both accepted for RPC calls."""
        # Set up JWT authenticator (local, no JWKS endpoint)
        key = JsonWebKey.generate_key("RSA", 2048, is_private=True)
        priv = key.as_dict(is_private=True)
        priv["kid"] = "test-kid"
        pub = JsonWebKey.import_key(priv).as_dict()

        now = int(time.time())
        header = {"alg": "RS256", "kid": "test-kid"}
        payload = {
            "iss": "https://auth.example.com",
            "aud": "https://api.example.com/vgi",
            "sub": "jwt-user",
            "iat": now,
            "exp": now + 3600,
        }
        token_bytes: bytes = jwt.encode(header, payload, priv)
        jwt_token = token_bytes.decode()

        def jwt_auth(req: falcon.Request) -> AuthContext:
            auth_header = req.get_header("Authorization") or ""
            if not auth_header.startswith("Bearer "):
                raise ValueError("Missing Authorization")
            from authlib.jose.errors import JoseError

            try:
                claims = jwt.decode(
                    auth_header[7:],
                    pub,
                    claims_options={
                        "iss": {"essential": True, "value": "https://auth.example.com"},
                        "aud": {"essential": True, "value": "https://api.example.com/vgi"},
                    },
                )
                claims.validate()
            except JoseError as exc:
                raise ValueError(f"Invalid JWT: {exc}") from exc
            return AuthContext(domain="jwt", authenticated=True, principal=str(claims["sub"]), claims=dict(claims))

        # Set up bearer authenticator
        api_key_auth = bearer_authenticate_static(tokens={"api-key-123": _BOB})

        # Chain them: try JWT first, fall back to API key
        chain = chain_authenticate(jwt_auth, api_key_auth)

        server = RpcServer(_IdentityService, _IdentityImpl())

        # Test with JWT token
        client_jwt = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=chain,
            default_headers={"Authorization": f"Bearer {jwt_token}"},
        )
        with http_connect(_IdentityService, client=client_jwt) as svc:
            assert svc.whoami() == "jwt:jwt-user"

        # Test with API key
        client_key = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=chain,
            default_headers={"Authorization": "Bearer api-key-123"},
        )
        with http_connect(_IdentityService, client=client_key) as svc:
            assert svc.whoami() == "apikey:bob"

    def test_invalid_token_rejected_by_all(self) -> None:
        """A completely invalid token is rejected by all authenticators in chain."""
        first = bearer_authenticate_static(tokens={"t1": _ALICE})
        second = bearer_authenticate_static(tokens={"t2": _BOB})
        chain = chain_authenticate(first, second)

        server = RpcServer(_IdentityService, _IdentityImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=chain,
            default_headers={"Authorization": "Bearer totally-invalid"},
        )
        # The HTTP layer should return 401
        resp = client.post(
            "/whoami",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401
