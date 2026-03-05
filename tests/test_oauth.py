# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for RFC 9728 OAuth Protected Resource Metadata and JWT authentication."""

from __future__ import annotations

import dataclasses
import json
import time
from collections.abc import Callable
from typing import Protocol

import falcon
import pytest
from authlib.jose import JsonWebKey, jwt
from authlib.jose.errors import JoseError

from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import (
    OAuthResourceMetadata,
    OAuthResourceMetadataResponse,
    fetch_oauth_metadata,
    http_connect,
    http_oauth_metadata,
    jwt_authenticate,
    make_sync_client,
    parse_client_id,
    parse_resource_metadata_url,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


class _EchoService(Protocol):
    def echo(self, message: str) -> str: ...


class _EchoImpl:
    def echo(self, message: str) -> str:
        return message


class _IdentityService(Protocol):
    def whoami(self) -> str: ...


class _IdentityImpl:
    def whoami(self, ctx: CallContext) -> str:
        ctx.auth.require_authenticated()
        return f"{ctx.auth.principal}"


def _make_rsa_key() -> tuple[dict[str, object], dict[str, object]]:
    """Generate an RSA key pair and return (private_dict, public_dict)."""
    key = JsonWebKey.generate_key("RSA", 2048, is_private=True)
    priv = key.as_dict(is_private=True)
    priv["kid"] = "test-kid"
    pub = JsonWebKey.import_key(priv).as_dict()
    return priv, pub


def _mint_jwt(
    private_key: dict[str, object],
    *,
    sub: str = "testuser",
    iss: str = "https://auth.example.com",
    aud: str = "https://api.example.com/vgi",
    exp_offset: int = 3600,
) -> str:
    """Create a signed JWT string."""
    now = int(time.time())
    header = {"alg": "RS256", "kid": private_key.get("kid", "test-kid")}
    payload = {"iss": iss, "aud": aud, "sub": sub, "iat": now, "exp": now + exp_offset}
    token_bytes: bytes = jwt.encode(header, payload, private_key)
    return token_bytes.decode()


def _make_local_auth(
    public_key: dict[str, object],
    *,
    issuer: str = "https://auth.example.com",
    audience: str = "https://api.example.com/vgi",
    principal_claim: str = "sub",
    domain: str = "jwt",
) -> Callable[[falcon.Request], AuthContext]:
    """Create a local JWT authenticate callback (no JWKS endpoint needed)."""

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")
        raw_token = auth_header[7:]
        try:
            claims = jwt.decode(
                raw_token,
                public_key,
                claims_options={
                    "iss": {"essential": True, "value": issuer},
                    "aud": {"essential": True, "value": audience},
                },
            )
            claims.validate()
        except JoseError as exc:
            raise ValueError(f"Invalid JWT: {exc}") from exc
        return AuthContext(
            domain=domain,
            authenticated=True,
            principal=str(claims.get(principal_claim, "")),
            claims=dict(claims),
        )

    return authenticate


_METADATA = OAuthResourceMetadata(
    resource="https://api.example.com/vgi",
    authorization_servers=("https://auth.example.com",),
    scopes_supported=("read", "write"),
    resource_name="Test Service",
)

_METADATA_WITH_CLIENT_ID = dataclasses.replace(_METADATA, client_id="my-client-id")


# ---------------------------------------------------------------------------
# TestOAuthResourceMetadata
# ---------------------------------------------------------------------------


class TestOAuthResourceMetadata:
    """Tests for RFC 9728 Protected Resource Metadata."""

    def test_well_known_returns_metadata_json(self) -> None:
        """GET /.well-known/oauth-protected-resource returns correct JSON."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA)
        resp = client.get("/.well-known/oauth-protected-resource")
        assert resp.status_code == 200
        body = json.loads(resp.content)
        assert body["resource"] == "https://api.example.com/vgi"
        assert body["authorization_servers"] == ["https://auth.example.com"]
        assert body["scopes_supported"] == ["read", "write"]
        assert body["resource_name"] == "Test Service"

    def test_well_known_path_variant(self) -> None:
        """GET /.well-known/oauth-protected-resource/vgi works with prefix."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA)
        resp = client.get("/.well-known/oauth-protected-resource/vgi")
        assert resp.status_code == 200
        body = json.loads(resp.content)
        assert body["resource"] == "https://api.example.com/vgi"

    def test_empty_authorization_servers_raises(self) -> None:
        """Empty authorization_servers raises ValueError."""
        with pytest.raises(ValueError, match="at least one entry"):
            OAuthResourceMetadata(
                resource="https://example.com/vgi",
                authorization_servers=(),
            )

    def test_empty_resource_raises(self) -> None:
        """Empty resource raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            OAuthResourceMetadata(
                resource="",
                authorization_servers=("https://auth.example.com",),
            )

    def test_malformed_metadata_json_raises(self) -> None:
        """Missing required fields in metadata JSON raises ValueError."""
        from vgi_rpc.http._client import _parse_metadata_json

        with pytest.raises(ValueError, match="missing required field"):
            _parse_metadata_json({"scopes_supported": ["read"]})

    def test_well_known_omits_default_fields(self) -> None:
        """Default-valued fields are omitted from the JSON."""
        minimal = OAuthResourceMetadata(
            resource="https://example.com/vgi",
            authorization_servers=("https://auth.example.com",),
        )
        d = minimal.to_json_dict()
        assert "scopes_supported" not in d
        assert "bearer_methods_supported" not in d
        assert "resource_name" not in d
        assert "client_id" not in d

    def test_well_known_exempt_from_auth(self) -> None:
        """Well-known endpoint is accessible even with auth enabled."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        # No default Authorization header — should still access well-known
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            oauth_resource_metadata=_METADATA,
        )
        resp = client.get("/.well-known/oauth-protected-resource")
        assert resp.status_code == 200

    def test_401_includes_www_authenticate(self) -> None:
        """401 responses include WWW-Authenticate header when metadata is configured."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            oauth_resource_metadata=_METADATA,
        )
        # POST without auth should get 401 with WWW-Authenticate
        resp = client.post(
            "/vgi/echo",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401
        www_auth = resp.headers.get("www-authenticate", "")
        assert "Bearer" in www_auth
        assert "resource_metadata" in www_auth

    def test_401_no_www_authenticate_without_metadata(self) -> None:
        """401 responses do NOT include WWW-Authenticate without metadata."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
        )
        resp = client.post(
            "/vgi/echo",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401
        www_auth = resp.headers.get("www-authenticate", "")
        assert "resource_metadata" not in www_auth

    def test_client_discovery(self) -> None:
        """http_oauth_metadata() discovers server metadata."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA)
        meta = http_oauth_metadata(client=client)
        assert meta is not None
        assert isinstance(meta, OAuthResourceMetadataResponse)
        assert meta.resource == "https://api.example.com/vgi"
        assert meta.authorization_servers == ("https://auth.example.com",)
        assert meta.scopes_supported == ("read", "write")
        assert meta.resource_name == "Test Service"

    def test_client_discovery_returns_none_on_404(self) -> None:
        """http_oauth_metadata() returns None when no metadata is configured."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k")
        meta = http_oauth_metadata(client=client)
        assert meta is None

    def test_cache_control_header(self) -> None:
        """Well-known response includes Cache-Control header."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA)
        resp = client.get("/.well-known/oauth-protected-resource")
        assert resp.status_code == 200
        cache = resp.headers.get("cache-control", "")
        assert "max-age" in cache

    def test_backwards_compatible(self) -> None:
        """Server without oauth_resource_metadata still works normally."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k")
        with http_connect(_EchoService, client=client) as svc:
            assert svc.echo(message="hello") == "hello"

    def test_parse_resource_metadata_url(self) -> None:
        """parse_resource_metadata_url extracts the URL from WWW-Authenticate."""
        header = 'Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource/vgi"'
        url = parse_resource_metadata_url(header)
        assert url == "https://api.example.com/.well-known/oauth-protected-resource/vgi"

    def test_parse_resource_metadata_url_missing(self) -> None:
        """parse_resource_metadata_url returns None when not present."""
        assert parse_resource_metadata_url("Bearer") is None
        assert parse_resource_metadata_url("Basic realm=test") is None
        assert parse_resource_metadata_url("") is None

    def test_client_id_rejects_unsafe_characters(self) -> None:
        """client_id with non-URL-safe characters raises ValueError."""
        with pytest.raises(ValueError, match="URL-safe"):
            OAuthResourceMetadata(
                resource="https://example.com/vgi",
                authorization_servers=("https://auth.example.com",),
                client_id='bad"id',
            )
        with pytest.raises(ValueError, match="URL-safe"):
            OAuthResourceMetadata(
                resource="https://example.com/vgi",
                authorization_servers=("https://auth.example.com",),
                client_id="has space",
            )

    def test_client_id_in_well_known_json(self) -> None:
        """client_id appears in well-known JSON when set."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA_WITH_CLIENT_ID)
        resp = client.get("/.well-known/oauth-protected-resource")
        body = json.loads(resp.content)
        assert body["client_id"] == "my-client-id"

    def test_client_id_in_www_authenticate(self) -> None:
        """client_id appears in WWW-Authenticate header when metadata has client_id."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            oauth_resource_metadata=_METADATA_WITH_CLIENT_ID,
        )
        resp = client.post(
            "/vgi/echo",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401
        www_auth = resp.headers.get("www-authenticate", "")
        assert 'client_id="my-client-id"' in www_auth

    def test_client_id_absent_from_www_authenticate(self) -> None:
        """client_id absent from WWW-Authenticate when metadata has no client_id."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            oauth_resource_metadata=_METADATA,
        )
        resp = client.post(
            "/vgi/echo",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401
        www_auth = resp.headers.get("www-authenticate", "")
        assert "client_id" not in www_auth

    def test_parse_client_id_extracts_value(self) -> None:
        """parse_client_id() extracts value from header."""
        header = (
            'Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource/vgi"'
            ', client_id="my-app"'
        )
        assert parse_client_id(header) == "my-app"

    def test_parse_client_id_returns_none_when_absent(self) -> None:
        """parse_client_id() returns None when not present."""
        assert parse_client_id("Bearer") is None
        assert parse_client_id('Bearer resource_metadata="https://example.com"') is None
        assert parse_client_id("") is None

    def test_client_discovery_round_trip_with_client_id(self) -> None:
        """Client discovers client_id set on server."""
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(server, signing_key=b"k", oauth_resource_metadata=_METADATA_WITH_CLIENT_ID)
        meta = http_oauth_metadata(client=client)
        assert meta is not None
        assert meta.client_id == "my-client-id"

    def test_401_discovery_flow(self) -> None:
        """Full 401-based discovery: get 401, parse header, fetch metadata."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_EchoService, _EchoImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            oauth_resource_metadata=_METADATA,
        )
        # Step 1: Make an unauthenticated request, get 401
        resp = client.post(
            "/vgi/echo",
            content=b"garbage",
            headers={"Content-Type": "application/octet-stream"},
        )
        assert resp.status_code == 401

        # Step 2: Parse the resource_metadata URL from WWW-Authenticate
        www_auth = resp.headers.get("www-authenticate", "")
        metadata_url = parse_resource_metadata_url(www_auth)
        assert metadata_url is not None

        # Step 3: Fetch metadata from that URL
        meta = fetch_oauth_metadata(metadata_url, client=client)
        assert meta.resource == "https://api.example.com/vgi"
        assert meta.authorization_servers == ("https://auth.example.com",)
        assert meta.scopes_supported == ("read", "write")


# ---------------------------------------------------------------------------
# TestJwtAuthenticate
# ---------------------------------------------------------------------------


class TestJwtAuthenticate:
    """Tests for jwt_authenticate factory (requires authlib)."""

    def test_valid_jwt_returns_auth_context(self) -> None:
        """A valid JWT returns an authenticated AuthContext."""
        priv, pub = _make_rsa_key()
        token = _mint_jwt(priv)
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req(headers={"Authorization": f"Bearer {token}"})
        auth = auth_fn(req)
        assert auth.authenticated is True
        assert auth.principal == "testuser"
        assert auth.domain == "jwt"

    def test_invalid_jwt_raises_value_error(self) -> None:
        """An invalid JWT raises ValueError."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req(headers={"Authorization": "Bearer invalid.token.here"})
        with pytest.raises(ValueError):
            auth_fn(req)

    def test_expired_jwt_raises_value_error(self) -> None:
        """An expired JWT raises ValueError."""
        priv, pub = _make_rsa_key()
        token = _mint_jwt(priv, exp_offset=-3600)
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req(headers={"Authorization": f"Bearer {token}"})
        with pytest.raises(ValueError):
            auth_fn(req)

    def test_wrong_audience_raises_value_error(self) -> None:
        """A JWT with wrong audience raises ValueError."""
        priv, pub = _make_rsa_key()
        token = _mint_jwt(priv, aud="https://wrong.example.com")
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req(headers={"Authorization": f"Bearer {token}"})
        with pytest.raises(ValueError):
            auth_fn(req)

    def test_wrong_issuer_raises_value_error(self) -> None:
        """A JWT with wrong issuer raises ValueError."""
        priv, pub = _make_rsa_key()
        token = _mint_jwt(priv, iss="https://wrong.example.com")
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req(headers={"Authorization": f"Bearer {token}"})
        with pytest.raises(ValueError):
            auth_fn(req)

    def test_missing_bearer_raises_value_error(self) -> None:
        """Missing Authorization header raises ValueError."""
        _priv, pub = _make_rsa_key()
        auth_fn = _make_local_auth(pub)
        req = falcon.testing.helpers.create_req()
        with pytest.raises(ValueError, match="Missing"):
            auth_fn(req)

    def test_custom_principal_claim(self) -> None:
        """Custom principal_claim extracts the right field."""
        priv, pub = _make_rsa_key()
        # Use "iss" as principal_claim for test simplicity
        auth_fn = _make_local_auth(pub, principal_claim="iss")
        token = _mint_jwt(priv)
        req = falcon.testing.helpers.create_req(headers={"Authorization": f"Bearer {token}"})
        auth = auth_fn(req)
        assert auth.principal == "https://auth.example.com"

    def test_end_to_end_rpc_with_jwt(self) -> None:
        """Full round-trip: JWT auth -> RPC call -> identity returned."""
        priv, pub = _make_rsa_key()
        token = _mint_jwt(priv, sub="alice")
        auth_fn = _make_local_auth(pub)
        server = RpcServer(_IdentityService, _IdentityImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            default_headers={"Authorization": f"Bearer {token}"},
            oauth_resource_metadata=_METADATA,
        )
        with http_connect(_IdentityService, client=client) as svc:
            result = svc.whoami()
            assert result == "alice"

    def test_jwt_authenticate_factory_creates_callable(self) -> None:
        """jwt_authenticate() returns a callable without errors."""
        auth_fn = jwt_authenticate(
            issuer="https://auth.example.com",
            audience="https://api.example.com/vgi",
            jwks_uri="https://auth.example.com/.well-known/jwks.json",
        )
        assert callable(auth_fn)
