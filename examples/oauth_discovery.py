"""OAuth Discovery with JWT authentication.

Demonstrates:
1. Configuring OAuthResourceMetadata (RFC 9728)
2. Using jwt_authenticate() for JWT token validation
3. Client-side OAuth metadata discovery via http_oauth_metadata()
4. An RPC service that returns the authenticated user's identity

Requires ``pip install vgi-rpc[http,oauth]``

Run::

    python examples/oauth_discovery.py
"""

from __future__ import annotations

import time
from typing import Protocol

import falcon
from authlib.jose import JsonWebKey, jwt

from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import OAuthResourceMetadata, http_connect, http_oauth_metadata, jwt_authenticate, make_sync_client

# ---------------------------------------------------------------------------
# 1. Define the service Protocol
# ---------------------------------------------------------------------------


class IdentityService(Protocol):
    """A service that returns the caller's identity."""

    def whoami(self) -> str:
        """Return the caller's identity."""
        ...

    def my_claims(self) -> str:
        """Return the caller's JWT claims."""
        ...


# ---------------------------------------------------------------------------
# 2. Implement with CallContext
# ---------------------------------------------------------------------------


class IdentityServiceImpl:
    """Concrete implementation of IdentityService."""

    def whoami(self, ctx: CallContext) -> str:
        """Return the caller's identity."""
        ctx.auth.require_authenticated()
        return f"You are {ctx.auth.principal} (domain={ctx.auth.domain})"

    def my_claims(self, ctx: CallContext) -> str:
        """Return the caller's JWT claims."""
        ctx.auth.require_authenticated()
        claims = dict(ctx.auth.claims)
        return f"Claims for {ctx.auth.principal}: {claims}"


# ---------------------------------------------------------------------------
# 3. Configure OAuth metadata + JWT auth
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the OAuth discovery example end-to-end."""
    # Generate a test RSA key pair
    key = JsonWebKey.generate_key("RSA", 2048, is_private=True)
    key_dict = key.as_dict(is_private=True)
    key_dict["kid"] = "test-key-1"
    jwk_public = JsonWebKey.import_key(key_dict).as_dict()

    # Create a test JWT
    now = int(time.time())
    header = {"alg": "RS256", "kid": "test-key-1"}
    payload = {
        "iss": "https://auth.example.com",
        "aud": "https://api.example.com/vgi",
        "sub": "alice",
        "iat": now,
        "exp": now + 3600,
        "name": "Alice Example",
        "scope": "read write",
    }
    token_bytes: bytes = jwt.encode(header, payload, key_dict)
    token = token_bytes.decode()

    # Build a JWKS-based authenticate callback.
    # In production, jwt_authenticate() fetches keys from a real JWKS endpoint.
    # For this example, we create a local callback that uses the generated key.
    def _local_authenticate(req: falcon.Request) -> AuthContext:
        """Validate JWT using local key (simulates jwt_authenticate behaviour)."""
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")
        raw_token = auth_header[7:]
        claims = jwt.decode(
            raw_token,
            jwk_public,
            claims_options={
                "iss": {"essential": True, "value": "https://auth.example.com"},
                "aud": {"essential": True, "value": "https://api.example.com/vgi"},
            },
        )
        claims.validate()
        return AuthContext(
            domain="jwt",
            authenticated=True,
            principal=str(claims.get("sub", "")),
            claims=dict(claims),
        )

    metadata = OAuthResourceMetadata(
        resource="https://api.example.com/vgi",
        authorization_servers=("https://auth.example.com",),
        scopes_supported=("read", "write"),
        resource_name="Example Identity Service",
    )

    server = RpcServer(IdentityService, IdentityServiceImpl())

    client = make_sync_client(
        server,
        signing_key=b"example-oauth-key",
        authenticate=_local_authenticate,
        default_headers={"Authorization": f"Bearer {token}"},
        oauth_resource_metadata=metadata,
    )

    # --- 4. Client discovers metadata, then calls service ---
    meta = http_oauth_metadata(client=client)
    assert meta is not None
    print(f"authorization_servers: {meta.authorization_servers}")
    print(f"scopes_supported: {meta.scopes_supported}")
    print(f"resource_name: {meta.resource_name}")

    with http_connect(IdentityService, client=client) as svc:
        print(svc.whoami())
        print(svc.my_claims())

    # Verify jwt_authenticate is importable (requires authlib)
    _auth_fn = jwt_authenticate(
        issuer="https://auth.example.com",
        audience="https://api.example.com/vgi",
        jwks_uri="https://auth.example.com/.well-known/jwks.json",
    )
    print(f"jwt_authenticate factory created: {_auth_fn is not None}")


if __name__ == "__main__":
    main()
