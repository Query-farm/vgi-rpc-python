"""HTTP authentication with Bearer tokens and guarded methods.

Demonstrates how to wire up an ``authenticate`` callback, inject
``CallContext`` into method implementations, and gate access with
``require_authenticated()``.  Uses ``make_sync_client`` so no real
HTTP server is needed.

Requires ``pip install vgi-rpc[http]``

Run::

    python examples/auth.py
"""

from __future__ import annotations

from typing import Protocol

import falcon

from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import http_connect, make_sync_client

# ---------------------------------------------------------------------------
# 1. Define an authenticate callback
# ---------------------------------------------------------------------------


def authenticate(req: falcon.Request) -> AuthContext:
    """Extract a Bearer token from the Authorization header.

    Returns an authenticated ``AuthContext`` with the token value as
    principal and a hard-coded ``role`` claim for demonstration.

    Raises:
        ValueError: If the header is missing or malformed.

    """
    auth_header = req.get_header("Authorization") or ""
    if not auth_header.startswith("Bearer "):
        raise ValueError("Missing or invalid Authorization header")
    principal = auth_header.removeprefix("Bearer ")
    return AuthContext(domain="bearer", authenticated=True, principal=principal, claims={"role": "admin"})


# ---------------------------------------------------------------------------
# 2. Define the service Protocol
# ---------------------------------------------------------------------------


class SecureService(Protocol):
    """A service with public and guarded methods."""

    def status(self) -> str:
        """Public health-check endpoint."""
        ...

    def whoami(self) -> str:
        """Return the caller's identity."""
        ...

    def secret_data(self) -> str:
        """Return secret data (requires authentication)."""
        ...


# ---------------------------------------------------------------------------
# 3. Implement the service
# ---------------------------------------------------------------------------


class SecureServiceImpl:
    """Concrete implementation of SecureService."""

    def status(self) -> str:
        """Public health-check endpoint."""
        return "ok"

    def whoami(self, ctx: CallContext) -> str:
        """Return the caller's identity."""
        return ctx.auth.principal or "anonymous"

    def secret_data(self, ctx: CallContext) -> str:
        """Return secret data (requires authentication)."""
        ctx.auth.require_authenticated()
        role = ctx.auth.claims.get("role", "unknown")
        return f"secret for {ctx.auth.principal} (role={role})"


# ---------------------------------------------------------------------------
# 4. Run the example
# ---------------------------------------------------------------------------


def main() -> None:
    """Create an authenticated HTTP client and call all three methods."""
    server = RpcServer(SecureService, SecureServiceImpl())

    client = make_sync_client(
        server,
        signing_key=b"example-signing-key",
        authenticate=authenticate,
        default_headers={"Authorization": "Bearer alice"},
    )

    with http_connect(SecureService, client=client) as svc:
        print(f"status (public):  {svc.status()}")
        print(f"whoami:           {svc.whoami()}")
        print(f"secret_data:      {svc.secret_data()}")


if __name__ == "__main__":
    main()
