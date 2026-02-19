# Auth & Context

Authentication and request-scoped context for RPC methods.

## Quick Overview

Server methods can accept an optional `ctx: CallContext` parameter. The framework injects it automatically — it does **not** appear in the Protocol definition:

```python
from vgi_rpc import CallContext


class MyServiceImpl:
    def public_method(self) -> str:
        """No ctx — accessible to all callers."""
        return "ok"

    def protected_method(self, ctx: CallContext) -> str:
        """Require authentication, then return caller identity."""
        ctx.auth.require_authenticated()
        return f"Hello, {ctx.auth.principal}"
```

### HTTP authentication

Pass an `authenticate` callback to `make_wsgi_app`:

```python
import falcon

from vgi_rpc import AuthContext, RpcServer, make_wsgi_app


def authenticate(req: falcon.Request) -> AuthContext:
    token = req.get_header("Authorization") or ""
    if not token.startswith("Bearer "):
        raise ValueError("Missing Bearer token")
    # ... validate token ...
    return AuthContext(domain="jwt", authenticated=True, principal="alice")


server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=authenticate)
```

Over pipe/subprocess transport, `ctx.auth` is always `AuthContext.anonymous()`.

### Transport metadata

`ctx.transport_metadata` provides transport-level information like `remote_addr` and `user_agent` (HTTP only). It's a read-only mapping populated by the transport layer.

## API Reference

### AuthContext

::: vgi_rpc.rpc.AuthContext

### CallContext

::: vgi_rpc.rpc.CallContext

### CallStatistics

::: vgi_rpc.rpc.CallStatistics

### ClientLog

::: vgi_rpc.rpc.ClientLog
