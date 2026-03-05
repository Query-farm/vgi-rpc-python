# OAuth Discovery

RFC 9728 Protected Resource Metadata and JWT authentication for vgi-rpc HTTP services.

## Quick Overview

vgi-rpc HTTP servers can advertise their OAuth configuration so clients
discover auth requirements automatically — no out-of-band configuration needed.

### Server setup

```python
from vgi_rpc.http import OAuthResourceMetadata, jwt_authenticate, make_wsgi_app
from vgi_rpc import RpcServer

metadata = OAuthResourceMetadata(
    resource="https://api.example.com/vgi",
    authorization_servers=("https://auth.example.com",),
    scopes_supported=("read", "write"),
)

auth = jwt_authenticate(
    issuer="https://auth.example.com",
    audience="https://api.example.com/vgi",
)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(
    server,
    authenticate=auth,
    oauth_resource_metadata=metadata,
)
```

### Client discovery

```python
from vgi_rpc.http import http_oauth_metadata, http_connect

meta = http_oauth_metadata("https://api.example.com")
print(meta.authorization_servers)  # ("https://auth.example.com",)

with http_connect(MyService, "https://api.example.com") as svc:
    result = svc.protected_method()
```

## How It Works

1. Server serves `/.well-known/oauth-protected-resource` (RFC 9728)
2. 401 responses include `WWW-Authenticate: Bearer resource_metadata="..."` (and optionally `client_id="..."`)
3. Client fetches metadata to discover authorization server(s)
4. Client authenticates with the AS and sends Bearer token

### Discovery from a 401

If a client doesn't know the server's auth requirements upfront, it can
discover them from a 401 response:

```python
from vgi_rpc.http import parse_resource_metadata_url, parse_client_id, fetch_oauth_metadata

# 1. Make a request that returns 401
resp = client.post("/vgi/my_method", ...)

# 2. Parse the metadata URL and optional client_id from WWW-Authenticate header
www_auth = resp.headers["www-authenticate"]
metadata_url = parse_resource_metadata_url(www_auth)
# "https://api.example.com/.well-known/oauth-protected-resource/vgi"
client_id = parse_client_id(www_auth)  # e.g. "my-app" or None

# 3. Fetch the metadata
meta = fetch_oauth_metadata(metadata_url)
print(meta.authorization_servers)  # use these to authenticate
print(meta.client_id)  # also available from the metadata document
```

## OAuthResourceMetadata

Frozen dataclass configuring the server's RFC 9728 metadata document.
Pass to `make_wsgi_app(oauth_resource_metadata=...)` to enable OAuth discovery.

| Field | Type | Required | Description |
|---|---|---|---|
| `resource` | `str` | Yes | Canonical URL of the protected resource |
| `authorization_servers` | `tuple[str, ...]` | Yes | Authorization server issuer URLs (must be non-empty) |
| `scopes_supported` | `tuple[str, ...]` | No | OAuth scopes the resource understands |
| `bearer_methods_supported` | `tuple[str, ...]` | No | Token delivery methods (default `("header",)`) |
| `resource_signing_alg_values_supported` | `tuple[str, ...]` | No | JWS algorithms for signed responses |
| `resource_name` | `str \| None` | No | Human-readable name |
| `resource_documentation` | `str \| None` | No | URL to developer docs |
| `resource_policy_uri` | `str \| None` | No | URL to privacy policy |
| `resource_tos_uri` | `str \| None` | No | URL to terms of service |
| `client_id` | `str \| None` | No | OAuth client_id for auth server *(custom extension, not in RFC 9728)* |

Raises `ValueError` if `resource` is empty or `authorization_servers` is empty.

## Bearer Token Authentication

For API keys, opaque tokens, or any non-JWT bearer token, use `bearer_authenticate`.
No extra dependencies beyond `vgi-rpc[http]`.

### bearer_authenticate()

Factory that creates a bearer-token `authenticate` callback with a custom `validate` function.
Supports any validation logic: database lookups, introspection endpoints, expiry checks, etc.

```python
from vgi_rpc.http import bearer_authenticate, make_wsgi_app
from vgi_rpc import AuthContext, RpcServer

def validate(token: str) -> AuthContext:
    # Look up token in database, call an introspection endpoint, etc.
    user = db.get_user_by_api_key(token)
    if user is None:
        raise ValueError("Invalid API key")
    return AuthContext(
        domain="apikey",
        authenticated=True,
        principal=user.name,
        claims={"role": user.role},
    )

auth = bearer_authenticate(validate=validate)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

| Parameter | Type | Description |
|---|---|---|
| `validate` | `Callable[[str], AuthContext]` | Receives the raw token, returns `AuthContext` on success, raises `ValueError` on failure |

### bearer_authenticate_static()

Convenience wrapper for a fixed set of known tokens. Useful for development,
testing, or services with a small number of pre-shared API keys.

```python
from vgi_rpc.http import bearer_authenticate_static, make_wsgi_app
from vgi_rpc import AuthContext, RpcServer

tokens = {
    "key-abc123": AuthContext(domain="apikey", authenticated=True, principal="alice"),
    "key-def456": AuthContext(domain="apikey", authenticated=True, principal="bob",
                              claims={"role": "admin"}),
}

auth = bearer_authenticate_static(tokens=tokens)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

| Parameter | Type | Description |
|---|---|---|
| `tokens` | `Mapping[str, AuthContext]` | Maps bearer token strings to pre-built `AuthContext` values |

## chain_authenticate()

Compose multiple `authenticate` callbacks into a single callback.
Authenticators are tried in order — `ValueError` (bad credentials) falls through
to the next; `PermissionError` or other exceptions propagate immediately.

This lets you accept **both** JWT and API key tokens on the same server:

```python
from vgi_rpc.http import (
    bearer_authenticate_static,
    chain_authenticate,
    jwt_authenticate,
    make_wsgi_app,
)
from vgi_rpc import AuthContext, RpcServer

# Accept JWTs from your identity provider
jwt_auth = jwt_authenticate(
    issuer="https://auth.example.com",
    audience="https://api.example.com/vgi",
)

# Also accept static API keys
api_key_auth = bearer_authenticate_static(tokens={
    "sk-service-account": AuthContext(
        domain="apikey", authenticated=True, principal="ci-bot",
    ),
})

# Try JWT first, fall back to API key lookup
auth = chain_authenticate(jwt_auth, api_key_auth)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

| Behaviour | Exception | Result |
|---|---|---|
| Credentials accepted | *(none)* | Returns `AuthContext`, stops chain |
| Bad / missing credentials | `ValueError` | Tries next authenticator |
| Authenticated but forbidden | `PermissionError` | Propagates immediately (401) |
| Bug in authenticator | Any other exception | Propagates immediately (500) |

Raises `ValueError` at construction time if called with no authenticators.

## jwt_authenticate()

Factory that creates a JWT-validating `authenticate` callback using Authlib.

```python
from vgi_rpc.http import jwt_authenticate

auth = jwt_authenticate(
    issuer="https://auth.example.com",
    audience="https://api.example.com/vgi",
    jwks_uri="https://auth.example.com/.well-known/jwks.json",  # optional
    principal_claim="sub",  # default
    domain="jwt",  # default
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `issuer` | `str` | required | Expected `iss` claim |
| `audience` | `str` | required | Expected `aud` claim |
| `jwks_uri` | `str \| None` | `None` | JWKS URL (discovered from OIDC if `None`) |
| `claims_options` | `Mapping \| None` | `None` | Additional Authlib claim options |
| `principal_claim` | `str` | `"sub"` | JWT claim for `AuthContext.principal` |
| `domain` | `str` | `"jwt"` | Domain for `AuthContext` |

**JWKS caching**: Keys are fetched lazily on first request and cached in-process.
On unknown `kid` (decode error), keys are automatically refreshed once. Other
validation failures (expired token, wrong issuer/audience) fail immediately
without a network round-trip.

Requires `pip install vgi-rpc[oauth]` — raises a clear `ImportError` with
install instructions if Authlib is not available.

## http_oauth_metadata()

Client-side function to discover a server's OAuth configuration.

```python
from vgi_rpc.http import http_oauth_metadata

meta = http_oauth_metadata("https://api.example.com")
if meta is not None:
    print(meta.authorization_servers)
```

Returns `OAuthResourceMetadataResponse` or `None` (if server returns 404).

**Note:** The `prefix` parameter (default `"/vgi"`) must match the server's
`make_wsgi_app(prefix=...)`. A mismatch results in a 404 (`None` return).

## fetch_oauth_metadata()

Fetch metadata from an explicit URL (typically from a 401 `WWW-Authenticate` header).

```python
from vgi_rpc.http import fetch_oauth_metadata

meta = fetch_oauth_metadata("https://api.example.com/.well-known/oauth-protected-resource/vgi")
```

## parse_resource_metadata_url()

Extract the `resource_metadata` URL from a `WWW-Authenticate` header.

```python
from vgi_rpc.http import parse_resource_metadata_url

url = parse_resource_metadata_url('Bearer resource_metadata="https://..."')
# "https://..."
```

Returns `None` if the header doesn't contain `resource_metadata`.

## parse_client_id()

Extract the `client_id` from a `WWW-Authenticate` header. Custom extension (not in RFC 9728).

```python
from vgi_rpc.http import parse_client_id

client_id = parse_client_id('Bearer resource_metadata="https://...", client_id="my-app"')
# "my-app"
```

Returns `None` if the header doesn't contain `client_id`.

## OAuthResourceMetadataResponse

Frozen dataclass returned by `http_oauth_metadata()` and `fetch_oauth_metadata()`.
Same fields as `OAuthResourceMetadata` (the server-side config class), including `client_id`.

## Standards Compliance

- [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728) — OAuth 2.0 Protected Resource Metadata
- [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) — OAuth 2.0 Authorization Server Metadata
- [RFC 6750](https://www.rfc-editor.org/rfc/rfc6750) — Bearer Token Usage
- Compatible with MCP's OAuth implementation
- **Custom extension**: `client_id` field on `OAuthResourceMetadata` / `OAuthResourceMetadataResponse` and in `WWW-Authenticate` headers is not defined in RFC 9728

## Installation

```bash
# OAuth discovery (no extra deps beyond [http])
pip install vgi-rpc[http]

# JWT authentication (adds Authlib)
pip install vgi-rpc[http,oauth]
```
