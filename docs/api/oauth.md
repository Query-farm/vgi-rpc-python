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
2. 401 responses include `WWW-Authenticate: Bearer resource_metadata="..."`
3. Client fetches metadata to discover authorization server(s)
4. Client authenticates with the AS and sends Bearer token

### Discovery from a 401

If a client doesn't know the server's auth requirements upfront, it can
discover them from a 401 response:

```python
from vgi_rpc.http import parse_resource_metadata_url, fetch_oauth_metadata

# 1. Make a request that returns 401
resp = client.post("/vgi/my_method", ...)

# 2. Parse the metadata URL from WWW-Authenticate header
www_auth = resp.headers["www-authenticate"]
metadata_url = parse_resource_metadata_url(www_auth)
# "https://api.example.com/.well-known/oauth-protected-resource/vgi"

# 3. Fetch the metadata
meta = fetch_oauth_metadata(metadata_url)
print(meta.authorization_servers)  # use these to authenticate
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

Raises `ValueError` if `resource` is empty or `authorization_servers` is empty.

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

## OAuthResourceMetadataResponse

Frozen dataclass returned by `http_oauth_metadata()` and `fetch_oauth_metadata()`.
Same fields as `OAuthResourceMetadata` (the server-side config class).

## Standards Compliance

- [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728) — OAuth 2.0 Protected Resource Metadata
- [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) — OAuth 2.0 Authorization Server Metadata
- [RFC 6750](https://www.rfc-editor.org/rfc/rfc6750) — Bearer Token Usage
- Compatible with MCP's OAuth implementation

## Installation

```bash
# OAuth discovery (no extra deps beyond [http])
pip install vgi-rpc[http]

# JWT authentication (adds Authlib)
pip install vgi-rpc[http,oauth]
```
