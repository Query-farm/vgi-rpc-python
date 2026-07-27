# Mutual TLS (mTLS)

Client certificate authentication for vgi-rpc HTTP services behind TLS-terminating proxies.

## Quick Overview

vgi-rpc delegates TLS termination to reverse proxies and load balancers.
For mTLS, the proxy verifies client certificates and forwards certificate
information as HTTP headers. vgi-rpc provides authenticate callback factories
that extract identity from these headers and return `AuthContext`.

Two header conventions are supported:

| Convention | Proxies | Header | Extra deps |
|---|---|---|---|
| **PEM-in-header** | nginx, AWS ALB, Cloudflare | `X-SSL-Client-Cert` (configurable) | `vgi-rpc[mtls]` |
| **XFCC** | Envoy | `x-forwarded-client-cert` | None |

!!! danger "Header Spoofing Warning"

    The reverse proxy **MUST** strip client-supplied `X-SSL-Client-Cert` /
    `x-forwarded-client-cert` headers before forwarding. Failure to do so
    allows clients to forge certificate identity. These factories trust the
    header unconditionally — certificate chain validation is the proxy's
    responsibility.

### Basic setup (PEM-in-header)

```python
from vgi_rpc import AuthContext, RpcServer
from vgi_rpc.http import mtls_authenticate_subject, make_wsgi_app

auth = mtls_authenticate_subject(
    allowed_subjects=frozenset({"my-service", "other-service"}),
)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

### Basic setup (XFCC / Envoy)

```python
from vgi_rpc import RpcServer
from vgi_rpc.http import mtls_authenticate_xfcc, make_wsgi_app

auth = mtls_authenticate_xfcc()

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

## PEM-in-Header Factories

These factories parse URL-encoded PEM certificates from proxy headers.
Require `pip install vgi-rpc[mtls]` (cryptography).

### mtls_authenticate()

Generic factory with full control over certificate validation.
Mirrors the `bearer_authenticate` pattern.

```python
from cryptography import x509
from vgi_rpc import AuthContext
from vgi_rpc.http import mtls_authenticate, make_wsgi_app

def validate(cert: x509.Certificate) -> AuthContext:
    cn_attrs = cert.subject.get_attributes_for_oid(
        x509.oid.NameOID.COMMON_NAME
    )
    cn = str(cn_attrs[0].value) if cn_attrs else ""
    if cn not in ALLOWED_SERVICES:
        raise ValueError(f"Unknown client: {cn}")
    return AuthContext(
        domain="mtls",
        authenticated=True,
        principal=cn,
        claims={"serial": format(cert.serial_number, "x")},
    )

auth = mtls_authenticate(validate=validate)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `validate` | `Callable[[x509.Certificate], AuthContext]` | required | Receives parsed cert, returns `AuthContext` or raises `ValueError` |
| `header` | `str` | `"X-SSL-Client-Cert"` | Header containing URL-encoded PEM |
| `check_expiry` | `bool` | `False` | Verify `not_valid_before` / `not_valid_after` |

**Common header names by proxy:**

| Proxy | Header |
|---|---|
| nginx | `X-SSL-Client-Cert` (default) |
| AWS ALB | `X-Amzn-Mtls-Clientcert` |
| Cloudflare | `X-SSL-Client-Cert` |

### mtls_authenticate_fingerprint()

Convenience factory that looks up certificates by fingerprint.
Mirrors `bearer_authenticate_static`.

```python
from vgi_rpc import AuthContext
from vgi_rpc.http import mtls_authenticate_fingerprint, make_wsgi_app

# Fingerprints: lowercase hex, no colons
# Get with: openssl x509 -fingerprint -sha256 -noout -in cert.pem | tr -d ':'
fingerprints = {
    "a1b2c3d4e5f6...": AuthContext(
        domain="mtls", authenticated=True, principal="service-a",
    ),
    "f6e5d4c3b2a1...": AuthContext(
        domain="mtls", authenticated=True, principal="service-b",
        claims={"role": "admin"},
    ),
}

auth = mtls_authenticate_fingerprint(fingerprints=fingerprints)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fingerprints` | `Mapping[str, AuthContext]` | required | Lowercase hex fingerprint → `AuthContext` |
| `header` | `str` | `"X-SSL-Client-Cert"` | Header containing URL-encoded PEM |
| `algorithm` | `str` | `"sha256"` | Hash algorithm (`sha256`, `sha1`, `sha384`, `sha512`) |
| `domain` | `str` | `"mtls"` | Domain for `AuthContext` |
| `check_expiry` | `bool` | `False` | Verify certificate validity period |

!!! note "Fingerprint format"

    Fingerprints must be **lowercase hex without colons**. To get the SHA-256
    fingerprint from a PEM file:

    ```bash
    openssl x509 -fingerprint -sha256 -noout -in cert.pem \
      | sed 's/.*=//; s/://g' | tr '[:upper:]' '[:lower:]'
    ```

### mtls_authenticate_subject()

Convenience factory that extracts the Subject Common Name as the principal
and populates claims with certificate metadata.

```python
from vgi_rpc.http import mtls_authenticate_subject, make_wsgi_app

# Accept only specific client CNs
auth = mtls_authenticate_subject(
    allowed_subjects=frozenset({"frontend", "batch-worker", "admin-cli"}),
    check_expiry=True,
)

# Or accept any valid certificate (deliberate security decision)
auth_any = mtls_authenticate_subject()
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `header` | `str` | `"X-SSL-Client-Cert"` | Header containing URL-encoded PEM |
| `domain` | `str` | `"mtls"` | Domain for `AuthContext` |
| `allowed_subjects` | `frozenset[str] \| None` | `None` | Restrict to these CNs; `None` = accept any |
| `check_expiry` | `bool` | `False` | Verify certificate validity period |

The returned `AuthContext.claims` contains:

| Claim | Description |
|---|---|
| `subject_dn` | Full RFC 4514 Distinguished Name |
| `serial` | Certificate serial number (hex) |
| `not_valid_after` | Expiry timestamp (ISO 8601) |

## XFCC (Envoy)

The `x-forwarded-client-cert` (XFCC) header is Envoy's standard for
forwarding client certificate information. No `cryptography` dependency
is needed — these factories parse the structured text header directly.

### XfccElement

Frozen dataclass representing a single element from the XFCC header.

| Field | Type | Description |
|---|---|---|
| `hash` | `str \| None` | Certificate hash |
| `cert` | `str \| None` | URL-decoded PEM certificate (if present) |
| `subject` | `str \| None` | Certificate subject DN |
| `uri` | `str \| None` | SAN URI (e.g. SPIFFE ID) |
| `dns` | `tuple[str, ...]` | SAN DNS names |
| `by` | `str \| None` | Server certificate identity |

### mtls_authenticate_xfcc()

Factory that parses the `x-forwarded-client-cert` header and extracts
client identity.

```python
from vgi_rpc.http import mtls_authenticate_xfcc, make_wsgi_app

# Default: extract CN from Subject field
auth = mtls_authenticate_xfcc()

# Custom validation (e.g. SPIFFE ID)
from vgi_rpc import AuthContext
from vgi_rpc.http._mtls import XfccElement

def validate_spiffe(elem: XfccElement) -> AuthContext:
    if not elem.uri or not elem.uri.startswith("spiffe://"):
        raise ValueError("Missing SPIFFE ID")
    return AuthContext(
        domain="spiffe",
        authenticated=True,
        principal=elem.uri,
        claims={"hash": elem.hash} if elem.hash else {},
    )

auth = mtls_authenticate_xfcc(validate=validate_spiffe)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `validate` | `Callable[[XfccElement], AuthContext] \| None` | `None` | Custom validation; `None` uses Subject CN |
| `domain` | `str` | `"mtls"` | Domain for `AuthContext` (when using default extraction) |
| `select_element` | `"first" \| "last"` | `"first"` | Which element in multi-proxy chains |

**Element selection in multi-proxy chains:**

When requests traverse multiple Envoy proxies, the XFCC header contains
one element per hop. `select_element` controls which is used:

- `"first"` (default) — original client certificate
- `"last"` — nearest proxy's certificate

## Combining with Other Authenticators

Use `chain_authenticate` to accept mTLS **or** bearer tokens:

```python
from vgi_rpc.http import (
    bearer_authenticate_static,
    chain_authenticate,
    mtls_authenticate_subject,
    make_wsgi_app,
)
from vgi_rpc import AuthContext, RpcServer

# mTLS for service-to-service calls
mtls_auth = mtls_authenticate_subject(
    allowed_subjects=frozenset({"backend-svc"}),
)

# API keys for human/CI access
api_key_auth = bearer_authenticate_static(tokens={
    "sk-ci-bot": AuthContext(
        domain="apikey", authenticated=True, principal="ci-bot",
    ),
})

# Try mTLS first, fall back to API key
auth = chain_authenticate(mtls_auth, api_key_auth)

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=auth)
```

The chain tries each authenticator in order. `ValueError` (missing or
invalid credentials) falls through to the next; other exceptions propagate
immediately. See [chain_authenticate](oauth.md#chain_authenticate) for details.

## Proxy Configuration

### nginx

```nginx
server {
    listen 443 ssl;
    ssl_client_certificate /etc/nginx/ca.pem;
    ssl_verify_client on;

    location / {
        # CRITICAL: strip any client-supplied header first
        proxy_set_header X-SSL-Client-Cert $ssl_client_escaped_cert;
        proxy_pass http://upstream;
    }
}
```

### AWS ALB

AWS ALB automatically populates `X-Amzn-Mtls-Clientcert` when mTLS
is enabled. Use `header="X-Amzn-Mtls-Clientcert"` in the factory:

```python
auth = mtls_authenticate_subject(header="X-Amzn-Mtls-Clientcert")
```

### Envoy

```yaml
http_filters:
  - name: envoy.filters.http.set_metadata
    # Envoy automatically sets x-forwarded-client-cert
    # Use SANITIZE or SANITIZE_SET to strip client-supplied headers
forward_client_cert_details: SANITIZE_SET
set_current_client_cert_details:
  subject: true
  uri: true
  dns: true
  cert: true
```

Use `mtls_authenticate_xfcc()` (no header configuration needed).

## Installation

```bash
# XFCC support (no extra deps beyond [http])
pip install vgi-rpc[http]

# PEM-in-header support (adds cryptography)
pip install vgi-rpc[http,mtls]
```
