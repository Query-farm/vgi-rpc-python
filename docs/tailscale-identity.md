# Tailscale peer identity

VGI treats Tailscale as an evidence provider, not as an authorization policy.
The worker receives immutable `CallContext.peer_evidence`; a configured peer
authentication policy decides whether that evidence is observational, required,
or combined with application credentials.

## Tailscale Serve

```python
from vgi_rpc.http import make_wsgi_app, tailscale_serve_header_provider
from vgi_rpc.rpc import require_peer_identity

serve_identity = tailscale_serve_header_provider(
    issuer="tailnet:example.com",
    trusted_proxy_addresses={"127.0.0.1", "::1"},
)

app = make_wsgi_app(
    server,
    peer_identity_providers=(serve_identity,),
    peer_authentication_policy=require_peer_identity("tailscale"),
)
```

The backend must only be reachable through the exact trusted proxy addresses.
Serve user headers produce a verified login subject with `stability="login"`;
they are intentionally ineligible for the built-in stable-subject primary
authenticator. Application capabilities are verified opaque JSON. A tagged node
can therefore produce capability-only, subjectless evidence. Funnel requests
produce no Tailscale identity.

The parser accepts only plain ASCII or strict RFC 2047 UTF-8 Q encoding, rejects
duplicate headers, controls, duplicate JSON keys, malformed capability shapes,
and bounded-size/depth violations. See Tailscale's official
[Serve identity-header behavior](https://tailscale.com/docs/features/tailscale-serve#identity-headers)
and [application capabilities](https://tailscale.com/docs/features/access-control/grants/grants-app-capabilities).

## LocalAPI WhoIs

Unix socket:

```python
from vgi_rpc.http import tailscale_localapi_provider
from vgi_rpc.rpc import peer_identity_primary

local_identity = tailscale_localapi_provider(
    issuer="tailnet:example.com",
    unix_socket="/var/run/tailscale/tailscaled.sock",
)

app = make_wsgi_app(
    server,
    peer_identity_providers=(local_identity,),
    peer_authentication_policy=peer_identity_primary("tailscale"),
    peer_service_name="svc:analytics",
)
```

Configurable local HTTP endpoint, including the macOS same-user-proof password:

```python
local_identity = tailscale_localapi_provider(
    issuer="tailnet:example.com",
    endpoint="http://127.0.0.1:49152",
    password=localapi_password,
)
```

WhoIs is queried once per request with no CLI subprocess and no cache. The
provider sends the official `Host: local-tailscaled.sock` and honors the
request's total monotonic deadline and response-size limit. Python's WSGI
adapter does not expose the listener's local socket address, so HTTP capability
lookup is service-scoped only when `peer_service_name` is configured; otherwise
it is node-scoped. Raw TCP supplies the actual or PROXY-asserted destination and
can use `dst_ip`. Untagged nodes use `user:<numeric UserProfile.ID>`
as their stable subject. Tagged nodes ignore `UserProfile` as caller identity and
use `node:<StableNodeID>`; names and tags remain attributes.

Outcomes remain distinct: unavailable daemon or timeout, permission denied,
WhoIs no-match, invalid response, and available evidence. Tailscale's
[LocalAPI WhoIs implementation](https://github.com/tailscale/tailscale/blob/main/ipn/localapi/localapi.go)
is the normative request/status behavior.

Both adapters are disabled unless explicitly configured. VGI does not invoke the
Tailscale CLI, manage tailnet membership, distribute auth keys, or cache WhoIs.

## Raw TCP and Tailscale Services

Stateful raw workers resolve identity once per accepted connection. Direct
Tailnet TCP can query the immediate source; a worker behind a Tailscale Service
must require PROXY v2 and trust only the exact local proxy address:

```python
from vgi_rpc.rpc import peer_identity_primary, serve_tcp

serve_tcp(
    server,
    "127.0.0.1",
    9400,
    threaded=True,
    proxy_protocol="required",
    trusted_proxy_addresses=("127.0.0.1",),
    service_name="svc:analytics",
    peer_identity_providers=(local_identity,),
    peer_authentication_policy=peer_identity_primary("tailscale"),
)
```

The listener checks the immediate socket peer before reading anything, accepts
PROXY v2 TCP/IPv4 or TCP/IPv6 only, rejects `LOCAL`, UDP, UNSPEC, truncation,
malformed TLVs, and oversized preambles, and uses an independent monotonic
preamble timeout. It consumes exactly the preamble so the following Arrow/VGI
bytes remain untouched. The resulting authentication and evidence snapshots
are fixed for every unary call, stream turn, and cancellation on that
connection.
