# Cloud and reverse-proxy peer identity

VGI treats a load balancer or reverse proxy as an evidence adapter, not as an
authorization authority.  Every header adapter requires an exact list of
immediate proxy addresses, a backend that cannot be reached around that proxy,
and proxy configuration that replaces or strips client-supplied copies of all
identity headers.  The resulting assurance is `configured_proxy`.

These adapters produce provider `spiffe` and stable workload subjects only
when the evidence contains exactly one canonical SPIFFE ID in an allowed trust
domain.  Certificate names, fingerprints, source IPs, and load-balancer names
remain attributes rather than principals.

## HTTP ingress profiles

### nginx

Use `nginx_spiffe_provider`.  Configure nginx to verify the client chain, pass
`$ssl_client_escaped_cert` as `X-SSL-Client-Cert`, and pass
`$ssl_client_verify` as `X-SSL-Client-Verify`.  The adapter requires the exact
value `SUCCESS` and validates the forwarded leaf as an X.509-SVID.

### AWS Application Load Balancer

Use `aws_alb_spiffe_provider` only with an ALB listener in mTLS **verify** mode.
AWS forwards the URL-encoded leaf certificate in
`X-Amzn-Mtls-Clientcert-Leaf`; unlike nginx and the other profiles below, ALB
does not provide a per-request boolean verification header in verify mode.
The operator declaration that the listener is in verify mode is therefore part
of the trust boundary.  Passthrough mode is not accepted by this adapter.

AWS documents the two modes and their headers in [Mutual authentication with
TLS in Application Load
Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html).

### Google Cloud Application Load Balancer

Use `gcp_load_balancer_spiffe_provider`.  Configure frontend mTLS and map the
following official custom-header variables to the adapter's default names (or
configure matching names in the factory):

| GCP variable | Default header |
| --- | --- |
| `client_cert_present` | `X-Client-Cert-Present` |
| `client_cert_chain_verified` | `X-Client-Cert-Chain-Verified` |
| `client_cert_spiffe_id` | `X-Client-Cert-Spiffe-Id` |
| `client_cert_error` | `X-Client-Cert-Error` |

The adapter requires `present=true`, `chain_verified=true`, no validation
error, and one allowed canonical SPIFFE ID.  Google documents these variables
in [Create custom headers in backend
services](https://cloud.google.com/load-balancing/docs/https/custom-headers-global#mutual_tls_custom_headers).

### Azure Application Gateway

Use `azure_application_gateway_spiffe_provider` only with mTLS strict mode.
Configure rewrite rules that replace the default adapter headers with
`client_certificate` and `client_certificate_verification`.  The adapter
requires verification value `SUCCESS` and validates the forwarded certificate
as an X.509-SVID.  Azure documents the variables and strict-mode behavior in
[Rewrite HTTP headers and
URL](https://learn.microsoft.com/azure/application-gateway/rewrite-http-headers-url#mutual-authentication-server-variables).

Azure App Service's `X-ARR-ClientCert` is deliberately not treated as verified
evidence: App Service forwards the certificate but requires the application to
validate it.  A future direct-bundle adapter may do that cryptographic work;
until then, use Application Gateway strict mode or a trusted Envoy/nginx tier.

### Envoy and service mesh

Use `envoy_xfcc_spiffe_provider` with an adjacent Envoy that validates mTLS and
sets `forward_client_cert_details: SANITIZE_SET` in the HTTP connection manager,
using text format and `set_current_client_cert_details.uri: true`. Envoy then
removes a caller's XFCC value and creates one element for the authenticated
downstream certificate. The adapter requires exactly one URI, Envoy's SHA-256
`Hash`, an allowed SPIFFE trust domain, and one XFCC element; append/forward
chains, duplicate singleton fields, malformed quoting/escaping, and unknown
fields fail closed. Envoy's [HTTP connection manager
reference](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#enum-extensions-filters-network-http-connection-manager-v3-httpconnectionmanager-forwardclientcertdetails)
defines these modes.

The older `mtls_authenticate_xfcc` callback remains application authentication
and does not by itself create `PeerIdentity` evidence. Multi-hop meshes should
have the final adjacent Envoy reset XFCC from its verified downstream mTLS
connection; the worker never guesses which element in a forwarded chain is the
caller.

## TCP/L4 ingress

NLBs and cloud TCP load balancers provide connectivity and flow balancing, not
portable end-user or workload identity.  VGI can recover the asserted source
and destination from trusted PROXY v2, then run a provider such as Tailscale
LocalAPI against that source.  An asserted IP address alone never becomes a
principal.  TLS passthrough can instead let the worker verify a client SVID
directly; TLS termination at the load balancer needs a provider-specific
authenticated evidence channel.

Serializable HTTP state tokens are safe under ordinary request-level load
balancing because they are authenticated and identity-bound. Process-local
sticky sessions additionally require load-balancer affinity to their owning
worker (or an explicitly shared registry); identity binding is not a routing
mechanism. Stateful TCP and Iroh connections are balanced only when the
connection is established; all stream turns remain on that worker. Draining
must preserve sticky affinity and existing connections, and new connections
must target compatible replicas.

### Direct TLS and SPIFFE

Python raw TCP can terminate mutual TLS in the worker and snapshot the verified
client X.509-SVID for the connection:

```python
import ssl

from vgi_rpc.rpc import peer_identity_primary, serve_tcp

tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("worker.pem", "worker.key")
tls.load_verify_locations(cafile="example-org-bundle.pem")
tls.verify_mode = ssl.CERT_REQUIRED

serve_tcp(
    server,
    "0.0.0.0",
    9400,
    threaded=True,
    tls_context=tls,
    spiffe_trust_domains=("example.org",),
    peer_authentication_policy=peer_identity_primary("spiffe"),
)
```

The TLS stack verifies the chain first; VGI then enforces the leaf X.509-SVID
profile and allowed trust domain. The resulting evidence uses
`cryptographic_peer` assurance and is immutable for the connection. The client
`tcp_connect` accepts a client `tls_context` and can independently require a
server SVID with `server_spiffe_trust_domains`. One setup budget covers direct
TCP or SOCKS, TLS negotiation, and certificate/profile validation.

This API accepts already-issued SVID material through the platform TLS context;
automatic SPIFFE Workload API rotation is still an open release gate. An
application can rotate by replacing/draining listeners as its SVID source
updates the context. Because the verified SVID is snapshotted at the TLS
handshake, a long-lived connection can outlive the leaf certificate's
`NotAfter` or a trust-bundle rotation. Production must cap and drain connection
lifetimes at or before the declared SVID/revocation bound; Workload API hot
rotation alone affects only new handshakes.
