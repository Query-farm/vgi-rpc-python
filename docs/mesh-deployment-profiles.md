# VGI Mesh Deployment Profiles

These profiles keep connectivity, identity evidence, and authorization as
separate layers. VGI does not manage Tailnet membership, cloud IAM, SPIFFE
bundles, load-balancer lifecycle, or provider policy.

## Direct TCP over a Tailnet

- Dial `tcp://worker.example-tailnet.ts.net:9400` with ordinary DNS/TCP on a
  TUN host.
- The worker snapshots LocalAPI WhoIs evidence from the accepted source before
  dispatch and applies an explicit authentication policy.
- Use a stable numeric Tailscale user ID or stable tagged-node ID as the
  principal. Login names, node names, and tags remain attributes.
- Treat subnet-routed/SNAT traffic as the router identity unless an independent
  authenticated channel preserves the original caller.

This profile is connection-stateful. A load balancer selects a worker when the
connection opens; every stream turn remains on that worker.

## Tailscale Service or PROXY-v2 L4 load balancer

- Put the worker on a backend address reachable only from the selected proxy.
- Require PROXY protocol v2 and trust exact immediate proxy addresses.
- Validate proxy trust before reading the bounded preamble; accept only TCP
  over IPv4/IPv6.
- Use the asserted address as WhoIs input and record the configured `svc:` name
  or asserted destination as the capability target.

This profile applies to Tailscale Services, Envoy/nginx configured to emit
PROXY v2, and [AWS Network Load Balancer with its target-group PROXY-v2
attribute enabled](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#proxy-protocol).
PROXY source addresses are routing evidence, not principals by themselves.
Verify each product's actual v2 and TLV behavior before enabling the listener;
VGI does not infer it from the cloud vendor name.

A Tailscale Service does not add PROXY metadata automatically. Configure Serve
explicitly with PROXY v2, for example
`tailscale serve --service=svc:vgi --proxy-protocol=2 --tcp=9400 tcp://127.0.0.1:9400`,
and validate the current CLI syntax in the real-Tailnet release gate. Without
that option the worker sees the local forwarder, so LocalAPI WhoIs cannot
recover the originating peer. See the
[Serve CLI reference](https://tailscale.com/docs/reference/tailscale-cli/serve).

For nginx stream proxying, require nginx 1.31.4 or later and configure
`proxy_protocol v2;`. Earlier nginx versions emit v1 when `proxy_protocol on;`
is used, which VGI rejects. Use Envoy or another explicit v1-to-v2 bridge when
that version is unavailable. See the
[nginx stream proxy directive](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html).

It does **not** directly apply to Google Cloud proxy Network Load Balancers:
Google's proxy NLB supports PROXY protocol v1, while VGI deliberately accepts
v2 only. Put an isolated Envoy/nginx v1-to-v2 bridge in front of VGI or treat
origin-address metadata as unavailable; never disable VGI's required PROXY-v2
mode behind that proxy and assume the backend socket peer is the caller. See
[Google's proxy NLB configuration](https://docs.cloud.google.com/load-balancing/docs/tcp/setup-cross-reg-proxy-migs#proxy-protocol).

## Source-preserving L4 load balancer

- Use `proxy_protocol="off"` when the selected product and topology preserve
  the client source IP on the backend socket.
- This is the normal profile for Google Cloud passthrough Network Load
  Balancers and Azure Load Balancer; AWS NLB can also use it only when client
  IP preservation is enabled and its documented topology constraints hold.
- Apply exact network/firewall boundaries and verify source preservation in
  the real deployment, including IPv4/IPv6 conversion, PrivateLink, peering,
  and cross-region paths.

The preserved socket source is still routing evidence, never a stable user or
workload principal. An identity authority such as Tailscale LocalAPI may use
that address to resolve verified identity, but VGI authorization must not
promote a bare cloud source IP into a principal. See the provider distinctions
in [Google's load-balancer selection guide](https://docs.cloud.google.com/load-balancing/docs/choosing-load-balancer)
and [Azure Load Balancer's five-tuple flow model](https://learn.microsoft.com/en-us/azure/load-balancer/distribution-mode-concepts).

## HTTP through Tailscale Serve

- Bind the VGI HTTP worker to loopback or another isolated backend.
- Trust Tailscale identity and capability headers only from exact Serve proxy
  peers.
- Funnel requests never produce Tailnet identity.
- A Serve login is a verified login-scoped subject, not a stable numeric user
  principal. Capability-only requests remain subjectless and cannot use the
  built-in primary authenticator.

Stateless unary calls and serializable cursor/state-token calls can be balanced
per request. Sticky sessions are different: their token names process-local
state, so ingress must maintain affinity to the owning worker (or the
deployment must provide a shared registry) and preserve that affinity while
draining. Identity binding prevents cross-caller replay; it does not route a
sticky request to its owner.

## Envoy or nginx HTTP ingress

- Terminate and verify client mTLS at the adjacent proxy.
- Prevent direct access to the worker and replace every client-supplied
  identity header.
- For Envoy, use `SANITIZE_SET` XFCC with URI details and exactly one adjacent
  mTLS hop. Forwarded/append chains are rejected.
- For nginx, forward the escaped leaf certificate and require
  `$ssl_client_verify` to be `SUCCESS`.

Python WSGI deployments must leave `REMOTE_ADDR` as the physical socket peer
until VGI identity resolution has run. Do not place Werkzeug `ProxyFix` or an
equivalent forwarded-address rewrite ahead of the VGI identity middleware;
WSGI cannot recover the original peer afterward. The asserted client address
belongs in the provider-specific trusted header/evidence path, not
`REMOTE_ADDR`.

The resulting assurance is `configured_proxy`. It is not equivalent to a
certificate verified directly by the worker.

## Cloud HTTP load balancers

- AWS ALB: mTLS verify mode only; the configured listener/trust store and
  isolated backend form the trust boundary because there is no separate
  per-request verification boolean.
- Google Cloud Application Load Balancer: require certificate-present and
  chain-verified signals, no validation error, and one allowed SPIFFE ID.
- Azure Application Gateway: strict mTLS plus rewrite rules that replace the
  certificate and verification headers. Azure App Service `X-ARR-ClientCert`
  alone is not verified identity evidence.

Cloud L7 products balance HTTP requests, not VGI raw stream turns. Use HTTP
tokens/shared state, or use an L4/stateful profile when keeping Arrow batches
on one connection is the objective.

Managed cloud L7 backend addresses may rotate. If their exact immediate peer
IPs cannot be pinned safely, terminate the cloud hop at a stable adjacent
Envoy/nginx/sidecar and trust only that exact hop. Do not replace the exact-IP
boundary with a broad subnet merely for convenience. A future operator-supplied
trusted-peer predicate would need platform network identity plus backend
isolation and must advertise its lower, configured-boundary assurance; VGI
does not automate cloud control-plane allowlists.

## Userspace Tailscale sidecar

- Configure the client explicitly with `socks5h://IP:PORT`.
- Resolve the worker hostname at the proxy, offer SOCKS5 `NO AUTH` only, apply
  one setup deadline, and never fall back to direct TCP.
- Keep the sidecar listener private to the application namespace. SOCKS NO
  AUTH is safe only when local access to that listener is already controlled.

## Kubernetes

- Use a Tailscale Operator/ProxyGroup, Envoy sidecar/gateway, nginx ingress, or
  cloud load balancer as the connectivity layer.
- Keep the worker backend reachable only through the selected identity
  boundary and use NetworkPolicy/security groups as defense in depth.
- Use versioned service names and terminate readiness before draining. Stop new
  connections first, allow existing TCP/Iroh streams to finish, then remove
  the replica.

Replicas behind one stateful service must be protocol-compatible and avoid
process-local state that a later connection needs. Connection-time attach
checks cannot prove which replica handles a future connection; replica
fingerprinting is therefore not part of the initial design.

## Iroh

Iroh is a direct stateful transport adapter. The worker snapshots the
cryptographic endpoint identity for the connection and exposes it through the
same provider-neutral evidence/authentication contract. Discovery, relay/NAT
traversal, and endpoint dialing belong to Iroh; VGI owns Arrow framing and
worker authorization.

There is no transparent request-level Iroh load-balancer contract. Scale with
multiple advertised endpoints plus client-side endpoint selection, or place an
application-aware gateway in front and accept that it is a proxy. Draining is
connection-oriented just like raw TCP.

The Iroh adapter enforces a global connection cap, an absolute first-request
budget that partial reads cannot extend, and per-operation I/O timeouts. It
also offers an optional per-cryptographic-endpoint active-connection cap;
production deployments should set that cap according to their tenant and
endpoint model rather than relying only on the global default.

All stateful transports snapshot identity and capabilities at connection
setup. Tailnet grants/tags, SPIFFE SVID expiry or bundle rotation, endpoint
allowlists, and authorization-policy changes do not rewrite an existing
`AuthContext`. Production deployments must declare a revocation bound by
enforcing a maximum connection age and draining/reconnecting within it, or use
per-call reauthorization when immediate revocation is required. Audit records
must retain the snapshot used for each call, and release tests must prove that
a reconnect observes changed evidence.

## Rollout checklist

1. Name the identity issuer and accepted assurance level.
2. Prove that the worker cannot be reached around the trusted proxy.
3. Decide whether balancing is per request or per connection.
4. Configure fail-closed authentication composition; test invalid-credential
   downgrade and conflicting-identity cases.
5. Verify redaction, bounded timeouts/concurrency, graceful drain, and
   compatibility across every replica.
6. Run `vgi-rpc mesh doctor` from the real client and worker environment, then
   run the opt-in real Tailnet/cloud integration suite before production.
