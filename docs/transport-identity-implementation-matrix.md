# Transport Identity Implementation Matrix

Status: working implementation ledger for the experimental branch. This file
records tested code, not release promises. A feature is not portable until the
shared conformance fixture and every applicable active SDK gate are green.

## Contract layers

| Layer | Responsibility | Wire change |
| --- | --- | --- |
| Dialer | Direct TCP, MagicDNS, or explicit SOCKS5h; never silently fall back | None |
| Ingress | HTTP/L4 balancing, TLS termination or passthrough, PROXY v2 | None |
| Evidence adapter | Snapshot Tailscale, SPIFFE, Iroh, or trusted-proxy evidence | None |
| Authentication policy | Observe, require, primary, any-of, or all-of | None |
| Worker context | Immutable evidence and resulting `AuthContext` for every call | None |

HTTP evidence is resolved per request. TCP and Iroh evidence is resolved once
per accepted connection and reused for every unary, stream, cancellation, and
resume turn on that connection. An ingress address or PROXY source is routing
evidence only; it never becomes a principal without an identity authority.

## Current branch status

Legend: **done** means implemented with focused and full repository gates on
the EC2 validation host; **focused** means the feature's tests pass but its
repository-wide gate is still pending or blocked; **building** means active
implementation; an em dash means no implementation yet.

| Capability | Python | Go | Rust | TypeScript | Java | C# | C++ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Provider-neutral evidence, policies, context, binding | done | done | done | done | done | done | done |
| Tailscale Serve / LocalAPI | done | done | done | provider only; HTTP socket adapter missing | done | done | done |
| Native LocalAPI transport | Unix/HTTP only | Unix + macOS + Windows | Unix/HTTP only | Unix/HTTP only | Unix/HTTP only | Unix + Windows + HTTP | Unix/HTTP only |
| Raw TCP connection snapshot + PROXY v2 | done | done | done | — | — | — | done |
| Trusted Envoy/nginx/AWS/GCP/Azure HTTP SPIFFE evidence | done | done | done | done | done | done | library only; host integration required |
| Direct TLS X.509-SVID verification | done (manual SVID rotation) | done (server) | done (server) | — | — | — | — |
| Explicit SOCKS5h client dialing | done | done | done | done | done (raw only) | done | done |
| Real Tailnet data-plane/identity gate | done (Linux reference, opt-in) | — | — | — | — | — | — |
| Optional Iroh stateful transport | N/A | N/A | done | N/A | N/A | N/A | N/A |

High-level `vgi-python` consumes the Python SOCKS5h option. DuckDB VGI consumes
`TCP_PROXY` internally; the core SOCKS transport sources compile, while the
repository's full extension build remains blocked by existing Haybarn API
conversion errors in unrelated worker-pool, result-cache, table-function, and
catalog/secret code.

## Release gates still open

- Complete the missing SDK cells or explicitly reduce the supported platform
  matrix before an experimental release.
- Add built-in SPIFFE Workload API rotation and the missing direct-TLS client
  surfaces. Python, Go, and Rust servers verify direct X.509-SVID peers, while
  trusted forwarded headers remain `configured_proxy`, not
  `cryptographic_peer` assurance.
- Add maximum connection age/drain controls for stateful TCP and Iroh so SVID
  expiry, Tailnet grant/tag/capability changes, allowlist changes, and policy
  revocation have a declared upper bound. Reconnect tests must prove refreshed
  evidence is observed.
- Extend the shared adapter vectors beyond the current SPIFFE ID, Envoy XFCC,
  GCP, and Tailscale Serve cases to cover all duplicate-header, downgrade,
  timeout, PROXY truncation, SOCKS, and stream-snapshot adversarial cases.
- Extend `vgi mesh doctor` beyond its TCP/VGI/SOCKS/LocalAPI checks. The
  opt-in Python reference suite now covers real MagicDNS TCP, userspace
  SOCKS5h, Tailscale Serve, user/tagged-node evidence, app capabilities, and
  Tailscale Services with PROXY v2; native client/server replacement gates
  remain open for Go, Rust, TypeScript, Java, C#, and C++. Deployment profiles
  and Python's redacted identity outcome/source access logs and OpenTelemetry
  labels are present; equivalent identity metrics remain coordinated gates.
- Validate Windows named pipes and both supported macOS Tailscale variants on
  real runners. Cross-compilation proves API/build coverage, not runtime
  behavior.
- Add the runtime socket adapter needed for TypeScript HTTP LocalAPI WhoIs;
  Fetch alone cannot expose the remote `IP:port`. Complete Java HTTP-over-SOCKS
  and its native Windows/macOS LocalAPI transports, or narrow the advertised
  platform matrix.
- Design provider-specific authorization-binding projections before optimizing
  long-lived state across observational attribute churn. The conservative
  built-in `require`/`all_of` binding currently includes all attributes, so a
  display-name, node-name, or certificate-fingerprint change safely forces a
  state reopen even when the stable principal is unchanged.
- Add consistent raw-TCP admission and lifecycle controls before calling the
  stateful profile production-safe: global pending/active connection caps,
  setup/first-frame and per-connection idle deadlines, graceful drain, and
  bounded response writes/backpressure, with optional per-principal quotas.
  Python's default sequential listener can be blocked by one silent client;
  its threaded listener is unlimited unless `max_connections` is configured.
  Current Go/Rust defaults can admit unbounded silent or stopped-reader
  connections. C++ now implements baseline pending/active caps plus bounded
  setup, read-idle, and response-write deadlines, but has no per-principal
  quotas and a synchronous identity callback can occupy one bounded worker
  until it returns. An L4 load balancer distributes these risks but does not
  remove them from each backend.
- Run an independent security review after the matrix is complete and apply
  every accepted correction before declaring the feature stable.

## Required invariants

1. Invalid application credentials never downgrade to transport identity.
2. Exact immediate-proxy trust is checked before consuming asserted headers or
   a PROXY preamble, and the backend is unreachable around that proxy.
3. Login names, tags, node names, IP addresses, and load-balancer names are
   attributes, not stable principals.
4. Stateful tokens, caches, cursors, and resumable streams bind the complete
   authorization evidence.
5. Network lookup and handshake stages have one monotonic budget, bounded
   input, bounded concurrency, cancellation where the host API permits it, and
   no direct fallback.
6. Raw capabilities, local daemon tokens, certificate bodies, profile fields,
   and proxy credentials never appear in normal logs or metrics.
