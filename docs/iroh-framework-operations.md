# Iroh framework operations

VGI frameworks expose the same two deployment shapes:

- `iroh://<endpoint-id>` carries stateful `vgi-rpc/arrow-mux/1` streams.
- `httpi://<endpoint-id>[/prefix]` carries stateless HTTP semantics over
  `iroh-http/2`, including OPTIONS limits, continuations, externalized batches,
  ordinary HTTP authentication, and request-level load balancing behind the
  bridge's fixed HTTP origin.

Rust workers may embed the native Iroh server. Every language can instead use
the narrow `vgi-iroh-bridge`; that is the normal cross-language production
shape. The worker listens only on loopback, the bridge strips any incoming
identity assertion, authenticates the Iroh peer, and forwards its EndpointId.
Raw upstreams use the dedicated PROXY-v2 `UNSPEC` identity TLV. HTTP upstreams
use `VGI-Forwarded-Iroh-Endpoint`.

## Start a framework worker

The framework CLIs share these flags:

```console
worker-command \
  --iroh-raw-upstream 127.0.0.1:9400 \
  --iroh-issuer production-mesh
```

Python (`vgi-serve`), Rust (`Worker::run`), TypeScript (`Worker.run`), C++
(`Worker::run`), C# (`Worker.RunFromArgsAsync`), Go's `workercli`, and the
Kotlin/Java-facing `runVgiWorkerCli` accept that spelling. The corresponding
HTTP workers use their ordinary `--http` switch plus `--iroh-issuer`.
Python's raw Iroh listener advertises the standard RPC method description,
allowing standalone clients in another language to bootstrap.

The exact trusted bridge address defaults to `127.0.0.1` only after Iroh mode
is enabled. Repeat `--iroh-trusted-proxy <IP>` (the Go/Kotlin CLIs also accept
comma-separated values) for another colocated address. `--iroh-observe`
exposes evidence without promoting the EndpointId to the application
principal. Production authorization should normally keep the default primary
authentication and then authorize `iroh:<issuer>:<endpoint-id>` in application
policy.

Framework APIs expose the same configuration without argv:

| Framework | Raw bridge upstream | HTTP bridge upstream |
| --- | --- | --- |
| Python | `vgi.serve` CLI / `serve_tcp` options | `create_app(..., iroh_bridge_issuer=...)` |
| Rust | `serve_iroh_tcp_upstream` | `serve_http_behind_iroh` |
| TypeScript | `Worker.run` flags | `irohBridge` on `serveVgiWorker` / `createVgiWorkerFetch` |
| Go | `Worker.RunIrohTcpUpstream` | `Worker.SetIrohBridge` then `RunHttp` |
| Java/Kotlin | `VgiWorker.serveIrohTcpUpstream` | `VgiWorker.serveHttpBehindIroh` |
| C# | `Worker.RunIrohTcpUpstreamAsync` | `Worker.RunHttpAsync(..., irohBridge: ...)` |
| C++ | `Worker::run` flags | `--http --iroh-issuer ...` |

TypeScript's Bun `serveVgiWorker` adapter supplies the physical peer and binds
loopback automatically. Portable `createVgiWorkerFetch` hosts must supply
`irohBridge.peerResolutionContext`: a bare Fetch `Request` does not expose the
physical socket peer. Other host adapters must snapshot that fact before
forwarded-address middleware runs.

Framework client entry points are intentionally URI-driven:

| Framework | Client entry point |
| --- | --- |
| Python | `Client.from_iroh("iroh://...")` or `Client.from_iroh("httpi://...")` |
| Rust | `VgiClient::connect_to`; use `connect_iroh_with_endpoint` for an application-built endpoint |
| TypeScript | `VgiClient.fromIroh`; browsers inject `@query-farm/vgi-rpc-iroh-browser` |
| Java/Kotlin | `VgiClient.connectIroh` or `VgiClient.connectHttpi` with the optional `vgirpc-iroh` provider |
| Go | the lower RPC client's explicit Iroh provider seam; no native Go provider is bundled |
| C# | the RPC client packages' `iroh://` and `httpi://` connectors |
| C++ | `RpcClient` for `iroh://`; `HttpClient` for `httpi://`, built with `VGI_RPC_WITH_IROH_CABI=ON` |

An endpoint URI selects semantics, not discovery policy. Client options still
carry the stable local key, relay set, remote relay hint, direct addresses,
timeouts, and cancellation. An absent optional native binding fails as
unsupported; clients never silently run or download a helper executable.

## Start the bridge

Use a persistent endpoint key in production:

```console
vgi-iroh-bridge \
  --secret-key-file /run/secrets/vgi-iroh-key \
  --raw-upstream tcp://127.0.0.1:9400 \
  --http-upstream http://127.0.0.1:9401
```

The first stdout line is the stable EndpointId. `--ephemeral` is explicitly a
development mode. The worker-facing ports must be unreachable except from the
bridge; trusting loopback means trusting every process able to connect through
that host boundary.

## Private relays and direct-only networks

Repeat `--relay-url https://relay.example` on the bridge. Clients use their
language's `relay_urls` / `relayUrls` / transport-options field. The local
relay set configures the client's own endpoint; a remote relay URL or endpoint
address is a discovery hint for the peer. `no_relay` is mutually exclusive
with a relay set and never falls back to the public network.

Rust's high-level `VgiClient::connect_iroh_with_endpoint` takes ownership of an
application-built `iroh::Endpoint`, `EndpointAddr`, and runtime. Use that entry
point for custom relay modes, direct-address hints, or a stable client key.
Other native SDKs expose the same knobs through their existing Iroh transport
options or injected native provider. Missing optional bindings fail as
`unsupported`; no framework downloads an executable at runtime.

## Scaling and health

Use HTTP-Iroh when replicas should externalize state and be balanced per HTTP
request. The bridge intentionally has one fixed HTTP origin; put Envoy, nginx,
or a cloud HTTP load balancer at that origin if it represents several
replicas. Preserve shared token keys/external storage and the platform's
request/response ceilings.

Raw Iroh selects a destination EndpointId and keeps each logical stream on the
selected worker. Scale it with multiple EndpointIds and client-side selection,
or accept an application-aware gateway. There is no transparent Iroh load
balancer contract. During shutdown, stop advertising/accepting new work, drain
existing streams within the configured deadline, and then terminate the
worker. Health probes belong on the worker's HTTP endpoint or a separate local
admin endpoint; they do not authenticate an Iroh caller.

Never log secret keys, raw capabilities, forwarded identity headers, or full
personal identity profiles. Record only outcome, provider, evidence source,
latency, and a suitably protected canonical principal when audit policy needs
it.
