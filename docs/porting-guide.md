# Implementer's Guide: Porting vgi-rpc to a New Language

This document is the minimum-viable checklist for shipping a vgi-rpc server in a language other than Python. Cross-language correctness is enforced by a single tool, `vgi-rpc-test`, which drives any worker over its wire protocol and validates both behavior and observability output.

If your worker passes `vgi-rpc-test --cmd "./your-worker --access-log /tmp/log" --access-log /tmp/log`, it is conformant.

## What you must implement

### 1. The wire protocol

See [`WIRE_PROTOCOL.md`](WIRE_PROTOCOL.md). Concretely:

- Read Arrow IPC streams from stdin (or accept an HTTP request, or accept a Unix socket connection — pick the transports you support).
- Validate `vgi_rpc.request_version` custom metadata against the constant declared in the spec.
- Dispatch by `vgi_rpc.method` custom metadata.
- Reply with one IPC stream containing zero-row log batches followed by one result batch (unary), or interleaved log/data batches terminated by EOS (stream).
- Encode errors as a zero-row batch with `vgi_rpc.error_*` metadata keys.
- Handle the two built-in synthetic methods before normal dispatch: `__describe__` (introspection; [WIRE_PROTOCOL §14](WIRE_PROTOCOL.md)) and `__transport_options__` (transport capability negotiation; [§15](WIRE_PROTOCOL.md)). A worker that does **not** implement SHM can omit `__transport_options__` entirely — the standard `method_not_implemented` error makes clients fall back to the pipe. A worker that **does** implement SHM MUST answer it, reporting `vgi_rpc.transport.shm = "true"`, or clients will not use SHM with it.

### 2. The CLI surface

Every worker MUST accept these flags. `vgi-rpc-test` spawns workers with combinations of them.

| Flag | Behavior |
|---|---|
| (none) | Serve over stdin/stdout (pipe transport). Print no extraneous lines on stdout — Arrow IPC is binary. |
| `--http` | Serve HTTP on the loopback interface. Print exactly `PORT:<port>\n` on stdout, then flush. |
| `--host HOST` | HTTP bind address (default `127.0.0.1`). |
| `--port PORT` | HTTP port (default 0 = auto-select). |
| `--unix PATH` | Listen on a Unix domain socket at `PATH`. Print exactly `UNIX:<path>\n` on stdout, then flush. |
| `--access-log PATH` | Open `PATH` for append, write one JSON record per RPC call as defined by [`access-log-spec.md`](access-log-spec.md). |

Workers SHOULD respond to `SIGTERM`/`SIGINT` by shutting down cleanly so the test runner doesn't have to escalate to SIGKILL.

### 3. The access log

This is the conformance proof for observability. The full spec is in [`access-log-spec.md`](access-log-spec.md); the schema is [`vgi_rpc/access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json). Highlights:

- One record per RPC call (or per stream init / per stream continuation).
- JSON-Lines (NDJSON), UTF-8.
- 11 always-required fields: `server_id`, `protocol`, `method`, `method_type`, `principal`, `auth_domain`, `authenticated`, `remote_addr`, `duration_ms`, `status`, `error_type` plus the four envelope fields `timestamp` (RFC 3339 UTC ms-precision), `level` (`"INFO"`), `logger` (`"vgi_rpc.access"`), `message`.
- Conditional fields keyed off `method_type` and `status` — never off method names.
- `request_data`: base64 of a self-contained Arrow IPC stream of the request batch. Round-trip equivalence is the test, not byte equivalence — your Arrow library's serialization is fine.

### 4. The conformance service

Your repo must include a runnable conformance worker that registers the [`vgi_rpc.conformance.ConformanceService`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/conformance/_protocol.py) protocol. The Python definition is the reference; your port translates each method to native types preserving Arrow schema. Method names, parameter names, and stream-state semantics must match exactly.

Run `vgi-rpc-test --list` to see the test surface. ~150 tests cover unary, streaming, errors, logging, defaults, enums, optionals, externalized batches, and HTTP-specific behavior.

## How to verify

1. `pip install "vgi-rpc[conformance]>=0.10"` in any Python environment.
2. Build your worker.
3. ```bash
   vgi-rpc-test --cmd "./your-worker --access-log /tmp/conformance.log" \
                --access-log /tmp/conformance.log \
                --format json
   ```
4. Exit code 0 means: every test passed AND every access-log record validated.
5. Wire that command into your CI.

## Recommended port path

In order, smallest-blast-radius first:

1. **Wire protocol round-trip.** Get a single unary `__describe__` call working end-to-end. `vgi-rpc-test --filter scalar_echo*` is the quickest first goal.
2. **Conformance service implementation.** Translate the protocol class. Run the full unary test set.
3. **Streaming.** Producer streams first (no client input), then exchange streams.
4. **HTTP transport.** State-token signing and replay protection.
5. **Access log.** Add a hook around dispatch that emits the JSON record. Begin with the 11 always-required fields; add conditional fields as you wire them up.
6. **External-location and shared-memory transports** if you need them. For SHM, implement the `__transport_options__` handshake on both sides: a worker reports `vgi_rpc.transport.shm = "true"`; a client negotiates once per worker (caching the result) and only writes SHM pointer batches when the worker confirmed support, else it stays on the pipe. See [WIRE_PROTOCOL §15](WIRE_PROTOCOL.md).
7. **Unix / TCP socket transports** if you need them. Both reuse the raw Arrow-IPC framing (no HTTP envelope); they differ only in the listening socket. The worker accepts `--unix PATH` / `--tcp [HOST:]PORT`, defaults the TCP host to loopback (`127.0.0.1`), and emits a `UNIX:<path>` / `TCP:<host>:<port>` discovery line on stdout once bound. Drive conformance with `vgi-rpc-test --unix <path>` / `--tcp <host>:<port>`. TCP carries **no auth/TLS** — it is for trusted networks only; use HTTP otherwise.

## Reference implementations

- **Python** (reference): this repository.
- **Go**: [vgi-rpc-go](https://github.com/Query-farm/vgi-rpc-go) — uses Apache Arrow Go and a `DispatchHook` interface for observability. Schema-aligned via `vgirpc.AccessLogHook` and `--access-log <path>` on the conformance worker.

### Conformance status

All four ports (Go, TypeScript, Java, Rust) implement HTTP transport with stream-state token plumbing, and all four pass the full `TestSticky` group including the failure-path fixtures. Total test counts are deliberately not quoted here — they move with every suite addition and the numbers in a doc go stale faster than anyone updates them; run the suite for the current figure.

Snapshot of how each port differs from the Python reference today:

- **Go**: fully aligned for pipe, HTTP, Unix, and shared-memory transports. State tokens are AEAD-sealed (XChaCha20-Poly1305) over a gob-encoded payload. `stream_id` stable across continuations.
- **TypeScript**: HTTP + pipe transports implemented. State tokens are AEAD-sealed (XChaCha20-Poly1305 via `@noble/ciphers`) over a pluggable serializer (default JSON + BigInt). Token wire-format v4 envelope matches Python.
- **Java**: HTTP + pipe + Unix transports implemented. State tokens are AEAD-sealed (ChaCha20-Poly1305 via JDK 21 native `javax.crypto.Cipher`, 12-byte nonce) over a CBOR-encoded payload (or caller-supplied via `PortableStreamState`).
- **Rust**: HTTP + pipe + Unix transports implemented. State tokens are AEAD-sealed (XChaCha20-Poly1305 via the `chacha20poly1305` crate) over a length-prefixed envelope matching Python.

**TCP transport.** The Python reference implements a raw-framing TCP transport (`TcpTransport` / `serve_tcp`, conformance via `vgi-rpc-test --tcp`); it shares the Unix-socket serve loop and differs only in the listening socket (AF_INET, `[HOST:]PORT`, loopback default). Ports that already have Unix transport (Go, Java, Rust) can add TCP by cloning their Unix serve path; TypeScript adds it alongside its launcher socket support. Status per port is tracked as each lands.

## HTTP response-cap conformance

The conformance suite includes HTTP-only tests under category
`http_response_cap.*` that verify the framework refuses to emit
oversize responses when the operator has configured caps and there is
no externalisation escape valve.

### Two operator knobs

- **`max_response_bytes`** — caps the HTTP body size (the bytes that
  literally land on the wire).  Externalised payloads do not count
  against this; their pointer batches are tiny.
- **`max_externalized_response_bytes`** — caps the total bytes
  uploaded to external storage during one HTTP response.  Bounds how
  much data the client will end up fetching for one RPC, regardless
  of how the framework chose to deliver it.

Both default to `None` (unbounded) and are configurable via
constructor kwargs, CLI flags, or env vars (see `make_wsgi_app`,
`serve_http`, and `run_server` for the full set; the deprecated
`max_stream_response_bytes` alias remains for one release cycle).

### Capability discovery (HTTP response headers)

Servers advertise the configured caps so capability-aware clients and
conformance tests can probe without a separate handshake:

| Header | Value when set |
|---|---|
| `VGI-Max-Response-Bytes` | integer body cap |
| `VGI-Max-Externalized-Response-Bytes` | integer external cap |
| `VGI-Externalization-Enabled` | `true` or `false` (always present) |

Cross-language ports must emit these headers on every response when
the corresponding knob is configured.

### Strict-fail behaviour by method type

| Method type | Wire cap (`max_response_bytes`) | External cap |
|---|---|---|
| Unary | hard — strict-fail | hard — strict-fail |
| Stream-exchange | hard — strict-fail | hard — strict-fail |
| Stream-producer | **soft** — continuation tokens cover overshoot | hard — strict-fail |

Strict-fail surfaces as the existing 200 + EXCEPTION-batch shape
(`_set_http_status` rewrites 500 → 200 with `X-VGI-RPC-Error: true`
for unary; producer/exchange append a zero-row EXCEPTION batch to the
in-progress IPC stream).  The client sees a normal `RpcError`.

The error message is one of:

- `HTTP body exceeds max_response_bytes (...) for method '<name>'`
- `Externalised payload exceeds max_externalized_response_bytes (...) for method '<name>'`

Cross-language ports must produce error messages containing the
token `max_response_bytes` or `max_externalized_response_bytes`
respectively — the conformance tests assert on those substrings.

### `transports` field on conformance tests

The Python catalog gained a `transports: tuple[Literal["pipe","http","unix"], ...]`
field on `@_conformance_test`, defaulting to all three.  The CLI
runner (`vgi-rpc-test`) detects the active transport from the user's
flag (`--cmd` → `pipe`, `--unix` → `unix`, `--url` → `http`) and
skips tests whose `transports` tuple excludes it.  Ports must
honour this for the four `http_response_cap.*` tests:

- `http_response_cap.unary_strict_fail` (HTTP only)
- `http_response_cap.exchange_strict_fail` (HTTP only)
- `http_response_cap.producer_external_strict_fail` (HTTP only;
  also requires externalisation enabled and an external cap)
- `http_response_cap.externalized_strict_fail` (HTTP only; same
  preconditions)

The tests self-skip when caps aren't configured, so a port can run
the full suite against any worker without these failing.  To
exercise them, boot a strict-cap worker — see
`tests/serve_conformance_http_strict.py` for the Python reference
(defaults to 1 MiB body + 1 MiB external).

### Worker visibility (optional)

`OutputCollector` exposes three new properties so worker code can
size its emit to the available budget:

- `out.remaining_response_bytes: int | None` — wire body bytes left
  this iteration.
- `out.remaining_externalized_response_bytes: int | None` — external
  channel bytes left.
- `out.externalization_enabled: bool` — whether the server has a
  storage backend wired up.

Snapshot semantic: each value is fixed at collector construction;
within one `state.process()` call it does not update as the worker
emits.  Wire bytes include IPC framing (slightly conservative for a
worker computing payload size).  Optional surface; ports that don't
expose it are still conformant — strict-fail catches workers that
ignore the budget.

## Gotchas

- **Arrow dictionary encoding.** Across language Arrow libraries, the placement of dictionary messages in IPC streams differs. The schema's `request_data` round-trip rule was chosen specifically to absorb this — don't try to byte-match Python.
- **Custom metadata key ordering.** Some Arrow libraries do not preserve insertion order. Test your reader against batches produced by Python.
- **HTTP state-token format.** Tokens are AEAD-sealed (XChaCha20-Poly1305 or ChaCha20-Poly1305, depending on what's available natively in the target language). Each port is free to choose its own plaintext encoding — Python uses length-prefixed Arrow IPC, Go uses gob, TypeScript uses JSON+BigInt, Java uses CBOR, Rust uses length-prefixed bytes — because tokens are not expected to round-trip across language ports. The behavioral contract is per-port: round-trip integrity, cross-principal replay protection (via AEAD AAD or per-principal key derivation), and TTL enforcement after authenticity. See `vgi_rpc/http/server/_state_token.py` for the Python reference.
- **Per-process server identity.** `server_id` is generated once per process lifetime, NOT per call. The same string must appear in every log record from the same instance.

## HTTP proxy proof

Proxy proof is an **opt-in additive feature**: a worker can refuse any request that did not arrive through a trusted proxy, which is verified by recomputing an HMAC over a timestamp, a nonce and the worker's own identifier. The full spec lives at [`docs/proxy-proof-spec.md`](proxy-proof-spec.md). A port may:

- **Skip it entirely.** The canonical `TestProxyProof` conformance group is gated on the runner supplying a `proof_worker_factory` fixture; a port that omits the fixture skips the whole group cleanly. The `TestProxyProofOffMode` group is *not* gated and runs everywhere — it asserts an unconfigured worker is completely unaffected, which is the property "opt-in, off by default" actually means.
- **Implement the verifier.** This is the whole feature for a worker. Everything needed is in each language's standard library: HMAC-SHA256 and a constant-time comparison. No new dependency was required in any of the five existing ports.

### Getting it right

The pieces that are easy to get subtly wrong, in the order they bite:

1. **Frame the canonical string exactly.** It is NUL-separated: `"vgi.proxy.proof.v1" \0 kid \0 ts \0 nonce \0 origin_id`. A port that concatenates without separators round-trips perfectly against itself and fails against every other language. Verify against the golden vectors rather than your own minter — that is the only check that catches this.
2. **Validate every field's charset before decoding, including the MAC.** Base64 decoders disagree about invalid input: Python's silently discards non-alphabet bytes while Go, Rust and Java raise. Without an explicit charset check one port reports `malformed` where another reports `bad_mac`, and the reason code is part of this contract.
3. **Make the timestamp window two-sided.** `|now - ts| <= skew`, not just `now - ts <= skew`. An upper-bound-only check lets a future-dated proof pass indefinitely.
4. **Bound the nonce cache by capacity as well as TTL.** A TTL bounds how long an entry lives, never how many arrive inside the window, so a TTL-only cache is a remote memory-exhaustion vector. Evict oldest on overflow rather than rejecting: a traffic burst should not become an outage.
5. **Compose it as an AND, never through your chain combinator.** Every port's chain helper is first-success-wins, so a proof gate placed in a chain can be bypassed by any later credential. Expose a dedicated wrapper instead, and consider making the gate a distinct type your chain helper rejects at construction — the mistake is invisible in review and in testing until someone omits the header and is served anyway.
6. **Never echo the rejection detail.** The caller controls `kid`; reflecting it puts attacker-supplied text in your response body. Log the reason, return a fixed message.
7. **Keep the exemptions.** `OPTIONS`, `/.well-known/` and `{prefix}/health` stay reachable without a proof in every mode — load-balancer probes reach the worker directly, not through the proxy.

### Conformance worker

A port claiming support spawns a worker mirroring the reference CLI:

```
--http-proof --proof-secrets <kid>:<hex>,... --proof-origin-id <id> \
             [--proof-mode off|allow|require] [--proof-skew <s>] [--proof-no-replay-cache]
```

and supplies a `proof_worker_factory` fixture returning a `ProofWorker` (port, prefix, echoed config). The `prefix` is mandatory: it is the only way a shared test can address a route that actually exists, and asserting a rejection against a path that does not exist is how a whole group passes for the wrong reason.

## HTTP sticky sessions

HTTP sticky sessions are an **opt-in additive feature** layered on top of the stateless HTTP transport. The full spec lives at [`docs/sticky-sessions-spec.md`](sticky-sessions-spec.md). A port may choose to:

- **Skip sticky entirely.** The canonical `TestSticky` conformance group is capability-gated on the `VGI-Sticky-Enabled` header; ports that don't advertise it skip every test in the group cleanly. The non-sticky wire path is byte-identical for both implementations, so the rest of the conformance suite still passes.
- **Implement the client side only.** A client running against a Python sticky server needs to (1) recognize `error_kind="session_lost"` / `error_kind="server_draining"` on EXCEPTION-level batches and surface them as typed exceptions, (2) optionally implement a `with_session_token()`-equivalent that sends `VGI-Session-Accept: true` + `VGI-Session: <token>` on every request inside a scope, captures `VGI-Session` / `VGI-Session-Close: true` from responses, and captures + replays any `VGI-Echo-<name>` response headers (case-insensitive, prefix-stripped) on subsequent requests in the same session. The cookie-jar avoidance is intentional — header-only multiplexes concurrent sessions cleanly.
- **Implement the full server side.** Port `_StickyMiddleware` (per-worker registry + reaper thread + token sealing + optional echo-header emission), the `DELETE /vgi/__session__` resource (idempotent, principal-bound), and the `ctx.open_session` / `ctx.close_session` runtime API. The session token format from the spec is language-neutral: `created_at:u64 | server_id_len:u8 | server_id | session_id:bytes(12) | expires_at:u64`, AEAD-sealed with the same AAD shape used by stream tokens.

If a port claims sticky support it MUST also implement the three sticky conformance methods (`open_counter`, `increment_counter`, `close_counter`) on its `ConformanceService` implementation, so the canonical `TestSticky` group has something to exercise. Servers that advertise `VGI-Sticky-Enabled: true` but fail `TestSticky` are non-conformant.

**Echo headers** (`VGI-Echo-<name>` response headers / `VGI-Sticky-Echo-Headers` capability advert) are a sub-feature; ports that don't implement them stay conformant on `TestSticky` core but skip `TestSticky::test_echo_header_round_trip` cleanly (the test is capability-gated on `VGI-Sticky-Echo-Headers`). Implementing them unlocks zero-LB-config deployments on Fly.io (`fly-force-instance-id`) and any other platform with header-based proactive routing. See [`vgi_rpc/http/fly.py`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/http/fly.py) for the Python Fly quickstart helpers — a ~25-line module that other ports can mirror directly.

**Graceful drain** (`drain_handle(app)` operator API / `POST /__test_drain__` conformance admin endpoint) is similarly a sub-feature. The canonical `TestSticky::test_drain_rejects_new_opens` is capability-gated on the presence of the admin endpoint — ports that don't expose it skip cleanly. Implementing drain on the server side means: a per-worker drain flag observable by the sticky middleware so `ctx.open_session` raises `error_kind="server_draining"` while the flag is set; an operator-facing equivalent of `drain_handle(app)` so SIGTERM handlers / pre-fork worker-exit hooks can wire shutdown; and a `POST /__test_drain__` admin endpoint on the conformance server (and `DELETE` to clear, so the same fixture can run multiple conformance passes). See [`tests/serve_conformance_http.py`](https://github.com/Query-farm/vgi-rpc-python/blob/main/tests/serve_conformance_http.py)'s `_TestDrainResource` for the ~10-line Python reference.

**Principal binding is mandatory** for any port claiming sticky support: the session token MUST carry the identity tail in its AAD so a token replayed under a different principal fails to open (see [`docs/sticky-sessions-spec.md`](sticky-sessions-spec.md) §3.1 for the normative rule and its lifetime consequences). Every current port already does this — it falls out of reusing the stream-token AAD — but nothing verified it until the failure-path fixtures below existed, which is precisely the risk: an implementation that dropped the tail would pass every other test in the group.

**Failure-path fixtures — required for any port advertising sticky.** Three tests in the group cover the ways a session must *fail*: expiry, a token presented to the wrong worker, and a token replayed by a different principal. Each needs a worker the default fixture can't stand in for, so each is supplied by a runner fixture — `conformance_http_sticky_short_ttl_port`, `conformance_http_sticky_peer_ports`, and `conformance_http_sticky_auth_port` respectively. A server advertising `VGI-Sticky-Enabled: true` that withholds one fails the group by name; ports without sticky still skip cleanly. See [`docs/sticky-sessions-spec.md`](sticky-sessions-spec.md) §9.1 for the exact shapes and [`tests/conftest.py`](https://github.com/Query-farm/vgi-rpc-python/blob/main/tests/conftest.py) for the Python reference (each is a few lines on top of the existing worker, driven by the `--sticky-ttl` / `--token-key` / `--sticky-auth` flags in `tests/serve_conformance_http.py`).

Two traps every port hit while implementing these. **`--server-id`**: the peer pair must report *distinct* ids, and most conformance workers hardcode one — with a single id both peers are literally the same worker and the test has nothing to reject. **Prefix**: the sticky-auth worker must serve where the plain worker serves; the existing reject-all auth fixture in several ports moves to `/vgi`, and reusing that wiring 404s the group.

**Access-log fields** `session_id` and `session_action` (see [`docs/access-log-spec.md`](access-log-spec.md) §4.7) are required for any port that emits the `vgi_rpc.access` log AND advertises `VGI-Sticky-Enabled: true`. Ports without sticky support omit both fields (they're absent, not null).

Recognising the two new error kinds is the **minimum** any port should do: even ports that have no sticky implementation may end up talking to a Python sticky server in the wild, and a typed exception is much friendlier than a flat `RpcError` whose meaning the caller has to grep out of the message text.
