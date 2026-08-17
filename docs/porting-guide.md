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
- 12 always-required fields: `server_id`, `protocol`, `protocol_hash`, `method`, `method_type`, `principal`, `auth_domain`, `authenticated`, `remote_addr`, `duration_ms`, `status`, `error_type` plus the four envelope fields `timestamp` (RFC 3339 UTC ms-precision), `level` (`"INFO"`), `logger` (`"vgi_rpc.access"`), `message` — 16 keys in total on every record. `protocol_hash` is the one most easily missed: it is what lets a consumer reading archived JSONL decide whether a cached schema decoder still applies, so it is required even though nothing in a single record's own content needs it.
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

### Run it at DEBUG, and require the payload

```bash
vgi-rpc-test --cmd "./your-worker --access-log /tmp/conformance.log --access-log-debug" \
             --access-log /tmp/conformance.log \
             --require-request-data
```

Two additions, and both exist because of a bug that shipped:

`request_data` is the heaviest field in the record and most emitters gate it
behind DEBUG. Validate at INFO and the field is simply absent, so every rule
governing it goes unexercised — the validator checks it is *well-formed when
present*, which a log that never carries it satisfies trivially. `--require-request-data`
turns that into a failure. It is the same shape as asserting a CORS expose
list without ever asserting the header: the rule is enforced everywhere except
where it applies.

Include a **zero-parameter method** in the filter (`void*`). A method with no
arguments sends an empty schema and no row — see §4.3 — and a validator that
demands one row unconditionally rejects it. That is not hypothetical: v0.36.1
shipped exactly that rule, failed the Python reference's own `void_noop`
records, and reported a correct Java port as non-conformant. Nothing caught it
because `vgi-rpc-conformance` had no `--access-log` flag, so the reference
implementation was the one that could never be run through its own validator.

**Run this in CI, not by hand.** Access-log conformance drifted precisely
because verification was manual: four people checked it by inspection and the
rule was still wrong.

## Recommended port path

In order, smallest-blast-radius first:

1. **Wire protocol round-trip.** Get a single unary `__describe__` call working end-to-end. `vgi-rpc-test --filter scalar_echo*` is the quickest first goal.
2. **Conformance service implementation.** Translate the protocol class. Run the full unary test set.
3. **Streaming.** Producer streams first (no client input), then exchange streams.
4. **HTTP transport.** State-token signing and replay protection.
5. **Access log.** Add a hook around dispatch that emits the JSON record. Begin with the 12 always-required fields; add conditional fields as you wire them up.
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
- **HTTP state-token format.** Tokens are AEAD-sealed (XChaCha20-Poly1305 or ChaCha20-Poly1305, depending on what's available natively in the target language). Each port is free to choose its own plaintext encoding — Python uses a compact msgpack codec for flat states and falls back to length-prefixed Arrow IPC for states holding Arrow values, Go uses gob, TypeScript uses JSON+BigInt, Java uses CBOR, Rust uses length-prefixed bytes — because tokens are not expected to round-trip across language ports. The behavioral contract is per-port: round-trip integrity, cross-principal replay protection (via AEAD AAD or per-principal key derivation), and TTL enforcement after authenticity. See `vgi_rpc/http/server/_state_token.py` for the Python reference.
- **Token payloads MUST be compressed inside the seal.** Codec is your choice — zstd if the runtime has it, deflate/gzip otherwise — but the *placement* is not: compressing after sealing accomplishes nothing, because a sealed token is ciphertext and the body codec then recovers only the base64 slack (~76–80%), never the state's structure. Inside the seal it reaches the real redundancy; the reference records a 7,800-byte call state packing to 1,872, taking the token from 10,820 bytes to 2,552. Prefix a self-describing codec tag, emit the raw tag and skip compression when it does not shrink (a small token must never grow), bound the decompressed size, and reject an unknown tag or a failed decompress as the same uniform 400 as any other token failure. Together with the call/cursor split this is what keeps continuation payloads small — splitting alone still pays full freight per turn if each half ships uncompressed. **None of this is visible on the wire, so the shared conformance suite cannot check it** — cover it with a language-local test over your own seal/open path: compression engages on a large payload, a tiny payload stays raw, a corrupt payload 400s.
- **HTTP stream state is two tokens, and a warm cache will hide getting it wrong.** A stream's state MUST be split by lifetime: a `vgi_rpc.call_state#b64` **call token** (minted once by `/init`, never re-issued) and a per-turn `vgi_rpc.stream_state#b64` **cursor**. See [WIRE_PROTOCOL §stream exchange](WIRE_PROTOCOL.md), including the normative resolution order — open the cursor first, and only then use its authenticated `call_id` as a cache key. Getting that order wrong is a cross-principal disclosure bug that every functional test still passes. Both sides are enforced: `TestCallTokenSplit` fails a *server* that packs everything into the cursor, and `TestColdCallStateCache` fails a *client* that does not echo the call token. The client half is the sneaky one — a splitting server may resolve the call from a per-process cache, so a non-echoing client passes every test you are likely to write and then fails in production the first time a continuation lands on a restarted worker, an evicted entry, or a node that never saw the `/init`. Boot the reference server with its cache disabled (`serve_http(..., call_state_cache_entries=0)`, or `tests/serve_conformance_http.py --no-call-state-cache`) and expose it as a `conformance_http_cold_call_cache_port` fixture; every continuation then takes the miss path. The same obligation binds intermediaries — forwarding a continuation means forwarding both tokens.
- **Per-process server identity.** `server_id` is generated once per process lifetime, NOT per call. The same string must appear in every log record from the same instance.

## HTTP unauthorized responses

Every 401 a conformant HTTP server emits carries a coarse reason code from a closed set, on a `VGI-Auth-Reason` header and in a JSON body, plus a static note on services whose authentication depends on a reverse proxy. The full spec lives at [`docs/unauthorized-spec.md`](unauthorized-spec.md). The canonical `TestUnauthorized` group is capability-gated on `VGI-Auth-Reason`, so a port that has not adopted this skips cleanly.

The pieces worth calling out:

1. **Negotiate on `Accept`, and default to JSON.** `*/*` — what every RPC client sends — MUST resolve to the JSON envelope. A port may serve JSON to browsers too; it may never serve HTML to a client that did not ask for `text/html`. Getting this backwards is the pre-existing bug this spec was written to fix: the Python client used to paste an entire HTML page into an exception message.
2. **Keep the reason set closed.** Six codes, listed in §3 of the spec. A failure that maps to none of them is `unauthorized`. A client switching on the code needs the set not to grow under it in a language it does not control.
3. **The code names the stage, not the diagnosis.** Every proxy-proof outcome collapses onto `proxy_required` — this is what keeps the uniform-rejection rule of the proxy-proof spec intact while still telling an operator which layer refused.
4. **Derive the proxy note from configuration, never from the request.** Emit it on every 401 from a proxy-dependent service, identically. That is what makes it safe to show, and it is still correct in the case it exists for: a proxy that is not forwarding the header 401s *everything*.
5. **Carry declarations through composition.** If your chain / require-all helpers do not propagate an authenticator's proxy-header dependency, wrapping one silently drops the note — which is exactly the deployment where you needed it.
6. **Prove your codes are actually discriminated.** A server that answers every 401 with `unauthorized` passes the closed-set check, which is the one assertion most ports will write first. Supply the optional `conformance_http_auth_reason_port` fixture — a worker whose `authenticate` maps the `X-Conformance-Auth-Reason` request header onto the matching reason — and four more tests turn on that fail exactly that server. The header is a fixture affordance, never a production behaviour; see §7.1 of the spec and `tests/serve_conformance_http_auth.py` for the ~10-line reference.

## HTTP CORS

CORS is opt-in (`make_wsgi_app(cors_origins=...)` / `serve_http(cors_origins=...)`), and a server without it configured MUST emit no CORS headers at all — `TestCorsOffMode` asserts that everywhere, ungated. Ports that implement the feature supply an optional `conformance_http_cors_port` fixture: a worker allowing the origin `https://conformance.example`. The canonical `TestCors` group then runs; ports without the fixture skip it.

This is the one contract the rest of the suite structurally cannot check. Every conformance test drives the server with an ordinary HTTP client that sees all response headers and may send any request header. A browser does neither. So a port can implement every capability header correctly, pass every other group, and still ship a server that is unusable from a browser — and nothing anywhere else in the suite will say so.

Both halves matter, and they fail differently:

1. **Response half — `Access-Control-Expose-Headers`.** A browser hides every response header not on this list from JavaScript. That covers the whole capability system (`VGI-Max-Response-Bytes`, `VGI-Supported-Encodings`, `VGI-Sticky-Enabled`, `VGI-Externalization-Enabled`, …), plus `VGI-Auth-Reason` on a 401, the session headers, and `X-VGI-RPC-Error` — which is how a client tells an error response from a result, so without it a browser client cannot distinguish the two at all. The rule is simply: **whatever you advertise, expose.** `test_advertised_capabilities_are_all_exposed` derives its expectation from what your server actually advertises on `OPTIONS /health`, so it adapts to your feature set rather than demanding features you never claimed.
2. **Request half — `Access-Control-Allow-Headers`.** A browser refuses to *send* a header the preflight did not permit. Falcon echoes `Access-Control-Request-Headers` back, so the Python reference is permissive by construction; a port that hardcodes `Content-Type` instead will look completely healthy on ordinary calls and then silently break sticky sessions (`VGI-Session`, `VGI-Session-Accept`), proxy proof (`VGI-Proxy-Proof`), and encoding negotiation (`X-VGI-Accept-Encoding`). Echoing the request, returning an explicit list, or `*` are all conformant.

3. **`Cross-Origin-Resource-Policy: cross-origin`** on every response, when CORS is configured. Correct CORS is *not* sufficient for a caller that has opted into cross-origin isolation: a page sending `Cross-Origin-Embedder-Policy: require-corp` — which any page using `SharedArrayBuffer` must — has its own fetches blocked unless each response also carries CORP. The server sees an ordinary successful response, so this fails invisibly from the operator's side. `cors_resource_policy` narrows it (`"same-site"`) or omits it (`None`); the conformance test requires the default, since reaching it means the port declared browser support.

Two further points the tests pin:

- `application/vnd.apache.arrow.stream` is not a CORS-safelisted `Content-Type`, so **every** RPC call is a preflighted request. There is no simple-request fast path to fall back on.
- `Access-Control-Allow-Origin` must be on the *actual* response, not only the preflight. A browser re-checks it and discards the body without it, so a server that sets it on `OPTIONS` alone fails every real call while passing a naive preflight-only test.

### Two blind spots the derived check cannot cover

`test_advertised_capabilities_are_all_exposed` derives its expectation from `OPTIONS /health`, which makes it adapt to a port's feature set — but that also bounds what it can see, in two ways that produced real misses in the Go and Rust ports:

- **Headers a plain worker never advertises.** The conditional capability headers — `VGI-Upload-URL-Support`, `VGI-Max-Upload-Bytes`, the size caps — are absent from a worker with no storage configured, so a missing exposure for them passes. **Point `conformance_http_cors_port` at a storage/upload-enabled worker**, not a bare one; `test_worker_advertises_the_optional_capabilities` fails the fixture if you don't.
- **Headers that only ride failures.** `OPTIONS /health` is a success-path surface, so nothing advertises `X-VGI-RPC-Error`, `VGI-Auth-Reason`, or `X-Request-ID` — a derived check structurally cannot reach them. They are named explicitly in `_ALWAYS_EXPOSED` and asserted one at a time. `VGI-Auth-Reason` is the one to check first: without it a browser client cannot read the machine-readable half of a 401 and is back to parsing prose.

## The externalized-response cap must be enforced, not just advertised

`max_externalized_response_bytes` is the cap with **no escape valve**. `max_response_bytes` governs what lands on the wire and is *soft* for producer streams, because a continuation token carries the overshoot to the next turn. Bytes already uploaded to external storage cannot be un-uploaded, so this cap is hard on every method type — and it is the one whose absence costs real egress before anyone notices.

One port shipped it advertised and unenforced: the configured value was read only to emit `VGI-Max-Externalized-Response-Bytes` and add it to the CORS expose list, and was never compared against anything. A worker capped at 512 bytes uploaded 200,336 bytes and answered success. A client sizing its requests against that header was reading a promise the server did not keep.

`TestExternalizedResponseCap` pins three things: an overshooting unary response fails, a payload *under* the cap still round-trips through the external channel (so the cap is a cap and not a wall), and a producer gets no continuation escape from it — the direct opposite of `TestHttpResponseCapSoftWire`, which pins that a producer *does* get one for the wire cap.

Supply `conformance_http_externalized_cap_port`: a worker with storage wired, a tight `max_externalized_response_bytes`, and a **generous** `max_response_bytes`. Getting that second part wrong is the trap — with both caps tight, the body cap fails first and the group passes while proving nothing about the external channel. The group is capability-gated on the advertisement, so a port with no external channel skips; a port that advertises has no way out, because the header is the promise.

## The error flag must be sent, not just exposed

A failed RPC answers **HTTP 200** — the error rides the body as an EXCEPTION batch, because the call reached the method and the method raised. The status line therefore says nothing, and `X-VGI-RPC-Error: true` is how a client tells a failure from a result without parsing the body.

`TestErrorHeader` asserts both directions: an error response sets it, and a successful one does not. A flag set on every response carries no information, which is the same outage as never setting it.

Until this existed the suite required the header in `Access-Control-Expose-Headers` and never checked any response carried it — a port could expose a header it never sends and pass.

## HTTP request-id correlation

`X-Request-ID` on the response and `request_id` in the access log must name the same request. That agreement is the whole value of the field: an ID that appears on the response but not in the log, or differs between them, is worse than none — it looks like a working trail right up to the moment someone tries to follow it.

`docs/access-log-spec.md` §4.4 makes propagation a SHOULD, so `TestRequestId`'s header cases skip cleanly for a port that emits nothing. What is **not** optional is agreement: a port that emits both must make them equal.

The group asserts four things — the response carries an ID, a caller-supplied one is echoed rather than replaced, generated ones differ between requests, and the header matches the record. That last one needs the worker's log, so supply the optional `conformance_http_access_log` fixture: `(port, path)` for a worker started with `--access-log PATH`. Omit it and only the correlation case skips.

This is the same gap the CORS work turned up twice — the suite asserted `X-Request-ID` was in the expose list and never that it was sent. A rule enforced everywhere except where it applies is not enforced.

## HTTP token introspection

**Optional, and off unless explicitly enabled.** A port that does not implement it MUST still answer `POST {prefix}/__introspect_token__` with a status a caller treats as **definitive** — `401`, `403` or `404`. That requirement is not decoration: a caller classifying anything else as transient will retry forever against a worker that is never going to support the feature, so a `415` from a generic catch-all route turns a misconfiguration into an infinite loop instead of a preflight failure. The reference answers `404 {"error": "not_enabled"}`.

The endpoint resolves an **opaque bearer credential** to a **principal**, for a reverse proxy that terminates the only public listener and must know which principal a credential authenticates as before it can authorize anything.

```
POST {prefix}/__introspect_token__
Authorization: Bearer <introspector credential>
Content-Type: application/json
{"token": "<opaque subject credential>"}

200  {"principal": "...", "token_name": "...", "ttl_seconds": 300}
401/403/404  definitive   — the caller may cache this
5xx / transport failure   — the caller MUST NOT cache this
```

### Why the guards are hard requirements

The response is **an identity assertion made by the thing being protected**, and the asker acts on it using credentials the worker does not hold — storage credentials on a data-plane host, service-credential attachments in an entitlement resolver, policy-tier selection. "Trust it as much as you trust the worker" is the wrong frame: it must be trusted *more*, because it steers privileges the worker never has.

1. **Never return claims.** The response is a closed set: `principal`, `token_name`, `ttl_seconds`. A pass-through claims field would let a worker choose its caller's tenant routing, its row scope, and its policy branch — the single most dangerous thing this feature could grow. Askers derive what they need from the principal alone.
2. **The route is absent unless explicitly enabled.** No worker grows a credential-to-identity oracle by upgrading a dependency.
3. **The introspector allowlist has no permissive default.** Authentication and introspection are different capabilities. A deployment where *any* valid credential may introspect lets any user test guesses of any other user's credential at unlimited rate, and resolve a stolen one to its owner. A port that checks only authentication passes every other test in the group — `test_non_introspector_refused` is the one that catches it.
4. **Reject JWS-shaped subjects without resolving them.** A JWS is validated locally against a key set; routing one here hands a third party a bearer token the asker may itself have rejected, and an expired access token is still live at its issuer for other resources.
5. **Uniform rejection.** Unknown, expired and malformed are one answer, byte for byte. Reporting which confirms that a guessed credential exists.
6. **Never log the credential.** Digest it (SHA-256) for diagnostics. The conformance group asserts the credential is absent from responses; the reference asserts it against captured log output too, and a port should do the same locally.
7. **Advertise on `/health`.** `VGI-Token-Introspection: true` when enabled, absent otherwise, so a proxy preflights at boot rather than at first login.

Do **not** implement this by replaying the credential through the worker's own authenticate chain. It is the attractive design and it breaks four ways: a precondition gate wrapping the chain makes the replay unimplementable; it runs the worker's independently-configured audience/issuer set, so a credential the asker *rejected* could be accepted; cookie- and mTLS/IP-derived identity cannot be replayed at all, and a synthesized request carries the proxy's own address — silently elevating any address-allowlist member rather than failing cleanly; and it invents a fake-request contract every future authenticator must honour with no type to enforce it. Take a narrow `resolve(credential) -> principal | None` callable instead.

### Definitive vs transient is normative

"The credential is bad" and "I could not find out whether the credential is bad" are different answers, and a caller's cache depends on telling them apart. A rejection may be negative-cached; an outage must not be, or a worker restart takes the fleet down for the cache's lifetime. The reference adds `AuthUnavailableError`, which is deliberately **not** a `ValueError` so that `chain_authenticate` — which advances on `ValueError` — propagates it instead of reading it as "not my credential, try the next" and emerging as a 401 from the end of the chain. It surfaces as `503` with `Retry-After`. A port needs the equivalent distinction in whatever its chaining primitive is.

The same distinction binds the **resolver**, and it is easier to get wrong there: the endpoint's own "did not resolve" is `404`, which is exactly the answer a caller may negative-cache, so a resolver whose backing store is down must not borrow it. In the reference the resolver raises the same `AuthUnavailableError` and the endpoint converts it to `503` + `Retry-After` — not through the uniform-rejection path, which is for definitive answers and carries `Cache-Control: no-store` instead.

### Conformance

Supply an optional `conformance_http_introspect_port` fixture: a worker with introspection enabled, configured with the exact constants in `_pytest_suite.py` (`_INTROSPECTOR`, `_SUBJECT_TOKEN`, `_SUBJECT_PRINCIPAL`, `_JWS_TRAP_TOKEN`). Omit it and `TestTokenIntrospection` skips; `TestTokenIntrospectionOffMode` runs everywhere regardless, because "off unless enabled" binds every port.

Note that `_JWS_TRAP_TOKEN` must be **resolvable** by your fixture's resolver. Against an unknown JWS a port with no shape guard rejects it as unknown and passes for the wrong reason; resolvable, the shape guard is the only thing that can produce a rejection.

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
