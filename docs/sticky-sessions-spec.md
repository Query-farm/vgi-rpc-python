# vgi-rpc Sticky Sessions Specification

This document is the cross-language contract for HTTP **sticky sessions** in vgi-rpc. The Python implementation in this repository is the reference; other-language implementations (Go, Rust, JS, Java, …) that wish to claim sticky-session conformance MUST implement the wire contract below so the canonical `TestSticky` conformance group in `vgi_rpc/conformance/_pytest_suite.py` passes against them.

## 1. Scope and constraints

Sticky sessions let an RPC method bind a **handle-bearing Python object** — an open DuckDB cursor, a loaded model in GPU memory, a streaming LLM client mid-generation, an open file — to the worker process that opened it, keyed by a short-lived AEAD-sealed session token the client echoes on subsequent requests. The state lives in process memory; it is **not** serialized, replicated, or persisted.

- **HTTP-only.** Pipe / subprocess / unix transports are single-process; sticky is meaningless there. The runtime API raises `RuntimeError("sticky sessions not available on this transport")` if invoked from a non-HTTP method.
- **Opt-in on both sides.** The server constructs its WSGI app with `enable_sticky=True`; the client opens calls inside a `with_session_token()` block. Outside that block neither side participates — the wire is byte-identical to the non-sticky framework.
- **Header-only transport.** Tokens ride in `VGI-Session` headers (request and response). Cookies are intentionally not used — they cannot multiplex multiple concurrent sticky sessions to the same host from a single client because the browser/jar maps one cookie per (origin, path).
- **Always-raise on session loss.** No silent retry. The framework surfaces a typed `SessionLostError`; apps decide whether to reopen or fail loudly.
- **In-process state only.** No pluggable session store. Cleanup is the context-manager contract: state objects with a `close()` method get it invoked on TTL eviction, explicit close, and graceful drain. Process crashes do NOT invoke `close()` — that's documented.

## 2. Wire contract

### 2.1 Request headers

| Header | Required | Purpose |
|---|---|---|
| `VGI-Session-Accept: true` | when a method may open a session | Client opt-in. Server MUST reject `ctx.open_session` calls on requests missing this header (raises a clear `RuntimeError` whose message names the header). Prevents the leaked-session bug where a server method opens a session for a client that isn't tracking it. |
| `VGI-Session: <token>` | when resuming an existing session | Echoes the token the server minted on a prior response. Server resolves it to a registry entry; failure to resolve (decode error, AAD mismatch, server_id mismatch, registry miss, expiry) is a session-lost error. |

### 2.2 Response headers

| Header | Emitted | Purpose |
|---|---|---|
| `VGI-Session: <token>` | when `ctx.open_session` was called this request | New token for the client to echo on subsequent requests. Base64url-encoded, AEAD-sealed envelope (see §3). |
| `VGI-Session-Close: true` | when `ctx.close_session` was called this request | Tells the client to drop its captured token. The server has already invoked `state.close()` and removed the registry entry. |

### 2.3 Capability headers

When `enable_sticky=True`, the server MUST advertise these on every response (cheapest discovery via `OPTIONS /health`):

| Header | Value | Notes |
|---|---|---|
| `VGI-Sticky-Enabled` | `"true"` | Discovery flag; absent or `"false"` on non-sticky servers. |
| `VGI-Sticky-Default-TTL` | integer seconds | The TTL applied by `ctx.open_session` when its `ttl` argument is `None`. Operator-tunable via `sticky_default_ttl`. |
| `VGI-Sticky-Echo-Headers` | comma-separated header names | Headers the client must replay on every subsequent request in the session — see §2.5. Absent when `sticky_echo_headers` is unset. |

### 2.4 Echo headers (`VGI-Echo-*`)

When the server is configured with `sticky_echo_headers={name: value, ...}`, every session-opening response (the response carrying the `VGI-Session` token) also carries `VGI-Echo-<name>: <value>` for each configured pair. The client MUST:

1. Capture each `VGI-Echo-<name>` header on the response (case-insensitive lookup).
2. Strip the `VGI-Echo-` prefix.
3. Send the inner header (`<name>: <value>`) on every subsequent request inside the same session view, until the server emits `VGI-Session-Close: true` (which clears the captured echo headers alongside the token).

Echo headers are emitted **once-only**, on the session-opening response. Subsequent responses MUST NOT re-emit them. Clients hold the captured map for the lifetime of the session view.

The primary use case is **client-driven routing**: on Fly.io the server emits `VGI-Echo-fly-force-instance-id: <machine-id>`, the client sends `fly-force-instance-id: <machine-id>` on every subsequent request, and fly-proxy routes directly to the owning Machine without any LB configuration. Other platforms with similar header-based routing (Railway, custom Envoy filters) work identically — only the header name and value change.

Echo headers carry no security guarantees beyond what the underlying transport provides; in particular they are NOT bound to the session token via AAD. A misbehaving client could echo a different header value than the server told it to. The contract assumes cooperative clients — the feature exists to make sticky routing *work*, not to enforce it.

### 2.5 Framework-managed endpoints

`DELETE {prefix}/__session__` — idempotent best-effort session teardown.

- Reads token from `VGI-Session` header (no cookie fallback).
- 204 No Content on hit (entry found, principal-bound, `state.close()` invoked, entry evicted).
- 200 OK on any failure — missing header, malformed token, AAD mismatch, server_id mismatch, registry miss. Idempotent so callers can't probe whether a session exists with a stolen token.
- Acquires the per-session RLock; queues behind any in-flight call on the same session.
- Not subject to the replay or routing mechanisms a future PR may add — DELETE is always local to the worker that owns the token.

## 3. Session token format

Each session token is an AEAD-sealed envelope binding the session ID to its issuing principal and worker. Both Python's stream token (`VGI-Stream-State`) and the session token share the same envelope construction.

- **Algorithm.** XChaCha20-Poly1305 via [`vgi_rpc.crypto`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/crypto.py).
- **Master key.** The same `token_key` that `make_wsgi_app(token_key=...)` consumes for stream tokens. Generated per-process by default; MUST be shared across workers for multi-process deployments (otherwise tokens minted on worker A are unreadable on worker B even if the LB routes correctly).
- **Envelope layout.** `version:u8 | nonce:bytes(24) | ciphertext+tag`. Version is currently `1`. The version byte is NOT authenticated as AAD — a tampered version byte still fails decryption because the recipient supplies the matching algorithm constant.
- **Plaintext frame** (inside the ciphertext): `created_at:u64 LE | server_id_len:u8 | server_id_bytes | session_id:bytes(12) | expires_at:u64 LE`.
- **AAD.** Same shape as the stream token's AAD — `b"vgi_rpc.state.v4\x00"` + principal-binding tail (`b"\x01" + domain + b"\x00" + principal` for authenticated requests, `b"\x00anonymous"` for unauthenticated). This makes cross-principal replay fail decryption at the crypto layer.
- **Encoding.** Base64url, no padding (`.rstrip("=")` on encode, re-pad on decode). Header-safe.

A session token forged by a third party will fail decryption (no key). A session token presented by a different principal than the one who opened it will fail AAD verification (cross-principal replay protection). A session token presented to a different worker will pass decryption but fail `server_id` comparison (no shared registry — that's a deliberate design choice for v1).

### 3.1 Principal binding (normative)

**Sessions are scoped to the principal that opened them.** An implementation MUST bind the session token to the request's authenticated identity through the AAD above, and a token presented under a different identity MUST fail — as `session_lost`, indistinguishable from a garbage token. Rejection MUST come from the AEAD open itself, not from a comparison performed after a successful decrypt; the registry's stored `principal_key` check is defense-in-depth behind that, not a substitute for it.

This is not a new rule — it is the same binding stream-state tokens have carried since the v4 AAD, and the session token reuses it verbatim. It is stated normatively here because the property is invisible in the happy path: an implementation that dropped the identity tail passes every lifecycle test and leaks sessions across users. `TestSticky::test_cross_principal_replay_rejected` exists to make that failure visible; see §9.1.

**What is bound, and what deliberately is not.** Only `domain` and `principal` feed the AAD. Claims do not. A JWT may refresh, `exp` / `jti` / `scope` may churn, and the bearer credential may rotate entirely without disturbing a live session, because only the stable identity (`sub`, a certificate CN, or whatever the deployment's auth hook resolves) is bound. Implementations MUST NOT extend the AAD with volatile per-request material.

**Consequences implementers should expect.** Because identity is bound for the session's lifetime, a session does not survive a change of identity:

- A session opened anonymously cannot be resumed once the caller authenticates, and vice versa.
- A session does not cross a `domain` change — relevant to deployments that carry tenant context there.
- Deployments that chain authenticators should keep `domain` / `principal` stable across the fallthrough, or callers whose credential type changes mid-session will see `session_lost`.

Every one of these surfaces as a typed `session_lost` error, never as a silent bind to the wrong state, so the worst outcome is a forced reopen (§1, "always-raise on session loss"). Auth-hook implementations should therefore derive `principal` from stable identity material and never from the credential itself.

**Binding is worth exactly as much as the authentication behind it.** On a server with no authentication configured, every request is anonymous, every session shares one `principal_key` bucket, and the AAD tail is identical for all callers — token secrecy is then the only control. That is a reason to configure authentication on sticky deployments, not a reason to weaken the binding.

## 4. Runtime API contract

Methods on `CallContext`:

| Member | Contract |
|---|---|
| `ctx.session: object | None` | The state object bound by a previous `open_session`, or `None`. Implementations MUST return the same object identity across calls within the same session. |
| `ctx.session_id: str | None` | The 24-char hex session ID (12 bytes, for logging). |
| `ctx.open_session(state, ttl=None)` | Registers `state` in the per-worker registry, schedules `VGI-Session` mint on the response. Raises `RuntimeError` if (1) the transport doesn't have sticky machinery installed, (2) the request lacks `VGI-Session-Accept: true`, or (3) a session is already active for this request. |
| `ctx.close_session()` | Invokes `state.close()` if defined, removes the registry entry, schedules `VGI-Session-Close: true` on the response. Idempotent. |

## 5. Per-session concurrency

The framework's registry holds a `threading.RLock` per session entry. The HTTP middleware acquires the lock before dispatching a call that resolves to that session and releases it after the response is generated. The contract:

- **Same-session calls serialize.** Two concurrent HTTP requests carrying the same `VGI-Session` token run sequentially through the worker. The second request blocks at the middleware until the first completes.
- **Different-session calls run in parallel.** The registry lock protects dict mutation only; it is never held during dispatch.

This matches gRPC's session semantics and is the safe default for non-thread-safe handles (DB cursors, model contexts, file objects). An app that wants parallel access to one session is responsible for thread-safety in the state object itself.

A future PR may add a `concurrent=True` flag on `open_session` to skip the lock; out of scope for v1.

## 6. Typed errors

| `error_kind` | Class | Wire shape |
|---|---|---|
| `session_lost` | `vgi_rpc.rpc.SessionLostError` | 200 + `X-VGI-RPC-Error: true` + EXCEPTION-level batch with `vgi_rpc.error_kind = "session_lost"` metadata. |
| `server_draining` | `vgi_rpc.rpc.ServerDrainingError` | Same shape with `vgi_rpc.error_kind = "server_draining"`. |

Reasons for `session_lost`: token decode failure, AAD mismatch (cross-principal replay), `server_id` mismatch (wrong worker), registry miss (entry never existed or aged out), TTL expiry. A `server_draining` error is raised only on `ctx.open_session` while the server is in drain mode; existing-session calls continue to serve.

Cross-language clients MUST recognize `error_kind="session_lost"` and `error_kind="server_draining"` from the batch metadata and surface them as typed exceptions, even if those clients don't issue session opens themselves. This is the minimum requirement for inter-op against a sticky-enabled Python server.

## 7. Graceful drain

The framework exposes a per-worker drain flag via the operator-facing :func:`vgi_rpc.http.drain_handle` helper:

```python
from vgi_rpc.http import drain_handle, make_wsgi_app

app = make_wsgi_app(server, enable_sticky=True)
handle = drain_handle(app)  # returns None when sticky is disabled
if handle is not None:
    handle.drain()       # flip the drain flag
    # ... wait for in-flight sessions to complete ...
    handle.shutdown()    # invoke state.close() on every live session
```

While the flag is set:

- `ctx.open_session` raises `ServerDrainingError`. Existing-session calls continue to serve until TTL or explicit close.
- `handle.shutdown()` invokes `state.close()` on every live registry entry — used by operators when their grace period elapses.

`serve_http` ships with a built-in graceful-shutdown handler that wires SIGTERM / SIGINT to this flow automatically. Pass `drain_grace_seconds=30.0` (default) to control how long the framework waits between flipping the flag and forcibly exiting. A second signal during grace skips the wait and exits immediately.

For pre-fork servers (gunicorn, uwsgi) operators wire their own hook against `drain_handle(app)`:

```python
# gunicorn config (gunicorn.conf.py)
import time
from vgi_rpc.http import drain_handle

def worker_exit(server, worker):
    """gunicorn calls this when a worker is being retired."""
    handle = drain_handle(worker.app.callable)  # the WSGI app
    if handle is not None:
        handle.drain()
        time.sleep(30)  # grace period — tune for your workload
        handle.shutdown()
```

The drain flag is per-worker process (it lives in the per-worker `_SessionRegistry`); pre-fork deployments effectively get one drain cycle per worker.

## 8. Crash semantics

A process crash (SIGKILL, segfault, OOM-kill, hardware failure) does NOT invoke `state.close()`. Sessions are not persistent; the contract is explicit: handles MAY leak external resources on crash. Apps that hold expensive external state SHOULD scope it to the session token's TTL via the underlying system's own expiry (idle-timeout on DB connections, GC on file descriptors), not rely on `state.close()` as the sole cleanup mechanism.

## 9. Conformance

Run the canonical sticky conformance group:

```bash
vgi-rpc-test --url http://<server> --filter "Sticky::*"
```

The group is capability-gated: servers without `VGI-Sticky-Enabled: true` skip every test in the group cleanly. The Python implementation passes all tests; cross-language ports that wire up sticky support must pass them too. See [`docs/porting-guide.md`](porting-guide.md) for the full porting checklist.

### 9.1 Optional failure-path fixtures

Most of the group runs against a single sticky worker. Three failure-path tests need a worker the default fixture cannot stand in for, so each looks up an **optional** fixture and skips when the runner doesn't supply it. A port claiming full sticky conformance should supply all three.

| Fixture | Shape | Worker configuration | Test |
|---|---|---|---|
| `conformance_http_sticky_short_ttl_port` | `int` | Sticky, `VGI-Sticky-Default-TTL` ≤ 5 seconds | `test_expired_session_surfaces_session_lost` |
| `conformance_http_sticky_peer_ports` | `(int, int)` | Two sticky workers sharing **one** AEAD token key | `test_token_from_other_worker_rejected` |
| `conformance_http_sticky_auth_port` | `int` | Sticky, authenticates the principal named in the `X-Conformance-Principal` request header (absent header ⇒ anonymous) | `test_cross_principal_replay_rejected` |

The shared key on the peer pair is deliberate: with per-process keys the peer would reject the token at decryption and the test would pass without ever exercising the `server_id` comparison. `X-Conformance-Principal` is a fixture convention, not part of the wire protocol — it exists only so the suite can present one worker's token under two identities. Ports are free to authenticate however they like as long as the two identities are distinguishable, but the header name is fixed, because the suite sends it.

These fixtures are currently optional so ports can adopt them incrementally. A port that advertises `VGI-Sticky-Enabled: true` SHOULD supply all three; `conformance_http_sticky_auth_port` in particular is what makes the §3.1 principal binding testable rather than merely asserted, and is expected to become required in a future revision.

Each of these tests pairs its rejection with a positive control (the owning worker, or the owning principal, resuming the same token successfully), so a fixture that mints unusable tokens fails rather than passing green.

## 10. Out of scope

- **Cookie emission.** AWS ALB application-based stickiness and CloudFront sticky sessions both require a cookie set by the application. Operators on those platforms can front with Envoy / NGINX (header-hash policies on `VGI-Session`) or switch to NLB (flow-hash). Cookie emission can be added as an additive operator flag in a follow-up without changing the wire surface.
- **Pluggable session store.** Sessions hold live Python objects in-process. Redis-style external stores are explicitly excluded — they don't work for the cursor/handle pattern the feature is designed for, and the additional persistence story would compete with the well-defined "TTL eviction + crash = state lost" contract.
