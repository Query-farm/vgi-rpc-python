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

This is the conformance proof for observability. The full spec is in [`access-log-spec.md`](access-log-spec.md); the schema is [`vgi_rpc/access_log.schema.json`](../vgi_rpc/access_log.schema.json). Highlights:

- One record per RPC call (or per stream init / per stream continuation).
- JSON-Lines (NDJSON), UTF-8.
- 11 always-required fields: `server_id`, `protocol`, `method`, `method_type`, `principal`, `auth_domain`, `authenticated`, `remote_addr`, `duration_ms`, `status`, `error_type` plus the four envelope fields `timestamp` (RFC 3339 UTC ms-precision), `level` (`"INFO"`), `logger` (`"vgi_rpc.access"`), `message`.
- Conditional fields keyed off `method_type` and `status` — never off method names.
- `request_data`: base64 of a self-contained Arrow IPC stream of the request batch. Round-trip equivalence is the test, not byte equivalence — your Arrow library's serialization is fine.

### 4. The conformance service

Your repo must include a runnable conformance worker that registers the [`vgi_rpc.conformance.ConformanceService`](../vgi_rpc/conformance/_protocol.py) protocol. The Python definition is the reference; your port translates each method to native types preserving Arrow schema. Method names, parameter names, and stream-state semantics must match exactly.

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
6. **External-location and shared-memory transports** if you need them.

## Reference implementations

- **Python** (reference): this repository.
- **Go**: [vgi-rpc-go](https://github.com/Query-farm/vgi-rpc-go) — uses Apache Arrow Go and a `DispatchHook` interface for observability. Schema-aligned via `vgirpc.AccessLogHook` and `--access-log <path>` on the conformance worker.

### Known conformance gaps in current ports

- **Go**: `stream_id` is generated per-dispatch rather than stable across the init/continuation records of a single stream call. The schema regex passes; the spec's "stable identifier across continuations" semantic does not yet hold. HTTP-specific fields (`http_status`, `request_state`, `response_state`) are also not yet plumbed. Tracked separately.

## Gotchas

- **Arrow dictionary encoding.** Across language Arrow libraries, the placement of dictionary messages in IPC streams differs. The schema's `request_data` round-trip rule was chosen specifically to absorb this — don't try to byte-match Python.
- **Custom metadata key ordering.** Some Arrow libraries do not preserve insertion order. Test your reader against batches produced by Python.
- **HTTP state-token format.** Tokens are HMAC-signed and opaque to clients but their internal layout MUST match across implementations so a load-balanced HTTP fleet can coexist. See `vgi_rpc/http/server/_state_token.py` for the reference layout.
- **Per-process server identity.** `server_id` is generated once per process lifetime, NOT per call. The same string must appear in every log record from the same instance.
