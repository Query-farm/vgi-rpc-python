# vgi-rpc Access Log Specification

This document is the cross-language contract for the access log emitted by every conformant vgi-rpc server implementation. The Python implementation in this repository is the reference; other-language implementations (Go, Rust, JS, Java, …) MUST emit records that satisfy this spec so a single tool — `vgi-rpc-test --access-log` — can validate them all.

The machine-checkable form of this spec is [`vgi_rpc/access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json) (JSON Schema 2020-12). Where this document and the schema disagree, **the schema wins**.

## 1. Stream

- **Logger / channel name**: `vgi_rpc.access`. The string `"vgi_rpc.access"` MUST appear in each record's `logger` field.
- **Severity**: every record is emitted at `INFO`. Implementations that don't carry severity SHOULD still set `"level": "INFO"` in each record.
- **Encoding**: one record per RPC call, one record per line, UTF-8 encoded JSON, no trailing comma. Records are independently parseable; the stream as a whole is JSON-Lines (NDJSON).
- **Sink**: implementations MUST accept a `--access-log <path>` command-line flag and write every record to that path. When the flag is absent the access log is implementation-defined (typically suppressed).
- **One record per call**: emit on completion (success, error, or cancellation). Never emit a partial record. Never emit more than one record for the same call. Stream calls produce one record per `init` and one per `exchange`/`produce` continuation.

## 2. Top-level shape

Every record is a JSON object with these keys:

| Key | Type | Required | Notes |
|---|---|---|---|
| `timestamp` | string | yes | RFC 3339 UTC, millisecond precision. Exact pattern: `YYYY-MM-DDTHH:MM:SS.sssZ` (e.g. `2026-04-26T15:30:45.123Z`). |
| `level` | string | yes | Always `"INFO"`. |
| `logger` | string | yes | Always `"vgi_rpc.access"`. |
| `message` | string | yes | Free-form summary, e.g. `"<protocol>.<method> ok"`. Not parsed by tooling — assertions go on structured fields. |

All fields below appear at the top level of the same object — they are NOT nested under `extra`, `data`, or any other envelope.

## 3. Always-required structured fields

These fields MUST appear in every record, regardless of method type or status.

| Field | Type | Notes |
|---|---|---|
| `server_id` | string | Stable identifier for the server instance (12-char hex by default). Same value attached to every record from the same process lifetime. |
| `protocol` | string | The Protocol class name being served, e.g. `"ConformanceService"`. |
| `protocol_hash` | string | SHA-256 hex digest of the canonical `__describe__` payload. 64 lowercase hex characters. Stable across processes/builds that expose the same Protocol; changes whenever any wire-relevant detail of the Protocol changes. Use as the registry key when decoding archived records. |
| `method` | string | The RPC method name. For built-ins, the leading double-underscore is preserved (e.g. `"__describe__"`). |
| `method_type` | string | One of `"unary"` or `"stream"`. |
| `principal` | string | Authenticated principal, or empty string when anonymous. |
| `auth_domain` | string | Auth scheme/realm, or empty string when anonymous. |
| `authenticated` | boolean | `true` iff the call was authenticated. |
| `remote_addr` | string | IP:port for HTTP transport, empty string for pipe/subprocess/Unix-socket. |
| `duration_ms` | number | Wall-clock dispatch duration in milliseconds, rounded to 2 decimal places. |
| `status` | string | One of `"ok"` or `"error"`. `"error"` is used for any failure, including cancellation by the client. |
| `error_type` | string | Python-style exception class name on error (e.g. `"ValueError"`, `"RpcError"`). Empty string when `status == "ok"`. Implementations in non-Python languages SHOULD map their own error types to a stable, descriptive string and document the mapping. |

## 4. Conditional fields

These fields appear when their condition is met and are absent (key not present) otherwise.

### 4.1 Errors

| Field | Type | Condition |
|---|---|---|
| `error_message` | string | Required and non-empty when `status == "error"`. No length cap. The full server-side message is reported. |

### 4.2 Stream lifecycle

| Field | Type | Condition |
|---|---|---|
| `stream_id` | string | Required when `method_type == "stream"`. UUID hex (32 lowercase hex chars, no dashes). MUST be the same value across the `init` record and every continuation record of the same stream call. |
| `cancelled` | boolean | Present and `true` when the stream was cancelled by the client. Absent on non-stream calls and on streams that completed normally or errored without cancellation. |

### 4.3 Request payload

| Field | Type | Condition |
|---|---|---|
| `request_data` | string | Required on every `unary` record AND every stream `init` record. Absent on stream continuations. The value is base64 (RFC 4648, padding required) of a self-contained Arrow IPC stream (one schema message followed by one record batch message). Round-trip equivalence — not byte equivalence — is the conformance test: the decoded bytes MUST decode through `pyarrow.ipc.open_stream(...)` to yield a `RecordBatch` whose schema and column data match the original request. The Python reference implementation uses `pyarrow.RecordBatch.serialize()`. Other-language implementations MAY use any encoding their Arrow library produces as long as the round-trip property holds. |

### 4.4 HTTP transport

These fields appear on HTTP transports only.

| Field | Type | Condition |
|---|---|---|
| `http_status` | integer | The HTTP response status code (e.g. 200, 401, 404, 500). |
| `request_id` | string | Per-request correlation ID. Implementations SHOULD propagate inbound `X-Request-ID` if present, otherwise mint a UUID. |
| `trace_id` | string | W3C trace ID, 32 lowercase hex characters, of the span this call ran under. Present when the server participates in a trace. This is the join key to the surrounding distributed trace — `request_id` only correlates records within one service, so without this a log line and the span describing the same call cannot be matched. Read it from whatever span is current rather than from anything the framework threads through, so a record correlates with an application-opened span as readily as a framework-opened one. |
| `span_id` | string | W3C span ID, 16 lowercase hex characters. Emitted together with `trace_id` — both or neither. |
| `request_state` | string | Base64 of the **decrypted** state bytes, in the server's own state encoding, on stream continuations. Absent on `init`. The on-wire token is an opaque AEAD ciphertext; servers MUST log the plaintext state bytes (or an envelope thereof for union-tagged states), not the ciphertext, so log readers can decode the state without holding the server's `token_key`. **The encoding itself is not specified** — see the note below. |
| `response_state` | string | Base64 of the **decrypted** outbound state bytes, in the server's own state encoding, on stream `init` and continuations that produce a continuation token. Absent on the terminal continuation that closes the stream and on unary calls. Symmetric with `request_state`: log readers see plaintext, not the AEAD ciphertext that travels on the wire. |

> **The state encoding is server-defined and deliberately outside this spec.**
> A stream state's plaintext encoding is a per-port choice — Go uses gob, Rust
> bincode, Java CBOR, TypeScript JSON, and the Python reference uses a compact
> msgpack codec for flat states, falling back to Arrow IPC only for states
> holding Arrow values. Tokens are not expected to round-trip across ports, so
> nothing requires them to agree.
>
> Earlier revisions of this document described these two fields as "the Arrow
> IPC payload". That was never satisfiable outside one implementation, and
> since the compact codec landed it is not satisfiable inside it either: a
> reader reaching for `pyarrow.ipc.open_stream` gets `ArrowInvalid` on the
> common case. The schema only ever constrained the fields to base64, so no
> implementation failed — the wording was simply wrong, and would have sent a
> log reader down a dead end.
>
> What **is** normative is the property the fields exist for: the value is the
> decrypted plaintext, never the AEAD ciphertext that travels on the wire, so
> a reader can decode state without holding the server's `token_key`. Decoding
> it requires knowing which server wrote the record. Do not assume Arrow.

### 4.5 Server identity & auth

| Field | Type | Condition |
|---|---|---|
| `server_version` | string | Present when the implementation knows its server *build* version (e.g. set from a build constant). |
| `claims` | object | Present and non-empty when `authenticated == true` and the auth provider produced claims. JSON-serializable; nested values follow JSON conventions. **Emitters MUST redact sensitive claim values** — see below. |
| `peer_identity_status` | string | Present when peer providers ran. Sorted comma-separated `provider:status` values; contains no subject/profile/capability data. |
| `peer_identity_sources` | string | Present when evidence is available. Sorted comma-separated `provider:evidence_source:assurance` values; contains no subject keys, certificates, or capabilities. |

An access log outlives the token it describes by months or years, and is shipped to systems chosen for searchability rather than for holding personal data. Standard OIDC claims (`email`, `phone_number`, `given_name`, …) and credential-shaped ones (`*_token`, `*_key`, `password`) MUST NOT reach it verbatim.

Redaction is **key-based**: match on the name a value arrived under, never on its content. A claim called `context` holding an email address is not caught, and cannot be without guessing at free text — a boundary worth stating rather than pretending to exceed.

Peer evidence logging is allowlist-based rather than key-redacted. Emit only
provider outcome, evidence source, and assurance. Raw capabilities, LocalAPI
tokens, user profile fields, certificate bodies, stable subject keys, and proxy
credentials MUST NOT appear in these fields.

Replace values, do not drop keys. *Which* claims a credential carried is a question an audit log exists to answer; what they contained is not. The Python reference substitutes `"[redacted]"`, exposes the policy as `vgi_rpc.logging_utils.redact_claims`, and allows replacement via `set_claim_redactor` (with `no_redaction` for services that own their logs end to end). A redactor that raises MUST fail **closed** — drop the claims entirely rather than emit them unredacted.

### 4.6 Call statistics

These six fields appear together. Implementations MAY omit the entire group, but if any one of them is present then ALL six MUST be present. They count work done while serving the call (the input/output direction is from the server's perspective: input = received from client, output = sent to client).

| Field | Type | Condition |
|---|---|---|
| `input_batches` | integer | Number of Arrow record batches received. |
| `output_batches` | integer | Number of Arrow record batches sent. |
| `input_rows` | integer | Total rows across all input batches. |
| `output_rows` | integer | Total rows across all output batches. |
| `input_bytes` | integer | Sum of `RecordBatch.nbytes` across input batches (uncompressed in-memory size). |
| `output_bytes` | integer | Sum of `RecordBatch.nbytes` across output batches. |

### 4.7 Sticky session lifecycle

When the request flows through a sticky-enabled HTTP transport (see [`sticky-sessions-spec.md`](sticky-sessions-spec.md)), the access record carries two additional fields describing the session lifecycle.

| Field | Type | Condition |
|---|---|---|
| `session_action` | enum | One of `"none"` / `"open"` / `"resume"` / `"close"`. `"none"` = the request flowed through sticky middleware but neither carried a session token nor opened one (e.g. a unary call from a non-`with_session_token()` caller). `"open"` = the method called `ctx.open_session(...)`. `"resume"` = a valid `VGI-Session` token resolved to a live registry entry. `"close"` = the method called `ctx.close_session()`. Absent for non-sticky servers. |
| `session_id` | string | Present when the request touched a session — i.e. when `session_action` is `"open"` / `"resume"` / `"close"`. Format: 12-byte hex, exactly 24 characters. Absent on `"none"` and on non-sticky servers. The id is stable across the open / resume / close lifecycle records for a given session. |

**Gaps**: middleware-short-circuit cases (token validation failed; `server_id` mismatch; registry miss for an apparently-valid token) currently do NOT produce access-log records. The middleware emits a typed `SessionLostError` response without invoking dispatch, and the access-log emitter lives in the dispatch path. Operators monitoring for misroutes should rely on the typed error surface on the wire instead. Adding short-circuit access-log records is a documented follow-up.

## 5. Method-type rules

All conditional behavior is keyed off `method_type` (and, for streams, whether the record is an init or continuation — distinguishable by the presence of `request_data`). **Rules MUST NOT be keyed off method names.** Method names are application-specific; framework conformance applies uniformly.

| Rule | Trigger |
|---|---|
| `request_data` present | `method_type == "unary"` OR (`method_type == "stream"` AND record is the init record). |
| `request_data` absent | Stream continuations. |
| `stream_id` present | `method_type == "stream"`. |
| `cancelled` present | Stream call cancelled by client. |
| `error_message` non-empty | `status == "error"`. |

### 4.8 Egress accounting

Three byte figures answer three different questions, and conflating them is how an egress bill ends up wrong by orders of magnitude. The reference implementation measured only the middle pair until these were added.

| Field | Type | Condition |
|---|---|---|
| `request_bytes` | integer | On-wire size of the request body as received, **before** decompression. What the peer actually sent. |
| `response_bytes` | integer | On-wire size of the response body as sent, **after** compression. Absent when the size cannot be known (a streamed response with no content length). |
| `externalized_bytes` | integer | Bytes uploaded to external storage during this call. Absent when nothing was externalised. |

Contrast with §4.6's `input_bytes` / `output_bytes`, which measure **logical Arrow buffer sizes** — what the worker processed. Those are unaffected by compression and exclude externalised payloads entirely, so they are the wrong number for anything that costs money and the right number for capacity work.

The gap is not marginal. A compressible 200 KB result measured 200,008 logical bytes and 183 bytes on the wire in the reference implementation — a factor of about 1,000. In the other direction, a call that externalises a 10 GB batch leaves a pointer batch of a few hundred bytes in the HTTP body; without `externalized_bytes` the 10 GB is invisible.

**Implementation note.** `response_bytes` cannot be measured where the other fields are. A handler knows what it produced, but response compression runs afterwards, so a record emitted at handler time can only ever report the uncompressed body. An emitter MUST therefore defer emission until the final body exists — in the Python reference, a middleware installs a per-request sink, handlers append to it, and the middleware emits after compression has run. The cost is that a crash between handler and response loses that request's records; the alternative is a permanently wrong number.

## 5b. Truncation

Downstream log shippers (Vector's `file` source, Fluent Bit's `tail` input) impose a per-line ceiling — Vector defaults to 100 KiB and Fluent Bit's `Buffer_Max_Size` defaults to 256 KiB. Lines longer than the shipper's ceiling are silently dropped.

To stay compatible, an emitter MAY enforce a per-record byte cap. When it does, it MUST shed fields in this order and signal the truncation via top-level keys:

1. Drop `request_data` and add `original_request_bytes` (integer, character length of the dropped field). Set `truncated: true`.
2. Replace `claims` with `{}`. Keep `truncated: true`.
3. If the record still exceeds the cap, emit a sentinel form: keep all always-required envelope fields plus `error_message` (when `status == "error"`) and set `truncated: "record_too_large"`. All other optional fields are dropped.

`error_message` MUST NOT be truncated — operators rely on the full server-side message for debugging. The Python reference implementation uses a default cap of 1 048 576 bytes (1 MiB), configurable via `--access-log-max-record-bytes` or the env var `VGI_RPC_ACCESS_LOG_MAX_RECORD_BYTES`. Pair the cap with shipper configs that raise their per-line limits to match (Vector's `max_line_bytes`, Fluent Bit's `Buffer_Max_Size`).

| Field | Type | Condition |
|---|---|---|
| `truncated` | `true`, `"record_too_large"`, or `"payload_omitted"` | Present iff the record does not carry everything it otherwise would. `true` = at least one optional field dropped to fit the size cap. `"record_too_large"` = sentinel form; most optional fields dropped. `"payload_omitted"` = **nothing was lost to a cap** — the emitter is simply not logging request payloads at this level. |
| `original_request_bytes` | integer | Present when `request_data` was dropped due to truncation. Reports the character length of the dropped string. |

A `unary` record carrying `truncated` is NOT required to also carry `request_data` — the schema relaxes that rule whichever marker is present.

`"payload_omitted"` exists because the other two values were carrying two incompatible meanings. A normally-configured server does not log payloads at INFO, so it set `truncated: true` on essentially every unary record — leaving a consumer scanning for real data loss with nothing to filter on. Emitters that gate payload logging by level MUST use `"payload_omitted"` for that case and reserve `true` for genuine size-driven shedding. Consumers MUST treat the two differently.

## 5bb. Sampling

An emitter MAY log only a fraction of calls. Sampling is optional; an emitter that does not implement it simply never emits `sample_rate`. An emitter that does MUST hold to three rules, each of which is the difference between a sampler that helps and one that quietly costs someone an incident.

1. **Never sample errors.** A rate below 1 exists because successful calls are repetitive, which is exactly what failures are not. `status == "error"` records MUST always be emitted regardless of rate. A consumer must be able to read a fall in error count as a fix landing, not as the dice going the other way.
2. **Decide deterministically, per call — not per record.** The decision MUST be a function of a stable identifier for the call, keyed on `stream_id` when present and `request_id` otherwise, so that every record of one stream shares its `init`'s fate. Random per-record sampling shreds a multi-record call into fragments indistinguishable from data loss, and the calls most likely to be split are the long streams most worth studying.
3. **Carry the rate in-band.** Every sampled-in record MUST carry `sample_rate`. A consumer counting calls has to divide by it, and a rate discoverable only from a deployment's flags is a rate that gets guessed wrong.

| Field | Type | Condition |
|---|---|---|
| `sample_rate` | number, `0 < r <= 1` | Present iff sampling is active (rate below 1). Absent when the emitter logs everything. Error records MAY lack it even under sampling, since they bypass the decision. |

The Python reference implements this as a `logging.Filter` on the handler — not the logger, so an application's own handlers keep seeing every record — configured by `--access-log-sample` / `VGI_RPC_ACCESS_LOG_SAMPLE`, defaulting to `1.0`. An out-of-range rate fails at startup rather than at the first request, because `100` meaning "100%" would otherwise silently log everything.

## 5bc. Asynchronous emission

An emitter MAY hand records to a background writer so disk latency stays out of the request path. Optional; a synchronous emitter is conformant.

An emitter that does MUST bound the queue and MUST NOT block when it is full. An unbounded queue turns a stalled disk into an OOM; a blocking put reintroduces exactly the latency the thread was meant to remove. Full therefore means drop.

What makes dropping acceptable rather than silent corruption is that it is reported: the next record to get through MUST carry `dropped_records` (integer, count since the last successful enqueue). A log that loses records without saying so is worse than a slow one, because a consumer cannot tell a quiet period from a lossy one.

| Field | Type | Condition |
|---|---|---|
| `dropped_records` | integer | Present on the first record enqueued after one or more were dropped. Reports how many were lost. |

This trades durability: with a synchronous writer, a record on disk means the call completed; with a queue, a crash loses whatever is still in it. That is why the Python reference makes it opt-in (`--access-log-async`, `--access-log-queue-size`, default 10000) rather than the default — right for high throughput, wrong for audit.

## 5c. Encoding & atomicity

- One JSON object per line, terminated by `\n`. UTF-8 encoded. No literal newlines inside field values (the standard `json.dumps` escapes them).
- A single emitter process appending via the stdlib `logging.FileHandler` is thread-safe (the handler holds a lock) and atomic on Linux.
- **Two processes writing to the same access-log file is unsupported.** Concurrent appends from multiple processes can interleave, and concurrent rotation will race. Run one access-log file per process — use `{pid}` and/or `{server_id}` placeholders in the path. The Python reference implementation expands these placeholders in `--access-log` paths automatically.

## 5d. Rotation

Implementations MAY rotate the access log via rename (e.g. `access.jsonl` → `access.jsonl.1`). Both `logging.handlers.RotatingFileHandler` (size-based) and `TimedRotatingFileHandler` (time-based) in Python's stdlib implement this correctly, and Vector and Fluent Bit are designed to follow rename-rotated files. **Do not truncate-in-place** — shippers will lose their read position.

The Python reference implementation exposes:

- `--access-log-max-bytes N` / `VGI_RPC_ACCESS_LOG_MAX_BYTES` — size-based rotation when > 0.
- `--access-log-when STR` / `VGI_RPC_ACCESS_LOG_WHEN` — time-based rotation (e.g. `H`, `D`, `midnight`); mutually exclusive with `--access-log-max-bytes`.
- `--access-log-backup-count N` / `VGI_RPC_ACCESS_LOG_BACKUP_COUNT` — number of rotated files retained (default 5).

## 6. Extra fields

Implementations MAY add fields beyond those defined here. Validators MUST NOT reject records carrying unknown fields (`additionalProperties: true`). Conformance is measured by what the schema requires, not by what it forbids.

To avoid collision with future spec additions, custom fields SHOULD use a vendor prefix (e.g. `acme_request_size`).

## 7. Conformance check

```bash
# Validate any worker's access log against this spec.
vgi-rpc-test --cmd "./my-go-worker" --access-log /tmp/go-worker.log
```

The exit code is `0` if every record passes, `1` if any record fails, `2` if the runner itself errored.

## 8. Reference

- JSON Schema: [`vgi_rpc/access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json)
- Python emitter: `vgi_rpc/rpc/_server.py` (`_emit_access_log`)
- Python JSON formatter: `vgi_rpc/logging_utils.py` (`VgiJsonFormatter`)
- Python validator: `vgi_rpc/access_log_conformance.py`
- Cross-language conformance overview: [`cross-language-conformance.md`](cross-language-conformance.md)
- Reference shipper configs (Vector and Fluent Bit, S3/GCS/Azure): [`log-shipping/`](log-shipping/README.md)
