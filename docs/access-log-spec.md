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
| `request_state` | string | Base64 of the inbound state-token bytes on stream continuations (the bytes the client supplied). Absent on `init`. State-token format is implementation-internal and HMAC-signed; consumers MUST treat the decoded bytes as opaque. |
| `response_state` | string | Base64 of the outbound state-token bytes on stream `init` and continuations that produce a continuation token. Absent on the terminal continuation that closes the stream and on unary calls. |

### 4.5 Server identity & auth

| Field | Type | Condition |
|---|---|---|
| `server_version` | string | Present when the implementation knows its server *build* version (e.g. set from a build constant). |
| `protocol_version` | string | Present when the operator labels the *protocol contract* with a version (separate from `server_version`, which describes the build). Free-form. Use `protocol_hash` for machine comparisons. |
| `claims` | object | Present and non-empty when `authenticated == true` and the auth provider produced claims. JSON-serializable; nested values follow JSON conventions. |

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

## 5. Method-type rules

All conditional behavior is keyed off `method_type` (and, for streams, whether the record is an init or continuation — distinguishable by the presence of `request_data`). **Rules MUST NOT be keyed off method names.** Method names are application-specific; framework conformance applies uniformly.

| Rule | Trigger |
|---|---|
| `request_data` present | `method_type == "unary"` OR (`method_type == "stream"` AND record is the init record). |
| `request_data` absent | Stream continuations. |
| `stream_id` present | `method_type == "stream"`. |
| `cancelled` present | Stream call cancelled by client. |
| `error_message` non-empty | `status == "error"`. |

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
