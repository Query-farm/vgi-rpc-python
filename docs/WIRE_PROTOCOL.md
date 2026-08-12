# vgi-rpc Wire Protocol Specification

**Wire protocol version**: 1
**Status**: Normative
**Audience**: Cross-language implementors (Go, Rust, TypeScript, C++, etc.)
**Reflects**: vgi-rpc 0.42.1

This document specifies the vgi-rpc wire protocol at byte level. A conforming
implementation can interoperate with the Python reference without reading
Python source code. The protocol is transport-agnostic; specific transport
bindings (pipe, HTTP, shared memory) are described in later sections.

---

## 1. Overview & Conventions

vgi-rpc is an RPC framework where:

- **Serialization** uses [Apache Arrow IPC Streaming Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
- **All integers** are little-endian unless stated otherwise.
- **All metadata strings** are UTF-8 encoded.
- **Wire protocol version**: `"1"` (the single ASCII byte `0x31`).
- **Metadata keys and values** in Arrow IPC custom metadata are byte strings. Keys in the `vgi_rpc.*` namespace are framework-reserved.

### Terminology

| Term | Definition |
|------|-----------|
| **IPC stream** | A complete Arrow IPC streaming-format message sequence: schema message, zero or more record batch messages, terminated by an EOS marker. |
| **Batch** | An Arrow `RecordBatch` — zero or more rows conforming to a schema. |
| **Custom metadata** | Per-batch `KeyValueMetadata` attached to individual record batches within an IPC stream (distinct from schema-level metadata). |
| **Zero-row batch** | A batch with `num_rows == 0`. Used for log messages, error signals, pointer batches, and stream-completion markers. |
| **Data batch** | A batch with `num_rows > 0`, or a zero-row batch that lacks log/error metadata keys (e.g., void return). |

---

## 2. Arrow IPC Framing

Each logical message exchange uses one or more **IPC streams** written
sequentially on the same byte stream (pipe, TCP socket, HTTP body, etc.).

An IPC stream consists of:

1. **Schema message** — describes the columns and their Arrow types.
2. **Zero or more RecordBatch messages** — each optionally carrying per-batch custom metadata.
3. **EOS marker** — the 8-byte sequence `0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x00 0x00` (continuation token `0xFFFFFFFF` followed by 4 zero bytes for metadata length).

Multiple IPC streams are written **sequentially** on the same underlying byte
stream. Each reader opens one stream, reads until EOS, and stops. The next
reader picks up immediately after the EOS marker.

Refer to the [Apache Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) for the byte-level encoding of schema messages, record batch messages, dictionary messages, and the encapsulated message format.

---

## 3. Metadata Key Reference

All framework-reserved metadata keys, their wire-format byte representations,
where they appear, and their semantics:

### Request metadata (on the request batch's custom metadata)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.method` | UTF-8 method name | Target RPC method to invoke. **Required.** |
| `vgi_rpc.request_version` | `"1"` (ASCII `0x31`) | Wire protocol version. **Required.** |
| `vgi_rpc.protocol_version` | Canonical semver `MAJOR.MINOR.PATCH` | Application protocol surface version. **Required when the peer Protocol declares one**, absent otherwise. See [Section 13](#protocol-version-negotiation). |
| `vgi_rpc.request_id` | UTF-8 string (16-char hex) | Per-request correlation ID. Optional; if absent, the server generates a new 16-char hex ID. |
| `vgi_rpc.cancel` | `"1"` — presence is the signal | Client-initiated stream cancellation, on a stream input batch. See [Section 9](#client-initiated-cancellation). Optional. |
| `traceparent` | W3C Trace Context string | OpenTelemetry trace propagation. Optional. |
| `tracestate` | W3C Trace Context string | OpenTelemetry trace state. Optional. |
| `vgi_rpc.shm_segment_name` | UTF-8 OS name | Shared memory segment name (session-level). Optional. |
| `vgi_rpc.shm_segment_size` | Decimal integer string | Shared memory segment total size in bytes. Optional. |
| `vgi_rpc.transport.shm` | `"true"` / `"false"` | Client's shared-memory capability, on the `__transport_options__` request. See [Section 15](#15-transport-capability-negotiation-__transport_options__). Optional. |

HTTP stream continuation requests additionally carry
`vgi_rpc.stream_state#b64` and `vgi_rpc.call_state#b64` — see the stream-state
table below.

### Response / log / error metadata (on response batch custom metadata)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.log_level` | One of: `EXCEPTION`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` | Severity level. Present on log and error batches. |
| `vgi_rpc.log_message` | UTF-8 string | Human-readable message text. |
| `vgi_rpc.log_extra` | JSON string | Additional structured data. Optional. |
| `vgi_rpc.error_kind` | UTF-8 token (open set) | Stable machine-readable error category on EXCEPTION batches. See [Section 8](#error-kinds). Optional. |
| `vgi_rpc.server_id` | UTF-8 string (12-char hex) | Server instance identifier for distributed tracing. |
| `vgi_rpc.request_id` | UTF-8 string | Echoed request correlation ID. |
| `vgi_rpc.transport.shm` | `"true"` / `"false"` | Server's shared-memory capability, on the `__transport_options__` response. See [Section 15](#15-transport-capability-negotiation-__transport_options__). |

### Stream state (HTTP transport)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.stream_state#b64` | Base64-encoded binary (signed token) | Serialized stream state for stateless HTTP exchanges — the per-turn *cursor* when the server splits its state. The `#b64` suffix signals that the value is base64-encoded binary data. |
| `vgi_rpc.call_state#b64` | Base64-encoded binary (signed token) | Optional. The stream's *call state* — the half fixed for the life of the call. Minted once by `/init`, never re-issued, and echoed by the client on every subsequent request. Absent from servers that keep everything in the cursor. |

### Shared memory pointer batch metadata

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.shm_offset` | Decimal integer string | Absolute byte offset in the SHM segment. |
| `vgi_rpc.shm_length` | Decimal integer string | Number of bytes of the serialized batch. |
| `vgi_rpc.shm_source` | UTF-8 SHM segment name | Provenance indicator on resolved batches (diagnostics). |

### External storage pointer batch metadata

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.location` | UTF-8 URL | URL to fetch the externalized batch data. |
| `vgi_rpc.location.sha256` | UTF-8 hex string | SHA-256 of the uploaded payload **before** compression. Optional; when present the reader MUST verify it after fetching. |
| `vgi_rpc.location.fetch_ms` | Decimal float string (e.g. `"42.3"`) | Fetch duration in milliseconds (diagnostics, on resolved batches). |
| `vgi_rpc.location.source` | UTF-8 URL | Original fetch URL (diagnostics, on resolved batches). |

### Introspection batch metadata (on `__describe__` response batch `custom_metadata`)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.protocol_name` | UTF-8 string | Protocol class name. |
| `vgi_rpc.request_version` | `"1"` | Wire protocol version. |
| `vgi_rpc.describe_version` | `"4"` | Introspection format version. |
| `vgi_rpc.protocol_hash` | UTF-8 hex string | SHA-256 digest over the canonical describe payload. |
| `vgi_rpc.protocol_version` | Canonical semver | Application protocol surface version. Present only when the Protocol declares one. |
| `vgi_rpc.server_id` | UTF-8 string | Server instance identifier. |

---

## 4. Type Mapping

RPC method parameters and return values are serialized as Arrow columns.
The following table defines the canonical mapping from abstract types to
Arrow types. Cross-language implementations MUST use these Arrow types
for interoperability.

| Abstract type | Arrow type | Serialization notes |
|--------------|-----------|-------------------|
| `string` | `utf8` | UTF-8 encoded. |
| `bytes` / `binary` | `binary` | Raw byte sequence. |
| `int` / `integer` | `int64` | 64-bit signed integer. |
| `float` / `double` | `float64` | IEEE 754 double precision. |
| `bool` | `bool` | — |
| `list[T]` | `list(T)` | Recursive. |
| `dict[K, V]` / `map` | `map(K, V)` | Serialized as list of `(key, value)` tuples. Deserialized back to map/dict. |
| `frozenset[T]` / `set[T]` | `list(T)` | Serialized as list (order undefined). Deserialized back to set. |
| `enum` | `dictionary(int16, utf8)` | Serialized as the enum **member name** (string). Deserialized by name lookup. |
| `optional[T]` / `T?` | Same as `T`, but with `nullable = true` on the Arrow field. | `null` represents the absent value. |
| `dataclass` (nested) | `binary` | Serialized as a complete Arrow IPC stream (schema + 1-row batch + EOS) in a `binary` column. See note below. |

> **Nested dataclass type context**: This `binary` mapping applies at the
> **RPC method parameter/return level** — each dataclass parameter or return
> value is a binary blob containing a serialized IPC stream. Within that
> IPC stream, the dataclass's own Arrow schema uses the same type mapping
> for primitive fields (string→utf8, int→int64, etc.), but sub-dataclass
> fields use Arrow `struct` type (not `binary`), since they are embedded
> inline rather than serialized as separate IPC streams.

### Serialization transforms

When writing a value to an Arrow column:

- **Enum** → write the member's **name** as a UTF-8 string (not its value).
- **dict** → convert to a list of `(key, value)` tuples, then write as `map(K, V)`.
- **frozenset/set** → convert to a list, then write as `list(T)`.
- **Nested dataclass** → serialize to Arrow IPC bytes, write as `binary`.

When reading:

- **Enum** → look up the string by member **name** (`Enum["NAME"]`). Name-based lookup is the normative wire format. (The Python reference implementation also supports a value-based fallback internally for nested dataclass fields, but cross-language implementations need only implement name-based lookup.)
- **map** → convert list of tuples back to dict.
- **list (when target is set)** → convert to frozenset/set.
- **binary (when target is dataclass)** → deserialize from Arrow IPC bytes.

---

## 5. Request Batch Format

Every RPC request is a **single IPC stream** containing exactly **one batch
with one row**:

```
IPC Stream:
  Schema message:
    - One field per method parameter, named after the parameter
    - Field types per the type mapping (Section 4)
    - Optional parameters have nullable = true
  RecordBatch message:
    - Exactly 1 row
    - custom_metadata:
        vgi_rpc.method = "<method_name>"       (REQUIRED)
        vgi_rpc.request_version = "1"          (REQUIRED)
        vgi_rpc.shm_segment_name = "<name>"    (optional, SHM transport)
        vgi_rpc.shm_segment_size = "<size>"    (optional, SHM transport)
        traceparent = "<W3C trace context>"    (optional)
        tracestate = "<W3C trace state>"       (optional)
  EOS marker
```

For methods with no parameters, the schema has zero fields and the batch
has one row with zero columns.

> **Row count validation**: The server only enforces `num_rows == 1` when
> the schema has one or more fields. For zero-field (parameterless) methods,
> the server accepts batches with any row count (including 0). Conforming
> clients SHOULD send 1 row for consistency.

### Worked example

Given an RPC method `add(a: float, b: float) -> float`:

**Schema**:
```
Field 0: name="a", type=float64, nullable=false
Field 1: name="b", type=float64, nullable=false
```

**Batch** (1 row, calling `add(a=1.0, b=2.0)`):
```
Column "a": [1.0]
Column "b": [2.0]
custom_metadata: {
  "vgi_rpc.method": "add",
  "vgi_rpc.request_version": "1"
}
```

**Default values**: When a parameter has a default and the caller omits it,
the client merges the default into the kwargs before serialization. The server
sees a complete row in all cases.

---

## 6. Response Format (Unary)

A unary response is a **single IPC stream** on the result schema:

```
IPC Stream:
  Schema message:
    - For methods returning a value: single field named "result"
    - For void methods (-> None): zero fields (empty schema)
  0..N log batches (zero-row, with log metadata — see Section 8)
  1 result or error batch:
    - Result: 1-row batch with the return value in column "result"
    - Void: 0-row batch on empty schema
    - Error: 0-row batch with EXCEPTION-level log metadata (see Section 8)
  EOS marker
```

Log batches MUST appear **before** the result/error batch. They share the
same schema as the result batch (the zero-row log batches conform to the
response stream's schema).

### Void return

When the method has no return value (`-> None`), the response schema is
empty (`pa.schema([])`) and the result batch has zero rows and zero columns.

---

## 7. Batch Classification Algorithm

When receiving any batch from a response stream, classify it using this
decision tree:

```
receive(batch, custom_metadata):

  IF custom_metadata is NULL:
    → DATA batch

  IF batch.num_rows > 0:
    → DATA batch

  // At this point: num_rows == 0 AND custom_metadata exists

  IF custom_metadata contains "vgi_rpc.log_level"
     AND custom_metadata contains "vgi_rpc.log_message":

    level = custom_metadata["vgi_rpc.log_level"]

    IF level == "EXCEPTION":
      → ERROR batch → raise RpcError (see Section 8)

    ELSE:
      → LOG batch → deliver to on_log callback

  IF custom_metadata contains "vgi_rpc.location":
    → EXTERNAL POINTER batch → resolve via URL fetch (see Section 12)

  IF custom_metadata contains "vgi_rpc.shm_offset":
    → SHM POINTER batch → resolve via shared memory (see Section 11)

  IF custom_metadata contains "vgi_rpc.stream_state#b64":
    → STATE TOKEN batch → stream continuation (see Section 10)

  // Zero-row batch with unrecognized metadata
  → DATA batch (e.g., void return, stream-finish marker)
```

> **Note**: Log-level keys take priority. A zero-row batch that has both
> `vgi_rpc.log_level` and `vgi_rpc.shm_offset` (or `vgi_rpc.location`) is
> classified as a log batch, not a pointer. This is by design — pointer
> detection explicitly excludes batches with log-level keys. A batch cannot
> be both an external pointer and an SHM pointer simultaneously, so the
> check order between those two does not matter functionally.

---

## 8. Log & Error Batch Format

### Log batches

A log batch is a **zero-row batch** on the response stream's schema, with
the following custom metadata keys:

| Key | Required | Value |
|-----|----------|-------|
| `vgi_rpc.log_level` | Yes | One of: `EXCEPTION`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` |
| `vgi_rpc.log_message` | Yes | Human-readable message text (UTF-8) |
| `vgi_rpc.log_extra` | No | JSON object with additional structured data |
| `vgi_rpc.error_kind` | No | Stable error category; EXCEPTION batches only (see below) |
| `vgi_rpc.server_id` | No | Server instance identifier |
| `vgi_rpc.request_id` | No | Request correlation ID |

### Error batches (EXCEPTION level)

When `vgi_rpc.log_level` is `"EXCEPTION"`, the batch represents a server-side
error. The client MUST raise/throw an error with the following fields
extracted from the metadata:

- **error_type**: `log_extra.exception_type` (string) or the level string `"EXCEPTION"` as fallback.
- **error_message**: `vgi_rpc.log_message` value.
- **remote_traceback**: `log_extra.traceback` (string) or empty string.
- **request_id**: `vgi_rpc.request_id` value or empty string.
- **error_kind**: `vgi_rpc.error_kind` value, when present.

### Error kinds

`vgi_rpc.error_kind` carries a stable identifier for errors a *client* is
expected to branch on, so callers pattern-match a token instead of
substring-searching a human-readable message. It is emitted as a top-level
metadata key, mirroring `log_extra.error_kind` — a reader may take either, but
the top-level key means the JSON blob need not be parsed to dispatch.

The set is **open**: a client MUST treat an unrecognised value as an
unclassified error rather than rejecting the batch. Well-known values:

| Value | Meaning |
|-------|---------|
| `method_not_implemented` | The server has no handler for the requested method (old server vs. new client, or a method that was removed). The intended signal for capability detection with fallback. |
| `protocol_version_mismatch` | The client's `vgi_rpc.protocol_version` is incompatible with the server's (see [Section 13](#protocol-version-negotiation)). |
| `session_lost` | An HTTP sticky-session token could not be honoured — expired, evicted, misrouted, or presented under a different principal (see [Section 17](#17-sticky-sessions-http-optional)). |
| `server_draining` | The server is shutting down and refuses new sticky-session opens. |

A batch carrying `error_kind` is otherwise an ordinary EXCEPTION batch: the
key adds classification and removes nothing. Implementations that do not emit
it remain conformant; clients simply lose the ability to branch.

### `log_extra` JSON structure for EXCEPTION

```json
{
  "exception_type": "ValueError",
  "exception_message": "invalid input",
  "traceback": "Traceback (most recent call last):\n  ...",
  "frames": [
    {
      "file": "/path/to/module.py",
      "line": 42,
      "function": "my_method",
      "code": "raise ValueError('invalid input')"
    }
  ],
  "cause": "Traceback ... (optional, from __cause__)",
  "context": "Traceback ... (optional, from __context__)"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `exception_type` | string | Exception class name. |
| `exception_message` | string | `str(exception)`. |
| `traceback` | string | Formatted traceback. Truncated at 16,000 characters with `"\n… <traceback truncated>"` suffix. |
| `frames` | array of objects | Last 5 stack frames (most recent at end). |
| `frames[].file` | string | Source file path. |
| `frames[].line` | integer | Line number. |
| `frames[].function` | string | Function/method name. |
| `frames[].code` | string or null | Source code at that line. |
| `cause` | string (optional) | Formatted `__cause__` traceback. Truncated at 16,000 chars. |
| `context` | string (optional) | Formatted `__context__` traceback (only when not suppressed). Truncated at 16,000 chars. |

### Non-exception log batches

For levels other than `EXCEPTION`, the `log_extra` JSON structure is
freeform — it contains whatever key-value pairs the server method attached.
Clients should deliver these to the `on_log` callback without attempting to
parse them as error structures.

---

## 9. Stream Protocol (Pipe / Subprocess Transport)

Streaming methods use a multi-phase exchange over a bidirectional byte
stream (two pipes: client→server and server→client).

### Phase 1: Request parameters

Identical to a unary request (Section 5):

```
Client → Server:  IPC stream (params_schema, 1 request row, EOS)
```

### Phase 1.5: Optional header stream

Only present when the stream method declares a header type. Sent by the
server immediately after reading the request, before the main data exchange.

```
Server → Client:  IPC stream (header_schema, 0..N log batches, 1 header row, EOS)
```

The header is a single-row batch containing serialized header data. If the
method does not declare a header type, this phase is skipped entirely.

If the server encounters an error during method initialization, it writes an
error stream (with EXCEPTION-level metadata) **on the empty schema** in place
of the header stream. The empty schema is used because the header schema may
not be available when the error occurs (e.g., the method raised before
returning a `Stream` object). Clients MUST be prepared to receive an
empty-schema error stream where a header stream was expected.

### Phase 2: Lockstep data exchange

Both directions use a **single long-lived IPC stream** each:

```
Client → Server:  IPC stream (input_schema, batch₁, batch₂, ..., EOS)
Server → Client:  IPC stream (output_schema, [log*+data]₁, [log*+data]₂, ..., EOS)
```

The exchange is **lockstep**: the client writes one input batch, then reads
the server's response (zero or more log batches followed by exactly one data
batch). This repeats until termination.

#### Producer streams

- **Input schema**: empty (`pa.schema([])`) — the client sends zero-row
  "tick" batches as timing signals.
- **Output**: the server produces one data batch per tick.
- **Termination**: the server signals completion by not writing a data batch
  after the final log batches — the output IPC stream reaches EOS. The
  client detects this as `StopIteration`.
- **Client-initiated close**: the client closes its input IPC stream (writes
  EOS). The server detects this as end of input and stops producing.

```
  Client                          Server
    |                                |
    |--- tick (0-row, empty) ------->|
    |<------ log* + data batch₁ ----|
    |--- tick (0-row, empty) ------->|
    |<------ log* + data batch₂ ----|
    |--- tick (0-row, empty) ------->|
    |<------ log* + [EOS] ----------|  (server called finish())
    |--- [EOS] --------------------->|
```

#### Exchange streams

- **Input schema**: a real schema matching the exchange input type.
- **Output**: the server produces one data batch per input batch.
- **Termination**: the **client** closes its input stream (EOS). The server
  drains remaining input and closes the output stream.

```
  Client                          Server
    |                                |
    |--- input batch₁ ------------->|
    |<------ log* + output batch₁ --|
    |--- input batch₂ ------------->|
    |<------ log* + output batch₂ --|
    |--- [EOS] --------------------->|
    |<------ [EOS] -----------------|
```

### Client-initiated cancellation

Either stream kind may be terminated early by the client with an explicit
**cancel batch**, distinct from simply closing the input stream:

- **num_rows**: 0
- **Schema**: the stream's input schema (the empty schema for producers).
- **Custom metadata**: `vgi_rpc.cancel` — the reference sets the value `"1"`,
  but **presence of the key is the signal**; a reader MUST NOT depend on the
  value.

On receipt the server MUST NOT invoke `process()` / `produce()` for that turn.
It invokes the stream state's optional `on_cancel` hook — a failure in the hook
is swallowed, never surfaced to the client — and then ends the stream cleanly
by closing the output stream. A cancel is not an error: no EXCEPTION batch is
written, and the exchange terminates normally.

Cancellation is best-effort on the client side: transport errors while sending
the cancel are swallowed, since the session is being abandoned regardless. After
cancelling, the session is closed — subsequent `exchange()` / `tick()` calls
fail locally.

The same key applies over HTTP, where it rides on the `/exchange` request batch
alongside the state tokens ([Section 10](#stream-exchange-http)). Because a
cancel is not a method dispatch, the server suppresses dispatch hooks for it;
the access log still records the call, marked cancelled.

### Error during streaming

If the server encounters an error during `process()`, it writes an
EXCEPTION-level log batch on the output stream, then the output stream
reaches EOS. The client reads the error batch, raises `RpcError`, and
the session is closed.

---

## 10. HTTP Transport

The HTTP transport maps the pipe-based protocol to stateless HTTP
request/response pairs. Streaming state is serialized into signed tokens
passed between exchanges.

### Content type

All requests and responses use:

```
Content-Type: application/vnd.apache.arrow.stream
```

The server MUST reject requests with any other Content-Type with HTTP 415
(Unsupported Media Type).

### Endpoints

Given a configurable URL prefix (default `/vgi`):

**RPC surface** — Arrow IPC in, Arrow IPC out:

| Endpoint | HTTP Method | Description |
|----------|-------------|-------------|
| `{prefix}/{method}` | POST | Unary RPC call |
| `{prefix}/{method}/init` | POST | Stream initialization (producer and exchange) |
| `{prefix}/{method}/exchange` | POST | Stream continuation / exchange / cancel |
| `{prefix}/__describe__` | POST | Introspection (unary; a synthetic method on the generic route) |
| `{prefix}/__upload_url__/init` | POST | Upload URL generation (only when an upload-URL provider is configured) |

**Framework endpoints** — not Arrow IPC:

| Endpoint | HTTP Method | Description |
|----------|-------------|-------------|
| `{prefix}/health` | GET, HEAD, OPTIONS | Health check + **capability discovery**. JSON body on GET; capability headers on all three. |
| `{prefix}/__session__` | DELETE | Sticky-session teardown (only when sticky sessions are enabled). See [Section 17](#17-sticky-sessions-http-optional). |
| `{prefix}/__introspect_token__` | POST | Token introspection (JSON). See [Section 16](#16-token-introspection-post-prefix__introspect_token__). Always routed; definitively refuses when disabled. |

**Optional, human- and IdP-facing** — present by default in the reference but
carrying no wire contract; a port may omit them entirely:
`GET {prefix}` (landing page), `GET {prefix}/describe` (HTML introspection
page), `/.well-known/oauth-protected-resource` (OAuth resource metadata), and
`{prefix}/_oauth/{callback,logout,token}` (OAuth PKCE browser flow).

### Capability discovery

Capability headers are stamped on **every** response, so a client that has
already made a call needs no separate probe. The dedicated discovery target is
`{prefix}/health`, because it is present in every implementation and exempt
from authentication; `OPTIONS` is the cheapest verb for it (`HEAD` and `GET`
carry the same headers). There is no Arrow IPC body on any of them.

> **Note for implementors migrating from an earlier draft of this document**:
> the discovery endpoint is `{prefix}/health`, **not** `{prefix}/__capabilities__`.
> The reference client has never probed the latter.

| Header | Type | Emitted | Description |
|--------|------|---------|-------------|
| `VGI-Max-Request-Bytes` | Integer | when configured | Maximum request body size the server accepts inline. Exceeding it is `413` (see [Section 13](#http-status-code-mapping)). |
| `VGI-Max-Response-Bytes` | Integer | when configured | HTTP body cap. *Soft* for producer streams (covered by continuation tokens), *hard* elsewhere. |
| `VGI-Max-Externalized-Response-Bytes` | Integer | when configured | Cap on total bytes uploaded to external storage during one response. Always *hard*. |
| `VGI-Externalization-Enabled` | `"true"` / `"false"` | always | Whether a storage backend is wired up, i.e. whether the client should expect pointer batches at all. |
| `VGI-Supported-Encodings` | Comma-separated codec tokens | always | Content codings this server will *produce*. See [Content-encoding negotiation](#content-encoding-negotiation). |
| `VGI-Upload-URL-Support` | `"true"` | when enabled | The upload-URL endpoint is available. |
| `VGI-Max-Upload-Bytes` | Integer | when enabled + configured | Maximum upload size for externalized batches. |
| `VGI-Proxy-Proof-Required` | `"true"` | when required | This worker rejects requests lacking a valid proxy proof. |
| `VGI-Sticky-Enabled` | `"true"` | when enabled | Sticky sessions are available. |
| `VGI-Sticky-Default-TTL` | Integer seconds | when sticky enabled | TTL applied when a method opens a session without specifying one. |
| `VGI-Sticky-Echo-Headers` | Comma-separated header names | when configured | Headers the client must replay for the life of a session. |
| `VGI-Token-Introspection` | `"true"` | when enabled | The token-introspection route is live. |

A capability header that is **absent** means "not configured / not supported",
with one deliberate exception: an absent `VGI-Supported-Encodings` means a
server predating the header, for which a client assumes `{zstd}`. A
*present but empty* value is that server positively stating it speaks no
compression — the two are not interchangeable.

The server MAY include `Cache-Control: max-age=N` on the discovery response;
a client that honours it should refresh on expiry.

### Upload URL generation

When the server has an `upload_url_provider` configured, the
`POST {prefix}/__upload_url__/init` endpoint generates pre-signed
upload/download URL pairs for client-side externalization.

**Request**: Standard unary request with `vgi_rpc.method` = `"__upload_url__"`.

| Parameter | Arrow type | Default | Description |
|-----------|-----------|---------|-------------|
| `count` | `int64` | 1 | Number of URL pairs to generate (1–100). |

**Response schema**:

| Column | Arrow type | Nullable | Description |
|--------|-----------|----------|-------------|
| `upload_url` | `utf8` | No | Pre-signed URL for uploading batch data. |
| `download_url` | `utf8` | No | Pre-signed URL the server uses to fetch the uploaded data. |
| `expires_at` | `timestamp("us", tz="UTC")` | No | Expiration time of the pre-signed URLs. |

The response has one row per requested URL pair.

### Request headers

| Header | Description |
|--------|-------------|
| `Content-Type` | MUST be `application/vnd.apache.arrow.stream` |
| `X-Request-ID` | Optional. Correlation ID echoed on response. If absent, server generates one. |
| `Content-Encoding` | Optional. Coding applied to the request body. An unsupported coding is `415`. |
| `Accept-Encoding` | Optional. Codings the client accepts on the response. |
| `X-VGI-Accept-Encoding` | Optional. Same, but takes precedence — see [Content-encoding negotiation](#content-encoding-negotiation). |
| `VGI-Proxy-Proof` | Optional. Per-request HMAC proof that the request arrived through a trusted proxy. See [Proxy Proof](proxy-proof-spec.md). |
| `VGI-Session-Accept` | Optional. `"true"` opts the client in to sticky sessions. See [Section 17](#17-sticky-sessions-http-optional). |
| `VGI-Session` | Optional. Resumes an existing sticky session. |

### Response headers

Every response carries the capability headers from
[Capability discovery](#capability-discovery) above. In addition:

| Header | Emitted | Description |
|--------|---------|-------------|
| `X-Request-ID` | always | Echoed or generated request correlation ID. |
| `X-VGI-RPC-Error` | on server-side errors | `"true"` marks a `200` response whose Arrow IPC body carries an EXCEPTION batch. See [Section 13](#http-status-code-mapping). |
| `Content-Encoding` | when the response body is compressed | The coding applied. |
| `X-VGI-Content-Encoding` | instead of the above | Used when the client negotiated via `X-VGI-Accept-Encoding`. |
| `VGI-Auth-Reason` | on `401` only | Machine-readable reason code from the closed set in [`docs/unauthorized-spec.md`](unauthorized-spec.md). |
| `VGI-Auth-Proxy-Required` | on `401` only, when applicable | `"true"` when this service's auth depends on headers a reverse proxy must inject. Derived from server *configuration*, so it is identical on every `401` and discloses nothing about the individual request. |
| `VGI-Session` | when a session was opened | The token the client echoes on subsequent requests. |
| `VGI-Session-Close` | when a session was closed | `"true"` tells the client to drop its captured token. |
| `VGI-Echo-<name>` | on a session-opening response, when configured | Instructs the client to send `<name>: <value>` on every subsequent request in the session. |

Servers that enable CORS expose `WWW-Authenticate`, `X-Request-ID`,
`X-VGI-Content-Encoding`, `X-VGI-RPC-Error`, `VGI-Auth-Reason`, and every
advertised capability header, so a browser client can read them cross-origin.

### Content-encoding negotiation

Request and response compression are independent: a server may decode a
compressed request while producing only uncompressed responses.

**Codec tokens** are the usual HTTP ones — `zstd`, `gzip`, and `identity`.
`identity` is the no-op transform, not a compressor: it exists so a client can
*explicitly* ask for an uncompressed response, which is otherwise only
reachable by accident when nothing it offers happens to be producible. It is
deliberately excluded from `VGI-Supported-Encodings`, since every
implementation can always do it and advertising it carries no information.

**Requests.** A client may compress the request body and name the coding in
`Content-Encoding`. A server that does not support the named coding MUST
answer `415`, not fall through to identity — the body would otherwise reach
the Arrow reader as garbage. A body that names a supported coding but fails to
decompress is `400`. Implementations MUST bound decompression output to
prevent a decompression-bomb DoS.

**Responses.** The client offers codings in `Accept-Encoding` and/or
`X-VGI-Accept-Encoding`. The server picks the first offered coding it can
produce, honouring client preference order, with **`X-VGI-Accept-Encoding`
taking precedence** over the generic header — both in choosing the codec and in
deciding which response header to stamp. If the first match is `identity`, the
server MUST honour that and send an uncompressed body rather than continuing
down the list. No overlap means an uncompressed body.

The custom header exists because general-purpose HTTP clients inject their own
`Accept-Encoding` (frequently listing `gzip` before `zstd`) that a caller
cannot suppress, which silently overrides the order vgi-rpc states. The
difference is not cosmetic — for large Arrow bodies gzip compression measured
roughly an order of magnitude slower than zstd end-to-end.

The chosen coding is stamped on `Content-Encoding`, or on
`X-VGI-Content-Encoding` when the client negotiated through the custom header.
Nothing is stamped for `identity`: an untransformed body is just a body.

### Unary call (HTTP)

```
POST {prefix}/{method}

Request body:  IPC stream (params_schema, 1 request row, EOS)
Response body: IPC stream (result_schema, 0..N log batches, 1 result/error batch, EOS)

HTTP 200: Success — and also server-side errors, which carry
          X-VGI-RPC-Error: true plus an EXCEPTION batch in the body
HTTP 400: Protocol error (bad IPC, missing metadata, param validation failure)
HTTP 401: Authentication failure (JSON envelope or HTML page, NOT Arrow IPC)
HTTP 404: Unknown method
HTTP 413: Request body exceeds VGI-Max-Request-Bytes
HTTP 415: Wrong Content-Type, or an unsupported Content-Encoding
```

See [Section 13](#http-status-code-mapping) for the full mapping, including why
a server implementation error surfaces as `200` rather than `500`.

The method name in the URL path MUST match the `vgi_rpc.method` value in
the request batch's custom metadata. A mismatch is a 400 error.

### Stream initialization (HTTP)

```
POST {prefix}/{method}/init

Request body:  IPC stream (params_schema, 1 request row, EOS)
```

The response depends on whether the stream is a **producer** or **exchange**
stream:

#### Producer stream init response

The response body contains the complete producer output:

```
Response body:
  [IPC stream: header_schema, 0..N log batches, 1 header row, EOS]   (if header declared)
  [IPC stream: output_schema, (log* + data)*, EOS]
```

All produced data batches are included inline. If the response body would
exceed `max_response_bytes` (the operator-configured HTTP body cap), the
server stops producing and appends a **continuation batch**: a zero-row
batch with `vgi_rpc.stream_state#b64` in its custom metadata. The client
then follows up with `/exchange` requests carrying that token. For
producer streams the wire cap is *soft* — continuation tokens cover the
overshoot. The companion cap `max_externalized_response_bytes` governs
external-channel uploads independently and is *hard*: a producer that
would exceed it surfaces an `RpcError` rather than continuing.

#### Exchange stream init response

```
Response body:
  [IPC stream: header_schema, 0..N log batches, 1 header row, EOS]   (if header declared)
  [IPC stream: output_schema, 0..N log batches, 1 zero-row batch with state token, EOS]
```

The zero-row batch carries the signed state token in
`vgi_rpc.stream_state#b64` custom metadata.

A server MUST split a stream's state in two, by lifetime. The half that is
fixed for the life of the call — the init request and the resolved
input/output schemas — is sealed into a separate **call token** under
`vgi_rpc.call_state#b64`, carried on this same zero-row batch;
`vgi_rpc.stream_state#b64` then holds only the per-turn **cursor**.

The split is required, not an optimisation. Packing both halves into one
token makes every continuation re-serialize, re-seal, re-open and re-parse a
payload that cannot have changed — for a typical stream, the overwhelming
majority of the token. Splitting also lets each half be compressed on its
own lifetime, and lets a server cache the resolved call so a warm process
skips opening the call token altogether. A server that keeps everything in
the cursor forces every peer and intermediary onto the expensive path
permanently, and is not conformant.

The call token is minted once, by `/init`, and is **never re-issued** — a
continuation response carries only a cursor. `/init` MUST emit both keys on
the same zero-row sentinel, for producer and exchange streams alike, and MUST
do so even when the method declares no call state of its own: the token still
carries the frozen schemas and the call id that binds the pair.

### Stream exchange (HTTP)

```
POST {prefix}/{method}/exchange

Request body:  IPC stream (input_schema, 1 input batch with state token in metadata, EOS)
Response body: IPC stream (output_schema, 0..N log batches, 1 data batch with updated state token, EOS)
```

The request batch's custom metadata MUST contain `vgi_rpc.stream_state#b64`
with the current state token (base64-encoded).

The request MUST also echo `vgi_rpc.call_state#b64`, unchanged, on **every**
subsequent request — continuations, exchanges, and cancels alike. A server
may resolve the call from a per-process cache, so a client that omits the
token still works while that cache is warm; it fails as soon as the cache is
not — a restarted worker, an evicted entry, or a request balanced onto a node
that never saw the `/init`. Since the cursor names a call the server minted
but no longer carries its payload, the client's copy is the only one such a
node has. A server MUST reject a continuation it cannot resolve with
`400 Bad Request` rather than guessing.

The same obligation extends to intermediaries: a proxy that forwards a
continuation must carry both tokens, not just the cursor.

#### Resolution order (normative)

A server MUST resolve the two tokens in this order:

1. Open and authenticate the **cursor** token first. Its AEAD tag covers the
   `call_id`; its AAD covers the caller's `(domain, principal)`.
2. Only then use that now-authenticated `call_id` to look up any cached
   resolved call.
3. On a cache miss, open the client-supplied **call token** and require its
   embedded `call_id` to equal the one the cursor named.

The ordering is a security property, not an implementation detail. A client
cannot name a `call_id` the server did not mint for it, so a cache hit can
never hand back another principal's call state. A server that opens the
client-supplied call token first, or that keys a cache on any value the
client controls directly, has a cross-principal disclosure bug even though
every functional test still passes.

For **producer continuation**, the input is a zero-row batch on empty schema
with the state token. The response may contain multiple data batches and
may end with another continuation token.

For **exchange**, the input carries real data plus the state token. The
response data batch carries an updated state token for the next exchange.

To **cancel**, the client sends a zero-row input batch carrying
`vgi_rpc.cancel` alongside both tokens ([Section 9](#client-initiated-cancellation)).
The server ends the stream without dispatching the method.

The client MUST strip `vgi_rpc.stream_state#b64` and `vgi_rpc.call_state#b64`
from the batch metadata before exposing it to application code.

### State token binary format

Both tokens are opaque AEAD-sealed blobs, base64-encoded for UTF-8 safe
metadata storage. The envelope is XChaCha20-Poly1305 (libsodium IETF
variant) — confidential (state is not visible to anything between client
and server) and authenticated (any tampering, including cross-principal
replay, fails decryption).

The two carry **independent version lines**, because they change for
independent reasons. After base64-decoding, both share this envelope:

```
Offset      Size     Field
0           1        version: uint8 (cursor token: 5; call token: 1)
1           24       nonce: random per-token (XChaCha20-Poly1305 NPUB)
25          ...      ciphertext: AEAD(sealed_payload, AAD) — includes the
                     16-byte Poly1305 tag at the end
```

#### Compression happens inside the seal (normative)

`sealed_payload` is not the framed plaintext directly. It is:

```
Offset      Size     Field
0           1        codec: uint8 — self-describing codec tag
                     (reference: 0x00 raw, 0x01 zstd)
1           ...      the framed plaintext below, compressed per `codec`
```

The order matters and is the point of the exercise: **compress, then
encrypt**. Once a token is sealed it is ciphertext, so the HTTP body codec
can no longer find any redundancy in it — measured, zstd over a sealed token
recovers only the slack base64 added (to ~76–80%) and never the state's own
structure. Compressing inside the seal reaches the real redundancy: a
7,800-byte call state packs to 1,872, turning a 10,820-byte token into 2,552.

This is the second half of what the lifetime split buys. Splitting lets each
half be compressed and cached on its own lifetime; a server that packs
everything into one token pays full freight on every turn even if it
compresses.

The requirements on a server are:

- It MUST compress the payload inside the seal whenever the compressed form
  is smaller than the raw one. Compressing outside the seal is not an
  alternative — it accomplishes nothing.
- It MUST prefix a self-describing codec tag, so the reader never guesses,
  and MUST emit the raw tag and skip compression when compression does not
  pay. A small token must never grow.
- It MUST bound the decompressed size (the reference caps output at 64 MiB).
  The payload is authenticated before it is decompressed, so this guards
  against a framework bug rather than an attacker, but an unbounded
  decompress on a request path is not worth having.
- It MUST reject an unknown codec tag, or a payload that fails to
  decompress, as the same uniform `400 Bad Request` as every other token
  failure — both mean a token this server did not mint.

The **codec is the port's choice**: zstd where the runtime has it, deflate or
gzip where it does not. Tag values are per-port, since a token never
round-trips across ports. The reference uses zstd level 3 for both token
kinds — the same speed as level 1 at these payload sizes and slightly
smaller, where the levels that compress materially better (9, 19) cost 8× and
84× the CPU for a few hundred bytes.

Because token internals are opaque by design, none of this is observable
from outside and the shared conformance suite cannot check it. Each port
should assert it in a language-local test over its own seal/open path — that
compression engages on a large payload, that a tiny payload stays raw, and
that a corrupt or unknown-codec payload surfaces as a 400.

Cursor token plaintext (encrypted, never on the wire) — v5 carries only the
advancing state plus the call id binding it to its call token. Everything
the pre-split v4 token also carried (both schemas, the stream id) moved into
the call token:

```
Offset      Size     Field
0           8        created_at: uint64 LE (seconds since Unix epoch)
8           16       call_id: the call token this cursor belongs to
24          4        state_len: uint32 LE
28          N        state_bytes: serialized StreamState
```

Call token plaintext — v1, minted once at `/init`:

```
Offset      Size     Field
0           8        created_at: uint64 LE (seconds since Unix epoch)
8           16       call_id: random, minted at /init
24          4        call_len: uint32 LE
28          N        call_state_bytes: serialized call state (empty when the
                     method declares none)
...         4        type_len: uint32 LE
...         M        call_state_type: UTF-8 class name, or empty
...         4        schema_len: uint32 LE
...         P        schema_bytes: serialized output pa.Schema
...         4        input_schema_len: uint32 LE
...         Q        input_schema_bytes: serialized input pa.Schema
...         4        stream_id_len: uint32 LE
...         R        stream_id_bytes: UTF-8 chain-correlation id
```

`call_state_type` is carried because a stream method may return a *union* of
state classes whose members declare different call-state types. A reader MUST
resolve that name against the set the method itself declares, and reject an
unrecognised one — never look up a class by a client-supplied name.

The AEAD AAD (authenticated, not encrypted) is:

```
cursor token:  b"vgi_rpc.state.v4\x00" || identity_tail
call token:    b"vgi_rpc.call.v1\x00"  || identity_tail
```

where `identity_tail` is `b"\x01" || domain || b"\x00" || principal` for
authenticated requests and the literal `b"\x00anonymous"` otherwise. The
prefixes differ deliberately, so a call token and a cursor token are not
interchangeable even for the same principal: presenting one where the other
is expected fails the tag check rather than decoding into a payload the
reader would misinterpret.

As elsewhere, the *plaintext* framing above is the reference implementation's;
a port may choose its own encoding for what goes inside each token, and its
own compression codec (see the porting guide). What is normative is that
there are two tokens, split by lifetime, bound by an authenticated `call_id`,
and that each is compressed inside its seal under a self-describing codec
tag.
This binds every token to its issuing identity: a token sealed for one
principal cannot be opened with another principal's AAD even if both
share the master `token_key`.

- **`version`**: Format selector. Not part of the AAD; mismatches are
  rejected before doing crypto work, but tampering with the byte to
  point at a different algorithm still fails the subsequent decrypt
  because the server uses the format-fixed algorithm constants.
- **`created_at`**: Token creation time as seconds since the Unix epoch.
  Used by the server to enforce a configurable TTL (`token_ttl`). When
  `token_ttl > 0`, tokens older than `token_ttl` seconds are rejected
  with HTTP 400 ("State token expired"). Set `token_ttl` to `0` to
  disable expiry checking. The default TTL is 3600 seconds (1 hour).
- **`state_bytes`**: The stream state dataclass serialized as a complete
  Arrow IPC stream (schema + 1-row batch + EOS).
- **`schema_bytes`**: The output Arrow schema serialized via
  `pa.Schema.serialize()`.
- **`input_schema_bytes`**: The input Arrow schema serialized via
  `pa.Schema.serialize()`. For producer streams, this is the serialized
  empty schema.

**Verification order**: The version byte is checked first (cheap
rejection), then the AEAD `decrypt` is attempted. Any authenticity
failure (bad key, bad AAD, tampered nonce or ciphertext) surfaces as a
uniform HTTP 400 "State token signature verification failed". TTL
enforcement only runs after authenticity is established — the timestamp
is inside the ciphertext, so it cannot be tampered with independently.

### Authentication (HTTP)

When the server has an `authenticate` callback configured:

- The callback receives the HTTP request and returns an `AuthContext`.
- On failure (`ValueError` or `PermissionError`), the server returns **HTTP 401**. The body is NOT Arrow IPC — no method has been resolved yet, so no output schema is available. Its shape is the standardized envelope of `docs/unauthorized-spec.md`: a JSON object carrying a `reason` code from a closed set, mirrored on a `VGI-Auth-Reason` header, or the styled HTML page when the request's `Accept` asks for `text/html`.
- Other exceptions from the callback propagate as HTTP 500.
- Clients MUST detect 401 responses before attempting to parse Arrow IPC.

#### Proxy proof (optional)

A worker may additionally require that a request arrived through a trusted proxy. The proxy mints a
per-request HMAC-SHA256 proof in a `VGI-Proxy-Proof` header; the worker verifies it against a shared
per-worker secret.

- It is a **precondition ANDed with** the `authenticate` callback above, never an alternative
  credential — the caller's `Authorization` header is untouched and still carries the end user.
- Failure maps to the same **HTTP 401** as any other authenticate failure, carrying the
  `proxy_required` reason code. The body MUST NOT echo the verifier's reason or the claimed key
  id — every proof outcome collapses onto that one code.
- `OPTIONS`, `/.well-known/`, and `{prefix}/health` are exempt in all modes, so load-balancer
  probes and capability discovery keep working.
- A worker requiring proofs advertises `VGI-Proxy-Proof-Required: true` on every response.
- Opt-in: an unconfigured worker reads no header, emits none, and is byte-identical to a worker
  built before the feature existed.

The token format, canonical MAC input, verifier algorithm, reason codes, and rotation procedure are
normative in [the Proxy Proof Specification](proxy-proof-spec.md).

---

## 11. Shared Memory (SHM) Transport

The shared memory side-channel enables zero-copy batch transfer between
co-located processes. It is used alongside a pipe transport — the pipe
carries control messages and small batches; large batches are written to
shared memory and replaced with pointer batches on the pipe.

> **Negotiation prerequisite.** Over the pipe / subprocess / AF-UNIX
> transports, SHM MUST be negotiated via `__transport_options__`
> ([Section 15](#15-transport-capability-negotiation-__transport_options__))
> before use: a client only advertises a segment (and writes SHM pointer
> batches) to a server that has confirmed `vgi_rpc.transport.shm = "true"`.
> A server that cannot do SHM (non-POSIX host, missing runtime support, or a
> server predating the method) reports `"false"` (or errors), and the client
> falls back to the inline pipe transport. (HTTP servers advertise the same
> capability via the `OPTIONS {prefix}/__capabilities__` endpoint in Section
> 10.)

### Segment header format

The shared memory segment begins with a 64 KiB (65,536 byte) header,
followed by a data region. All integers are little-endian.

```
Offset  Size    Field
0       4       magic: bytes "VGIS" (0x56 0x47 0x49 0x53)
4       4       version: uint32 = 1
8       8       data_size: uint64 (segment size minus 65536)
16      4       num_allocs: uint32 (number of active allocations)
20      4       padding: uint32 = 0
24      N*16    allocations: array of (offset: uint64, length: uint64)
                sorted by offset, where N = num_allocs
```

- **Maximum allocations**: `(65536 - 24) / 16 = 4094`.
- **Offsets are absolute** — measured from the start of the shared memory segment (not from the data region start).
- **Data region** starts at byte offset 65,536 (immediately after the header).

### Allocation strategy

The allocator uses a first-fit strategy with implicit coalescing:

1. Scan the sorted allocation list for the first gap that fits the requested size.
2. Gaps are computed as: before the first allocation (from offset 65536), between consecutive allocations, and after the last allocation (to segment end).
3. New allocations are inserted to maintain sorted order.
4. Freeing an allocation removes its entry; adjacent free space coalesces implicitly since only occupied regions are tracked.

### Batch serialization in SHM

For **non-dictionary-encoded batches**: A complete Arrow IPC stream (schema + record batch + EOS) is written directly into the allocated SHM region.

For **dictionary-encoded batches**: The IPC stream is written to a temporary buffer, then the schema message and EOS marker are stripped — only the dictionary messages and record batch message are stored in SHM.

**Dictionary batch reconstruction** (reader side):

To deserialize a dictionary-encoded batch from SHM:

1. Serialize the pointer batch's schema into a schema message by creating a
   temporary IPC stream writer (which emits a schema message + EOS), then
   strip the trailing 8-byte EOS marker. This yields the schema message bytes.
2. Concatenate: `schema_message_bytes` + `shm_stored_bytes` + `EOS_marker`
   (8 bytes: `0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x00 0x00`).
3. Open the concatenated buffer as a standard Arrow IPC stream and read the
   batch.

Non-dictionary batches do not need this reconstruction — they are stored as
complete IPC streams and can be read directly.

### SHM pointer batch

A batch stored in shared memory is replaced on the pipe with a **pointer batch**:

- **num_rows**: 0
- **Schema**: Same as the original batch's schema.
- **Custom metadata**:
  - `vgi_rpc.shm_offset`: Absolute byte offset in the segment (decimal string).
  - `vgi_rpc.shm_length`: Number of bytes written (decimal string).

### SHM segment identity in request metadata

When a client owns a shared memory segment, it advertises the segment in the
request batch's custom metadata:

- `vgi_rpc.shm_segment_name`: OS name of the shared memory segment.
- `vgi_rpc.shm_segment_size`: Total segment size in bytes (decimal string).

The server dynamically attaches to the segment (read-only, untracked by
the resource tracker) for the duration of the request.

### Resolution algorithm

```
resolve_shm_batch(batch, custom_metadata, shm_segment):
  IF shm_segment is NULL:
    return (batch, custom_metadata, null)

  IF batch.num_rows != 0:
    return (batch, custom_metadata, null)

  IF custom_metadata is NULL:
    return (batch, custom_metadata, null)

  IF "vgi_rpc.shm_offset" NOT IN custom_metadata:
    return (batch, custom_metadata, null)

  IF "vgi_rpc.log_level" IN custom_metadata:
    return (batch, custom_metadata, null)  // log batch, not pointer

  offset = int(custom_metadata["vgi_rpc.shm_offset"])
  length = int(custom_metadata["vgi_rpc.shm_length"])

  buffer = shm_segment.read(offset, length)
  resolved_batch = deserialize_ipc_stream(buffer, batch.schema)

  // Strip pointer keys, add provenance
  resolved_metadata = remove_keys(custom_metadata, "vgi_rpc.shm_offset", "vgi_rpc.shm_length")
  resolved_metadata["vgi_rpc.shm_source"] = shm_segment.name

  release_fn = () => shm_segment.free(offset)

  return (resolved_batch, resolved_metadata, release_fn)
```

---

## 12. External Storage Pointer Batches

When batches exceed a configurable size threshold, they can be externalized
to remote storage (e.g., S3, GCS) and replaced with pointer batches.

### Pointer batch format

- **num_rows**: 0
- **Schema**: Same as the original batch's schema.
- **Custom metadata**:
  - `vgi_rpc.location`: URL to fetch the batch data (typically a pre-signed URL).
  - `vgi_rpc.location.sha256`: Optional. SHA-256 hex digest of the payload
    **before** compression.
- **Must NOT contain** `vgi_rpc.log_level` (to distinguish from log batches).

### Externalization (writing)

When a data batch's total buffer size exceeds the threshold:

1. Serialize **all** batches from the current output cycle (log batches + data batch) as a single IPC stream.
2. Compute the SHA-256 of those bytes, before any compression.
3. Optionally compress with zstd.
4. Upload to external storage via the `ExternalStorage.upload()` interface.
5. Replace the entire cycle with a single zero-row pointer batch containing `vgi_rpc.location` (and `vgi_rpc.location.sha256` when the digest was computed).

### Integrity

`vgi_rpc.location.sha256` is optional on the wire — a pointer batch without it
is valid, which keeps readers compatible with writers that predate the key.
When the key **is** present, a reader MUST verify the digest against the fetched
payload (decompressed, if a coding was applied) and MUST fail the resolution
on mismatch rather than handing the batch to application code.

### Resolution (reading)

```
resolve_external_location(batch, custom_metadata, config):
  IF config is NULL:
    return (batch, custom_metadata)

  IF NOT is_external_pointer(batch, custom_metadata):
    return (batch, custom_metadata)

  url = custom_metadata["vgi_rpc.location"]

  // Validate URL (default: HTTPS only)
  config.url_validator(url)

  // Fetch with retries
  // Default: max 3 total attempts (max_retries=2, capped at 2)
  // Retry delay: 0.5s fixed between attempts
  // Retryable errors: network/OS errors, Arrow parse errors, HTTP client errors
  data = fetch_url(url, config.fetch_config)

  // Decompress if needed (zstd)
  // Open as IPC stream, dispatch log batches, extract data batch
  reader = open_ipc_stream(data)
  FOR each batch in reader:
    IF is_log_or_error(batch):
      deliver to on_log callback
      CONTINUE
    IF has "vgi_rpc.location":
      ERROR: redirect loop detected
    data_batch = batch

  // Validate schema match
  IF data_batch.schema != expected_schema:
    ERROR: schema mismatch

  // Add fetch provenance metadata
  resolved_metadata["vgi_rpc.location.fetch_ms"] = elapsed_ms
  resolved_metadata["vgi_rpc.location.source"] = url

  return (data_batch, resolved_metadata)
```

### Stream externalization

For stream methods, the server may externalize an entire output cycle
(log batches + data batch) as one IPC stream. The pointer batch replaces
the entire cycle. On resolution, the client reads back all batches,
dispatches log batches, and returns the data batch.

---

## 13. Version Negotiation & Error Handling

### Version checking

Every request batch MUST carry `vgi_rpc.request_version` in its custom
metadata with the value `"1"`.

| Condition | Error |
|-----------|-------|
| `vgi_rpc.request_version` missing | `VersionError` — server writes an error stream on the empty schema. |
| `vgi_rpc.request_version` != `"1"` | `VersionError` — server writes an error stream on the empty schema. |
| `vgi_rpc.protocol_version` missing or mismatched (when enforced) | `ProtocolVersionError` — see below. |
| `vgi_rpc.method` missing | `RpcError` (ProtocolError) — server writes an error stream. |
| Unknown method name | `RpcError` (AttributeError) — error stream includes available method names. |
| Request batch has wrong row count (not 1, on non-empty schema) | `RpcError` (ProtocolError). |
| Non-optional parameter is null | `TypeError` — error stream on the method's result schema. |

### Protocol version negotiation

`vgi_rpc.request_version` versions the **framing** in this document, and has
been `"1"` throughout. `vgi_rpc.protocol_version` is a second, independent
line: it versions the *application's* RPC surface — the set of methods, their
parameters, and their schemas — so a client and worker built against different
releases of a service fail with a directional message instead of a schema
error deep inside a call.

It is **opt-in by declaration**. A Protocol that declares a version (in the
Python reference, a `protocol_version: ClassVar[str]` on the Protocol class)
turns the check on for both peers; a Protocol that declares none disables it
entirely, and the key never appears on the wire.

- **Format**: canonical semver `MAJOR.MINOR.PATCH` — non-negative integers, no
  leading zeros, **no prereleases and no build metadata**. `1.0.0-rc1` and
  `1.0.0+build3` are malformed, not merely unusual.
- **Client obligation**: when the bound Protocol declares a version, the client
  MUST send it on **every** request batch.
- **Server obligation**: when its own Protocol declares a version, the server
  MUST check the client's at the dispatch boundary — before parameter
  deserialization, so a mismatch cannot be mistaken for a schema problem.
- **Comparison rule**: exact **major and minor** match. Patch is ignored, so a
  `1.4.0` client and a `1.4.9` server interoperate.
- **`__describe__` is exempt.** It is the diagnostic path a version-mismatched
  client uses to discover what the server actually speaks, so gating it would
  make the mismatch undiagnosable.

Every failure — absent key, undecodable bytes, malformed semver, or a genuine
major/minor difference — raises `ProtocolVersionError`, a subclass of
`VersionError`, and is written as an ordinary error stream carrying
`error_kind = "protocol_version_mismatch"`. The message MUST state both
versions and which side to upgrade; a bare "mismatch" leaves the reader to
guess, which is the whole failure this key exists to prevent.

The server also emits its `protocol_version` in the `__describe__` response
metadata, so a client can read it without triggering a failure.

> **Relationship to `protocol_hash`**: the hash ([Section 14](#14-introspection-__describe__))
> is a byte-stable fingerprint of the *Python* describe payload, useful as a
> drift detector within one runtime. It is **not** guaranteed identical across
> Arrow implementations, so it is not a cross-language contract.
> `protocol_version` is.

### Error stream format

Protocol-level errors are written as a complete IPC stream on the
appropriate schema (empty schema for version/method errors, result schema
for parameter validation errors):

```
IPC Stream (error):
  Schema message (empty or result schema)
  1 zero-row batch with EXCEPTION-level log metadata
  EOS marker
```

### HTTP status code mapping

Two distinct classes of failure are deliberately not conflated. A **transport
or protocol** failure — the request never became a valid RPC call — carries a
4xx status. An **application** failure — the method was dispatched and raised —
is reported *in band*, as a `200` whose Arrow IPC body carries an EXCEPTION
batch.

| Error condition | HTTP status |
|----------------|-------------|
| Bad IPC, missing metadata, request-version mismatch, param validation | 400 Bad Request |
| `protocol_version` mismatch | 400 Bad Request |
| Expired, tampered, or unresolvable state token | 400 Bad Request |
| Request body fails to decompress | 400 Bad Request |
| Authentication failure (including proxy proof) | 401 Unauthorized |
| Unknown method | 404 Not Found |
| Request body exceeds `VGI-Max-Request-Bytes` | 413 Payload Too Large |
| Wrong `Content-Type`, or unsupported `Content-Encoding` | 415 Unsupported Media Type |
| **Any error raised by the method implementation** | **200 OK** + `X-VGI-RPC-Error: true` |
| Response overshoots a hard response cap | 200 OK + `X-VGI-RPC-Error: true` |

#### Why implementation errors are `200`

A server implementation error never reaches the client as `500`. The server
translates it to `200` and marks it with `X-VGI-RPC-Error: true`, because
intermediaries and HTTP client libraries routinely discard or replace response
bodies on 5xx — and the body is precisely where the typed error lives. A `500`
would strip the exception type, message, traceback, and `error_kind` and leave
the caller with a bare status code.

Clients MUST therefore treat `200` as "a response arrived", not "the call
succeeded", and classify by inspecting the body — the batch-classification
algorithm in [Section 7](#7-batch-classification-algorithm) already does this,
so `X-VGI-RPC-Error` is a fast path and a diagnostic aid, not a second source
of truth. A client that branches only on status code will silently treat
failures as successes.

This also means the status code no longer varies with the *class* of exception
the method raised: a `TypeError` from inside a method body is `200` like any
other. Caller-supplied shape errors are validated before dispatch, and so
remain `400`.

> **Note**: For 400 and 413 responses the body is still a valid Arrow IPC
> stream containing an error batch. The exceptions are 401 (JSON or HTML, per
> [`docs/unauthorized-spec.md`](unauthorized-spec.md)), 415 (framework default
> response), and the non-Arrow framework endpoints in
> [Section 16](#16-token-introspection-post-prefix__introspect_token__) and
> [Section 17](#17-sticky-sessions-http-optional).

---

## 14. Introspection (`__describe__`)

The `__describe__` method is a built-in synthetic unary method that returns
machine-readable metadata about all methods exposed by the server. It is
optional for implementors.

### Request

Standard unary request with:
- `vgi_rpc.method` = `"__describe__"`
- Empty params schema (zero fields, one row)

### Response

A single IPC stream with one row per method. The response batch carries
custom metadata:

- `vgi_rpc.protocol_name` — Protocol class name
- `vgi_rpc.request_version` — Wire protocol version (`"1"`)
- `vgi_rpc.describe_version` — Introspection format version (`"4"`)
- `vgi_rpc.protocol_hash` — SHA-256 hex digest over the canonical describe payload
- `vgi_rpc.server_id` — Server instance identifier

### Response batch schema

The schema is deliberately language-neutral (`describe_version` `"4"`). Python-flavoured
fields present in earlier versions (`doc`, `param_types_json`, `param_defaults_json`,
`param_docs_json`) were **dropped in v4** — human-readable type names, defaults, and
docstrings live in the Protocol source class, not on the wire.

| Column | Arrow type | Nullable | Description |
|--------|-----------|----------|-------------|
| `name` | `utf8` | No | Method name |
| `method_type` | `utf8` | No | `"unary"` or `"stream"` |
| `has_return` | `bool` | No | Whether the unary method returns a value |
| `params_schema_ipc` | `binary` | No | Serialized `pa.Schema` for request parameters |
| `result_schema_ipc` | `binary` | No | Serialized `pa.Schema` for unary response |
| `has_header` | `bool` | No | Whether the stream method has a header type |
| `header_schema_ipc` | `binary` | Yes | Serialized `pa.Schema` for the header (null if no header) |
| `is_exchange` | `bool` | Yes | For streams: `true` = exchange (bidi), `false` = producer; `null` for unary |

The `params_schema_ipc`, `result_schema_ipc`, and `header_schema_ipc`
columns contain Arrow schemas serialized via `pa.Schema.serialize()`.

---

## 15. Transport Capability Negotiation (`__transport_options__`)

`__transport_options__` is a built-in synthetic unary method (parallel to
`__describe__`) through which a client and server discover each other's
**transport** capabilities — chiefly whether the shared-memory side-channel
(Section 11) may be used. It is the pipe / subprocess / AF-UNIX analogue of the
HTTP `OPTIONS {prefix}/__capabilities__` endpoint (Section 10).

It is **mandatory before SHM is used**: a client that has SHM available MUST NOT
write SHM pointer batches (or advertise a segment) to a server unless that server
has confirmed SHM support via this method. A server that cannot attach SHM (e.g.
a non-POSIX host, or a runtime without the required FFM support) reports
`shm = "false"` and the client falls back to inline transport. A server that does
not implement the method at all returns a `method_not_implemented` error (or any
error), which the client treats as "no SHM".

Capabilities are negotiated **once per worker** and may be cached for the life of
the worker process (they are process-level, not per-connection), so there is no
per-call overhead.

### Request

Standard unary request with:
- `vgi_rpc.method` = `"__transport_options__"`
- Empty params schema (zero fields, one row)
- The client's own capabilities as request metadata under the
  `vgi_rpc.transport.*` namespace (e.g. `vgi_rpc.transport.shm = "true"`)

### Response

An IPC stream with an **empty** batch (zero fields). Capabilities ride as the
response batch's `custom_metadata` under the `vgi_rpc.transport.*` namespace:

| Key | Value | Description |
|-----|-------|-------------|
| `vgi_rpc.transport.shm` | `"true"` / `"false"` | Whether the server can use the SHM side-channel |
| `vgi_rpc.server_id` | UTF-8 | Server instance identifier |
| `vgi_rpc.request_version` | `"1"` | Wire protocol version |

The capability set is open-ended: keys are matched by the `vgi_rpc.transport.`
prefix and unknown keys are ignored, so future capabilities (e.g. compression,
AEAD) can be added without a protocol-version bump. A feature is used only when
**both** peers advertise it.

### Negotiation rule

```
shm_enabled = client.advertises("vgi_rpc.transport.shm" == "true")
              AND server.advertises("vgi_rpc.transport.shm" == "true")
```

A server that has not attached a segment but still receives an inbound SHM
pointer batch (a negotiation violation) MUST fail loudly rather than silently
treat the zero-row pointer as empty input.

---

## 16. Token Introspection (`POST {prefix}/__introspect_token__`)

**HTTP-only. Optional. Absent unless explicitly enabled.**

Resolves an **opaque bearer credential** to a **principal**, for a reverse proxy that terminates the only public listener and must know which principal a credential authenticates as before it can authorize. Not part of the Arrow RPC surface: it is JSON in and JSON out, because its consumer is an authorization layer, not an Arrow client.

### Request

```http
POST {prefix}/__introspect_token__
Authorization: Bearer <introspector credential>
Content-Type: application/json

{"token": "<opaque subject credential>"}
```

The body carries exactly one key. Implementations MUST cap it — the only legitimate content is one credential, and the generic request-size cap would otherwise admit megabytes into a JSON parse.

### Response

| Status | Meaning | Caller behaviour |
|---|---|---|
| `200` | Resolved. Body below. | Cache for `ttl_seconds`. |
| `401` / `403` / `404` | **Definitive** — refused, or did not resolve. | MAY negative-cache. |
| `5xx`, transport failure | **Transient** — could not answer. | MUST NOT cache; retry. |

```json
{"principal": "alice@example.com", "token_name": "laptop", "ttl_seconds": 300}
```

Exactly three keys. **A `claims` field MUST NEVER be returned** — see the porting guide for why this is the constraint the whole feature rests on. `ttl_seconds` MUST be finite and positive; `NaN` silently disables a caller's cache and turns every request into a round trip.

The definitive/transient split is **normative**. A caller's negative cache depends on it: cache an outage and a worker restart takes the fleet down for the cache's lifetime; retry a rejection and the worker is hammered. A worker that has *not* enabled introspection MUST still answer definitively — `404` in the reference — rather than letting the path fall through to a generic route whose status a caller reads as transient.

### Guards

Normative for any implementation that enables the route:

- The route is **absent** (or definitively refusing) unless explicitly enabled.
- An **introspector-principal allowlist** with no permissive default. Authentication is not the same capability as introspection.
- **JWS-shaped subjects are rejected without being resolved.**
- **Uniform rejection**: unknown, expired and malformed are byte-identical answers.
- The credential appears in **no** response, error message, log record, or span. Digest it (SHA-256) for diagnostics.
- `VGI-Token-Introspection: true` on `/health` when enabled, absent otherwise.

Conformance group: `TestTokenIntrospection` (optional fixture) and `TestTokenIntrospectionOffMode` (ungated).

---

## 17. Sticky Sessions (HTTP, optional)

**HTTP-only. Optional. Opt-in on both sides.**

Sticky sessions let a method bind a **handle-bearing object** — an open
database cursor, a loaded model, a file handle, an in-progress generation — to
the worker process that created it, keyed by a short-lived AEAD-sealed token
the client echoes on subsequent requests. The state lives in process memory; it
is never serialized onto the wire.

The other transports are single-process, so sticky is meaningless there: a
runtime that exposes the session API at all MUST raise on a non-HTTP transport
rather than silently no-op. When neither side opts in, the wire is
byte-identical to a framework built before the feature existed.

The full normative contract — token envelope, principal binding, TTL and
eviction, drain semantics, concurrency, and the `TestSticky` conformance
group — is [`docs/sticky-sessions-spec.md`](sticky-sessions-spec.md). What
follows is the wire surface only.

### Request headers

| Header | Required | Purpose |
|---|---|---|
| `VGI-Session-Accept: true` | when a method may open a session | Client opt-in. A server MUST refuse to open a session for a request lacking it — otherwise it leaks sessions to clients that are not tracking them. |
| `VGI-Session: <token>` | when resuming | The token minted on a prior response. |

### Response headers

| Header | Emitted | Purpose |
|---|---|---|
| `VGI-Session: <token>` | when a session was opened this request | Token for the client to echo. Base64url, no padding. |
| `VGI-Session-Close: true` | when the session was closed this request | Client drops its captured token and any echo headers. |
| `VGI-Echo-<name>: <value>` | once, on the session-opening response | Client MUST strip the `VGI-Echo-` prefix and send `<name>: <value>` on every subsequent request in the session. Used for client-driven routing on platforms that steer by header. |

Capability headers (`VGI-Sticky-Enabled`, `VGI-Sticky-Default-TTL`,
`VGI-Sticky-Echo-Headers`) are listed under
[Capability discovery](#capability-discovery).

### Teardown endpoint

```
DELETE {prefix}/__session__
VGI-Session: <token>
```

Idempotent and best-effort. `204 No Content` when the entry was found and
evicted; `200 OK` on **any** failure — missing header, malformed token,
identity mismatch, registry miss. The two are deliberately not
distinguishable, so a stolen token cannot be used to probe whether a session
exists.

### Failure surfacing

A token that cannot be honoured — expired, evicted, routed to a worker that
never saw it, or presented under a different principal — surfaces as an
ordinary EXCEPTION batch with `error_kind = "session_lost"`. A session open
refused because the server is shutting down surfaces as `server_draining`.
Neither is retried transparently by the framework: the client is told, and
decides whether to reopen or fail.

## Appendix A: IPC Stream EOS Marker

The end-of-stream marker is the 8-byte sequence:

```
0xFF 0xFF 0xFF 0xFF    (continuation token = -1 as int32 LE)
0x00 0x00 0x00 0x00    (metadata length = 0)
```

This signals to the IPC stream reader that no more messages follow.

## Appendix B: Empty Schema

The "empty schema" referenced throughout this specification is an Arrow
schema with zero fields: `pa.schema([])`. When serialized, it produces a
small fixed-size blob. Batches on the empty schema have zero columns.

## Appendix C: Empty Schema Serialized Form

The empty schema (`pa.schema([])`) serializes to a fixed 56-byte blob via
`pa.Schema.serialize()`. This is useful for cross-language implementations
that need to produce or compare serialized empty schemas (e.g., for the
`input_schema_bytes` field in state tokens for producer streams):

```
ff ff ff ff 30 00 00 00  10 00 00 00 00 00 0a 00
0c 00 06 00 05 00 08 00  0a 00 00 00 00 01 04 00
0c 00 00 00 08 00 08 00  00 00 04 00 08 00 00 00
04 00 00 00 00 00 00 00
```

This is a Flatbuffers-encoded Arrow Schema message with zero fields.
Implementations MAY hard-code this constant rather than generating it at
runtime. The serialized form is stable across Arrow versions.

## Appendix D: Request Metadata Location

A common implementation question: the `vgi_rpc.method` key appears in the
request batch's **custom metadata** (per-batch metadata), **not** in the
schema-level metadata. This is by design — schema-level metadata is part of
the IPC stream schema message and cannot vary between batches, while
custom metadata is per-batch and can carry request-specific values.
`vgi_rpc.request_version` is also in batch custom metadata.
