# vgi-rpc Wire Protocol Specification

**Version**: 1
**Status**: Normative
**Audience**: Cross-language implementors (Go, Rust, TypeScript, C++, etc.)

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
| `vgi_rpc.request_id` | UTF-8 string (16-char hex) | Per-request correlation ID. Optional. |
| `traceparent` | W3C Trace Context string | OpenTelemetry trace propagation. Optional. |
| `tracestate` | W3C Trace Context string | OpenTelemetry trace state. Optional. |
| `vgi_rpc.shm_segment_name` | UTF-8 OS name | Shared memory segment name (session-level). Optional. |
| `vgi_rpc.shm_segment_size` | Decimal integer string | Shared memory segment total size in bytes. Optional. |

### Response / log / error metadata (on response batch custom metadata)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.log_level` | One of: `EXCEPTION`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` | Severity level. Present on log and error batches. |
| `vgi_rpc.log_message` | UTF-8 string | Human-readable message text. |
| `vgi_rpc.log_extra` | JSON string | Additional structured data. Optional. |
| `vgi_rpc.server_id` | UTF-8 string (12-char hex) | Server instance identifier for distributed tracing. |
| `vgi_rpc.request_id` | UTF-8 string | Echoed request correlation ID. |

### Stream state (HTTP transport)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.stream_state` | Opaque binary (signed token) | Serialized stream state for stateless HTTP exchanges. |

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
| `vgi_rpc.location.fetch_ms` | Decimal float string (e.g. `"42.3"`) | Fetch duration in milliseconds (diagnostics, on resolved batches). |
| `vgi_rpc.location.source` | UTF-8 URL | Original fetch URL (diagnostics, on resolved batches). |

### Introspection batch metadata (on `__describe__` response batch `custom_metadata`)

| Key (bytes) | Value | Description |
|-------------|-------|-------------|
| `vgi_rpc.protocol_name` | UTF-8 string | Protocol class name. |
| `vgi_rpc.request_version` | `"1"` | Wire protocol version. |
| `vgi_rpc.describe_version` | `"2"` | Introspection format version. |
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
| `dataclass` (nested) | `binary` | Serialized as a complete Arrow IPC stream (schema + 1-row batch + EOS) in a `binary` column. Recursively applies the same type mapping. |

### Serialization transforms

When writing a value to an Arrow column:

- **Enum** → write the member's **name** as a UTF-8 string (not its value).
- **dict** → convert to a list of `(key, value)` tuples, then write as `map(K, V)`.
- **frozenset/set** → convert to a list, then write as `list(T)`.
- **Nested dataclass** → serialize to Arrow IPC bytes, write as `binary`.

When reading:

- **Enum** → look up the string by member **name** first; fall back to member **value** for legacy compatibility.
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

  IF custom_metadata contains "vgi_rpc.shm_offset":
    → SHM POINTER batch → resolve via shared memory (see Section 11)

  IF custom_metadata contains "vgi_rpc.location":
    → EXTERNAL POINTER batch → resolve via URL fetch (see Section 12)

  IF custom_metadata contains "vgi_rpc.stream_state":
    → STATE TOKEN batch → stream continuation (see Section 10)

  // Zero-row batch with unrecognized metadata
  → DATA batch (e.g., void return, stream-finish marker)
```

> **Note**: Log-level keys take priority. A zero-row batch that has both
> `vgi_rpc.log_level` and `vgi_rpc.shm_offset` is classified as a log batch,
> not an SHM pointer. This is by design — SHM pointer detection explicitly
> excludes batches with log-level keys.

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
error stream (with EXCEPTION-level metadata) in place of the header stream.

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

| Endpoint | HTTP Method | Description |
|----------|-------------|-------------|
| `{prefix}/{method}` | POST | Unary RPC call |
| `{prefix}/{method}/init` | POST | Stream initialization (producer and exchange) |
| `{prefix}/{method}/exchange` | POST | Stream continuation / exchange |
| `{prefix}/__describe__` | POST | Introspection (unary) |
| `{prefix}/__upload_url__/init` | POST | Upload URL generation (when enabled) |
| `{prefix}/__capabilities__` | OPTIONS | Server capability discovery |

### Request headers

| Header | Description |
|--------|-------------|
| `Content-Type` | MUST be `application/vnd.apache.arrow.stream` |
| `X-Request-ID` | Optional. Correlation ID echoed on response. If absent, server generates one. |

### Response headers

| Header | Description |
|--------|-------------|
| `X-Request-ID` | Echoed or generated request correlation ID. |
| `VGI-Max-Request-Bytes` | Server-advertised maximum request body size (optional). |
| `VGI-Upload-URL-Support` | `"true"` when upload URL endpoint is available (optional). |
| `VGI-Max-Upload-Bytes` | Server-advertised maximum upload size (optional). |

### Unary call (HTTP)

```
POST {prefix}/{method}

Request body:  IPC stream (params_schema, 1 request row, EOS)
Response body: IPC stream (result_schema, 0..N log batches, 1 result/error batch, EOS)

HTTP 200: Success (even when the response contains an error batch)
HTTP 400: Protocol error (bad IPC, missing metadata, param validation failure)
HTTP 401: Authentication failure (plain-text body, NOT Arrow IPC)
HTTP 404: Unknown method
HTTP 415: Wrong Content-Type
HTTP 500: Server implementation error
```

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

All produced data batches are included inline. If the response would exceed
`max_stream_response_bytes`, the server truncates the output and appends a
**continuation batch**: a zero-row batch with `vgi_rpc.stream_state` in its
custom metadata. The client then follows up with `/exchange` requests.

#### Exchange stream init response

```
Response body:
  [IPC stream: header_schema, 0..N log batches, 1 header row, EOS]   (if header declared)
  [IPC stream: output_schema, 0..N log batches, 1 zero-row batch with state token, EOS]
```

The zero-row batch carries the signed state token in
`vgi_rpc.stream_state` custom metadata.

### Stream exchange (HTTP)

```
POST {prefix}/{method}/exchange

Request body:  IPC stream (input_schema, 1 input batch with state token in metadata, EOS)
Response body: IPC stream (output_schema, 0..N log batches, 1 data batch with updated state token, EOS)
```

The request batch's custom metadata MUST contain `vgi_rpc.stream_state`
with the current state token.

For **producer continuation**, the input is a zero-row batch on empty schema
with the state token. The response may contain multiple data batches and
may end with another continuation token.

For **exchange**, the input carries real data plus the state token. The
response data batch carries an updated state token for the next exchange.

The client MUST strip `vgi_rpc.stream_state` from the batch metadata before
exposing it to application code.

### State token binary format

The state token is an opaque signed blob with the following wire format:

```
Offset  Size     Field
0       1        version: uint8 (currently 1)
1       4        state_len: uint32 LE
5       N        state_bytes: Arrow IPC stream of the StreamState dataclass
5+N     4        schema_len: uint32 LE
9+N     M        schema_bytes: serialized output pa.Schema
9+N+M   4        input_schema_len: uint32 LE
13+N+M  P        input_schema_bytes: serialized input pa.Schema
13+N+M+P 32      HMAC-SHA256(signing_key, all preceding bytes)
```

- **`state_bytes`**: The stream state dataclass serialized as a complete Arrow IPC stream (schema + 1-row batch + EOS).
- **`schema_bytes`**: The output Arrow schema serialized via `pa.Schema.serialize()`.
- **`input_schema_bytes`**: The input Arrow schema serialized via `pa.Schema.serialize()`. For producer streams, this is the serialized empty schema.
- **HMAC**: SHA-256 HMAC over the entire payload (version byte through input_schema_bytes), using the server's signing key.

**Verification order**: The HMAC MUST be verified **before** inspecting any
payload fields (including the version byte) to prevent information leakage.

### Authentication (HTTP)

When the server has an `authenticate` callback configured:

- The callback receives the HTTP request and returns an `AuthContext`.
- On failure (`ValueError` or `PermissionError`), the server returns **HTTP 401** with a **plain-text** body (NOT Arrow IPC), because no method has been resolved yet and no output schema is available.
- Other exceptions from the callback propagate as HTTP 500.
- Clients MUST detect 401 responses before attempting to parse Arrow IPC.

---

## 11. Shared Memory (SHM) Transport

The shared memory side-channel enables zero-copy batch transfer between
co-located processes. It is used alongside a pipe transport — the pipe
carries control messages and small batches; large batches are written to
shared memory and replaced with pointer batches on the pipe.

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

For **dictionary-encoded batches**: The IPC stream is written to a temporary buffer, then the schema message and EOS marker are stripped — only the dictionary messages and record batch message are stored in SHM. On deserialization, the schema is reconstructed from the pointer batch's schema and prepended to recreate a valid IPC stream.

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
- **Must NOT contain** `vgi_rpc.log_level` (to distinguish from log batches).

### Externalization (writing)

When a data batch's total buffer size exceeds the threshold:

1. Serialize **all** batches from the current output cycle (log batches + data batch) as a single IPC stream.
2. Optionally compress with zstd.
3. Upload to external storage via the `ExternalStorage.upload()` interface.
4. Replace the entire cycle with a single zero-row pointer batch containing `vgi_rpc.location`.

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

  // Fetch with retries (max 3 attempts)
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
| `vgi_rpc.method` missing | `RpcError` (ProtocolError) — server writes an error stream. |
| Unknown method name | `RpcError` (AttributeError) — error stream includes available method names. |
| Request batch has wrong row count (not 1, on non-empty schema) | `RpcError` (ProtocolError). |
| Non-optional parameter is null | `TypeError` — error stream on the method's result schema. |

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

| Error condition | HTTP status |
|----------------|-------------|
| Bad IPC, missing metadata, version mismatch, param validation | 400 Bad Request |
| Authentication failure | 401 Unauthorized |
| Unknown method | 404 Not Found |
| Wrong Content-Type | 415 Unsupported Media Type |
| Implementation error (unary) | 500 Internal Server Error |
| Implementation error (stream init) | 500 Internal Server Error |
| Type error in implementation | 400 Bad Request |

> **Note**: Even for HTTP 400/500 responses, the response body is a valid
> Arrow IPC stream containing an error batch, except for 401 (plain text)
> and 415 (Falcon default response).

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
- `vgi_rpc.describe_version` — Introspection format version (`"2"`)
- `vgi_rpc.server_id` — Server instance identifier

### Response batch schema

| Column | Arrow type | Nullable | Description |
|--------|-----------|----------|-------------|
| `name` | `utf8` | No | Method name |
| `method_type` | `utf8` | No | `"unary"` or `"stream"` |
| `doc` | `utf8` | Yes | Method docstring |
| `has_return` | `bool` | No | Whether the unary method returns a value |
| `params_schema_ipc` | `binary` | No | Serialized `pa.Schema` for request parameters |
| `result_schema_ipc` | `binary` | No | Serialized `pa.Schema` for unary response |
| `param_types_json` | `utf8` | Yes | JSON: `{"param_name": "type_name", ...}` |
| `param_defaults_json` | `utf8` | Yes | JSON: `{"param_name": default_value, ...}` |
| `has_header` | `bool` | No | Whether the stream method has a header type |
| `header_schema_ipc` | `binary` | Yes | Serialized `pa.Schema` for the header (null if no header) |

The `params_schema_ipc`, `result_schema_ipc`, and `header_schema_ipc`
columns contain Arrow schemas serialized via `pa.Schema.serialize()`.

---

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

## Appendix C: Request Metadata Location

A common implementation question: the `vgi_rpc.method` key appears in the
request batch's **custom metadata** (per-batch metadata), **not** in the
schema-level metadata. This is by design — schema-level metadata is part of
the IPC stream schema message and cannot vary between batches, while
custom metadata is per-batch and can carry request-specific values.
`vgi_rpc.request_version` is also in batch custom metadata.
