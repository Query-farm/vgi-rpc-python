# Access Log

VGI servers emit a structured access log entry for every RPC request via
the `vgi_rpc.access` Python logger. When `--log-format json` is enabled,
each entry is a single JSON line on stderr containing transport-level
metrics, authentication context, and the full serialized request parameters.

## Enabling JSON Output

```bash
vgi-serve my_worker:MyWorker --http --log-format json
vgi-example-http --log-format json
```

For pipe (subprocess) transport, the access log is written to stderr
alongside other worker logs.

## Fields

Every access log entry includes these fields:

### Transport Fields (from `vgi_rpc`)

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 timestamp |
| `server_id` | string | Short hex identifier for this server instance |
| `protocol` | string | Protocol name (e.g. `VgiProtocol`) |
| `method` | string | RPC method name (e.g. `bind`, `init`, `catalog_schemas`) |
| `method_type` | string | `unary` or `stream` |
| `principal` | string | Caller identity (empty if unauthenticated) |
| `auth_domain` | string | Authentication scheme (e.g. `bearer`, `jwt`, empty if none) |
| `authenticated` | bool | Whether the caller was authenticated |
| `remote_addr` | string | Client IP address (HTTP only) |
| `duration_ms` | float | Request duration in milliseconds |
| `status` | string | `ok` or `error` |
| `error_type` | string | Exception class name (empty on success) |
| `request_id` | string | Per-request correlation ID (16-char hex) |
| `http_status` | int | HTTP status code (HTTP only) |
| `input_batches` | int | Number of input Arrow batches |
| `output_batches` | int | Number of output Arrow batches |
| `input_rows` | int | Total input rows |
| `output_rows` | int | Total output rows |
| `input_bytes` | int | Approximate input bytes (Arrow buffer size) |
| `output_bytes` | int | Approximate output bytes (Arrow buffer size) |

### Enrichment Fields

| Field | Type | When Present | Description |
|-------|------|-------------|-------------|
| `server_version` | string | Always | VGI package version |
| `request_data` | string | Dispatch methods | Base64-encoded Arrow IPC bytes of the request batch. Contains all call parameters: function name, arguments, schemas, pushdown filters, catalog names, etc. |
| `stream_id` | string | Stream methods | UUID hex correlating all HTTP requests in one logical stream execution |
| `claims` | object | Authenticated requests with claims | JWT/OAuth claims dict |
| `error_message` | string | Error entries | Raw exception message (truncated to 500 chars) |
| `request_state` | string | HTTP stream continuations | Base64-encoded state token received from the client |
| `response_state` | string | HTTP exchange stream init/continuations | Base64-encoded serialized state returned to the client |

## Request Types and Their Fields

### `bind` (unary)

Schema resolution and argument validation. Has `request_data` containing
the function name, type, arguments, input schema, and settings.

### `init` (stream)

Stream initialization. Has `request_data` with the full `InitRequest`
(including bind call, output schema, pushdown filters, projection IDs,
order-by hints), plus `stream_id` for correlation.

For exchange streams (scalar, table-in-out), also has `response_state`
with the initial serialized state token.

### `init` continuations (HTTP stream exchanges)

Subsequent HTTP requests in the same stream. Have `stream_id` (from the
state token) and `request_state` (the token the client sent). Exchange
continuations also have `response_state` (the updated token). The
`request_data` field is absent on continuations since the call parameters
were logged on the initial `init` entry.

For producer streams (table functions) that complete in a single HTTP
request, there are no continuations — all data is returned inline.

### `table_function_cardinality` (unary)

Cardinality estimation. Has `request_data` with the bind call.

### `catalog_*` (unary)

Catalog operations (attach, schemas, table_get, etc.). Have `request_data`
with the method parameters (attach_id, schema_name, table_name, etc.).

## Correlating Stream Requests

Over HTTP transport, a single DuckDB query may generate multiple HTTP
requests: one `bind`, one `init`, and zero or more exchange/produce
continuations. The `stream_id` field (a UUID) is the same across all
requests in one stream, allowing analysis tools to reconstruct the
full execution:

```sql
-- Find all requests for a specific stream
SELECT * FROM access_log WHERE stream_id = 'abcdef1234567890'
ORDER BY timestamp;
```

The initial `init` entry has the full `request_data` with function name,
arguments, and pushdown info. Continuation entries have per-request
`duration_ms`, `input_rows`, `output_rows`, etc.

## Decoding `request_data`

The `request_data` field contains base64-encoded Arrow IPC bytes of the
request batch. Each column in the batch is a method parameter. To decode:

```python
import base64
import pyarrow as pa

request_data = "QVJST1cxAAA..."  # from the log entry
batch_bytes = base64.b64decode(request_data)
reader = pa.ipc.open_stream(batch_bytes)
batch = reader.read_all().to_batches()[0]
print(batch.to_pydict())
# {'function_name': ['sequence'], 'arguments': [...], 'function_type': [1], ...}
```

Note: `BindRequest` contains a `secrets` field which may include sensitive
data (database passwords, API keys). If your access logs are forwarded to
a log aggregation system, use an external pipeline step to strip the
`secrets` column before ingestion.

## Conformance Validation

The `vgi_rpc.access_log_conformance` module validates that a server's
access log output meets the required field presence rules. This is
language-agnostic: any VGI server implementation can be validated.

```bash
# Validate a log file
python -m vgi_rpc.access_log_conformance /tmp/vgi-http-test-server.log

# Pipe from stdin
cat server.log | python -m vgi_rpc.access_log_conformance
```

Rules enforced:

- All entries must have `server_version` and `authenticated`
- Dispatch methods (`bind`, `init`, `catalog_*`, `table_function_cardinality`) must have `request_data`
- Stream methods (`init`) must have `stream_id`
- Error entries must have `error_message`

## Example Entry

```json
{
  "timestamp": "2026-04-12T15:30:00.123",
  "level": "INFO",
  "logger": "vgi_rpc.access",
  "message": "VgiProtocol.init ok",
  "server_id": "a85cd92650b7",
  "protocol": "VgiProtocol",
  "method": "init",
  "method_type": "stream",
  "principal": "alice@example.com",
  "auth_domain": "jwt",
  "authenticated": true,
  "remote_addr": "10.0.0.1",
  "duration_ms": 12.3,
  "status": "ok",
  "error_type": "",
  "server_version": "0.2.0",
  "request_id": "f41f090e23b84789",
  "http_status": 200,
  "request_data": "QVJST1cxAAA...",
  "stream_id": "85b7099a1e9b4a6b9d1fc917f27212ee",
  "claims": {"sub": "alice@example.com", "aud": "vgi"},
  "response_state": "AAAA/////6gA...",
  "input_batches": 1,
  "output_batches": 2,
  "input_rows": 1,
  "output_rows": 1,
  "input_bytes": 5864,
  "output_bytes": 41
}
```

## Central Publishing

The access log uses Python's standard `logging` module. Attach any
handler to `vgi_rpc.access` for forwarding to external systems:

```python
import logging

# Forward to an HTTP endpoint
handler = logging.handlers.HTTPHandler(
    "analytics.example.com", "/ingest", method="POST"
)
logging.getLogger("vgi_rpc.access").addHandler(handler)
```

Container log collectors (fluentd, Vector, promtail) can parse the JSON
lines from stderr directly when `--log-format json` is enabled.
