# Logging

vgi-rpc has two logging systems: **client-directed logs** (transmitted over the wire to the caller) and **server-side logs** (stdlib `logging` for observability).

## Client-Directed Logging

Server methods emit log messages that travel to the client's `on_log` callback:

```python
from vgi_rpc import CallContext, Level


class MyServiceImpl:
    def process(self, data: str, ctx: CallContext) -> str:
        ctx.client_log(Level.INFO, "Processing started", input_size=str(len(data)))
        result = data.upper()
        ctx.client_log(Level.DEBUG, "Processing complete")
        return result
```

The client receives them via the `on_log` callback:

```python
from vgi_rpc import Message, serve_pipe


def handle_log(msg: Message) -> None:
    print(f"[{msg.level.value}] {msg.message}")


with serve_pipe(MyService, MyServiceImpl(), on_log=handle_log) as proxy:
    proxy.process(data="hello")
    # [INFO] Processing started
    # [DEBUG] Processing complete
```

## Server-Side Logging

Use `ctx.logger` for server-side observability. Every log record automatically includes `server_id`, `method`, `principal`, and `remote_addr`:

```python
class MyServiceImpl:
    def process(self, data: str, ctx: CallContext) -> str:
        ctx.logger.info("Processing request", extra={"input_size": len(data)})
        return data.upper()
```

### Access log fields

Every completed RPC call emits one structured INFO record on the `vgi_rpc.access` logger with per-call I/O statistics:

| Field | Type | Description |
|---|---|---|
| `server_id` | `str` | Server instance identifier |
| `protocol` | `str` | Protocol class name |
| `method` | `str` | Method name |
| `method_type` | `str` | `"unary"` or `"stream"` |
| `principal` | `str` | Caller identity (empty if anonymous) |
| `auth_domain` | `str` | Authentication scheme (empty if anonymous) |
| `remote_addr` | `str` | Client address (HTTP only) |
| `duration_ms` | `float` | Call duration in milliseconds |
| `status` | `str` | `"ok"` or `"error"` |
| `error_type` | `str` | Exception class name (empty on success) |
| `http_status` | `int` | HTTP response status code (HTTP transport only) |
| `input_batches` | `int` | Number of input batches read by the server |
| `output_batches` | `int` | Number of output batches written by the server |
| `input_rows` | `int` | Total input rows |
| `output_rows` | `int` | Total output rows |
| `input_bytes` | `int` | Approximate logical input bytes |
| `output_bytes` | `int` | Approximate logical output bytes |

> **Note:** Byte counts use `pa.RecordBatch.get_total_buffer_size()` — logical Arrow buffer sizes without IPC framing overhead.

### Logger hierarchy

| Logger name | Purpose |
|---|---|
| `vgi_rpc` | Root — add handlers here |
| `vgi_rpc.access` | Auto access log (one per call) |
| `vgi_rpc.service.<Protocol>` | Developer `ctx.logger` per service |
| `vgi_rpc.rpc` | Framework lifecycle |
| `vgi_rpc.http` | HTTP transport lifecycle |
| `vgi_rpc.external` | External storage operations |
| `vgi_rpc.subprocess.stderr` | Child process stderr (`StderrMode.PIPE`) |
| `vgi_rpc.wire.request` | Request serialization / deserialization |
| `vgi_rpc.wire.response` | Response serialization / deserialization |
| `vgi_rpc.wire.batch` | Batch classification (log / error / data dispatch) |
| `vgi_rpc.wire.stream` | Stream session lifecycle |
| `vgi_rpc.wire.transport` | Transport lifecycle (pipe, subprocess) |
| `vgi_rpc.wire.http` | HTTP client requests / responses |

### Wire protocol debugging

Enable the `vgi_rpc.wire` hierarchy at DEBUG to see exactly what flows over the wire — request/response batches, metadata, batch classification, stream lifecycle, and transport events:

```python
import logging

logging.getLogger("vgi_rpc.wire").setLevel(logging.DEBUG)
logging.getLogger("vgi_rpc.wire").addHandler(logging.StreamHandler())
```

Sample output for a unary `add(a=1.0, b=2.0) -> 3.0` call:

```
DEBUG vgi_rpc.wire.request  Send request: method=add, type=unary, defaults_applied=[]
DEBUG vgi_rpc.wire.request  Write request: method=add, schema=(a: double, b: double), kwargs={a=1.0, b=2.0}, metadata={...}
DEBUG vgi_rpc.wire.request  Read request batch: RecordBatch(rows=1, cols=2, schema=(a: double, b: double), bytes=16), metadata={...}
DEBUG vgi_rpc.wire.response Write result batch: RecordBatch(rows=1, cols=1, schema=(result: double), bytes=8), route=inline
DEBUG vgi_rpc.wire.batch    Classify batch: rows=1, no metadata -> data
DEBUG vgi_rpc.wire.response Read unary response: method=add, result_type=float
```

All formatting is gated behind `isEnabledFor` guards — zero overhead when disabled.

### What to enable when

Rather than enabling everything, pick the loggers that match your scenario:

| Scenario | Loggers to enable |
|---|---|
| Method calls return wrong results | `vgi_rpc.wire.request` + `vgi_rpc.wire.response` — see schemas, kwargs, and result types |
| Client can't connect or hangs | `vgi_rpc.wire.transport` — see pipe/subprocess lifecycle and fd numbers |
| Streaming batches lost or out of order | `vgi_rpc.wire.stream` — see each tick, exchange, and close event |
| Log/error batches not arriving at client | `vgi_rpc.wire.batch` — see how each batch is classified (data vs. log vs. error) |
| HTTP transport issues | `vgi_rpc.wire.http` — see HTTP request URLs, response status codes, and body sizes |
| Need everything | `vgi_rpc.wire` — all six loggers at once |

Selectively enable only what you need:

```python
# Debug only request/response serialization
logging.getLogger("vgi_rpc.wire.request").setLevel(logging.DEBUG)
logging.getLogger("vgi_rpc.wire.request").addHandler(logging.StreamHandler())
```

### Debugging cross-language implementations

When building a vgi-rpc client or server in another language, wire logging on the Python side lets you see the exact Arrow IPC bytes and metadata your implementation needs to produce or consume. Run the Python side with full wire debugging to use it as a reference:

```python
import logging

# Full wire + access logging for interop debugging
logging.basicConfig(level=logging.DEBUG, format="%(name)-30s %(message)s")
logging.getLogger("vgi_rpc.wire").setLevel(logging.DEBUG)
logging.getLogger("vgi_rpc.access").setLevel(logging.INFO)
```

**What to verify for a non-Python client** (sending requests to a Python server):

1. **Schema metadata** — The request IPC stream schema must include `vgi_rpc.method` in schema-level metadata. `wire.request` logs show the expected schema and metadata for every inbound request.
2. **Batch metadata** — Every request batch must include `vgi_rpc.request_version` set to `"1"` in batch-level custom metadata. Missing or wrong version triggers a `VersionError`.
3. **Single-row batches** — Requests are always exactly one row. The column order must match the schema.
4. **IPC stream framing** — Each request/response is a complete Arrow IPC stream (schema message + record batch messages + EOS continuation bytes `0xFFFFFFFF 0x00000000`). Multiple streams are written sequentially on the same byte stream.

**What to verify for a non-Python server** (receiving requests from a Python client):

1. **Response classification** — `wire.batch` logs show how the Python client classifies each batch it reads. A data batch has `num_rows > 0` or no `vgi_rpc.log_level` in custom metadata. A log/error batch has `num_rows == 0` with `vgi_rpc.log_level` and `vgi_rpc.log_message` set.
2. **Error propagation** — Errors are zero-row batches with `vgi_rpc.log_level` set to `EXCEPTION`, `vgi_rpc.log_message` as the error text, and optionally `vgi_rpc.log_extra` with JSON `{"error_type": "...", "traceback": "..."}`.
3. **Stream protocol** — `wire.stream` logs show the expected lockstep sequence: init → (tick/input → output)* → close. Your server must write exactly one output batch per input batch.
4. **Server identity** — Include `vgi_rpc.server_id` in batch metadata on log and error batches for tracing. The Python client doesn't require it, but it helps with debugging.

**Common cross-language pitfalls:**

- Arrow metadata keys and values must be UTF-8 encoded bytes — some Arrow libraries default to other encodings
- The IPC stream EOS marker is 8 bytes (`0xFFFFFFFF` continuation token + `0x00000000` body size), not just closing the connection
- Zero-row batches are valid Arrow IPC — ensure your library can write and read them
- For HTTP transport, the content type must be `application/vnd.apache.arrow.stream` and the entire request/response body is a single IPC stream

### Production JSON logging

```python
import logging

from vgi_rpc.logging_utils import VgiJsonFormatter

handler = logging.StreamHandler()
handler.setFormatter(VgiJsonFormatter())
logging.getLogger("vgi_rpc").addHandler(handler)
logging.getLogger("vgi_rpc").setLevel(logging.INFO)
```

## API Reference

### Level

::: vgi_rpc.log.Level

### Message

::: vgi_rpc.log.Message

### VgiJsonFormatter

::: vgi_rpc.logging_utils.VgiJsonFormatter
