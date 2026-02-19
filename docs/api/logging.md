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
| `duration_ms` | `float` | Call duration in milliseconds |
| `status` | `str` | `"ok"` or `"error"` |
| `error_type` | `str` | Exception class name (empty on success) |
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

All formatting is gated behind `isEnabledFor` guards — zero overhead when disabled. Enable individual sub-loggers (e.g. `vgi_rpc.wire.request` only) for targeted debugging.

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
