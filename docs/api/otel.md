# OpenTelemetry

Server-side OpenTelemetry instrumentation for distributed tracing and metrics. Requires `pip install vgi-rpc[otel]`.

## Usage

```python
from vgi_rpc import RpcServer
from vgi_rpc.otel import OtelConfig, instrument_server

server = RpcServer(MyService, MyServiceImpl())
instrument_server(server)  # uses global TracerProvider / MeterProvider
```

This creates spans and records metrics for every RPC call:

- **Span**: includes method name, service name, auth context, duration, error status, and per-call I/O statistics
- **Counter**: `rpc.server.requests` — number of RPC requests handled
- **Histogram**: `rpc.server.duration` — duration in seconds

Trace context propagates automatically: from contextvars (pipe transport) or W3C headers (HTTP transport).

## Span Attributes

Each span carries the following attributes:

| Attribute | Type | Description |
|---|---|---|
| `rpc.system` | `str` | Always `"vgi_rpc"` |
| `rpc.service` | `str` | Protocol class name |
| `rpc.method` | `str` | Method name |
| `rpc.vgi_rpc.method_type` | `str` | `"unary"` or `"stream"` |
| `rpc.vgi_rpc.server_id` | `str` | Server instance identifier |
| `enduser.id` | `str` | Caller principal (when authenticated) |
| `rpc.vgi_rpc.auth.domain` | `str` | Auth scheme (when authenticated) |
| `rpc.vgi_rpc.auth.authenticated` | `str` | `"true"` or `"false"` |
| `net.peer.ip` | `str` | Client IP address (HTTP only) |
| `user_agent.original` | `str` | User agent string (HTTP only) |
| `rpc.vgi_rpc.input_batches` | `int` | Number of input batches read by the server |
| `rpc.vgi_rpc.output_batches` | `int` | Number of output batches written by the server |
| `rpc.vgi_rpc.input_rows` | `int` | Total rows across all input batches |
| `rpc.vgi_rpc.output_rows` | `int` | Total rows across all output batches |
| `rpc.vgi_rpc.input_bytes` | `int` | Approximate logical bytes across all input batches |
| `rpc.vgi_rpc.output_bytes` | `int` | Approximate logical bytes across all output batches |
| `rpc.vgi_rpc.error_type` | `str` | Exception class name (error spans only) |

The I/O statistics attributes (`input_batches` through `output_bytes`) are populated from `CallStatistics` — see [Call Statistics](../README.md#call-statistics) for counting semantics.

> **Note:** Byte counts use `pa.RecordBatch.get_total_buffer_size()` — logical Arrow buffer sizes without IPC framing overhead.

## API Reference

### OtelConfig

::: vgi_rpc.otel.OtelConfig

### instrument_server

::: vgi_rpc.otel.instrument_server
