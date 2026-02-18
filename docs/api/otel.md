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

- **Span**: includes method name, service name, auth context, duration, and error status
- **Counter**: `rpc.server.requests` — number of RPC requests handled
- **Histogram**: `rpc.server.duration` — duration in seconds

Trace context propagates automatically: from contextvars (pipe transport) or W3C headers (HTTP transport).

## API Reference

### OtelConfig

::: vgi_rpc.otel.OtelConfig

### instrument_server

::: vgi_rpc.otel.instrument_server
