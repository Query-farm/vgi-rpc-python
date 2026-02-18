---
description: "vgi-rpc API reference — RpcServer, RpcConnection, Stream, AuthContext, transports, HTTP, introspection, external storage, and CLI."
---

# API Reference

## Where to Start

New to vgi-rpc? Follow this path:

1. **Define a service** — write a `Protocol` class ([Core RPC](core.md))
2. **Choose a transport** — `serve_pipe` for testing, `connect` for subprocess, `http_connect` for HTTP ([Transports](transports.md), [HTTP](http.md))
3. **Add streaming** — return `Stream[S]` for producer or exchange patterns ([Streaming](streaming.md))

Everything else is optional and can be added incrementally.

## Modules

| Module | Description | Required? |
|---|---|---|
| [Core RPC](core.md) | `RpcServer`, `RpcConnection`, errors, `serve_pipe`, `connect` | Yes |
| [Streaming](streaming.md) | `Stream`, `StreamState`, `ProducerState`, `ExchangeState` | If using streams |
| [Auth & Context](auth.md) | `AuthContext`, `CallContext`, `ClientLog` | If using auth or logging |
| [Transports](transports.md) | `PipeTransport`, `SubprocessTransport`, `ShmPipeTransport` | Built-in |
| [Serialization](serialization.md) | `ArrowSerializableDataclass`, `ArrowType`, `IpcValidation` | If using custom dataclasses |
| [HTTP](http.md) | `make_wsgi_app`, `http_connect`, `make_sync_client` | `pip install vgi-rpc[http]` |
| [Introspection](introspection.md) | `introspect`, `ServiceDescription`, `rpc_methods` | If using `enable_describe` |
| [External Storage](external.md) | `ExternalLocationConfig`, `S3Storage`, `GCSStorage` | `pip install vgi-rpc[s3\|gcs]` |
| [Logging](logging.md) | `Level`, `Message`, `VgiJsonFormatter` | If using client-directed logs |
| [OpenTelemetry](otel.md) | `OtelConfig`, `instrument_server` | `pip install vgi-rpc[otel]` |
| [CLI](cli.md) | `vgi-rpc describe` and `vgi-rpc call` commands | `pip install vgi-rpc[cli]` |

## Import Convention

All public symbols are re-exported from the top-level `vgi_rpc` package:

```python
from vgi_rpc import RpcServer, serve_pipe, Stream, AuthContext
```

Optional modules require their corresponding extras to be installed.
