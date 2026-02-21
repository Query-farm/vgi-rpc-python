---
description: "vgi-rpc: a transport-agnostic Python RPC framework built on Apache Arrow IPC serialization. Typed proxies, streaming, shared memory, and HTTP transport."
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<div class="hero-logo" markdown>
![vgi-rpc logo](assets/logo-hero.png){ .hero-logo-img }
</div>

# vgi-rpc

Transport-agnostic RPC framework built on [Apache Arrow](https://arrow.apache.org/) IPC serialization.

<p class="built-by">Built by <a href="https://query.farm">🚜 Query.Farm</a></p>

</div>

Define RPC interfaces as Python `Protocol` classes. The framework derives Arrow schemas from type annotations and provides typed client proxies with automatic serialization/deserialization.

## Key Features

- **Protocol-based interfaces** — define services as typed Python Protocol classes; proxies preserve the Protocol type for full IDE autocompletion
- **Apache Arrow IPC wire format** — zero-copy serialization for structured data
- **Two method types** — unary and streaming (producer and exchange patterns)
- **Transport-agnostic** — in-process pipes, subprocess, Unix domain sockets, shared memory, or HTTP
- **Automatic schema inference** — Python type annotations map to Arrow types
- **Pluggable authentication** — `AuthContext` + middleware for HTTP auth (JWT, API key, etc.)
- **Runtime introspection** — opt-in `__describe__` RPC method for dynamic service discovery
- **CLI tool** — `vgi-rpc describe` and `vgi-rpc call` for ad-hoc service interaction
- **Shared memory transport** — zero-copy batch transfer between co-located processes
- **IPC validation** — configurable batch validation levels for untrusted data
- **Large batch support** — transparent externalization to S3/GCS for oversized data
- **Per-call I/O statistics** — `CallStatistics` tracks batches, rows, and bytes for usage accounting (access log + OTel spans)
- **Wire protocol debug logging** — enable `vgi_rpc.wire` at DEBUG for full wire-level visibility

## Two Method Types

vgi-rpc supports two RPC patterns. The method's return type in the Protocol determines which one is used:

### Unary

A single request produces a single response — like a function call across a process boundary. The client sends parameters, the server returns a result.

```d2
shape: sequence_diagram
Client -> Server: "add(a=2, b=3)"
Server -> Server: compute
Server -> Client: "5.0"
```

Use unary for: lookups, computations, CRUD operations — anything that returns one value.

### Streaming

A single request opens an ongoing session that produces multiple batches of data. The return type `Stream[S]` signals a streaming method, where `S` is a `StreamState` subclass that holds state between iterations.

There are two streaming patterns:

**Producer** — the server pushes data to the client (like a generator). The client iterates, the server emits batches until calling `out.finish()`:

```d2
shape: sequence_diagram
Client -> Server: "countdown(n=3)"
Server -> Client: "{value: [3]}"
Server -> Client: "{value: [2]}"
Server -> Client: "{value: [1]}"
Server -> Client: "[finish]"
```

**Exchange** — lockstep bidirectional streaming. The client sends data, the server responds, one round at a time:

```d2
shape: sequence_diagram
Client -> Server: "transform(factor=2)"
Client -> Server: "{value: [10]}"
Server -> Client: "{result: [20]}"
Client -> Server: "{value: [5]}"
Server -> Client: "{result: [10]}"
Client -> Server: "[close]"
```

Use streaming for: pagination, progress reporting, incremental computation, or any workflow where data is produced or exchanged over time.

## Installation

```bash
pip install vgi-rpc
```

Optional extras:

```bash
pip install vgi-rpc[http]       # HTTP transport (Falcon + httpx)
pip install vgi-rpc[s3]         # S3 storage backend
pip install vgi-rpc[gcs]        # Google Cloud Storage backend
pip install vgi-rpc[cli]        # CLI tool (typer + httpx)
pip install vgi-rpc[external]   # External storage fetch (aiohttp + zstandard)
pip install vgi-rpc[otel]       # OpenTelemetry instrumentation
```

Requires Python 3.13+.

## Quick Start

Define a service as a `Protocol`, implement it, and call methods through a typed proxy:

```python
from typing import Protocol

from vgi_rpc import serve_pipe


class Calculator(Protocol):
    """A simple calculator service."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...


class CalculatorImpl:
    """Calculator implementation."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b


with serve_pipe(Calculator, CalculatorImpl()) as proxy:
    print(proxy.add(a=2.0, b=3.0))  # 5.0
```

See the [Examples](examples.md) page for streaming, HTTP transport, authentication, and more.

## Limitations

vgi-rpc is designed for Python-to-Python RPC with structured, tabular data. Some things it deliberately does not do:

- **Python only** — no cross-language code generation. The wire format (Arrow IPC) is language-neutral, but there are no client/server libraries for other languages.
- **No full-duplex streaming** — the exchange pattern is lockstep (one request, one response, repeat), not concurrent bidirectional like gRPC.
- **No client streaming** — the client cannot push a stream of batches to the server independently. Use exchange for bidirectional workflows.
- **Columnar data model** — all data crosses the wire as Arrow RecordBatches. Scalar values are wrapped in single-row batches. If your payloads are small heterogeneous messages, a row-oriented format (protobuf, JSON) may be more natural.
- **No service mesh integration** — no built-in load balancing, circuit breaking, or service discovery. The HTTP transport is a standard WSGI app, so you can put it behind any reverse proxy.
- **No async server** — the server is synchronous. Streaming methods run in a blocking loop. This keeps the implementation simple but limits concurrency to one request at a time per connection (HTTP transport handles concurrency at the WSGI layer).

## Next Steps

- Browse the [API Reference](api/index.md) for detailed documentation
- Check out the [Examples](examples.md) for runnable scripts
- See the [Contributing](contributing.md) guide to get involved

---

<p style="text-align: center; opacity: 0.7;">
  <a href="https://query.farm">🚜 Query.Farm</a>
</p>

