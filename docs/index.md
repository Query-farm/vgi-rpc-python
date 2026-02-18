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
- **Transport-agnostic** — in-process pipes, subprocess, shared memory, or HTTP
- **Automatic schema inference** — Python type annotations map to Arrow types
- **Pluggable authentication** — `AuthContext` + middleware for HTTP auth (JWT, API key, etc.)
- **Runtime introspection** — opt-in `__describe__` RPC method for dynamic service discovery
- **CLI tool** — `vgi-rpc describe` and `vgi-rpc call` for ad-hoc service interaction
- **Shared memory transport** — zero-copy batch transfer between co-located processes
- **Large batch support** — transparent externalization to S3/GCS for oversized data
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

Requires Python 3.12+.

## Quick Start

### Unary RPC

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

### Streaming

Return `Stream[S]` where `S` is a state class that produces batches. Producer streams push data to the client; exchange streams accept input each iteration:

```python
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import CallContext, OutputCollector, ProducerState, Stream, StreamState, serve_pipe


@dataclass
class CountdownState(ProducerState):
    """Counts down from n to 0."""

    n: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one value per tick, then finish."""
        if self.n <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.n]})
        self.n -= 1


class CountdownService(Protocol):
    """Service with a producer stream."""

    def countdown(self, n: int) -> Stream[StreamState]:
        """Count down from n."""
        ...


class CountdownImpl:
    """Countdown implementation."""

    def countdown(self, n: int) -> Stream[CountdownState]:
        """Count down from n."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=CountdownState(n=n))


with serve_pipe(CountdownService, CountdownImpl()) as proxy:
    for batch in proxy.countdown(n=3):
        print(batch.batch.to_pydict())
    # {'value': [3]}
    # {'value': [2]}
    # {'value': [1]}
```

See the [Examples](examples.md) page for exchange streams, HTTP transport, authentication, and more.

## Next Steps

- Browse the [API Reference](api/index.md) for detailed documentation
- Check out the [Examples](examples.md) for runnable scripts
- See the [Contributing](contributing.md) guide to get involved

---

<p style="text-align: center; opacity: 0.7;">
  <a href="https://query.farm">🚜 Query.Farm</a>
</p>

