# vgi-rpc

Transport-agnostic RPC framework built on [Apache Arrow](https://arrow.apache.org/) IPC serialization.

Define RPC interfaces as Python `Protocol` classes. The framework derives Arrow schemas from type annotations and provides typed client proxies with automatic serialization/deserialization.

**Key features:**

- **Protocol-based interfaces** — define services as typed Python Protocol classes
- **Apache Arrow IPC wire format** — zero-copy serialization for structured data
- **Three method types** — unary, server-streaming, and bidirectional streaming
- **Transport-agnostic** — in-process pipes, subprocess, or HTTP
- **Automatic schema inference** — Python type annotations map to Arrow types
- **Pluggable authentication** — `AuthContext` + middleware for HTTP auth (JWT, API key, etc.)
- **Large batch support** — transparent externalization to S3/GCS for oversized data

## Installation

```bash
pip install vgi-rpc
```

Optional extras:

```bash
pip install vgi-rpc[http]   # HTTP transport (Falcon + httpx)
pip install vgi-rpc[s3]     # S3 storage backend
pip install vgi-rpc[gcs]    # Google Cloud Storage backend
```

Requires Python 3.12+.

## Quick Start

```python
from typing import Protocol

from vgi_rpc import serve_pipe


# 1. Define your service as a Protocol
class Calculator(Protocol):
    """A simple calculator service."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def greet(self, name: str) -> str:
        """Greet by name."""
        ...


# 2. Implement it
class CalculatorImpl:
    """Calculator implementation."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def greet(self, name: str) -> str:
        """Greet by name."""
        return f"Hello, {name}!"


# 3. Serve and call methods through a typed proxy
with serve_pipe(Calculator, CalculatorImpl()) as proxy:
    result = proxy.add(2.0, 3.0)
    print(result)  # 5.0

    greeting = proxy.greet("World")
    print(greeting)  # Hello, World!
```

## Defining Services

Services are defined as `Protocol` classes. The return type annotation determines the method type:

| Return type | Method type | Description |
|---|---|---|
| `-> T` | **Unary** | Single request, single response |
| `-> ServerStream[S]` | **Server stream** | Single request, stream of batches |
| `-> BidiStream[S]` | **Bidi stream** | Bidirectional lockstep exchange |

```python
from typing import Protocol

from vgi_rpc import BidiStream, BidiStreamState, ServerStream, ServerStreamState


class MyService(Protocol):
    """Example service with all three method types."""

    def echo(self, message: str) -> str:
        """Unary: single request/response."""
        ...

    def generate(self, count: int) -> ServerStream[ServerStreamState]:
        """Server stream: produces multiple batches."""
        ...

    def transform(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi stream: lockstep input/output exchange."""
        ...
```

### Supported types

Parameters and return types are automatically mapped to Arrow types:

| Python type | Arrow type |
|---|---|
| `str` | `utf8` |
| `bytes` | `binary` |
| `int` | `int64` |
| `float` | `float64` |
| `bool` | `bool_` |
| `list[T]` | `list_<T>` |
| `dict[K, V]` | `map_<K, V>` |
| `frozenset[T]` | `list_<T>` |
| `Enum` | `dictionary(int32, utf8)` |
| `Optional[T]` | nullable `T` |
| `ArrowSerializableDataclass` | `struct` |
| `Annotated[T, ArrowType(...)]` | explicit override |

### Parameter defaults

Protocol methods can declare default parameter values. These are used when the client omits the argument:

```python
from typing import Protocol


class SearchService(Protocol):
    """Service with default parameters."""

    def search(self, query: str, limit: int = 10) -> str:
        """Search with an optional limit."""
        ...
```

### Introspection

Use `rpc_methods()` to inspect a protocol's methods, or `describe_rpc()` for a human-readable summary:

```python
from vgi_rpc import describe_rpc, rpc_methods

methods = rpc_methods(Calculator)
for name, info in methods.items():
    print(f"{name}: {info.method_type.value}, params={info.params_schema}")

print(describe_rpc(Calculator))
```

## Serialization — ArrowSerializableDataclass

Use `ArrowSerializableDataclass` as a mixin for dataclasses that need Arrow serialization. The Arrow schema is generated automatically from field annotations.

```python
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa

from vgi_rpc import ArrowSerializableDataclass, ArrowType


@dataclass(frozen=True)
class Measurement(ArrowSerializableDataclass):
    """A measurement with auto-generated Arrow schema."""

    timestamp: str
    value: float
    count: Annotated[int, ArrowType(pa.int32())]  # explicit Arrow type override


# Serialize / deserialize
m = Measurement(timestamp="2024-01-01T00:00:00Z", value=42.0, count=7)
data = m.serialize_to_bytes()
m2 = Measurement.deserialize_from_bytes(data)
assert m == m2

# Access the auto-generated schema
print(Measurement.ARROW_SCHEMA)
```

These dataclasses can be used directly as RPC parameters and return types:

```python
from typing import Protocol


class DataService(Protocol):
    """Service that uses dataclass parameters."""

    def record(self, measurement: Measurement) -> str:
        """Accept a dataclass parameter."""
        ...

    def latest(self) -> Measurement:
        """Return a dataclass result."""
        ...
```

## Transports

### Pipe (in-process)

`serve_pipe` starts a server on a background thread and yields a typed proxy. No subprocess needed — useful for tests and embedded use:

```python
from vgi_rpc import serve_pipe

with serve_pipe(Calculator, CalculatorImpl()) as proxy:
    print(proxy.add(1.0, 2.0))  # 3.0
```

### Subprocess

Run the server in a child process over stdin/stdout. The server entry point calls `run_server`:

```python
# worker.py
from vgi_rpc import RpcServer, run_server

from my_service import MyService, MyServiceImpl

run_server(MyService, MyServiceImpl())
```

The client connects with `connect`:

```python
import sys

from vgi_rpc import connect

with connect(MyService, [sys.executable, "worker.py"]) as proxy:
    result = proxy.echo("hello")
```

### HTTP

Requires `pip install vgi-rpc[http]`. The server exposes a Falcon WSGI app; the client uses httpx:

```python
from vgi_rpc import RpcServer, http_connect, make_wsgi_app

# Server
server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server)
# Serve `app` with any WSGI server (waitress, gunicorn, etc.)

# Client
with http_connect(MyService, "http://localhost:8080") as proxy:
    result = proxy.echo("hello")
```

`make_wsgi_app` options:

| Parameter | Default | Description |
|---|---|---|
| `prefix` | `"/vgi"` | URL prefix for all RPC endpoints |
| `signing_key` | random 32 bytes | HMAC key for signing state tokens |
| `max_stream_response_bytes` | `None` | Split server-stream responses across multiple HTTP exchanges |
| `authenticate` | `None` | Callback `(falcon.Request) -> AuthContext` for request authentication |

## Streaming

### Server Streams

A server stream method returns `ServerStream[S]` where `S` is a `ServerStreamState` subclass. The state's `produce(out, ctx)` method is called repeatedly until `out.finish()` is called:

```python
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import CallContext, OutputCollector, ServerStream, ServerStreamState


@dataclass
class CountdownState(ServerStreamState):
    """Counts down from n to 1."""

    n: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next countdown value, or finish."""
        if self.n <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.n]})
        self.n -= 1


class CountdownService(Protocol):
    """Service with a server stream."""

    def countdown(self, n: int) -> ServerStream[ServerStreamState]:
        """Count down from n."""
        ...


class CountdownServiceImpl:
    """Countdown implementation."""

    def countdown(self, n: int) -> ServerStream[CountdownState]:
        """Count down from n."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return ServerStream(output_schema=schema, state=CountdownState(n=n))
```

The client iterates over a `StreamSession`:

```python
with serve_pipe(CountdownService, CountdownServiceImpl()) as proxy:
    for batch in proxy.countdown(3):
        print(batch.batch.to_pydict())
    # {'value': [3]}
    # {'value': [2]}
    # {'value': [1]}
```

### Bidi Streams

A bidi stream method returns `BidiStream[S]` where `S` is a `BidiStreamState` subclass. The state's `process(input, out, ctx)` method is called once per input batch. State is mutated in-place across exchanges:

```python
from dataclasses import dataclass

import pyarrow as pa

from vgi_rpc import (
    AnnotatedBatch,
    BidiStream,
    BidiStreamState,
    CallContext,
    OutputCollector,
)


@dataclass
class RunningSum(BidiStreamState):
    """Accumulates a running sum across exchanges."""

    total: float = 0.0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Add input values to running total."""
        values = input.batch.column("value")
        for v in values:
            self.total += v.as_py()
        out.emit_pydict({"total": [self.total]})
```

The client uses `BidiSession.exchange()` for lockstep communication:

```python
with serve_pipe(SumService, SumServiceImpl()) as proxy:
    with proxy.accumulate(0.0) as session:
        result = session.exchange(
            AnnotatedBatch.from_pydict({"value": [1.0, 2.0]})
        )
        print(result.batch.to_pydict())  # {'total': [3.0]}

        result = session.exchange(
            AnnotatedBatch.from_pydict({"value": [10.0]})
        )
        print(result.batch.to_pydict())  # {'total': [13.0]}
```

## Logging

### Server-side logging

Server method implementations can emit log messages by accepting a `ctx` parameter (type `CallContext`). The framework injects it automatically — it does **not** appear in the Protocol definition:

```python
from vgi_rpc import CallContext, Level


class MyServiceImpl:
    """Implementation with logging."""

    def process(self, data: str, ctx: CallContext) -> str:
        """Process data with logging."""
        ctx.log(Level.INFO, "Processing started", input_size=str(len(data)))
        result = data.upper()
        ctx.log(Level.DEBUG, "Processing complete")
        return result
```

In streaming methods, `ctx` is passed to `produce()` and `process()`. You can also use `OutputCollector.log()`:

```python
def produce(self, out: OutputCollector, ctx: CallContext) -> None:
    """Produce with logging."""
    ctx.log(Level.INFO, "generating batch")
    out.emit_pydict({"value": [42]})
```

### Log levels

The `Level` enum provides: `EXCEPTION`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`.

`Message` has convenience factory methods: `Message.error(...)`, `Message.warn(...)`, `Message.info(...)`, `Message.debug(...)`, `Message.trace(...)`.

### Client-side log handling

Pass an `on_log` callback when creating a connection:

```python
from vgi_rpc import Message, serve_pipe


def handle_log(msg: Message) -> None:
    """Handle log messages from the server."""
    print(f"[{msg.level.value}] {msg.message}")


with serve_pipe(MyService, MyServiceImpl(), on_log=handle_log) as proxy:
    proxy.process("hello")
    # [INFO] Processing started
    # [DEBUG] Processing complete
```

## Error Handling

Server exceptions are propagated to the client as `RpcError`:

```python
from vgi_rpc import RpcError

try:
    proxy.failing_method()
except RpcError as e:
    print(e.error_type)        # "ValueError"
    print(e.error_message)     # "something went wrong"
    print(e.remote_traceback)  # full server-side traceback
```

Errors are transmitted as zero-row batches with `EXCEPTION`-level log metadata. The transport remains clean for subsequent requests — a single failed call does not poison the connection.

## Authentication

### AuthContext

`AuthContext` is a frozen dataclass representing the authentication state for a request:

```python
from vgi_rpc import AuthContext

# Created by your authenticate callback
ctx = AuthContext(domain="jwt", authenticated=True, principal="alice", claims={"role": "admin"})

# Anonymous (default for pipe transport)
anon = AuthContext.anonymous()
```

Fields:

| Field | Type | Description |
|---|---|---|
| `domain` | `str \| None` | Authentication scheme (e.g. `"jwt"`, `"api_key"`, `"mtls"`) |
| `authenticated` | `bool` | Whether the caller was successfully authenticated |
| `principal` | `str \| None` | Identity of the caller |
| `claims` | `Mapping[str, Any]` | Arbitrary claims from the authentication token |

### HTTP authentication

Pass an `authenticate` callback to `make_wsgi_app`. The callback receives a Falcon `Request` and returns an `AuthContext`. Raise `ValueError` or `PermissionError` to reject the request (mapped to HTTP 401):

```python
import falcon

from vgi_rpc import AuthContext, RpcServer, make_wsgi_app


def authenticate(req: falcon.Request) -> AuthContext:
    """Validate a Bearer token and return auth context."""
    auth_header = req.get_header("Authorization") or ""
    if not auth_header.startswith("Bearer "):
        raise ValueError("Missing Bearer token")
    token = auth_header.removeprefix("Bearer ")
    # ... validate token, extract claims ...
    return AuthContext(domain="jwt", authenticated=True, principal="alice")


server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=authenticate)
```

### Using auth in methods

Methods that declare a `ctx: CallContext` parameter receive the authentication context. Use `ctx.auth.require_authenticated()` to gate access:

```python
from vgi_rpc import CallContext


class MyServiceImpl:
    """Implementation with auth checks."""

    def public(self) -> str:
        """No ctx — works for all callers."""
        return "ok"

    def whoami(self, ctx: CallContext) -> str:
        """Return the caller's principal."""
        return ctx.auth.principal or "anonymous"

    def secret(self, ctx: CallContext) -> str:
        """Require authentication."""
        ctx.auth.require_authenticated()  # raises PermissionError if not authenticated
        return f"secret for {ctx.auth.principal}"
```

Over pipe transport, `ctx.auth` is always `AuthContext.anonymous()` (unauthenticated).

### Transport metadata

`ctx.transport_metadata` provides transport-level information (e.g. `remote_addr`, `user_agent` for HTTP). This is a read-only mapping populated by the transport layer.

## External Location (Large Batch Support)

When batches exceed a configurable size threshold, they can be transparently uploaded to external storage (S3, GCS) and replaced with lightweight pointer batches. The client resolves pointers automatically using parallel range-request fetching.

```python
from vgi_rpc import Compression, ExternalLocationConfig, FetchConfig, RpcServer, S3Storage

storage = S3Storage(bucket="my-bucket", prefix="rpc-data/")
config = ExternalLocationConfig(
    storage=storage,
    externalize_threshold_bytes=1_048_576,  # 1 MiB (default)
    compression=Compression(),              # optional zstd compression
    fetch_config=FetchConfig(
        chunk_size_bytes=8 * 1024 * 1024,    # 8 MiB chunks
        max_parallel_requests=8,              # concurrent fetches
        speculative_retry_multiplier=2.0,     # hedge slow chunks
    ),
)

server = RpcServer(MyService, MyServiceImpl(), external_location=config)
```

### Storage backends

**S3** (`pip install vgi-rpc[s3]`):

```python
from vgi_rpc import S3Storage

storage = S3Storage(
    bucket="my-bucket",
    prefix="vgi-rpc/",               # key prefix (default)
    presign_expiry_seconds=3600,      # signed URL lifetime (default)
    endpoint_url="http://localhost:9000",  # for MinIO/LocalStack
)
```

**GCS** (`pip install vgi-rpc[gcs]`):

```python
from vgi_rpc import GCSStorage

storage = GCSStorage(
    bucket="my-bucket",
    prefix="vgi-rpc/",               # key prefix (default)
    presign_expiry_seconds=3600,      # signed URL lifetime (default)
)
```

### Fetch configuration

`FetchConfig` controls parallel range-request fetching:

| Parameter | Default | Description |
|---|---|---|
| `parallel_threshold_bytes` | 64 MiB | Below this, use a single GET |
| `chunk_size_bytes` | 8 MiB | Size of each Range request chunk |
| `max_parallel_requests` | 8 | Concurrent request limit |
| `timeout_seconds` | 60.0 | Overall fetch deadline |
| `max_fetch_bytes` | 256 MiB | Hard cap on total download size |
| `speculative_retry_multiplier` | 2.0 | Hedge threshold (multiplier of median chunk time) |

## Wire Protocol Specification

### Framing

Multiple Arrow IPC streams are written sequentially on the same byte stream. Each `ipc.open_stream()` reads one complete IPC stream (schema + batches + EOS marker) and stops. The next `ipc.open_stream()` picks up where the previous one left off.

### Request format

Every request is a single-row Arrow IPC stream:

```
[schema (with vgi_rpc.method in schema metadata)]
[1-row batch (with vgi_rpc.request_version in batch metadata)]
[EOS]
```

### Metadata keys

All framework metadata keys live in the `vgi_rpc.` namespace:

| Key | Location | Description |
|---|---|---|
| `vgi_rpc.method` | schema metadata | Target RPC method name |
| `vgi_rpc.request_version` | batch metadata | Wire protocol version (`"1"`) |
| `vgi_rpc.bidi_state` | batch metadata | Serialized stream state (HTTP transport) |
| `vgi_rpc.log_level` | batch metadata | Log level on zero-row log/error batches |
| `vgi_rpc.log_message` | batch metadata | Log message text |
| `vgi_rpc.log_extra` | batch metadata | JSON-encoded extra fields |
| `vgi_rpc.server_id` | batch metadata | Server instance identifier (log/error tracing) |
| `vgi_rpc.location` | batch metadata | External storage URL for large batches |
| `vgi_rpc.location.fetch_ms` | batch metadata | Fetch duration (diagnostics) |
| `vgi_rpc.location.source` | batch metadata | Fetch source (diagnostics) |

### Unary wire format

```
Client → Server:  [IPC stream: params_schema + 1 request batch + EOS]
Server → Client:  [IPC stream: result_schema + 0..N log batches + 1 result/error batch + EOS]
```

### Server stream wire format (pipe)

```
Client → Server:  [IPC stream: params_schema + 1 request batch + EOS]
Server → Client:  [IPC stream: output_schema + (log_batch | data_batch)* + EOS]
```

On error, the final batch is zero-row with EXCEPTION-level log metadata.

### Bidi stream wire format (pipe)

**Phase 1 — request params** (same as unary):

```
Client → Server:  [IPC stream: params_schema + 1 request batch + EOS]
```

**Phase 2 — lockstep exchange** (one IPC stream per direction):

```
Client → Server:  [IPC stream: input_batch₁ + input_batch₂ + ... + EOS]
Server → Client:  [IPC stream: (log_batch* + output_batch)* + EOS]
```

Each input batch produces exactly one output batch (1:1 lockstep). Log batches may appear before each output batch. The client closes its input stream (EOS) to signal end of exchange.

### HTTP endpoints

All endpoints use `Content-Type: application/vnd.apache.arrow.stream`.

| Endpoint | Method | Description |
|---|---|---|
| `{prefix}/{method}` | POST | Unary and server-stream calls |
| `{prefix}/{method}/bidi` | POST | Bidi stream initialization |
| `{prefix}/{method}/exchange` | POST | Bidi and server-stream continuation |

Over HTTP, bidi streaming is **stateless**: each exchange carries serialized `BidiStreamState` in a signed token in the `vgi_rpc.bidi_state` batch metadata key.

### State tokens (HTTP)

State tokens use HMAC-SHA256 signing to prevent tampering:

```
[4 bytes: state_len  (uint32 LE)]
[state_len bytes: state_bytes]
[4 bytes: schema_len (uint32 LE)]
[schema_len bytes: schema_bytes]
[32 bytes: HMAC-SHA256(key, state_bytes + schema_bytes)]
```

### Log and error batches

Log messages and errors are signaled as **zero-row batches** with metadata:

- `vgi_rpc.log_level` — severity level (`EXCEPTION`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`)
- `vgi_rpc.log_message` — message text
- `vgi_rpc.log_extra` — JSON-encoded extra fields (optional)

`EXCEPTION` level causes the client to raise `RpcError`. All other levels are delivered to the `on_log` callback.

### External location pointer batches

When a batch is externalized, it is replaced by a **zero-row batch** with:

- `vgi_rpc.location` — URL to fetch the full batch data
- Original schema preserved for type checking

The client transparently resolves these pointers via parallel range-request fetching.
