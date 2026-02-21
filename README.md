<p align="center">
  <img src="docs/assets/logo-hero.png" alt="vgi-rpc logo" width="200">
</p>

<h1 align="center">vgi-rpc</h1>

<p align="center">
  Transport-agnostic RPC framework built on <a href="https://arrow.apache.org/">Apache Arrow</a> IPC serialization.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

Define RPC interfaces as Python `Protocol` classes. The framework derives Arrow schemas from type annotations and provides typed client proxies with automatic serialization/deserialization. Unlike gRPC, there are no `.proto` files or codegen steps — your Python type annotations are the schema. Unlike JSON-over-HTTP, structured data stays in Arrow columnar format for efficient transfer, especially with large or batch-oriented workloads.

**Key features:**

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
- **Wire protocol debug logging** — enable `vgi_rpc.wire` at DEBUG for full visibility into what flows over the wire

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
    # proxy is typed as Calculator — IDE shows add(), greet() with full signatures
    result = proxy.add(a=2.0, b=3.0)
    print(result)  # 5.0

    greeting = proxy.greet(name="World")
    print(greeting)  # Hello, World!
```

`serve_pipe` runs the server on a background thread — great for trying things out. For production deployments, see [Subprocess](#subprocess) or [HTTP](#http) transports.

## CLI

The `vgi-rpc` command-line tool lets you introspect and call methods on any service that has `enable_describe=True`. Requires `pip install vgi-rpc[cli]`.

### Describe a service

```bash
# Subprocess transport
vgi-rpc describe --cmd "python worker.py"

# HTTP transport
vgi-rpc describe --url http://localhost:8000

# Unix domain socket
vgi-rpc describe --unix /tmp/my-service.sock

# JSON output
vgi-rpc describe --cmd "python worker.py" --format json
```

### Call a method

```bash
# Unary call with key=value parameters
vgi-rpc call add --cmd "python worker.py" a=1.0 b=2.0

# JSON parameter input
vgi-rpc call add --url http://localhost:8000 --json '{"a": 1.0, "b": 2.0}'

# Producer stream (table output)
vgi-rpc call countdown --cmd "python worker.py" n=5 --format table

# Exchange stream (pipe JSON lines to stdin)
echo '{"value": 1.0}' | vgi-rpc call accumulate --url http://localhost:8000
```

### Output format

Results are printed as **one JSON object per row** (NDJSON). When a batch contains multiple rows, each row is emitted as its own line. With `--format table`, all rows are collected into a single aligned table. With `--format auto` (the default), the CLI uses pretty-printed JSON for TTYs and compact NDJSON when piped. With `--format arrow`, raw Arrow IPC data is written to the output (use `--output`/`-o` to direct to a file).

Stream methods with headers display the header before data rows: as a `{"__header__": {...}}` line in JSON, a `Header:` section in table format, or a separate IPC stream in arrow format.

### Options

| Option | Short | Description |
|---|---|---|
| `--url` | `-u` | HTTP base URL |
| `--cmd` | `-c` | Subprocess command |
| `--unix` | | Unix domain socket path |
| `--prefix` | `-p` | URL path prefix (default `/vgi`) |
| `--format` | `-f` | Output format: `auto`, `json`, `table`, or `arrow` |
| `--output` | `-o` | Output file path (default: stdout) |
| `--verbose` | `-v` | Show server log messages on stderr |
| `--debug` | | Enable DEBUG on all `vgi_rpc` loggers to stderr |
| `--log-level` | | Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-logger` | | Target specific logger(s), repeatable |
| `--log-format` | | Stderr log format: `text` (default) or `json` |
| `--json` | `-j` | Pass parameters as a JSON string (for `call`) |
| `--input` | `-i` | Arrow IPC file for exchange stream input (for `call`) |
| `--version` | `-V` | Show version and exit |

## Defining Services

Services are defined as `Protocol` classes. The return type annotation determines the method type:

| Return type | Method type | Description |
|---|---|---|
| `-> T` | **Unary** | Single request, single response |
| `-> Stream[S]` | **Stream** | Stateful streaming (producer or exchange) |
| `-> Stream[S, H]` | **Stream** | Streaming with a one-time header |

```python
from typing import Protocol

from vgi_rpc import ArrowSerializableDataclass, Stream, StreamState


class JobHeader(ArrowSerializableDataclass):
    """One-time metadata sent before the stream begins."""

    total_rows: int
    description: str


class MyService(Protocol):
    """Example service with both method types."""

    def echo(self, message: str) -> str:
        """Unary: single request/response."""
        ...

    def generate(self, count: int) -> Stream[StreamState]:
        """Producer stream: produces multiple batches."""
        ...

    def transform(self, factor: float) -> Stream[StreamState]:
        """Exchange stream: lockstep input/output exchange."""
        ...

    def generate_with_meta(self, count: int) -> Stream[StreamState, JobHeader]:
        """Producer stream with metadata header."""
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
| `Enum` | `dictionary(int16, utf8)` |
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

### Static introspection

Use `rpc_methods()` to inspect a protocol's methods, or `describe_rpc()` for a human-readable summary:

```python
from vgi_rpc import describe_rpc, rpc_methods

methods = rpc_methods(Calculator)
for name, info in methods.items():
    print(f"{name}: {info.method_type.value}, params={info.params_schema}")

print(describe_rpc(Calculator))
```

### Runtime introspection

Enable the built-in `__describe__` RPC method to let clients discover a server's methods at runtime — without needing the Python Protocol class. This is useful for dynamic clients, debugging tools, and cross-language interop.

Enable it on the server with `enable_describe=True`:

```python
from vgi_rpc import RpcServer

server = RpcServer(Calculator, CalculatorImpl(), enable_describe=True)
```

Query over pipe/subprocess transport with `introspect()`:

```python
from vgi_rpc import introspect, connect

with connect(Calculator, ["python", "worker.py"]) as proxy:
    desc = introspect(proxy._transport)
    for name, method in desc.methods.items():
        print(f"{name}: {method.method_type.value}")
        print(f"  params: {method.param_types}")
        print(f"  has_return: {method.has_return}")
```

Query over HTTP with `http_introspect()` (requires `pip install vgi-rpc[http]`):

```python
from vgi_rpc import http_introspect

desc = http_introspect("http://localhost:8080")
print(desc)
```

The `ServiceDescription` returned contains:

| Field | Type | Description |
|---|---|---|
| `protocol_name` | `str` | Name of the Protocol class |
| `request_version` | `str` | Wire protocol version |
| `describe_version` | `str` | Introspection format version |
| `server_id` | `str` | Server instance identifier |
| `methods` | `Mapping[str, MethodDescription]` | Method metadata keyed by name |

Each `MethodDescription` contains:

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Method name |
| `method_type` | `MethodType` | `UNARY` or `STREAM` |
| `doc` | `str \| None` | Method docstring |
| `has_return` | `bool` | Whether the method returns a value |
| `params_schema` | `pa.Schema` | Arrow schema for request parameters |
| `result_schema` | `pa.Schema` | Arrow schema for the response |
| `param_types` | `dict[str, str]` | Human-readable type names by parameter |
| `param_defaults` | `dict[str, object]` | Default values by parameter |
| `has_header` | `bool` | Whether the stream method declares a header |
| `header_schema` | `pa.Schema \| None` | Arrow schema for the header type |

The response is a standard Arrow IPC batch — any Arrow-capable language can parse it directly.

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

`serve_pipe` starts a server on a background thread and yields a proxy typed as the Protocol class. No subprocess needed — useful for tests and embedded use:

```python
from vgi_rpc import serve_pipe

with serve_pipe(Calculator, CalculatorImpl()) as proxy:
    print(proxy.add(a=1.0, b=2.0))  # 3.0
```

### Subprocess

Run the server in a child process over stdin/stdout. The server entry point calls `run_server`:

```python
# worker.py
from vgi_rpc import RpcServer, run_server

from my_service import MyService, MyServiceImpl

run_server(MyService, MyServiceImpl())
```

The client connects with `connect`, which returns a proxy typed as the Protocol class:

```python
import sys

from vgi_rpc import connect

with connect(MyService, [sys.executable, "worker.py"]) as proxy:
    result = proxy.echo(message="hello")  # proxy is typed as MyService
```

### Unix Domain Socket

Run the server on a Unix domain socket for low-latency local IPC without subprocess management. The server entry point calls `serve_unix_pipe`:

```python
# worker.py
from vgi_rpc.rpc import serve_unix_pipe, RpcServer

from my_service import MyService, MyServiceImpl

server = RpcServer(MyService, MyServiceImpl())
serve_unix_pipe(server, "/tmp/my-service.sock")
```

The client connects with `unix_connect`:

```python
from vgi_rpc.rpc import unix_connect

with unix_connect(MyService, "/tmp/my-service.sock") as proxy:
    result = proxy.echo(message="hello")  # proxy is typed as MyService
```

For in-process testing, `serve_unix` starts the server on a background thread with an auto-generated socket path:

```python
from vgi_rpc.rpc import serve_unix

with serve_unix(MyService, MyServiceImpl()) as proxy:
    print(proxy.add(a=1.0, b=2.0))  # 3.0
```

### Shared memory

`ShmPipeTransport` wraps a `PipeTransport` with a shared memory side-channel for zero-copy batch transfer. When a batch fits in the shared memory segment, only a small pointer is sent over the pipe; the receiver reads the data directly from shared memory. Falls back to normal pipe IPC for oversized batches.

```python
from vgi_rpc.shm import ShmSegment

from vgi_rpc import ShmPipeTransport, RpcServer, make_pipe_pair

# Create a shared memory segment (both sides must agree on name and size)
shm = ShmSegment.create(size=100 * 1024 * 1024)  # 100 MB

# Wrap a pipe transport with shared memory
client_pipe, server_pipe = make_pipe_pair()
client_transport = ShmPipeTransport(client_pipe, shm)
server_transport = ShmPipeTransport(server_pipe, shm)

# Use as normal transports
server = RpcServer(MyService, MyServiceImpl())
# ... serve on server_transport, connect via client_transport ...

# Cleanup
shm.close()
shm.unlink()  # destroy the segment
```

The lockstep RPC protocol guarantees only one side is active at a time, so no locking is needed. The segment uses a bump-pointer allocator stored in a fixed 4 KB header.

### HTTP

Requires `pip install vgi-rpc[http]`. The server exposes a Falcon WSGI app; the client uses httpx. Like pipe and subprocess transports, `http_connect` returns a proxy typed as the Protocol class:

```python
from vgi_rpc import RpcServer, http_connect, make_wsgi_app

# Server
server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server)
# Serve `app` with any WSGI server (waitress, gunicorn, etc.)

# Client
with http_connect(MyService, "http://localhost:8080") as proxy:
    result = proxy.echo(message="hello")  # proxy is typed as MyService
```

`make_wsgi_app` options:

| Parameter | Default | Description |
|---|---|---|
| `prefix` | `"/vgi"` | URL prefix for all RPC endpoints |
| `signing_key` | random 32 bytes | HMAC key for signing state tokens |
| `max_stream_response_bytes` | `None` | Split producer stream responses across multiple HTTP exchanges |
| `authenticate` | `None` | Callback `(falcon.Request) -> AuthContext` for request authentication |
| `max_request_bytes` | `None` | Advertise max request body size via `VGI-Max-Request-Bytes` header (no enforcement) |
| `cors_origins` | `None` | Allowed CORS origins — `"*"` for all, a string, or list of strings |
| `upload_url_provider` | `None` | `UploadUrlProvider` for generating pre-signed upload URLs (enables `__upload_url__/init` endpoint) |
| `max_upload_bytes` | `None` | Advertise max upload size via `VGI-Max-Upload-Bytes` header (requires `upload_url_provider`) |
| `otel_config` | `None` | `OtelConfig` for OpenTelemetry instrumentation |
| `token_ttl` | `3600` | Maximum age (seconds) for stream state tokens |

### CORS (browser clients)

To allow browser-based clients to call the HTTP transport, pass `cors_origins` to `make_wsgi_app`:

```python
# Allow all origins
app = make_wsgi_app(server, cors_origins="*")

# Allow specific origins
app = make_wsgi_app(server, cors_origins=["https://app.example.com", "https://staging.example.com"])
```

This uses Falcon's built-in `CORSMiddleware`, which handles preflight `OPTIONS` requests automatically.

### Server capabilities

When `max_request_bytes` is set, the server advertises the limit via the `VGI-Max-Request-Bytes` response header on every response (including OPTIONS). Clients can discover this with `http_capabilities()`:

```python
from vgi_rpc import http_capabilities

caps = http_capabilities("http://localhost:8080")
if caps.max_request_bytes is not None:
    print(f"Server accepts up to {caps.max_request_bytes} bytes")
```

### Server-vended upload URLs

When clients need to upload large payloads that exceed the server's HTTP POST size limit, the server can vend pre-signed upload URLs. The server has storage credentials (S3/GCS); the client uploads directly to storage, then sends pointer batches referencing the download URLs.

Enable it by passing an `upload_url_provider` (any `ExternalStorage` backend that implements `UploadUrlProvider`) to `make_wsgi_app`:

```python
from vgi_rpc import S3Storage, make_wsgi_app, RpcServer

storage = S3Storage(bucket="my-bucket", prefix="uploads/")
app = make_wsgi_app(
    server,
    upload_url_provider=storage,
    max_upload_bytes=100_000_000,  # optional: advertise 100 MB limit
)
```

Clients discover support via `http_capabilities()` and request URLs with `request_upload_urls()`:

```python
from vgi_rpc import http_capabilities, request_upload_urls

caps = http_capabilities("http://localhost:8080")
if caps.upload_url_support:
    urls = request_upload_urls("http://localhost:8080", count=3)
    for url in urls:
        # Upload data directly to storage via pre-signed PUT URL
        httpx.put(url.upload_url, content=data)
        # url.download_url goes into vgi_rpc.location pointer batches
```

### Worker Pool

`WorkerPool` keeps idle subprocess workers alive between `connect()` calls, avoiding repeated process spawn/teardown overhead for session-oriented workloads. Each `connect()` call borrows a worker (or spawns a new one), yields a typed proxy, and returns the worker to the pool on context exit.

```python
from vgi_rpc import WorkerPool

pool = WorkerPool(max_idle=4, idle_timeout=60.0)
with pool.connect(MyService, ["python", "worker.py"]) as proxy:
    result = proxy.echo(message="hello")
# subprocess returned to pool, not terminated
with pool.connect(MyService, ["python", "worker.py"]) as proxy:
    result = proxy.echo(message="world")  # reuses same subprocess
pool.close()  # terminates all idle workers
```

Workers are keyed by their command tuple. Idle workers are cached up to `max_idle` globally. A daemon reaper thread evicts workers that exceed `idle_timeout`.

The pool also supports pool-managed shared memory for zero-copy data transfer. Pass `shm_size` to the constructor and each `connect()` call gets its own isolated segment, automatically destroyed on exit:

```python
from vgi_rpc import WorkerPool

pool = WorkerPool(max_idle=4, shm_size=4 * 1024 * 1024)
with pool.connect(MyService, cmd) as svc:
    batches = list(svc.generate(count=100))
# subprocess back in pool; SHM segment cleaned up automatically
pool.close()
```

Use `pool.metrics` for observability: borrows, spawns, reuses, returns, discards, evictions, idle/active counts.

## Streaming

Streaming methods return `Stream[S]` where `S` is a `StreamState` subclass. The state's `process(input, out, ctx)` method is called once per iteration. There are two patterns:

- **Producer streams** — server pushes data; client iterates. `input_schema` defaults to empty. Call `out.finish()` to end.
- **Exchange streams** — client sends data; server responds. Set `input_schema` to the expected input schema. Client controls termination via `close()`.

### Producer Streams

A producer stream ignores the `input` parameter and calls `out.finish()` when done:

```python
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import AnnotatedBatch, CallContext, OutputCollector, Stream, StreamState


@dataclass
class CountdownState(StreamState):
    """Counts down from n to 1."""

    n: int

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next countdown value, or finish."""
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


class CountdownServiceImpl:
    """Countdown implementation."""

    def countdown(self, n: int) -> Stream[CountdownState]:
        """Count down from n."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=CountdownState(n=n))
```

The client iterates over the result — the proxy type preserves `Stream`'s `__iter__` signature, so IDEs know each `batch` is an `AnnotatedBatch`:

```python
with serve_pipe(CountdownService, CountdownServiceImpl()) as proxy:
    for batch in proxy.countdown(n=3):  # batch: AnnotatedBatch
        print(batch.batch.to_pydict())
    # {'value': [3]}
    # {'value': [2]}
    # {'value': [1]}
```

### Exchange Streams

An exchange stream sets `input_schema` to a real schema. The `process()` method receives client data and emits a response. State is mutated in-place across exchanges:

```python
from dataclasses import dataclass

import pyarrow as pa

from vgi_rpc import AnnotatedBatch, CallContext, OutputCollector, Stream, StreamState


@dataclass
class RunningSum(StreamState):
    """Accumulates a running sum across exchanges."""

    total: float = 0.0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Add input values to running total."""
        values = input.batch.column("value")
        for v in values:
            self.total += v.as_py()
        out.emit_pydict({"total": [self.total]})
```

The client uses the result as a context manager with `exchange()` for lockstep communication — the proxy type preserves `Stream`'s context manager and `exchange` signatures:

```python
with serve_pipe(SumService, SumServiceImpl()) as proxy:
    with proxy.accumulate(initial=0.0) as session:  # session supports exchange() and close()
        result = session.exchange(
            AnnotatedBatch.from_pydict({"value": [1.0, 2.0]})
        )
        print(result.batch.to_pydict())  # {'total': [3.0]}

        result = session.exchange(
            AnnotatedBatch.from_pydict({"value": [10.0]})
        )
        print(result.batch.to_pydict())  # {'total': [13.0]}
```

### Stream Headers

Stream methods can send a one-time header before the data stream begins by declaring `Stream[S, H]` where `H` is an `ArrowSerializableDataclass`. Headers carry metadata that applies to the entire stream — total row counts, column descriptions, job identifiers, or any fixed information the client needs before processing batches.

Define a header as a frozen dataclass with the `ArrowSerializableDataclass` mixin:

```python
from dataclasses import dataclass

from vgi_rpc import ArrowSerializableDataclass


@dataclass(frozen=True)
class JobHeader(ArrowSerializableDataclass):
    """One-time metadata sent before the stream begins."""

    total_rows: int
    description: str
```

Declare the header in the Protocol with `Stream[S, H]`:

```python
from typing import Protocol

from vgi_rpc import Stream, StreamState


class DataService(Protocol):
    """Service with header-bearing streams."""

    def fetch_rows(self, query: str) -> Stream[StreamState, JobHeader]:
        """Producer stream with metadata header."""
        ...

    def transform_rows(self, factor: float) -> Stream[StreamState, JobHeader]:
        """Exchange stream with metadata header."""
        ...
```

Return the header from the server implementation via `Stream(..., header=...)`:

```python
from vgi_rpc import OutputCollector, CallContext, ProducerState, Stream

import pyarrow as pa


@dataclass
class FetchState(ProducerState):
    """Produces rows from a query."""

    remaining: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one batch per call."""
        if self.remaining <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.remaining]})
        self.remaining -= 1


class DataServiceImpl:
    """Implementation with stream headers."""

    def fetch_rows(self, query: str) -> Stream[FetchState, JobHeader]:
        """Return a stream with a header."""
        schema = pa.schema([pa.field("value", pa.int64())])
        header = JobHeader(total_rows=100, description=f"Results for: {query}")
        return Stream(output_schema=schema, state=FetchState(remaining=100), header=header)
```

On the client side, access the header via `session.header`:

```python
from vgi_rpc import serve_pipe

with serve_pipe(DataService, DataServiceImpl()) as proxy:
    # Producer stream with header
    session = proxy.fetch_rows(query="SELECT *")
    print(session.header)  # JobHeader(total_rows=100, description='Results for: SELECT *')
    for batch in session:
        print(batch.batch.to_pydict())

    # Exchange stream with header
    with proxy.transform_rows(factor=2.0) as session:
        print(session.header)  # JobHeader(...)
        result = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
```

For streams without a header (`Stream[S]`), `session.header` returns `None`. Use `session.typed_header(JobHeader)` for a typed narrowing that raises `TypeError` if the header is missing or the wrong type.

Stream headers work across all transports (pipe, subprocess, HTTP). For HTTP, the header is included in the `/init` response — subsequent `/exchange` requests do not re-send it.

## Logging

### Client-directed logging

Server method implementations can emit log messages that are transmitted **to the caller** as zero-row Arrow IPC batches. These are not server-side logs — they travel over the wire to the client's `on_log` callback.

Accept a `ctx` parameter (type `CallContext`) to access the logging API. The framework injects it automatically — it does **not** appear in the Protocol definition:

```python
from vgi_rpc import CallContext, Level


class MyServiceImpl:
    """Implementation with client-directed logging."""

    def process(self, data: str, ctx: CallContext) -> str:
        """Process data with logging."""
        ctx.client_log(Level.INFO, "Processing started", input_size=str(len(data)))
        result = data.upper()
        ctx.client_log(Level.DEBUG, "Processing complete")
        return result
```

In streaming methods, `ctx` is passed to `process()`. You can also use `OutputCollector.client_log()`:

```python
def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
    """Process with logging."""
    ctx.client_log(Level.INFO, "generating batch")
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
    proxy.process(data="hello")
    # [INFO] Processing started
    # [DEBUG] Processing complete
```

### Server-side logging

vgi-rpc uses stdlib `logging` for server-side observability. Three features work together:

- **`ctx.logger`** — a pre-configured logger with request context (server_id, method, principal, remote_addr) automatically bound to every log record
- **Automatic access logging** — one structured INFO record per RPC call on the `vgi_rpc.access` logger, with duration, status, and error details
- **Logger hierarchy** — fine-grained control over what gets logged, from individual services to storage backends

#### ctx.logger in a service method

```python
import logging

from vgi_rpc import CallContext

logging.basicConfig(level=logging.DEBUG)


class MyServiceImpl:
    """Implementation with server-side logging."""

    def process(self, data: str, ctx: CallContext) -> str:
        """Process data with server-side debug logging."""
        ctx.logger.info("Processing request", extra={"input_size": len(data)})
        result = data.upper()
        ctx.logger.debug("Transform complete", extra={"output_size": len(result)})
        return result
```

Each log record always includes `server_id` and `method` in its extra dict. When available, `principal`, `auth_domain`, and `remote_addr` are also included. The logger name is `vgi_rpc.service.<ProtocolName>`.

#### Access log (automatic)

Every completed RPC call emits one structured INFO record on the `vgi_rpc.access` logger:

```python
import logging

# Enable access logging
logging.getLogger("vgi_rpc.access").setLevel(logging.INFO)
logging.getLogger("vgi_rpc.access").addHandler(logging.StreamHandler())
```

Access log extra fields:

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
| `http_status` | `int` | HTTP response status code (HTTP transport only; reflects the HTTP layer, not the RPC-level `status`) |
| `input_batches` | `int` | Number of input batches read by the server |
| `output_batches` | `int` | Number of output batches written by the server |
| `input_rows` | `int` | Total rows across all input batches |
| `output_rows` | `int` | Total rows across all output batches |
| `input_bytes` | `int` | Approximate logical bytes across all input batches |
| `output_bytes` | `int` | Approximate logical bytes across all output batches |

> **Note:** Byte counts use `pa.RecordBatch.get_total_buffer_size()` — logical Arrow buffer sizes without IPC framing overhead. This is an approximation suitable for usage accounting, not exact wire bytes.

> **Note:** Access logs include `principal` and `remote_addr`, which may be PII under GDPR/CCPA. Configure appropriate log retention and access controls.

#### Production JSON logging

```python
import logging

from vgi_rpc.logging_utils import VgiJsonFormatter

handler = logging.StreamHandler()
handler.setFormatter(VgiJsonFormatter())
logging.getLogger("vgi_rpc").addHandler(handler)
logging.getLogger("vgi_rpc").setLevel(logging.INFO)
```

Sample JSON output:

```json
{
  "timestamp": "2026-01-15 10:30:00,123",
  "level": "INFO",
  "logger": "vgi_rpc.access",
  "message": "MyService.process ok",
  "server_id": "a1b2c3d4e5f6",
  "protocol": "MyService",
  "method": "process",
  "method_type": "unary",
  "duration_ms": 3.45,
  "status": "ok",
  "error_type": "",
  "input_batches": 1,
  "output_batches": 1,
  "input_rows": 1,
  "output_rows": 1,
  "input_bytes": 24,
  "output_bytes": 16
}
```

#### Per-service log levels

```python
import logging

# All framework logs at WARNING
logging.getLogger("vgi_rpc").setLevel(logging.WARNING)
# DEBUG for one specific service
logging.getLogger("vgi_rpc.service.MyService").setLevel(logging.DEBUG)
# Access log always on
logging.getLogger("vgi_rpc.access").setLevel(logging.INFO)
```

#### Subprocess stderr handling

```python
from vgi_rpc import StderrMode, connect

# Forward child stderr to Python logging (prevents pipe buffer stalls)
with connect(MyService, ["python", "worker.py"], stderr=StderrMode.PIPE) as proxy:
    proxy.process(data="hello")

# Discard child stderr entirely
with connect(MyService, ["python", "worker.py"], stderr=StderrMode.DEVNULL) as proxy:
    proxy.process(data="hello")
```

`StderrMode.PIPE` starts a daemon thread that drains the child's stderr line-by-line into the `vgi_rpc.subprocess.stderr` logger. `StderrMode.INHERIT` (default) sends stderr to the parent's stderr. Only applies to subprocess transport.

#### OpenTelemetry

`instrument_server()` adds distributed tracing spans and metrics to every RPC dispatch. Requires `pip install vgi-rpc[otel]`:

```python
from vgi_rpc import RpcServer
from vgi_rpc.otel import OtelConfig, instrument_server

server = RpcServer(MyService, MyServiceImpl())
instrument_server(server)  # uses global TracerProvider / MeterProvider
```

Each span includes:

| Attribute | Description |
|---|---|
| `rpc.system` | Always `"vgi_rpc"` |
| `rpc.service` | Protocol class name |
| `rpc.method` | Method name |
| `rpc.vgi_rpc.method_type` | `"unary"` or `"stream"` |
| `rpc.vgi_rpc.server_id` | Server instance identifier |
| `rpc.vgi_rpc.input_batches` | Number of input batches |
| `rpc.vgi_rpc.output_batches` | Number of output batches |
| `rpc.vgi_rpc.input_rows` | Total input rows |
| `rpc.vgi_rpc.output_rows` | Total output rows |
| `rpc.vgi_rpc.input_bytes` | Approximate input bytes |
| `rpc.vgi_rpc.output_bytes` | Approximate output bytes |

Trace context propagates automatically: from contextvars (pipe transport) or W3C `traceparent`/`tracestate` headers (HTTP transport).

Metrics recorded: `rpc.server.requests` (counter) and `rpc.server.duration` (histogram, seconds).

To bridge stdlib access logs into OTel:

```python
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

handler = LoggingHandler(logger_provider=provider)
logging.getLogger("vgi_rpc").addHandler(handler)
# All structured fields become OTel log record attributes automatically.
```

#### Logger hierarchy

| Logger name | Purpose |
|---|---|
| `vgi_rpc` | Root — add handlers here |
| `vgi_rpc.access` | Auto access log (one per call) |
| `vgi_rpc.service.<Protocol>` | Developer `ctx.logger` per service |
| `vgi_rpc.rpc` | Framework lifecycle (server init, errors) |
| `vgi_rpc.http` | HTTP transport lifecycle |
| `vgi_rpc.http.retry` | HTTP retry logic |
| `vgi_rpc.pool` | Worker pool lifecycle (borrows, returns, evictions) |
| `vgi_rpc.shm` | Shared memory transport operations |
| `vgi_rpc.otel` | OpenTelemetry instrumentation |
| `vgi_rpc.external` | External storage operations |
| `vgi_rpc.external_fetch` | URL fetch operations |
| `vgi_rpc.s3` / `vgi_rpc.gcs` | Storage backend operations |
| `vgi_rpc.subprocess.stderr` | Child process stderr (`StderrMode.PIPE`) |
| `vgi_rpc.wire.request` | Request serialization / deserialization |
| `vgi_rpc.wire.response` | Response serialization / deserialization |
| `vgi_rpc.wire.batch` | Batch classification (log / error / data dispatch) |
| `vgi_rpc.wire.stream` | Stream session lifecycle |
| `vgi_rpc.wire.transport` | Transport lifecycle (pipe, subprocess) |
| `vgi_rpc.wire.http` | HTTP client requests / responses |

#### Wire protocol debugging

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

All formatting is gated behind `isEnabledFor` guards — zero overhead when debug logging is disabled.

#### What to enable when

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

#### Debugging cross-language implementations

When building a vgi-rpc client or server in another language, wire logging on the Python side lets you see the exact Arrow IPC bytes and metadata your implementation needs to produce or consume. Run the Python side with full wire debugging to use it as a reference:

```python
import logging

# Full wire + access logging for interop debugging
logging.basicConfig(level=logging.DEBUG, format="%(name)-30s %(message)s")
logging.getLogger("vgi_rpc.wire").setLevel(logging.DEBUG)
logging.getLogger("vgi_rpc.access").setLevel(logging.INFO)
```

**What to verify for a non-Python client** (sending requests to a Python server):

1. **Batch metadata** — The request IPC stream schema must include `vgi_rpc.method` in batch-level custom metadata. `wire.request` logs show the expected schema and metadata for every inbound request.
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

## Call Statistics

Every RPC dispatch automatically tracks per-call I/O counters via `CallStatistics`:

| Counter | Description |
|---|---|
| `input_batches` | Number of input batches read by the server |
| `output_batches` | Number of output batches written by the server (includes log and error batches) |
| `input_rows` | Total rows across all input batches |
| `output_rows` | Total rows across all output batches |
| `input_bytes` | Approximate logical bytes across all input batches |
| `output_bytes` | Approximate logical bytes across all output batches |

Statistics are **server-side only** — they measure what the server reads and writes, not client-side wire traffic.

### What gets counted

- **Unary calls**: 1 input batch (params) + 1+ output batches (result + any log messages)
- **Producer streams**: tick batches count as input; each produced batch + logs count as output
- **Exchange streams**: each client-sent batch counts as input; each response batch + logs count as output
- **`__describe__`**: the describe response batch counts as output
- **Errors**: partial stats up to the error point, including the error batch as output

### Where statistics appear

1. **Access log** — six extra fields on every `vgi_rpc.access` record (see [Logging](#server-side-logging))
2. **OTel spans** — six span attributes when `instrument_server()` is active (see [OpenTelemetry](#opentelemetry))

### Transport semantics

- **Pipe/subprocess**: stats accumulate across the full call lifecycle
- **HTTP**: stats are per-HTTP-request (inherent to the stateless model — init, exchange, and continuation are separate requests)

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
    compression=Compression(),              # zstd level 3 by default
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
    project="my-gcp-project",        # optional GCP project ID
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
| `max_speculative_hedges` | 4 | Maximum number of hedge requests per fetch |

## Examples

The [`examples/`](examples/) directory contains runnable scripts demonstrating key features:

| Example | Description |
|---|---|
| [`hello_world.py`](examples/hello_world.py) | Minimal quickstart with in-process pipe transport |
| [`streaming.py`](examples/streaming.py) | Producer and exchange stream patterns |
| [`structured_types.py`](examples/structured_types.py) | Dataclass parameters with enums and nested types |
| [`http_server.py`](examples/http_server.py) | HTTP server with Falcon + waitress |
| [`http_client.py`](examples/http_client.py) | HTTP client connecting to the server |
| [`subprocess_worker.py`](examples/subprocess_worker.py) | Subprocess worker entry point |
| [`subprocess_client.py`](examples/subprocess_client.py) | Subprocess client with error handling |
| [`testing_pipe.py`](examples/testing_pipe.py) | Unit-testing with `serve_pipe()` (no network) |
| [`testing_http.py`](examples/testing_http.py) | Unit-testing the HTTP transport with `make_sync_client()` |
| [`auth.py`](examples/auth.py) | HTTP authentication with Bearer tokens and guarded methods |
| [`introspection.py`](examples/introspection.py) | Runtime service introspection with `enable_describe` |
| [`shared_memory.py`](examples/shared_memory.py) | Zero-copy shared memory transport with `ShmPipeTransport` |

## Testing

### Pipe transport (fastest)

`serve_pipe` runs the server on a background thread — no subprocess or network needed:

```python
from vgi_rpc import serve_pipe

with serve_pipe(MyService, MyServiceImpl()) as svc:
    assert svc.add(a=2.0, b=3.0) == 5.0

    values = [row["value"] for b in svc.countdown(n=3) for row in b.batch.to_pylist()]
    assert values == [3, 2, 1]
```

See [`examples/testing_pipe.py`](examples/testing_pipe.py) for a complete runnable example.

### HTTP transport (no real server)

`make_sync_client` wraps a Falcon `TestClient` so you can exercise the full HTTP stack — including authentication — without starting a server:

```python
from vgi_rpc import RpcServer
from vgi_rpc.http import http_connect, make_sync_client

server = RpcServer(MyService, MyServiceImpl())
client = make_sync_client(server, authenticate=my_auth, default_headers={"Authorization": "Bearer tok"})

with http_connect(MyService, client=client) as svc:
    assert svc.greet(name="World") == "Hello, World!"
```

See [`examples/testing_http.py`](examples/testing_http.py) for a complete runnable example with authentication testing.

## IPC Validation

Incoming Arrow IPC batches can be validated at configurable levels via `IpcValidation`:

| Level | Description |
|---|---|
| `IpcValidation.NONE` | No validation (fastest) |
| `IpcValidation.STANDARD` | Calls `batch.validate()` — checks structural integrity |
| `IpcValidation.FULL` | Calls `batch.validate(full=True)` — also checks data buffers |

Both server and client default to `IpcValidation.FULL`. You can lower the level for performance-sensitive paths:

```python
from vgi_rpc import IpcValidation, RpcServer, connect, serve_pipe

# Server: validate incoming requests (default is FULL)
server = RpcServer(MyService, MyServiceImpl(), ipc_validation=IpcValidation.FULL)

# Client: opt into lighter validation for performance
with connect(MyService, ["python", "worker.py"], ipc_validation=IpcValidation.STANDARD) as proxy:
    proxy.echo(message="hello")

# In-process: both sides default to FULL
with serve_pipe(MyService, MyServiceImpl()) as proxy:
    proxy.echo(message="hello")
```

Invalid batches raise `IPCError`.

## Wire Protocol Specification

### Framing

Multiple Arrow IPC streams are written sequentially on the same byte stream. Each `ipc.open_stream()` reads one complete IPC stream (schema + batches + EOS marker) and stops. The next `ipc.open_stream()` picks up where the previous one left off.

### Request format

Every request is a single-row Arrow IPC stream:

```
[schema]
[1-row batch (with vgi_rpc.method and vgi_rpc.request_version in custom metadata)]
[EOS]
```

### Metadata keys

All framework metadata keys live in the `vgi_rpc.` namespace:

| Key | Location | Description |
|---|---|---|
| `vgi_rpc.method` | batch metadata | Target RPC method name |
| `vgi_rpc.request_version` | batch metadata | Wire protocol version (`"1"`) |
| `vgi_rpc.stream_state` | batch metadata | Serialized stream state (HTTP transport) |
| `vgi_rpc.log_level` | batch metadata | Log level on zero-row log/error batches |
| `vgi_rpc.log_message` | batch metadata | Log message text |
| `vgi_rpc.log_extra` | batch metadata | JSON-encoded extra fields |
| `vgi_rpc.server_id` | batch metadata | Server instance identifier (log/error tracing) |
| `vgi_rpc.location` | batch metadata | External storage URL for large batches |
| `vgi_rpc.location.fetch_ms` | batch metadata | Fetch duration (diagnostics) |
| `vgi_rpc.location.source` | batch metadata | Fetch source (diagnostics) |
| `vgi_rpc.protocol_name` | batch metadata | Protocol class name (`__describe__` response) |
| `vgi_rpc.describe_version` | batch metadata | Introspection format version (`__describe__` response) |
| `vgi_rpc.shm_offset` | batch metadata | Shared memory offset (SHM pointer batch) |
| `vgi_rpc.shm_length` | batch metadata | Shared memory length (SHM pointer batch) |
| `vgi_rpc.shm_source` | batch metadata | SHM source indicator (diagnostics) |

### Unary wire format

```
Client → Server:  [IPC stream: params_schema + 1 request batch + EOS]
Server → Client:  [IPC stream: result_schema + 0..N log batches + 1 result/error batch + EOS]
```

### Stream wire format (pipe)

All streams use the same lockstep exchange protocol:

**Phase 1 — request params** (same as unary):

```
Client → Server:  [IPC stream: params_schema + 1 request batch + EOS]
```

**Phase 1.5 — optional header** (only if the Protocol declares `Stream[S, H]`):

```
Server → Client:  [IPC stream: header_schema + 0..N log batches + 1 header batch + EOS]
```

The header is a complete IPC stream containing a single-row batch with the header data. It is written once, before the main output stream begins. Streams declared as `Stream[S]` (no header type) skip this phase entirely.

**Phase 2 — lockstep exchange** (one IPC stream per direction):

```
Client → Server:  [IPC stream: input_batch₁ + input_batch₂ + ... + EOS]
Server → Client:  [IPC stream: (log_batch* + output_batch)* + EOS]
```

Each input batch produces exactly one output batch (1:1 lockstep). For producer streams, input batches are zero-row ticks on an empty schema. For exchange streams, input batches carry real data.

Log batches may appear before each output batch. The client closes its input stream (EOS) to signal end of exchange. For producer streams, the server closes its output stream (EOS) when `out.finish()` is called.

### HTTP endpoints

All endpoints use `Content-Type: application/vnd.apache.arrow.stream`.

| Endpoint | Method | Description |
|---|---|---|
| `{prefix}/{method}` | POST | Unary calls |
| `{prefix}/{method}/init` | POST | Stream initialization (producer and exchange) |
| `{prefix}/{method}/exchange` | POST | Stream continuation (producer and exchange) |

Over HTTP, streaming is **stateless**: each exchange carries serialized `StreamState` in a signed token in the `vgi_rpc.stream_state` batch metadata key. Producer stream init returns data batches directly; exchange stream init returns a state token.

For streams with headers, the `/init` response body contains the header IPC stream prepended to the main output IPC stream. The `/exchange` endpoint never re-sends the header — it is only included in the initial response.

### State tokens (HTTP)

State tokens use HMAC-SHA256 signing to prevent tampering. The token is versioned for forward compatibility, and the HMAC is verified before inspecting any payload fields (including the version byte) to avoid leaking format information to unauthenticated callers.

```
[1 byte:  version            (uint8, currently 2)]
[8 bytes: created_at         (uint64 LE, seconds since epoch)]
[4 bytes: state_len          (uint32 LE)]
[state_len bytes: state_bytes]
[4 bytes: schema_len         (uint32 LE)]
[schema_len bytes: schema_bytes]
[4 bytes: input_schema_len   (uint32 LE)]
[input_schema_len bytes: input_schema_bytes]
[32 bytes: HMAC-SHA256(key, all preceding bytes)]
```

For producer streams, `input_schema_bytes` is the serialized empty schema (small fixed-size blob).

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
