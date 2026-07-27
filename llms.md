# vgi-rpc — LLM Reference

Transport-agnostic RPC framework built on Apache Arrow IPC serialization. Define services as Python `Protocol` classes; the framework derives Arrow schemas from type annotations and provides typed client proxies.

Requires Python 3.12+.

## Installation

```bash
pip install vgi-rpc              # core (pipe + subprocess transports)
pip install vgi-rpc[http]        # + HTTP transport (Falcon server, httpx client)
pip install vgi-rpc[s3]          # + S3 external storage backend
pip install vgi-rpc[gcs]         # + GCS external storage backend
pip install vgi-rpc[cli]         # + CLI tool
pip install vgi-rpc[external]    # + external fetch support (aiohttp, zstd)
```

## Core Pattern

Every vgi-rpc service follows three steps: define a Protocol, implement it, connect via a transport.

```python
from typing import Protocol
from vgi_rpc import serve_pipe

# 1. Define the interface
class Calculator(Protocol):
    """Calculator service."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

# 2. Implement it
class CalculatorImpl:
    """Calculator implementation."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

# 3. Connect — proxy is typed as Calculator
with serve_pipe(Calculator, CalculatorImpl()) as proxy:
    result = proxy.add(a=2.0, b=3.0)  # 5.0
```

## Type Mappings

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
| `Enum` subclass | `dictionary(int32, utf8)` |
| `Optional[T]` | nullable `T` |
| `ArrowSerializableDataclass` | `struct` |
| `Annotated[T, ArrowType(...)]` | explicit override |

Override with `Annotated`:

```python
from typing import Annotated
import pyarrow as pa
from vgi_rpc import ArrowType

def count(self, n: Annotated[int, ArrowType(pa.int32())]) -> int: ...
```

## Method Types

The return type annotation determines method type:

| Return type | Method type |
|---|---|
| `-> T` | **Unary** — single request/response |
| `-> Stream[S]` | **Stream** — stateful streaming |

## Transports

### In-process pipe (testing / embedded)

```python
from vgi_rpc import serve_pipe

with serve_pipe(MyService, MyServiceImpl()) as proxy:
    result = proxy.some_method(arg="value")
```

### Subprocess

Worker entry point (`worker.py`):

```python
from vgi_rpc import run_server

run_server(MyService, MyServiceImpl())
```

Client:

```python
import sys
from vgi_rpc import connect

with connect(MyService, [sys.executable, "worker.py"]) as proxy:
    result = proxy.some_method(arg="value")
```

### HTTP

Server (produces a WSGI app for any WSGI server):

```python
from vgi_rpc import RpcServer, make_wsgi_app

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server)
# Serve with waitress, gunicorn, etc.
```

Client:

```python
from vgi_rpc import http_connect

with http_connect(MyService, "http://localhost:8080") as proxy:
    result = proxy.some_method(arg="value")
```

### Shared memory (zero-copy between co-located processes)

```python
from vgi_rpc import ShmPipeTransport, make_pipe_pair
from vgi_rpc.shm import ShmSegment

shm = ShmSegment.create(size=100 * 1024 * 1024)  # 100 MB
client_pipe, server_pipe = make_pipe_pair()
client_transport = ShmPipeTransport(client_pipe, shm)
server_transport = ShmPipeTransport(server_pipe, shm)
# Use transports with RpcServer / RpcConnection
```

### Worker pool (subprocess reuse)

Keeps idle subprocess workers alive between calls, avoiding repeated spawn/teardown overhead:

```python
from vgi_rpc import WorkerPool

pool = WorkerPool(max_idle=4, idle_timeout=60.0)

# Workers are reused across connect() calls
with pool.connect(MyService, ["python", "worker.py"]) as proxy:
    result = proxy.some_method(arg="value")

# With pool-managed shared memory (each borrow gets its own isolated segment)
pool_shm = WorkerPool(max_idle=4, shm_size=4 * 1024 * 1024)
with pool_shm.connect(MyService, ["python", "worker.py"]) as proxy:
    batches = list(proxy.generate(count=100))
# subprocess back in pool; SHM segment cleaned up automatically

pool.close()  # terminates all idle workers
```

## Streaming

### Producer stream (server pushes data)

```python
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import CallContext, OutputCollector, ProducerState, Stream, StreamState, serve_pipe


@dataclass
class CountdownState(ProducerState):
    """Counts down from n."""

    n: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce next value or finish."""
        if self.n <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.n]})
        self.n -= 1


class CountdownService(Protocol):
    """Service with a producer stream."""

    def countdown(self, n: int) -> Stream[StreamState]: ...


class CountdownServiceImpl:
    """Countdown implementation."""

    def countdown(self, n: int) -> Stream[CountdownState]:
        """Count down from n."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=CountdownState(n=n))


# Client iterates:
with serve_pipe(CountdownService, CountdownServiceImpl()) as proxy:
    for batch in proxy.countdown(n=3):
        print(batch.batch.to_pydict())
    # {'value': [3]}
    # {'value': [2]}
    # {'value': [1]}
```

### Exchange stream (bidirectional lockstep)

```python
from dataclasses import dataclass

import pyarrow as pa

from vgi_rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    Stream,
    StreamState,
    serve_pipe,
)


@dataclass
class RunningSum(ExchangeState):
    """Accumulates a running sum."""

    total: float = 0.0
    input_schema: pa.Schema = pa.schema([pa.field("value", pa.float64())])

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Add input values to running total."""
        for v in input.batch.column("value"):
            self.total += v.as_py()
        out.emit_pydict({"total": [self.total]})


# Client uses context manager + exchange():
with serve_pipe(SumService, SumServiceImpl()) as proxy:
    with proxy.accumulate(initial=0.0) as session:
        result = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
        print(result.batch.to_pydict())  # {'total': [3.0]}
```

## Dataclass Serialization

Use `ArrowSerializableDataclass` as a mixin for structured types:

```python
from dataclasses import dataclass
from vgi_rpc import ArrowSerializableDataclass


@dataclass(frozen=True)
class Measurement(ArrowSerializableDataclass):
    """A measurement with auto-generated Arrow schema."""

    timestamp: str
    value: float
    tags: list[str]


# Serialize / deserialize
m = Measurement(timestamp="2025-01-01T00:00:00Z", value=42.0, tags=["a"])
data = m.serialize_to_bytes()
m2 = Measurement.deserialize_from_bytes(data)

# Use directly as RPC parameters and return types
class DataService(Protocol):
    def record(self, measurement: Measurement) -> str: ...
    def latest(self) -> Measurement: ...
```

## Authentication (HTTP)

```python
import falcon
from vgi_rpc import AuthContext, RpcServer, make_wsgi_app


def authenticate(req: falcon.Request) -> AuthContext:
    """Validate Bearer token."""
    header = req.get_header("Authorization") or ""
    if not header.startswith("Bearer "):
        raise ValueError("Missing Bearer token")
    # ... validate token ...
    return AuthContext(domain="jwt", authenticated=True, principal="alice")


server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, authenticate=authenticate)
```

Check auth in methods via `CallContext`:

```python
from vgi_rpc import CallContext


class MyServiceImpl:
    """Implementation with auth."""

    def secret(self, ctx: CallContext) -> str:
        """Require authentication."""
        ctx.auth.require_authenticated()  # raises PermissionError if not
        return f"hello {ctx.auth.principal}"
```

`ctx: CallContext` is injected by the framework — it does NOT appear in the Protocol definition.

## Logging

### Client-directed logs (travel over the wire to the caller)

```python
from vgi_rpc import CallContext, Level


class MyServiceImpl:
    """Implementation with client logging."""

    def process(self, data: str, ctx: CallContext) -> str:
        """Process with client-visible logs."""
        ctx.client_log(Level.INFO, "Processing started", input_size=str(len(data)))
        return data.upper()
```

Receive on the client:

```python
from vgi_rpc import Message, serve_pipe


def handle_log(msg: Message) -> None:
    """Handle server log messages."""
    print(f"[{msg.level.value}] {msg.message}")


with serve_pipe(MyService, MyServiceImpl(), on_log=handle_log) as proxy:
    proxy.process(data="hello")
```

### Server-side logging (stdlib logging)

```python
from vgi_rpc import CallContext


class MyServiceImpl:
    """Implementation with server logging."""

    def process(self, data: str, ctx: CallContext) -> str:
        """Process with server-side logs."""
        ctx.logger.info("Processing request", extra={"input_size": len(data)})
        return data.upper()
```

Access logs are emitted automatically on `vgi_rpc.access` with per-call I/O statistics (`input_batches`, `output_batches`, `input_rows`, `output_rows`, `input_bytes`, `output_bytes`). Logger hierarchy:

| Logger | Purpose |
|---|---|
| `vgi_rpc` | Root — attach handlers here |
| `vgi_rpc.access` | One INFO record per RPC call (automatic, with I/O stats) |
| `vgi_rpc.service.<Protocol>` | `ctx.logger` for each service |
| `vgi_rpc.rpc` | Framework lifecycle |
| `vgi_rpc.pool` | Worker pool: spawn, evict, borrow/return, abandoned streams |
| `vgi_rpc.http` | HTTP transport |
| `vgi_rpc.wire.*` | Wire protocol debugging (see below) |

### Wire protocol debugging

Enable `vgi_rpc.wire` at DEBUG to see exact Arrow IPC batches, schemas, metadata, and stream lifecycle events flowing over the wire:

```python
import logging

logging.getLogger("vgi_rpc.wire").setLevel(logging.DEBUG)
logging.getLogger("vgi_rpc.wire").addHandler(logging.StreamHandler())
```

Six sub-loggers for targeted debugging:

| Logger | What it shows |
|---|---|
| `vgi_rpc.wire.request` | Request schemas, kwargs, metadata (both send and receive sides) |
| `vgi_rpc.wire.response` | Result batches, error batches, routing (inline/shm/external) |
| `vgi_rpc.wire.batch` | How each batch is classified: data vs. log vs. error |
| `vgi_rpc.wire.stream` | Stream lifecycle: init, tick, exchange, close |
| `vgi_rpc.wire.transport` | Pipe/subprocess open, close, fd numbers, process IDs |
| `vgi_rpc.wire.http` | HTTP request URLs, response status codes, body sizes |

For cross-language implementations, run the Python side with full wire debugging to see the exact IPC format your implementation must produce/consume. Key requirements: schema metadata must include `vgi_rpc.method`, batch metadata must include `vgi_rpc.request_version` = `"1"`, requests are single-row batches, and log/error batches are zero-row batches with `vgi_rpc.log_level` + `vgi_rpc.log_message` in batch metadata.

## Call Statistics

`CallStatistics` tracks per-call I/O counters (batches, rows, bytes) for usage accounting. Created automatically at dispatch start, surfaced through the access log and OTel spans:

```python
from vgi_rpc import CallStatistics  # public export for type references
```

Counters: `input_batches`, `output_batches`, `input_rows`, `output_rows`, `input_bytes`, `output_bytes`. Byte counts use `pa.RecordBatch.get_total_buffer_size()` (logical Arrow buffer sizes, no IPC framing overhead).

When `instrument_server()` is active, stats appear as span attributes: `rpc.vgi_rpc.input_batches`, `rpc.vgi_rpc.output_batches`, etc.

## Error Handling

Server exceptions propagate to the client as `RpcError`:

```python
from vgi_rpc import RpcError

try:
    proxy.failing_method()
except RpcError as e:
    print(e.error_type)        # "ValueError"
    print(e.error_message)     # "something went wrong"
    print(e.remote_traceback)  # full server-side traceback
```

The transport stays clean after errors — subsequent calls work normally.

## Introspection

### Static (from Protocol class, no server needed)

```python
from vgi_rpc import describe_rpc, rpc_methods

methods = rpc_methods(Calculator)
print(describe_rpc(Calculator))
```

### Runtime (requires `enable_describe=True` on server)

```python
from vgi_rpc import RpcServer, introspect, http_introspect

# Enable on server
server = RpcServer(MyService, MyServiceImpl(), enable_describe=True)

# Query over pipe/subprocess
desc = introspect(proxy._transport)

# Query over HTTP
desc = http_introspect("http://localhost:8080")
```

## External Storage (Large Batches)

When batches exceed a size threshold, they're uploaded to S3/GCS and replaced with pointer batches. Clients resolve pointers transparently.

```python
from vgi_rpc import Compression, ExternalLocationConfig, FetchConfig, RpcServer, S3Storage

storage = S3Storage(bucket="my-bucket", prefix="rpc-data/")
config = ExternalLocationConfig(
    storage=storage,
    externalize_threshold_bytes=1_048_576,  # 1 MiB
    compression=Compression(),  # optional zstd
    fetch_config=FetchConfig(chunk_size_bytes=8 * 1024 * 1024),
)

server = RpcServer(MyService, MyServiceImpl(), external_location=config)
```

## IPC Validation

```python
from vgi_rpc import IpcValidation, RpcServer, connect

# Server: full validation (default)
server = RpcServer(MyService, MyServiceImpl(), ipc_validation=IpcValidation.FULL)

# Client: lighter validation for trusted servers
with connect(MyService, cmd, ipc_validation=IpcValidation.STANDARD) as proxy: ...
```

Levels: `NONE` (no checks), `STANDARD` (structural), `FULL` (structural + data buffers, default).

## Key API Signatures

### RpcServer

```python
RpcServer(
    protocol: type,
    implementation: object,
    *,
    external_location: ExternalLocationConfig | None = None,
    server_id: str | None = None,
    enable_describe: bool = False,
    ipc_validation: IpcValidation = IpcValidation.FULL,
)
```

### connect (subprocess)

```python
connect(
    protocol: type[P],
    cmd: list[str],
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    stderr: StderrMode = StderrMode.INHERIT,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> ContextManager[P]
```

### serve_pipe (in-process)

```python
serve_pipe(
    protocol: type[P],
    implementation: object,
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation | None = None,
) -> ContextManager[P]
```

### http_connect

```python
http_connect(
    protocol: type[P],
    base_url: str,
    *,
    prefix: str = "/vgi",
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> ContextManager[P]
```

### make_wsgi_app

```python
make_wsgi_app(
    server: RpcServer,
    *,
    prefix: str = "/vgi",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    cors_origins: str | Iterable[str] | None = None,
    upload_url_provider: UploadUrlProvider | None = None,
    max_upload_bytes: int | None = None,
) -> falcon.App
```

### Stream

```python
Stream(
    output_schema: pa.Schema,
    state: S,
    input_schema: pa.Schema = _EMPTY_SCHEMA,  # set for exchange streams
)
```

### AnnotatedBatch

```python
AnnotatedBatch(batch: pa.RecordBatch, custom_metadata: pa.KeyValueMetadata | None = None)
AnnotatedBatch.from_pydict(data: dict) -> AnnotatedBatch  # convenience constructor
```

### WorkerPool

```python
WorkerPool(
    max_idle: int = 4,           # global cap on idle workers (all commands)
    idle_timeout: float = 60.0,  # seconds before idle worker eviction
    stderr: StderrMode = StderrMode.INHERIT,
    stderr_logger: logging.Logger | None = None,
    shm_size: int | None = None, # per-borrow SHM segment size (bytes); None = no SHM
)

pool.connect(
    protocol: type[P],
    cmd: list[str],
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> ContextManager[P]

pool.metrics -> PoolMetrics  # borrows, spawns, reuses, returns, discards, evictions, idle, active
pool.idle_count -> int
pool.active_count -> int
pool.close()  # terminates all idle workers, stops reaper thread
```

### OutputCollector (used in StreamState.process)

```python
out.emit(batch: pa.RecordBatch)
out.emit_pydict(data: dict)
out.emit_arrays(arrays: list[pa.Array])
out.client_log(level: Level, message: str, **extra: str)
out.finish()  # end producer stream
```
