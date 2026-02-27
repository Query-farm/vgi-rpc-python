# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**vgi-rpc** is a transport-agnostic RPC framework built on Apache Arrow IPC serialization. RPC interfaces are defined as Python Protocol classes; the framework derives Arrow schemas from type annotations and provides typed client proxies with automatic serialization/deserialization.

## Commands

```bash
# Run all tests (includes mypy type checking and ruff linting via pytest plugins)
pytest

# Run a single test
pytest tests/test_rpc.py::test_name

# Lint and format
ruff check vgi_rpc/ tests/
ruff format vgi_rpc/ tests/

# Type checking
mypy vgi_rpc/

# Coverage (80% minimum, branch coverage enabled)
pytest --cov=vgi_rpc
```

Uses `uv` as the package manager. Install dev dependencies with `uv sync --all-extras`.

Tests should complete in 50 seconds or less ALWAYS!

Discourage the use of Any types, check mypy strict type coverage and always try to improve it.

Before pushing changes make sure, mypy, ruff and tests pass.

Pay attention to mypy strict type checking make sure strict typing is preserved.

Verify "ty" type checking too.

The full process before committing code is

1. Run mypy and resolve all errors
2. Run ty and resolve all errors
3. Run pytest for all tests
4. Run ruff format on all files

Then you can commit.

## Architecture

### Core modules (`vgi_rpc/`)

- **`rpc/`** — The RPC framework package. Defines the wire protocol, method types (UNARY, STREAM), and the core classes: `RpcServer`, `RpcConnection`, `RpcTransport`, `PipeTransport`, `ShmPipeTransport`, `SubprocessTransport`, `StreamSession`, `AnnotatedBatch`, `OutputCollector`. Also defines `AuthContext` (frozen dataclass for authentication state), `CallContext` (request-scoped context injected into methods via `ctx` parameter), `_TransportContext` (contextvar bridge for HTTP auth), `RpcError`, and `VersionError`. Introspects Protocol classes via `rpc_methods()` to extract `RpcMethodInfo` (schemas, method type). Client gets a typed proxy from `RpcConnection`; server dispatches via `RpcServer.serve()`. The `_debug.py` submodule provides wire protocol debug logging infrastructure: 6 logger instances under the `vgi_rpc.wire.*` hierarchy (`request`, `response`, `batch`, `stream`, `transport`, `http`) and 4 formatting helpers (`fmt_schema`, `fmt_metadata`, `fmt_batch`, `fmt_kwargs`). All log points use `isEnabledFor` guards for zero overhead when disabled.

- **`utils.py`** — Arrow serialization layer. `ArrowSerializableDataclass` mixin auto-generates `ARROW_SCHEMA` from dataclass field annotations and provides `serialize()`/`deserialize_from_batch()`. Handles type inference from Python types to Arrow types (including generics, Enum, Optional, nested dataclasses). Also provides low-level IPC stream read/write helpers, `IpcValidation` enum, and `ValidatedReader`.

- **`log.py`** — Structured log messages (`Message` with `Level` enum). Messages are serialized out-of-band as zero-row batches with metadata keys `vgi_rpc.log_level`, `vgi_rpc.log_message`, `vgi_rpc.log_extra`. Server methods access logging via the `CallContext` (see below).

- **`logging_utils.py`** — `VgiJsonFormatter`, a `logging.Formatter` subclass that serializes log records as single-line JSON. Not auto-imported; must be imported explicitly from `vgi_rpc.logging_utils`.

- **`metadata.py`** — Shared helpers for `pa.KeyValueMetadata`. Centralises well-known metadata key constants (`vgi_rpc.method`, `vgi_rpc.stream_state#b64`, `vgi_rpc.log_level`, `vgi_rpc.log_message`, `vgi_rpc.log_extra`, `vgi_rpc.server_id`, `vgi_rpc.request_version`, `vgi_rpc.location`, `vgi_rpc.shm_offset`, etc.) and provides encoding, merging, and key-stripping utilities used by `rpc/`, `http/`, `log.py`, `external.py`, `shm.py`, and `introspect.py`.

- **`introspect.py`** — Introspection support. Provides the built-in `__describe__` RPC method, `MethodDescription`, `ServiceDescription`, `build_describe_batch`, `parse_describe_batch`, and `introspect()`. Enabled on `RpcServer` via `enable_describe=True`.

- **`shm.py`** — Shared memory transport support. Provides `ShmAllocator`, `ShmSegment`, and pointer batch helpers for zero-copy Arrow IPC batch transfer between co-located processes. Used by `ShmPipeTransport`.

- **`pool.py`** — Subprocess process pool with shared memory support. `WorkerPool` keeps idle worker subprocesses alive between `connect()` calls, avoiding repeated spawn/teardown overhead. Workers are keyed by command tuple, cached up to `max_idle` globally (LIFO for cache warmth), and evicted by a daemon reaper thread after `idle_timeout`. When `shm_size` is set on the pool, each `connect()` borrow gets its own isolated `ShmSegment` that is automatically created and destroyed per borrow. Tracks `_stream_opened` / `_last_stream_session` to detect abandoned streams and discard workers with stale transport state. Provides `PoolMetrics` for observability. Logger: `vgi_rpc.pool`.

- **`external.py`** — ExternalLocation batch support for large data. When batches exceed a configurable size threshold, they are uploaded to pluggable `ExternalStorage` (e.g. S3) and replaced with zero-row pointer batches containing a `vgi_rpc.location` URL metadata key. Readers resolve pointers transparently via `external_fetch.fetch_url()` (aiohttp-based parallel fetching); writers externalize batches above the threshold. Provides `ExternalLocationConfig`, `ExternalStorage` protocol, `UploadUrl`, `UploadUrlProvider`, `Compression` enum, `https_only_validator`, and production/resolution functions. Supports optional zstd compression.

- **`external_fetch.py`** — Parallel range-request URL fetching. Issues a HEAD probe to learn `Content-Length` and `Accept-Ranges`, then either fetches in parallel chunks with speculative hedging for stragglers, or falls back to a single GET. Maintains a persistent `aiohttp.ClientSession` per `FetchConfig` on a daemon thread. Handles zstd decompression and stale-connection recovery. Provides `FetchConfig` and `fetch_url()`.

- **`cli.py`** *(optional — `pip install vgi-rpc[cli]`)* — Command-line interface. A `typer`-based CLI registered as the `vgi-rpc` entry point. Provides `describe`, `call`, and `loggers` commands for introspecting and invoking methods on any vgi-rpc service. Supports output formats `auto`, `json`, `table`, and `arrow` (raw Arrow IPC via `--format arrow`), with `--output`/`-o` for file output. Stream headers are surfaced in all formats.

- **`s3.py`** *(optional — `pip install vgi-rpc[s3]`)* — S3 storage backend implementing `ExternalStorage`. Uses boto3 to upload IPC data and generate pre-signed URLs. Supports custom endpoints for MinIO/LocalStack.

- **`gcs.py`** *(optional — `pip install vgi-rpc[gcs]`)* — Google Cloud Storage backend implementing `ExternalStorage`. Uses google-cloud-storage to upload IPC data and generate V4 signed URLs. Relies on Application Default Credentials.

- **`http/`** *(optional — `pip install vgi-rpc[http]`)* — HTTP transport package using Falcon (server) and httpx (client). Exposes `make_wsgi_app()` to serve an `RpcServer` as a Falcon WSGI app, and `http_connect()` for the client side. Streaming is stateless: each exchange carries serialized `StreamState` in a signed token in Arrow custom metadata. Supports pluggable authentication via an `authenticate` callback and `_AuthMiddleware`. Includes `_testing.py` with `make_sync_client()` for in-process testing without a real HTTP server.

### Wire protocol

Multiple IPC streams are written sequentially on the same pipe. Every request batch carries `vgi_rpc.request_version` in custom metadata; the server validates this before dispatch and rejects mismatches with `VersionError`. Each method call writes one request stream and reads one response stream:

- **Unary**: Client sends params batch → Server replies with log batches + result/error batch
- **Stream**: Initial params exchange, then lockstep: client sends input batch (tick for producer, real data for exchange), server replies with log batches + output batch, repeating until EOS

For HTTP transport, the wire protocol maps to separate endpoints: `POST /vgi/{method}` (unary), `POST /vgi/{method}/init` (stream init), `POST /vgi/{method}/exchange` (stream exchange).

### Key patterns

**Defining an RPC service**: Write a `Protocol` class where return types determine method type — plain types for unary, `Stream[S]` for streaming (both producer and exchange patterns).

**Stream state**: Streaming methods return a `Stream[S]` where `S` is a `StreamState` subclass. The state's `process(input, out, ctx)` method is called once per iteration. Producer streams (default `input_schema=_EMPTY_SCHEMA`) ignore the input and call `out.finish()` to end. Exchange streams set `input_schema` to a real schema and process client data.

**CallContext injection**: Server method implementations can accept an optional `ctx: CallContext` parameter. `CallContext` provides `auth` (`AuthContext`), `client_log()` (client-directed logging), `emit_client_log` (raw `ClientLog` callback), `transport_metadata` (e.g. `remote_addr` from HTTP), and a `logger` property returning a `LoggerAdapter` with request context pre-bound. The parameter is injected by the framework — it does **not** appear in the Protocol definition.

**Authentication**: `AuthContext` (frozen dataclass) carries `domain`, `authenticated`, `principal`, and `claims`. For HTTP transport, `make_wsgi_app(authenticate=...)` installs `_AuthMiddleware` that calls the callback on each request and populates `CallContext.auth`. Pipe transport gets anonymous auth by default. Methods can call `ctx.auth.require_authenticated()` to gate access.

**Server identity**: Each `RpcServer` gets a `server_id` (auto-generated 12-char hex or caller-supplied). This ID is attached to all log and error batches as `vgi_rpc.server_id` metadata for distributed tracing. `RpcServer` also accepts `enable_describe=True` to register the synthetic `__describe__` introspection method.

**Error propagation**: Server exceptions become zero-row batches with error metadata; clients receive `RpcError` with `error_type`, `error_message`, and `remote_traceback`. The transport stays clean for subsequent requests.

## Code Style

- Line length 120, double quotes, target Python 3.12+
- Strict mypy (`python_version = "3.12"`, `strict = true`)
- Ruff rules: E, F, I, UP, B, SIM, D, RUF, PERF (includes docstring enforcement)
- Google-style docstrings with Args/Returns/Raises sections
