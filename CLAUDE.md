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

Tests should complete in 30 seconds or less ALWAYS!

Discourage the use of Any types, check mypy strict type coverage and always try to improve it.

Before pushing changes make sure, mypy, ruff and tests pass.

Pay attention to mypy strict type checking make sure strict typing is preserved.

Verify "ty" type checking too.

## Architecture

### Core modules (`vgi_rpc/`)

- **`rpc.py`** — The RPC framework. Defines the wire protocol, method types (UNARY, SERVER_STREAM, BIDI_STREAM), and the core classes: `RpcServer`, `RpcConnection`, `RpcTransport`, `PipeTransport`. Introspects Protocol classes via `rpc_methods()` to extract `RpcMethodInfo` (schemas, method type). Client gets a typed proxy from `RpcConnection`; server dispatches via `RpcServer.serve()`.

- **`utils.py`** — Arrow serialization layer. `ArrowSerializableDataclass` mixin auto-generates `ARROW_SCHEMA` from dataclass field annotations and provides `serialize()`/`deserialize_from_batch()`. Handles type inference from Python types to Arrow types (including generics, Enum, Optional, nested dataclasses). Also provides low-level IPC stream read/write helpers.

- **`log.py`** — Structured log messages (`Message` with `Level` enum). Messages are serialized out-of-band as zero-row batches with metadata keys `vgi_rpc.log_level`, `vgi_rpc.log_message`, `vgi_rpc.log_extra`. Server methods can accept an optional `emit_log: EmitLog` parameter injected by the framework.

- **`metadata.py`** — Shared helpers for `pa.KeyValueMetadata`. Centralises well-known metadata key constants (`vgi_rpc.method`, `vgi_rpc.bidi_state`, `vgi_rpc.log_level`, etc.) and provides encoding, decoding, merging, and key-stripping utilities used by `rpc.py`, `http.py`, `utils.py`, `log.py`, and `external.py`.

- **`external.py`** — ExternalLocation batch support for large data. When batches exceed a configurable size threshold, they are uploaded to pluggable `ExternalStorage` (e.g. S3) and replaced with zero-row pointer batches containing a `vgi_rpc.location` URL metadata key. Readers resolve pointers transparently via `external_fetch.fetch_url()` (aiohttp-based parallel fetching); writers externalize batches above the threshold. Provides `ExternalLocationConfig`, `ExternalStorage` protocol, and production/resolution functions. Supports optional zstd compression.

- **`external_fetch.py`** — Parallel range-request URL fetching. Issues a HEAD probe to learn `Content-Length` and `Accept-Ranges`, then either fetches in parallel chunks with speculative hedging for stragglers, or falls back to a single GET. Maintains a persistent `aiohttp.ClientSession` per `FetchConfig` on a daemon thread. Handles zstd decompression and stale-connection recovery. Provides `FetchConfig` and `fetch_url()`.

- **`s3.py`** *(optional — `pip install vgi-rpc[s3]`)* — S3 storage backend implementing `ExternalStorage`. Uses boto3 to upload IPC data and generate pre-signed URLs. Supports custom endpoints for MinIO/LocalStack.

- **`gcs.py`** *(optional — `pip install vgi-rpc[gcs]`)* — Google Cloud Storage backend implementing `ExternalStorage`. Uses google-cloud-storage to upload IPC data and generate V4 signed URLs. Relies on Application Default Credentials.

- **`http.py`** *(optional — `pip install vgi-rpc[http]`)* — HTTP transport using Falcon (server) and httpx (client). Exposes `make_wsgi_app()` to serve an `RpcServer` as a Falcon WSGI app, and `http_connect()` for the client side. Bidi streaming is stateless: each exchange carries serialized `BidiStreamState` in Arrow custom metadata.

### Wire protocol

Multiple IPC streams are written sequentially on the same pipe. Each method call writes one request stream and reads one response stream:

- **Unary**: Client sends params batch → Server replies with log batches + result/error batch
- **Server Stream**: Client sends params batch → Server replies with interleaved log and data batches
- **Bidi Stream**: Initial params exchange, then lockstep: client sends input batch, server replies with log batches + output batch, repeating until EOS

### Key patterns

**Defining an RPC service**: Write a `Protocol` class where return types determine method type — plain types for unary, `ServerStream[S]` for server streaming, `BidiStream[S]` for bidirectional.

**Stream state**: Streaming methods return a state object (`ServerStreamState` or `BidiStreamState` subclass) that drives iteration via `produce(out)` or `process(input, out)` callbacks on `OutputCollector`.

**Error propagation**: Server exceptions become zero-row batches with error metadata; clients receive `RpcError` with `error_type`, `error_message`, and `remote_traceback`. The transport stays clean for subsequent requests.

## Code Style

- Line length 120, double quotes, target Python 3.12+
- Strict mypy (`python_version = "3.13"`, `strict = true`)
- Ruff rules: E, F, I, UP, B, SIM, D (includes docstring enforcement)
- Google-style docstrings with Args/Returns/Raises sections
