---
description: "Runnable vgi-rpc examples — hello world, streaming, HTTP transport, authentication, introspection, shared memory, and testing patterns."
hide:
  - navigation
---

# Examples

Runnable scripts demonstrating key vgi-rpc features. Each example is self-contained — copy, run, and modify. All examples are tested via `test_examples.py` to stay in sync with the API.

## Hello World

The minimal starting point: define a Protocol, implement it, and call through a typed proxy using in-process pipe transport.

```python
--8<-- "examples/hello_world.py"
```

## Streaming

Producer streams (server pushes data, client iterates) and exchange streams (lockstep bidirectional communication). Shows `ProducerState`, `ExchangeState`, `OutputCollector`, and `out.finish()`.

```python
--8<-- "examples/streaming.py"
```

## Structured Types

Using `ArrowSerializableDataclass` for complex parameters: dataclasses with enums, nested types, and optional fields. Demonstrates automatic Arrow schema inference.

```python
--8<-- "examples/structured_types.py"
```

## HTTP Server

Serve an RPC service over HTTP using Falcon + waitress. Shows `make_wsgi_app` with a WSGI server.

```python
--8<-- "examples/http_server.py"
```

## HTTP Client

Connect to an HTTP RPC service using `http_connect`. The proxy is typed as the Protocol class.

```python
--8<-- "examples/http_client.py"
```

## Subprocess Worker

Entry point for a subprocess RPC worker. Uses `run_server` to serve over stdin/stdout.

```python
--8<-- "examples/subprocess_worker.py"
```

## Subprocess Client

Connect to a subprocess worker with `connect`. Shows error handling with `RpcError`.

```python
--8<-- "examples/subprocess_client.py"
```

## Testing with Pipe Transport

Unit-testing pattern using `serve_pipe()` — no network or subprocess needed. Tests both unary and streaming methods.

```python
--8<-- "examples/testing_pipe.py"
```

## Testing with HTTP Transport

Unit-testing the full HTTP stack (including auth middleware) using `make_sync_client()` — no real HTTP server needed.

```python
--8<-- "examples/testing_http.py"
```

## Authentication

HTTP authentication with Bearer tokens. Shows `authenticate` callback, `AuthContext`, `CallContext.auth.require_authenticated()`, and guarded methods.

```python
--8<-- "examples/auth.py"
```

## Introspection

Runtime service discovery with `enable_describe=True`. Shows `introspect()` for pipe transport and `http_introspect()` for HTTP.

```python
--8<-- "examples/introspection.py"
```

## Shared Memory

Zero-copy shared memory transport with `ShmPipeTransport` and `ShmSegment`. Demonstrates the side-channel optimization for large batches between co-located processes.

```python
--8<-- "examples/shared_memory.py"
```
