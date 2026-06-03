# Core RPC

The core module provides the server, connection, transport interface, error types, and convenience functions for defining and running RPC services.

## Typical Usage

Most users only need `serve_pipe` (testing) or `connect` (subprocess):

```python
from vgi_rpc import serve_pipe, connect

# In-process (tests)
with serve_pipe(MyService, MyServiceImpl()) as proxy:
    proxy.my_method(arg=42)

# Subprocess
with connect(MyService, ["python", "worker.py"]) as proxy:
    proxy.my_method(arg=42)
```

For more control, use `RpcServer` and `RpcConnection` directly.

## API Reference

### RpcServer

::: vgi_rpc.rpc.RpcServer

## RpcConnection

::: vgi_rpc.rpc.RpcConnection

## RpcTransport

::: vgi_rpc.rpc.RpcTransport

## RpcMethodInfo

::: vgi_rpc.rpc.RpcMethodInfo

## MethodType

::: vgi_rpc.rpc.MethodType

## Errors

::: vgi_rpc.rpc.RpcError

::: vgi_rpc.rpc.VersionError

::: vgi_rpc.rpc.ProtocolVersionError

`ProtocolVersionError` (a `VersionError` subclass) is raised at the dispatch
boundary when the client's `vgi_rpc.protocol_version` differs from the
server's on the major+minor components (patch is ignored). It is only active
when the Protocol class declares a `protocol_version`; otherwise the check is
disabled. `__describe__` is exempt so a mismatched client can still introspect
to discover the server's version.

### Typed marker errors

These exception classes carry a stable `error_kind` class attribute that
the wire serializer surfaces as the `vgi_rpc.error_kind` metadata key on
the EXCEPTION-level batch. Clients can pattern-match the kind instead of
substring-searching the error message.

::: vgi_rpc.rpc.MethodNotImplementedError

::: vgi_rpc.rpc.SessionLostError

::: vgi_rpc.rpc.ServerDrainingError

See also [`IPCError`](serialization.md#vgi_rpc.utils.IPCError) in the Serialization module.

## CallStatistics

::: vgi_rpc.rpc.CallStatistics

## Convenience Functions

::: vgi_rpc.rpc.run_server

::: vgi_rpc.rpc.connect

::: vgi_rpc.rpc.serve_pipe

::: vgi_rpc.rpc.describe_rpc

## Constants

::: vgi_rpc.metadata.REQUEST_VERSION
