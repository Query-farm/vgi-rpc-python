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

See also [`IPCError`](serialization.md#vgi_rpc.utils.IPCError) in the Serialization module.

## Convenience Functions

::: vgi_rpc.rpc.run_server

::: vgi_rpc.rpc.connect

::: vgi_rpc.rpc.serve_pipe

::: vgi_rpc.rpc.describe_rpc

## Constants

::: vgi_rpc.metadata.REQUEST_VERSION
