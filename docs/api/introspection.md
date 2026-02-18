# Introspection

Discover an RPC service's methods, schemas, and parameter types at runtime or statically.

## Usage

### Static introspection (no server needed)

Use `rpc_methods()` to inspect a Protocol class, or `describe_rpc()` for a human-readable summary:

```python
from vgi_rpc import describe_rpc, rpc_methods

methods = rpc_methods(Calculator)
for name, info in methods.items():
    print(f"{name}: {info.method_type.value}, params={info.params_schema}")

print(describe_rpc(Calculator))
```

### Runtime introspection (over a connection)

Enable the built-in `__describe__` method on the server, then query it from any client:

```python
from vgi_rpc import RpcServer, introspect, connect

# Server: enable describe
server = RpcServer(Calculator, CalculatorImpl(), enable_describe=True)

# Client: query over pipe/subprocess
with connect(Calculator, ["python", "worker.py"]) as proxy:
    desc = introspect(proxy._transport)
    for name, method in desc.methods.items():
        print(f"{name}: {method.method_type.value}")

# Client: query over HTTP
from vgi_rpc import http_introspect
desc = http_introspect("http://localhost:8080")
```

The `ServiceDescription` contains method metadata, parameter schemas, default values, and docstrings — everything a dynamic client needs without the Python Protocol class.

## API Reference

### Functions

::: vgi_rpc.introspect.introspect

::: vgi_rpc.rpc.rpc_methods

::: vgi_rpc.rpc.describe_rpc

### Data Classes

::: vgi_rpc.introspect.ServiceDescription

::: vgi_rpc.introspect.MethodDescription

### Constants

::: vgi_rpc.introspect.DESCRIBE_METHOD_NAME
