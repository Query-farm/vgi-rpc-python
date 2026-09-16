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

Enable the built-in reflection protocol on the server, then query it from any client:

```python
from vgi_rpc import RpcServer, introspect, connect

# Server: host `vgi_rpc.Reflection.v1` alongside the application protocol
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

The `ServiceDescription` carries language-neutral method metadata — name, method type, request/result/header Arrow schemas, and stream flags — everything a dynamic client needs to *invoke* methods without the Python Protocol class. It also includes `protocol_hash` (a SHA-256 digest identifying the contract) and `protocol_version`.

Discovery takes two round trips — `list_protocols` to learn what the server hosts, then `describe` for one of them. With a server free to host several protocols there is no longer a single "the" protocol to describe without first asking; both `introspect()` and `http_introspect()` accept an explicit `protocol` to skip the discovery hop.

The retired `__describe__` method is gone: introspection is the ordinary `vgi_rpc.Reflection.v1` protocol, dispatched through a normal binding, so it gets access logging, telemetry and the normal error envelope like any other call. A server that externalizes payloads externalizes reflection replies too, so an HTTP client needs its `external_location` configured to read them.

Python-flavoured fields (parameter type names, default values, docstrings) are **not** on the wire, having been dropped for cross-language neutrality. Consumers that need those human-readable details import the Protocol source class directly rather than reconstructing them from the wire. `DESCRIBE_VERSION` is reported as `"5"` and is vestigial — the protocol's major version is part of its own name now, so there is no separate format number to negotiate.

For stream methods that declare a header type (`Stream[S, H]`), `MethodDescription` includes `has_header` (`bool`) and `header_schema` (`pa.Schema | None`) fields describing the header's Arrow schema. Streams also carry `is_exchange` (`bool | None`) — `True` for exchange (bidi), `False` for producer, `None` for unary. Header fields are `False` / `None` for methods without headers.

## API Reference

### Functions

::: vgi_rpc.introspect.introspect

::: vgi_rpc.rpc.rpc_methods

::: vgi_rpc.rpc.describe_rpc

### Data Classes

::: vgi_rpc.introspect.ServiceDescription

::: vgi_rpc.introspect.MethodDescription

### Constants

::: vgi_rpc.introspect.DESCRIBE_VERSION
