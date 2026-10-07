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

### Runtime introspection (over a connection you already hold)

An `RpcServer` built with `enable_describe=True` hosts the built-in
`vgi_rpc.Reflection.v1` protocol beside its application protocols. Ask it
through the proxy you already have — on any transport — with
`list_protocols()` and `describe_protocol()`:

```python
from vgi_rpc import connect, describe_protocol, list_protocols

with connect(Calculator, ["python", "worker.py"]) as proxy:
    for p in list_protocols(proxy):
        print(p.name, p.version, p.hash)
    desc = describe_protocol(proxy, "Calculator")
    for name, method in desc.methods.items():
        print(f"{name}: {method.method_type.value}")
    proxy.add(a=1.0, b=2.0)  # the connection is still yours
```

Both reuse the target's connection rather than opening one. A proxy from
`connect`, `serve_pipe`, `unix_connect`, `tcp_connect`, `iroh_connect`,
`WorkerPool.connect` or `RpcConnection` is rebound to reflection over the same
byte stream — the server routes each request by its `vgi_rpc.protocol` key — so
do not call them while a stream is open on that connection. A proxy from
`http_connect` or `httpi_connect` shares its `httpx2.Client`, prefix, auth,
retry and response-budget settings. The proxy may be bound to any protocol the
server hosts; only its connection matters. A raw `RpcTransport` is accepted
too.

`list_protocols()` returns one `HostedProtocol` per hosted protocol, in the
server's order: application protocols in registration order (the primary
first), then the framework's own (`vgi_rpc.Reflection.v1`, and
`vgi_rpc.Identity.v1` on an HTTP server that hosts it). `hash` is the
protocol's canonical SHA-256, identical in every port for the same wire
surface, so a client that cached a description for that hash can skip
`describe_protocol()`. Use the listing to discover an optional protocol before
calling it, rather than calling it and reading an error.

`describe_protocol()` makes two calls — `list_protocols` and then
`describe(name)` — and returns a `ServiceDescription`. A name the server does
not host is an `RpcError` with `error_kind == "protocol_not_supported"`.

**Servers without reflection.** A server built without `enable_describe=True`
(the default), or one older than reflection, answers "not hosted" (`protocol_not_supported`, an
unknown method, `UNIMPLEMENTED`, or over HTTP a bare 404). Both functions raise
`ReflectionNotSupportedError` for that answer — a subclass of `RpcError`
carrying the server's error fields — and nothing else: no listing is inferred,
because only the caller knows which protocol it expected the server to speak.
The connection remains usable. Any other failure propagates as itself.

`introspect(transport)` and `http_introspect(url)` remain for callers that
have only a raw transport or a URL and want the primary protocol described in
one call:

```python
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

::: vgi_rpc.introspect.list_protocols

::: vgi_rpc.introspect.describe_protocol

::: vgi_rpc.introspect.introspect

::: vgi_rpc.rpc.rpc_methods

::: vgi_rpc.rpc.describe_rpc

### Data Classes

::: vgi_rpc.introspect.HostedProtocol

::: vgi_rpc.introspect.ServiceDescription

::: vgi_rpc.introspect.MethodDescription

### Exceptions

::: vgi_rpc.introspect.ReflectionNotSupportedError

### Constants

::: vgi_rpc.introspect.DESCRIBE_VERSION
