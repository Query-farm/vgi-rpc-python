# HTTP Transport

HTTP transport using Falcon (server) and httpx (client). Requires `pip install vgi-rpc[http]`.

## Quick Start

### Server

Create a WSGI app and serve it with any WSGI server (waitress, gunicorn, etc.):

```python
from vgi_rpc import RpcServer, make_wsgi_app

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server)
# serve `app` with waitress, gunicorn, etc.
```

### Client

```python
from vgi_rpc import http_connect

with http_connect(MyService, "http://localhost:8080") as proxy:
    result = proxy.echo(message="hello")  # proxy is typed as MyService
```

### Testing (no real server)

`make_sync_client` wraps a Falcon `TestClient` so you can test the full HTTP stack in-process:

```python
from vgi_rpc import RpcServer
from vgi_rpc.http import http_connect, make_sync_client

server = RpcServer(MyService, MyServiceImpl())
client = make_sync_client(server)

with http_connect(MyService, client=client) as proxy:
    assert proxy.echo(message="hello") == "hello"
```

### Landing Page

By default, `GET {prefix}` (e.g. `GET /vgi`) returns an HTML landing page showing the `vgi-rpc` logo, the protocol name, server ID, and links. When the server has `enable_describe=True`, the landing page includes a link to the [describe page](#describe-page).

To disable the landing page:

```python
app = make_wsgi_app(server, enable_landing_page=False)
```

`POST {prefix}` returns 405 Method Not Allowed — it does not interfere with RPC routing.

### Describe Page

When the server has `enable_describe=True`, `GET {prefix}/describe` (e.g. `GET /vgi/describe`) returns an HTML page listing all methods, their parameters (name, type, default), return types, docstrings, and method type badges (UNARY / STREAM). The `__describe__` introspection method is filtered out.

Both `enable_describe=True` on the `RpcServer` **and** `enable_describe_page=True` (the default) on `make_wsgi_app()` are required.

To disable only the HTML page while keeping the `__describe__` RPC method available:

```python
app = make_wsgi_app(server, enable_describe_page=False)
```

!!! note "Reserved path"
    When the describe page is active, the path `{prefix}/describe` is reserved for the HTML page. If your service has an RPC method literally named `describe`, you must set `enable_describe_page=False`.

### Not-Found Page

By default, `make_wsgi_app()` installs a friendly HTML 404 page for any request that does not match an RPC route. If someone navigates to the server root or a random path in a browser, they see the `vgi-rpc` logo, the service protocol name, and a link to [vgi-rpc.query.farm](https://vgi-rpc.query.farm) instead of a generic error.

This does **not** affect RPC clients — a request to a valid RPC route for a non-existent method still returns a machine-readable Arrow IPC error with HTTP 404.

To disable the page:

```python
app = make_wsgi_app(server, enable_not_found_page=False)
```

## API Reference

### Server

::: vgi_rpc.http.make_wsgi_app

### Client

::: vgi_rpc.http.http_connect

::: vgi_rpc.http.http_introspect

::: vgi_rpc.http.http_capabilities

::: vgi_rpc.http.request_upload_urls

### Capabilities

::: vgi_rpc.http.HttpServerCapabilities

### Stream Session

::: vgi_rpc.http.HttpStreamSession

### Testing

::: vgi_rpc.http.make_sync_client

### Header Constants

::: vgi_rpc.http.MAX_REQUEST_BYTES_HEADER

::: vgi_rpc.http.MAX_UPLOAD_BYTES_HEADER

::: vgi_rpc.http.UPLOAD_URL_HEADER
