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
