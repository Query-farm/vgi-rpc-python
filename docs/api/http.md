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

### Sticky Sessions (opt-in)

HTTP sticky sessions let an RPC method bind a Python object — an open DuckDB cursor, a loaded model handle, a streaming LLM client — to the worker process that opened it, keyed by a signed session token that the client echoes in a `VGI-Session` header. Subsequent requests from the same client (inside a `with_session_token()` block) carry the header and the framework restores the object as `ctx.session`. Misroutes, expiries, and process restarts surface as a typed `SessionLostError` so apps can decide whether to retry or fail loudly.

The full wire contract — token format, header conventions, error kinds, the per-session serialization model, drain and crash semantics, load-balancer integration — lives in [`docs/sticky-sessions-spec.md`](../sticky-sessions-spec.md). The quickstart:

```python
from vgi_rpc import RpcServer, make_wsgi_app

server = RpcServer(MyService, MyServiceImpl())
app = make_wsgi_app(server, enable_sticky=True, sticky_default_ttl=300)
```

A method body opens a session by handing the framework a state object:

```python
class MyServiceImpl:
    def open_query(self, sql: str, ctx) -> str:
        cursor = duckdb.connect().execute(sql)
        ctx.open_session(cursor)               # framework mints + returns the token
        return "ok"

    def next_rows(self, n: int, ctx) -> bytes:
        return ctx.session.fetch_arrow_table(n).serialize().to_pybytes()

    def close_query(self, ctx) -> None:
        ctx.close_session()                    # closes cursor + evicts entry
```

On the client side, every session-using call lives inside a `with_session_token()` block — that's the opt-in signal the server requires (the leaked-session guard):

```python
from vgi_rpc.http import http_connect

with http_connect(MyService, "http://localhost:8080") as conn, conn.with_session_token() as sess:
    sess.open_query(sql="SELECT * FROM big")
    rows = sess.next_rows(n=1000)
    sess.close_query()
```

The block's exit fires a best-effort `DELETE /vgi/__session__` so handle-bearing state gets released promptly. To stash a token across processes, call `sess.detach()` before the block exits — that hands the caller the token and suppresses the DELETE so the server-side session survives until its TTL or another caller closes it.

**HTTP-only.** Sticky machinery is not installed on pipe/subprocess/unix transports — those run as single processes where "sticky" is meaningless. `ctx.open_session` raises `RuntimeError("sticky sessions not available on this transport")` if called over a non-HTTP transport, so apps can detect-and-fall-back.

#### Client-driven routing via echo headers

Sticky LBs are not the only way to get a session-token-carrying request back to the worker that owns the session. With **echo headers**, the server tells the client (at session-open time) to attach an arbitrary set of headers on every subsequent request in the session, and the platform's edge proxy routes on those headers. Two helpers ship for [Fly.io](https://fly.io), where `fly-force-instance-id` is the proactive routing header `fly-proxy` honours:

```python
from vgi_rpc import RpcServer
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.http.fly import auto_server_id, fly_sticky_echo_headers

server = RpcServer(
    MyService, MyServiceImpl(),
    server_id=auto_server_id(),                # ⇒ FLY_MACHINE_ID on Fly, random elsewhere
)
app = make_wsgi_app(
    server,
    enable_sticky=True,
    sticky_echo_headers=fly_sticky_echo_headers(),  # ⇒ {"fly-force-instance-id": <id>} on Fly, None elsewhere
)
```

On Fly the server emits `VGI-Echo-fly-force-instance-id: <machine-id>` on session-opening responses; the client captures it and replays `fly-force-instance-id: <machine-id>` on every subsequent request in the session; fly-proxy routes directly to the owning Machine. No LB configuration required.

Off Fly the helpers return `None` so the same code is a no-op — operators don't need conditional branches.

Generic API (for non-Fly platforms): pass any `dict[str, str]` as `sticky_echo_headers` and the server will emit them as `VGI-Echo-<name>` on the session-opening response. The client's `with_session_token()` view captures + replays automatically; `sess.current_echo_headers()` exposes the captured map for inspection or stashing.

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
