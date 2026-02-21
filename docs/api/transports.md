# Transports

All transports implement the `RpcTransport` protocol (a readable + writable byte stream). Your service code is identical regardless of transport — only the setup differs.

## Choosing a Transport

| Transport | Use case | Latency | Setup |
|---|---|---|---|
| `serve_pipe` | Tests, demos, embedded | Lowest (in-process) | One line |
| `connect` / `SubprocessTransport` | Isolated workers, CLI tools | Low (stdin/stdout) | Spawn a child process |
| `serve_unix` / `unix_connect` | Local IPC, long-lived services | Low (Unix socket) | Socket path |
| `ShmPipeTransport` | Co-located processes, large batches | Lowest (zero-copy) | Shared memory segment |
| `http_connect` / `make_wsgi_app` | Network services, browser clients | Higher (HTTP) | WSGI server + client |

### Pipe (in-process, for tests)

```python
from vgi_rpc import serve_pipe

with serve_pipe(MyService, MyServiceImpl()) as proxy:
    result = proxy.add(a=1.0, b=2.0)  # proxy is typed as MyService
```

### Subprocess

```python
# worker.py
from vgi_rpc import run_server
run_server(MyService, MyServiceImpl())

# client.py
from vgi_rpc import connect
with connect(MyService, ["python", "worker.py"]) as proxy:
    result = proxy.add(a=1.0, b=2.0)
```

### Unix domain socket

Low-latency local IPC without subprocess management. The server listens on a socket path; clients connect by path. Not available on Windows.

Server entry point:

```python
# worker.py
from vgi_rpc.rpc import RpcServer, serve_unix

server = RpcServer(MyService, MyServiceImpl())
serve_unix(server, "/tmp/my-service.sock")
```

Client:

```python
from vgi_rpc.rpc import unix_connect

with unix_connect(MyService, "/tmp/my-service.sock") as proxy:
    result = proxy.add(a=1.0, b=2.0)
```

For in-process testing, `serve_unix_pipe` starts the server on a background thread with an auto-generated socket path:

```python
from vgi_rpc.rpc import serve_unix_pipe

with serve_unix_pipe(MyService, MyServiceImpl()) as proxy:
    result = proxy.add(a=1.0, b=2.0)
```

Use `threaded=True` on `serve_unix` to handle multiple concurrent clients (each connection gets its own thread):

```python
serve_unix(server, "/tmp/my-service.sock", threaded=True)
```

### Shared memory

Wraps a `PipeTransport` with a shared memory side-channel. When a batch fits in the segment, only a small pointer is sent over the pipe — the receiver reads data directly from shared memory:

```python
from vgi_rpc import ShmPipeTransport, make_pipe_pair
from vgi_rpc.shm import ShmSegment

shm = ShmSegment.create(size=100 * 1024 * 1024)  # 100 MB
client_pipe, server_pipe = make_pipe_pair()
client_transport = ShmPipeTransport(client_pipe, shm)
server_transport = ShmPipeTransport(server_pipe, shm)
```

Falls back to normal pipe IPC for batches that exceed the segment size.

## API Reference

### PipeTransport

::: vgi_rpc.rpc.PipeTransport

### SubprocessTransport

::: vgi_rpc.rpc.SubprocessTransport

### ShmPipeTransport

::: vgi_rpc.rpc.ShmPipeTransport

### StderrMode

::: vgi_rpc.rpc.StderrMode

### Utility Functions

::: vgi_rpc.rpc.make_pipe_pair

::: vgi_rpc.rpc.serve_stdio

::: vgi_rpc.rpc.serve_unix

::: vgi_rpc.rpc.unix_connect

::: vgi_rpc.rpc.serve_unix_pipe
