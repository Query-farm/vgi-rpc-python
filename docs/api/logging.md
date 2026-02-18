# Logging

vgi-rpc has two logging systems: **client-directed logs** (transmitted over the wire to the caller) and **server-side logs** (stdlib `logging` for observability).

## Client-Directed Logging

Server methods emit log messages that travel to the client's `on_log` callback:

```python
from vgi_rpc import CallContext, Level


class MyServiceImpl:
    def process(self, data: str, ctx: CallContext) -> str:
        ctx.client_log(Level.INFO, "Processing started", input_size=str(len(data)))
        result = data.upper()
        ctx.client_log(Level.DEBUG, "Processing complete")
        return result
```

The client receives them via the `on_log` callback:

```python
from vgi_rpc import Message, serve_pipe


def handle_log(msg: Message) -> None:
    print(f"[{msg.level.value}] {msg.message}")


with serve_pipe(MyService, MyServiceImpl(), on_log=handle_log) as proxy:
    proxy.process(data="hello")
    # [INFO] Processing started
    # [DEBUG] Processing complete
```

## Server-Side Logging

Use `ctx.logger` for server-side observability. Every log record automatically includes `server_id`, `method`, `principal`, and `remote_addr`:

```python
class MyServiceImpl:
    def process(self, data: str, ctx: CallContext) -> str:
        ctx.logger.info("Processing request", extra={"input_size": len(data)})
        return data.upper()
```

### Logger hierarchy

| Logger name | Purpose |
|---|---|
| `vgi_rpc` | Root — add handlers here |
| `vgi_rpc.access` | Auto access log (one per call) |
| `vgi_rpc.service.<Protocol>` | Developer `ctx.logger` per service |
| `vgi_rpc.rpc` | Framework lifecycle |
| `vgi_rpc.http` | HTTP transport lifecycle |
| `vgi_rpc.external` | External storage operations |
| `vgi_rpc.subprocess.stderr` | Child process stderr (`StderrMode.PIPE`) |

### Production JSON logging

```python
import logging

from vgi_rpc.logging_utils import VgiJsonFormatter

handler = logging.StreamHandler()
handler.setFormatter(VgiJsonFormatter())
logging.getLogger("vgi_rpc").addHandler(handler)
logging.getLogger("vgi_rpc").setLevel(logging.INFO)
```

## API Reference

### Level

::: vgi_rpc.log.Level

### Message

::: vgi_rpc.log.Message

### VgiJsonFormatter

::: vgi_rpc.logging_utils.VgiJsonFormatter
