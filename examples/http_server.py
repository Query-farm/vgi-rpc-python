"""HTTP server example using Falcon (WSGI) and waitress.

Requires the HTTP extra: ``pip install vgi-rpc[http]``

Start the server::

    python examples/http_server.py

Then run the client in another terminal::

    python examples/http_client.py
"""

from __future__ import annotations

import socket
import sys
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa
import waitress

from vgi_rpc import (
    AnnotatedBatch,
    CallContext,
    Level,
    OutputCollector,
    RpcServer,
    Stream,
    StreamState,
)
from vgi_rpc.http import make_wsgi_app

PORT = 8234


# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------


@dataclass
class FibState(StreamState):
    """Produce Fibonacci numbers up to *limit*."""

    limit: int
    a: int = 0
    b: int = 1

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the next Fibonacci number."""
        if self.a > self.limit:
            out.finish()
            return
        out.emit_pydict({"fib": [self.a]})
        self.a, self.b = self.b, self.a + self.b


class DemoService(Protocol):
    """Demonstration HTTP service."""

    def echo(self, message: str) -> str:
        """Echo a message back."""
        ...

    def fibonacci(self, limit: int) -> Stream[StreamState]:
        """Stream Fibonacci numbers up to *limit*."""
        ...


class DemoServiceImpl:
    """Concrete implementation of DemoService."""

    def echo(self, message: str, ctx: CallContext | None = None) -> str:
        """Echo a message back, with optional server-side logging."""
        if ctx:
            ctx.client_log(Level.INFO, f"Echoing: {message}")
        return message

    def fibonacci(self, limit: int) -> Stream[FibState]:
        """Stream Fibonacci numbers up to *limit*."""
        schema = pa.schema([pa.field("fib", pa.int64())])
        return Stream(output_schema=schema, state=FibState(limit=limit))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def main() -> None:
    """Start the HTTP server."""
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    if port == 0:
        port = _find_free_port()

    server = RpcServer(DemoService, DemoServiceImpl())
    app = make_wsgi_app(server)

    print(f"Serving DemoService on http://127.0.0.1:{port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=port, _quiet=True)


if __name__ == "__main__":
    main()
