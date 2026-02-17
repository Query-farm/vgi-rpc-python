"""HTTP client that connects to the demo HTTP server.

Requires the HTTP extra: ``pip install vgi-rpc[http]``

Start the server first::

    python examples/http_server.py

Then run this client::

    python examples/http_client.py
"""

from __future__ import annotations

from typing import Protocol

from vgi_rpc import Stream, StreamState
from vgi_rpc.http import http_connect

PORT = 8234


class DemoService(Protocol):
    """Demonstration HTTP service.

    This Protocol is duplicated from http_server.py so each file is
    self-contained.  In a real project you'd define the Protocol once in a
    shared module and import it from both sides.
    """

    def echo(self, message: str) -> str:
        """Echo a message back."""
        ...

    def fibonacci(self, limit: int) -> Stream[StreamState]:
        """Stream Fibonacci numbers up to *limit*."""
        ...


def main() -> None:
    """Connect to the HTTP server and make calls."""
    url = f"http://127.0.0.1:{PORT}"

    with http_connect(DemoService, url) as svc:
        # Unary call
        result = svc.echo(message="Hello from HTTP!")
        print(f"echo: {result}")

        # Streaming call
        print("\nFibonacci numbers up to 100:")
        for batch in svc.fibonacci(limit=100):
            for row in batch.batch.to_pylist():
                print(f"  {row['fib']}")


if __name__ == "__main__":
    main()
