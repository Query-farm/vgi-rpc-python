"""Testing the HTTP transport without a running server.

``make_sync_client`` wraps a Falcon ``TestClient`` so you can exercise the
full HTTP transport stack (authentication, serialization, streaming) in-process
with zero network I/O.

Requires ``pip install vgi-rpc[http]``

Run::

    python examples/testing_http.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import falcon
import pyarrow as pa

from vgi_rpc import (
    AuthContext,
    CallContext,
    OutputCollector,
    ProducerState,
    RpcServer,
    Stream,
    StreamState,
    serve_pipe,
)
from vgi_rpc.http import http_connect, make_sync_client

# ---------------------------------------------------------------------------
# 1. Define a Protocol and implementation
# ---------------------------------------------------------------------------


class DemoService(Protocol):
    """Service used for HTTP testing examples."""

    def greet(self, name: str) -> str:
        """Greet by name."""
        ...

    def whoami(self) -> str:
        """Return the caller's identity."""
        ...

    def countdown(self, n: int) -> Stream[StreamState]:
        """Count down from *n* to 1."""
        ...


@dataclass
class CountdownState(ProducerState):
    """State for the countdown producer stream."""

    n: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one value per tick, finish when done."""
        if self.n <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.n]})
        self.n -= 1


_COUNTDOWN_SCHEMA = pa.schema([pa.field("value", pa.int64())])


class DemoServiceImpl:
    """Concrete implementation of DemoService."""

    def greet(self, name: str) -> str:
        """Greet by name."""
        return f"Hello, {name}!"

    def whoami(self, ctx: CallContext) -> str:
        """Return the caller's identity."""
        return ctx.auth.principal or "anonymous"

    def countdown(self, n: int) -> Stream[CountdownState]:
        """Count down from *n* to 1."""
        return Stream(output_schema=_COUNTDOWN_SCHEMA, state=CountdownState(n=n))


# ---------------------------------------------------------------------------
# 2. Helper: create an authenticate callback
# ---------------------------------------------------------------------------


def _authenticate(req: falcon.Request) -> AuthContext:
    """Authenticate requests using a simple bearer token."""
    token = req.get_header("Authorization") or ""
    if token == "Bearer test-token":
        return AuthContext(domain="bearer", authenticated=True, principal="alice")
    return AuthContext.anonymous()


# ---------------------------------------------------------------------------
# 3. Test with make_sync_client (HTTP transport, no real server)
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the HTTP testing examples."""
    server = RpcServer(DemoService, DemoServiceImpl())

    # --- Basic HTTP testing -------------------------------------------------
    client = make_sync_client(server)
    with http_connect(DemoService, client=client) as svc:
        result = svc.greet(name="World")
        assert result == "Hello, World!"
        print(f"greet('World') = {result}")

        # Anonymous caller
        who = svc.whoami()
        assert who == "anonymous"
        print(f"whoami() = {who}")

        # Producer stream over HTTP
        values: list[int] = [row["value"] for batch in svc.countdown(n=3) for row in batch.batch.to_pylist()]
        assert values == [3, 2, 1]
        print(f"countdown(3) = {values}")

    # --- Authenticated HTTP testing -----------------------------------------
    client = make_sync_client(
        server,
        authenticate=_authenticate,
        default_headers={"Authorization": "Bearer test-token"},
    )
    with http_connect(DemoService, client=client) as svc:
        who = svc.whoami()
        assert who == "alice"
        print(f"whoami() [authenticated] = {who}")

    # --- Pipe transport for comparison --------------------------------------
    with serve_pipe(DemoService, DemoServiceImpl()) as svc:
        result = svc.greet(name="Pipe")
        assert result == "Hello, Pipe!"
        print(f"greet('Pipe') [pipe] = {result}")

    print("All assertions passed!")


if __name__ == "__main__":
    main()
