"""Runtime service introspection with ``enable_describe``.

Demonstrates how to discover methods, their types, and parameter
signatures at runtime using the built-in ``__describe__`` RPC method
and the ``introspect()`` helper.

Run::

    python examples/introspection.py
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import (
    CallContext,
    MethodType,
    OutputCollector,
    ProducerState,
    RpcServer,
    Stream,
    StreamState,
    introspect,
    make_pipe_pair,
)

# ---------------------------------------------------------------------------
# 1. Define a Protocol with a mix of unary and stream methods
# ---------------------------------------------------------------------------


class DemoService(Protocol):
    """A demo service for introspection."""

    def greet(self, name: str) -> str:
        """Greet someone by name."""
        ...

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def count(self, limit: int) -> Stream[StreamState]:
        """Count from 1 up to *limit*."""
        ...


# ---------------------------------------------------------------------------
# 2. Implement the service (methods are only called for schema extraction)
# ---------------------------------------------------------------------------


@dataclass
class CountState(ProducerState):
    """State for the count producer stream."""

    current: int
    limit: int

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one value per tick."""
        if self.current > self.limit:
            out.finish()
            return
        out.emit_pydict({"value": [self.current]})
        self.current += 1


_COUNT_SCHEMA = pa.schema([pa.field("value", pa.int64())])


class DemoServiceImpl:
    """Concrete implementation of DemoService."""

    def greet(self, name: str) -> str:
        """Greet someone by name."""
        return f"Hello, {name}!"

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def count(self, limit: int) -> Stream[CountState]:
        """Count from 1 up to *limit*."""
        return Stream(output_schema=_COUNT_SCHEMA, state=CountState(current=1, limit=limit))


# ---------------------------------------------------------------------------
# 3. Introspect the service at runtime
# ---------------------------------------------------------------------------


def main() -> None:
    """Start a server with introspection enabled and discover its methods."""
    server = RpcServer(DemoService, DemoServiceImpl(), enable_describe=True)
    client_pipe, server_pipe = make_pipe_pair()

    thread = threading.Thread(target=server.serve, args=(server_pipe,), daemon=True)
    thread.start()

    try:
        desc = introspect(client_pipe)

        print(f"Service: {desc.protocol_name}")
        print(f"Methods ({len(desc.methods)}):")
        for name in sorted(desc.methods):
            method = desc.methods[name]
            kind = "unary" if method.method_type == MethodType.UNARY else "stream"
            params = ", ".join(f"{p}: {method.param_types[p]}" for p in method.param_types)
            print(f"  {name} ({kind})")
            print(f"    {params}")
    finally:
        client_pipe.close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
