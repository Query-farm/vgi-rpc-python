"""Testing a service with ``serve_pipe()`` — no subprocess or network needed.

``serve_pipe`` runs the server on a background thread and gives you a typed
proxy.  This is the fastest way to write unit tests for your RPC service.

Run::

    python examples/testing_pipe.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from vgi_rpc import CallContext, OutputCollector, ProducerState, Stream, StreamState, serve_pipe

# ---------------------------------------------------------------------------
# 1. Define a Protocol and implementation (same as production code)
# ---------------------------------------------------------------------------


class MathService(Protocol):
    """A small service with a unary method and a producer stream."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
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


class MathServiceImpl:
    """Concrete implementation of MathService."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def countdown(self, n: int) -> Stream[CountdownState]:
        """Count down from *n* to 1."""
        return Stream(output_schema=_COUNTDOWN_SCHEMA, state=CountdownState(n=n))


# ---------------------------------------------------------------------------
# 2. Test the service using serve_pipe()
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the testing example."""
    with serve_pipe(MathService, MathServiceImpl()) as svc:
        # --- Unary method ---------------------------------------------------
        result = svc.add(a=2.0, b=3.0)
        assert result == 5.0
        print(f"add(2, 3) = {result}")

        # --- Producer stream ------------------------------------------------
        values: list[int] = [row["value"] for batch in svc.countdown(n=3) for row in batch.batch.to_pylist()]
        assert values == [3, 2, 1]
        print(f"countdown(3) = {values}")

    print("All assertions passed!")


if __name__ == "__main__":
    main()
