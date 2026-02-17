"""Streaming examples: producer streams and exchange (bidirectional) streams.

Producer streams generate a sequence of Arrow RecordBatches from the server.
Exchange streams allow the client to send data and receive transformed results
in a lockstep request/response pattern.

Run::

    python examples/streaming.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc

from vgi_rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    ProducerState,
    Stream,
    StreamState,
    serve_pipe,
)

# ---------------------------------------------------------------------------
# Producer stream: server generates batches, client iterates
# ---------------------------------------------------------------------------


@dataclass
class CounterState(ProducerState):
    """State for the counter producer stream.

    Extends ``ProducerState`` so only ``produce(out, ctx)`` needs to be
    implemented — no phantom ``input`` parameter to ignore.
    Call ``out.finish()`` to signal the end of the stream.
    """

    limit: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one batch per call, finish when done."""
        if self.current >= self.limit:
            out.finish()
            return
        out.emit_pydict({"n": [self.current], "n_squared": [self.current**2]})
        self.current += 1


# ---------------------------------------------------------------------------
# Exchange stream: client sends data, server transforms and returns it
# ---------------------------------------------------------------------------


@dataclass
class ScaleState(ExchangeState):
    """State for the scale exchange stream.

    Extends ``ExchangeState`` so only ``exchange(input, out, ctx)`` needs
    to be implemented.  Exchange streams must emit exactly one output batch
    per call and must not call ``out.finish()``.
    """

    factor: float

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Multiply each value by the configured factor."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))
        out.emit_arrays([scaled])


# ---------------------------------------------------------------------------
# Service definition
# ---------------------------------------------------------------------------

_COUNTER_SCHEMA = pa.schema([pa.field("n", pa.int64()), pa.field("n_squared", pa.int64())])
_SCALE_SCHEMA = pa.schema([pa.field("value", pa.float64())])


class MathService(Protocol):
    """Service demonstrating producer and exchange streams."""

    def count(self, limit: int) -> Stream[StreamState]:
        """Produce *limit* batches of (n, n_squared)."""
        ...

    def scale(self, factor: float) -> Stream[StreamState]:
        """Multiply incoming values by *factor*."""
        ...


class MathServiceImpl:
    """Concrete implementation of MathService."""

    def count(self, limit: int) -> Stream[CounterState]:
        """Produce *limit* batches of (n, n_squared)."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=CounterState(limit=limit))

    def scale(self, factor: float) -> Stream[ScaleState]:
        """Multiply incoming values by *factor*."""
        return Stream(
            output_schema=_SCALE_SCHEMA,
            state=ScaleState(factor=factor),
            input_schema=_SCALE_SCHEMA,  # Setting input_schema makes this an exchange stream
        )


# ---------------------------------------------------------------------------
# Client usage
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the streaming examples."""
    with serve_pipe(MathService, MathServiceImpl()) as svc:
        # --- Producer stream: iterate over server-generated batches ----------
        print("=== Producer stream (count to 5) ===")
        for batch in svc.count(limit=5):
            rows = batch.batch.to_pylist()
            for row in rows:
                print(f"  n={row['n']}  n^2={row['n_squared']}")

        # --- Exchange stream: send data, receive transformed results ---------
        print("\n=== Exchange stream (scale by 10) ===")
        with svc.scale(factor=10.0) as stream:
            for values in [[1.0, 2.0, 3.0], [100.0, 200.0]]:
                input_batch = AnnotatedBatch(pa.RecordBatch.from_pydict({"value": values}))
                result = stream.exchange(input_batch)
                print(f"  input={values}  output={result.batch.column('value').to_pylist()}")


if __name__ == "__main__":
    main()
