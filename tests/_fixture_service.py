# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Test fixture RPC service — Protocol, Impl, and supporting types.

Extracted from ``tests.test_rpc`` so subprocess workers (``serve_fixture_*.py``)
can import the service definitions without dragging in pytest, conftest, and
the rest of the test machinery. That cuts ~70ms off every worker spawn.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc

from vgi_rpc.log import Level
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    ProducerState,
    Stream,
    StreamState,
)
from vgi_rpc.utils import ArrowSerializableDataclass, ArrowType


class Color(Enum):
    """Test enum for type fidelity tests."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


# ---------------------------------------------------------------------------
# State classes for stream implementations
# ---------------------------------------------------------------------------


@dataclass
class GenerateState(ProducerState):
    """State for the generate producer stream."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class GenerateMultiRowState(ProducerState):
    """State for a producer that emits multi-row batches."""

    count: int
    rows_per_batch: int
    offset: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a batch with multiple rows."""
        if self.offset >= self.count:
            out.finish()
            return
        end = min(self.offset + self.rows_per_batch, self.count)
        indices = list(range(self.offset, end))
        out.emit_pydict({"i": indices, "value": [v * 10 for v in indices]})
        self.offset = end


@dataclass
class TransformState(ExchangeState):
    """State for the transform exchange stream."""

    factor: float

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process an input batch."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class FailStreamState(ProducerState):
    """State for a stream that fails after the first batch."""

    emitted: bool = False

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a batch, then fail on the next call."""
        if not self.emitted:
            self.emitted = True
            out.emit_pydict({"x": [1]})
            return
        raise RuntimeError("stream boom")


@dataclass
class FailBidiMidState(ExchangeState):
    """State for a bidi that fails after processing one batch."""

    factor: float
    count: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one batch, then fail on the next."""
        if self.count > 0:
            raise RuntimeError("bidi boom")
        self.count += 1
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class GenerateWithLogsState(ProducerState):
    """State for generate with interleaved log messages."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch with a log message."""
        if self.current >= self.count:
            out.finish()
            return
        out.client_log(Level.INFO, f"generating batch {self.current}")
        out.emit_pydict({"i": [self.current]})
        self.current += 1


@dataclass
class TransformWithLogsState(ExchangeState):
    """State for bidi transform with log messages per exchange."""

    factor: float

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process input with a log message."""
        out.client_log(Level.INFO, f"transforming batch with factor={self.factor}")
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class GenerateWithHeaderState(ProducerState):
    """State for the generate-with-header producer stream."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class TransformWithHeaderState(ExchangeState):
    """State for the transform-with-header exchange stream."""

    factor: float

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process an input batch."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class PassthroughState(StreamState):
    """State for a passthrough exchange stream."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Pass through the input batch unchanged."""
        out.emit(input.batch)


@dataclass
class EmptyStreamState(StreamState):
    """State for a stream that immediately finishes."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Finish immediately."""
        out.finish()


class _CancelProbe:
    """Shared counters observed from in-process pipe tests (thread-local server)."""

    produce_calls: int = 0
    exchange_calls: int = 0
    on_cancel_calls: int = 0

    @classmethod
    def reset(cls) -> None:
        """Reset all counters to zero."""
        cls.produce_calls = 0
        cls.exchange_calls = 0
        cls.on_cancel_calls = 0


@dataclass
class CancellableProducerState(ProducerState):
    """Producer that counts produce() and on_cancel() invocations into _CancelProbe."""

    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce one batch per tick; never self-terminates."""
        _CancelProbe.produce_calls += 1
        out.emit_pydict({"i": [self.current]})
        self.current += 1

    def on_cancel(self, ctx: CallContext) -> None:
        """Record the cancel callback."""
        _CancelProbe.on_cancel_calls += 1


@dataclass
class CancellableExchangeState(ExchangeState):
    """Exchange stream that counts exchange() and on_cancel() invocations."""

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo the input batch back."""
        _CancelProbe.exchange_calls += 1
        out.emit(input.batch)

    def on_cancel(self, ctx: CallContext) -> None:
        """Record the cancel callback."""
        _CancelProbe.on_cancel_calls += 1


# ---------------------------------------------------------------------------
# Test fixtures: Protocol + Implementation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DescribeResult(ArrowSerializableDataclass):
    """Result from describe() — contains a schema and a sample batch."""

    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    sample_batch: Annotated[pa.RecordBatch, ArrowType(pa.binary())]


@dataclass(frozen=True)
class StreamHeader(ArrowSerializableDataclass):
    """Stream header for testing — carries one-time metadata."""

    total_count: int
    label: str


class RpcFixtureService(Protocol):
    """Service with unary, stream, bidi, and error methods."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers.

        Args:
            a: The first number.
            b: The second number.

        """
        ...

    def greet(self, name: str) -> str:
        """Greet by name.

        Args:
            name: The name to greet.

        """
        ...

    def noop(self) -> None:
        """Do nothing."""
        ...

    def generate(self, count: int) -> Stream[ProducerState]:
        """Generate count batches."""
        ...

    def generate_multi(self, count: int, rows_per_batch: int) -> Stream[ProducerState]:
        """Generate batches with multiple rows each."""
        ...

    def transform(self, factor: float) -> Stream[ExchangeState]:
        """Scale values by factor."""
        ...

    def fail_unary(self) -> str:
        """Raise ValueError."""
        ...

    def fail_stream(self) -> Stream[ProducerState]:
        """Stream that fails mid-iteration."""
        ...

    def fail_bidi_mid(self, factor: float) -> Stream[ExchangeState]:
        """Bidi that fails after first batch."""
        ...

    def describe(self) -> DescribeResult:
        """Return a dataclass with schema and sample batch."""
        ...

    def inspect(self, result: DescribeResult) -> str:
        """Accept a dataclass param and return info about it."""
        ...

    def roundtrip_types(self, color: Color, mapping: dict[str, int], tags: frozenset[int]) -> str:
        """Accept Enum, dict, frozenset and return proof of type fidelity."""
        ...

    def echo_color(self, color: Color) -> Color:
        """Return the color back."""
        ...

    def echo_mapping(self, mapping: dict[str, int]) -> dict[str, int]:
        """Return the mapping back."""
        ...

    def echo_tags(self, tags: frozenset[int]) -> frozenset[int]:
        """Return the tags back."""
        ...

    def greet_with_logs(self, name: str) -> str:
        """Greet by name, emitting log messages."""
        ...

    def generate_with_logs(self, count: int) -> Stream[ProducerState]:
        """Generate batches with interleaved log messages."""
        ...

    def transform_with_logs(self, factor: float) -> Stream[ExchangeState]:
        """Bidi transform with log messages per exchange."""
        ...

    def generate_with_header(self, count: int) -> Stream[ProducerState, StreamHeader]:
        """Generate batches with a stream header."""
        ...

    def transform_with_header(self, factor: float) -> Stream[ExchangeState, StreamHeader]:
        """Exchange stream with a stream header."""
        ...

    def fail_stream_init_with_header(self) -> Stream[ProducerState, StreamHeader]:
        """Stream that raises during init (with header declared)."""
        ...

    def generate_with_header_and_log(self, count: int) -> Stream[ProducerState, StreamHeader]:
        """Generate batches with a stream header and init log."""
        ...

    def cancellable_producer(self) -> Stream[ProducerState]:
        """Run a never-terminating producer; exercised by cancel() tests."""
        ...

    def cancellable_exchange(self) -> Stream[ExchangeState]:
        """Run an exchange stream used by cancel() tests."""
        ...


class RpcFixtureServiceImpl:
    """Implementation of RpcFixtureService."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def greet(self, name: str) -> str:
        """Greet by name."""
        return f"Hello, {name}!"

    def noop(self) -> None:
        """Do nothing."""
        return None

    def generate(self, count: int) -> Stream[GenerateState]:
        """Generate count batches."""
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=GenerateState(count=count))

    def generate_multi(self, count: int, rows_per_batch: int) -> Stream[GenerateMultiRowState]:
        """Generate batches with multiple rows each."""
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=GenerateMultiRowState(count=count, rows_per_batch=rows_per_batch))

    def transform(self, factor: float) -> Stream[TransformState]:
        """Scale values by factor."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return Stream(output_schema=schema, state=TransformState(factor=factor), input_schema=schema)

    def fail_unary(self) -> str:
        """Raise ValueError."""
        raise ValueError("unary boom")

    def fail_stream(self) -> Stream[FailStreamState]:
        """Stream that fails mid-iteration."""
        schema = pa.schema([pa.field("x", pa.int64())])
        return Stream(output_schema=schema, state=FailStreamState())

    def fail_bidi_mid(self, factor: float) -> Stream[FailBidiMidState]:
        """Bidi that fails after processing one batch."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return Stream(output_schema=schema, state=FailBidiMidState(factor=factor), input_schema=schema)

    def describe(self) -> DescribeResult:
        """Return a dataclass with schema and sample batch."""
        schema = pa.schema([pa.field("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        return DescribeResult(output_schema=schema, sample_batch=batch)

    def inspect(self, result: DescribeResult) -> str:
        """Accept a dataclass param and return info about it."""
        return f"{len(result.output_schema)}:{result.sample_batch.num_rows}"

    def roundtrip_types(self, color: Color, mapping: dict[str, int], tags: frozenset[int]) -> str:
        """Accept Enum, dict, frozenset and return proof of type fidelity."""
        return f"{color.name}:{isinstance(color, Color)}:{dict(sorted(mapping.items()))}:{sorted(tags)}"

    def echo_color(self, color: Color) -> Color:
        """Return the color back."""
        return color

    def echo_mapping(self, mapping: dict[str, int]) -> dict[str, int]:
        """Return the mapping back."""
        return mapping

    def echo_tags(self, tags: frozenset[int]) -> frozenset[int]:
        """Return the tags back."""
        return tags

    def greet_with_logs(self, name: str, ctx: CallContext | None = None) -> str:
        """Greet by name, emitting INFO + DEBUG logs."""
        if ctx:
            ctx.client_log(Level.INFO, f"greeting {name}")
            ctx.client_log(Level.DEBUG, "debug detail", detail="extra-info")
        return f"Hello, {name}!"

    def generate_with_logs(self, count: int, ctx: CallContext | None = None) -> Stream[GenerateWithLogsState]:
        """Generate batches with interleaved log messages."""
        schema = pa.schema([pa.field("i", pa.int64())])
        if ctx:
            ctx.client_log(Level.INFO, "pre-stream log")
        return Stream(output_schema=schema, state=GenerateWithLogsState(count=count))

    def transform_with_logs(self, factor: float, ctx: CallContext | None = None) -> Stream[TransformWithLogsState]:
        """Bidi transform with log messages per exchange."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return Stream(output_schema=schema, state=TransformWithLogsState(factor=factor), input_schema=schema)

    def generate_with_header(self, count: int) -> Stream[GenerateWithHeaderState, StreamHeader]:
        """Generate batches with a stream header."""
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        header = StreamHeader(total_count=count, label="generate")
        return Stream(output_schema=schema, state=GenerateWithHeaderState(count=count), header=header)

    def transform_with_header(self, factor: float) -> Stream[TransformWithHeaderState, StreamHeader]:
        """Exchange stream with a stream header."""
        schema = pa.schema([pa.field("value", pa.float64())])
        header = StreamHeader(total_count=0, label=f"transform-{factor}")
        return Stream(
            output_schema=schema, state=TransformWithHeaderState(factor=factor), input_schema=schema, header=header
        )

    def fail_stream_init_with_header(self) -> Stream[GenerateWithHeaderState, StreamHeader]:
        """Stream that raises during init (with header declared)."""
        raise ValueError("init boom with header")

    def generate_with_header_and_log(
        self, count: int, ctx: CallContext | None = None
    ) -> Stream[GenerateWithHeaderState, StreamHeader]:
        """Generate batches with a stream header and init log."""
        if ctx:
            ctx.client_log(Level.INFO, "stream init log")
        header = StreamHeader(total_count=count, label="logged")
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=GenerateWithHeaderState(count=count), header=header)

    def cancellable_producer(self) -> Stream[CancellableProducerState]:
        """Run a never-terminating producer; exercised by cancel() tests."""
        schema = pa.schema([pa.field("i", pa.int64())])
        return Stream(output_schema=schema, state=CancellableProducerState())

    def cancellable_exchange(self) -> Stream[CancellableExchangeState]:
        """Run an exchange stream used by cancel() tests."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(
            output_schema=schema,
            state=CancellableExchangeState(),
            input_schema=schema,
        )
