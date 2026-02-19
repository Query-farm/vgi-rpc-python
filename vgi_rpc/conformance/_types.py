"""Type definitions for the conformance test server.

Enums, dataclasses, and stream state classes used by the conformance
service Protocol and implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from vgi_rpc.log import Level
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    ProducerState,
)
from vgi_rpc.utils import ArrowSerializableDataclass, ArrowType

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Status(Enum):
    """Status enum for conformance testing."""

    PENDING = "pending"
    ACTIVE = "active"
    CLOSED = "closed"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Point(ArrowSerializableDataclass):
    """A 2D point — exercises struct-in-struct nesting."""

    x: float
    y: float


@dataclass(frozen=True)
class BoundingBox(ArrowSerializableDataclass):
    """Bounding box with nested Point structs."""

    top_left: Point
    bottom_right: Point
    label: str


@dataclass(frozen=True)
class AllTypes(ArrowSerializableDataclass):
    """Exercises every supported Arrow type mapping."""

    str_field: str
    bytes_field: bytes
    int_field: int
    float_field: float
    bool_field: bool
    list_of_int: list[int]
    list_of_str: list[str]
    dict_field: dict[str, int]
    enum_field: Status
    nested_point: Point
    optional_str: str | None
    optional_int: int | None
    optional_nested: Point | None
    list_of_nested: list[Point]
    annotated_int32: Annotated[int, ArrowType(pa.int32())]
    annotated_float32: Annotated[float, ArrowType(pa.float32())]
    nested_list: list[list[int]]
    dict_str_str: dict[str, str]


@dataclass(frozen=True)
class ConformanceHeader(ArrowSerializableDataclass):
    """Stream header for conformance testing."""

    total_expected: int
    description: str


# ---------------------------------------------------------------------------
# Producer stream states
# ---------------------------------------------------------------------------

_COUNTER_SCHEMA = pa.schema([pa.field("index", pa.int64()), pa.field("value", pa.int64())])


@dataclass
class CounterState(ProducerState):
    """Produce ``count`` batches with ``{index, value}``."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch or finish."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"index": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class EmptyProducerState(ProducerState):
    """Finish immediately — zero batches."""

    _placeholder: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Finish immediately."""
        out.finish()


@dataclass
class SingleProducerState(ProducerState):
    """Emit exactly one batch, then finish."""

    emitted: bool = False

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one batch then finish."""
        if self.emitted:
            out.finish()
            return
        self.emitted = True
        out.emit_pydict({"index": [0], "value": [0]})


@dataclass
class LargeProducerState(ProducerState):
    """Produce ``batch_count`` batches of ``rows_per_batch`` rows each."""

    rows_per_batch: int
    batch_count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a multi-row batch or finish."""
        if self.current >= self.batch_count:
            out.finish()
            return
        offset = self.current * self.rows_per_batch
        indices = list(range(offset, offset + self.rows_per_batch))
        out.emit_pydict({"index": indices, "value": [v * 10 for v in indices]})
        self.current += 1


@dataclass
class LoggingProducerState(ProducerState):
    """Produce batches with an INFO log before each."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Log then produce, or finish."""
        if self.current >= self.count:
            out.finish()
            return
        out.client_log(Level.INFO, f"producing batch {self.current}")
        out.emit_pydict({"index": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class ErrorAfterNState(ProducerState):
    """Raise after emitting ``emit_before_error`` batches."""

    emit_before_error: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce or raise."""
        if self.current >= self.emit_before_error:
            raise RuntimeError(f"intentional error after {self.emit_before_error} batches")
        out.emit_pydict({"index": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class HeaderProducerState(ProducerState):
    """Producer state used with stream headers — same as CounterState."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch or finish."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"index": [self.current], "value": [self.current * 10]})
        self.current += 1


# ---------------------------------------------------------------------------
# Exchange stream states
# ---------------------------------------------------------------------------

_SCALE_INPUT_SCHEMA = pa.schema([pa.field("value", pa.float64())])
_SCALE_OUTPUT_SCHEMA = pa.schema([pa.field("value", pa.float64())])

_ACCUM_INPUT_SCHEMA = pa.schema([pa.field("value", pa.float64())])
_accum_fields: list[pa.Field[pa.DataType]] = [
    pa.field("running_sum", pa.float64()),
    pa.field("exchange_count", pa.int64()),
]
_ACCUM_OUTPUT_SCHEMA = pa.schema(_accum_fields)


@dataclass
class ScaleExchangeState(ExchangeState):
    """Multiply ``value`` column by ``factor``."""

    factor: float

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Scale each value."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class AccumulatingExchangeState(ExchangeState):
    """Running sum + exchange count across exchanges."""

    running_sum: float = 0.0
    exchange_count: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Accumulate values."""
        col_sum = cast(float, pc.sum(input.batch.column("value")).as_py())
        self.running_sum += col_sum
        self.exchange_count += 1
        out.emit_pydict({"running_sum": [self.running_sum], "exchange_count": [self.exchange_count]})


@dataclass
class LoggingExchangeState(ExchangeState):
    """INFO + DEBUG log per exchange, then echo input."""

    _placeholder: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Log then echo."""
        out.client_log(Level.INFO, "exchange processing")
        out.client_log(Level.DEBUG, "exchange debug")
        out.emit(input.batch)


@dataclass
class FailOnExchangeNState(ExchangeState):
    """Raise on the Nth exchange (1-indexed)."""

    fail_on: int
    exchange_count: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo or raise."""
        self.exchange_count += 1
        if self.exchange_count >= self.fail_on:
            raise RuntimeError(f"intentional error on exchange {self.exchange_count}")
        out.emit(input.batch)
