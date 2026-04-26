# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Type definitions for the conformance test server.

Enums, dataclasses, and stream state classes used by the conformance
service Protocol and implementation.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from decimal import Decimal
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
class WideTypes(ArrowSerializableDataclass):
    """Exercises Arrow type widths and date/time/decimal mappings.

    Distinct from :class:`AllTypes`, which focuses on Python container
    shapes (list/dict/optional/nested dataclass).  This dataclass focuses
    on the breadth of *Arrow* primitive types reachable from Python via
    ``Annotated[T, ArrowType(pa.X())]``.
    """

    int8_field: Annotated[int, ArrowType(pa.int8())]
    int16_field: Annotated[int, ArrowType(pa.int16())]
    int32_field: Annotated[int, ArrowType(pa.int32())]
    uint8_field: Annotated[int, ArrowType(pa.uint8())]
    uint16_field: Annotated[int, ArrowType(pa.uint16())]
    uint32_field: Annotated[int, ArrowType(pa.uint32())]
    uint64_field: Annotated[int, ArrowType(pa.uint64())]
    float32_field: Annotated[float, ArrowType(pa.float32())]
    date_field: Annotated[_dt.date, ArrowType(pa.date32())]
    timestamp_field: Annotated[_dt.datetime, ArrowType(pa.timestamp("us"))]
    timestamp_utc_field: Annotated[_dt.datetime, ArrowType(pa.timestamp("us", tz="UTC"))]
    time_field: Annotated[_dt.time, ArrowType(pa.time64("us"))]
    duration_field: Annotated[_dt.timedelta, ArrowType(pa.duration("us"))]
    decimal_field: Annotated[Decimal, ArrowType(pa.decimal128(20, 4))]
    large_string_field: Annotated[str, ArrowType(pa.large_string())]
    large_binary_field: Annotated[bytes, ArrowType(pa.large_binary())]
    fixed_binary_field: Annotated[bytes, ArrowType(pa.binary(8))]


@dataclass(frozen=True)
class ContainerWideTypes(ArrowSerializableDataclass):
    """Wide Arrow types nested inside containers (list/dict/optional).

    Verifies that a port's type-inference machinery composes correctly:
    Annotated wide types inside ``list[T]``, ``dict[K, V]`` and
    ``T | None`` positions, plus a list with nullable elements.
    """

    list_decimal: Annotated[list[Decimal], ArrowType(pa.list_(pa.decimal128(20, 4)))]
    list_date: Annotated[list[_dt.date], ArrowType(pa.list_(pa.date32()))]
    list_timestamp: Annotated[list[_dt.datetime], ArrowType(pa.list_(pa.timestamp("us")))]
    optional_date: Annotated[_dt.date | None, ArrowType(pa.date32())]
    optional_decimal: Annotated[Decimal | None, ArrowType(pa.decimal128(20, 4))]
    optional_timestamp: Annotated[_dt.datetime | None, ArrowType(pa.timestamp("us"))]
    dict_str_decimal: Annotated[dict[str, Decimal], ArrowType(pa.map_(pa.string(), pa.decimal128(20, 4)))]
    frozenset_int: frozenset[int]
    list_optional_int: list[int | None]


@dataclass(frozen=True)
class DeepNested(ArrowSerializableDataclass):
    """Deeply-nested and dictionary-encoded Arrow types.

    Verifies multi-level container nesting with non-default element types,
    optional containers, and direct use of dictionary encoding (which the
    framework auto-applies to enums but is exposed here as a primary
    field type for explicit coverage).
    """

    list_of_lists_decimal: Annotated[list[list[Decimal]], ArrowType(pa.list_(pa.list_(pa.decimal128(20, 4))))]
    optional_list_date: Annotated[list[_dt.date] | None, ArrowType(pa.list_(pa.date32()))]
    dict_encoded_string: Annotated[str, ArrowType(pa.dictionary(pa.int16(), pa.string()))]
    list_of_dict_encoded: Annotated[list[str], ArrowType(pa.list_(pa.dictionary(pa.int16(), pa.string())))]


@dataclass(frozen=True)
class EmbeddedArrow(ArrowSerializableDataclass):
    """Arrow ``RecordBatch`` and ``Schema`` carried as nested IPC binary."""

    batch: pa.RecordBatch
    schema: pa.Schema


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
class ZeroColumnExchangeState(ExchangeState):
    """Accept and emit zero-column batches."""

    call_count: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Accept a zero-column batch and emit a zero-column batch."""
        self.call_count += 1
        out.emit(pa.record_batch([], schema=pa.schema([])))


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


# ---------------------------------------------------------------------------
# Cancellation observability
# ---------------------------------------------------------------------------


class _CancelProbe:
    """Process-wide counters observed by cancel conformance tests.

    Module-level so observation works identically across pipe, subprocess,
    and HTTP transports: counters live in the *server* process, and are
    read back over the wire via ``cancel_probe_counters``.

    When ``VGI_RPC_CONFORMANCE_PROBE_FILE`` is set in the environment, the
    counters are stored in that file under an ``fcntl`` lock so multiple
    server processes (e.g. the round-robin two-worker HTTP fixture) share
    a single observable counter set.
    """

    produce_calls: int = 0
    exchange_calls: int = 0
    on_cancel_calls: int = 0

    @classmethod
    def _shared_path(cls) -> str | None:
        import os

        return os.environ.get("VGI_RPC_CONFORMANCE_PROBE_FILE") or None

    @classmethod
    def _bump(cls, field: str) -> None:
        path = cls._shared_path()
        if path is None:
            setattr(cls, field, getattr(cls, field) + 1)
            return
        import fcntl
        import json

        with open(path, "a+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0)
                raw = f.read()
                data = json.loads(raw) if raw else {"produce_calls": 0, "exchange_calls": 0, "on_cancel_calls": 0}
                data[field] = data.get(field, 0) + 1
                f.seek(0)
                f.truncate()
                f.write(json.dumps(data))
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    @classmethod
    def snapshot(cls) -> tuple[int, int, int]:
        """Return ``(produce, exchange, on_cancel)`` from the active store."""
        path = cls._shared_path()
        if path is None:
            return cls.produce_calls, cls.exchange_calls, cls.on_cancel_calls
        import fcntl
        import json
        import os

        if not os.path.exists(path):
            return 0, 0, 0
        with open(path) as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                raw = f.read()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        if not raw:
            return 0, 0, 0
        data = json.loads(raw)
        return data.get("produce_calls", 0), data.get("exchange_calls", 0), data.get("on_cancel_calls", 0)

    @classmethod
    def reset(cls) -> None:
        """Zero all counters."""
        cls.produce_calls = 0
        cls.exchange_calls = 0
        cls.on_cancel_calls = 0
        path = cls._shared_path()
        if path is not None:
            import fcntl
            import json

            with open(path, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    f.write(json.dumps({"produce_calls": 0, "exchange_calls": 0, "on_cancel_calls": 0}))
                    f.flush()
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)


@dataclass
class CancellableProducerState(ProducerState):
    """Infinite producer that records cancel observations in ``_CancelProbe``."""

    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one ``{index, value}`` row per tick without ever finishing."""
        _CancelProbe._bump("produce_calls")
        out.emit_pydict({"index": [self.current], "value": [self.current * 10]})
        self.current += 1

    def on_cancel(self, ctx: CallContext) -> None:
        """Bump the on_cancel counter so tests can observe the hook firing."""
        _CancelProbe._bump("on_cancel_calls")


@dataclass
class CancellableExchangeState(ExchangeState):
    """Echo exchange that records cancel observations in ``_CancelProbe``."""

    _placeholder: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo the input batch unchanged and bump the exchange counter."""
        _CancelProbe._bump("exchange_calls")
        out.emit(input.batch)

    def on_cancel(self, ctx: CallContext) -> None:
        """Bump the on_cancel counter so tests can observe the hook firing."""
        _CancelProbe._bump("on_cancel_calls")


# ---------------------------------------------------------------------------
# Rich multi-type stream header
# ---------------------------------------------------------------------------

_STATUS_CYCLE: list[Status] = [Status.PENDING, Status.ACTIVE, Status.CLOSED]


@dataclass(frozen=True)
class RichHeader(ArrowSerializableDataclass):
    """Multi-type stream header for cross-language conformance testing.

    Every field is computed from a ``seed`` parameter so tests can verify
    the full range of Arrow type mappings in stream headers.
    """

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
    nested_list: list[list[int]]
    annotated_int32: Annotated[int, ArrowType(pa.int32())]
    annotated_float32: Annotated[float, ArrowType(pa.float32())]
    dict_str_str: dict[str, str]


def build_rich_header(seed: int) -> RichHeader:
    """Build a ``RichHeader`` deterministically from *seed*.

    This is the reference specification: other language implementations
    must produce identical field values for the same seed.

    Args:
        seed: Determines all header field values.

    Returns:
        A fully-populated ``RichHeader``.

    """
    return RichHeader(
        str_field=f"seed-{seed}",
        bytes_field=bytes([seed % 256, (seed + 1) % 256, (seed + 2) % 256]),
        int_field=seed * 7,
        float_field=seed * 1.5,
        bool_field=seed % 2 == 0,
        list_of_int=[seed, seed + 1, seed + 2],
        list_of_str=[f"item-{seed}", f"item-{seed + 1}"],
        dict_field={"a": seed, "b": seed + 1},
        enum_field=_STATUS_CYCLE[seed % 3],
        nested_point=Point(x=float(seed), y=float(seed * 2)),
        optional_str=f"opt-{seed}" if seed % 2 == 0 else None,
        optional_int=seed * 3 if seed % 2 == 1 else None,
        optional_nested=Point(x=float(seed), y=0.0) if seed % 3 == 0 else None,
        list_of_nested=[Point(x=float(seed), y=float(seed + 1))],
        nested_list=[[seed, seed + 1], [seed + 2]],
        annotated_int32=seed % 1000,
        annotated_float32=float(seed) / 3.0,
        dict_str_str={"key": f"val-{seed}"},
    )


# ---------------------------------------------------------------------------
# Dynamic schema producer
# ---------------------------------------------------------------------------


def build_dynamic_schema(include_strings: bool, include_floats: bool) -> pa.Schema:
    """Build the output schema for ``produce_dynamic_schema``.

    Args:
        include_strings: Whether to include a ``label: utf8`` column.
        include_floats: Whether to include a ``score: float64`` column.

    Returns:
        An Arrow schema with the requested columns.

    """
    fields: list[pa.Field[pa.DataType]] = [pa.field("index", pa.int64())]
    if include_strings:
        fields.append(pa.field("label", pa.utf8()))
    if include_floats:
        fields.append(pa.field("score", pa.float64()))
    return pa.schema(fields)


@dataclass
class DynamicProducerState(ProducerState):
    """Producer with a schema that varies based on boolean flags."""

    count: int
    include_strings: bool
    include_floats: bool
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch or finish."""
        if self.current >= self.count:
            out.finish()
            return
        data: dict[str, list[Any]] = {"index": [self.current]}
        if self.include_strings:
            data["label"] = [f"row-{self.current}"]
        if self.include_floats:
            data["score"] = [self.current * 1.5]
        out.emit_pydict(data)
        self.current += 1
