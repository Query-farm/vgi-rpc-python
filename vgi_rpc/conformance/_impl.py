# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""ConformanceServiceImpl — implementation of the conformance Protocol.

Straightforward implementations: echo methods return their input,
error methods raise the named exception, stream methods construct
the appropriate Stream with state objects.
"""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pyarrow as pa

from vgi_rpc.log import Level
from vgi_rpc.rpc import CallContext, Stream

from ._types import (
    _ACCUM_INPUT_SCHEMA,
    _ACCUM_OUTPUT_SCHEMA,
    _COUNTER_SCHEMA,
    _SCALE_INPUT_SCHEMA,
    _SCALE_OUTPUT_SCHEMA,
    AccumulatingExchangeState,
    AllTypes,
    BoundingBox,
    CancellableExchangeState,
    CancellableProducerState,
    ConformanceHeader,
    ContainerWideTypes,
    CounterState,
    DeepNested,
    DynamicProducerState,
    EmbeddedArrow,
    EmptyProducerState,
    ErrorAfterNState,
    FailOnExchangeNState,
    HeaderProducerState,
    LargeProducerState,
    LoggingExchangeState,
    LoggingProducerState,
    OversizedBatchState,
    OversizedExchangeState,
    Point,
    RichHeader,
    ScaleExchangeState,
    SingleProducerState,
    Status,
    WideTypes,
    ZeroColumnExchangeState,
    _CancelProbe,
    build_dynamic_schema,
    build_rich_header,
)

# Reusable schemas
_LOGGING_EXCHANGE_INPUT = pa.schema([pa.field("value", pa.float64())])
_LOGGING_EXCHANGE_OUTPUT = pa.schema([pa.field("value", pa.float64())])


class ConformanceServiceImpl:
    """Implementation of ConformanceService."""

    # ------------------------------------------------------------------
    # Unary: Scalar Echo
    # ------------------------------------------------------------------

    def echo_string(self, value: str) -> str:
        """Echo a string value."""
        return value

    def echo_bytes(self, data: bytes) -> bytes:
        """Echo a bytes value."""
        return data

    def oversized_unary(self, target_bytes: int) -> bytes:
        """Return a bytes payload of approximately ``target_bytes`` bytes."""
        # ``b"\x00"`` is the cheapest predictable filler; the test only
        # cares that the result is large.
        if target_bytes < 0:
            raise ValueError("target_bytes must be non-negative")
        return b"\x00" * target_bytes

    def echo_int(self, value: int) -> int:
        """Echo an integer value."""
        return value

    def echo_float(self, value: float) -> float:
        """Echo a float value."""
        return value

    def echo_bool(self, value: bool) -> bool:
        """Echo a boolean value."""
        return value

    # ------------------------------------------------------------------
    # Unary: Void Returns
    # ------------------------------------------------------------------

    def void_noop(self) -> None:
        """No-op returning void."""

    def void_with_param(self, value: int) -> None:
        """Accept a parameter, return void."""

    # ------------------------------------------------------------------
    # Unary: Complex Type Echo
    # ------------------------------------------------------------------

    def echo_enum(self, status: Status) -> Status:
        """Echo an enum value."""
        return status

    def echo_list(self, values: list[str]) -> list[str]:
        """Echo a list of strings."""
        return values

    def echo_dict(self, mapping: dict[str, int]) -> dict[str, int]:
        """Echo a dict mapping."""
        return mapping

    def echo_nested_list(self, matrix: list[list[int]]) -> list[list[int]]:
        """Echo a nested list."""
        return matrix

    # ------------------------------------------------------------------
    # Unary: Optional/Nullable
    # ------------------------------------------------------------------

    def echo_optional_string(self, value: str | None) -> str | None:
        """Echo an optional string."""
        return value

    def echo_optional_int(self, value: int | None) -> int | None:
        """Echo an optional int."""
        return value

    # ------------------------------------------------------------------
    # Unary: Dataclass Round-trip
    # ------------------------------------------------------------------

    def echo_point(self, point: Point) -> Point:
        """Echo a Point dataclass."""
        return point

    def echo_all_types(self, data: AllTypes) -> AllTypes:
        """Echo an AllTypes dataclass."""
        return data

    def echo_bounding_box(self, box: BoundingBox) -> BoundingBox:
        """Echo a BoundingBox."""
        return box

    # ------------------------------------------------------------------
    # Unary: Dataclass as Parameter
    # ------------------------------------------------------------------

    def inspect_point(self, point: Point) -> str:
        """Accept a Point, return formatted string."""
        return f"Point({point.x}, {point.y})"

    # ------------------------------------------------------------------
    # Unary: Annotated Types
    # ------------------------------------------------------------------

    def echo_int32(self, value: int) -> int:
        """Echo an int32 value."""
        return value

    def echo_float32(self, value: float) -> float:
        """Echo a float32 value."""
        return value

    # ------------------------------------------------------------------
    # Unary: Wide Arrow Types
    # ------------------------------------------------------------------

    def echo_int8(self, value: int) -> int:
        """Echo an int8 value."""
        return value

    def echo_int16(self, value: int) -> int:
        """Echo an int16 value."""
        return value

    def echo_uint8(self, value: int) -> int:
        """Echo a uint8 value."""
        return value

    def echo_uint16(self, value: int) -> int:
        """Echo a uint16 value."""
        return value

    def echo_uint32(self, value: int) -> int:
        """Echo a uint32 value."""
        return value

    def echo_uint64(self, value: int) -> int:
        """Echo a uint64 value."""
        return value

    def echo_date(self, value: _dt.date) -> _dt.date:
        """Echo a date value."""
        return value

    def echo_timestamp(self, value: _dt.datetime) -> _dt.datetime:
        """Echo a naive timestamp."""
        return value

    def echo_timestamp_utc(self, value: _dt.datetime) -> _dt.datetime:
        """Echo a UTC timestamp."""
        return value

    def echo_time(self, value: _dt.time) -> _dt.time:
        """Echo a time-of-day value."""
        return value

    def echo_duration(self, value: _dt.timedelta) -> _dt.timedelta:
        """Echo a duration."""
        return value

    def echo_decimal(self, value: Decimal) -> Decimal:
        """Echo a decimal value."""
        return value

    def echo_large_string(self, value: str) -> str:
        """Echo a large_string value."""
        return value

    def echo_large_binary(self, value: bytes) -> bytes:
        """Echo a large_binary value."""
        return value

    def echo_fixed_binary(self, value: bytes) -> bytes:
        """Echo a fixed_size_binary(8) value."""
        return value

    def echo_wide_types(self, data: WideTypes) -> WideTypes:
        """Echo a WideTypes dataclass."""
        return data

    def echo_container_wide_types(self, data: ContainerWideTypes) -> ContainerWideTypes:
        """Echo a ContainerWideTypes dataclass."""
        return data

    def echo_embedded_arrow(self, data: EmbeddedArrow) -> EmbeddedArrow:
        """Echo an EmbeddedArrow dataclass."""
        return data

    def echo_deep_nested(self, data: DeepNested) -> DeepNested:
        """Echo a DeepNested dataclass."""
        return data

    def echo_dict_encoded_string(self, value: str) -> str:
        """Echo a dictionary-encoded string."""
        return value

    # ------------------------------------------------------------------
    # Unary: Multi-Param & Defaults
    # ------------------------------------------------------------------

    def add_floats(self, a: float, b: float) -> float:
        """Add two floats."""
        return a + b

    def concatenate(self, prefix: str, suffix: str, separator: str = "-") -> str:
        """Concatenate prefix + separator + suffix."""
        return f"{prefix}{separator}{suffix}"

    def with_defaults(self, required: int, optional_str: str = "default", optional_int: int = 42) -> str:
        """Return formatted string showing all param values."""
        return f"required={required}, optional_str={optional_str}, optional_int={optional_int}"

    # ------------------------------------------------------------------
    # Unary: Error Propagation
    # ------------------------------------------------------------------

    def raise_value_error(self, message: str) -> str:
        """Raise a ValueError."""
        raise ValueError(message)

    def raise_runtime_error(self, message: str) -> str:
        """Raise a RuntimeError."""
        raise RuntimeError(message)

    def raise_type_error(self, message: str) -> str:
        """Raise a TypeError."""
        raise TypeError(message)

    # ------------------------------------------------------------------
    # Unary: Client-Directed Logging
    # ------------------------------------------------------------------

    def echo_with_info_log(self, value: str, ctx: CallContext | None = None) -> str:
        """Echo value, emitting one INFO log."""
        if ctx:
            ctx.client_log(Level.INFO, f"info: {value}")
        return value

    def echo_with_multi_logs(self, value: str, ctx: CallContext | None = None) -> str:
        """Echo value, emitting DEBUG + INFO + WARN logs."""
        if ctx:
            ctx.client_log(Level.DEBUG, f"debug: {value}")
            ctx.client_log(Level.INFO, f"info: {value}")
            ctx.client_log(Level.WARN, f"warn: {value}")
        return value

    def echo_with_log_extras(self, value: str, ctx: CallContext | None = None) -> str:
        """Echo value, emitting an INFO log with extras."""
        if ctx:
            ctx.client_log(Level.INFO, f"info: {value}", source="conformance", detail=value)
        return value

    def echo_with_all_log_levels(self, value: str, ctx: CallContext | None = None) -> str:
        """Emit one log at each non-EXCEPTION Level so clients can verify level round-trip.

        ``Level.EXCEPTION`` is reserved on the wire — the framework raises
        ``RpcError`` rather than delivering it as an in-band log — so it is
        excluded here.
        """
        if ctx:
            for level in (Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR):
                ctx.client_log(level, f"{level.value.lower()}: {value}")
        return value

    # ------------------------------------------------------------------
    # Producer Streams
    # ------------------------------------------------------------------

    def produce_n(self, count: int) -> Stream[CounterState]:
        """Produce count batches."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=CounterState(count=count))

    def produce_empty(self) -> Stream[EmptyProducerState]:
        """Produce zero batches."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=EmptyProducerState())

    def produce_single(self) -> Stream[SingleProducerState]:
        """Produce exactly one batch."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=SingleProducerState())

    def produce_large_batches(self, rows_per_batch: int, batch_count: int) -> Stream[LargeProducerState]:
        """Produce large batches."""
        return Stream(
            output_schema=_COUNTER_SCHEMA,
            state=LargeProducerState(rows_per_batch=rows_per_batch, batch_count=batch_count),
        )

    def produce_with_logs(self, count: int) -> Stream[LoggingProducerState]:
        """Produce batches with logs."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=LoggingProducerState(count=count))

    def produce_error_mid_stream(self, emit_before_error: int) -> Stream[ErrorAfterNState]:
        """Raise after N batches."""
        return Stream(
            output_schema=_COUNTER_SCHEMA,
            state=ErrorAfterNState(emit_before_error=emit_before_error),
        )

    def produce_error_on_init(self) -> Stream[CounterState]:
        """Raise during init."""
        raise RuntimeError("intentional init error")

    def produce_oversized_batch(self, rows_per_batch: int) -> Stream[OversizedBatchState]:
        """Emit one oversized batch then finish."""
        return Stream(
            output_schema=_COUNTER_SCHEMA,
            state=OversizedBatchState(rows_per_batch=rows_per_batch),
        )

    # ------------------------------------------------------------------
    # Producer Streams With Headers
    # ------------------------------------------------------------------

    def produce_with_header(self, count: int) -> Stream[HeaderProducerState, ConformanceHeader]:
        """Produce batches with a header."""
        header = ConformanceHeader(total_expected=count, description=f"producing {count} batches")
        return Stream(output_schema=_COUNTER_SCHEMA, state=HeaderProducerState(count=count), header=header)

    def produce_with_header_and_logs(
        self, count: int, ctx: CallContext | None = None
    ) -> Stream[HeaderProducerState, ConformanceHeader]:
        """Produce batches with header and logs."""
        if ctx:
            ctx.client_log(Level.INFO, "stream init log")
        header = ConformanceHeader(total_expected=count, description=f"producing {count} with logs")
        return Stream(output_schema=_COUNTER_SCHEMA, state=HeaderProducerState(count=count), header=header)

    # ------------------------------------------------------------------
    # Exchange Streams
    # ------------------------------------------------------------------

    def exchange_scale(self, factor: float) -> Stream[ScaleExchangeState]:
        """Multiply input values by factor."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=ScaleExchangeState(factor=factor),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    def exchange_accumulate(self) -> Stream[AccumulatingExchangeState]:
        """Accumulate running sum and exchange count."""
        return Stream(
            output_schema=_ACCUM_OUTPUT_SCHEMA,
            state=AccumulatingExchangeState(),
            input_schema=_ACCUM_INPUT_SCHEMA,
        )

    def exchange_with_logs(self) -> Stream[LoggingExchangeState]:
        """Exchange with logs."""
        return Stream(
            output_schema=_LOGGING_EXCHANGE_OUTPUT,
            state=LoggingExchangeState(),
            input_schema=_LOGGING_EXCHANGE_INPUT,
        )

    def exchange_zero_columns(self) -> Stream[ZeroColumnExchangeState]:
        """Exchange stream with zero-column input and output."""
        empty = pa.schema([])
        return Stream(output_schema=empty, state=ZeroColumnExchangeState(), input_schema=empty)

    def exchange_error_on_nth(self, fail_on: int) -> Stream[FailOnExchangeNState]:
        """Raise on Nth exchange."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=FailOnExchangeNState(fail_on=fail_on),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    def exchange_cast_compatible(self) -> Stream[ScaleExchangeState]:
        """Exchange expecting float64 input — echoes values via factor=1.0."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=ScaleExchangeState(factor=1.0),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    def exchange_error_on_init(self) -> Stream[ScaleExchangeState]:
        """Raise during exchange init."""
        raise RuntimeError("intentional exchange init error")

    def exchange_oversized(self, rows_per_batch: int) -> Stream[OversizedExchangeState]:
        """Exchange that emits a large output for any input."""
        return Stream(
            output_schema=_COUNTER_SCHEMA,
            state=OversizedExchangeState(rows_per_batch=rows_per_batch),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    # ------------------------------------------------------------------
    # Exchange Streams With Headers
    # ------------------------------------------------------------------

    def exchange_with_header(self, factor: float) -> Stream[ScaleExchangeState, ConformanceHeader]:
        """Exchange with header."""
        header = ConformanceHeader(total_expected=0, description=f"scale by {factor}")
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=ScaleExchangeState(factor=factor),
            input_schema=_SCALE_INPUT_SCHEMA,
            header=header,
        )

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancellable_producer(self) -> Stream[CancellableProducerState]:
        """Build an infinite producer used by cancel() conformance tests."""
        return Stream(output_schema=_COUNTER_SCHEMA, state=CancellableProducerState())

    def cancellable_exchange(self) -> Stream[CancellableExchangeState]:
        """Build an echo exchange used by cancel() conformance tests."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=CancellableExchangeState(),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    def cancel_probe_counters(self) -> list[int]:
        """Return the current ``[produce_calls, exchange_calls, on_cancel_calls]`` counters."""
        produce, exchange, on_cancel = _CancelProbe.snapshot()
        return [produce, exchange, on_cancel]

    def reset_cancel_probe(self) -> None:
        """Zero all cancel-probe counters."""
        _CancelProbe.reset()

    # ------------------------------------------------------------------
    # Dynamic Streams With Rich Multi-Type Headers
    # ------------------------------------------------------------------

    def produce_with_rich_header(self, seed: int, count: int) -> Stream[HeaderProducerState, RichHeader]:
        """Produce batches with a rich multi-type header."""
        return Stream(
            output_schema=_COUNTER_SCHEMA,
            state=HeaderProducerState(count=count),
            header=build_rich_header(seed),
        )

    def produce_dynamic_schema(
        self, seed: int, count: int, include_strings: bool, include_floats: bool
    ) -> Stream[DynamicProducerState, RichHeader]:
        """Produce batches with dynamic output schema and rich header."""
        return Stream(
            output_schema=build_dynamic_schema(include_strings, include_floats),
            state=DynamicProducerState(
                count=count,
                include_strings=include_strings,
                include_floats=include_floats,
            ),
            header=build_rich_header(seed),
        )

    def exchange_with_rich_header(self, seed: int, factor: float) -> Stream[ScaleExchangeState, RichHeader]:
        """Exchange stream with a rich multi-type header."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=ScaleExchangeState(factor=factor),
            input_schema=_SCALE_INPUT_SCHEMA,
            header=build_rich_header(seed),
        )
