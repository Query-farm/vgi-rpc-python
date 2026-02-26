# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""ConformanceServiceImpl — implementation of the conformance Protocol.

Straightforward implementations: echo methods return their input,
error methods raise the named exception, stream methods construct
the appropriate Stream with state objects.
"""

from __future__ import annotations

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
    ConformanceHeader,
    CounterState,
    DynamicProducerState,
    EmptyProducerState,
    ErrorAfterNState,
    FailOnExchangeNState,
    HeaderProducerState,
    LargeProducerState,
    LoggingExchangeState,
    LoggingProducerState,
    Point,
    RichHeader,
    ScaleExchangeState,
    SingleProducerState,
    Status,
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

    def exchange_error_on_nth(self, fail_on: int) -> Stream[FailOnExchangeNState]:
        """Raise on Nth exchange."""
        return Stream(
            output_schema=_SCALE_OUTPUT_SCHEMA,
            state=FailOnExchangeNState(fail_on=fail_on),
            input_schema=_SCALE_INPUT_SCHEMA,
        )

    def exchange_error_on_init(self) -> Stream[ScaleExchangeState]:
        """Raise during exchange init."""
        raise RuntimeError("intentional exchange init error")

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
