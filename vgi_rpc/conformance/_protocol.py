# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""ConformanceService Protocol definition.

Defines ~45 RPC methods covering every framework capability:
scalar echo, void, complex types, optionals, dataclass round-trip,
annotated types, multi-param, errors, logging, producer streams,
exchange streams, headers, and introspection.
"""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from typing import Annotated, Protocol

import pyarrow as pa

from vgi_rpc.rpc import Stream, StreamState
from vgi_rpc.utils import ArrowType

from ._types import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    ContainerWideTypes,
    DeepNested,
    EmbeddedArrow,
    Point,
    RichHeader,
    Status,
    WideTypes,
)


class ConformanceService(Protocol):
    """Wire-protocol conformance service exercising all framework capabilities."""

    # ------------------------------------------------------------------
    # Unary: Scalar Echo
    # ------------------------------------------------------------------

    def echo_string(self, value: str) -> str:
        """Echo a string value."""
        ...

    def echo_bytes(self, data: bytes) -> bytes:
        """Echo a bytes value."""
        ...

    def oversized_unary(self, target_bytes: int) -> bytes:
        """Return a bytes payload of approximately ``target_bytes`` bytes.

        Used by HTTP-only conformance tests to deliberately overshoot
        the operator-configured ``max_response_bytes`` (or
        ``max_externalized_response_bytes`` when externalisation is on)
        so the strict-fail behaviour can be verified.  Implementations
        should return a payload sized within ~1% of the requested
        value; the test's only sensitivity is "is this clearly larger
        than the cap?".
        """
        ...

    def echo_int(self, value: int) -> int:
        """Echo an integer value."""
        ...

    def echo_float(self, value: float) -> float:
        """Echo a float value."""
        ...

    def echo_bool(self, value: bool) -> bool:
        """Echo a boolean value."""
        ...

    # ------------------------------------------------------------------
    # Unary: Void Returns
    # ------------------------------------------------------------------

    def void_noop(self) -> None:
        """No-op returning void."""
        ...

    def void_with_param(self, value: int) -> None:
        """Accept a parameter, return void."""
        ...

    # ------------------------------------------------------------------
    # Unary: Complex Type Echo
    # ------------------------------------------------------------------

    def echo_enum(self, status: Status) -> Status:
        """Echo an enum value."""
        ...

    def echo_list(self, values: list[str]) -> list[str]:
        """Echo a list of strings."""
        ...

    def echo_dict(self, mapping: dict[str, int]) -> dict[str, int]:
        """Echo a dict mapping."""
        ...

    def echo_nested_list(self, matrix: list[list[int]]) -> list[list[int]]:
        """Echo a nested list."""
        ...

    # ------------------------------------------------------------------
    # Unary: Optional/Nullable
    # ------------------------------------------------------------------

    def echo_optional_string(self, value: str | None) -> str | None:
        """Echo an optional string (may be None)."""
        ...

    def echo_optional_int(self, value: int | None) -> int | None:
        """Echo an optional int (may be None)."""
        ...

    # ------------------------------------------------------------------
    # Unary: Dataclass Round-trip
    # ------------------------------------------------------------------

    def echo_point(self, point: Point) -> Point:
        """Echo a Point dataclass."""
        ...

    def echo_all_types(self, data: AllTypes) -> AllTypes:
        """Echo an AllTypes dataclass exercising every type mapping."""
        ...

    def echo_bounding_box(self, box: BoundingBox) -> BoundingBox:
        """Echo a BoundingBox with nested Points."""
        ...

    # ------------------------------------------------------------------
    # Unary: Dataclass as Parameter
    # ------------------------------------------------------------------

    def inspect_point(self, point: Point) -> str:
        """Accept a Point param (pa.binary() on wire), return formatted string."""
        ...

    # ------------------------------------------------------------------
    # Unary: Annotated Types
    # ------------------------------------------------------------------

    def echo_int32(self, value: Annotated[int, ArrowType(pa.int32())]) -> Annotated[int, ArrowType(pa.int32())]:
        """Echo an int32 value."""
        ...

    def echo_float32(
        self, value: Annotated[float, ArrowType(pa.float32())]
    ) -> Annotated[float, ArrowType(pa.float32())]:
        """Echo a float32 value."""
        ...

    # ------------------------------------------------------------------
    # Unary: Wide Arrow Types
    # ------------------------------------------------------------------

    def echo_int8(self, value: Annotated[int, ArrowType(pa.int8())]) -> Annotated[int, ArrowType(pa.int8())]:
        """Echo an int8 value."""
        ...

    def echo_int16(self, value: Annotated[int, ArrowType(pa.int16())]) -> Annotated[int, ArrowType(pa.int16())]:
        """Echo an int16 value."""
        ...

    def echo_uint8(self, value: Annotated[int, ArrowType(pa.uint8())]) -> Annotated[int, ArrowType(pa.uint8())]:
        """Echo a uint8 value."""
        ...

    def echo_uint16(self, value: Annotated[int, ArrowType(pa.uint16())]) -> Annotated[int, ArrowType(pa.uint16())]:
        """Echo a uint16 value."""
        ...

    def echo_uint32(self, value: Annotated[int, ArrowType(pa.uint32())]) -> Annotated[int, ArrowType(pa.uint32())]:
        """Echo a uint32 value."""
        ...

    def echo_uint64(self, value: Annotated[int, ArrowType(pa.uint64())]) -> Annotated[int, ArrowType(pa.uint64())]:
        """Echo a uint64 value (exercises values above int64 max)."""
        ...

    def echo_date(
        self, value: Annotated[_dt.date, ArrowType(pa.date32())]
    ) -> Annotated[_dt.date, ArrowType(pa.date32())]:
        """Echo a date32 value."""
        ...

    def echo_timestamp(
        self, value: Annotated[_dt.datetime, ArrowType(pa.timestamp("us"))]
    ) -> Annotated[_dt.datetime, ArrowType(pa.timestamp("us"))]:
        """Echo a naive microsecond timestamp."""
        ...

    def echo_timestamp_utc(
        self, value: Annotated[_dt.datetime, ArrowType(pa.timestamp("us", tz="UTC"))]
    ) -> Annotated[_dt.datetime, ArrowType(pa.timestamp("us", tz="UTC"))]:
        """Echo a UTC-tagged microsecond timestamp."""
        ...

    def echo_time(
        self, value: Annotated[_dt.time, ArrowType(pa.time64("us"))]
    ) -> Annotated[_dt.time, ArrowType(pa.time64("us"))]:
        """Echo a microsecond time-of-day value."""
        ...

    def echo_duration(
        self, value: Annotated[_dt.timedelta, ArrowType(pa.duration("us"))]
    ) -> Annotated[_dt.timedelta, ArrowType(pa.duration("us"))]:
        """Echo a microsecond duration."""
        ...

    def echo_decimal(
        self, value: Annotated[Decimal, ArrowType(pa.decimal128(20, 4))]
    ) -> Annotated[Decimal, ArrowType(pa.decimal128(20, 4))]:
        """Echo a decimal128(20, 4) value."""
        ...

    def echo_large_string(
        self, value: Annotated[str, ArrowType(pa.large_string())]
    ) -> Annotated[str, ArrowType(pa.large_string())]:
        """Echo a large_string value."""
        ...

    def echo_large_binary(
        self, value: Annotated[bytes, ArrowType(pa.large_binary())]
    ) -> Annotated[bytes, ArrowType(pa.large_binary())]:
        """Echo a large_binary value."""
        ...

    def echo_fixed_binary(
        self, value: Annotated[bytes, ArrowType(pa.binary(8))]
    ) -> Annotated[bytes, ArrowType(pa.binary(8))]:
        """Echo a fixed_size_binary(8) value."""
        ...

    def echo_wide_types(self, data: WideTypes) -> WideTypes:
        """Round-trip every Arrow primitive width via a single dataclass."""
        ...

    def echo_container_wide_types(self, data: ContainerWideTypes) -> ContainerWideTypes:
        """Round-trip wide Arrow types nested inside list/dict/optional."""
        ...

    def echo_embedded_arrow(self, data: EmbeddedArrow) -> EmbeddedArrow:
        """Round-trip a ``pa.RecordBatch`` and ``pa.Schema`` carried as nested IPC."""
        ...

    def echo_deep_nested(self, data: DeepNested) -> DeepNested:
        """Round-trip multi-level nested containers and dictionary-encoded strings."""
        ...

    def echo_dict_encoded_string(
        self,
        value: Annotated[str, ArrowType(pa.dictionary(pa.int16(), pa.string()))],
    ) -> Annotated[str, ArrowType(pa.dictionary(pa.int16(), pa.string()))]:
        """Echo a string carried as a dictionary-encoded Arrow column."""
        ...

    # ------------------------------------------------------------------
    # Unary: Multi-Param & Defaults
    # ------------------------------------------------------------------

    def add_floats(self, a: float, b: float) -> float:
        """Add two floats."""
        ...

    def concatenate(self, prefix: str, suffix: str, separator: str = "-") -> str:
        """Concatenate prefix + separator + suffix."""
        ...

    def with_defaults(self, required: int, optional_str: str = "default", optional_int: int = 42) -> str:
        """Return a formatted string showing all param values."""
        ...

    # ------------------------------------------------------------------
    # Unary: Error Propagation
    # ------------------------------------------------------------------

    def raise_value_error(self, message: str) -> str:
        """Raise a ValueError with the given message."""
        ...

    def raise_runtime_error(self, message: str) -> str:
        """Raise a RuntimeError with the given message."""
        ...

    def raise_type_error(self, message: str) -> str:
        """Raise a TypeError with the given message."""
        ...

    # ------------------------------------------------------------------
    # Unary: Client-Directed Logging
    # ------------------------------------------------------------------

    def echo_with_info_log(self, value: str) -> str:
        """Echo value, emitting one INFO log."""
        ...

    def echo_with_multi_logs(self, value: str) -> str:
        """Echo value, emitting DEBUG + INFO + WARN logs."""
        ...

    def echo_with_log_extras(self, value: str) -> str:
        """Echo value, emitting an INFO log with extra key-value pairs."""
        ...

    def echo_with_all_log_levels(self, value: str) -> str:
        """Echo value, emitting one log at each of TRACE/DEBUG/INFO/WARN/ERROR/EXCEPTION."""
        ...

    # ------------------------------------------------------------------
    # Producer Streams
    # ------------------------------------------------------------------

    def produce_n(self, count: int) -> Stream[StreamState]:
        """Produce count batches with {index, value}."""
        ...

    def produce_empty(self) -> Stream[StreamState]:
        """Produce zero batches (finish immediately)."""
        ...

    def produce_single(self) -> Stream[StreamState]:
        """Produce exactly one batch."""
        ...

    def produce_large_batches(self, rows_per_batch: int, batch_count: int) -> Stream[StreamState]:
        """Produce batch_count batches of rows_per_batch rows each."""
        ...

    def produce_with_logs(self, count: int) -> Stream[StreamState]:
        """Produce batches with an INFO log before each."""
        ...

    def produce_error_mid_stream(self, emit_before_error: int) -> Stream[StreamState]:
        """Raise after emitting emit_before_error batches."""
        ...

    def produce_error_on_init(self) -> Stream[StreamState]:
        """Raise during stream initialization."""
        ...

    def produce_oversized_batch(self, rows_per_batch: int) -> Stream[StreamState]:
        """Emit one batch of ``rows_per_batch`` int64 rows, then finish.

        Used by HTTP-only conformance tests to deliberately overshoot the
        operator-configured ``max_response_bytes`` (or
        ``max_externalized_response_bytes`` when externalisation is on)
        for a single producer turn, so the strict-fail behaviour can be
        verified.  The single-batch shape ensures the overshoot happens
        before any continuation-token boundary.
        """
        ...

    # ------------------------------------------------------------------
    # Producer Streams With Headers
    # ------------------------------------------------------------------

    def produce_with_header(self, count: int) -> Stream[StreamState, ConformanceHeader]:
        """Produce batches with a stream header."""
        ...

    def produce_with_header_and_logs(self, count: int) -> Stream[StreamState, ConformanceHeader]:
        """Produce batches with a header and INFO logs."""
        ...

    # ------------------------------------------------------------------
    # Exchange Streams
    # ------------------------------------------------------------------

    def exchange_scale(self, factor: float) -> Stream[StreamState]:
        """Multiply input values by factor."""
        ...

    def exchange_accumulate(self) -> Stream[StreamState]:
        """Accumulate running sum and exchange count across exchanges."""
        ...

    def exchange_with_logs(self) -> Stream[StreamState]:
        """Exchange with INFO + DEBUG logs per exchange."""
        ...

    def exchange_error_on_nth(self, fail_on: int) -> Stream[StreamState]:
        """Raise on the Nth exchange (1-indexed)."""
        ...

    def exchange_zero_columns(self) -> Stream[StreamState]:
        """Exchange stream with zero-column input and output."""
        ...

    def exchange_error_on_init(self) -> Stream[StreamState]:
        """Raise during exchange stream initialization."""
        ...

    def exchange_oversized(self, rows_per_batch: int) -> Stream[StreamState]:
        """Exchange that emits an oversized output batch for any input.

        Companion to :meth:`produce_oversized_batch` for the lockstep
        exchange path.  Each input batch produces a single output batch
        of ``rows_per_batch`` int64 rows — sized to deliberately exceed
        the operator-configured response cap so HTTP strict-fail can be
        verified.
        """
        ...

    # ------------------------------------------------------------------
    # Exchange Streams With Headers
    # ------------------------------------------------------------------

    def exchange_with_header(self, factor: float) -> Stream[StreamState, ConformanceHeader]:
        """Exchange stream with a header."""
        ...

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancellable_producer(self) -> Stream[StreamState]:
        """Produce one batch per tick forever — designed to be cancelled by the client."""
        ...

    def cancellable_exchange(self) -> Stream[StreamState]:
        """Echo each input batch — designed to be cancelled by the client."""
        ...

    def cancel_probe_counters(self) -> list[int]:
        """Return ``[produce_calls, exchange_calls, on_cancel_calls]`` observed on the server."""
        ...

    def reset_cancel_probe(self) -> None:
        """Reset all cancel-probe counters to zero on the server."""
        ...

    # ------------------------------------------------------------------
    # Dynamic Streams With Rich Multi-Type Headers
    # ------------------------------------------------------------------

    def produce_with_rich_header(self, seed: int, count: int) -> Stream[StreamState, RichHeader]:
        """Produce batches with a rich multi-type stream header.

        Args:
            seed: Determines all header field values deterministically.
            count: Number of {index, value} batches to produce.

        """
        ...

    def produce_dynamic_schema(
        self, seed: int, count: int, include_strings: bool, include_floats: bool
    ) -> Stream[StreamState, RichHeader]:
        """Produce batches with a dynamic output schema and rich header.

        The output schema changes based on ``include_strings`` and
        ``include_floats``. Always includes ``index: int64``.

        Args:
            seed: Determines all header field values deterministically.
            count: Number of batches to produce.
            include_strings: Whether to include a ``label: utf8`` column.
            include_floats: Whether to include a ``score: float64`` column.

        """
        ...

    def exchange_cast_compatible(self) -> Stream[StreamState]:
        """Exchange expecting float64 input — tests server-side cast for compatible schemas."""
        ...

    def exchange_with_rich_header(self, seed: int, factor: float) -> Stream[StreamState, RichHeader]:
        """Exchange stream with a rich multi-type header.

        Args:
            seed: Determines all header field values deterministically.
            factor: Multiplier applied to input values.

        """
        ...
