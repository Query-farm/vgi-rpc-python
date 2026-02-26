# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""ConformanceService Protocol definition.

Defines ~43 RPC methods covering every framework capability:
scalar echo, void, complex types, optionals, dataclass round-trip,
annotated types, multi-param, errors, logging, producer streams,
exchange streams, headers, and introspection.
"""

from __future__ import annotations

from typing import Annotated, Protocol

import pyarrow as pa

from vgi_rpc.rpc import Stream, StreamState
from vgi_rpc.utils import ArrowType

from ._types import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    Point,
    RichHeader,
    Status,
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

    def exchange_error_on_init(self) -> Stream[StreamState]:
        """Raise during exchange stream initialization."""
        ...

    # ------------------------------------------------------------------
    # Exchange Streams With Headers
    # ------------------------------------------------------------------

    def exchange_with_header(self, factor: float) -> Stream[StreamState, ConformanceHeader]:
        """Exchange stream with a header."""
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

    def exchange_with_rich_header(self, seed: int, factor: float) -> Stream[StreamState, RichHeader]:
        """Exchange stream with a rich multi-type header.

        Args:
            seed: Determines all header field values deterministically.
            factor: Multiplier applied to input values.

        """
        ...
