# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance test suite — reference behavior specification for vgi-rpc.

Tests all framework capabilities through the conformance server across
pipe, subprocess, and HTTP transports.
"""

from __future__ import annotations

import math
import os
import threading

import pyarrow as pa
import pytest

from vgi_rpc.conformance import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    ConformanceService,
    ConformanceServiceImpl,
    Point,
    RichHeader,
    Status,
    build_dynamic_schema,
    build_rich_header,
    run_describe_conformance,
)
from vgi_rpc.introspect import ServiceDescription, introspect
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import AnnotatedBatch, MethodType, RpcError, RpcServer, make_pipe_pair

from .conftest import ConnFactory


def _is_subprocess(request: pytest.FixtureRequest) -> bool:
    """Check if current parametrized transport is subprocess."""
    return str(request.node.callspec.params.get("conformance_conn")) == "subprocess"


def _is_http(request: pytest.FixtureRequest) -> bool:
    """Check if current parametrized transport is http."""
    return str(request.node.callspec.params.get("conformance_conn")) == "http"


# ---------------------------------------------------------------------------
# Unary: Scalar Echo
# ---------------------------------------------------------------------------


class TestUnaryScalarEcho:
    """Test basic scalar echo methods."""

    def test_echo_string(self, conformance_conn: ConnFactory) -> None:
        """Echo a simple string."""
        with conformance_conn() as proxy:
            assert proxy.echo_string(value="hello") == "hello"

    def test_echo_bytes(self, conformance_conn: ConnFactory) -> None:
        """Echo simple bytes."""
        with conformance_conn() as proxy:
            assert proxy.echo_bytes(data=b"hello") == b"hello"

    def test_echo_int(self, conformance_conn: ConnFactory) -> None:
        """Echo an integer."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=42) == 42

    def test_echo_float(self, conformance_conn: ConnFactory) -> None:
        """Echo a float."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=3.14) == pytest.approx(3.14)

    def test_echo_bool(self, conformance_conn: ConnFactory) -> None:
        """Echo booleans."""
        with conformance_conn() as proxy:
            assert proxy.echo_bool(value=True) is True
            assert proxy.echo_bool(value=False) is False


# ---------------------------------------------------------------------------
# Unary: Void Returns
# ---------------------------------------------------------------------------


class TestUnaryVoid:
    """Test void return methods."""

    def test_void_noop(self, conformance_conn: ConnFactory) -> None:
        """Call no-op, verify no error."""
        with conformance_conn() as proxy:
            result = proxy.void_noop()
            assert result is None

    def test_void_with_param(self, conformance_conn: ConnFactory) -> None:
        """Call void method with parameter."""
        with conformance_conn() as proxy:
            result = proxy.void_with_param(value=99)
            assert result is None


# ---------------------------------------------------------------------------
# Unary: Complex Type Echo
# ---------------------------------------------------------------------------


class TestUnaryComplexTypes:
    """Test complex type echo methods."""

    def test_echo_enum_pending(self, conformance_conn: ConnFactory) -> None:
        """Echo enum PENDING."""
        with conformance_conn() as proxy:
            assert proxy.echo_enum(status=Status.PENDING) == Status.PENDING

    def test_echo_enum_active(self, conformance_conn: ConnFactory) -> None:
        """Echo enum ACTIVE."""
        with conformance_conn() as proxy:
            assert proxy.echo_enum(status=Status.ACTIVE) == Status.ACTIVE

    def test_echo_enum_closed(self, conformance_conn: ConnFactory) -> None:
        """Echo enum CLOSED."""
        with conformance_conn() as proxy:
            assert proxy.echo_enum(status=Status.CLOSED) == Status.CLOSED

    def test_echo_list(self, conformance_conn: ConnFactory) -> None:
        """Echo a list of strings."""
        with conformance_conn() as proxy:
            assert proxy.echo_list(values=["a", "b", "c"]) == ["a", "b", "c"]

    def test_echo_dict(self, conformance_conn: ConnFactory) -> None:
        """Echo a dict, verify key ordering preserved."""
        with conformance_conn() as proxy:
            mapping = {"z": 1, "a": 2, "m": 3}
            result = proxy.echo_dict(mapping=mapping)
            assert result == mapping

    def test_echo_nested_list(self, conformance_conn: ConnFactory) -> None:
        """Echo a nested list."""
        with conformance_conn() as proxy:
            matrix = [[1, 2], [3, 4, 5], [6]]
            assert proxy.echo_nested_list(matrix=matrix) == matrix


# ---------------------------------------------------------------------------
# Unary: Optional/Nullable
# ---------------------------------------------------------------------------


class TestUnaryOptional:
    """Test optional/nullable echo methods."""

    def test_optional_string_none(self, conformance_conn: ConnFactory) -> None:
        """Echo None for optional string."""
        with conformance_conn() as proxy:
            assert proxy.echo_optional_string(value=None) is None

    def test_optional_string_non_none(self, conformance_conn: ConnFactory) -> None:
        """Echo a non-None optional string."""
        with conformance_conn() as proxy:
            assert proxy.echo_optional_string(value="hello") == "hello"

    def test_optional_int_none(self, conformance_conn: ConnFactory) -> None:
        """Echo None for optional int."""
        with conformance_conn() as proxy:
            assert proxy.echo_optional_int(value=None) is None

    def test_optional_int_non_none(self, conformance_conn: ConnFactory) -> None:
        """Echo a non-None optional int."""
        with conformance_conn() as proxy:
            assert proxy.echo_optional_int(value=7) == 7

    def test_empty_string_vs_null(self, conformance_conn: ConnFactory) -> None:
        """Verify empty string is distinct from None."""
        with conformance_conn() as proxy:
            assert proxy.echo_optional_string(value="") == ""
            assert proxy.echo_optional_string(value=None) is None


# ---------------------------------------------------------------------------
# Unary: Dataclass Round-trip
# ---------------------------------------------------------------------------


class TestUnaryDataclass:
    """Test dataclass round-trip echo methods."""

    def test_echo_point(self, conformance_conn: ConnFactory) -> None:
        """Echo a Point dataclass."""
        with conformance_conn() as proxy:
            p = Point(x=1.5, y=2.5)
            result = proxy.echo_point(point=p)
            assert isinstance(result, Point)
            assert result.x == pytest.approx(1.5)
            assert result.y == pytest.approx(2.5)

    def test_echo_bounding_box(self, conformance_conn: ConnFactory) -> None:
        """Echo a BoundingBox with nested Points."""
        with conformance_conn() as proxy:
            box = BoundingBox(top_left=Point(x=0.0, y=10.0), bottom_right=Point(x=10.0, y=0.0), label="test")
            result = proxy.echo_bounding_box(box=box)
            assert isinstance(result, BoundingBox)
            assert result.top_left.x == pytest.approx(0.0)
            assert result.top_left.y == pytest.approx(10.0)
            assert result.bottom_right.x == pytest.approx(10.0)
            assert result.label == "test"

    def test_echo_all_types(self, conformance_conn: ConnFactory) -> None:
        """Echo AllTypes — exercises every supported type mapping."""
        with conformance_conn() as proxy:
            data = AllTypes(
                str_field="hello",
                bytes_field=b"\x01\x02\x03",
                int_field=42,
                float_field=3.14,
                bool_field=True,
                list_of_int=[1, 2, 3],
                list_of_str=["a", "b"],
                dict_field={"k": 1},
                enum_field=Status.ACTIVE,
                nested_point=Point(x=1.0, y=2.0),
                optional_str="present",
                optional_int=7,
                optional_nested=Point(x=3.0, y=4.0),
                list_of_nested=[Point(x=5.0, y=6.0)],
                annotated_int32=100,
                annotated_float32=1.5,
                nested_list=[[1, 2], [3]],
                dict_str_str={"key": "val"},
            )
            result = proxy.echo_all_types(data=data)
            assert isinstance(result, AllTypes)
            assert result.str_field == "hello"
            assert result.bytes_field == b"\x01\x02\x03"
            assert result.int_field == 42
            assert result.float_field == pytest.approx(3.14)
            assert result.bool_field is True
            assert result.list_of_int == [1, 2, 3]
            assert result.list_of_str == ["a", "b"]
            assert result.dict_field == {"k": 1}
            assert result.enum_field == Status.ACTIVE
            assert result.nested_point.x == pytest.approx(1.0)
            assert result.optional_str == "present"
            assert result.optional_int == 7
            assert result.optional_nested is not None
            assert result.optional_nested.x == pytest.approx(3.0)
            assert len(result.list_of_nested) == 1
            assert result.annotated_int32 == 100
            assert result.annotated_float32 == pytest.approx(1.5)
            assert result.nested_list == [[1, 2], [3]]
            assert result.dict_str_str == {"key": "val"}

    def test_echo_all_types_with_nulls(self, conformance_conn: ConnFactory) -> None:
        """Echo AllTypes with optional fields set to None."""
        with conformance_conn() as proxy:
            data = AllTypes(
                str_field="test",
                bytes_field=b"",
                int_field=0,
                float_field=0.0,
                bool_field=False,
                list_of_int=[],
                list_of_str=[],
                dict_field={},
                enum_field=Status.PENDING,
                nested_point=Point(x=0.0, y=0.0),
                optional_str=None,
                optional_int=None,
                optional_nested=None,
                list_of_nested=[],
                annotated_int32=0,
                annotated_float32=0.0,
                nested_list=[],
                dict_str_str={},
            )
            result = proxy.echo_all_types(data=data)
            assert result.optional_str is None
            assert result.optional_int is None
            assert result.optional_nested is None

    def test_inspect_point(self, conformance_conn: ConnFactory) -> None:
        """Inspect a Point — exercises pa.binary() deserialization path."""
        with conformance_conn() as proxy:
            result = proxy.inspect_point(point=Point(x=1.5, y=2.5))
            assert result == "Point(1.5, 2.5)"


# ---------------------------------------------------------------------------
# Unary: Annotated Types
# ---------------------------------------------------------------------------


class TestUnaryAnnotated:
    """Test annotated type echo methods."""

    def test_echo_int32(self, conformance_conn: ConnFactory) -> None:
        """Echo int32 value."""
        with conformance_conn() as proxy:
            assert proxy.echo_int32(value=42) == 42

    def test_echo_float32(self, conformance_conn: ConnFactory) -> None:
        """Echo float32 value."""
        with conformance_conn() as proxy:
            assert proxy.echo_float32(value=1.5) == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# Unary: Multi-Param & Defaults
# ---------------------------------------------------------------------------


class TestUnaryMultiParam:
    """Test multi-parameter and default value methods."""

    def test_add_floats(self, conformance_conn: ConnFactory) -> None:
        """Add two floats."""
        with conformance_conn() as proxy:
            assert proxy.add_floats(a=1.5, b=2.5) == pytest.approx(4.0)

    def test_concatenate_with_default(self, conformance_conn: ConnFactory) -> None:
        """Concatenate with default separator."""
        with conformance_conn() as proxy:
            assert proxy.concatenate(prefix="hello", suffix="world") == "hello-world"

    def test_concatenate_custom_separator(self, conformance_conn: ConnFactory) -> None:
        """Concatenate with custom separator."""
        with conformance_conn() as proxy:
            result = proxy.concatenate(prefix="hello", suffix="world", separator="_")
            assert result == "hello_world"

    def test_with_defaults_all_default(self, conformance_conn: ConnFactory) -> None:
        """Call with only required param."""
        with conformance_conn() as proxy:
            result = proxy.with_defaults(required=1)
            assert result == "required=1, optional_str=default, optional_int=42"

    def test_with_defaults_override_all(self, conformance_conn: ConnFactory) -> None:
        """Call overriding all defaults."""
        with conformance_conn() as proxy:
            result = proxy.with_defaults(required=2, optional_str="custom", optional_int=99)
            assert result == "required=2, optional_str=custom, optional_int=99"


# ---------------------------------------------------------------------------
# Unary: Error Propagation
# ---------------------------------------------------------------------------


class TestUnaryErrors:
    """Test error propagation through RPC."""

    def test_raise_value_error(self, conformance_conn: ConnFactory) -> None:
        """Verify ValueError propagation."""
        with conformance_conn() as proxy, pytest.raises(RpcError, match="test error") as exc_info:
            proxy.raise_value_error(message="test error")
        assert exc_info.value.error_type == "ValueError"
        assert "test error" in exc_info.value.error_message
        assert exc_info.value.remote_traceback is not None

    def test_raise_runtime_error(self, conformance_conn: ConnFactory) -> None:
        """Verify RuntimeError propagation."""
        with conformance_conn() as proxy, pytest.raises(RpcError, match="runtime error") as exc_info:
            proxy.raise_runtime_error(message="runtime error")
        assert exc_info.value.error_type == "RuntimeError"

    def test_raise_type_error(self, conformance_conn: ConnFactory) -> None:
        """Verify TypeError propagation."""
        with conformance_conn() as proxy, pytest.raises(RpcError, match="type error") as exc_info:
            proxy.raise_type_error(message="type error")
        assert exc_info.value.error_type == "TypeError"


# ---------------------------------------------------------------------------
# Unary: Client-Directed Logging
# ---------------------------------------------------------------------------


class TestUnaryLogging:
    """Test client-directed logging through RPC."""

    def test_echo_with_info_log(self, conformance_conn: ConnFactory) -> None:
        """Verify single INFO log."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            result = proxy.echo_with_info_log(value="test")
            assert result == "test"
            assert len(logs) == 1
            assert logs[0].level == Level.INFO
            assert "test" in logs[0].message

    def test_echo_with_multi_logs(self, conformance_conn: ConnFactory) -> None:
        """Verify DEBUG + INFO + WARN logs."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            result = proxy.echo_with_multi_logs(value="multi")
            assert result == "multi"
            assert len(logs) == 3
            assert logs[0].level == Level.DEBUG
            assert logs[1].level == Level.INFO
            assert logs[2].level == Level.WARN

    def test_echo_with_log_extras(self, conformance_conn: ConnFactory) -> None:
        """Verify log with extra key-value pairs."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            result = proxy.echo_with_log_extras(value="extra")
            assert result == "extra"
            assert len(logs) == 1
            assert logs[0].level == Level.INFO
            assert logs[0].extra is not None
            assert logs[0].extra["source"] == "conformance"
            assert logs[0].extra["detail"] == "extra"


# ---------------------------------------------------------------------------
# Boundary Values
# ---------------------------------------------------------------------------


class TestBoundaryValues:
    """Test boundary values through echo methods."""

    # --- Strings ---

    def test_empty_string(self, conformance_conn: ConnFactory) -> None:
        """Echo empty string."""
        with conformance_conn() as proxy:
            assert proxy.echo_string(value="") == ""

    def test_unicode_emoji(self, conformance_conn: ConnFactory) -> None:
        """Echo emoji string."""
        with conformance_conn() as proxy:
            assert proxy.echo_string(value="\U0001f600\U0001f680") == "\U0001f600\U0001f680"

    def test_unicode_cjk(self, conformance_conn: ConnFactory) -> None:
        """Echo CJK characters."""
        with conformance_conn() as proxy:
            val = "\u4f60\u597d\u4e16\u754c"
            assert proxy.echo_string(value=val) == val

    def test_unicode_rtl(self, conformance_conn: ConnFactory) -> None:
        """Echo RTL text."""
        with conformance_conn() as proxy:
            val = "\u0645\u0631\u062d\u0628\u0627"
            assert proxy.echo_string(value=val) == val

    def test_string_with_null_byte(self, conformance_conn: ConnFactory) -> None:
        """Echo string containing null byte."""
        with conformance_conn() as proxy:
            assert proxy.echo_string(value="a\x00b") == "a\x00b"

    def test_string_with_escapes(self, conformance_conn: ConnFactory) -> None:
        """Echo string with escape characters."""
        with conformance_conn() as proxy:
            assert proxy.echo_string(value="\n\t\\") == "\n\t\\"

    # --- Bytes ---

    def test_empty_bytes(self, conformance_conn: ConnFactory) -> None:
        """Echo empty bytes."""
        with conformance_conn() as proxy:
            assert proxy.echo_bytes(data=b"") == b""

    def test_null_bytes(self, conformance_conn: ConnFactory) -> None:
        """Echo null bytes."""
        with conformance_conn() as proxy:
            assert proxy.echo_bytes(data=b"\x00" * 1000) == b"\x00" * 1000

    def test_high_bytes(self, conformance_conn: ConnFactory) -> None:
        """Echo high byte values."""
        with conformance_conn() as proxy:
            assert proxy.echo_bytes(data=b"\xff" * 1000) == b"\xff" * 1000

    # --- Integers ---

    def test_int_zero(self, conformance_conn: ConnFactory) -> None:
        """Echo zero."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=0) == 0

    def test_int_negative(self, conformance_conn: ConnFactory) -> None:
        """Echo negative integer."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=-1) == -1

    def test_int_max_int64(self, conformance_conn: ConnFactory) -> None:
        """Echo max int64."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=2**63 - 1) == 2**63 - 1

    def test_int_min_int64(self, conformance_conn: ConnFactory) -> None:
        """Echo min int64."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=-(2**63)) == -(2**63)

    # --- Floats ---

    def test_float_zero(self, conformance_conn: ConnFactory) -> None:
        """Echo 0.0."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=0.0) == 0.0

    def test_float_negative_zero(self, conformance_conn: ConnFactory) -> None:
        """Echo -0.0."""
        with conformance_conn() as proxy:
            result = proxy.echo_float(value=-0.0)
            assert result == 0.0
            assert math.copysign(1.0, result) == math.copysign(1.0, -0.0)

    def test_float_inf(self, conformance_conn: ConnFactory) -> None:
        """Echo infinity."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=float("inf")) == float("inf")

    def test_float_neg_inf(self, conformance_conn: ConnFactory) -> None:
        """Echo negative infinity."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=float("-inf")) == float("-inf")

    def test_float_nan(self, conformance_conn: ConnFactory) -> None:
        """Echo NaN."""
        with conformance_conn() as proxy:
            result = proxy.echo_float(value=float("nan"))
            assert math.isnan(result)

    def test_float_small(self, conformance_conn: ConnFactory) -> None:
        """Echo very small float."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=5e-324) == pytest.approx(5e-324)

    def test_float_large(self, conformance_conn: ConnFactory) -> None:
        """Echo very large float."""
        with conformance_conn() as proxy:
            assert proxy.echo_float(value=1e300) == pytest.approx(1e300)

    # --- Lists ---

    def test_empty_list(self, conformance_conn: ConnFactory) -> None:
        """Echo empty list."""
        with conformance_conn() as proxy:
            assert proxy.echo_list(values=[]) == []

    def test_single_element_list(self, conformance_conn: ConnFactory) -> None:
        """Echo single-element list."""
        with conformance_conn() as proxy:
            assert proxy.echo_list(values=["only"]) == ["only"]

    # --- Dicts ---

    def test_empty_dict(self, conformance_conn: ConnFactory) -> None:
        """Echo empty dict."""
        with conformance_conn() as proxy:
            assert proxy.echo_dict(mapping={}) == {}

    def test_single_entry_dict(self, conformance_conn: ConnFactory) -> None:
        """Echo single-entry dict."""
        with conformance_conn() as proxy:
            assert proxy.echo_dict(mapping={"k": 1}) == {"k": 1}

    # --- Nested lists ---

    def test_empty_nested_list(self, conformance_conn: ConnFactory) -> None:
        """Echo list containing empty list."""
        with conformance_conn() as proxy:
            assert proxy.echo_nested_list(matrix=[[]]) == [[]]

    def test_nested_list_varied(self, conformance_conn: ConnFactory) -> None:
        """Echo nested list with varied lengths."""
        with conformance_conn() as proxy:
            matrix = [[1], [2, 3], [4, 5, 6]]
            assert proxy.echo_nested_list(matrix=matrix) == matrix


# ---------------------------------------------------------------------------
# Large Data
# ---------------------------------------------------------------------------


class TestLargeData:
    """Test large data transfers across all transports."""

    def test_large_string(self, conformance_conn: ConnFactory) -> None:
        """Echo a 10KB string."""
        with conformance_conn() as proxy:
            big = "x" * 10_000
            assert proxy.echo_string(value=big) == big

    def test_large_bytes(self, conformance_conn: ConnFactory) -> None:
        """Echo 100KB of random bytes."""
        with conformance_conn() as proxy:
            big = os.urandom(100_000)
            assert proxy.echo_bytes(data=big) == big

    def test_large_list(self, conformance_conn: ConnFactory) -> None:
        """Echo a list of 10K strings."""
        with conformance_conn() as proxy:
            big = [str(i) for i in range(10_000)]
            assert proxy.echo_list(values=big) == big

    def test_large_dict(self, conformance_conn: ConnFactory) -> None:
        """Echo a dict with 1K entries."""
        with conformance_conn() as proxy:
            big = {f"key_{i}": i for i in range(1_000)}
            assert proxy.echo_dict(mapping=big) == big

    def test_large_batch_producer(self, conformance_conn: ConnFactory) -> None:
        """Produce 5 batches of 10K rows each."""
        with conformance_conn() as proxy:
            batches = list(proxy.produce_large_batches(rows_per_batch=10_000, batch_count=5))
            assert len(batches) == 5
            for ab in batches:
                assert ab.batch.num_rows == 10_000

    def test_large_exchange(self, conformance_conn: ConnFactory) -> None:
        """Exchange 5K-row batches, 10 exchanges."""
        with conformance_conn() as proxy, proxy.exchange_scale(factor=2.0) as session:
            for _ in range(10):
                values = [float(v) for v in range(5_000)]
                inp = AnnotatedBatch.from_pydict({"value": values})
                out = session.exchange(inp)
                assert out.batch.num_rows == 5_000

    def test_many_small_batches(self, conformance_conn: ConnFactory) -> None:
        """Produce 100 single-row batches."""
        with conformance_conn() as proxy:
            batches = list(proxy.produce_n(count=100))
            assert len(batches) == 100


# ---------------------------------------------------------------------------
# Producer Streams
# ---------------------------------------------------------------------------


class TestProducerStream:
    """Test producer stream methods."""

    def test_produce_n(self, conformance_conn: ConnFactory) -> None:
        """Produce N batches and verify index/value."""
        with conformance_conn() as proxy:
            batches = list(proxy.produce_n(count=5))
            assert len(batches) == 5
            for i, ab in enumerate(batches):
                assert ab.batch.column("index")[0].as_py() == i
                assert ab.batch.column("value")[0].as_py() == i * 10

    def test_produce_empty(self, conformance_conn: ConnFactory) -> None:
        """Produce zero batches."""
        with conformance_conn() as proxy:
            batches = list(proxy.produce_empty())
            assert len(batches) == 0

    def test_produce_single(self, conformance_conn: ConnFactory) -> None:
        """Produce exactly one batch."""
        with conformance_conn() as proxy:
            batches = list(proxy.produce_single())
            assert len(batches) == 1
            assert batches[0].batch.column("index")[0].as_py() == 0

    def test_produce_with_logs(self, conformance_conn: ConnFactory) -> None:
        """Verify log before each data batch."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            batches = list(proxy.produce_with_logs(count=3))
            assert len(batches) == 3
            assert len(logs) == 3
            for i, log in enumerate(logs):
                assert log.level == Level.INFO
                assert str(i) in log.message

    def test_produce_error_mid_stream(self, conformance_conn: ConnFactory) -> None:
        """Emit N good batches then RpcError."""
        with conformance_conn() as proxy:
            count = 0
            with pytest.raises(RpcError, match="intentional error"):
                for _ab in proxy.produce_error_mid_stream(emit_before_error=3):
                    count += 1
            # Pipe and subprocess deliver batches incrementally;
            # HTTP may deliver the error before any batches depending on transport.
            assert count <= 3

    def test_produce_error_on_init(self, conformance_conn: ConnFactory, request: pytest.FixtureRequest) -> None:
        """Raise RpcError immediately on init.

        Skipped on subprocess because stream init errors corrupt the shared transport.
        """
        if _is_subprocess(request):
            pytest.skip("stream init errors corrupt shared subprocess transport")
        with conformance_conn() as proxy, pytest.raises(RpcError, match="intentional init error"):
            list(proxy.produce_error_on_init())


# ---------------------------------------------------------------------------
# Producer Streams With Headers
# ---------------------------------------------------------------------------


class TestProducerStreamWithHeader:
    """Test producer streams that include a header."""

    def test_header_values(self, conformance_conn: ConnFactory) -> None:
        """Verify header fields before data."""
        with conformance_conn() as proxy:
            session = proxy.produce_with_header(count=3)
            header = session.header
            assert header is not None
            assert isinstance(header, ConformanceHeader)
            assert header.total_expected == 3
            assert "3" in header.description
            batches = list(session)
            assert len(batches) == 3

    def test_header_with_logs(self, conformance_conn: ConnFactory) -> None:
        """Verify logs, header, then data ordering."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            session = proxy.produce_with_header_and_logs(count=2)
            header = session.header
            assert header is not None
            assert isinstance(header, ConformanceHeader)
            batches = list(session)
            assert len(batches) == 2
            assert any(log.message == "stream init log" for log in logs)


# ---------------------------------------------------------------------------
# Exchange Streams
# ---------------------------------------------------------------------------


class TestExchangeStream:
    """Test exchange stream methods."""

    def test_scale_exchange(self, conformance_conn: ConnFactory) -> None:
        """Verify multiplication."""
        with conformance_conn() as proxy, proxy.exchange_scale(factor=3.0) as session:
            inp = AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]})
            out = session.exchange(inp)
            assert out.batch.column("value").to_pylist() == [
                pytest.approx(3.0),
                pytest.approx(6.0),
                pytest.approx(9.0),
            ]

    def test_echo_via_scale(self, conformance_conn: ConnFactory) -> None:
        """Verify factor=1.0 echoes input."""
        with conformance_conn() as proxy, proxy.exchange_scale(factor=1.0) as session:
            inp = AnnotatedBatch.from_pydict({"value": [5.0, 10.0]})
            out = session.exchange(inp)
            assert out.batch.column("value").to_pylist() == [pytest.approx(5.0), pytest.approx(10.0)]

    def test_accumulate(self, conformance_conn: ConnFactory) -> None:
        """Verify running sum across exchanges — tests state persistence."""
        with conformance_conn() as proxy, proxy.exchange_accumulate() as session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert out1.batch.column("running_sum")[0].as_py() == pytest.approx(3.0)
            assert out1.batch.column("exchange_count")[0].as_py() == 1

            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert out2.batch.column("running_sum")[0].as_py() == pytest.approx(13.0)
            assert out2.batch.column("exchange_count")[0].as_py() == 2

    def test_exchange_with_logs(self, conformance_conn: ConnFactory) -> None:
        """Verify logs per exchange."""
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy, proxy.exchange_with_logs() as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert len(logs) == 2  # INFO + DEBUG
            assert logs[0].level == Level.INFO
            assert logs[1].level == Level.DEBUG

    def test_error_first_exchange(self, conformance_conn: ConnFactory) -> None:
        """Verify error on first exchange (fail_on=1)."""
        with (
            conformance_conn() as proxy,
            proxy.exchange_error_on_nth(fail_on=1) as session,
            pytest.raises(RpcError, match="intentional error"),
        ):
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

    def test_error_nth_exchange(self, conformance_conn: ConnFactory) -> None:
        """Verify N-1 good exchanges, then error."""
        with conformance_conn() as proxy, proxy.exchange_error_on_nth(fail_on=3) as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            with pytest.raises(RpcError, match="intentional error"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))

    def test_error_on_init(self, conformance_conn: ConnFactory, request: pytest.FixtureRequest) -> None:
        """Verify RpcError when exchange stream init raises.

        Skipped on pipe/subprocess: stream init errors without headers leave
        the pipe transport in an inconsistent state because the client sends
        a tick before reading the error response.
        Only tested on HTTP where each exchange is a separate request.
        """
        if not _is_http(request):
            pytest.skip("exchange init errors only clean on HTTP transport")
        with conformance_conn() as proxy, pytest.raises(RpcError, match="intentional exchange init error"):
            proxy.exchange_error_on_init()

    def test_empty_exchange_session(self, conformance_conn: ConnFactory) -> None:
        """Open stream, close without exchanging."""
        with conformance_conn() as proxy:
            with proxy.exchange_scale(factor=1.0):
                pass  # just open and close
            # Verify transport is still usable
            assert proxy.echo_int(value=42) == 42

    def test_zero_row_input(self, conformance_conn: ConnFactory) -> None:
        """Send zero-row batch to exchange."""
        with conformance_conn() as proxy, proxy.exchange_scale(factor=2.0) as session:
            schema = pa.schema([pa.field("value", pa.float64())])
            empty = pa.RecordBatch.from_pydict({"value": pa.array([], type=pa.float64())}, schema=schema)
            out = session.exchange(AnnotatedBatch(batch=empty))
            assert out.batch.num_rows == 0


# ---------------------------------------------------------------------------
# Exchange Streams With Headers
# ---------------------------------------------------------------------------


class TestExchangeStreamWithHeader:
    """Test exchange streams with headers."""

    def test_exchange_header_then_data(self, conformance_conn: ConnFactory) -> None:
        """Verify header arrives, then exchanges work."""
        with conformance_conn() as proxy:
            session = proxy.exchange_with_header(factor=2.0)
            header = session.header
            assert header is not None
            assert isinstance(header, ConformanceHeader)
            assert "2.0" in header.description

            with session:
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out.batch.column("value")[0].as_py() == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# Error Recovery
# ---------------------------------------------------------------------------


class TestErrorRecovery:
    """Test that the transport remains usable after errors."""

    def test_unary_error_then_success(self, conformance_conn: ConnFactory) -> None:
        """Verify unary error then successful unary call."""
        with conformance_conn() as proxy:
            with pytest.raises(RpcError):
                proxy.raise_value_error(message="boom")
            assert proxy.echo_int(value=42) == 42

    def test_stream_mid_error_then_unary(self, conformance_conn: ConnFactory) -> None:
        """Verify mid-stream error then successful unary call."""
        with conformance_conn() as proxy:
            with pytest.raises(RpcError):
                for _ab in proxy.produce_error_mid_stream(emit_before_error=1):
                    pass
            assert proxy.echo_string(value="ok") == "ok"

    def test_exchange_error_then_exchange(self, conformance_conn: ConnFactory) -> None:
        """Verify exchange error then new successful exchange."""
        with conformance_conn() as proxy:
            with proxy.exchange_error_on_nth(fail_on=1) as session, pytest.raises(RpcError):
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

            with proxy.exchange_scale(factor=2.0) as session2:
                out = session2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out.batch.column("value")[0].as_py() == pytest.approx(10.0)

    def test_multiple_sequential_sessions(self, conformance_conn: ConnFactory) -> None:
        """Verify multiple sequential sessions on same transport."""
        with conformance_conn() as proxy:
            assert proxy.echo_int(value=1) == 1
            assert len(list(proxy.produce_n(count=2))) == 2
            with proxy.exchange_scale(factor=2.0) as session:
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
                assert out.batch.column("value")[0].as_py() == pytest.approx(6.0)
            assert proxy.echo_string(value="end") == "end"


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


class TestDescribeConformance:
    """Validate __describe__ introspection output for the conformance service."""

    @pytest.fixture(scope="class")
    def service_description(self) -> ServiceDescription:
        """Build a ServiceDescription via in-process pipe introspect()."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)

        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            desc = introspect(client_transport)
        finally:
            client_transport.close()
            thread.join(timeout=5)
        return desc

    def test_run_describe_conformance(self, service_description: ServiceDescription) -> None:
        """Run the full describe conformance suite and fail with detailed errors."""
        suite = run_describe_conformance(service_description)
        if not suite.success:
            failures = [r for r in suite.results if not r.passed]
            details = "\n".join(f"  {r.name}: {r.error}" for r in failures)
            pytest.fail(f"{suite.failed}/{suite.total} describe conformance tests failed:\n{details}")

    def test_describe_via_rpc(self, service_description: ServiceDescription) -> None:
        """Smoke test: basic transport-level describe call works."""
        assert len(service_description.methods) == 46
        assert service_description.protocol_name == "ConformanceService"
        echo_str = service_description.methods["echo_string"]
        assert echo_str.method_type == MethodType.UNARY


# ---------------------------------------------------------------------------
# Dynamic Streams With Rich Multi-Type Headers
# ---------------------------------------------------------------------------


def _assert_rich_header(actual: RichHeader, seed: int) -> None:
    """Assert all fields of a ``RichHeader`` match the expected seed values."""
    expected = build_rich_header(seed)
    assert actual.str_field == expected.str_field
    assert actual.bytes_field == expected.bytes_field
    assert actual.int_field == expected.int_field
    assert actual.float_field == pytest.approx(expected.float_field)
    assert actual.bool_field == expected.bool_field
    assert actual.list_of_int == expected.list_of_int
    assert actual.list_of_str == expected.list_of_str
    assert actual.dict_field == expected.dict_field
    assert actual.enum_field == expected.enum_field
    assert actual.nested_point.x == pytest.approx(expected.nested_point.x)
    assert actual.nested_point.y == pytest.approx(expected.nested_point.y)
    assert actual.optional_str == expected.optional_str
    assert actual.optional_int == expected.optional_int
    if expected.optional_nested is None:
        assert actual.optional_nested is None
    else:
        assert actual.optional_nested is not None
        assert actual.optional_nested.x == pytest.approx(expected.optional_nested.x)
        assert actual.optional_nested.y == pytest.approx(expected.optional_nested.y)
    assert len(actual.list_of_nested) == len(expected.list_of_nested)
    for a_pt, e_pt in zip(actual.list_of_nested, expected.list_of_nested, strict=True):
        assert a_pt.x == pytest.approx(e_pt.x)
        assert a_pt.y == pytest.approx(e_pt.y)
    assert actual.nested_list == expected.nested_list
    assert actual.annotated_int32 == expected.annotated_int32
    assert actual.annotated_float32 == pytest.approx(expected.annotated_float32)
    assert actual.dict_str_str == expected.dict_str_str


class TestDynamicRichHeader:
    """Test producer streams with rich multi-type headers."""

    def test_seed_42(self, conformance_conn: ConnFactory) -> None:
        """Rich header with seed=42: PENDING, bool=True, opt_nested present."""
        with conformance_conn() as proxy:
            session = proxy.produce_with_rich_header(seed=42, count=3)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 42)
            batches = list(session)
            assert len(batches) == 3
            for i, ab in enumerate(batches):
                assert ab.batch.column("index")[0].as_py() == i
                assert ab.batch.column("value")[0].as_py() == i * 10

    def test_seed_7(self, conformance_conn: ConnFactory) -> None:
        """Rich header with seed=7: ACTIVE, bool=False, opt_int present."""
        with conformance_conn() as proxy:
            session = proxy.produce_with_rich_header(seed=7, count=2)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 7)
            batches = list(session)
            assert len(batches) == 2

    def test_seed_0(self, conformance_conn: ConnFactory) -> None:
        """Rich header with seed=0: edge case zeros."""
        with conformance_conn() as proxy:
            session = proxy.produce_with_rich_header(seed=0, count=1)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 0)
            batches = list(session)
            assert len(batches) == 1


class TestDynamicSchemaProducer:
    """Test producer streams with dynamic output schema and rich header."""

    def test_all_columns(self, conformance_conn: ConnFactory) -> None:
        """Dynamic schema with all columns: index + label + score."""
        with conformance_conn() as proxy:
            session = proxy.produce_dynamic_schema(seed=42, count=3, include_strings=True, include_floats=True)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 42)
            batches = list(session)
            assert len(batches) == 3
            expected_schema = build_dynamic_schema(include_strings=True, include_floats=True)
            for i, ab in enumerate(batches):
                assert ab.batch.schema.equals(expected_schema)
                assert ab.batch.column("index")[0].as_py() == i
                assert ab.batch.column("label")[0].as_py() == f"row-{i}"
                assert ab.batch.column("score")[0].as_py() == pytest.approx(i * 1.5)

    def test_strings_only(self, conformance_conn: ConnFactory) -> None:
        """Dynamic schema with strings only: index + label."""
        with conformance_conn() as proxy:
            session = proxy.produce_dynamic_schema(seed=7, count=2, include_strings=True, include_floats=False)
            header = session.header
            assert header is not None
            _assert_rich_header(header, 7)
            batches = list(session)
            assert len(batches) == 2
            for i, ab in enumerate(batches):
                assert ab.batch.schema.names == ["index", "label"]
                assert ab.batch.column("label")[0].as_py() == f"row-{i}"

    def test_floats_only(self, conformance_conn: ConnFactory) -> None:
        """Dynamic schema with floats only: index + score."""
        with conformance_conn() as proxy:
            session = proxy.produce_dynamic_schema(seed=5, count=2, include_strings=False, include_floats=True)
            header = session.header
            assert header is not None
            _assert_rich_header(header, 5)
            batches = list(session)
            assert len(batches) == 2
            for i, ab in enumerate(batches):
                assert ab.batch.schema.names == ["index", "score"]
                assert ab.batch.column("score")[0].as_py() == pytest.approx(i * 1.5)

    def test_minimal(self, conformance_conn: ConnFactory) -> None:
        """Dynamic schema minimal: index only."""
        with conformance_conn() as proxy:
            session = proxy.produce_dynamic_schema(seed=0, count=1, include_strings=False, include_floats=False)
            header = session.header
            assert header is not None
            _assert_rich_header(header, 0)
            batches = list(session)
            assert len(batches) == 1
            assert batches[0].batch.schema.names == ["index"]
            assert batches[0].batch.column("index")[0].as_py() == 0


class TestRichHeaderExchange:
    """Test exchange streams with rich multi-type headers."""

    def test_header_then_exchange(self, conformance_conn: ConnFactory) -> None:
        """Exchange with rich header seed=5, factor=2.5."""
        with conformance_conn() as proxy:
            session = proxy.exchange_with_rich_header(seed=5, factor=2.5)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 5)
            with session:
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [4.0]}))
                assert out.batch.column("value")[0].as_py() == pytest.approx(10.0)

    def test_different_seed(self, conformance_conn: ConnFactory) -> None:
        """Exchange with rich header seed=12, factor=1.0."""
        with conformance_conn() as proxy:
            session = proxy.exchange_with_rich_header(seed=12, factor=1.0)
            header = session.header
            assert header is not None
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 12)
            with session:
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [7.0]}))
                assert out.batch.column("value")[0].as_py() == pytest.approx(7.0)
