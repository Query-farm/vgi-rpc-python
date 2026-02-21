# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance test runner library.

Provides test registration, execution, and result collection for validating
vgi-rpc wire-protocol conformance. Tests are derived from the reference
``test_conformance.py`` pytest suite and cover all framework capabilities.

Usage::

    from vgi_rpc.conformance import run_conformance, LogCollector

    logs = LogCollector()
    suite = run_conformance(proxy, logs)
    assert suite.success

"""

from __future__ import annotations

import contextlib
import fnmatch
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.conformance._types import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    Point,
    Status,
)
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import AnnotatedBatch, RpcError

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConformanceResult:
    """Result of a single conformance test."""

    name: str
    category: str
    passed: bool
    duration_ms: float
    error: str | None = None


@dataclass(frozen=True)
class ConformanceSuite:
    """Aggregate results of a conformance test run."""

    results: list[ConformanceResult]
    total: int
    passed: int
    failed: int
    skipped: int
    duration_ms: float

    @property
    def success(self) -> bool:
        """Whether all tests passed."""
        return self.failed == 0


# ---------------------------------------------------------------------------
# Log collector
# ---------------------------------------------------------------------------


class LogCollector:
    """Collects log messages from RPC calls for assertion."""

    def __init__(self) -> None:
        """Initialize with an empty message list."""
        self.messages: list[Message] = []

    def __call__(self, msg: Message) -> None:
        """Append a log message."""
        self.messages.append(msg)

    def clear(self) -> None:
        """Clear all collected messages."""
        self.messages = []


# ---------------------------------------------------------------------------
# Test registration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ConformanceTest:
    """A registered conformance test."""

    category: str
    name: str
    fn: Callable[[ConformanceService, LogCollector], None]

    @property
    def full_name(self) -> str:
        """Return category.name format."""
        return f"{self.category}.{self.name}"


_TESTS: list[_ConformanceTest] = []


def _conformance_test(
    *, category: str, name: str
) -> Callable[[Callable[[ConformanceService, LogCollector], None]], Callable[[ConformanceService, LogCollector], None]]:
    """Register a conformance test function."""

    def decorator(
        fn: Callable[[ConformanceService, LogCollector], None],
    ) -> Callable[[ConformanceService, LogCollector], None]:
        _TESTS.append(_ConformanceTest(category=category, name=name, fn=fn))
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Scalar echo tests
# ---------------------------------------------------------------------------


@_conformance_test(category="scalar_echo", name="echo_string")
def _test_echo_string(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_string(value="hello") == "hello"


@_conformance_test(category="scalar_echo", name="echo_bytes")
def _test_echo_bytes(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_bytes(data=b"hello") == b"hello"


@_conformance_test(category="scalar_echo", name="echo_int")
def _test_echo_int(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=42) == 42


@_conformance_test(category="scalar_echo", name="echo_float")
def _test_echo_float(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.echo_float(value=3.14)
    assert abs(result - 3.14) < 1e-6


@_conformance_test(category="scalar_echo", name="echo_bool")
def _test_echo_bool(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_bool(value=True) is True
    assert proxy.echo_bool(value=False) is False


# ---------------------------------------------------------------------------
# Void tests
# ---------------------------------------------------------------------------


@_conformance_test(category="void", name="void_noop")
def _test_void_noop(proxy: ConformanceService, logs: LogCollector) -> None:
    proxy.void_noop()  # should not raise


@_conformance_test(category="void", name="void_with_param")
def _test_void_with_param(proxy: ConformanceService, logs: LogCollector) -> None:
    proxy.void_with_param(value=99)  # should not raise


# ---------------------------------------------------------------------------
# Complex types tests
# ---------------------------------------------------------------------------


@_conformance_test(category="complex_types", name="echo_enum_pending")
def _test_echo_enum_pending(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_enum(status=Status.PENDING) == Status.PENDING


@_conformance_test(category="complex_types", name="echo_enum_active")
def _test_echo_enum_active(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_enum(status=Status.ACTIVE) == Status.ACTIVE


@_conformance_test(category="complex_types", name="echo_enum_closed")
def _test_echo_enum_closed(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_enum(status=Status.CLOSED) == Status.CLOSED


@_conformance_test(category="complex_types", name="echo_list")
def _test_echo_list(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_list(values=["a", "b", "c"]) == ["a", "b", "c"]


@_conformance_test(category="complex_types", name="echo_dict")
def _test_echo_dict(proxy: ConformanceService, logs: LogCollector) -> None:
    mapping = {"z": 1, "a": 2, "m": 3}
    assert proxy.echo_dict(mapping=mapping) == mapping


@_conformance_test(category="complex_types", name="echo_nested_list")
def _test_echo_nested_list(proxy: ConformanceService, logs: LogCollector) -> None:
    matrix = [[1, 2], [3, 4, 5], [6]]
    assert proxy.echo_nested_list(matrix=matrix) == matrix


# ---------------------------------------------------------------------------
# Optional/Nullable tests
# ---------------------------------------------------------------------------


@_conformance_test(category="optional", name="optional_string_none")
def _test_optional_string_none(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_optional_string(value=None) is None


@_conformance_test(category="optional", name="optional_string_non_none")
def _test_optional_string_non_none(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_optional_string(value="hello") == "hello"


@_conformance_test(category="optional", name="optional_int_none")
def _test_optional_int_none(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_optional_int(value=None) is None


@_conformance_test(category="optional", name="optional_int_non_none")
def _test_optional_int_non_none(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_optional_int(value=7) == 7


@_conformance_test(category="optional", name="empty_string_vs_null")
def _test_empty_string_vs_null(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_optional_string(value="") == ""
    assert proxy.echo_optional_string(value=None) is None


# ---------------------------------------------------------------------------
# Dataclass tests
# ---------------------------------------------------------------------------


@_conformance_test(category="dataclass", name="echo_point")
def _test_echo_point(proxy: ConformanceService, logs: LogCollector) -> None:
    p = Point(x=1.5, y=2.5)
    result = proxy.echo_point(point=p)
    assert isinstance(result, Point)
    assert abs(result.x - 1.5) < 1e-6
    assert abs(result.y - 2.5) < 1e-6


@_conformance_test(category="dataclass", name="echo_bounding_box")
def _test_echo_bounding_box(proxy: ConformanceService, logs: LogCollector) -> None:
    box = BoundingBox(top_left=Point(x=0.0, y=10.0), bottom_right=Point(x=10.0, y=0.0), label="test")
    result = proxy.echo_bounding_box(box=box)
    assert isinstance(result, BoundingBox)
    assert abs(result.top_left.x - 0.0) < 1e-6
    assert abs(result.top_left.y - 10.0) < 1e-6
    assert abs(result.bottom_right.x - 10.0) < 1e-6
    assert result.label == "test"


@_conformance_test(category="dataclass", name="echo_all_types")
def _test_echo_all_types(proxy: ConformanceService, logs: LogCollector) -> None:
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
    assert abs(result.float_field - 3.14) < 1e-6
    assert result.bool_field is True
    assert result.list_of_int == [1, 2, 3]
    assert result.list_of_str == ["a", "b"]
    assert result.dict_field == {"k": 1}
    assert result.enum_field == Status.ACTIVE
    assert abs(result.nested_point.x - 1.0) < 1e-6
    assert result.optional_str == "present"
    assert result.optional_int == 7
    assert result.optional_nested is not None
    assert abs(result.optional_nested.x - 3.0) < 1e-6
    assert len(result.list_of_nested) == 1
    assert result.annotated_int32 == 100
    assert abs(result.annotated_float32 - 1.5) < 1e-6
    assert result.nested_list == [[1, 2], [3]]
    assert result.dict_str_str == {"key": "val"}


@_conformance_test(category="dataclass", name="echo_all_types_with_nulls")
def _test_echo_all_types_with_nulls(proxy: ConformanceService, logs: LogCollector) -> None:
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


@_conformance_test(category="dataclass", name="inspect_point")
def _test_inspect_point(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.inspect_point(point=Point(x=1.5, y=2.5))
    assert result == "Point(1.5, 2.5)"


# ---------------------------------------------------------------------------
# Annotated types tests
# ---------------------------------------------------------------------------


@_conformance_test(category="annotated", name="echo_int32")
def _test_echo_int32(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int32(value=42) == 42


@_conformance_test(category="annotated", name="echo_float32")
def _test_echo_float32(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.echo_float32(value=1.5)
    assert abs(result - 1.5) < 1e-6


# ---------------------------------------------------------------------------
# Multi-param tests
# ---------------------------------------------------------------------------


@_conformance_test(category="multi_param", name="add_floats")
def _test_add_floats(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.add_floats(a=1.5, b=2.5)
    assert abs(result - 4.0) < 1e-6


@_conformance_test(category="multi_param", name="concatenate_default")
def _test_concatenate_default(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.concatenate(prefix="hello", suffix="world") == "hello-world"


@_conformance_test(category="multi_param", name="concatenate_custom")
def _test_concatenate_custom(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.concatenate(prefix="hello", suffix="world", separator="_") == "hello_world"


@_conformance_test(category="multi_param", name="with_defaults_all")
def _test_with_defaults_all(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.with_defaults(required=1)
    assert result == "required=1, optional_str=default, optional_int=42"


@_conformance_test(category="multi_param", name="with_defaults_override")
def _test_with_defaults_override(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.with_defaults(required=2, optional_str="custom", optional_int=99)
    assert result == "required=2, optional_str=custom, optional_int=99"


# ---------------------------------------------------------------------------
# Error propagation tests
# ---------------------------------------------------------------------------


@_conformance_test(category="errors", name="value_error")
def _test_value_error(proxy: ConformanceService, logs: LogCollector) -> None:
    try:
        proxy.raise_value_error(message="test error")
        raise AssertionError("Expected RpcError")
    except RpcError as e:
        assert e.error_type == "ValueError"
        assert "test error" in e.error_message


@_conformance_test(category="errors", name="runtime_error")
def _test_runtime_error(proxy: ConformanceService, logs: LogCollector) -> None:
    try:
        proxy.raise_runtime_error(message="runtime error")
        raise AssertionError("Expected RpcError")
    except RpcError as e:
        assert e.error_type == "RuntimeError"


@_conformance_test(category="errors", name="type_error")
def _test_type_error(proxy: ConformanceService, logs: LogCollector) -> None:
    try:
        proxy.raise_type_error(message="type error")
        raise AssertionError("Expected RpcError")
    except RpcError as e:
        assert e.error_type == "TypeError"


# ---------------------------------------------------------------------------
# Logging tests
# ---------------------------------------------------------------------------


@_conformance_test(category="logging", name="info_log")
def _test_info_log(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    result = proxy.echo_with_info_log(value="test")
    assert result == "test"
    assert len(logs.messages) == 1
    assert logs.messages[0].level == Level.INFO
    assert "test" in logs.messages[0].message


@_conformance_test(category="logging", name="multi_level")
def _test_multi_level(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    result = proxy.echo_with_multi_logs(value="multi")
    assert result == "multi"
    assert len(logs.messages) == 3
    assert logs.messages[0].level == Level.DEBUG
    assert logs.messages[1].level == Level.INFO
    assert logs.messages[2].level == Level.WARN


@_conformance_test(category="logging", name="log_extras")
def _test_log_extras(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    result = proxy.echo_with_log_extras(value="extra")
    assert result == "extra"
    assert len(logs.messages) == 1
    assert logs.messages[0].level == Level.INFO
    assert logs.messages[0].extra is not None
    assert logs.messages[0].extra["source"] == "conformance"
    assert logs.messages[0].extra["detail"] == "extra"


# ---------------------------------------------------------------------------
# Boundary values tests
# ---------------------------------------------------------------------------


@_conformance_test(category="boundary_values", name="empty_string")
def _test_empty_string(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_string(value="") == ""


@_conformance_test(category="boundary_values", name="unicode_emoji")
def _test_unicode_emoji(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_string(value="\U0001f600\U0001f680") == "\U0001f600\U0001f680"


@_conformance_test(category="boundary_values", name="unicode_cjk")
def _test_unicode_cjk(proxy: ConformanceService, logs: LogCollector) -> None:
    val = "\u4f60\u597d\u4e16\u754c"
    assert proxy.echo_string(value=val) == val


@_conformance_test(category="boundary_values", name="unicode_rtl")
def _test_unicode_rtl(proxy: ConformanceService, logs: LogCollector) -> None:
    val = "\u0645\u0631\u062d\u0628\u0627"
    assert proxy.echo_string(value=val) == val


@_conformance_test(category="boundary_values", name="string_null_byte")
def _test_string_null_byte(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_string(value="a\x00b") == "a\x00b"


@_conformance_test(category="boundary_values", name="string_escapes")
def _test_string_escapes(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_string(value="\n\t\\") == "\n\t\\"


@_conformance_test(category="boundary_values", name="empty_bytes")
def _test_empty_bytes(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_bytes(data=b"") == b""


@_conformance_test(category="boundary_values", name="null_bytes")
def _test_null_bytes(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_bytes(data=b"\x00" * 1000) == b"\x00" * 1000


@_conformance_test(category="boundary_values", name="high_bytes")
def _test_high_bytes(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_bytes(data=b"\xff" * 1000) == b"\xff" * 1000


@_conformance_test(category="boundary_values", name="int_zero")
def _test_int_zero(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=0) == 0


@_conformance_test(category="boundary_values", name="int_negative")
def _test_int_negative(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=-1) == -1


@_conformance_test(category="boundary_values", name="int_max_int64")
def _test_int_max_int64(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=2**63 - 1) == 2**63 - 1


@_conformance_test(category="boundary_values", name="int_min_int64")
def _test_int_min_int64(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=-(2**63)) == -(2**63)


@_conformance_test(category="boundary_values", name="float_zero")
def _test_float_zero(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_float(value=0.0) == 0.0


@_conformance_test(category="boundary_values", name="float_negative_zero")
def _test_float_negative_zero(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.echo_float(value=-0.0)
    assert result == 0.0
    assert math.copysign(1.0, result) == math.copysign(1.0, -0.0)


@_conformance_test(category="boundary_values", name="float_inf")
def _test_float_inf(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_float(value=float("inf")) == float("inf")


@_conformance_test(category="boundary_values", name="float_neg_inf")
def _test_float_neg_inf(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_float(value=float("-inf")) == float("-inf")


@_conformance_test(category="boundary_values", name="float_nan")
def _test_float_nan(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.echo_float(value=float("nan"))
    assert math.isnan(result)


@_conformance_test(category="boundary_values", name="float_small")
def _test_float_small(proxy: ConformanceService, logs: LogCollector) -> None:
    result = proxy.echo_float(value=5e-324)
    assert abs(result - 5e-324) < 1e-330 or result == 5e-324


@_conformance_test(category="boundary_values", name="empty_list")
def _test_empty_list(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_list(values=[]) == []


@_conformance_test(category="boundary_values", name="empty_dict")
def _test_empty_dict(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_dict(mapping={}) == {}


@_conformance_test(category="boundary_values", name="empty_nested_list")
def _test_empty_nested_list(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_nested_list(matrix=[[]]) == [[]]


# ---------------------------------------------------------------------------
# Producer stream tests
# ---------------------------------------------------------------------------


@_conformance_test(category="producer_stream", name="produce_n")
def _test_produce_n(proxy: ConformanceService, logs: LogCollector) -> None:
    batches = list(proxy.produce_n(count=5))
    assert len(batches) == 5
    for i, ab in enumerate(batches):
        assert cast(int, ab.batch.column("index")[0].as_py()) == i
        assert cast(int, ab.batch.column("value")[0].as_py()) == i * 10


@_conformance_test(category="producer_stream", name="produce_empty")
def _test_produce_empty(proxy: ConformanceService, logs: LogCollector) -> None:
    batches = list(proxy.produce_empty())
    assert len(batches) == 0


@_conformance_test(category="producer_stream", name="produce_single")
def _test_produce_single(proxy: ConformanceService, logs: LogCollector) -> None:
    batches = list(proxy.produce_single())
    assert len(batches) == 1
    assert cast(int, batches[0].batch.column("index")[0].as_py()) == 0


@_conformance_test(category="producer_stream", name="produce_with_logs")
def _test_produce_with_logs(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    batches = list(proxy.produce_with_logs(count=3))
    assert len(batches) == 3
    assert len(logs.messages) == 3
    for i, log in enumerate(logs.messages):
        assert log.level == Level.INFO
        assert str(i) in log.message


@_conformance_test(category="producer_stream", name="error_mid_stream")
def _test_produce_error_mid_stream(proxy: ConformanceService, logs: LogCollector) -> None:
    count = 0
    try:
        for _ab in proxy.produce_error_mid_stream(emit_before_error=3):
            count += 1
        raise AssertionError("Expected RpcError")
    except RpcError as e:
        assert "intentional error" in str(e)
    assert count <= 3


# ---------------------------------------------------------------------------
# Producer header tests
# ---------------------------------------------------------------------------


@_conformance_test(category="producer_header", name="header_values")
def _test_header_values(proxy: ConformanceService, logs: LogCollector) -> None:
    session = proxy.produce_with_header(count=3)
    header = session.header
    assert header is not None
    assert isinstance(header, ConformanceHeader)
    assert header.total_expected == 3
    assert "3" in header.description
    batches = list(session)
    assert len(batches) == 3


@_conformance_test(category="producer_header", name="header_with_logs")
def _test_header_with_logs(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    session = proxy.produce_with_header_and_logs(count=2)
    header = session.header
    assert header is not None
    assert isinstance(header, ConformanceHeader)
    batches = list(session)
    assert len(batches) == 2
    assert any(log.message == "stream init log" for log in logs.messages)


# ---------------------------------------------------------------------------
# Exchange stream tests
# ---------------------------------------------------------------------------


@_conformance_test(category="exchange_stream", name="scale")
def _test_exchange_scale(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_scale(factor=3.0) as session:
        inp = AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]})
        out = session.exchange(inp)
        values = cast("list[float]", out.batch.column("value").to_pylist())
        assert abs(values[0] - 3.0) < 1e-6
        assert abs(values[1] - 6.0) < 1e-6
        assert abs(values[2] - 9.0) < 1e-6


@_conformance_test(category="exchange_stream", name="echo")
def _test_exchange_echo(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_scale(factor=1.0) as session:
        inp = AnnotatedBatch.from_pydict({"value": [5.0, 10.0]})
        out = session.exchange(inp)
        values = cast("list[float]", out.batch.column("value").to_pylist())
        assert abs(values[0] - 5.0) < 1e-6
        assert abs(values[1] - 10.0) < 1e-6


@_conformance_test(category="exchange_stream", name="accumulate")
def _test_exchange_accumulate(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_accumulate() as session:
        out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
        assert abs(cast(float, out1.batch.column("running_sum")[0].as_py()) - 3.0) < 1e-6
        assert cast(int, out1.batch.column("exchange_count")[0].as_py()) == 1

        out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
        assert abs(cast(float, out2.batch.column("running_sum")[0].as_py()) - 13.0) < 1e-6
        assert cast(int, out2.batch.column("exchange_count")[0].as_py()) == 2


@_conformance_test(category="exchange_stream", name="with_logs")
def _test_exchange_with_logs(proxy: ConformanceService, logs: LogCollector) -> None:
    logs.clear()
    with proxy.exchange_with_logs() as session:
        session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
        assert len(logs.messages) == 2
        assert logs.messages[0].level == Level.INFO
        assert logs.messages[1].level == Level.DEBUG


@_conformance_test(category="exchange_stream", name="error_first")
def _test_exchange_error_first(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_error_on_nth(fail_on=1) as session:
        try:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            raise AssertionError("Expected RpcError")
        except RpcError as e:
            assert "intentional error" in str(e)


@_conformance_test(category="exchange_stream", name="error_nth")
def _test_exchange_error_nth(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_error_on_nth(fail_on=3) as session:
        session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
        session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
        try:
            session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
            raise AssertionError("Expected RpcError")
        except RpcError as e:
            assert "intentional error" in str(e)


@_conformance_test(category="exchange_stream", name="empty_session")
def _test_exchange_empty_session(proxy: ConformanceService, logs: LogCollector) -> None:
    with proxy.exchange_scale(factor=1.0):
        pass
    # Verify transport is still usable
    assert proxy.echo_int(value=42) == 42


# ---------------------------------------------------------------------------
# Exchange header tests
# ---------------------------------------------------------------------------


@_conformance_test(category="exchange_header", name="header_then_exchange")
def _test_exchange_header_then_exchange(proxy: ConformanceService, logs: LogCollector) -> None:
    session = proxy.exchange_with_header(factor=2.0)
    header = session.header
    assert header is not None
    assert isinstance(header, ConformanceHeader)
    assert "2.0" in header.description

    with session:
        out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
        assert abs(cast(float, out.batch.column("value")[0].as_py()) - 10.0) < 1e-6


# ---------------------------------------------------------------------------
# Error recovery tests
# ---------------------------------------------------------------------------


@_conformance_test(category="error_recovery", name="unary_error_then_success")
def _test_unary_error_then_success(proxy: ConformanceService, logs: LogCollector) -> None:
    with contextlib.suppress(RpcError):
        proxy.raise_value_error(message="boom")
    assert proxy.echo_int(value=42) == 42


@_conformance_test(category="error_recovery", name="stream_error_then_unary")
def _test_stream_error_then_unary(proxy: ConformanceService, logs: LogCollector) -> None:
    with contextlib.suppress(RpcError):
        for _ab in proxy.produce_error_mid_stream(emit_before_error=1):
            pass
    assert proxy.echo_string(value="ok") == "ok"


@_conformance_test(category="error_recovery", name="exchange_error_then_exchange")
def _test_exchange_error_then_exchange(proxy: ConformanceService, logs: LogCollector) -> None:
    with contextlib.suppress(RpcError), proxy.exchange_error_on_nth(fail_on=1) as session:
        session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

    with proxy.exchange_scale(factor=2.0) as session2:
        out = session2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
        assert abs(cast(float, out.batch.column("value")[0].as_py()) - 10.0) < 1e-6


@_conformance_test(category="error_recovery", name="multiple_sequential_sessions")
def _test_multiple_sequential_sessions(proxy: ConformanceService, logs: LogCollector) -> None:
    assert proxy.echo_int(value=1) == 1
    assert len(list(proxy.produce_n(count=2))) == 2
    with proxy.exchange_scale(factor=2.0) as session:
        out = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
        assert abs(cast(float, out.batch.column("value")[0].as_py()) - 6.0) < 1e-6
    assert proxy.echo_string(value="end") == "end"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _matches_filter(name: str, patterns: list[str]) -> bool:
    """Check if a test name matches any of the given glob patterns."""
    return any(fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(name.split(".")[0], pattern) for pattern in patterns)


def list_conformance_tests(filter_patterns: list[str] | None = None) -> list[str]:
    """Return sorted names of available tests, optionally filtered.

    Args:
        filter_patterns: Optional glob patterns to filter tests.

    Returns:
        Sorted list of test names in ``category.name`` format.

    """
    names = [t.full_name for t in _TESTS]
    if filter_patterns:
        names = [n for n in names if _matches_filter(n, filter_patterns)]
    return sorted(names)


def run_conformance(
    proxy: ConformanceService,
    log_collector: LogCollector,
    *,
    filter_patterns: list[str] | None = None,
    on_progress: Callable[[ConformanceResult], None] | None = None,
) -> ConformanceSuite:
    """Run conformance tests against a proxy and return results.

    Args:
        proxy: A typed ConformanceService proxy (from RpcConnection or serve_pipe).
        log_collector: LogCollector instance to receive server log messages.
        filter_patterns: Optional glob patterns to filter which tests run.
        on_progress: Optional callback invoked after each test completes.

    Returns:
        A ConformanceSuite with all results.

    """
    suite_start = time.monotonic()
    results: list[ConformanceResult] = []

    tests_to_run = _TESTS
    if filter_patterns:
        tests_to_run = [t for t in _TESTS if _matches_filter(t.full_name, filter_patterns)]

    for test in tests_to_run:
        log_collector.clear()
        start = time.monotonic()
        error: str | None = None
        passed = True
        try:
            test.fn(proxy, log_collector)
        except AssertionError as e:
            passed = False
            error = str(e) if str(e) else "Assertion failed"
        except RpcError as e:
            passed = False
            error = f"RpcError({e.error_type}): {e.error_message}"
        except Exception as e:
            passed = False
            error = f"{type(e).__name__}: {e}"
        elapsed_ms = (time.monotonic() - start) * 1000

        result = ConformanceResult(
            name=test.full_name,
            category=test.category,
            passed=passed,
            duration_ms=elapsed_ms,
            error=error,
        )
        results.append(result)
        if on_progress:
            on_progress(result)

    suite_elapsed = (time.monotonic() - suite_start) * 1000
    passed_count = sum(1 for r in results if r.passed)
    failed_count = sum(1 for r in results if not r.passed)

    return ConformanceSuite(
        results=results,
        total=len(results),
        passed=passed_count,
        failed=failed_count,
        skipped=0,
        duration_ms=suite_elapsed,
    )
