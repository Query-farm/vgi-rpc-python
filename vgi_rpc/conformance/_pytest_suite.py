# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance test suite — reference behavior specification for vgi-rpc.

Tests all framework capabilities through the conformance server across
pipe, subprocess, and HTTP transports.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import math
import os
import threading
import time
from collections.abc import Callable
from decimal import Decimal
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from pyarrow import ipc

if TYPE_CHECKING:
    import httpx

from vgi_rpc.conformance import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    ConformanceService,
    ConformanceServiceImpl,
    ContainerWideTypes,
    DeepNested,
    EmbeddedArrow,
    Point,
    RichHeader,
    Status,
    WideTypes,
    build_dynamic_schema,
    build_rich_header,
    run_describe_conformance,
)
from vgi_rpc.conformance.proof_harness import ProofUnsupported, ProofWorker, ProofWorkerFactory
from vgi_rpc.introspect import ServiceDescription, introspect
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import CALL_STATE_KEY, STATE_KEY
from vgi_rpc.rpc import AnnotatedBatch, MethodType, RpcError, RpcServer, make_pipe_pair
from vgi_rpc.utils import empty_batch, new_ipc_stream

ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]

#: Duplicated rather than imported from ``vgi_rpc.http`` so the suite stays
#: importable for runners that install vgi-rpc without the ``http`` extra.
_PROOF_HEADER = "VGI-Proxy-Proof"
_PROOF_REQUIRED_HEADER = "VGI-Proxy-Proof-Required"
_AUTH_REASON_HEADER = "VGI-Auth-Reason"
_AUTH_PROXY_REQUIRED_HEADER = "VGI-Auth-Proxy-Required"
#: The closed set of docs/unauthorized-spec.md §3. Spelled out rather than
#: derived from the enum so that a port silently widening the set in Python
#: would still fail here.
_AUTH_REASONS = frozenset(
    {
        "missing_credential",
        "invalid_credential",
        "expired_credential",
        "insufficient_scope",
        "proxy_required",
        "unauthorized",
    }
)
#: Header a request uses to name the reason it wants refused with, honoured by
#: workers behind the ``conformance_http_auth_reason_port`` fixture.
_CONFORMANCE_REASON_HEADER = "X-Conformance-Auth-Reason"
#: The reasons a request can ask for. ``proxy_required`` is excluded because
#: §5 derives it from server configuration, not from the request, and
#: ``unauthorized`` because it is what the *absence* of a request reason must
#: produce — both are asserted separately below.
_REQUESTABLE_REASONS = (
    "missing_credential",
    "invalid_credential",
    "expired_credential",
    "insufficient_scope",
)

pytestmark = pytest.mark.timeout(5)


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
# Unary: Wide Arrow Types (integer widths, dates, decimals, large/fixed binary)
# ---------------------------------------------------------------------------


class TestUnaryWideTypes:
    """Echo methods covering Arrow primitive widths beyond int64/float64."""

    def test_echo_int8(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_int8(value=-128) == -128
            assert proxy.echo_int8(value=127) == 127

    def test_echo_int16(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_int16(value=-32768) == -32768
            assert proxy.echo_int16(value=32767) == 32767

    def test_echo_uint8(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_uint8(value=0) == 0
            assert proxy.echo_uint8(value=255) == 255

    def test_echo_uint16(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_uint16(value=65535) == 65535

    def test_echo_uint32(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_uint32(value=4_294_967_295) == 4_294_967_295

    def test_echo_uint64_above_int64_max(self, conformance_conn: ConnFactory) -> None:
        """Values above ``int64`` max prove uint64 isn't being truncated to int64."""
        with conformance_conn() as proxy:
            big = 18_000_000_000_000_000_000
            assert proxy.echo_uint64(value=big) == big

    def test_echo_date(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            d = _dt.date(2026, 4, 25)
            assert proxy.echo_date(value=d) == d

    def test_echo_timestamp(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            t = _dt.datetime(2026, 4, 25, 12, 0, 0, 123456)
            assert proxy.echo_timestamp(value=t) == t

    def test_echo_timestamp_utc(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            t = _dt.datetime(2026, 4, 25, 12, 0, 0, 123456, tzinfo=_dt.UTC)
            result = proxy.echo_timestamp_utc(value=t)
            assert result == t
            assert result.tzinfo is not None

    def test_echo_time(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            t = _dt.time(12, 30, 45, 123456)
            assert proxy.echo_time(value=t) == t

    def test_echo_duration(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            d = _dt.timedelta(seconds=10, microseconds=500)
            assert proxy.echo_duration(value=d) == d

    def test_echo_decimal(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            v = Decimal("12345.6789")
            assert proxy.echo_decimal(value=v) == v

    def test_echo_decimal_negative(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            v = Decimal("-99999999999999.9999")
            assert proxy.echo_decimal(value=v) == v

    def test_echo_large_string(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            s = "wide" * 1000
            assert proxy.echo_large_string(value=s) == s

    def test_echo_large_binary(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            b = b"\x00\x01\x02\x03" * 1000
            assert proxy.echo_large_binary(value=b) == b

    def test_echo_fixed_binary(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            b = b"12345678"
            assert proxy.echo_fixed_binary(value=b) == b

    def test_echo_container_wide_types(self, conformance_conn: ConnFactory) -> None:
        """Wide types nested in list/dict/optional positions round-trip."""
        c = ContainerWideTypes(
            list_decimal=[Decimal("1.0000"), Decimal("2.5000"), Decimal("-3.7500")],
            list_date=[_dt.date(2026, 1, 1), _dt.date(2026, 4, 25)],
            list_timestamp=[
                _dt.datetime(2026, 1, 1, 0, 0, 0),
                _dt.datetime(2026, 4, 25, 12, 0, 0, 123456),
            ],
            optional_date=None,
            optional_decimal=Decimal("99.0000"),
            optional_timestamp=None,
            dict_str_decimal={"a": Decimal("1.5000"), "b": Decimal("2.5000")},
            frozenset_int=frozenset([1, 2, 3]),
            list_optional_int=[1, None, 3, None, 5],
        )
        with conformance_conn() as proxy:
            result = proxy.echo_container_wide_types(data=c)
            assert result == c
            assert result.optional_date is None
            assert result.optional_timestamp is None
            assert None in result.list_optional_int

    def test_echo_container_wide_types_all_present(self, conformance_conn: ConnFactory) -> None:
        """Optional fields populated round-trip just as None ones do."""
        c = ContainerWideTypes(
            list_decimal=[Decimal("0.0001")],
            list_date=[_dt.date(1970, 1, 1)],
            list_timestamp=[_dt.datetime(1970, 1, 1, 0, 0, 0)],
            optional_date=_dt.date(2026, 4, 25),
            optional_decimal=Decimal("-12345.6789"),
            optional_timestamp=_dt.datetime(2026, 4, 25, 12, 0, 0, 123456),
            dict_str_decimal={},
            frozenset_int=frozenset(),
            list_optional_int=[],
        )
        with conformance_conn() as proxy:
            assert proxy.echo_container_wide_types(data=c) == c

    def test_echo_deep_nested(self, conformance_conn: ConnFactory) -> None:
        """Multi-level nested containers and dictionary-encoded strings round-trip."""
        d = DeepNested(
            list_of_lists_decimal=[[Decimal("1.0000"), Decimal("2.0000")], [Decimal("3.5000")], []],
            optional_list_date=[_dt.date(2026, 1, 1), _dt.date(2026, 4, 25)],
            dict_encoded_string="hello",
            list_of_dict_encoded=["a", "b", "a", "c", "a"],
        )
        with conformance_conn() as proxy:
            assert proxy.echo_deep_nested(data=d) == d

    def test_echo_deep_nested_empty_and_none(self, conformance_conn: ConnFactory) -> None:
        """Empty inner lists and ``None`` optional list round-trip."""
        d = DeepNested(
            list_of_lists_decimal=[],
            optional_list_date=None,
            dict_encoded_string="",
            list_of_dict_encoded=[],
        )
        with conformance_conn() as proxy:
            result = proxy.echo_deep_nested(data=d)
            assert result == d
            assert result.optional_list_date is None
            assert result.dict_encoded_string == ""

    def test_echo_dict_encoded_string(self, conformance_conn: ConnFactory) -> None:
        """Dictionary-encoded string column round-trips as a plain ``str``."""
        with conformance_conn() as proxy:
            assert proxy.echo_dict_encoded_string(value="hello") == "hello"
            assert proxy.echo_dict_encoded_string(value="") == ""
            assert proxy.echo_dict_encoded_string(value="🌍" * 100) == "🌍" * 100

    def test_echo_large_string_empty(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_large_string(value="") == ""

    def test_echo_large_binary_empty(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            assert proxy.echo_large_binary(value=b"") == b""

    def test_echo_fixed_binary_zero_bytes(self, conformance_conn: ConnFactory) -> None:
        """Fixed-size binary with all-zero bytes (a common boundary case)."""
        with conformance_conn() as proxy:
            assert proxy.echo_fixed_binary(value=b"\x00" * 8) == b"\x00" * 8

    def test_echo_embedded_arrow(self, conformance_conn: ConnFactory) -> None:
        """``pa.RecordBatch`` and ``pa.Schema`` fields round-trip via nested IPC binary."""
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        fields: list[pa.Field[pa.DataType]] = [pa.field("x", pa.int32()), pa.field("y", pa.string())]
        schema = pa.schema(fields)
        e = EmbeddedArrow(batch=batch, schema=schema)
        with conformance_conn() as proxy:
            result = proxy.echo_embedded_arrow(data=e)
            assert result.batch.equals(batch)
            assert result.schema.equals(schema)

    def test_echo_wide_types_dataclass(self, conformance_conn: ConnFactory) -> None:
        """Round-trip every wide-type field via a single dataclass call."""
        w = WideTypes(
            int8_field=-12,
            int16_field=-30000,
            int32_field=2_000_000_000,
            uint8_field=200,
            uint16_field=60000,
            uint32_field=4_000_000_000,
            uint64_field=18_000_000_000_000_000_000,
            float32_field=1.5,
            date_field=_dt.date(2026, 4, 25),
            timestamp_field=_dt.datetime(2026, 4, 25, 12, 0, 0, 123456),
            timestamp_utc_field=_dt.datetime(2026, 4, 25, 12, 0, 0, 123456, tzinfo=_dt.UTC),
            time_field=_dt.time(12, 30, 45, 123456),
            duration_field=_dt.timedelta(seconds=10, microseconds=500),
            decimal_field=Decimal("12345.6789"),
            large_string_field="wide string",
            large_binary_field=b"wide bytes",
            fixed_binary_field=b"12345678",
        )
        with conformance_conn() as proxy:
            assert proxy.echo_wide_types(data=w) == w


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
# Protocol-Level Errors (standardized across transports)
# ---------------------------------------------------------------------------


class TestProtocolErrors:
    """Errors raised by the framework itself (not by user method bodies).

    These behaviors must be identical across all transports so that callers
    can rely on consistent error_type/message shape regardless of pipe vs
    HTTP vs subprocess.
    """

    def test_unknown_method_raises_attribute_error(self, conformance_conn: ConnFactory) -> None:
        """Calling a method not in the protocol raises AttributeError with a stable message."""
        with (
            conformance_conn() as proxy,
            pytest.raises(AttributeError, match="has no RPC method 'definitely_not_a_method'"),
        ):
            proxy.definitely_not_a_method()

    def test_none_for_required_param_raises_type_error(self, conformance_conn: ConnFactory) -> None:
        """Passing None to a non-Optional parameter raises TypeError before any RPC happens."""
        with (
            conformance_conn() as proxy,
            pytest.raises(TypeError, match="parameter 'message' is not optional but got None"),
        ):
            proxy.raise_value_error(message=None)


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

    def test_echo_with_all_log_levels(self, conformance_conn: ConnFactory) -> None:
        """Every non-EXCEPTION ``Level`` round-trips with its level value preserved.

        Other tests cover DEBUG/INFO/WARN; this one closes the gap on
        TRACE and ERROR — all five in one call so order is also asserted.
        ``Level.EXCEPTION`` is reserved on the wire (raises ``RpcError``
        client-side) and is therefore not deliverable as an in-band log.
        """
        logs: list[Message] = []
        with conformance_conn(on_log=logs.append) as proxy:
            result = proxy.echo_with_all_log_levels(value="lvl")
            assert result == "lvl"
        expected = [Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR]
        assert [m.level for m in logs] == expected
        for msg, level in zip(logs, expected, strict=True):
            assert msg.message == f"{level.value.lower()}: lvl"


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

    def test_zero_column_exchange(self, conformance_conn: ConnFactory) -> None:
        """Exchange stream with zero-column batches works over 100 iterations."""
        empty_schema = pa.schema([])
        empty_input = AnnotatedBatch(batch=pa.record_batch([], schema=empty_schema))
        with conformance_conn() as proxy, proxy.exchange_zero_columns() as session:
            for _ in range(100):
                output = session.exchange(empty_input)
                assert output.batch.schema == empty_schema
                assert output.batch.num_rows == 0
                assert output.batch.num_columns == 0

    def test_zero_row_input(self, conformance_conn: ConnFactory) -> None:
        """Send zero-row batch to exchange."""
        with conformance_conn() as proxy, proxy.exchange_scale(factor=2.0) as session:
            schema = pa.schema([pa.field("value", pa.float64())])
            empty = pa.RecordBatch.from_pydict({"value": pa.array([], type=pa.float64())}, schema=schema)
            out = session.exchange(AnnotatedBatch(batch=empty))
            assert out.batch.num_rows == 0


# ---------------------------------------------------------------------------
# Exchange Streams: Cast-Compatible Schemas
# ---------------------------------------------------------------------------


class TestExchangeCastCompatible:
    """Test that exchange streams cast compatible input schemas (e.g. int32 -> float64)."""

    def test_cast_int32_to_float64(self, conformance_conn: ConnFactory) -> None:
        """Send int32 values to a float64 exchange, expect float64 output."""
        with conformance_conn() as proxy, proxy.exchange_cast_compatible() as session:
            batch = pa.record_batch(
                [pa.array([1, 2, 3], type=pa.int32())],
                schema=pa.schema([pa.field("value", pa.int32())]),
            )
            out = session.exchange(AnnotatedBatch(batch=batch))
            assert out.batch.schema.field("value").type == pa.float64()
            assert out.batch.column("value").to_pylist() == [
                pytest.approx(1.0),
                pytest.approx(2.0),
                pytest.approx(3.0),
            ]

    def test_cast_int64_to_float64(self, conformance_conn: ConnFactory) -> None:
        """Send int64 values to a float64 exchange, expect float64 output."""
        with conformance_conn() as proxy, proxy.exchange_cast_compatible() as session:
            batch = pa.record_batch(
                [pa.array([10, 20, 30], type=pa.int64())],
                schema=pa.schema([pa.field("value", pa.int64())]),
            )
            out = session.exchange(AnnotatedBatch(batch=batch))
            assert out.batch.schema.field("value").type == pa.float64()
            assert out.batch.column("value").to_pylist() == [
                pytest.approx(10.0),
                pytest.approx(20.0),
                pytest.approx(30.0),
            ]

    def test_cast_float32_to_float64(self, conformance_conn: ConnFactory) -> None:
        """Send float32 values to a float64 exchange, expect float64 output."""
        with conformance_conn() as proxy, proxy.exchange_cast_compatible() as session:
            batch = pa.record_batch(
                [pa.array([1.5, 2.5, 3.5], type=pa.float32())],
                schema=pa.schema([pa.field("value", pa.float32())]),
            )
            out = session.exchange(AnnotatedBatch(batch=batch))
            assert out.batch.schema.field("value").type == pa.float64()
            assert out.batch.column("value").to_pylist() == [
                pytest.approx(1.5),
                pytest.approx(2.5),
                pytest.approx(3.5),
            ]

    def test_cast_exact_schema(self, conformance_conn: ConnFactory) -> None:
        """Send matching float64 values — no cast needed."""
        with conformance_conn() as proxy, proxy.exchange_cast_compatible() as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0, 10.0]}))
            assert out.batch.column("value").to_pylist() == [pytest.approx(5.0), pytest.approx(10.0)]

    def test_cast_incompatible_column_name(self, conformance_conn: ConnFactory) -> None:
        """Send wrong column name, expect RpcError."""
        with conformance_conn() as proxy, proxy.exchange_cast_compatible() as session:
            batch = pa.record_batch(
                [pa.array([1.0], type=pa.float64())],
                schema=pa.schema([pa.field("wrong", pa.float64())]),
            )
            with pytest.raises(RpcError):
                session.exchange(AnnotatedBatch(batch=batch))


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
# Connection Reuse
# ---------------------------------------------------------------------------


class TestConnectionReuse:
    """Many calls over a single connection.

    On a persistent transport (pipe / subprocess / Unix socket) one connection
    carries every call, each framed as its own Arrow IPC stream
    ``[schema][batch...][EOS]``. A client must fully consume each response —
    through its trailing EOS marker — before issuing the next call, or the
    next call's reader sees the stale EOS and fails (e.g. "Unexpected end of
    input. Missing schema"). The first call always succeeds; a client that
    skips the drain fails on the second. These tests pin that requirement.
    """

    def test_many_sequential_unary_calls(self, conformance_conn: ConnFactory) -> None:
        """A long run of consecutive unary calls on one connection all succeed."""
        with conformance_conn() as proxy:
            for i in range(25):
                assert proxy.echo_string(value=f"call-{i}") == f"call-{i}"
                assert proxy.echo_int(value=i) == i
                assert proxy.add_floats(a=float(i), b=0.5) == pytest.approx(i + 0.5)
                assert proxy.echo_point(point=Point(x=float(i), y=-float(i))) == Point(x=float(i), y=-float(i))

    def test_optional_absence_round_trips_repeatedly(self, conformance_conn: ConnFactory) -> None:
        """Absent optionals stay absent across many reused-connection calls.

        A correct client round-trips an absent optional as the language's
        "absent" value (Python ``None``) every time — never degrading to a
        present-but-empty value, and never poisoning a later call.
        """
        with conformance_conn() as proxy:
            for i in range(15):
                assert proxy.echo_optional_string(value=None) is None
                assert proxy.echo_optional_string(value=f"v{i}") == f"v{i}"
                assert proxy.echo_optional_int(value=None) is None
                assert proxy.echo_optional_int(value=i) == i
            # A trailing plain unary call must still work after the run.
            assert proxy.echo_string(value="done") == "done"


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class TestCancel:
    """Test client-initiated stream cancellation via ``cancel()``."""

    def test_cancel_producer_mid_stream(self, conformance_conn: ConnFactory) -> None:
        """Cancelling a producer mid-stream fires on_cancel() on the server."""
        with conformance_conn() as proxy:
            proxy.reset_cancel_probe()
            session = proxy.cancellable_producer()
            it = iter(session)
            for _ in range(3):
                next(it)
            session.cancel()
            produce_calls, _, on_cancel_calls = proxy.cancel_probe_counters()
            assert on_cancel_calls == 1
            assert produce_calls >= 3
            assert proxy.echo_int(value=42) == 42

    def test_cancel_exchange_after_n(self, conformance_conn: ConnFactory) -> None:
        """Cancelling after N exchanges stops further processing and fires on_cancel()."""
        with conformance_conn() as proxy:
            proxy.reset_cancel_probe()
            session = proxy.cancellable_exchange()
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            session.cancel()
            _, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
            assert exchange_calls == 2
            assert on_cancel_calls == 1

    def test_cancel_before_any_exchange(self, conformance_conn: ConnFactory) -> None:
        """Cancelling before any exchange leaves the transport usable."""
        with conformance_conn() as proxy:
            proxy.reset_cancel_probe()
            session = proxy.cancellable_exchange()
            session.cancel()
            _, exchange_calls, _ = proxy.cancel_probe_counters()
            assert exchange_calls == 0
            assert proxy.echo_int(value=1) == 1

    def test_cancel_idempotent(self, conformance_conn: ConnFactory) -> None:
        """cancel(), cancel() and close()+cancel() and cancel()+close() are all safe."""
        with conformance_conn() as proxy:
            proxy.reset_cancel_probe()
            s1 = proxy.cancellable_exchange()
            s1.cancel()
            s1.cancel()
            s2 = proxy.cancellable_exchange()
            s2.close()
            s2.cancel()
            s3 = proxy.cancellable_exchange()
            s3.cancel()
            s3.close()
            _, _, on_cancel_calls = proxy.cancel_probe_counters()
            # HTTP cancel POSTs only when a state token is live; tolerate both counts.
            assert on_cancel_calls in (2, 3)

    def test_exchange_after_cancel_raises(self, conformance_conn: ConnFactory) -> None:
        """Using an exchange session after cancel() raises ProtocolError."""
        with conformance_conn() as proxy:
            session = proxy.cancellable_exchange()
            session.cancel()
            with pytest.raises(RpcError, match="ProtocolError"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [99.0]}))

    def test_transport_reusable_after_cancel(self, conformance_conn: ConnFactory) -> None:
        """After mid-stream cancel, subsequent RPC calls succeed."""
        with conformance_conn() as proxy:
            session = proxy.cancellable_producer()
            it = iter(session)
            for _ in range(2):
                next(it)
            session.cancel()
            assert len(list(proxy.produce_n(count=3))) == 3
            assert proxy.echo_string(value="ok") == "ok"

    def test_cancel_can_be_issued(self, conformance_conn: ConnFactory) -> None:
        """Smoke test: ``cancel()`` is accepted by every transport.

        Minimal contract check that does not depend on the cancel-probe
        counters: open a producer stream, pull one batch, call ``cancel()``,
        and verify a follow-up unary RPC succeeds. Any transport that
        cannot route the cancel notification fails here.
        """
        with conformance_conn() as proxy:
            session = proxy.cancellable_producer()
            it = iter(session)
            next(it)
            session.cancel()
            assert proxy.echo_int(value=7) == 7


# ---------------------------------------------------------------------------
# External-location / large-batch externalization (HTTP-only)
# ---------------------------------------------------------------------------


class TestExternalLocation:
    """End-to-end coverage of the external-location feature.

    These tests run the conformance HTTP worker with a fake-storage
    backend (``vgi_rpc.conformance.fake_storage``) so server-side batches
    above the configured threshold are uploaded as zero-row pointer
    batches with ``vgi_rpc.location`` metadata, and the client
    transparently re-fetches the bytes.

    Required runner fixtures:

    * ``conformance_http_with_storage_port`` — port of an HTTP worker
      wired against the fake storage with no compression.
    * ``conformance_http_with_zstd_storage_port`` — same, but with zstd
      compression enabled on externalized batches.
    """

    @staticmethod
    def _client_external_config() -> object:
        """Build an ``ExternalLocationConfig`` that allows http URLs."""
        from vgi_rpc.external import ExternalLocationConfig

        return ExternalLocationConfig(url_validator=None)

    def test_small_payload_inline(self, conformance_http_with_storage_port: int) -> None:
        """Below-threshold payloads round-trip inline (no externalization)."""
        from vgi_rpc.http import http_connect

        with http_connect(
            ConformanceService,
            f"http://127.0.0.1:{conformance_http_with_storage_port}",
            external_location=self._client_external_config(),  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        ) as proxy:
            assert proxy.echo_string(value="small") == "small"

    def test_large_payload_externalized(self, conformance_http_with_storage_port: int) -> None:
        """Above-threshold response triggers externalization and transparent re-fetch."""
        from vgi_rpc.http import http_connect

        big = "x" * 32_000
        with http_connect(
            ConformanceService,
            f"http://127.0.0.1:{conformance_http_with_storage_port}",
            external_location=self._client_external_config(),  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        ) as proxy:
            assert proxy.echo_large_string(value=big) == big

    def test_large_payload_externalized_with_zstd(self, conformance_http_with_zstd_storage_port: int) -> None:
        """Externalized batches with zstd compression decompress on the client."""
        from vgi_rpc.http import http_connect

        big = ("vgi-rpc " * 8000).strip()
        with http_connect(
            ConformanceService,
            f"http://127.0.0.1:{conformance_http_with_zstd_storage_port}",
            external_location=self._client_external_config(),  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        ) as proxy:
            assert proxy.echo_large_string(value=big) == big

    def test_capabilities_advertise_externalization(self, conformance_http_with_storage_port: int) -> None:
        """The OPTIONS capabilities endpoint must advertise the externalization protocol.

        When the server is wired with an ``upload_url_provider`` and a
        ``max_request_bytes`` limit, ``http_capabilities()`` must report:
        ``upload_url_support=True``, ``max_request_bytes`` matching the
        worker's threshold, and a non-``None`` ``max_upload_bytes``.
        Together these tell a client when to externalize an outgoing
        batch and where the upload limit is.
        """
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(f"http://127.0.0.1:{conformance_http_with_storage_port}")
        assert caps.upload_url_support is True
        assert caps.max_request_bytes is not None and caps.max_request_bytes > 0
        assert caps.max_upload_bytes is not None and caps.max_upload_bytes > 0

    def test_request_upload_urls_returns_pairs(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """``request_upload_urls(count=N)`` returns N usable PUT/GET URL pairs.

        Asserts both the protocol shape (count, ``upload_url`` /
        ``download_url`` strings) and end-to-end usability: PUT to the
        upload URL succeeds and GET on the download URL returns the same
        bytes.  Also confirms that the upload registered against the
        backing fake-storage service via its ``/_stats`` counter.
        """
        import httpx

        from vgi_rpc.http import request_upload_urls

        before = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        urls = request_upload_urls(f"http://127.0.0.1:{conformance_http_with_storage_port}", count=2)
        assert len(urls) == 2
        for u in urls:
            assert u.upload_url and isinstance(u.upload_url, str)
            assert u.download_url and isinstance(u.download_url, str)

        payload = b"client-vended upload contents"
        put_resp = httpx.put(urls[0].upload_url, content=payload, timeout=5.0)
        assert put_resp.status_code == 204, f"PUT failed: {put_resp.status_code}"
        get_resp = httpx.get(urls[0].download_url, timeout=5.0)
        assert get_resp.status_code == 200
        assert get_resp.content == payload

        after = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        # Each request_upload_urls(count=2) call performs >=1 alloc per URL.
        assert after >= before + 1

    def test_externalization_uses_fake_storage(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Externalization actually deposits objects in the fake storage.

        Exercises the upload-side proof: after a large-payload RPC, the
        fake storage's ``/_stats`` endpoint must report at least one
        stored object (proving the server invoked the storage backend
        rather than inlining the response).
        """
        import httpx

        from vgi_rpc.http import http_connect

        before = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        big = "z" * 32_000
        with http_connect(
            ConformanceService,
            f"http://127.0.0.1:{conformance_http_with_storage_port}",
            external_location=self._client_external_config(),  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        ) as proxy:
            assert proxy.echo_large_string(value=big) == big
        after = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        assert after > before, f"expected new objects in fake storage, before={before} after={after}"

    def test_client_to_server_auto_externalization(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """End-to-end: client externalizes a large *request* via server-vended URL.

        The client sends a payload larger than the server's advertised
        ``max_request_bytes``.  The framework should:

        1. POST the inline body, get back ``413 Payload Too Large``.
        2. Discover capabilities (``upload_url_support`` is True).
        3. Call ``__upload_url__/init``, PUT the bytes to the vended URL.
        4. Re-POST a pointer batch carrying ``vgi_rpc.location``.
        5. Server resolves the pointer in ``_read_request`` (stage 1)
           and dispatches with the reconstructed parameters.

        Asserts the round-trip succeeds (proving 1-5 worked) and that
        the fake-storage object count grew by at least 2 (client's
        request upload + server's response upload).
        """
        import os

        import httpx

        from vgi_rpc.http import http_connect

        before = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        # Use a high-entropy payload (hex-encoded random bytes) so wire
        # compression can't shrink it under max_request_bytes.  Disable
        # request compression on this connection so the *wire* body
        # stays large enough to trip the server's 413 enforcement.
        big = os.urandom(20_000).hex()  # 40 000 chars of pseudo-random hex
        with http_connect(
            ConformanceService,
            f"http://127.0.0.1:{conformance_http_with_storage_port}",
            external_location=self._client_external_config(),  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
            compression_level=None,
        ) as proxy:
            assert proxy.echo_large_string(value=big) == big
        after = httpx.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        # At least 2 new objects: client's request upload + server's response upload.
        assert after >= before + 2, f"expected ≥2 new objects (1 request + 1 response), before={before} after={after}"


# ---------------------------------------------------------------------------
# Health endpoint (HTTP-only)
# ---------------------------------------------------------------------------


class TestHealth:
    """HTTP ``GET /health`` contract — every implementation MUST honor it.

    These tests bypass the RPC proxy and hit the HTTP endpoint directly.
    They depend on two session-scoped fixtures that each runner is
    expected to provide:

    * ``conformance_http_port`` — a normal (no-auth) HTTP conformance server.
    * ``conformance_http_auth_port`` — an HTTP conformance server with a
      reject-all ``authenticate`` callback installed; every RPC endpoint
      returns 401, but ``GET /health`` must still return 200.
    """

    def test_health_endpoint_returns_ok(self, conformance_http_port: int) -> None:
        """``GET /health`` returns 200 with JSON ``{status, server_id, protocol}``."""
        import httpx

        resp = httpx.get(f"http://127.0.0.1:{conformance_http_port}/health", timeout=5.0)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert isinstance(body["server_id"], str) and body["server_id"]
        assert body["protocol"] == "ConformanceService"

    def test_health_does_not_require_auth(self, conformance_http_auth_port: int) -> None:
        """``GET /health`` must succeed even when every RPC endpoint requires auth.

        Sanity-checks the auth-enforcing fixture by also asserting that an
        unauthenticated unary RPC POST is rejected — otherwise the health
        assertion would be a false positive.
        """
        import httpx

        url = f"http://127.0.0.1:{conformance_http_auth_port}"
        health = httpx.get(f"{url}/health", timeout=5.0)
        assert health.status_code == 200, f"health must bypass auth, got {health.status_code}"
        assert health.json()["status"] == "ok"

        # Runners mount RPC at different prefixes, and they differ in whether
        # auth runs before or after routing: a port that authenticates first
        # returns 401 for any path (so asserting one fixed path proves nothing
        # about auth), while a port that routes first returns 404 for a path it
        # does not serve. Probe both candidate layouts and require that the real
        # endpoint — wherever it lives — is refused, and that neither is served.
        candidates = {
            path: httpx.post(f"{url}{path}", content=b"", timeout=5.0).status_code
            for path in ("/echo_int", "/vgi/echo_int")
        }
        assert 200 not in candidates.values(), f"RPC must not be served unauthenticated: {candidates}"
        assert 401 in candidates.values(), f"expected an RPC endpoint to require auth, got {candidates}"


class TestUnauthorized:
    """Standardized 401 contract — see ``docs/unauthorized-spec.md``.

    Runs against ``conformance_http_auth_port``, the same reject-all worker
    ``TestHealth`` uses. The two proxy-note tests additionally need a worker
    whose auth depends on a proxy, and borrow ``proof_worker_factory``.

    The whole group is capability-gated on the server emitting
    ``VGI-Auth-Reason``, so a port that has not adopted the contract skips
    rather than fails. Every test that asserts on a rejection first confirms
    the endpoint really is gated — a fixture pointed at a path that does not
    exist would otherwise satisfy these assertions for the wrong reason.
    """

    @staticmethod
    def _post(port: int, accept: str | None = None, want_reason: str | None = None) -> Any:
        """POST a well-formed unary body to whichever RPC path the runner serves.

        Runners mount RPC at different prefixes and differ in whether auth
        runs before or after routing, so both candidate layouts are probed and
        the one that answers 401 is used.

        Args:
            port: Port of the auth-enforcing conformance worker.
            accept: Value for the ``Accept`` header, or ``None`` to send none.
            want_reason: Reason code to request via
                ``X-Conformance-Auth-Reason``, or ``None`` to send none.

        Returns:
            The ``httpx.Response`` for the gated endpoint.

        """
        import httpx

        headers = {"content-type": "application/vnd.apache.arrow.stream"}
        if accept is not None:
            headers["accept"] = accept
        if want_reason is not None:
            headers[_CONFORMANCE_REASON_HEADER] = want_reason
        body = _unary_request_body("echo_int", value=1)
        last: Any = None
        for path in ("/echo_int", "/vgi/echo_int"):
            last = httpx.post(f"http://127.0.0.1:{port}{path}", content=body, headers=headers, timeout=5.0)
            if last.status_code == 401:
                return last
        pytest.fail(f"no RPC endpoint returned 401 on port {port}; last status {last.status_code}")

    def _gated(self, port: int, accept: str | None = None) -> Any:
        """Return a 401 response, skipping the test if the port predates this contract."""
        resp = self._post(port, accept)
        if _AUTH_REASON_HEADER.lower() not in resp.headers:
            pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
        return resp

    def test_reason_header_present(self, conformance_http_auth_port: int) -> None:
        """Every 401 carries a machine-readable reason code."""
        resp = self._gated(conformance_http_auth_port)
        assert resp.headers[_AUTH_REASON_HEADER.lower()]

    def test_reason_in_closed_set(self, conformance_http_auth_port: int) -> None:
        """The code comes from the closed set, so a client can switch on it."""
        resp = self._gated(conformance_http_auth_port)
        reason = resp.headers[_AUTH_REASON_HEADER.lower()]
        assert reason in _AUTH_REASONS, f"{reason!r} is not one of {sorted(_AUTH_REASONS)}"

    @staticmethod
    def _reason_port(request: pytest.FixtureRequest) -> int:
        """Port of a worker that honours the reason-request header, or skip."""
        try:
            port: int = request.getfixturevalue("conformance_http_auth_reason_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_auth_reason_port")
        return port

    @pytest.mark.parametrize("want", _REQUESTABLE_REASONS)
    def test_requested_reason_is_honoured(self, request: pytest.FixtureRequest, want: str) -> None:
        """The server reports the reason that actually applies, not a constant.

        Membership in the closed set is not enough on its own: a server that
        answers every 401 with ``unauthorized`` satisfies it. This is what
        makes the code worth branching on — a client that refreshes a token
        on ``expired_credential`` and gives up on ``insufficient_scope``
        needs the two to be told apart.
        """
        resp = self._post(self._reason_port(request), accept="*/*", want_reason=want)
        if _AUTH_REASON_HEADER.lower() not in resp.headers:
            pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
        assert resp.headers[_AUTH_REASON_HEADER.lower()] == want
        assert resp.json()["reason"] == want

    def test_unclassified_failure_is_unauthorized(self, request: pytest.FixtureRequest) -> None:
        """A rejection that names no reason lands on the fallback, not a guess.

        Guessing a finer code from an unclassified failure means matching on
        message text, which misclassifies the moment someone rewords a
        string — so the fallback has to be reachable and stable.
        """
        resp = self._post(self._reason_port(request), accept="*/*")
        if _AUTH_REASON_HEADER.lower() not in resp.headers:
            pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
        assert resp.headers[_AUTH_REASON_HEADER.lower()] == "unauthorized"

    def test_reason_codes_are_distinct(self, request: pytest.FixtureRequest) -> None:
        """Asserted in one place so a server collapsing codes cannot pass piecemeal.

        The per-reason tests above each fail on their own for a server that
        hardcodes one code, but only if the runner wires every parametrisation
        up. This states the property directly: N distinct requests, N distinct
        answers.
        """
        port = self._reason_port(request)
        first = self._post(port, accept="*/*", want_reason=_REQUESTABLE_REASONS[0])
        if _AUTH_REASON_HEADER.lower() not in first.headers:
            pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
        seen = {
            self._post(port, accept="*/*", want_reason=want).headers[_AUTH_REASON_HEADER.lower()]
            for want in _REQUESTABLE_REASONS
        }
        assert len(seen) == len(_REQUESTABLE_REASONS), f"reason codes are not discriminated: {sorted(seen)}"

    def test_proxy_required_is_not_request_driven(self, request: pytest.FixtureRequest) -> None:
        """A caller cannot summon the proxy reason on a service with no proxy.

        §5 derives the proxy note from server configuration precisely so that
        it says the same thing on every 401. A server that let the request
        steer it would be advertising a proxy dependency that does not exist.
        """
        resp = self._post(self._reason_port(request), accept="*/*", want_reason="proxy_required")
        if _AUTH_REASON_HEADER.lower() not in resp.headers:
            pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
        assert resp.headers[_AUTH_REASON_HEADER.lower()] != "proxy_required"
        assert _AUTH_PROXY_REQUIRED_HEADER.lower() not in resp.headers

    def test_json_envelope_for_machine_clients(self, conformance_http_auth_port: int) -> None:
        """A client that does not ask for HTML gets the JSON envelope."""
        resp = self._gated(conformance_http_auth_port, accept="*/*")
        assert resp.headers["content-type"].startswith("application/json"), resp.headers["content-type"]
        body = resp.json()
        assert body["error"] == "unauthorized"
        assert body["reason"] in _AUTH_REASONS
        assert isinstance(body["detail"], str)
        # Header and body must agree, or a client reading one and logging the
        # other reports two different stories about the same rejection.
        assert body["reason"] == resp.headers[_AUTH_REASON_HEADER.lower()]

    def test_html_page_for_browsers(self, conformance_http_auth_port: int) -> None:
        """A browser gets a page, and the reason header survives negotiation.

        Rendering a page is optional; answering an HTML request with JSON is
        allowed, answering a JSON request with HTML is not. So this asserts
        the content type is one of the two, and that the header — the part a
        client actually parses — is there either way.
        """
        resp = self._gated(conformance_http_auth_port, accept="text/html,application/xhtml+xml")
        content_type = resp.headers["content-type"]
        assert content_type.startswith(("text/html", "application/json")), content_type
        assert resp.headers[_AUTH_REASON_HEADER.lower()] in _AUTH_REASONS

    def test_not_cached(self, conformance_http_auth_port: int) -> None:
        """A 401 must not be held by a shared cache — the next attempt may be a 200."""
        resp = self._gated(conformance_http_auth_port)
        cache_control = resp.headers.get("cache-control", "")
        assert "no-store" in cache_control or "no-cache" in cache_control, f"got {cache_control!r}"

    def test_no_proxy_note_without_proxy_auth(self, conformance_http_auth_port: int) -> None:
        """A service with no proxy dependency must stay quiet about proxies.

        A note that shows up on every service is a note operators learn to
        skip, which costs exactly the deployments it exists to help.
        """
        resp = self._gated(conformance_http_auth_port, accept="*/*")
        assert _AUTH_PROXY_REQUIRED_HEADER.lower() not in resp.headers
        assert "proxy_hint" not in resp.json()

    def test_proxy_note_when_proxy_required(self, request: pytest.FixtureRequest) -> None:
        """A require-mode proxy-proof worker explains that a proxy is involved."""
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with TestProxyProof._factory(request)(ProofWorkerConfig()) as worker:
            resp = self._proof_post(worker, token=None)
            if _AUTH_REASON_HEADER.lower() not in resp.headers:
                pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")
            assert resp.status_code == 401
            assert resp.headers[_AUTH_PROXY_REQUIRED_HEADER.lower()] == "true"
            assert resp.headers[_AUTH_REASON_HEADER.lower()] == "proxy_required"
            assert resp.json()["proxy_hint"], "proxy_hint must be present and non-empty"

    def test_proxy_rejection_is_uniform(self, request: pytest.FixtureRequest) -> None:
        """Absent, malformed, and bad-MAC proofs are indistinguishable to the caller.

        This is ``docs/proxy-proof-spec.md`` §6 asserted from the outside: the
        reason code names the stage, never the verifier's diagnosis, so an
        attacker cannot use the response to tell which check they tripped.
        """
        from vgi_rpc.conformance.proof_harness import CONFORMANCE_KID, ProofWorkerConfig, secret_bytes
        from vgi_rpc.http import mint_proof

        with TestProxyProof._factory(request)(ProofWorkerConfig()) as worker:
            good = mint_proof(secret_bytes(), CONFORMANCE_KID, worker.config.origin_id)
            # Positive control: without it, a worker that refuses everything
            # would pass this test by refusing everything identically.
            assert self._proof_post(worker, token=good).status_code == 200

            tampered = good[:-1] + ("A" if good[-1] != "A" else "B")
            responses = [
                self._proof_post(worker, token=None),
                self._proof_post(worker, token="not-a-proof"),
                self._proof_post(worker, token=tampered),
            ]
            if _AUTH_REASON_HEADER.lower() not in responses[0].headers:
                pytest.skip(f"server does not emit {_AUTH_REASON_HEADER}")

            reasons = {r.headers[_AUTH_REASON_HEADER.lower()] for r in responses}
            assert reasons == {"proxy_required"}, f"reason leaked the verifier's diagnosis: {reasons}"
            details = {r.json()["detail"] for r in responses}
            assert len(details) == 1, f"detail distinguishes proof failures: {details}"

    @staticmethod
    def _proof_post(worker: ProofWorker, *, token: str | None) -> Any:
        """POST a well-formed unary body to a proof worker, optionally with a proof."""
        import httpx

        headers = {"content-type": "application/vnd.apache.arrow.stream", "accept": "*/*"}
        if token is not None:
            headers[_PROOF_HEADER] = token
        return httpx.post(
            worker.rpc_url("echo_int"),
            content=_unary_request_body("echo_int", value=1),
            headers=headers,
            timeout=5.0,
        )


class TestProxyProof:
    """Proxy-proof contract — see ``docs/proxy-proof-spec.md``.

    A runner that supports this feature supplies one session-scoped fixture,
    ``proof_worker_factory``: a callable taking a
    :class:`~vgi_rpc.conformance.proof_harness.ProofWorkerConfig` and
    returning a context manager yielding a
    :class:`~vgi_rpc.conformance.proof_harness.ProofWorker`. Runners without
    the feature omit the fixture and the whole group skips.

    Every rejection test asserts a paired positive control against the same
    worker and URL. Roughly two thirds of these assert 401, so without that
    control a misconfigured fixture — or a URL that simply does not exist —
    would make them all pass for the wrong reason.
    """

    @staticmethod
    def _factory(request: pytest.FixtureRequest) -> ProofWorkerFactory:
        try:
            factory: ProofWorkerFactory = request.getfixturevalue("proof_worker_factory")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no proof_worker_factory")
        return factory

    @staticmethod
    def _post(worker: ProofWorker, token: str | None, method: str = "echo_int") -> int:
        """POST a real, well-formed RPC body, returning the status code.

        A well-formed body matters: posting empty content to a path that does
        not exist also yields a rejection, which would make these assertions
        vacuous.
        """
        import httpx

        headers = {"content-type": "application/vnd.apache.arrow.stream"}
        if token is not None:
            headers[_PROOF_HEADER] = token
        resp = httpx.post(
            worker.rpc_url(method),
            content=_unary_request_body(method, value=1),
            headers=headers,
            timeout=5.0,
        )
        return resp.status_code

    def _mint(self, worker: ProofWorker, *, now: int | None = None, nonce: str | None = None) -> str:
        from vgi_rpc.conformance.proof_harness import CONFORMANCE_KID, secret_bytes
        from vgi_rpc.http import mint_proof

        cfg = worker.config
        return mint_proof(secret_bytes(), CONFORMANCE_KID, cfg.origin_id, now=now, nonce=nonce)

    def test_valid_proof_accepted(self, request: pytest.FixtureRequest) -> None:
        """A proof minted by an independent implementation is accepted.

        The token is built here, not by the worker, so a port whose canonical
        string is framed differently fails — which round-tripping inside one
        implementation could never reveal.
        """
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            assert self._post(worker, self._mint(worker)) == 200

    def test_missing_proof_rejected(self, request: pytest.FixtureRequest) -> None:
        """Require mode refuses a request with no proof, and accepts one with."""
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            assert self._post(worker, self._mint(worker)) == 200, "positive control failed"
            assert self._post(worker, None) == 401

    @pytest.mark.parametrize(
        "token",
        [
            "",
            "garbage",
            "v1.a.b.c",
            "v1.a.b.c.d.e",
            "v2.conformance-proxy.1.AAAAAAAAAAAAAAAAAAAAAA." + "A" * 43,
            "v1.bad!kid.1.AAAAAAAAAAAAAAAAAAAAAA." + "A" * 43,
            "v1.conformance-proxy.notanumber.AAAAAAAAAAAAAAAAAAAAAA." + "A" * 43,
            "v1.conformance-proxy.1.short." + "A" * 43,
        ],
    )
    def test_malformed_rejected_not_500(self, request: pytest.FixtureRequest, token: str) -> None:
        """Malformed tokens are refused as 401, never 500.

        A 5xx on adversarial input is a conformance failure in its own
        right: it means the parser raised somewhere it was not expected to.
        """
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            status = self._post(worker, token)
            assert status != 500, f"malformed token produced a server error: {token!r}"
            assert status == 401

    def test_tampered_fields_rejected(self, request: pytest.FixtureRequest) -> None:
        """Mutating any signed field invalidates the proof."""
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            base = self._mint(worker, now=None, nonce="AAAAAAAAAAAAAAAAAAAAAA")
            parts = base.split(".")
            for index, replacement in ((2, "1"), (3, "BBBBBBBBBBBBBBBBBBBBBB"), (4, "A" * 43)):
                mutated = list(parts)
                mutated[index] = replacement
                assert self._post(worker, ".".join(mutated)) == 401, f"field {index} tamper accepted"

    def test_wrong_origin_rejected(self, request: pytest.FixtureRequest) -> None:
        """A proof minted for another worker does not verify here.

        Audience binding: the origin id is folded into the MAC but never
        transmitted, so it cannot be adjusted by the caller.
        """
        from vgi_rpc.conformance.proof_harness import CONFORMANCE_KID, ProofWorkerConfig, secret_bytes
        from vgi_rpc.http import mint_proof

        with self._factory(request)(ProofWorkerConfig()) as worker:
            assert self._post(worker, self._mint(worker)) == 200, "positive control failed"
            foreign = mint_proof(secret_bytes(), CONFORMANCE_KID, "some-other-worker")
            assert self._post(worker, foreign) == 401

    def test_expired_and_future_rejected(self, request: pytest.FixtureRequest) -> None:
        """Both ends of the timestamp window are enforced.

        The future case is the one that catches a verifier checking only an
        upper bound, which would let a future-dated proof pass indefinitely.
        """
        import time

        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            skew = worker.config.skew_seconds
            now = int(time.time())
            assert self._post(worker, self._mint(worker)) == 200, "positive control failed"
            assert self._post(worker, self._mint(worker, now=now - skew - 60)) == 401, "expired accepted"
            assert self._post(worker, self._mint(worker, now=now + skew + 60)) == 401, "future accepted"

    def test_replayed_nonce_rejected(self, request: pytest.FixtureRequest) -> None:
        """A replayed proof is refused; a fresh nonce at the same second is not."""
        import time

        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            now = int(time.time())
            token = self._mint(worker, now=now, nonce="Q0ZPUk1BTkNFTk9OQ0UxMQ")
            assert self._post(worker, token) == 200
            assert self._post(worker, token) == 401, "replay accepted"
            other = self._mint(worker, now=now, nonce="Q0ZPUk1BTkNFTk9OQ0UyMg")
            assert self._post(worker, other) == 200, "distinct nonce in the same second must pass"

    def test_rotation_overlap(self, request: pytest.FixtureRequest) -> None:
        """Two keys verify simultaneously so a rotation loses no request."""
        from vgi_rpc.conformance.proof_harness import CONFORMANCE_KID, ProofWorkerConfig, secret_bytes
        from vgi_rpc.http import mint_proof

        config = ProofWorkerConfig().with_second_key()
        with self._factory(request)(config) as worker:
            origin = worker.config.origin_id
            old = mint_proof(secret_bytes(), CONFORMANCE_KID, origin)
            new = mint_proof(secret_bytes("22" * 32), "conformance-proxy-v2", origin)
            assert self._post(worker, old) == 200, "old key rejected during overlap"
            assert self._post(worker, new) == 200, "new key rejected during overlap"

    def test_unknown_kid_rejected(self, request: pytest.FixtureRequest) -> None:
        """A key id with no configured secret is refused."""
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig, secret_bytes
        from vgi_rpc.http import mint_proof

        with self._factory(request)(ProofWorkerConfig()) as worker:
            origin = worker.config.origin_id
            assert self._post(worker, mint_proof(secret_bytes(), "no-such-kid", origin)) == 401

    def test_health_and_options_exempt(self, request: pytest.FixtureRequest) -> None:
        """Health and CORS preflight stay reachable without a proof.

        Load-balancer probes reach the worker directly rather than through
        the proxy, so gating them would mark a healthy worker down.
        """
        import httpx

        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            assert self._post(worker, None) == 401, "positive control: RPC must be gated"
            # Runners differ in whether health is mounted under the RPC prefix
            # or at the root, so probe both. What matters is that it is
            # reachable somewhere and never gated, not where it lives.
            codes = {
                path: httpx.get(f"{worker.base_url}{path}", timeout=5.0).status_code
                for path in ("/health", f"{worker.prefix}/health")
            }
            # A 401 on the *other* candidate is not a gate failure: a port that
            # authenticates before routing returns 401 for any path it does not
            # serve. What must hold is that health answers somewhere without a
            # proof, while the RPC endpoint above does not.
            assert 200 in codes.values(), f"health must be reachable unproofed: {codes}"

    @staticmethod
    def _health_headers(worker: ProofWorker) -> dict[str, str]:
        """Return the response headers of whichever health path this runner serves.

        Runners mount health under the RPC prefix or at the root; probing both
        keeps the assertion about the header rather than about the layout.

        Args:
            worker: The spawned worker to probe.

        Returns:
            Lower-cased header names to values, from the first path answering 200.

        """
        import httpx

        for path in ("/health", f"{worker.prefix}/health"):
            resp = httpx.get(f"{worker.base_url}{path}", timeout=5.0)
            if resp.status_code == 200:
                return {k.lower(): v for k, v in resp.headers.items()}
        raise AssertionError("no health endpoint answered 200")

    def test_require_mode_advertises_the_capability(self, request: pytest.FixtureRequest) -> None:
        """A ``require``-mode worker advertises ``VGI-Proxy-Proof-Required``.

        Without this a proxy cannot tell an enforcing worker from one that
        silently ignores the header — which is exactly the misconfiguration
        that turns the whole feature into a no-op. The header carries no
        enforcement itself; it is how an operator confirms the rollout landed.
        """
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        with self._factory(request)(ProofWorkerConfig()) as worker:
            headers = self._health_headers(worker)
            value = headers.get(_PROOF_REQUIRED_HEADER.lower())
            assert value is not None, (
                f"require-mode worker must advertise {_PROOF_REQUIRED_HEADER}; got {sorted(headers)}"
            )
            assert value.lower() == "true", f"expected 'true', got {value!r}"

    def test_allow_mode_does_not_advertise_the_capability(self, request: pytest.FixtureRequest) -> None:
        """Only ``require`` advertises it — ``allow`` never denies, so it must not.

        The positive control lives in the previous test; if the header were
        emitted unconditionally that one would pass while this one fails,
        which is the point of asserting both postures.
        """
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        factory = self._factory(request)
        try:
            ctx = factory(ProofWorkerConfig(mode="allow"))
        except ProofUnsupported as exc:  # pragma: no cover - runner-dependent
            pytest.skip(str(exc))
        with ctx as worker:
            headers = self._health_headers(worker)
            assert _PROOF_REQUIRED_HEADER.lower() not in headers, (
                f"allow mode must not advertise {_PROOF_REQUIRED_HEADER}"
            )

    def test_allow_mode_does_not_deny(self, request: pytest.FixtureRequest) -> None:
        """Allow mode records the outcome but serves the request either way."""
        from vgi_rpc.conformance.proof_harness import ProofWorkerConfig

        factory = self._factory(request)
        try:
            ctx = factory(ProofWorkerConfig(mode="allow"))
        except ProofUnsupported as exc:  # pragma: no cover - runner-dependent
            pytest.skip(str(exc))
        with ctx as worker:
            assert self._post(worker, None) == 200, "allow mode must not deny"
            assert self._post(worker, self._mint(worker)) == 200


class TestProxyProofOffMode:
    """An unconfigured worker must be unaffected by the feature.

    Not gated on ``proof_worker_factory``: it runs against the ordinary
    conformance server on every runner from day one. If it is not green, then
    "opt-in, off by default" is already false somewhere.
    """

    def test_unconfigured_worker_accepts_without_a_proof(self, conformance_http_port: int) -> None:
        """No header, no gate, no change."""
        import httpx

        resp = httpx.post(
            f"http://127.0.0.1:{conformance_http_port}/echo_int",
            content=_unary_request_body("echo_int", value=1),
            headers={"content-type": "application/vnd.apache.arrow.stream"},
            timeout=5.0,
        )
        assert resp.status_code == 200

    def test_unconfigured_worker_ignores_a_bogus_proof_header(self, conformance_http_port: int) -> None:
        """A garbage proof header is ignored rather than parsed.

        Catches a port that installs the verifier whenever the header is
        present rather than whenever the feature is configured.
        """
        import httpx

        resp = httpx.post(
            f"http://127.0.0.1:{conformance_http_port}/echo_int",
            content=_unary_request_body("echo_int", value=1),
            headers={
                "content-type": "application/vnd.apache.arrow.stream",
                _PROOF_HEADER: "v1.whatever.0.not-a-real-nonce.nope",
            },
            timeout=5.0,
        )
        assert resp.status_code == 200


def _unary_request_body(method_name: str, **kwargs: object) -> bytes:
    """Serialize a unary conformance request as a complete Arrow IPC stream.

    Lets these tests POST directly, with full control over the request
    headers, instead of going through a client that would impose its own
    ``Accept-Encoding``.  The protocol version is read off the protocol
    class exactly as :class:`RpcClient` does — the server rejects a
    request that omits it.
    """
    from io import BytesIO

    from vgi_rpc.conformance._protocol import ConformanceService
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._wire import _write_request

    info = rpc_methods(ConformanceService)[method_name]
    version = vars(ConformanceService).get("protocol_version")
    buf = BytesIO()
    _write_request(
        buf,
        method_name,
        info.params_schema,
        kwargs,
        protocol_version=version if isinstance(version, str) else None,
    )
    return buf.getvalue()


def _advertised_encodings(port: int) -> list[str] | None:
    """Read ``VGI-Supported-Encodings`` from ``OPTIONS /health``.

    Returns ``None`` when the header is absent (a legacy server predating
    the advertisement), and a possibly-empty list when it is present.
    Absent and present-but-empty are deliberately different answers.
    """
    import httpx

    resp = httpx.options(f"http://127.0.0.1:{port}/health", timeout=5.0)
    raw = resp.headers.get("VGI-Supported-Encodings")
    if raw is None:
        return None
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def _response_codec(resp: httpx.Response) -> str | None:
    """Return the codec a response claims, from either stamping header."""
    ce = resp.headers.get("Content-Encoding") or resp.headers.get("X-VGI-Content-Encoding")
    return ce.strip().lower() if ce else None


class TestHttpCompressionNegotiationConformance:
    """Cross-language conformance for HTTP response-codec negotiation.

    Every implementation must agree on how a response codec is chosen, so
    that the DuckDB engine's cpp-httplib client and a browser ``fetch()``
    both get a correct answer from any SDK.  The rules under test:

    * ``X-VGI-Accept-Encoding`` outranks the generic ``Accept-Encoding``.
      cpp-httplib injects ``deflate, gzip, br, zstd`` — gzip first — and
      honouring that order instead of VGI's cost a 4.2x slower round-trip.
    * ``identity`` is a first-class token: a client can explicitly demand
      an uncompressed body.
    * A codec chosen only because of the custom request header is stamped
      on ``X-VGI-Content-Encoding``, since such a client's fetch layer
      would mangle or auto-decode a standard ``Content-Encoding``.
    * ``VGI-Supported-Encodings`` advertises what the server can actually
      do.  Present-but-empty means "no compression"; absent means a legacy
      server (assume zstd).

    Capability-gated throughout: servers advertising no codecs (compression
    disabled — the default in several SDKs) skip the codec-specific cases
    but must still prove they send nothing compressed.
    """

    # Large and highly compressible, so it clears every implementation's
    # minimum-size floor (Rust, for one, won't compress below 1 KiB).
    PAYLOAD = "conformance-compression-probe " * 4096

    def _echo(self, port: int, headers: dict[str, str]) -> httpx.Response:
        import httpx

        from vgi_rpc.http._common import _ARROW_CONTENT_TYPE

        body = _unary_request_body("echo_string", value=self.PAYLOAD)
        resp = httpx.post(
            f"http://127.0.0.1:{port}/echo_string",
            content=body,
            headers={"Content-Type": _ARROW_CONTENT_TYPE, **headers},
            timeout=30.0,
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.content[:200]!r}"
        return resp

    def _echo_compressed(self, port: int, codec: str) -> httpx.Response:
        """POST a *compressed* ``echo_string`` request, asking for a plain response.

        The accept headers pin ``identity`` so the reply is readable
        without a client-side decompressor, isolating the direction under
        test (request decoding) from response negotiation.  The status is
        deliberately not asserted — callers decide what is acceptable.
        """
        import httpx

        from vgi_rpc.http._common import _ARROW_CONTENT_TYPE, Encoding, compress

        body = compress(Encoding(codec), _unary_request_body("echo_string", value=self.PAYLOAD))
        return httpx.post(
            f"http://127.0.0.1:{port}/echo_string",
            content=body,
            headers={
                "Content-Type": _ARROW_CONTENT_TYPE,
                "Content-Encoding": codec,
                "Accept-Encoding": "identity",
                "X-VGI-Accept-Encoding": "identity",
            },
            timeout=30.0,
        )

    @staticmethod
    def _echoed_value(resp: httpx.Response) -> object:
        """Read the ``result`` column out of an uncompressed unary response."""
        from io import BytesIO

        reader = pa.ipc.open_stream(BytesIO(resp.content))
        for batch in reader:
            if "result" in batch.schema.names:
                return batch.column("result")[0].as_py()
        raise AssertionError("response carried no result batch")

    def test_advertised_codec_decodes_a_compressed_request(self, conformance_http_port: int) -> None:
        """A codec the server advertises must be accepted on a request body.

        The advertisement is what a client negotiates its *request*
        encoding against, so anything listed there has to round-trip.
        """
        advertised = _advertised_encodings(conformance_http_port)
        if not advertised:
            pytest.skip("server advertises no codecs")
        for codec in advertised:
            resp = self._echo_compressed(conformance_http_port, codec)
            assert resp.status_code == 200, f"{codec}: {resp.status_code}: {resp.content[:200]!r}"
            assert self._echoed_value(resp) == self.PAYLOAD, codec

    def test_compression_disabled_server_survives_a_compressed_request(self, request: pytest.FixtureRequest) -> None:
        """A compression-disabled server must not choke on a compressed request body.

        "I compress no responses" is a server policy about the *response*
        direction; it says nothing about being able to read a request.
        Peers compress by default (the DuckDB engine sends zstd request
        bodies), and an implementation that drops its decoder along with
        its compressor feeds ciphertext to the Arrow reader and answers
        500 -- the Python SDK did exactly that.

        Decoding is the right behaviour and is asserted whenever the
        server answers 200: the echoed payload must come back intact.  A
        clean 415 is tolerated as the other defensible answer (an
        implementation that genuinely cannot decode), because the
        conformance contract here is that the failure is *negotiated*,
        never a 5xx.
        """
        try:
            port = request.getfixturevalue("conformance_http_no_compression_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no compression-disabled server")

        from vgi_rpc.http._common import available_encodings

        codec = available_encodings()[0].value
        resp = self._echo_compressed(port, codec)
        assert resp.status_code != 500, f"compressed request must not 500: {resp.content[:200]!r}"
        assert resp.status_code in (200, 415), f"{resp.status_code}: {resp.content[:200]!r}"
        if resp.status_code == 200:
            assert _response_codec(resp) is None, "a compression-disabled server must reply uncompressed"
            assert self._echoed_value(resp) == self.PAYLOAD

    def test_supported_encodings_is_advertised(self, conformance_http_port: int) -> None:
        """The header is present, lists only real codecs, and omits identity."""
        advertised = _advertised_encodings(conformance_http_port)
        if advertised is None:
            pytest.skip("server does not advertise VGI-Supported-Encodings")
        assert set(advertised) <= {"zstd", "gzip"}, advertised
        # identity is always available, so advertising it carries no
        # information and would wrongly imply it is a compressor.
        assert "identity" not in advertised

    def test_identity_forces_an_uncompressed_response(self, conformance_http_port: int) -> None:
        """``identity`` first in the custom header wins over any codec after it."""
        if _advertised_encodings(conformance_http_port) is None:
            pytest.skip("server does not advertise VGI-Supported-Encodings")
        resp = self._echo(
            conformance_http_port,
            {"X-VGI-Accept-Encoding": "identity", "Accept-Encoding": "gzip, zstd"},
        )
        assert _response_codec(resp) is None, "identity must not be answered with a codec"

    def test_custom_header_alone_negotiates(self, conformance_http_port: int) -> None:
        """The browser case: ``fetch()`` cannot set ``Accept-Encoding`` at all.

        A codec reachable only via the custom header must be stamped on
        ``X-VGI-Content-Encoding``, not ``Content-Encoding``.
        """
        advertised = _advertised_encodings(conformance_http_port)
        if not advertised:
            pytest.skip("server advertises no response codecs")
        codec = advertised[0]
        # httpx injects its own ``Accept-Encoding`` unless told otherwise —
        # the same trap cpp-httplib sets.  Blanking it is what actually
        # reproduces a browser, where the header cannot be set at all; leave
        # it in and the codec is found in *both* lists and stamped on the
        # standard header, quietly testing nothing.
        resp = self._echo(
            conformance_http_port,
            {"X-VGI-Accept-Encoding": codec, "Accept-Encoding": ""},
        )
        assert _response_codec(resp) == codec
        assert resp.headers.get("X-VGI-Content-Encoding", "").strip().lower() == codec

    def test_custom_header_outranks_gzip_first_standard(self, conformance_http_port: int) -> None:
        """The cpp-httplib regression: a gzip-first Accept-Encoding must not win."""
        advertised = _advertised_encodings(conformance_http_port)
        if not advertised or "zstd" not in advertised:
            pytest.skip("server cannot produce zstd")
        resp = self._echo(
            conformance_http_port,
            {
                "Accept-Encoding": "deflate, gzip, br, zstd",
                "X-VGI-Accept-Encoding": "zstd, gzip",
            },
        )
        assert _response_codec(resp) == "zstd", "gzip listed first must not beat VGI's zstd preference"
        # zstd is in both headers, so the standard header carries it.
        assert resp.headers.get("Content-Encoding", "").strip().lower() == "zstd"

    def test_empty_advertisement_means_never_compressed(self, request: pytest.FixtureRequest) -> None:
        """A server advertising no codecs must not compress, however asked.

        Runs against a *separate* server booted with compression disabled,
        because the state under test is a server configuration that no
        client request can induce.  ``identity`` (tested above) covers a
        client's ability to demand an uncompressed body; only this server
        emits the present-but-empty ``VGI-Supported-Encodings`` that
        distinguishes "speaks no compression" from the absent header of a
        legacy server -- a distinction clients act on, and one that has to
        survive both the server's HTTP stack and the client's header
        parsing to be worth anything.

        Requested indirectly so a runner that does not yet provide the
        fixture skips rather than erroring at collection.
        """
        try:
            port = request.getfixturevalue("conformance_http_no_compression_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no compression-disabled server")

        advertised = _advertised_encodings(port)
        assert advertised is not None, "header must be present, not omitted"
        assert advertised == [], f"expected an empty advertisement, got {advertised}"

        resp = self._echo(
            port,
            {"Accept-Encoding": "zstd, gzip", "X-VGI-Accept-Encoding": "zstd, gzip"},
        )
        assert _response_codec(resp) is None

    def test_unproducible_codec_is_not_forced(self, conformance_http_port: int) -> None:
        """Offering only a codec the server can't produce yields an uncompressed body."""
        if _advertised_encodings(conformance_http_port) is None:
            pytest.skip("server does not advertise VGI-Supported-Encodings")
        resp = self._echo(conformance_http_port, {"Accept-Encoding": "br"})
        assert _response_codec(resp) is None, "server must not claim a codec it was never offered"


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


class TestDescribeConformance:
    """Validate __describe__ introspection output for the conformance service.

    The ``conformance_describe`` fixture (provided by the host harness) sends a
    real ``__describe__`` request to the worker under test — across every
    transport in the conformance matrix — so introspection is validated against
    the actual server, not a throwaway in-process Python one.
    """

    def test_run_describe_conformance(self, conformance_describe: ServiceDescription) -> None:
        """Run the full describe conformance suite and fail with detailed errors."""
        suite = run_describe_conformance(conformance_describe)
        if not suite.success:
            failures = [r for r in suite.results if not r.passed]
            details = "\n".join(f"  {r.name}: {r.error}" for r in failures)
            pytest.fail(f"{suite.failed}/{suite.total} describe conformance tests failed:\n{details}")

    def test_describe_via_rpc(self, conformance_describe: ServiceDescription) -> None:
        """Smoke test: basic transport-level describe call works."""
        # 76 + 3 sticky unary methods (open_counter / increment_counter /
        # close_counter) + 2 sticky streaming methods (stream_session_counter
        # / exchange_session_counter), added 2026-05 alongside the
        # Sticky.* conformance group.
        assert len(conformance_describe.methods) == 81
        assert conformance_describe.protocol_name == "ConformanceService"
        echo_str = conformance_describe.methods["echo_string"]
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


# ---------------------------------------------------------------------------
# HTTP response cap (strict-fail) tests — HTTP-only
# ---------------------------------------------------------------------------
#
# These tests verify the framework's strict-fail behaviour for HTTP
# responses that overshoot the operator-configured caps.  They require a
# server booted with ``max_response_bytes`` (and optionally
# ``max_externalized_response_bytes``) set, so they bypass the
# ``conformance_conn`` matrix and use the strict-cap fixture directly.


class TestHttpResponseCap:
    """HTTP-only strict-fail tests for response-size caps.

    Mirror the catalog-based tests in ``_runner.py``'s ``http_response_cap``
    category.  Use ``conformance_http_strict_cap_port`` (a session-scoped
    fixture booting a worker with tight caps) so the overshoot is provably
    triggered.
    """

    def _connect(self, port: int) -> Any:
        """Open an HTTP connection to the strict-cap conformance worker."""
        from vgi_rpc.http import http_connect

        return http_connect(ConformanceService, f"http://127.0.0.1:{port}")

    def test_unary_strict_fail(self, conformance_http_strict_cap_port: int) -> None:
        """Unary returning more bytes than ``max_response_bytes`` allows surfaces RpcError."""
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{conformance_http_strict_cap_port}")
        assert caps.max_response_bytes is not None, "strict-cap fixture must advertise a wire cap"
        with (
            self._connect(conformance_http_strict_cap_port) as proxy,
            pytest.raises(RpcError, match=r"max_response_bytes"),
        ):
            proxy.oversized_unary(target_bytes=caps.max_response_bytes * 4)

    def test_exchange_strict_fail(self, conformance_http_strict_cap_port: int) -> None:
        """Exchange returning oversize output surfaces RpcError."""
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{conformance_http_strict_cap_port}")
        assert caps.max_response_bytes is not None
        target_rows = max(1024, (caps.max_response_bytes * 4) // 16)
        with (
            self._connect(conformance_http_strict_cap_port) as proxy,
            pytest.raises(RpcError, match=r"max_response_bytes"),
            proxy.exchange_oversized(rows_per_batch=target_rows) as session,
        ):
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))


class TestHttpResponseCapSoftWire:
    """Producer streams have a *soft* wire cap.

    Verifies the design choice that a producer emitting more than
    ``max_response_bytes`` does **not** strict-fail when there's no
    externalisation — continuation tokens cover the overshoot
    transparently.  The opposite direction (strict-fail) is exercised
    in :class:`TestHttpResponseCap` for unary and exchange.
    """

    def test_producer_overshoot_uses_continuation(self, conformance_http_strict_cap_port: int) -> None:
        """Oversize producer emit splits across continuation tokens, not RpcError."""
        from vgi_rpc.http import http_capabilities, http_connect

        caps = http_capabilities(base_url=f"http://127.0.0.1:{conformance_http_strict_cap_port}")
        assert caps.max_response_bytes is not None
        # Emit ~2x the wire cap of int64+int64 rows in a single batch.
        # A single oversized iteration overflows the cap; the framework
        # emits the body and mints a continuation token.  No RpcError.
        target_rows = max(1024, (caps.max_response_bytes * 2) // 16)
        with http_connect(ConformanceService, f"http://127.0.0.1:{conformance_http_strict_cap_port}") as proxy:
            batches = list(proxy.produce_oversized_batch(rows_per_batch=target_rows))
            # Single emit + finish; the framework may have split it into
            # the data batch + a continuation token + trailing finish, but
            # the client-visible batches are: 1 data batch.
            assert sum(b.batch.num_rows for b in batches) == target_rows


# ---------------------------------------------------------------------------
# Call-token conformance (HTTP-only)
# ---------------------------------------------------------------------------


def _empty_schema() -> pa.Schema:
    """Build the zero-column schema a producer continuation request is framed on."""
    return pa.schema([])


def _stream_batches(body: bytes) -> list[tuple[int, dict[bytes, bytes]]]:
    """Walk a response body, returning ``(num_rows, custom_metadata)`` per batch.

    A stream response may be several *concatenated* IPC streams — a header
    stream followed by the data stream — so this reads streams until the
    buffer is exhausted rather than stopping at the first EOS.

    Args:
        body: The raw, uncompressed response body.

    Returns:
        One entry per batch, in wire order.

    """
    from io import BytesIO

    buf = BytesIO(body)
    out: list[tuple[int, dict[bytes, bytes]]] = []
    while buf.tell() < len(body):
        before = buf.tell()
        try:
            reader = ipc.open_stream(buf)
        except pa.ArrowInvalid:
            break
        while True:
            try:
                batch, md = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                break
            out.append((batch.num_rows, dict(md) if md is not None else {}))
        if buf.tell() == before:  # no forward progress — don't spin
            break
    return out


class TestCallTokenSplit:
    """A stream's state MUST travel as two tokens, split by lifetime.

    Half of a stream's state is fixed for the life of the call — the init
    request and the resolved input/output schemas — and half advances every
    turn.  Carrying both in one token makes every continuation re-serialize,
    re-seal, re-open and re-parse the fixed half, which for a typical stream
    is most of the payload.  So the two travel separately:

    * :data:`~vgi_rpc.metadata.CALL_STATE_KEY` — the **call token**, minted
      once by ``/init`` and never re-issued.
    * :data:`~vgi_rpc.metadata.STATE_KEY` — the **cursor token**, re-minted
      every turn, and the only token a continuation response returns.

    Splitting is required, not an optimisation a port may skip: it is what
    lets a server cache the resolved call, compress the two halves on their
    own lifetimes, and keep continuation payloads small.  A server that
    packs everything into the cursor forces every peer and intermediary onto
    the expensive path forever, so it fails here.

    These tests read the wire directly rather than through a client, which
    strips both keys before application code sees them.  They need nothing
    from a runner beyond the plain ``conformance_http_port`` worker.

    The companion :class:`TestColdCallStateCache` covers the other side of
    the contract — that the *client* echoes the call token, and that the
    server can resume from it with nothing cached.
    """

    def _init(self, port: int, method: str, **params: object) -> list[tuple[int, dict[bytes, bytes]]]:
        """POST ``{method}/init`` and return the response's batches.

        A stream ``/init`` request body is the same request stream shape as
        a unary call — params batch plus dispatch metadata — so it is built
        with the same helper.
        """
        import httpx

        from vgi_rpc.http._common import _ARROW_CONTENT_TYPE

        resp = httpx.post(
            f"http://127.0.0.1:{port}/{method}/init",
            content=_unary_request_body(method, **params),
            headers={
                "Content-Type": _ARROW_CONTENT_TYPE,
                # Pin identity so the body is readable without a decompressor.
                "Accept-Encoding": "identity",
                "X-VGI-Accept-Encoding": "identity",
            },
            timeout=30.0,
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.content[:200]!r}"
        return _stream_batches(resp.content)

    @staticmethod
    def _sentinel(batches: list[tuple[int, dict[bytes, bytes]]]) -> dict[bytes, bytes]:
        """Return the metadata of the zero-row batch carrying the cursor token."""
        for num_rows, md in batches:
            if num_rows == 0 and STATE_KEY in md:
                return md
        raise AssertionError(f"no token sentinel in response; batches={[(n, sorted(m)) for n, m in batches]}")

    def test_producer_init_mints_a_call_token(self, conformance_http_port: int) -> None:
        """A producer ``/init`` hands over both tokens on the same sentinel."""
        md = self._sentinel(self._init(conformance_http_port, "produce_n", count=3))
        assert CALL_STATE_KEY in md, (
            f"producer /init must mint a call token alongside the cursor; got only {sorted(k.decode() for k in md)}"
        )
        assert md[CALL_STATE_KEY], "call token must not be empty"

    def test_exchange_init_mints_a_call_token(self, conformance_http_port: int) -> None:
        """An exchange ``/init`` hands over both tokens on the same sentinel."""
        md = self._sentinel(self._init(conformance_http_port, "exchange_accumulate"))
        assert CALL_STATE_KEY in md, (
            f"exchange /init must mint a call token alongside the cursor; got only {sorted(k.decode() for k in md)}"
        )
        assert md[CALL_STATE_KEY], "call token must not be empty"

    def test_continuation_does_not_reissue_the_call_token(self, conformance_http_port: int) -> None:
        """Only the cursor is re-minted per turn; the call token is issued once.

        This is what makes the split pay: if a server re-sealed the call
        half every turn it would be doing exactly the work the split exists
        to avoid, while looking conformant to a client that merely stores
        whatever it was last sent.
        """
        import httpx

        from vgi_rpc.http._common import _ARROW_CONTENT_TYPE

        init = self._sentinel(self._init(conformance_http_port, "produce_n", count=3))
        assert CALL_STATE_KEY in init, (
            "cannot check re-issue: /init minted no call token (see test_producer_init_mints_a_call_token)"
        )
        req = BytesIO()
        with new_ipc_stream(req, _empty_schema()) as writer:
            writer.write_batch(
                empty_batch(_empty_schema()),
                custom_metadata=pa.KeyValueMetadata({STATE_KEY: init[STATE_KEY], CALL_STATE_KEY: init[CALL_STATE_KEY]}),
            )
        resp = httpx.post(
            f"http://127.0.0.1:{conformance_http_port}/produce_n/exchange",
            content=req.getvalue(),
            headers={
                "Content-Type": _ARROW_CONTENT_TYPE,
                "Accept-Encoding": "identity",
                "X-VGI-Accept-Encoding": "identity",
            },
            timeout=30.0,
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.content[:200]!r}"
        for _num_rows, md in _stream_batches(resp.content):
            assert CALL_STATE_KEY not in md, "a continuation response must not re-issue the call token"


class TestColdCallStateCache:
    """A stream continuation must carry everything the server needs to resume it.

    A server splits its stream state into a *call* token (fixed for the life
    of the call — the init request and resolved schemas, minted once by
    ``/init``) and a *cursor* token (re-minted every turn); see
    :class:`TestCallTokenSplit`, which pins that requirement.  Having split,
    it is free to keep a per-process cache of resolved calls so a
    continuation need not re-open the call token — but that cache is an
    accelerator, never a contract.  A client owes the server both tokens on
    **every** request:
    :data:`~vgi_rpc.metadata.STATE_KEY` and, when ``/init`` handed one over,
    :data:`~vgi_rpc.metadata.CALL_STATE_KEY`.

    A client that echoes only the cursor works perfectly while the cache is
    warm and fails the moment it is not — a restarted worker, an evicted
    entry, or a continuation load-balanced to a node that never saw this
    stream's ``/init``.  That is precisely the stateless-relay case the split
    exists to serve, so it must not be left to chance.

    These tests run against a worker booted with the call-state cache
    **disabled**, which turns that load-dependent bug into a deterministic
    one: every continuation takes the miss path.

    Requested through an optional ``conformance_http_cold_call_cache_port``
    fixture — a runner whose server does not split call state (everything
    rides the cursor, so there is nothing to echo) omits it and this group
    skips.
    """

    def _port(self, request: pytest.FixtureRequest) -> int:
        """Resolve the cold-cache worker's port, or skip."""
        try:
            port: int = request.getfixturevalue("conformance_http_cold_call_cache_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no cache-disabled server")
        return port

    def test_producer_continuations_survive_a_cold_cache(self, request: pytest.FixtureRequest) -> None:
        """Draining a producer stream works with every turn on the miss path."""
        from vgi_rpc.http import http_connect

        port = self._port(request)
        with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
            batches = list(proxy.produce_n(count=5))
        assert sum(b.batch.num_rows for b in batches) == 5

    def test_exchange_turns_survive_a_cold_cache(self, request: pytest.FixtureRequest) -> None:
        """Successive exchange turns each re-resolve the call from the echoed token."""
        from vgi_rpc.http import http_connect

        port = self._port(request)
        with (
            http_connect(ConformanceService, f"http://127.0.0.1:{port}") as conn,
            conn.exchange_accumulate() as session,
        ):
            # Three turns: the first rides the /init round-trip, the rest are
            # continuations. Each one must stand on its own.
            sums = [
                session.exchange(AnnotatedBatch.from_pydict({"value": [n]})).batch.column("running_sum")[0].as_py()
                for n in (1.0, 2.0, 3.0)
            ]
        assert sums == [pytest.approx(1.0), pytest.approx(3.0), pytest.approx(6.0)]


# ---------------------------------------------------------------------------
# Error flag on the wire (HTTP-only)
# ---------------------------------------------------------------------------


#: Marks a 200 that is really a failure.
_RPC_ERROR_HEADER = "X-VGI-RPC-Error"


class TestErrorHeader:
    """``X-VGI-RPC-Error`` must actually be sent, not merely CORS-exposed.

    A failed RPC returns **HTTP 200** — the error travels as an EXCEPTION
    batch in a well-formed response body, because the call reached the method
    and the method raised. So the status line says nothing, and this header is
    how a client tells a failure from a result without parsing the body.

    The suite required the header to appear in
    ``Access-Control-Expose-Headers`` and never checked that any response
    carries it: a port could expose a header it never sends and pass. Exposing
    something you do not emit is not a smaller bug than not exposing it —
    both leave a client unable to see the failure.

    Normative in ``docs/porting-guide.md`` (500 is rewritten to 200 with this
    header) and ``docs/sticky-sessions-spec.md`` §7.
    """

    @staticmethod
    def _post(port: int, method: str, **kwargs: object) -> Any:
        """POST a unary call, returning the raw response."""
        import httpx

        for path in (f"/{method}", f"/vgi/{method}"):
            resp = httpx.post(
                f"http://127.0.0.1:{port}{path}",
                content=_unary_request_body(method, **kwargs),
                headers={"content-type": "application/vnd.apache.arrow.stream"},
                timeout=10.0,
            )
            if resp.status_code != 404:
                return resp
        return resp

    def test_error_response_sets_the_flag(self, conformance_http_port: int) -> None:
        """A method that raises answers 200 with the flag set."""
        resp = self._post(conformance_http_port, "raise_value_error", message="boom")
        assert resp.status_code == 200, (
            f"a raising method must still answer 200 — the error rides the body, not the status "
            f"(got {resp.status_code})"
        )
        flag = resp.headers.get(_RPC_ERROR_HEADER.lower())
        assert flag is not None, (
            f"{_RPC_ERROR_HEADER} absent on an error response; a client sees 200 and reads a failure as a result"
        )
        assert flag.lower() == "true", f"expected 'true', got {flag!r}"

    def test_success_response_does_not_set_the_flag(self, conformance_http_port: int) -> None:
        """The flag must discriminate.

        A header set on every response carries no information — it would make
        every result look like a failure, which is the same outage as the
        reverse.
        """
        resp = self._post(conformance_http_port, "echo_int", value=1)
        assert resp.status_code == 200
        flag = resp.headers.get(_RPC_ERROR_HEADER.lower())
        assert flag is None or flag.lower() != "true", (
            f"{_RPC_ERROR_HEADER}={flag!r} on a successful call; the flag must distinguish failure from success"
        )


# ---------------------------------------------------------------------------
# Request-ID correlation (HTTP-only)
# ---------------------------------------------------------------------------


#: Header carrying the per-request correlation ID.
_REQUEST_ID_HEADER = "X-Request-ID"


class TestRequestId:
    """``X-Request-ID`` must be emitted, and must match the access log.

    The suite already required this header to be CORS-exposed, but never
    checked that it is *sent* — the same shape as validating an expose list
    without validating the header, where the rule holds everywhere except
    where it applies.

    The correlation half is the point. A request id that appears on the
    response but not in the log, or differs between them, is worse than
    having none: it looks like a working trail right up to the moment
    somebody tries to follow it.

    ``docs/access-log-spec.md`` §4.4 makes propagation a SHOULD, so the
    header tests skip for a port that emits nothing rather than failing it.
    What is *not* optional is agreement: a port that emits the header and a
    ``request_id`` must make them the same value.
    """

    @staticmethod
    def _call(port: int, request_id: str | None = None) -> Any:
        """Make a unary call, optionally supplying a correlation ID."""
        import httpx

        headers = {"content-type": "application/vnd.apache.arrow.stream"}
        if request_id is not None:
            headers[_REQUEST_ID_HEADER] = request_id
        for path in ("/echo_int", "/vgi/echo_int"):
            resp = httpx.post(
                f"http://127.0.0.1:{port}{path}",
                content=_unary_request_body("echo_int", value=1),
                headers=headers,
                timeout=10.0,
            )
            if resp.status_code == 200:
                return resp
        return resp

    def _emitted(self, port: int, request_id: str | None = None) -> str:
        """Return the response's request id, skipping if the port emits none."""
        resp = self._call(port, request_id)
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:200]}"
        got = resp.headers.get(_REQUEST_ID_HEADER.lower())
        if got is None:
            pytest.skip(f"server does not emit {_REQUEST_ID_HEADER}")
        return str(got)

    def test_response_carries_a_request_id(self, conformance_http_port: int) -> None:
        """Every response carries a correlation ID, generated when none was sent."""
        got = self._emitted(conformance_http_port)
        assert got.strip(), "request id must not be empty"

    def test_inbound_request_id_is_echoed(self, conformance_http_port: int) -> None:
        """A caller-supplied ID is propagated, not replaced.

        Propagation is what makes the ID useful across a hop: a proxy or
        client that minted an ID needs the worker's records to carry the same
        one, or the two logs cannot be joined.
        """
        mine = "conformance-request-id-0123456789"
        got = self._emitted(conformance_http_port, mine)
        assert got == mine, f"inbound {_REQUEST_ID_HEADER} was replaced with {got!r}"

    def test_generated_ids_differ_between_requests(self, conformance_http_port: int) -> None:
        """A minted ID identifies one request, so it cannot be a constant."""
        first = self._emitted(conformance_http_port)
        second = self._emitted(conformance_http_port)
        assert first != second, "generated request ids must be unique per request"

    def test_request_id_matches_the_access_log(self, request: pytest.FixtureRequest) -> None:
        """The header and the record must name the same request.

        This is the assertion the whole field exists for, and the one the
        suite could not make until a runner exposed its worker's log. Gated
        on the optional ``conformance_http_access_log`` fixture, which yields
        ``(port, path)`` for a worker writing JSONL access records.
        """
        import json
        import time

        try:
            port, log_path = request.getfixturevalue("conformance_http_access_log")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_access_log")

        mine = "conformance-correlation-abcdef0123"
        got = self._emitted(port, mine)
        assert got == mine

        # The record is written as the response completes; allow the writer a
        # moment rather than racing it.
        deadline = time.time() + 10.0
        ids: list[str] = []
        while time.time() < deadline:
            if log_path.exists():
                ids = [
                    str(rec.get("request_id", ""))
                    for line in log_path.read_text().splitlines()
                    if line.strip()
                    for rec in [json.loads(line)]
                    if rec.get("logger") == "vgi_rpc.access"
                ]
                if mine in ids:
                    break
            time.sleep(0.2)
        assert mine in ids, (
            f"the response reported {_REQUEST_ID_HEADER}={mine!r} but no access-log record "
            f"carries that request_id (saw {ids[-5:]}); header and log must name the same request"
        )


# ---------------------------------------------------------------------------
# Token introspection conformance (HTTP-only, optional)
# ---------------------------------------------------------------------------


#: Path of the introspection endpoint, appended to the runner's prefix.
_INTROSPECT_PATH = "/__introspect_token__"
#: Header the conformance workers read to decide a request's principal.
_PRINCIPAL_HEADER_NAME = "X-Conformance-Principal"
#: Advertised on ``OPTIONS /health`` when the route is enabled.
_INTROSPECT_ENABLED_HEADER = "VGI-Token-Introspection"

#: Fixed values a runner supplying ``conformance_http_introspect_port`` must
#: configure. The shared tests post the subject credential and assert the
#: principal, so these are part of the fixture's contract, not decoration.
_INTROSPECTOR = "conformance-introspector"
_SUBJECT_TOKEN = "conformance-opaque-subject-token"
_SUBJECT_PRINCIPAL = "subject@conformance.example"
#: JWS-shaped and resolvable by the fixture — see the test that posts it.
_JWS_TRAP_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"

#: Statuses a caller may treat as final. Anything else — including a transport
#: failure — must be retried rather than cached, which is the distinction the
#: caller's negative cache is built on.
_DEFINITIVE = (401, 403, 404)


class TestTokenIntrospection:
    """Resolving an opaque credential to a principal, for a fronting proxy.

    A proxy terminating the only public listener must know which principal a
    credential authenticates as before it can authorize anything. When the
    credential is opaque it has to ask the worker.

    What makes this group worth its length is that the answer is **an identity
    assertion made by the thing being protected**, which the asker then acts on
    using credentials the worker does not hold. It has to be trusted *more*
    than the worker, so almost everything asserted here is a refusal.

    Requested through an optional ``conformance_http_introspect_port``
    fixture, configured with the module constants above. A port that does not
    implement introspection omits it and this group skips;
    :class:`TestTokenIntrospectionOffMode` still runs everywhere.
    """

    @staticmethod
    def _port(request: pytest.FixtureRequest) -> int:
        """Resolve the introspection-enabled worker's port, or skip."""
        try:
            port: int = request.getfixturevalue("conformance_http_introspect_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_introspect_port")
        return port

    @staticmethod
    def _post(port: int, caller: str | None, body: object) -> Any:
        """POST *body* to the introspection endpoint as *caller*."""
        import httpx

        headers = {"content-type": "application/json"}
        if caller is not None:
            headers[_PRINCIPAL_HEADER_NAME] = caller
        for path in (_INTROSPECT_PATH, f"/vgi{_INTROSPECT_PATH}"):
            resp = httpx.post(f"http://127.0.0.1:{port}{path}", json=body, headers=headers, timeout=5.0)
            if resp.status_code != 404 or "not_enabled" not in resp.text:
                return resp
        return resp

    def test_valid_credential_resolves(self, request: pytest.FixtureRequest) -> None:
        """The endpoint's reason for existing."""
        resp = self._post(self._port(request), _INTROSPECTOR, {"token": _SUBJECT_TOKEN})
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:200]}"
        body = resp.json()
        assert body["principal"] == _SUBJECT_PRINCIPAL

    def test_ttl_is_usable(self, request: pytest.FixtureRequest) -> None:
        """``ttl_seconds`` exists so the *caller* caches; it must be a live number.

        A non-finite or non-positive value silently disables a caller's cache
        and turns every request into a round trip.
        """
        body = self._post(self._port(request), _INTROSPECTOR, {"token": _SUBJECT_TOKEN}).json()
        ttl = body["ttl_seconds"]
        assert isinstance(ttl, int | float)
        assert ttl > 0
        assert math.isfinite(ttl), "ttl_seconds must be finite: NaN silently disables a caller's cache"

    def test_response_carries_no_claims(self, request: pytest.FixtureRequest) -> None:
        """The most dangerous field this endpoint could grow.

        A pass-through claims field would let a worker choose its caller's
        tenant routing, its row scope, and its policy branch — the asker
        derives everything it needs from the principal alone, and must keep
        doing so. Asserted as a closed key set, so a port adding a field has
        to come through this test.
        """
        body = self._post(self._port(request), _INTROSPECTOR, {"token": _SUBJECT_TOKEN}).json()
        assert "claims" not in body
        assert set(body) <= {"principal", "token_name", "ttl_seconds"}, f"unexpected keys: {sorted(body)}"

    def test_unknown_credential_is_definitive(self, request: pytest.FixtureRequest) -> None:
        """A caller must be able to negative-cache this without fear.

        Definitive and transient are different answers: a caller that caches
        an outage stops serving a live credential, and a caller that retries a
        rejection hammers the worker.
        """
        resp = self._post(self._port(request), _INTROSPECTOR, {"token": "no-such-credential"})
        assert resp.status_code in _DEFINITIVE, f"not a definitive rejection: {resp.status_code}"

    @pytest.mark.parametrize("token", ["no-such-credential", "expired-credential", "!!malformed!!"])
    def test_rejections_are_indistinguishable(self, request: pytest.FixtureRequest, token: str) -> None:
        """Unknown, expired and malformed are one answer.

        Reporting which would confirm that a guessed credential exists, which
        is exactly the oracle the allowlist below exists to close.
        """
        port = self._port(request)
        baseline = self._post(port, _INTROSPECTOR, {"token": "no-such-credential"})
        resp = self._post(port, _INTROSPECTOR, {"token": token})
        assert resp.status_code == baseline.status_code
        assert resp.text == baseline.text, f"rejection for {token!r} is distinguishable"

    def test_unauthenticated_caller_refused(self, request: pytest.FixtureRequest) -> None:
        """No credential, no answer."""
        resp = self._post(self._port(request), None, {"token": _SUBJECT_TOKEN})
        assert resp.status_code in _DEFINITIVE
        assert _SUBJECT_PRINCIPAL not in resp.text

    def test_non_introspector_refused(self, request: pytest.FixtureRequest) -> None:
        """The oracle test, and the one most likely to be skipped.

        A deployment where *any* valid credential may introspect lets any user
        test guesses of any other user's credential at unlimited rate, and
        resolve a stolen one to its owner. Authentication is not the same
        capability as introspection, and a port that checks only the former
        passes every other test in this group.
        """
        resp = self._post(self._port(request), "someone-else", {"token": _SUBJECT_TOKEN})
        assert resp.status_code in _DEFINITIVE, f"a non-introspector was answered: {resp.status_code}"
        assert resp.status_code != 200
        assert _SUBJECT_PRINCIPAL not in resp.text, "the principal leaked to an unauthorized caller"

    def test_jws_shaped_subject_is_rejected(self, request: pytest.FixtureRequest) -> None:
        """A locally-validated JWS must never be vouched for here.

        Routing one through hands a third party a bearer token the asker may
        itself have rejected — an expired access token is still live at its
        issuer for other resources.
        """
        # The fixture's resolver *would* resolve this one. That is deliberate:
        # against an unknown JWS a port with no shape guard rejects it as
        # unknown and passes for the wrong reason. Resolvable, the guard is
        # the only thing that can produce a rejection.
        resp = self._post(self._port(request), _INTROSPECTOR, {"token": _JWS_TRAP_TOKEN})
        assert resp.status_code in _DEFINITIVE
        assert resp.status_code != 200, "a JWS-shaped subject was resolved"

    @pytest.mark.parametrize("token", [_SUBJECT_TOKEN, "no-such-credential"])
    def test_credential_never_appears_in_the_response(self, request: pytest.FixtureRequest, token: str) -> None:
        """Neither a success nor a rejection may echo the subject credential."""
        resp = self._post(self._port(request), _INTROSPECTOR, {"token": token})
        assert token not in resp.text, "the credential was echoed back"

    def test_capability_is_advertised(self, request: pytest.FixtureRequest) -> None:
        """A proxy preflights at boot rather than discovering at first login."""
        import httpx

        port = self._port(request)
        resp = httpx.options(f"http://127.0.0.1:{port}/health", timeout=5.0)
        assert resp.headers.get(_INTROSPECT_ENABLED_HEADER.lower()) == "true"


class TestTokenIntrospectionOffMode:
    """A worker that did not enable introspection must resolve nothing.

    Ungated: "off unless explicitly enabled" binds every port whether or not
    it implements the feature, and it is the guard that stops a worker growing
    a credential-to-identity oracle by upgrading a dependency.
    """

    def test_not_advertised_by_default(self, conformance_http_port: int) -> None:
        """The absent capability header is how a proxy knows not to configure this."""
        import httpx

        resp = httpx.options(f"http://127.0.0.1:{conformance_http_port}/health", timeout=5.0)
        assert _INTROSPECT_ENABLED_HEADER.lower() not in resp.headers

    def test_disabled_worker_refuses_definitively(self, conformance_http_port: int) -> None:
        """A caller must stop, not spin.

        The answer has to be one a caller classifies as final. Anything else —
        a 415 from a generic route that happens to catch the path, say — reads
        as "retry later", so pointing a proxy at a worker without the feature
        retries forever instead of failing at preflight.
        """
        import httpx

        for path in (_INTROSPECT_PATH, f"/vgi{_INTROSPECT_PATH}"):
            resp = httpx.post(
                f"http://127.0.0.1:{conformance_http_port}{path}",
                json={"token": "anything"},
                headers={"content-type": "application/json"},
                timeout=5.0,
            )
            assert resp.status_code in _DEFINITIVE, (
                f"{path} answered {resp.status_code}; a caller would treat that as transient and retry forever"
            )
            assert resp.status_code != 200


# ---------------------------------------------------------------------------
# CORS conformance (HTTP-only)
# ---------------------------------------------------------------------------


#: Origin the CORS conformance worker must allow. A runner supplying
#: ``conformance_http_cors_port`` configures its worker with exactly this.
_CORS_ORIGIN = "https://conformance.example"

#: Headers that MUST be exposed even though nothing advertises them, because
#: they ride error and rejection responses rather than the success path that
#: ``OPTIONS /health`` describes. A check derived from advertisements cannot
#: reach these by construction, so they are named.
_ALWAYS_EXPOSED = frozenset({"x-vgi-rpc-error", "vgi-auth-reason", "x-request-id"})

#: Value a CORS-enabled server is expected to send for
#: ``Cross-Origin-Resource-Policy``. A server that has opted into serving
#: cross-origin callers has, by construction, opted into this.
_CORP_VALUE = "cross-origin"


class TestCors:
    """Browser clients must be able to *read* what the server advertises.

    Every capability this framework exposes over HTTP is carried on a
    response header — the size caps, the compression set, the sticky
    advert, the 401 reason code, the ``X-VGI-RPC-Error`` flag. A browser
    hides every one of those from JavaScript unless the server names it in
    ``Access-Control-Expose-Headers``.

    That makes this the one contract the rest of the suite structurally
    cannot check: conformance tests drive the server with an HTTP client
    that sees all headers regardless of CORS, so a port can implement every
    capability header correctly, pass every other group, and still ship a
    server no browser client can read a single capability from.

    Requested through an optional ``conformance_http_cors_port`` fixture —
    a worker configured to allow :data:`_CORS_ORIGIN`. A port that does not
    implement CORS omits the fixture and this group skips; the companion
    :class:`TestCorsOffMode` still runs everywhere.
    """

    @staticmethod
    def _port(request: pytest.FixtureRequest) -> int:
        """Resolve the CORS-enabled worker's port, or skip."""
        try:
            port: int = request.getfixturevalue("conformance_http_cors_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_cors_port")
        return port

    @staticmethod
    def _preflight(port: int, path: str = "/echo_int") -> Any:
        """Send a CORS preflight for an RPC call, as a browser would."""
        import httpx

        return httpx.options(
            f"http://127.0.0.1:{port}{path}",
            headers={
                "Origin": _CORS_ORIGIN,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
            timeout=5.0,
        )

    @staticmethod
    def _exposed(resp: Any) -> set[str]:
        """Parse ``Access-Control-Expose-Headers`` into a lowercase name set."""
        raw = resp.headers.get("access-control-expose-headers", "")
        if raw.strip() == "*":
            return {"*"}
        return {name.strip().lower() for name in raw.split(",") if name.strip()}

    def test_preflight_allows_the_origin(self, request: pytest.FixtureRequest) -> None:
        """A preflight from the configured origin is answered, not refused."""
        resp = self._preflight(self._port(request))
        allow = resp.headers.get("access-control-allow-origin")
        if allow is None:
            pytest.skip("server does not answer CORS preflights")
        assert allow in (_CORS_ORIGIN, "*"), f"origin not allowed: {allow!r}"

    def test_preflight_allows_post(self, request: pytest.FixtureRequest) -> None:
        """RPC calls are POSTs, so the preflight must permit POST.

        A server that answers the preflight but omits POST has told the
        browser to block every RPC call it was about to make.
        """
        resp = self._preflight(self._port(request))
        if "access-control-allow-origin" not in resp.headers:
            pytest.skip("server does not answer CORS preflights")
        allowed = {m.strip().upper() for m in resp.headers.get("access-control-allow-methods", "").split(",")}
        assert "POST" in allowed or "*" in allowed, f"POST not permitted: {sorted(allowed)}"

    def test_preflight_allows_the_content_type(self, request: pytest.FixtureRequest) -> None:
        """The Arrow content type is not a CORS-safelisted value.

        ``application/vnd.apache.arrow.stream`` makes every RPC call a
        non-simple request, so a browser will not send it unless the
        preflight allows the ``Content-Type`` header explicitly.
        """
        resp = self._preflight(self._port(request))
        if "access-control-allow-origin" not in resp.headers:
            pytest.skip("server does not answer CORS preflights")
        allowed = {h.strip().lower() for h in resp.headers.get("access-control-allow-headers", "").split(",")}
        assert "content-type" in allowed or "*" in allowed, f"Content-Type not allowed: {sorted(allowed)}"

    @pytest.mark.parametrize(
        "header",
        ["content-type", "x-vgi-accept-encoding", "vgi-session", "vgi-session-accept", "vgi-proxy-proof"],
    )
    def test_preflight_allows_the_request_headers(self, request: pytest.FixtureRequest, header: str) -> None:
        """A browser may only *send* the headers the preflight permits.

        The expose list is the response half of CORS; this is the request
        half, and it fails differently. A server that permits only
        ``Content-Type`` still serves plain calls perfectly, so nothing
        looks wrong — but a browser silently refuses to send
        ``VGI-Session``, which takes out sticky sessions, or
        ``VGI-Proxy-Proof``, which takes out every call behind a proof gate.

        Asked for individually rather than as one batch so a failure names
        the header that was refused. A server may answer with the literal
        echo of the request, an explicit list, or ``*`` — all three are
        conformant; silently dropping one from the list is not.
        """
        import httpx

        port = self._port(request)
        resp = httpx.options(
            f"http://127.0.0.1:{port}/echo_int",
            headers={
                "Origin": _CORS_ORIGIN,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": header,
            },
            timeout=5.0,
        )
        if "access-control-allow-origin" not in resp.headers:
            pytest.skip("server does not answer CORS preflights")
        allowed = {h.strip().lower() for h in resp.headers.get("access-control-allow-headers", "").split(",")}
        assert header in allowed or "*" in allowed, f"a browser may not send {header!r}; allowed: {sorted(allowed)}"

    def test_actual_response_carries_the_origin(self, request: pytest.FixtureRequest) -> None:
        """The preflight is not enough — the real response needs the header too.

        A browser re-checks ``Access-Control-Allow-Origin`` on the actual
        response and discards the body without it, so a server that sets it
        only on ``OPTIONS`` fails every real call while passing a naive
        preflight-only test.
        """
        import httpx

        port = self._port(request)
        resp = httpx.post(
            f"http://127.0.0.1:{port}/echo_int",
            content=_unary_request_body("echo_int", value=1),
            headers={"Content-Type": "application/vnd.apache.arrow.stream", "Origin": _CORS_ORIGIN},
            timeout=5.0,
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.content[:200]!r}"
        allow = resp.headers.get("access-control-allow-origin")
        if allow is None and "access-control-allow-origin" not in self._preflight(port).headers:
            pytest.skip("server does not implement CORS")
        assert allow in (_CORS_ORIGIN, "*"), f"actual response not readable cross-origin: {allow!r}"

    def test_advertised_capabilities_are_all_exposed(self, request: pytest.FixtureRequest) -> None:
        """Every capability header the server advertises must be readable.

        The expected set is derived from what this server actually
        advertises on ``OPTIONS /health`` rather than hardcoded, so the
        assertion adapts to a port's feature set instead of demanding
        features it never claimed. Whatever you advertise, expose.

        This is the assertion the rest of the suite cannot make: an
        unexposed capability header is invisible only to a browser, and
        every other test here drives the server with a client that ignores
        CORS entirely.
        """
        import httpx

        port = self._port(request)
        preflight = self._preflight(port)
        if "access-control-allow-origin" not in preflight.headers:
            pytest.skip("server does not implement CORS")
        exposed = self._exposed(preflight)
        if "*" in exposed:
            return  # a wildcard exposes everything; nothing to enumerate

        health = httpx.options(f"http://127.0.0.1:{port}/health", timeout=5.0)
        advertised = {name.lower() for name in health.headers if name.lower().startswith(("vgi-", "x-vgi-"))}
        assert advertised, "server advertises no capability headers on OPTIONS /health"
        missing = advertised - exposed
        assert not missing, (
            f"advertised but not readable by a browser: {sorted(missing)} — "
            f"add them to Access-Control-Expose-Headers (exposed: {sorted(exposed)})"
        )

    def test_worker_advertises_the_optional_capabilities(self, request: pytest.FixtureRequest) -> None:
        """Guard the fixture itself: a bare worker under-tests the derived check.

        ``test_advertised_capabilities_are_all_exposed`` can only catch a
        missing exposure for a header the worker actually advertises, so a
        CORS fixture pointed at a plain worker silently skips the whole
        conditional half of the capability set — externalisation, upload
        URLs, the size caps. Those are exactly the exposures a port is most
        likely to miss, because they are the ones its default worker never
        exercises.

        This asserts the fixture is pointed at a worker with those features
        on. It is a test about test coverage, which is unusual, but the
        alternative is a green suite that proves less than it appears to.
        """
        import httpx

        port = self._port(request)
        health = httpx.options(f"http://127.0.0.1:{port}/health", timeout=5.0)
        advertised = {name.lower() for name in health.headers if name.lower().startswith(("vgi-", "x-vgi-"))}
        assert "vgi-upload-url-support" in advertised, (
            "the CORS worker advertises no upload-URL support, so the derived "
            "exposure check never sees the storage capability headers — point "
            f"conformance_http_cors_port at a storage-enabled worker (got {sorted(advertised)})"
        )

    def test_resource_policy_allows_cross_origin_embedders(self, request: pytest.FixtureRequest) -> None:
        """CORS alone does not survive a cross-origin-isolated caller.

        A page that sends ``Cross-Origin-Embedder-Policy: require-corp`` —
        which any page using ``SharedArrayBuffer`` must — has its own
        fetches blocked unless each response carries
        ``Cross-Origin-Resource-Policy``. CORS being correct does not help,
        and the server sees a perfectly ordinary successful response, so
        this fails invisibly from the operator's side.
        """
        import httpx

        port = self._port(request)
        resp = httpx.post(
            f"http://127.0.0.1:{port}/echo_int",
            content=_unary_request_body("echo_int", value=1),
            headers={"Content-Type": "application/vnd.apache.arrow.stream", "Origin": _CORS_ORIGIN},
            timeout=5.0,
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.content[:200]!r}"
        if "access-control-allow-origin" not in resp.headers:
            pytest.skip("server does not implement CORS")
        # Deliberately not skipped when absent. Reaching here means the port
        # supplied the CORS fixture, i.e. declared browser support; a missing
        # CORP is then a real gap, and skipping on absence would let exactly
        # the servers that need fixing report green.
        corp = resp.headers.get("cross-origin-resource-policy")
        assert corp == _CORP_VALUE, (
            f"Cross-Origin-Resource-Policy is {corp!r}, expected {_CORP_VALUE!r}; "
            f"a caller under COEP require-corp has this response blocked without it"
        )

    @pytest.mark.parametrize("header", sorted(_ALWAYS_EXPOSED))
    def test_failure_path_headers_are_exposed(self, request: pytest.FixtureRequest, header: str) -> None:
        """Headers that only ride failures must be exposed too.

        The derived check above can only see what ``OPTIONS /health``
        advertises, which is a *success*-path surface. These headers appear
        on errors and rejections instead, so nothing advertises them and a
        derived check structurally cannot reach them — they have to be
        named.

        Each is load-bearing for a browser client and silent when missing:
        ``X-VGI-RPC-Error`` is how a client tells an error response from a
        result, ``VGI-Auth-Reason`` is the machine-readable half of a 401
        (without it a browser client is back to parsing prose), and
        ``X-Request-ID`` is what correlates a failure with the server's own
        log. Each is asserted separately so a failure names the header.
        """
        preflight = self._preflight(self._port(request))
        if "access-control-allow-origin" not in preflight.headers:
            pytest.skip("server does not implement CORS")
        exposed = self._exposed(preflight)
        assert header in exposed or "*" in exposed, (
            f"{header} rides failure responses but is not exposed, so a browser "
            f"client cannot read it (exposed: {sorted(exposed)})"
        )


class TestCorsOffMode:
    """A server with no CORS configured must emit no CORS headers.

    Not gated on any fixture: it runs against the plain conformance worker
    everywhere, because "off by default" is a property every port has to
    hold whether or not it implements the feature. A server that answers
    every origin regardless of configuration is a different bug from one
    that answers none.
    """

    def test_no_cors_headers_by_default(self, conformance_http_port: int) -> None:
        """The default worker answers a preflight without allowing an origin."""
        import httpx

        resp = httpx.options(
            f"http://127.0.0.1:{conformance_http_port}/echo_int",
            headers={"Origin": _CORS_ORIGIN, "Access-Control-Request-Method": "POST"},
            timeout=5.0,
        )
        assert "access-control-allow-origin" not in resp.headers, (
            "an unconfigured server must not grant cross-origin access"
        )

    def test_no_resource_policy_by_default(self, conformance_http_port: int) -> None:
        """``Cross-Origin-Resource-Policy`` is part of the same opt-in.

        It only means anything for a server serving cross-origin callers,
        so a server that grants no cross-origin access has nothing to say
        about resource policy either. Emitting it unconditionally would
        also make the header useless as a signal that CORS is configured.
        """
        import httpx

        resp = httpx.post(
            f"http://127.0.0.1:{conformance_http_port}/echo_int",
            content=_unary_request_body("echo_int", value=1),
            headers={"Content-Type": "application/vnd.apache.arrow.stream", "Origin": _CORS_ORIGIN},
            timeout=5.0,
        )
        assert "cross-origin-resource-policy" not in resp.headers, (
            "a server with no CORS configured must not send a resource policy"
        )


# ---------------------------------------------------------------------------
# Sticky session conformance (HTTP-only, capability-gated)
# ---------------------------------------------------------------------------


class TestSticky:
    """Canonical wire-protocol conformance for HTTP sticky sessions.

    Capability-gated: every test calls :meth:`_skip_unless_sticky` first,
    which probes ``OPTIONS /health`` for ``VGI-Sticky-Enabled: true`` and
    skips otherwise. Cross-language ports without sticky support are
    automatically skipped here; ports that implement sticky must pass.

    The contract under test is the wire shape described in
    ``docs/sticky-sessions-spec.md`` plus the porting-guide section on
    sticky sessions. Tests exercise ``open_counter`` / ``increment_counter``
    / ``close_counter`` on the conformance service which themselves use
    the runtime API ``ctx.open_session`` / ``ctx.session`` / ``ctx.close_session``.

    Most tests need only ``conformance_http_port``. The failure-path tests
    at the end of the group need workers a normal fixture can't stand in
    for, so each one looks up an **optional** fixture and skips when the
    runner doesn't provide it:

    * ``conformance_http_sticky_short_ttl_port`` — a sticky worker whose
      advertised ``VGI-Sticky-Default-TTL`` is short enough (≤5s) for a
      test to outwait, for expiry.
    * ``conformance_http_sticky_peer_ports`` — ``(port_a, port_b)``: two
      sticky workers sharing one AEAD token key, for the wrong-worker check.
    * ``conformance_http_sticky_auth_port`` — a sticky worker that
      authenticates the principal named in the ``X-Conformance-Principal``
      request header, for cross-principal replay.
    """

    def _skip_unless_sticky(self, port: int) -> None:
        """Skip the calling test when the server doesn't advertise sticky support."""
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{port}")
        if not caps.sticky_enabled:
            pytest.skip("server does not advertise VGI-Sticky-Enabled — sticky conformance N/A")

    def _connect(self, port: int) -> Any:
        """Open an HTTP connection to the sticky-enabled conformance worker."""
        from vgi_rpc.http import http_connect

        return http_connect(ConformanceService, f"http://127.0.0.1:{port}")

    def _connect_with_client(self, client: Any) -> Any:
        """Open a connection over a caller-supplied HTTP client (used to pin a principal)."""
        from vgi_rpc.http import http_connect

        return http_connect(ConformanceService, client=client)

    def _require_failure_path_fixture(self, request: pytest.FixtureRequest, name: str) -> Any:
        """Return the named failure-path fixture, or decide between skip and fail.

        Sticky is opt-in, so a runner with no sticky support skips. But a
        server that advertises ``VGI-Sticky-Enabled: true`` and then withholds
        these fixtures would silently drop the session-loss coverage — the
        failure mode this group exists to prevent — so that is an error, not a
        skip. See ``docs/sticky-sessions-spec.md`` §9.1.
        """
        try:
            return request.getfixturevalue(name)
        except pytest.FixtureLookupError:
            pass
        from vgi_rpc.http import http_capabilities

        port: int = request.getfixturevalue("conformance_http_port")
        if not http_capabilities(base_url=f"http://127.0.0.1:{port}").sticky_enabled:
            pytest.skip("server does not advertise VGI-Sticky-Enabled — sticky conformance N/A")
        pytest.fail(
            f"server advertises VGI-Sticky-Enabled but the runner supplies no {name!r} fixture. "
            "Ports claiming sticky support must supply all three failure-path fixtures — "
            "see docs/sticky-sessions-spec.md §9.1.",
        )

    def test_open_and_resume(self, conformance_http_port: int) -> None:
        """A session opened on one call is visible to subsequent calls echoing its token."""
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            initial = sess.open_counter(initial=7)
            assert initial == 7
            assert sess.current_session_token() is not None, "server must mint a VGI-Session token"
            after = sess.increment_counter(by=10)
            assert after == 17, "session state must survive the second call"
            # Multiple increments observe accumulating state — proof we hit
            # the same backend worker across calls.
            assert sess.increment_counter(by=10) == 27
            assert sess.increment_counter(by=10) == 37

    def test_explicit_close(self, conformance_http_port: int) -> None:
        """``close_counter`` evicts the session and clears the client's token."""
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            final = sess.close_counter()
            assert final == 1
            # After close: server emitted VGI-Session-Close: true, so the
            # client's tracking view dropped the token. Without a token on
            # the next call there's no session to bind; the method's own
            # guard raises "no sticky counter bound". The registry-eviction
            # path is exercised separately by test_session_lost_after_explicit_close,
            # which re-presents the stale token to surface SessionLostError.
            assert sess.current_session_token() is None
            with pytest.raises(RpcError):
                sess.increment_counter(by=1)

    def test_open_requires_accept_header(self, conformance_http_port: int) -> None:
        """``ctx.open_session`` raises when the request lacks ``VGI-Session-Accept: true``."""
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy:
            # Calling open_counter outside any with_session_token() block →
            # no VGI-Session-Accept header → server rejects.
            with pytest.raises(RpcError) as excinfo:
                proxy.open_counter(initial=1)
            assert excinfo.value.error_type == "RuntimeError"
            assert "VGI-Session-Accept" in str(excinfo.value)

    def test_session_lost_on_invalid_token(self, conformance_http_port: int) -> None:
        """Presenting a malformed / forged token returns ``SessionLostError`` on the next call."""
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy:
            with proxy.with_session_token(token="not-a-real-token") as sess, pytest.raises(RpcError) as excinfo:
                sess.increment_counter(by=1)
            assert excinfo.value.error_type == "SessionLostError"

    def test_session_lost_after_explicit_close(self, conformance_http_port: int) -> None:
        """A previously-valid token rejected after close — proves the registry actually evicted."""
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy:
            stash: str | None = None
            with proxy.with_session_token() as sess:
                sess.open_counter(initial=99)
                stash = sess.current_session_token()
                sess.close_counter()
            assert stash is not None
            # Re-present the (now-stale) token via a new with_session_token() block.
            with (
                proxy.with_session_token(token=stash) as sess2,
                pytest.raises(RpcError) as excinfo,
            ):
                sess2.increment_counter(by=1)
            assert excinfo.value.error_type == "SessionLostError"

    def test_delete_session_endpoint_idempotent_no_token(self, conformance_http_port: int) -> None:
        """``DELETE /vgi/__session__`` with no token returns 200 (idempotent no-op)."""
        self._skip_unless_sticky(conformance_http_port)
        import httpx

        resp = httpx.delete(f"http://127.0.0.1:{conformance_http_port}/__session__", timeout=5.0)
        assert resp.status_code == 200

    def test_delete_session_endpoint_idempotent_on_garbage_token(self, conformance_http_port: int) -> None:
        """``DELETE /vgi/__session__`` with a garbage token returns 200 (no info leak)."""
        self._skip_unless_sticky(conformance_http_port)
        import httpx

        resp = httpx.delete(
            f"http://127.0.0.1:{conformance_http_port}/__session__",
            headers={"VGI-Session": "garbage"},
            timeout=5.0,
        )
        assert resp.status_code == 200

    def test_describe_unchanged(self) -> None:
        """Enabling sticky must NOT change ``DESCRIBE_VERSION`` or the protocol payload shape.

        Builds an in-process pipe-transport conformance server with
        ``enable_describe=True`` and verifies the describe payload — runs
        regardless of which HTTP fixtures the runner provides (so other-
        language ports without a Python sticky HTTP fixture can still skip
        via :meth:`_skip_unless_sticky` but exercise this contract).
        """
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            desc = introspect(client_transport)
        finally:
            client_transport.close()
            thread.join(timeout=5)
        # PR1 contract: __describe__ wire format is untouched by sticky
        # support. Future changes to DESCRIBE_VERSION should be a
        # deliberate bump with a corresponding cross-language port update.
        assert desc.describe_version == "4"
        # The 3 sticky conformance methods must be visible (since the
        # conformance service defines them) but `is_sticky` is not a
        # describe field — methods look just like normal unary methods.
        for name in ("open_counter", "increment_counter", "close_counter"):
            assert name in desc.methods, f"sticky conformance method '{name}' must appear in __describe__"
            assert desc.methods[name].method_type == MethodType.UNARY

    def test_capabilities_advertised(self, conformance_http_port: int) -> None:
        """``OPTIONS /health`` advertises ``VGI-Sticky-Enabled: true`` and a default TTL."""
        self._skip_unless_sticky(conformance_http_port)
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{conformance_http_port}")
        assert caps.sticky_enabled is True
        # Default TTL is operator-tunable. Conformance just requires it be advertised as a positive int.
        assert caps.sticky_default_ttl is not None and caps.sticky_default_ttl > 0

    def test_drain_rejects_new_opens(self, conformance_http_port: int) -> None:
        """After server enters drain mode, ``ctx.open_session`` raises ``ServerDrainingError``.

        Existing-session calls continue to serve (the contract: drain
        lets in-flight sessions complete but rejects new ones).

        Uses the conformance server's test-only ``POST /__test_drain__``
        admin endpoint to flip the drain flag without sending SIGTERM
        (which would kill the subprocess fixture mid-test). Ports
        implementing drain MUST expose the same admin endpoint on
        their conformance server for this test to run; ports that
        don't expose it skip via the HTTP 404. The test always clears
        the flag (``DELETE /__test_drain__``) on exit so subsequent
        tests in the same fixture session aren't poisoned.
        """
        self._skip_unless_sticky(conformance_http_port)
        import httpx

        url_base = f"http://127.0.0.1:{conformance_http_port}"

        # Probe the admin endpoint up front so we skip cleanly if the
        # conformance server doesn't expose it (other-language ports
        # without drain admin support).
        probe = httpx.delete(f"{url_base}/__test_drain__", timeout=5.0)
        if probe.status_code == 404:
            pytest.skip(
                "conformance server doesn't expose /__test_drain__ admin endpoint — "
                "drain conformance N/A for this port",
            )

        try:
            with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as existing:
                # Step 1: open a session BEFORE draining — it stays valid
                # after drain (the contract: existing sessions continue
                # to serve).
                existing.open_counter(initial=10)

                # Step 2: flip the drain flag via the admin endpoint.
                drain_resp = httpx.post(f"{url_base}/__test_drain__", timeout=5.0)
                assert drain_resp.status_code in (200, 204), (
                    f"unexpected status {drain_resp.status_code} from POST /__test_drain__"
                )

                # Step 3: existing session still works.
                assert existing.increment_counter(by=5) == 15, (
                    "existing-session calls must continue to serve during drain"
                )

                # Step 4: opening a NEW session while draining raises ServerDrainingError.
                with proxy.with_session_token() as new_sess, pytest.raises(RpcError) as excinfo:
                    new_sess.open_counter(initial=1)
                assert excinfo.value.error_type == "ServerDrainingError", (
                    f"expected ServerDrainingError, got {excinfo.value.error_type}"
                )

                existing.close_counter()
        finally:
            # Always clear the drain flag so subsequent tests in this
            # session-scoped fixture aren't poisoned.
            httpx.delete(f"{url_base}/__test_drain__", timeout=5.0)

    def test_echo_header_round_trip(self, conformance_http_port: int) -> None:
        """Echo headers advertised by the server are captured + replayed by a conformant client.

        Capability-gated on ``VGI-Sticky-Echo-Headers`` so deployments
        without echo-header support skip cleanly. The conformance server
        is configured with a fixed marker echo header
        (``x-vgi-conformance-echo: conformance-fixed-marker``) so this
        test has a stable contract to exercise; real deployments
        substitute their own (e.g. ``fly-force-instance-id`` on Fly).

        Cross-language ports that implement sticky must implement the
        same capture/replay contract to pass this test — see
        ``docs/sticky-sessions-spec.md`` for the ``VGI-Echo-<name>``
        wire shape.
        """
        self._skip_unless_sticky(conformance_http_port)
        from vgi_rpc.http import http_capabilities

        caps = http_capabilities(base_url=f"http://127.0.0.1:{conformance_http_port}")
        if not caps.sticky_echo_headers:
            pytest.skip(
                "server doesn't advertise sticky echo headers — echo conformance N/A",
            )
        expected_name = "x-vgi-conformance-echo"
        assert expected_name in caps.sticky_echo_headers, (
            f"conformance server must advertise the {expected_name!r} echo header; got {caps.sticky_echo_headers!r}"
        )
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            # Opening a session must populate the captured echo headers
            # via the VGI-Echo-* response header. We check the captured
            # map directly (the contract surface) — the *next* call would
            # carry the header on the wire, but verifying that requires
            # server-side echo-back, which is out of scope for the
            # conformance service. The capture proves the contract.
            sess.open_counter(initial=1)
            captured = dict(sess.current_echo_headers())
            assert expected_name in captured, (
                f"client must capture VGI-Echo-{expected_name} into current_echo_headers(); got {captured!r}"
            )
            assert captured[expected_name] == "conformance-fixed-marker", (
                f"captured value must round-trip the server-configured marker; got {captured[expected_name]!r}"
            )

    # ------------------------------------------------------------------
    # Streaming methods that share a sticky session
    # ------------------------------------------------------------------
    #
    # The unary tests above prove the open / resume / close lifecycle on
    # one-shot HTTP calls.  The tests below prove the same lifecycle holds
    # when the session is touched by *streaming* RPCs: producer streams
    # (multiple HTTP turns per call) and exchange streams (one HTTP turn
    # per exchange round-trip).  Every iteration re-enters the sticky
    # middleware so the spec must hold on each turn, not just the first.

    def test_producer_stream_resumes_session(self, conformance_http_port: int) -> None:
        """A producer stream sees the session counter on every iteration.

        Open the counter at value 0, then drive a five-batch producer
        stream that increments and emits the counter on every produce()
        call.  The emitted values prove the same ``_StickyCounter`` is
        rebound via ``ctx.session`` across the multi-request shape of
        producer streams.
        """
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=0)
            batches = list(sess.stream_session_counter(count=5))
            assert len(batches) == 5
            assert [ab.batch.column("value")[0].as_py() for ab in batches] == [1, 2, 3, 4, 5]
            # Unary read-back confirms the producer mutated the same
            # backing object the unary path sees.
            assert sess.increment_counter(by=10) == 15

    def test_exchange_stream_resumes_session(self, conformance_http_port: int) -> None:
        """An exchange stream sees the session counter on every round-trip.

        Each exchange turn is its own HTTP request, so the sticky
        middleware re-resolves the token from scratch every time. Verify
        the counter accumulates across multiple turns and remains
        consistent with a unary read after the stream closes.
        """
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=100)
            running = 100
            with sess.exchange_session_counter() as stream:
                for delta in (1, 2, 3, 4):
                    running += delta
                    inp = AnnotatedBatch.from_pydict({"by": [delta]})
                    out = stream.exchange(inp)
                    assert out.batch.column("value")[0].as_py() == running
            # Sanity check via the unary path — same counter, same value.
            assert sess.increment_counter(by=0) == running

    def test_stream_without_session_raises(self, conformance_http_port: int) -> None:
        """A streaming method that requires a session must surface RpcError when none is bound.

        Drives ``stream_session_counter`` from outside any
        ``with_session_token()`` block — no token presented, no session
        resumed, so the producer raises and the client sees ``RpcError``.
        """
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, pytest.raises(RpcError):
            list(proxy.stream_session_counter(count=3))

    def test_session_shared_between_unary_and_stream(self, conformance_http_port: int) -> None:
        """A single session is visible to both unary and streaming methods.

        Order: unary open → unary increment → producer stream → unary
        increment.  Every step observes the same backing counter,
        proving sticky-session state is transport-shape-agnostic within
        an HTTP session view.
        """
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=10)
            assert sess.increment_counter(by=5) == 15
            batches = list(sess.stream_session_counter(count=3))
            assert [ab.batch.column("value")[0].as_py() for ab in batches] == [16, 17, 18]
            assert sess.increment_counter(by=2) == 20
            final = sess.close_counter()
            assert final == 20

    # ------------------------------------------------------------------
    # Failure paths: method exceptions, expiry, wrong worker, wrong principal
    # ------------------------------------------------------------------
    #
    # Every test below pairs its rejection with a positive control on the
    # same session, so a fixture that hands out broken tokens (or a client
    # that silently drops them) cannot make the rejection pass for the
    # wrong reason.

    def test_session_survives_method_exception(self, conformance_http_port: int) -> None:
        """A method that raises inside a session must not wedge the session.

        Implementations that serialize same-session calls hold a lock for
        the duration of dispatch (spec §5). If that lock leaks when the
        method raises, every subsequent call on the session blocks
        forever — so the follow-up call runs on a worker thread with a
        deadline rather than hanging the suite.
        """
        self._skip_unless_sticky(conformance_http_port)
        with self._connect(conformance_http_port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            with pytest.raises(RpcError):
                sess.raise_value_error(message="boom inside a sticky session")

            values: list[int] = []
            failures: list[Exception] = []

            def _probe() -> None:
                try:
                    values.append(sess.increment_counter(by=1))
                except Exception as exc:
                    failures.append(exc)

            thread = threading.Thread(target=_probe, daemon=True)
            thread.start()
            thread.join(timeout=15)
            assert not thread.is_alive(), (
                "call after an in-session exception never completed — the per-session lock was not released"
            )
            assert not failures, f"call after an in-session exception failed: {failures[0]!r}"
            assert values == [2], f"session state must survive a raising method; got {values}"
            sess.close_counter()

    def test_expired_session_surfaces_session_lost(self, request: pytest.FixtureRequest) -> None:
        """A token presented after its TTL elapses is rejected as ``SessionLostError``.

        Needs a worker whose advertised ``VGI-Sticky-Default-TTL`` is short
        enough to outwait; runners supply one as the optional
        ``conformance_http_sticky_short_ttl_port`` fixture and skip otherwise.

        The contract is that an expired session is refused, not that it is
        refused to the millisecond. Implementations that track expiry in
        whole seconds (the TTL is advertised in whole seconds, so this is a
        reasonable choice) can keep a session alive for up to a second past
        its nominal deadline, depending on where the open landed in the
        current second. Evicting late is conformant; evicting *early* would
        not be. Hence the extra second of slack below — without it this test
        is a coin flip against any second-granularity port.
        """
        port: int = self._require_failure_path_fixture(request, "conformance_http_sticky_short_ttl_port")
        self._skip_unless_sticky(port)

        from vgi_rpc.http import http_capabilities

        ttl = http_capabilities(base_url=f"http://127.0.0.1:{port}").sticky_default_ttl
        assert ttl is not None, "sticky server must advertise VGI-Sticky-Default-TTL"
        assert ttl <= 5, f"short-TTL fixture must advertise a TTL a test can outwait; got {ttl}s"

        with self._connect(port) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            # Positive control: the session works before it ages out.
            assert sess.increment_counter(by=1) == 2
            # ttl + 1s covers whole-second rounding, + 0.5s of scheduling slack.
            time.sleep(ttl + 1.5)
            with pytest.raises(RpcError) as excinfo:
                sess.increment_counter(by=1)
            assert excinfo.value.error_type == "SessionLostError", (
                f"an expired session must surface SessionLostError, got {excinfo.value.error_type}"
            )

    def test_token_from_other_worker_rejected(self, request: pytest.FixtureRequest) -> None:
        """A session token minted by worker A is refused by worker B (spec §3).

        Sessions live in per-worker memory with no shared registry, so a
        misrouted request must fail loudly rather than resolve to a
        stranger's state. Runners supply two workers sharing one AEAD key
        as ``conformance_http_sticky_peer_ports``; sharing the key is what
        makes the test meaningful — the rejection has to come from the
        ``server_id`` comparison, not from a decryption failure.
        """
        ports: tuple[int, int] = self._require_failure_path_fixture(request, "conformance_http_sticky_peer_ports")
        port_a, port_b = ports
        self._skip_unless_sticky(port_a)

        import httpx

        server_ids = {httpx.get(f"http://127.0.0.1:{p}/health", timeout=5.0).json()["server_id"] for p in ports}
        assert len(server_ids) == 2, f"peer fixture must supply two distinct workers; got {server_ids}"

        with self._connect(port_a) as proxy_a, proxy_a.with_session_token() as sess_a:
            sess_a.open_counter(initial=5)
            token = sess_a.detach()
        assert token is not None

        with (
            self._connect(port_b) as proxy_b,
            proxy_b.with_session_token(token=token) as sess_b,
            pytest.raises(RpcError) as excinfo,
        ):
            sess_b.increment_counter(by=1)
        assert excinfo.value.error_type == "SessionLostError", (
            f"a token from another worker must surface SessionLostError, got {excinfo.value.error_type}"
        )

        # Positive control: the same token still resolves on its owner.
        with self._connect(port_a) as proxy_a2, proxy_a2.with_session_token(token=token) as sess_a2:
            assert sess_a2.increment_counter(by=1) == 6, "the owning worker must still honour the token"
            sess_a2.close_counter()

    def test_cross_principal_replay_rejected(self, request: pytest.FixtureRequest) -> None:
        """One principal cannot resume another principal's session (spec §3, §6).

        The session token is bound to its issuing principal via AAD, so a
        stolen token replayed under a different identity must fail. Runners
        supply ``conformance_http_sticky_auth_port``: a sticky worker that
        authenticates the principal named in the
        ``X-Conformance-Principal`` request header.
        """
        port: int = self._require_failure_path_fixture(request, "conformance_http_sticky_auth_port")
        self._skip_unless_sticky(port)

        import httpx

        base_url = f"http://127.0.0.1:{port}"
        principal_header = "X-Conformance-Principal"

        with (
            httpx.Client(base_url=base_url, headers={principal_header: "alice"}) as alice_client,
            self._connect_with_client(alice_client) as alice,
            alice.with_session_token() as alice_sess,
        ):
            alice_sess.open_counter(initial=5)
            assert alice_sess.increment_counter(by=1) == 6, "a principal must be able to use its own session"
            token = alice_sess.detach()
        assert token is not None

        with (
            httpx.Client(base_url=base_url, headers={principal_header: "bob"}) as bob_client,
            self._connect_with_client(bob_client) as bob,
            bob.with_session_token(token=token) as bob_sess,
            pytest.raises(RpcError) as excinfo,
        ):
            bob_sess.increment_counter(by=100)
        assert excinfo.value.error_type == "SessionLostError", (
            f"a replayed cross-principal token must surface SessionLostError, got {excinfo.value.error_type}"
        )

        # Positive control: the rejection was about identity, not the token.
        with (
            httpx.Client(base_url=base_url, headers={principal_header: "alice"}) as alice_client2,
            self._connect_with_client(alice_client2) as alice2,
            alice2.with_session_token(token=token) as alice_sess2,
        ):
            assert alice_sess2.increment_counter(by=1) == 7, "the owning principal must still resume the session"
            alice_sess2.close_counter()
