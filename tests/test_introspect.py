"""Tests for __describe__ introspection feature."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import pyarrow as pa
import pytest

from vgi_rpc.http import _SyncTestClient, http_introspect, make_sync_client
from vgi_rpc.introspect import (
    DESCRIBE_VERSION,
    ServiceDescription,
    _safe_defaults_json,
    _type_name,
    build_describe_batch,
    introspect,
    parse_describe_batch,
)
from vgi_rpc.metadata import DESCRIBE_VERSION_KEY, PROTOCOL_NAME_KEY, REQUEST_VERSION_KEY, SERVER_ID_KEY
from vgi_rpc.rpc import (
    AnnotatedBatch,
    AuthContext,
    CallContext,
    MethodType,
    OutputCollector,
    PipeTransport,
    RpcConnection,
    RpcError,
    RpcServer,
    Stream,
    StreamState,
    make_pipe_pair,
    rpc_methods,
)
from vgi_rpc.utils import ArrowSerializableDataclass

from .test_rpc import RpcFixtureService, RpcFixtureServiceImpl

# ---------------------------------------------------------------------------
# Helper Protocol for testing
# ---------------------------------------------------------------------------


class Color(Enum):
    """Test enum."""

    RED = "red"
    GREEN = "green"


@dataclass(frozen=True)
class Point(ArrowSerializableDataclass):
    """Test ArrowSerializableDataclass."""

    x: float
    y: float


@dataclass
class _SimpleStreamState(StreamState):
    """Minimal stream state for test protocol."""

    done: bool = False

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce once then finish."""
        if self.done:
            out.finish()
            return
        self.done = True
        out.emit_pydict({"v": [1]})


@dataclass
class _SimpleBidiState(StreamState):
    """Minimal bidi state for test protocol."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo the input."""
        out.emit(input.batch)


class _TestProto(Protocol):
    """Protocol with all method types for introspection tests."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def greet(self, name: str, greeting: str = "Hello") -> str:
        """Greet someone."""
        ...

    def noop(self) -> None:
        """Do nothing."""
        ...

    def stream_data(self, count: int) -> Stream[StreamState]:
        """Stream data batches."""
        ...

    def bidi(self, factor: float) -> Stream[StreamState]:
        """Bidirectional stream."""
        ...

    def with_enum(self, color: Color) -> str:
        """Accept an enum param."""
        ...

    def with_optional(self, value: float | None) -> float | None:
        """Accept and return optional."""
        ...


class _TestProtoImpl:
    """Implementation of _TestProto."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def greet(self, name: str, greeting: str = "Hello") -> str:
        """Greet someone."""
        return f"{greeting}, {name}!"

    def noop(self) -> None:
        """Do nothing."""

    def stream_data(self, count: int) -> Stream[_SimpleStreamState]:
        """Stream data batches."""
        return Stream(output_schema=pa.schema([pa.field("v", pa.int64())]), state=_SimpleStreamState())

    def bidi(self, factor: float) -> Stream[_SimpleBidiState]:
        """Bidirectional stream."""
        return Stream(
            output_schema=pa.schema([pa.field("value", pa.float64())]),
            input_schema=pa.schema([pa.field("value", pa.float64())]),
            state=_SimpleBidiState(),
        )

    def with_enum(self, color: Color) -> str:
        """Accept an enum param."""
        return color.name

    def with_optional(self, value: float | None) -> float | None:
        """Accept and return optional."""
        return value


class _EmptyProto(Protocol):
    """Protocol with no methods."""


class _EmptyProtoImpl:
    """Implementation of empty protocol."""


# ---------------------------------------------------------------------------
# Unit tests: _type_name
# ---------------------------------------------------------------------------


class TestTypeName:
    """Tests for _type_name helper."""

    def test_basic_types(self) -> None:
        """Basic Python types produce simple names."""
        assert _type_name(str) == "str"
        assert _type_name(int) == "int"
        assert _type_name(float) == "float"
        assert _type_name(bool) == "bool"
        assert _type_name(bytes) == "bytes"

    def test_none_type(self) -> None:
        """NoneType produces 'None'."""
        assert _type_name(type(None)) == "None"

    def test_optional(self) -> None:
        """Optional types produce 'T | None'."""
        assert _type_name(float | None) == "float | None"
        assert _type_name(str | None) == "str | None"

    def test_enum(self) -> None:
        """Enum subclass produces class name."""
        assert _type_name(Color) == "Color"

    def test_arrow_serializable(self) -> None:
        """ArrowSerializableDataclass produces class name."""
        assert _type_name(Point) == "Point"

    def test_generic_list(self) -> None:
        """list[T] produces 'list[T]'."""
        assert _type_name(list[int]) == "list[int]"
        assert _type_name(list[str]) == "list[str]"

    def test_generic_dict(self) -> None:
        """dict[K, V] produces 'dict[K, V]'."""
        assert _type_name(dict[str, int]) == "dict[str, int]"

    def test_generic_frozenset(self) -> None:
        """frozenset[T] produces 'frozenset[T]'."""
        assert _type_name(frozenset[int]) == "frozenset[int]"

    def test_annotated(self) -> None:
        """Annotated[T, ...] unwraps to T."""
        from typing import Annotated

        assert _type_name(Annotated[int, "some metadata"]) == "int"
        assert _type_name(Annotated[str, 42]) == "str"

    def test_unknown_type_falls_back_to_str(self) -> None:
        """Non-standard types fall back to str(python_type)."""
        from typing import TypeVar

        T = TypeVar("T")
        result = _type_name(T)
        assert result == "~T"


# ---------------------------------------------------------------------------
# Unit tests: _safe_defaults_json
# ---------------------------------------------------------------------------


class TestSafeDefaultsJson:
    """Tests for _safe_defaults_json helper."""

    def test_empty(self) -> None:
        """Empty dict returns None."""
        assert _safe_defaults_json({}) is None

    def test_scalars(self) -> None:
        """Scalar defaults are serialized."""
        result = _safe_defaults_json({"x": 1, "y": "hello", "z": True, "w": None})
        assert result is not None
        parsed = json.loads(result)
        assert parsed == {"x": 1, "y": "hello", "z": True, "w": None}

    def test_enum_default(self) -> None:
        """Enum defaults are serialized as name."""
        result = _safe_defaults_json({"color": Color.RED})
        assert result is not None
        parsed = json.loads(result)
        assert parsed == {"color": "RED"}

    def test_non_serializable_skipped(self) -> None:
        """Non-serializable values are silently skipped."""
        result = _safe_defaults_json({"good": 1, "bad": object()})
        assert result is not None
        parsed = json.loads(result)
        assert parsed == {"good": 1}

    def test_serializable_list_default(self) -> None:
        """List defaults that are JSON-serializable are included."""
        result = _safe_defaults_json({"items": [1, 2, 3]})
        assert result is not None
        parsed = json.loads(result)
        assert parsed == {"items": [1, 2, 3]}

    def test_non_serializable_container_skipped(self) -> None:
        """Container defaults with non-serializable content are skipped."""
        result = _safe_defaults_json({"bad_list": [object()]})
        assert result is None

    def test_all_non_serializable_returns_none(self) -> None:
        """When all values are non-serializable, returns None."""
        result = _safe_defaults_json({"a": object(), "b": object()})
        assert result is None


# ---------------------------------------------------------------------------
# Unit tests: build_describe_batch
# ---------------------------------------------------------------------------


class TestBuildDescribeBatch:
    """Tests for build_describe_batch."""

    def test_correct_schema_and_row_count(self) -> None:
        """Batch has correct schema and one row per method."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        assert batch.num_rows == len(methods)
        assert batch.schema.get_field_index("name") >= 0
        assert batch.schema.get_field_index("method_type") >= 0
        assert batch.schema.get_field_index("doc") >= 0
        assert batch.schema.get_field_index("has_return") >= 0
        assert batch.schema.get_field_index("params_schema_ipc") >= 0
        assert batch.schema.get_field_index("result_schema_ipc") >= 0
        assert batch.schema.get_field_index("param_types_json") >= 0
        assert batch.schema.get_field_index("param_defaults_json") >= 0

    def test_batch_metadata(self) -> None:
        """Batch custom_metadata contains protocol name, versions, server_id."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        assert batch.schema.metadata is None
        assert cm[PROTOCOL_NAME_KEY] == b"TestProto"
        assert cm[REQUEST_VERSION_KEY] == b"1"
        assert cm[DESCRIBE_VERSION_KEY] == DESCRIBE_VERSION.encode()
        assert cm[SERVER_ID_KEY] == b"srv123"

    def test_method_types_correct(self) -> None:
        """Method type column matches expected values."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        rows = batch.to_pydict()
        name_to_type = dict(zip(rows["name"], rows["method_type"], strict=True))
        assert name_to_type["add"] == "unary"
        assert name_to_type["stream_data"] == "stream"
        assert name_to_type["bidi"] == "stream"

    def test_has_return_column(self) -> None:
        """has_return is True for unary returning value, False for None/streams."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        rows = batch.to_pydict()
        name_to_ret = dict(zip(rows["name"], rows["has_return"], strict=True))
        assert name_to_ret["add"] is True
        assert name_to_ret["noop"] is False
        assert name_to_ret["stream_data"] is False
        # bidi returns Stream which has no unary return
        assert name_to_ret["bidi"] is False

    def test_schemas_deserializable(self) -> None:
        """params_schema_ipc and result_schema_ipc are valid Arrow schemas."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        for i in range(batch.num_rows):
            ps_bytes: bytes = batch.column("params_schema_ipc")[i].as_py()
            rs_bytes: bytes = batch.column("result_schema_ipc")[i].as_py()
            ps = pa.ipc.read_schema(pa.py_buffer(ps_bytes))
            rs = pa.ipc.read_schema(pa.py_buffer(rs_bytes))
            assert isinstance(ps, pa.Schema)
            assert isinstance(rs, pa.Schema)

    def test_param_types_json_correct(self) -> None:
        """param_types_json has correct type names."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        rows = batch.to_pydict()
        name_to_pt = dict(zip(rows["name"], rows["param_types_json"], strict=True))
        add_types = json.loads(name_to_pt["add"])
        assert add_types == {"a": "float", "b": "float"}
        enum_types = json.loads(name_to_pt["with_enum"])
        assert enum_types == {"color": "Color"}

    def test_param_defaults_json(self) -> None:
        """param_defaults_json contains defaults for methods that have them."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        rows = batch.to_pydict()
        name_to_pd = dict(zip(rows["name"], rows["param_defaults_json"], strict=True))
        greet_defaults = json.loads(name_to_pd["greet"])
        assert greet_defaults == {"greeting": "Hello"}
        # Methods without defaults have null
        assert name_to_pd["add"] is None

    def test_empty_protocol(self) -> None:
        """Empty protocol produces 0-row batch."""
        methods = rpc_methods(_EmptyProto)
        batch, _cm = build_describe_batch("EmptyProto", methods, "srv123")
        assert batch.num_rows == 0

    def test_doc_column(self) -> None:
        """Doc column contains method docstrings."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        rows = batch.to_pydict()
        name_to_doc = dict(zip(rows["name"], rows["doc"], strict=True))
        assert name_to_doc["add"] == "Add two numbers."
        assert name_to_doc["greet"] == "Greet someone."


# ---------------------------------------------------------------------------
# Unit tests: parse_describe_batch (round-trip)
# ---------------------------------------------------------------------------


class TestParseDescribeBatch:
    """Tests for parse_describe_batch round-trip with build_describe_batch."""

    def test_round_trip(self) -> None:
        """Build then parse produces correct ServiceDescription."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        assert desc.protocol_name == "TestProto"
        assert desc.request_version == "1"
        assert desc.describe_version == DESCRIBE_VERSION
        assert desc.server_id == "srv123"
        assert len(desc.methods) == len(methods)

    def test_method_description_fields(self) -> None:
        """Parsed MethodDescription has correct fields."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        add = desc.methods["add"]
        assert add.name == "add"
        assert add.method_type == MethodType.UNARY
        assert add.has_return is True
        assert add.doc == "Add two numbers."
        assert add.param_types == {"a": "float", "b": "float"}

    def test_schemas_preserved(self) -> None:
        """Params and result schemas are correctly round-tripped."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        add_info = methods["add"]
        add_desc = desc.methods["add"]
        assert add_desc.params_schema == add_info.params_schema
        assert add_desc.result_schema == add_info.result_schema

    def test_defaults_preserved(self) -> None:
        """Param defaults survive the round-trip."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        greet = desc.methods["greet"]
        assert greet.param_defaults == {"greeting": "Hello"}

    def test_empty_protocol_round_trip(self) -> None:
        """Empty protocol round-trips correctly."""
        methods = rpc_methods(_EmptyProto)
        batch, cm = build_describe_batch("EmptyProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        assert desc.protocol_name == "EmptyProto"
        assert len(desc.methods) == 0


# ---------------------------------------------------------------------------
# ServiceDescription.__str__
# ---------------------------------------------------------------------------


class TestServiceDescriptionStr:
    """Tests for ServiceDescription.__str__."""

    def test_readable_output(self) -> None:
        """__str__ produces human-readable output."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)
        text = str(desc)

        assert "TestProto" in text
        assert "srv123" in text
        assert "add(unary)" in text
        assert "stream_data(stream)" in text
        assert "bidi(stream)" in text

    def test_doc_in_output(self) -> None:
        """Methods with docs show doc line in __str__."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)
        text = str(desc)
        # "noop" has no return but has a doc — tests the doc branch for non-returning methods
        assert "doc: Do nothing." in text

    def test_no_doc_method_omits_doc_line(self) -> None:
        """Methods without docs omit the doc line."""
        from vgi_rpc.introspect import MethodDescription

        desc = ServiceDescription(
            protocol_name="Test",
            request_version="1",
            describe_version="1",
            server_id="x",
            methods={
                "nodoc": MethodDescription(
                    name="nodoc",
                    method_type=MethodType.UNARY,
                    doc=None,
                    has_return=True,
                    params_schema=pa.schema([]),
                    result_schema=pa.schema([pa.field("result", pa.float64())]),
                ),
            },
        )
        text = str(desc)
        assert "nodoc(unary)" in text
        assert "doc:" not in text

    def test_empty_protocol_str(self) -> None:
        """Empty protocol produces minimal output."""
        methods = rpc_methods(_EmptyProto)
        batch, cm = build_describe_batch("EmptyProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)
        text = str(desc)
        assert "EmptyProto" in text


# ---------------------------------------------------------------------------
# Integration tests: pipe transport
# ---------------------------------------------------------------------------


def _run_server_thread(server: RpcServer, transport: PipeTransport) -> None:
    """Run the RPC server in a background thread."""
    server.serve(transport)


class TestIntrospectPipe:
    """Integration tests for introspect() over pipe transport."""

    def test_introspect_returns_service_description(self) -> None:
        """introspect() returns ServiceDescription when enable_describe=True."""
        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=_run_server_thread, args=(server, server_transport), daemon=True)
        thread.start()
        try:
            desc = introspect(client_transport)
            assert isinstance(desc, ServiceDescription)
            assert desc.protocol_name == "_TestProto"
            assert len(desc.methods) == len(rpc_methods(_TestProto))
            assert "add" in desc.methods
            assert desc.methods["add"].method_type == MethodType.UNARY
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

    def test_introspect_raises_when_disabled(self) -> None:
        """introspect() raises RpcError when enable_describe=False."""
        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=False)
        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=_run_server_thread, args=(server, server_transport), daemon=True)
        thread.start()
        try:
            with pytest.raises(RpcError, match="Unknown method"):
                introspect(client_transport)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

    def test_normal_rpc_after_introspect(self) -> None:
        """Normal RPC calls work after introspection on the same transport."""
        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=_run_server_thread, args=(server, server_transport), daemon=True)
        thread.start()
        try:
            # Introspect first
            desc = introspect(client_transport)
            assert "add" in desc.methods

            # Then do a normal RPC call
            with RpcConnection(_TestProto, client_transport) as proxy:
                result = proxy.add(a=3.0, b=4.0)
                assert result == 7.0
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

    def test_describe_with_fixture_service(self) -> None:
        """Introspection works with the full RpcFixtureService."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=_run_server_thread, args=(server, server_transport), daemon=True)
        thread.start()
        try:
            desc = introspect(client_transport)
            assert desc.protocol_name == "RpcFixtureService"
            assert "add" in desc.methods
            assert "greet" in desc.methods
            assert "generate" in desc.methods
            assert "transform" in desc.methods
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()


# ---------------------------------------------------------------------------
# Integration tests: HTTP transport
# ---------------------------------------------------------------------------


@pytest.fixture
def describe_http_client() -> Iterator[_SyncTestClient]:
    """Create a sync HTTP client with introspection enabled."""
    server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
    c = make_sync_client(server, signing_key=b"test-key")
    yield c
    c.close()


@pytest.fixture
def describe_http_client_disabled() -> Iterator[_SyncTestClient]:
    """Create a sync HTTP client with introspection disabled."""
    server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=False)
    c = make_sync_client(server, signing_key=b"test-key")
    yield c
    c.close()


class TestHttpIntrospect:
    """Integration tests for http_introspect()."""

    def test_returns_service_description(self, describe_http_client: _SyncTestClient) -> None:
        """http_introspect() returns ServiceDescription when enabled."""
        desc = http_introspect(client=describe_http_client)
        assert isinstance(desc, ServiceDescription)
        assert desc.protocol_name == "_TestProto"
        assert len(desc.methods) == len(rpc_methods(_TestProto))
        assert "add" in desc.methods

    def test_raises_when_disabled(self, describe_http_client_disabled: _SyncTestClient) -> None:
        """http_introspect() raises RpcError when disabled."""
        with pytest.raises(RpcError):
            http_introspect(client=describe_http_client_disabled)

    def test_method_details_correct(self, describe_http_client: _SyncTestClient) -> None:
        """Method details are correct in HTTP introspection."""
        desc = http_introspect(client=describe_http_client)
        add = desc.methods["add"]
        assert add.method_type == MethodType.UNARY
        assert add.has_return is True
        assert add.param_types == {"a": "float", "b": "float"}

    def test_auth_middleware_applies(self) -> None:
        """Auth middleware applies to __describe__ endpoint."""

        def _authenticate(req: object) -> AuthContext:
            raise ValueError("Unauthorized")

        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
        c = make_sync_client(server, signing_key=b"test-key", authenticate=_authenticate)
        try:
            with pytest.raises(RpcError, match="AuthenticationError"):
                http_introspect(client=c)
        finally:
            c.close()

    def test_with_fixture_service(self) -> None:
        """HTTP introspection works with the full RpcFixtureService."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
        c = make_sync_client(server, signing_key=b"test-key")
        try:
            desc = http_introspect(client=c)
            assert desc.protocol_name == "RpcFixtureService"
            assert "add" in desc.methods
            assert "generate" in desc.methods
        finally:
            c.close()


# ---------------------------------------------------------------------------
# RpcServer properties
# ---------------------------------------------------------------------------


class TestRpcServerDescribe:
    """Tests for RpcServer describe-related properties."""

    def test_describe_enabled_true(self) -> None:
        """describe_enabled returns True when enabled."""
        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
        assert server.describe_enabled is True
        assert server._describe_batch is not None

    def test_describe_enabled_false(self) -> None:
        """describe_enabled returns False by default."""
        server = RpcServer(_TestProto, _TestProtoImpl())
        assert server.describe_enabled is False
        assert server._describe_batch is None
