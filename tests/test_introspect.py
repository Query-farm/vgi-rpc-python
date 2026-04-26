# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for __describe__ introspection feature."""

from __future__ import annotations

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
    build_describe_batch,
    compute_protocol_hash,
    introspect,
    parse_describe_batch,
)
from vgi_rpc.metadata import (
    DESCRIBE_VERSION_KEY,
    PROTOCOL_HASH_KEY,
    PROTOCOL_NAME_KEY,
    REQUEST_VERSION_KEY,
    SERVER_ID_KEY,
)
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
        assert batch.schema.get_field_index("has_return") >= 0
        assert batch.schema.get_field_index("params_schema_ipc") >= 0
        assert batch.schema.get_field_index("result_schema_ipc") >= 0
        assert batch.schema.get_field_index("has_header") >= 0
        assert batch.schema.get_field_index("header_schema_ipc") >= 0
        assert batch.schema.get_field_index("is_exchange") >= 0

    def test_pythonisms_dropped(self) -> None:
        """v4 dropped doc, param_types_json, param_defaults_json, param_docs_json."""
        methods = rpc_methods(_TestProto)
        batch, _cm = build_describe_batch("TestProto", methods, "srv123")
        names = set(batch.schema.names)
        assert "doc" not in names
        assert "param_types_json" not in names
        assert "param_defaults_json" not in names
        assert "param_docs_json" not in names

    def test_batch_metadata(self) -> None:
        """Batch custom_metadata contains protocol name, versions, hash, server_id."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        assert batch.schema.metadata is None
        assert cm[PROTOCOL_NAME_KEY] == b"TestProto"
        assert cm[REQUEST_VERSION_KEY] == b"1"
        assert cm[DESCRIBE_VERSION_KEY] == DESCRIBE_VERSION.encode()
        assert cm[SERVER_ID_KEY] == b"srv123"
        h = cm[PROTOCOL_HASH_KEY].decode()
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

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

    def test_empty_protocol(self) -> None:
        """Empty protocol produces 0-row batch."""
        methods = rpc_methods(_EmptyProto)
        batch, _cm = build_describe_batch("EmptyProto", methods, "srv123")
        assert batch.num_rows == 0


class TestProtocolHash:
    """Tests for compute_protocol_hash."""

    def test_stable_across_calls(self) -> None:
        """Same protocol → same hash, regardless of server_id."""
        methods = rpc_methods(_TestProto)
        b1, _ = build_describe_batch("TestProto", methods, "srv-a")
        b2, _ = build_describe_batch("TestProto", methods, "srv-zzz")
        assert compute_protocol_hash("TestProto", b1) == compute_protocol_hash("TestProto", b2)

    def test_changes_with_protocol_name(self) -> None:
        """Different protocol_name → different hash, even for same methods."""
        methods = rpc_methods(_TestProto)
        b, _ = build_describe_batch("TestProto", methods, "srv-a")
        h1 = compute_protocol_hash("TestProto", b)
        h2 = compute_protocol_hash("Renamed", b)
        assert h1 != h2

    def test_hex_format(self) -> None:
        """Hash is 64 lowercase hex characters."""
        methods = rpc_methods(_TestProto)
        b, _ = build_describe_batch("TestProto", methods, "srv-a")
        h = compute_protocol_hash("TestProto", b)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


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

    def test_schemas_preserved(self) -> None:
        """Params and result schemas are correctly round-tripped."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)

        add_info = methods["add"]
        add_desc = desc.methods["add"]
        assert add_desc.params_schema == add_info.params_schema
        assert add_desc.result_schema == add_info.result_schema

    def test_protocol_hash_round_trip(self) -> None:
        """ServiceDescription.protocol_hash is populated from the wire."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)
        assert len(desc.protocol_hash) == 64
        assert desc.protocol_hash == compute_protocol_hash("TestProto", batch)

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

    def test_protocol_hash_in_output(self) -> None:
        """The hash is rendered in the __str__ summary."""
        methods = rpc_methods(_TestProto)
        batch, cm = build_describe_batch("TestProto", methods, "srv123")
        desc = parse_describe_batch(batch, cm)
        text = str(desc)
        assert "protocol_hash:" in text
        assert desc.protocol_hash in text

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
        # Arrow schema is the wire-canonical type information.
        assert add.params_schema.names == ["a", "b"]

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
