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
    ServiceDescription,
    compute_protocol_hash,
    introspect,
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


class TestProtocolHash:
    """Tests for compute_protocol_hash.

    It takes a method table rather than a describe batch: the batch is one
    *encoding* of the description, and not the same encoding in every port.
    See ``tests/test_protocol_hash.py`` for the canonical-form properties.
    """

    def test_stable_across_calls(self) -> None:
        """Same protocol, same hash -- server identity is not in the preimage."""
        methods = rpc_methods(_TestProto)
        assert compute_protocol_hash("TestProto", methods) == compute_protocol_hash("TestProto", methods)

    def test_changes_with_protocol_name(self) -> None:
        """Different protocol_name, different hash, even for the same methods."""
        methods = rpc_methods(_TestProto)
        assert compute_protocol_hash("TestProto", methods) != compute_protocol_hash("Renamed", methods)

    def test_hex_format(self) -> None:
        """Hash is 64 lowercase hex characters."""
        h = compute_protocol_hash("TestProto", rpc_methods(_TestProto))
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


def _describe(protocol: type, impl: object) -> ServiceDescription:
    """Build a ServiceDescription in-process, without a wire round trip."""
    from vgi_rpc.introspect import _adapt_description
    from vgi_rpc.rpc._reflection import Reflection, ReflectionImpl

    server = RpcServer(protocol, impl, server_id="srv123", enable_describe=True)
    reflection = server.bindings[Reflection.protocol_name].impl
    assert isinstance(reflection, ReflectionImpl)
    return _adapt_description(reflection.describe(server.protocol_name), reflection.list_protocols())


class TestServiceDescriptionStr:
    """Tests for ServiceDescription.__str__."""

    def test_readable_output(self) -> None:
        """__str__ produces human-readable output."""
        desc = _describe(_TestProto, _TestProtoImpl())
        text = str(desc)

        assert "TestProto" in text
        assert "srv123" in text
        assert "add(unary)" in text
        assert "stream_data(stream)" in text
        assert "bidi(stream)" in text

    def test_protocol_hash_in_output(self) -> None:
        """The hash is rendered in the __str__ summary."""
        desc = _describe(_TestProto, _TestProtoImpl())
        text = str(desc)
        assert "protocol_hash:" in text
        assert desc.protocol_hash in text

    def test_empty_protocol_str(self) -> None:
        """Empty protocol produces minimal output."""
        text = str(_describe(_EmptyProto, _EmptyProtoImpl()))
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
            with pytest.raises(RpcError, match="does not host protocol"):
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
    c = make_sync_client(server, token_key=b"test-key")
    yield c
    c.close()


@pytest.fixture
def describe_http_client_disabled() -> Iterator[_SyncTestClient]:
    """Create a sync HTTP client with introspection disabled."""
    server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=False)
    c = make_sync_client(server, token_key=b"test-key")
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
        c = make_sync_client(server, token_key=b"test-key", authenticate=_authenticate)
        try:
            with pytest.raises(RpcError, match="AuthenticationError"):
                http_introspect(client=c)
        finally:
            c.close()

    def test_with_fixture_service(self) -> None:
        """HTTP introspection works with the full RpcFixtureService."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
        c = make_sync_client(server, token_key=b"test-key")
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
        """``enable_describe`` is reflection-binding registration now."""
        server = RpcServer(_TestProto, _TestProtoImpl(), enable_describe=True)
        assert server.describe_enabled is True
        assert "vgi_rpc.Reflection.v1" in server.bindings

    def test_describe_enabled_false(self) -> None:
        """Off by default, and then the protocol is simply not hosted.

        Not routed-and-refusing: a client gets the same "no such protocol"
        answer it would get from any server that does not host one, rather
        than a bespoke "introspection is disabled" it has to special-case.
        """
        server = RpcServer(_TestProto, _TestProtoImpl())
        assert server.describe_enabled is False
        assert "vgi_rpc.Reflection.v1" not in server.bindings
