"""Tests for vgi.rpc — Arrow IPC-based RPC framework."""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from vgi_rpc.http import _HttpProxy, http_connect
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    BidiStream,
    BidiStreamState,
    EmitLog,
    MethodType,
    OutputCollector,
    PipeTransport,
    RpcConnection,
    RpcError,
    RpcServer,
    ServerStream,
    ServerStreamState,
    StreamSession,
    _RpcProxy,
    connect,
    describe_rpc,
    make_pipe_pair,
    rpc_methods,
    run_server,
    serve_pipe,
)
from vgi_rpc.utils import ArrowSerializableDataclass, ArrowType

from .conftest import ConnFactory, _worker_cmd

# ---------------------------------------------------------------------------
# Enum for type fidelity tests
# ---------------------------------------------------------------------------


class Color(Enum):
    """Test enum for type fidelity tests."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


# ---------------------------------------------------------------------------
# State classes for stream implementations
# ---------------------------------------------------------------------------


@dataclass
class GenerateState(ServerStreamState):
    """State for the generate server stream."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector) -> None:
        """Produce the next batch."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class TransformState(BidiStreamState):
    """State for the transform bidi stream."""

    factor: float

    def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
        """Process an input batch."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class FailStreamState(ServerStreamState):
    """State for a stream that fails after the first batch."""

    emitted: bool = False

    def produce(self, out: OutputCollector) -> None:
        """Produce a batch, then fail on the next call."""
        if not self.emitted:
            self.emitted = True
            out.emit_pydict({"x": [1]})
            return
        raise RuntimeError("stream boom")


@dataclass
class FailBidiMidState(BidiStreamState):
    """State for a bidi that fails after processing one batch."""

    factor: float
    count: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
        """Process one batch, then fail on the next."""
        if self.count > 0:
            raise RuntimeError("bidi boom")
        self.count += 1
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class GenerateWithLogsState(ServerStreamState):
    """State for generate with interleaved log messages."""

    count: int
    current: int = 0

    def produce(self, out: OutputCollector) -> None:
        """Produce the next batch with a log message."""
        if self.current >= self.count:
            out.finish()
            return
        out.log(Level.INFO, f"generating batch {self.current}")
        out.emit_pydict({"i": [self.current]})
        self.current += 1


@dataclass
class TransformWithLogsState(BidiStreamState):
    """State for bidi transform with log messages per exchange."""

    factor: float

    def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
        """Process input with a log message."""
        out.log(Level.INFO, f"transforming batch with factor={self.factor}")
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


@dataclass
class PassthroughState(BidiStreamState):
    """State for a passthrough bidi stream."""

    def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
        """Pass through the input batch unchanged."""
        out.emit(input.batch)


@dataclass
class EmptyStreamState(ServerStreamState):
    """State for a stream that immediately finishes."""

    def produce(self, out: OutputCollector) -> None:
        """Finish immediately."""
        out.finish()


# ---------------------------------------------------------------------------
# Test fixtures: Protocol + Implementation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DescribeResult(ArrowSerializableDataclass):
    """Result from describe() — contains a schema and a sample batch."""

    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    sample_batch: Annotated[pa.RecordBatch, ArrowType(pa.binary())]


class RpcFixtureService(Protocol):
    """Service with unary, stream, bidi, and error methods."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def greet(self, name: str) -> str:
        """Greet by name."""
        ...

    def noop(self) -> None:
        """Do nothing."""
        ...

    def generate(self, count: int) -> ServerStream[ServerStreamState]:
        """Generate count batches."""
        ...

    def transform(self, factor: float) -> BidiStream[BidiStreamState]:
        """Scale values by factor."""
        ...

    def fail_unary(self) -> str:
        """Raise ValueError."""
        ...

    def fail_stream(self) -> ServerStream[ServerStreamState]:
        """Stream that fails mid-iteration."""
        ...

    def fail_bidi_mid(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi that fails after first batch."""
        ...

    def describe(self) -> DescribeResult:
        """Return a dataclass with schema and sample batch."""
        ...

    def inspect(self, result: DescribeResult) -> str:
        """Accept a dataclass param and return info about it."""
        ...

    def roundtrip_types(self, color: Color, mapping: dict[str, int], tags: frozenset[int]) -> str:
        """Accept Enum, dict, frozenset and return proof of type fidelity."""
        ...

    def echo_color(self, color: Color) -> Color:
        """Return the color back."""
        ...

    def echo_mapping(self, mapping: dict[str, int]) -> dict[str, int]:
        """Return the mapping back."""
        ...

    def echo_tags(self, tags: frozenset[int]) -> frozenset[int]:
        """Return the tags back."""
        ...

    def greet_with_logs(self, name: str) -> str:
        """Greet by name, emitting log messages."""
        ...

    def generate_with_logs(self, count: int) -> ServerStream[ServerStreamState]:
        """Generate batches with interleaved log messages."""
        ...

    def transform_with_logs(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi transform with log messages per exchange."""
        ...


class RpcFixtureServiceImpl:
    """Implementation of RpcFixtureService."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def greet(self, name: str) -> str:
        """Greet by name."""
        return f"Hello, {name}!"

    def noop(self) -> None:
        """Do nothing."""
        return None

    def generate(self, count: int) -> ServerStream[GenerateState]:
        """Generate count batches."""
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        return ServerStream(output_schema=schema, state=GenerateState(count=count))

    def transform(self, factor: float) -> BidiStream[TransformState]:
        """Scale values by factor."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=TransformState(factor=factor))

    def fail_unary(self) -> str:
        """Raise ValueError."""
        raise ValueError("unary boom")

    def fail_stream(self) -> ServerStream[FailStreamState]:
        """Stream that fails mid-iteration."""
        schema = pa.schema([pa.field("x", pa.int64())])
        return ServerStream(output_schema=schema, state=FailStreamState())

    def fail_bidi_mid(self, factor: float) -> BidiStream[FailBidiMidState]:
        """Bidi that fails after processing one batch."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=FailBidiMidState(factor=factor))

    def describe(self) -> DescribeResult:
        """Return a dataclass with schema and sample batch."""
        schema = pa.schema([pa.field("x", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        return DescribeResult(output_schema=schema, sample_batch=batch)

    def inspect(self, result: DescribeResult) -> str:
        """Accept a dataclass param and return info about it."""
        return f"{len(result.output_schema)}:{result.sample_batch.num_rows}"

    def roundtrip_types(self, color: Color, mapping: dict[str, int], tags: frozenset[int]) -> str:
        """Accept Enum, dict, frozenset and return proof of type fidelity."""
        return f"{color.name}:{isinstance(color, Color)}:{dict(sorted(mapping.items()))}:{sorted(tags)}"

    def echo_color(self, color: Color) -> Color:
        """Return the color back."""
        return color

    def echo_mapping(self, mapping: dict[str, int]) -> dict[str, int]:
        """Return the mapping back."""
        return mapping

    def echo_tags(self, tags: frozenset[int]) -> frozenset[int]:
        """Return the tags back."""
        return tags

    def greet_with_logs(self, name: str, emit_log: EmitLog | None = None) -> str:
        """Greet by name, emitting INFO + DEBUG logs."""
        if emit_log:
            emit_log(Message.info(f"greeting {name}"))
            emit_log(Message.debug("debug detail", detail="extra-info"))
        return f"Hello, {name}!"

    def generate_with_logs(self, count: int, emit_log: EmitLog | None = None) -> ServerStream[GenerateWithLogsState]:
        """Generate batches with interleaved log messages."""
        schema = pa.schema([pa.field("i", pa.int64())])
        # Emit log BEFORE creating the stream — exercises _LogSink buffering
        if emit_log:
            emit_log(Message.info("pre-stream log"))
        return ServerStream(output_schema=schema, state=GenerateWithLogsState(count=count))

    def transform_with_logs(self, factor: float, emit_log: EmitLog | None = None) -> BidiStream[TransformWithLogsState]:
        """Bidi transform with log messages per exchange."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=TransformWithLogsState(factor=factor))


# ---------------------------------------------------------------------------
# Helpers: context managers for server lifecycle
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def rpc_conn(
    protocol: type = RpcFixtureService,
    impl: object | None = None,
    on_log: Callable[[Message], None] | None = None,
) -> Iterator[_RpcProxy]:
    """Start a server thread, yield a proxy, and clean up on exit."""
    if impl is None:
        impl = RpcFixtureServiceImpl()
    with serve_pipe(protocol, impl, on_log=on_log) as proxy:
        yield proxy


@contextlib.contextmanager
def rpc_server_transport(
    protocol: type = RpcFixtureService,
    impl: object | None = None,
) -> Iterator[PipeTransport]:
    """Start a server thread, yield client transport for RpcConnection tests."""
    if impl is None:
        impl = RpcFixtureServiceImpl()
    client_transport, server_transport = make_pipe_pair()
    server = RpcServer(protocol, impl)
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        yield client_transport
    finally:
        client_transport.close()
        thread.join(timeout=5)
        assert not thread.is_alive(), "Server thread did not terminate"


@contextlib.contextmanager
def http_conn(port: int, on_log: Callable[[Message], None] | None = None) -> Iterator[_HttpProxy]:
    """Yield an HTTP proxy connected to a shared server on *port*."""
    with http_connect(RpcFixtureService, f"http://127.0.0.1:{port}", on_log=on_log) as proxy:
        yield proxy


# ---------------------------------------------------------------------------
# Tests: rpc_methods introspection
# ---------------------------------------------------------------------------


class TestRpcMethods:
    """Tests for rpc_methods() protocol introspection."""

    def test_unary_method_detection(self) -> None:
        """Unary methods are detected from scalar return types."""
        methods = rpc_methods(RpcFixtureService)
        assert "add" in methods
        assert methods["add"].method_type == MethodType.UNARY
        assert methods["add"].has_return is True

    def test_noop_method(self) -> None:
        """Methods returning None are detected as unary with has_return=False."""
        methods = rpc_methods(RpcFixtureService)
        assert "noop" in methods
        assert methods["noop"].method_type == MethodType.UNARY
        assert methods["noop"].has_return is False

    def test_server_stream_detection(self) -> None:
        """ServerStream return types are detected as server stream."""
        methods = rpc_methods(RpcFixtureService)
        assert "generate" in methods
        assert methods["generate"].method_type == MethodType.SERVER_STREAM

    def test_bidi_stream_detection(self) -> None:
        """BidiStream return type is detected as bidi stream."""
        methods = rpc_methods(RpcFixtureService)
        assert "transform" in methods
        assert methods["transform"].method_type == MethodType.BIDI_STREAM
        assert methods["transform"].has_return is False

    def test_params_schema(self) -> None:
        """Parameter schemas are derived from type hints."""
        methods = rpc_methods(RpcFixtureService)
        schema = methods["add"].params_schema
        assert len(schema) == 2
        assert schema.field("a").type == pa.float64()
        assert schema.field("b").type == pa.float64()

    def test_result_schema(self) -> None:
        """Result schemas are derived from return type hints."""
        methods = rpc_methods(RpcFixtureService)
        schema = methods["add"].result_schema
        assert len(schema) == 1
        assert schema.field("result").type == pa.float64()

    def test_mixed_method_types(self) -> None:
        """A protocol with all three method types is correctly introspected."""
        methods = rpc_methods(RpcFixtureService)
        assert methods["add"].method_type == MethodType.UNARY
        assert methods["generate"].method_type == MethodType.SERVER_STREAM
        assert methods["transform"].method_type == MethodType.BIDI_STREAM

    def test_method_doc(self) -> None:
        """Method docstrings are captured when available."""
        methods = rpc_methods(RpcFixtureService)
        assert methods["add"].doc == "Add two numbers."

    def test_server_methods_property(self) -> None:
        """RpcServer.methods property returns all protocol methods."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        assert "add" in server.methods
        assert "greet" in server.methods
        assert "noop" in server.methods

    def test_rpc_methods_immutable(self) -> None:
        """rpc_methods() returns an immutable mapping — assignment raises TypeError."""
        methods = rpc_methods(RpcFixtureService)
        with pytest.raises(TypeError):
            methods["add"] = methods["add"]  # type: ignore[index]


# ---------------------------------------------------------------------------
# Tests: Unary calls over pipe transport
# ---------------------------------------------------------------------------


class TestUnary:
    """Tests for unary RPC calls over pipe and subprocess transports."""

    def test_add(self, make_conn: ConnFactory) -> None:
        """Unary float addition roundtrips through Arrow IPC."""
        with make_conn() as proxy:
            result = proxy.add(a=1.5, b=2.5)
            assert result == pytest.approx(4.0)

    def test_greet(self, make_conn: ConnFactory) -> None:
        """Unary string return roundtrips through Arrow IPC."""
        with make_conn() as proxy:
            result = proxy.greet(name="World")
            assert result == "Hello, World!"

    def test_noop(self, make_conn: ConnFactory) -> None:
        """Unary None-returning method returns None."""
        with make_conn() as proxy:
            result = proxy.noop()
            assert result is None

    def test_dataclass_result(self, make_conn: ConnFactory) -> None:
        """Unary method returning ArrowSerializableDataclass roundtrips correctly."""
        with make_conn() as proxy:
            result = proxy.describe()
            assert isinstance(result, DescribeResult)
            assert result.output_schema == pa.schema([pa.field("x", pa.int64())])
            assert result.sample_batch.to_pydict() == {"x": [1, 2, 3]}

    def test_dataclass_param(self, make_conn: ConnFactory) -> None:
        """Unary method accepting ArrowSerializableDataclass param roundtrips correctly."""
        with make_conn() as proxy:
            desc = DescribeResult(
                output_schema=pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())]),
                sample_batch=pa.RecordBatch.from_pydict({"x": [10, 20]}),
            )
            result = proxy.inspect(result=desc)
            assert result == "2:2"


# ---------------------------------------------------------------------------
# Tests: Server stream over pipe transport
# ---------------------------------------------------------------------------


class TestServerStream:
    """Tests for server stream RPC calls over pipe and subprocess transports."""

    def test_generate(self, make_conn: ConnFactory) -> None:
        """Server stream yields the expected AnnotatedBatch objects."""
        with make_conn() as proxy:
            batches = list(proxy.generate(count=3))
            assert len(batches) == 3
            assert batches[0].batch.column("i")[0].as_py() == 0
            assert batches[1].batch.column("value")[0].as_py() == 10
            assert batches[2].batch.column("i")[0].as_py() == 2

    def test_empty_stream(self, make_conn: ConnFactory) -> None:
        """Server stream with count=0 yields no batches."""
        with make_conn() as proxy:
            batches = list(proxy.generate(count=0))
            assert len(batches) == 0

    def test_large_stream(self, make_conn: ConnFactory) -> None:
        """Streaming 100 batches works correctly."""
        with make_conn() as proxy:
            batches = list(proxy.generate(count=100))
            assert len(batches) == 100
            assert batches[99].batch.column("i")[0].as_py() == 99

    def test_stream_session_type(self, make_conn: ConnFactory) -> None:
        """Server stream returns a StreamSession (or HttpStreamSession for HTTP)."""
        from vgi_rpc.http import HttpStreamSession

        with make_conn() as proxy:
            stream = proxy.generate(count=2)
            assert isinstance(stream, (StreamSession, HttpStreamSession))
            batches = list(stream)
            assert len(batches) == 2
            assert all(isinstance(ab, AnnotatedBatch) for ab in batches)


# ---------------------------------------------------------------------------
# Tests: Bidi stream over pipe transport (state + process pattern)
# ---------------------------------------------------------------------------


class TestBidiStream:
    """Tests for bidi stream RPC calls using state + process pattern."""

    def test_bidi_exchange(self, make_conn: ConnFactory) -> None:
        """Bidi stream exchanges batches correctly via BidiSession."""
        with make_conn() as proxy:
            session = proxy.transform(factor=2.0)

            input1 = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1, 2, 3]}))
            output1 = session.exchange(input1)
            assert output1.batch.column("value").to_pylist() == [2, 4, 6]

            input2 = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [10, 20]}))
            output2 = session.exchange(input2)
            assert output2.batch.column("value").to_pylist() == [20, 40]

            session.close()

    def test_bidi_context_manager(self, make_conn: ConnFactory) -> None:
        """BidiSession works as a context manager."""
        with make_conn() as proxy, proxy.transform(factor=2.0) as session:
            output = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [5.0]})))
            assert output.batch.column("value").to_pylist() == [10.0]


# ---------------------------------------------------------------------------
# Tests: RpcConnection (context manager + proxy)
# ---------------------------------------------------------------------------


class TestRpcConnection:
    """Tests for RpcConnection context manager."""

    def test_unary_via_connection(self) -> None:
        """Unary calls work through RpcConnection context manager."""
        with rpc_server_transport() as transport, RpcConnection(RpcFixtureService, transport) as svc:
            assert svc.add(a=10.0, b=20.0) == pytest.approx(30.0)

    def test_stream_via_connection(self) -> None:
        """Server stream works through RpcConnection context manager."""
        with rpc_server_transport() as transport, RpcConnection(RpcFixtureService, transport) as svc:
            assert len(list(svc.generate(count=2))) == 2

    def test_bidi_via_connection(self) -> None:
        """Bidi stream works through RpcConnection context manager."""
        with (
            rpc_server_transport() as transport,
            RpcConnection(RpcFixtureService, transport) as svc,
            svc.transform(factor=10.0) as session,
        ):
            output = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1, 2]})))
            assert output.batch.column("value").to_pylist() == [10, 20]


# ---------------------------------------------------------------------------
# Tests: Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    """Tests for error propagation through the RPC layer."""

    def test_unary_error(self, make_conn: ConnFactory) -> None:
        """Errors from unary methods are raised as RpcError with error details."""
        with make_conn() as proxy, pytest.raises(RpcError, match="unary boom") as exc_info:
            proxy.fail_unary()
        assert exc_info.value.error_type == "ValueError"
        assert "unary boom" in exc_info.value.remote_traceback

    def test_unknown_method_raises(self, make_conn: ConnFactory) -> None:
        """Accessing a non-existent method raises AttributeError."""
        with make_conn() as proxy, pytest.raises(AttributeError, match="no RPC method"):
            proxy.nonexistent()

    def test_stream_error_propagates(self, make_conn: ConnFactory) -> None:
        """Errors from server stream methods are raised as RpcError with error details."""
        with make_conn() as proxy, pytest.raises(RpcError, match="stream boom") as exc_info:
            list(proxy.fail_stream())
        assert exc_info.value.error_type == "RuntimeError"
        assert "stream boom" in exc_info.value.remote_traceback


# ---------------------------------------------------------------------------
# Tests: describe_rpc
# ---------------------------------------------------------------------------


class TestDescribeRpc:
    """Tests for describe_rpc() output."""

    def test_describe_rpc(self) -> None:
        """Describe output includes protocol name, methods, types, and params."""
        desc = describe_rpc(RpcFixtureService)
        assert desc.startswith("RPC Protocol: RpcFixtureService\n")
        assert "  add(unary)" in desc
        assert "  generate(server_stream)" in desc
        assert "  transform(bidi_stream)" in desc
        assert "    doc: Add two numbers." in desc
        assert "double" in desc  # Arrow calls float64 "double"


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and diverse parameter types."""

    def test_optional_param(self) -> None:
        """Optional parameters are nullable in the schema and work at runtime."""

        class OptService(Protocol):
            def maybe(self, x: int, y: str | None = None) -> str: ...

        class OptServiceImpl:
            def maybe(self, x: int, y: str | None = None) -> str:
                return f"{x}-{y}"

        assert rpc_methods(OptService)["maybe"].params_schema.field("y").nullable is True

        with rpc_conn(OptService, OptServiceImpl()) as proxy:
            assert proxy.maybe(x=42) == "42-None"
            assert proxy.maybe(x=1, y="hi") == "1-hi"

    def test_bool_params(self) -> None:
        """Boolean parameters roundtrip correctly."""

        class BoolService(Protocol):
            def check(self, flag: bool) -> bool: ...

        class BoolServiceImpl:
            def check(self, flag: bool) -> bool:
                return not flag

        with rpc_conn(BoolService, BoolServiceImpl()) as proxy:
            assert proxy.check(flag=True) is False
            assert proxy.check(flag=False) is True

    def test_int_params(self) -> None:
        """Integer parameters roundtrip correctly."""

        class IntService(Protocol):
            def double(self, n: int) -> int: ...

        class IntServiceImpl:
            def double(self, n: int) -> int:
                return n * 2

        with rpc_conn(IntService, IntServiceImpl()) as proxy:
            assert proxy.double(n=21) == 42

    def test_bytes_param(self) -> None:
        """Bytes parameters roundtrip correctly."""

        class BytesService(Protocol):
            def echo(self, data: bytes) -> bytes: ...

        class BytesServiceImpl:
            def echo(self, data: bytes) -> bytes:
                return data

        with rpc_conn(BytesService, BytesServiceImpl()) as proxy:
            assert proxy.echo(data=b"hello") == b"hello"

    def test_param_defaults_applied(self) -> None:
        """Default parameter values are sent when the caller omits them."""

        class DefaultService(Protocol):
            def compute(self, x: int, y: int = 42) -> int: ...

        class DefaultServiceImpl:
            def compute(self, x: int, y: int = 42) -> int:
                return x + y

        with rpc_conn(DefaultService, DefaultServiceImpl()) as proxy:
            # Omit y — default of 42 should be merged in
            assert proxy.compute(x=8) == 50
            # Explicit y overrides default
            assert proxy.compute(x=8, y=10) == 18


# ---------------------------------------------------------------------------
# Tests: Error recovery — error then success on same transport
# ---------------------------------------------------------------------------


class TestErrorRecovery:
    """Tests for error recovery: after an error the server handles the next request."""

    def test_unary_error_then_success(self, make_conn: ConnFactory) -> None:
        """Unary error followed by successful unary call."""
        with make_conn() as proxy:
            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()
            assert proxy.add(a=1.0, b=2.0) == pytest.approx(3.0)

    def test_stream_error_then_success(self, make_conn: ConnFactory) -> None:
        """Stream error followed by successful call."""
        with make_conn() as proxy:
            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())
            assert len(list(proxy.generate(count=2))) == 2

    def test_bidi_mid_stream_error_then_success(self, make_conn: ConnFactory) -> None:
        """Bidi error mid-stream followed by successful call."""
        with make_conn() as proxy:
            session = proxy.fail_bidi_mid(factor=2.0)
            output = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0, 2.0]})))
            assert output.batch.column("value").to_pylist() == [2.0, 4.0]

            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [3.0]})))

            assert proxy.add(a=5.0, b=6.0) == pytest.approx(11.0)

    def test_multiple_errors_then_success(self, make_conn: ConnFactory) -> None:
        """Multiple consecutive errors followed by success."""
        with make_conn() as proxy:
            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()
            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()
            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())
            assert proxy.add(a=42.0, b=0.0) == pytest.approx(42.0)

    def test_mixed_errors_and_successes(self, make_conn: ConnFactory) -> None:
        """Alternating errors and successes."""
        with make_conn() as proxy:
            assert proxy.add(a=1.0, b=1.0) == pytest.approx(2.0)

            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()

            assert len(list(proxy.generate(count=3))) == 3

            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())

            assert proxy.add(a=10.0, b=10.0) == pytest.approx(20.0)

            with proxy.transform(factor=5.0) as session:
                output = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [2.0]})))
                assert output.batch.column("value").to_pylist() == [10.0]

            assert proxy.add(a=99.0, b=1.0) == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Tests: Multi-method session — realistic connection reuse scenarios
# ---------------------------------------------------------------------------


class TestMultiMethodSession:
    """Tests exercising many different methods on a single connection.

    Demonstrates that one connection supports arbitrary sequences of
    unary, streaming, bidi, logging, error, and complex-type calls without
    corruption or desync.
    """

    def test_full_session_lifecycle(self, make_conn: ConnFactory) -> None:
        """Realistic session: diverse calls, errors, and recovery on one connection."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            # 1. Simple unary
            assert proxy.add(a=1.0, b=2.0) == pytest.approx(3.0)

            # 2. String unary
            assert proxy.greet(name="Alice") == "Hello, Alice!"

            # 3. Void unary
            assert proxy.noop() is None

            # 4. Server stream
            batches = list(proxy.generate(count=3))
            assert len(batches) == 3
            assert batches[0].batch.column("i").to_pylist() == [0]

            # 5. Unary error — connection must survive
            with pytest.raises(RpcError, match="unary boom") as exc_info:
                proxy.fail_unary()
            assert exc_info.value.error_type == "ValueError"

            # 6. Prove connection still works after error
            assert proxy.add(a=10.0, b=20.0) == pytest.approx(30.0)

            # 7. Bidi stream
            with proxy.transform(factor=3.0) as session:
                out = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0, 2.0, 3.0]})))
                assert out.batch.column("value").to_pylist() == [3.0, 6.0, 9.0]
                out2 = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [10.0]})))
                assert out2.batch.column("value").to_pylist() == [30.0]

            # 8. Stream error — connection must survive
            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())

            # 9. Another unary after stream error
            assert proxy.greet(name="Bob") == "Hello, Bob!"

            # 10. Dataclass round-trip
            desc = proxy.describe()
            assert desc.sample_batch.num_rows == 3
            info = proxy.inspect(result=desc)
            assert info == "1:3"

            # 11. Complex types
            assert proxy.echo_color(color=Color.GREEN) is Color.GREEN
            assert proxy.echo_mapping(mapping={"a": 1, "b": 2}) == {"a": 1, "b": 2}

            # 12. Bidi error mid-stream — connection must survive
            bidi_session = proxy.fail_bidi_mid(factor=2.0)
            ok_out = bidi_session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [5.0]})))
            assert ok_out.batch.column("value").to_pylist() == [10.0]
            with pytest.raises(RpcError, match="bidi boom"):
                bidi_session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [6.0]})))

            # 13. Unary with logging after bidi error
            logs.clear()
            assert proxy.greet_with_logs(name="Carol") == "Hello, Carol!"
            assert any(m.level == Level.INFO and "greeting Carol" in m.message for m in logs)

            # 14. Stream with logs
            logs.clear()
            batches = list(proxy.generate_with_logs(count=2))
            assert len(batches) == 2
            assert any(m.level == Level.INFO for m in logs)

            # 15. Final unary proves the pipe is still clean
            assert proxy.add(a=999.0, b=1.0) == pytest.approx(1000.0)

    def test_many_sequential_calls_same_method(self, make_conn: ConnFactory) -> None:
        """Stress test: many calls to the same method on one connection."""
        with make_conn() as proxy:
            for i in range(50):
                assert proxy.add(a=float(i), b=1.0) == pytest.approx(float(i + 1))

    def test_errors_do_not_leak_into_subsequent_results(self, make_conn: ConnFactory) -> None:
        """Verify that error metadata never appears in a subsequent successful call."""
        with make_conn() as proxy:
            for _ in range(5):
                with pytest.raises(RpcError, match="unary boom"):
                    proxy.fail_unary()
                result = proxy.greet(name="OK")
                assert result == "Hello, OK!"
                # Ensure the result is a clean string, not an error message
                assert "boom" not in result

    def test_interleaved_stream_types(self, make_conn: ConnFactory) -> None:
        """Alternate between server-stream and bidi-stream calls."""
        with make_conn() as proxy:
            for i in range(1, 6):
                # Server stream
                batches = list(proxy.generate(count=i))
                assert len(batches) == i

                # Bidi stream
                with proxy.transform(factor=float(i)) as session:
                    out = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0]})))
                    assert out.batch.column("value").to_pylist() == [float(i)]

    def test_error_between_every_method_type(self, make_conn: ConnFactory) -> None:
        """Error recovery between each method type transition."""
        with make_conn() as proxy:
            # Unary OK
            assert proxy.add(a=1.0, b=1.0) == pytest.approx(2.0)

            # Error
            with pytest.raises(RpcError):
                proxy.fail_unary()

            # Server stream OK
            assert len(list(proxy.generate(count=2))) == 2

            # Error
            with pytest.raises(RpcError):
                list(proxy.fail_stream())

            # Bidi OK
            with proxy.transform(factor=2.0) as session:
                out = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [7.0]})))
                assert out.batch.column("value").to_pylist() == [14.0]

            # Error
            bidi_err = proxy.fail_bidi_mid(factor=1.0)
            bidi_err.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0]})))
            with pytest.raises(RpcError):
                bidi_err.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [2.0]})))

            # Logging method OK
            assert proxy.greet_with_logs(name="Final") == "Hello, Final!"

    def test_complex_types_after_errors(self, make_conn: ConnFactory) -> None:
        """Complex type serialization works correctly after error recovery."""
        with make_conn() as proxy:
            # Error first
            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()

            # Enum round-trip
            assert proxy.echo_color(color=Color.RED) is Color.RED

            # Error again
            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())

            # Dict round-trip
            assert proxy.echo_mapping(mapping={"x": 42}) == {"x": 42}

            # Error again
            with pytest.raises(RpcError, match="unary boom"):
                proxy.fail_unary()

            # Frozenset round-trip
            assert proxy.echo_tags(tags=frozenset({1, 2, 3})) == frozenset({1, 2, 3})

            # Dataclass round-trip
            desc = proxy.describe()
            assert desc.sample_batch.num_rows == 3

            # Multi-type call
            result = proxy.roundtrip_types(
                color=Color.BLUE,
                mapping={"a": 1},
                tags=frozenset({10, 20}),
            )
            assert "BLUE:True" in result

    def test_bidi_multi_exchange_after_stream_error(self, make_conn: ConnFactory) -> None:
        """Multiple bidi exchanges work after a server-stream error."""
        with make_conn() as proxy:
            # Stream error
            with pytest.raises(RpcError, match="stream boom"):
                list(proxy.fail_stream())

            # Multi-exchange bidi session
            with proxy.transform(factor=2.0) as session:
                for val in [1.0, 2.0, 3.0, 4.0, 5.0]:
                    out = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [val]})))
                    assert out.batch.column("value").to_pylist() == [val * 2.0]

            # Connection still works
            assert proxy.add(a=100.0, b=200.0) == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# Tests: Type fidelity — Enum, dict, frozenset round-trips
# ---------------------------------------------------------------------------


class TestTypeFidelity:
    """Tests for as_py() type fidelity fixes (Enum, dict, frozenset)."""

    def test_enum_result_roundtrip(self, make_conn: ConnFactory) -> None:
        """Enum result survives client-side deserialization."""
        with make_conn() as proxy:
            result = proxy.echo_color(color=Color.BLUE)
            assert result is Color.BLUE
            assert isinstance(result, Color)

    def test_dict_result_roundtrip(self, make_conn: ConnFactory) -> None:
        """Dict result survives client-side deserialization."""
        with make_conn() as proxy:
            result = proxy.echo_mapping(mapping={"x": 42, "y": 99})
            assert isinstance(result, dict)
            assert result == {"x": 42, "y": 99}

    def test_frozenset_result_roundtrip(self, make_conn: ConnFactory) -> None:
        """Frozenset result survives client-side deserialization."""
        with make_conn() as proxy:
            result = proxy.echo_tags(tags=frozenset({1, 2, 3}))
            assert isinstance(result, frozenset)
            assert result == frozenset({1, 2, 3})


# ---------------------------------------------------------------------------
# Tests: Annotated[T, ArrowType(...)] schema control
# ---------------------------------------------------------------------------


class TestAnnotatedArrowType:
    """Tests for Annotated[T, ArrowType(...)] on RPC method parameters and results."""

    def test_annotated_param_schema(self) -> None:
        """Annotated[int, ArrowType(pa.int32())] produces int32 param schema."""

        class AnnotatedService(Protocol):
            def precise(self, x: Annotated[int, ArrowType(pa.int32())]) -> int: ...

        methods = rpc_methods(AnnotatedService)
        assert methods["precise"].params_schema.field("x").type == pa.int32()

    def test_annotated_result_schema(self) -> None:
        """Annotated[int, ArrowType(pa.int32())] produces int32 result schema."""

        class AnnotatedService(Protocol):
            def precise(self, x: int) -> Annotated[int, ArrowType(pa.int32())]: ...

        methods = rpc_methods(AnnotatedService)
        assert methods["precise"].result_schema.field("result").type == pa.int32()

    def test_annotated_param_roundtrip(self) -> None:
        """Annotated params roundtrip through the wire correctly."""

        class AnnotatedService(Protocol):
            def add_small(
                self,
                a: Annotated[int, ArrowType(pa.int32())],
                b: Annotated[int, ArrowType(pa.int32())],
            ) -> int: ...

        class AnnotatedServiceImpl:
            def add_small(self, a: int, b: int) -> int:
                return a + b

        with rpc_conn(AnnotatedService, AnnotatedServiceImpl()) as proxy:
            assert proxy.add_small(a=10, b=20) == 30


# ---------------------------------------------------------------------------
# Tests: Null validation — None/not-None type constraints
# ---------------------------------------------------------------------------


class NullValidationService(Protocol):
    """Service for testing null validation on params and results."""

    def add(self, a: float, b: float) -> float:
        """Non-optional params, non-optional result."""
        ...

    def maybe(self, x: int, y: str | None = None) -> str:
        """One required param, one optional param."""
        ...

    def return_optional(self) -> str | None:
        """Return an optional string."""
        ...

    def return_required(self) -> str:
        """Non-optional return type."""
        ...

    def stream_non_optional(self, a: float) -> ServerStream[ServerStreamState]:
        """Server stream with non-optional param."""
        ...

    def bidi_non_optional(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi stream with non-optional param."""
        ...


class NullValidationServiceImpl:
    """Implementation that lets us test null validation."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def maybe(self, x: int, y: str | None = None) -> str:
        """Format x and y."""
        return f"{x}-{y}"

    def return_optional(self) -> str | None:
        """Return None (valid for optional return)."""
        return None

    def return_required(self) -> str:
        """Return None (invalid for non-optional return)."""
        return None  # type: ignore[return-value]  # intentionally wrong

    def stream_non_optional(self, a: float) -> ServerStream[EmptyStreamState]:
        """Return an empty stream."""
        schema = pa.schema([pa.field("x", pa.float64())])
        return ServerStream(output_schema=schema, state=EmptyStreamState())

    def bidi_non_optional(self, factor: float) -> BidiStream[PassthroughState]:
        """Return a passthrough bidi stream."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=PassthroughState())


def _null_conn() -> contextlib.AbstractContextManager[_RpcProxy]:
    """Shorthand for ``rpc_conn(NullValidationService, NullValidationServiceImpl())``."""
    return rpc_conn(NullValidationService, NullValidationServiceImpl())


class TestNullValidation:
    """Tests for None/not-None type constraint enforcement."""

    def test_none_for_non_optional_param_raises_client_side(self) -> None:
        """Passing None for a non-optional param raises TypeError on the client."""
        with _null_conn() as proxy, pytest.raises(TypeError, match="parameter 'a' is not optional but got None"):
            proxy.add(a=None, b=1.0)

    def test_none_for_optional_param_allowed(self) -> None:
        """Passing None for an optional param works fine."""
        with _null_conn() as proxy:
            result = proxy.maybe(x=42, y=None)
            assert result == "42-None"

    def test_non_optional_result_none_raises(self) -> None:
        """Returning None for a non-optional return type raises RpcError."""
        with _null_conn() as proxy, pytest.raises(RpcError, match="expected a non-None return value but got None"):
            proxy.return_required()

    def test_optional_result_none_allowed(self) -> None:
        """Returning None for an optional return type works fine."""
        with _null_conn() as proxy:
            result = proxy.return_optional()
            assert result is None

    def test_none_param_server_stream_raises(self) -> None:
        """Passing None for a non-optional param on a stream method raises TypeError."""
        with _null_conn() as proxy, pytest.raises(TypeError, match="parameter 'a' is not optional but got None"):
            list(proxy.stream_non_optional(a=None))

    def test_none_param_bidi_raises(self) -> None:
        """Passing None for a non-optional param on a bidi method raises TypeError."""
        with _null_conn() as proxy, pytest.raises(TypeError, match="parameter 'factor' is not optional but got None"):
            proxy.bidi_non_optional(factor=None)


# ---------------------------------------------------------------------------
# Tests: Out-of-band log messages (emit_log)
# ---------------------------------------------------------------------------


class TestEmitLog:
    """Tests for out-of-band log message delivery via emit_log."""

    def test_unary_emit_log(self, make_conn: ConnFactory) -> None:
        """Unary method emits log messages that are delivered to on_log callback."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            result = proxy.greet_with_logs(name="Alice")
            assert result == "Hello, Alice!"
            assert len(logs) == 2
            assert logs[0].level == Level.INFO
            assert logs[0].message == "greeting Alice"
            assert logs[1].level == Level.DEBUG
            assert logs[1].message == "debug detail"

    def test_unary_emit_log_no_callback(self, make_conn: ConnFactory) -> None:
        """Unary with on_log=None: logs are silently discarded, result works."""
        with make_conn() as proxy:
            result = proxy.greet_with_logs(name="Bob")
            assert result == "Hello, Bob!"

    def test_server_stream_emit_log(self, make_conn: ConnFactory) -> None:
        """Server stream delivers log messages from OutputCollector to on_log callback."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            batches = list(proxy.generate_with_logs(count=3))
            assert len(batches) == 3
            assert batches[0].batch.column("i")[0].as_py() == 0
            assert batches[2].batch.column("i")[0].as_py() == 2
            # 1 pre-stream log (from emit_log in method body) + 3 per-batch logs (from OutputCollector)
            assert len(logs) == 4
            assert logs[0].message == "pre-stream log"
            assert "generating batch 0" in logs[1].message
            assert "generating batch 2" in logs[3].message

    def test_bidi_emit_log(self, make_conn: ConnFactory) -> None:
        """Bidi stream delivers log messages from OutputCollector during exchange."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            with proxy.transform_with_logs(factor=2.0) as session:
                input1 = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0, 2.0]}))
                output1 = session.exchange(input1)
                assert output1.batch.column("value").to_pylist() == [2.0, 4.0]

                input2 = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [5.0]}))
                output2 = session.exchange(input2)
                assert output2.batch.column("value").to_pylist() == [10.0]

            assert len(logs) == 2
            assert all(m.level == Level.INFO for m in logs)
            assert "factor=2.0" in logs[0].message

    def test_emit_log_multiple_levels(self) -> None:
        """All non-exception log levels are delivered correctly."""

        class MultiLevelService(Protocol):
            def multi_level(self) -> str: ...

        class MultiLevelServiceImpl:
            def multi_level(self, emit_log: EmitLog | None = None) -> str:
                if emit_log:
                    emit_log(Message.error("error msg"))
                    emit_log(Message.warn("warn msg"))
                    emit_log(Message.info("info msg"))
                    emit_log(Message.debug("debug msg"))
                    emit_log(Message.trace("trace msg"))
                return "done"

        logs: list[Message] = []
        with rpc_conn(MultiLevelService, MultiLevelServiceImpl(), on_log=logs.append) as proxy:
            result = proxy.multi_level()
            assert result == "done"
            assert len(logs) == 5
            assert [m.level for m in logs] == [Level.ERROR, Level.WARN, Level.INFO, Level.DEBUG, Level.TRACE]

    def test_emit_log_with_extras(self, make_conn: ConnFactory) -> None:
        """Message.extra kwargs are preserved through the wire."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            result = proxy.greet_with_logs(name="Carol")
            assert result == "Hello, Carol!"
            # The DEBUG message has detail="extra-info"
            debug_msg = next(m for m in logs if m.level == Level.DEBUG)
            assert debug_msg.extra is not None
            assert debug_msg.extra["detail"] == "extra-info"

    def test_emit_log_then_error(self) -> None:
        """Logs are delivered before RpcError is raised."""

        class LogThenErrorService(Protocol):
            def log_then_fail(self) -> str: ...

        class LogThenErrorServiceImpl:
            def log_then_fail(self, emit_log: EmitLog | None = None) -> str:
                if emit_log:
                    emit_log(Message.info("about to fail"))
                raise ValueError("intentional failure")

        logs: list[Message] = []
        with rpc_conn(LogThenErrorService, LogThenErrorServiceImpl(), on_log=logs.append) as proxy:
            with pytest.raises(RpcError, match="intentional failure"):
                proxy.log_then_fail()
            assert len(logs) == 1
            assert logs[0].level == Level.INFO
            assert logs[0].message == "about to fail"

    def test_emit_log_buffered_before_stream(self) -> None:
        """Logs emitted before ServerStream opens are buffered and delivered."""

        @dataclass
        class EarlyLogStreamState(ServerStreamState):
            count: int
            current: int = 0

            def produce(self, out: OutputCollector) -> None:
                if self.current >= self.count:
                    out.finish()
                    return
                out.emit_pydict({"i": [self.current]})
                self.current += 1

        class BufferedLogService(Protocol):
            def stream_with_early_log(self, count: int) -> ServerStream[ServerStreamState]: ...

        class BufferedLogServiceImpl:
            def stream_with_early_log(
                self, count: int, emit_log: EmitLog | None = None
            ) -> ServerStream[EarlyLogStreamState]:
                # Emit log BEFORE creating the stream — exercises _LogSink buffering
                if emit_log:
                    emit_log(Message.info("before stream open"))
                schema = pa.schema([pa.field("i", pa.int64())])
                return ServerStream(output_schema=schema, state=EarlyLogStreamState(count=count))

        logs: list[Message] = []
        with rpc_conn(BufferedLogService, BufferedLogServiceImpl(), on_log=logs.append) as proxy:
            batches = list(proxy.stream_with_early_log(count=2))
            assert len(batches) == 2
            assert len(logs) == 1
            assert logs[0].message == "before stream open"

    def test_emit_log_backward_compatible(self, make_conn: ConnFactory) -> None:
        """Existing methods without emit_log parameter work unchanged."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            result = proxy.add(a=1.5, b=2.5)
            assert result == pytest.approx(4.0)
            assert len(logs) == 0  # No logs emitted

    def test_emit_log_via_connection(self) -> None:
        """Log callback works through RpcConnection context manager."""
        logs: list[Message] = []
        with (
            rpc_server_transport() as transport,
            RpcConnection(RpcFixtureService, transport, on_log=logs.append) as svc,
        ):
            result = svc.greet_with_logs(name="Dave")
            assert result == "Hello, Dave!"
            assert len(logs) == 2


# ---------------------------------------------------------------------------
# Tests: OutputCollector
# ---------------------------------------------------------------------------


class TestOutputCollector:
    """Tests for OutputCollector standalone behavior."""

    def test_emit_arrays(self) -> None:
        """emit_arrays builds a batch from the output schema."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit_arrays([pa.array([1, 2, 3])])
        assert len(out.batches) == 1
        assert out.batches[0].batch.to_pydict() == {"x": [1, 2, 3]}

    def test_emit_pydict(self) -> None:
        """emit_pydict builds a batch from a Python dict."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit_pydict({"x": [10, 20]})
        assert len(out.batches) == 1
        assert out.batches[0].batch.to_pydict() == {"x": [10, 20]}

    def test_double_emit_raises(self) -> None:
        """Emitting a second data batch raises RuntimeError."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit_pydict({"x": [1]})
        with pytest.raises(RuntimeError, match="Only one data batch"):
            out.emit_pydict({"x": [2]})

    def test_emit_with_metadata(self) -> None:
        """emit() accepts optional metadata dict."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit(pa.RecordBatch.from_pydict({"x": [1]}, schema=schema), metadata={"key": "val"})
        assert out.batches[0].custom_metadata is not None

    def test_log_emits_zero_row_batch(self) -> None:
        """log() emits a zero-row batch with log metadata."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.log(Level.INFO, "test message")
        assert len(out.batches) == 1
        assert out.batches[0].batch.num_rows == 0
        assert out.batches[0].custom_metadata is not None

    def test_finish_sets_flag(self) -> None:
        """finish() sets the finished flag."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        assert not out.finished
        out.finish()
        assert out.finished

    def test_log_then_emit_ordering(self) -> None:
        """Log batches appear before data batches in the accumulated list."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.log(Level.DEBUG, "before data")
        out.emit_pydict({"x": [42]})
        assert len(out.batches) == 2
        assert out.batches[0].batch.num_rows == 0  # log
        assert out.batches[1].batch.num_rows == 1  # data


# ---------------------------------------------------------------------------
# Tests: AnnotatedBatch
# ---------------------------------------------------------------------------


class TestAnnotatedBatch:
    """Tests for AnnotatedBatch dataclass."""

    def test_basic_creation(self) -> None:
        """AnnotatedBatch can be created with just a batch."""
        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        ab = AnnotatedBatch(batch=batch)
        assert ab.batch.to_pydict() == {"x": [1, 2, 3]}
        assert ab.custom_metadata is None

    def test_with_metadata(self) -> None:
        """AnnotatedBatch can carry custom metadata."""
        batch = pa.RecordBatch.from_pydict({"x": [1]})
        metadata = pa.KeyValueMetadata({b"key": b"value"})
        ab = AnnotatedBatch(batch=batch, custom_metadata=metadata)
        assert ab.custom_metadata is not None

    def test_frozen(self) -> None:
        """AnnotatedBatch is immutable (frozen dataclass)."""
        batch = pa.RecordBatch.from_pydict({"x": [1]})
        ab = AnnotatedBatch(batch=batch)
        with pytest.raises(AttributeError):
            ab.batch = batch  # type: ignore[misc]

    def test_from_pydict(self) -> None:
        """from_pydict convenience constructor builds an AnnotatedBatch."""
        ab = AnnotatedBatch.from_pydict({"x": [1, 2, 3]})
        assert ab.batch.to_pydict() == {"x": [1, 2, 3]}
        assert ab.custom_metadata is None

    def test_from_pydict_with_schema(self) -> None:
        """from_pydict accepts an explicit schema."""
        schema = pa.schema([pa.field("x", pa.int32())])
        ab = AnnotatedBatch.from_pydict({"x": [1]}, schema=schema)
        assert ab.batch.schema.field("x").type == pa.int32()


# ---------------------------------------------------------------------------
# Tests: Input schema validation
# ---------------------------------------------------------------------------


class TestInputSchemaValidation:
    """Tests for BidiStream.input_schema validation."""

    def test_input_schema_mismatch_raises(self) -> None:
        """Sending a batch with wrong schema raises TypeError."""

        class SchemaCheckService(Protocol):
            def checked_transform(self, factor: float) -> BidiStream[BidiStreamState]: ...

        class SchemaCheckServiceImpl:
            def checked_transform(self, factor: float) -> BidiStream[TransformState]:
                expected = pa.schema([pa.field("value", pa.float64())])
                return BidiStream(
                    output_schema=expected,
                    state=TransformState(factor=factor),
                    input_schema=expected,
                )

        with rpc_conn(SchemaCheckService, SchemaCheckServiceImpl()) as proxy:
            session = proxy.checked_transform(factor=2.0)
            # Wrong column name — should raise TypeError propagated as RpcError
            with pytest.raises(RpcError, match="Input schema mismatch"):
                session.exchange(AnnotatedBatch.from_pydict({"wrong_col": [1.0]}))

    def test_input_schema_match_succeeds(self) -> None:
        """Sending a batch with correct schema works normally."""

        class SchemaCheckService(Protocol):
            def checked_transform(self, factor: float) -> BidiStream[BidiStreamState]: ...

        class SchemaCheckServiceImpl:
            def checked_transform(self, factor: float) -> BidiStream[TransformState]:
                expected = pa.schema([pa.field("value", pa.float64())])
                return BidiStream(
                    output_schema=expected,
                    state=TransformState(factor=factor),
                    input_schema=expected,
                )

        with (
            rpc_conn(SchemaCheckService, SchemaCheckServiceImpl()) as proxy,
            proxy.checked_transform(factor=3.0) as session,
        ):
            result = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert result.batch.column("value").to_pylist() == [30.0]


# ---------------------------------------------------------------------------
# Tests: State mutation across calls
# ---------------------------------------------------------------------------


class TestStateMutation:
    """Tests verifying state is mutated across produce/process calls."""

    def test_server_stream_state_mutation(self) -> None:
        """GenerateState.current increments across produce() calls."""
        state = GenerateState(count=3)
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])

        out1 = OutputCollector(schema)
        state.produce(out1)
        assert state.current == 1
        assert not out1.finished

        out2 = OutputCollector(schema)
        state.produce(out2)
        assert state.current == 2

        out3 = OutputCollector(schema)
        state.produce(out3)
        assert state.current == 3

        out4 = OutputCollector(schema)
        state.produce(out4)
        assert out4.finished

    def test_bidi_state_across_exchanges(self) -> None:
        """Bidi state persists across multiple process() calls."""

        @dataclass
        class CountingState(BidiStreamState):
            factor: float
            call_count: int = 0

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                self.call_count += 1
                scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
                out.emit_arrays([scaled])

        state = CountingState(factor=2.0)
        schema = pa.schema([pa.field("value", pa.float64())])

        for i in range(5):
            inp = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [float(i)]}))
            out = OutputCollector(schema)
            state.process(inp, out)

        assert state.call_count == 5


# ---------------------------------------------------------------------------
# Tests: Generic ServerStream/BidiStream type introspection
# ---------------------------------------------------------------------------


class TestGenericStreamIntrospection:
    """Tests that generic ServerStream[S] / BidiStream[S] are correctly introspected."""

    def test_generic_server_stream_detected(self) -> None:
        """ServerStream[SomeState] return types are detected as server_stream."""

        class GenericService(Protocol):
            def gen(self, n: int) -> ServerStream[GenerateState]: ...

        methods = rpc_methods(GenericService)
        assert methods["gen"].method_type == MethodType.SERVER_STREAM

    def test_generic_bidi_stream_detected(self) -> None:
        """BidiStream[SomeState] return types are detected as bidi_stream."""

        class GenericService(Protocol):
            def xform(self, f: float) -> BidiStream[TransformState]: ...

        methods = rpc_methods(GenericService)
        assert methods["xform"].method_type == MethodType.BIDI_STREAM


# ---------------------------------------------------------------------------
# Tests: State serialization round-trip (ArrowSerializableDataclass)
# ---------------------------------------------------------------------------


class TestStateSerializationRoundTrip:
    """Tests that stream state can be serialized and deserialized for HTTP transport."""

    def test_server_stream_state_roundtrip(self) -> None:
        """ServerStreamState can be serialized, deserialized, and continue producing."""
        state = GenerateState(count=5, current=2)

        # Serialize
        state_bytes = state.serialize_to_bytes()

        # Deserialize
        restored = GenerateState.deserialize_from_bytes(state_bytes)
        assert restored.count == 5
        assert restored.current == 2

        # Continue producing from restored state
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        out = OutputCollector(schema)
        restored.produce(out)
        assert not out.finished
        assert out.batches[0].batch.column("i")[0].as_py() == 2
        assert restored.current == 3

    def test_bidi_stream_state_roundtrip(self) -> None:
        """BidiStreamState can be serialized, deserialized, and continue processing."""
        state = TransformState(factor=3.0)

        # Serialize
        state_bytes = state.serialize_to_bytes()

        # Deserialize
        restored = TransformState.deserialize_from_bytes(state_bytes)
        assert restored.factor == 3.0

        # Continue processing from restored state
        schema = pa.schema([pa.field("value", pa.float64())])
        inp = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [10.0]}))
        out = OutputCollector(schema)
        restored.process(inp, out)
        assert out.batches[0].batch.column("value")[0].as_py() == 30.0

    def test_stateful_bidi_roundtrip(self) -> None:
        """Bidi state with mutation survives serialization round-trip."""
        state = FailBidiMidState(factor=2.0)

        # Process one batch
        schema = pa.schema([pa.field("value", pa.float64())])
        inp = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [5.0]}))
        out = OutputCollector(schema)
        state.process(inp, out)
        assert state.count == 1

        # Serialize after mutation
        state_bytes = state.serialize_to_bytes()
        restored = FailBidiMidState.deserialize_from_bytes(state_bytes)
        assert restored.count == 1
        assert restored.factor == 2.0


# ---------------------------------------------------------------------------
# Tests: Convenience functions (run_server, connect, serve_pipe)
# ---------------------------------------------------------------------------


class TestConvenienceFunctions:
    """Tests for run_server, connect, and serve_pipe convenience APIs."""

    def test_serve_pipe_unary(self) -> None:
        """serve_pipe yields a working proxy for unary calls."""
        with serve_pipe(RpcFixtureService, RpcFixtureServiceImpl()) as svc:
            assert svc.add(a=1.0, b=2.0) == pytest.approx(3.0)

    def test_serve_pipe_stream(self) -> None:
        """serve_pipe yields a working proxy for server stream calls."""
        with serve_pipe(RpcFixtureService, RpcFixtureServiceImpl()) as svc:
            batches = list(svc.generate(count=3))
            assert len(batches) == 3

    def test_serve_pipe_bidi(self) -> None:
        """serve_pipe yields a working proxy for bidi stream calls."""
        with serve_pipe(RpcFixtureService, RpcFixtureServiceImpl()) as svc, svc.transform(factor=2.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
            assert out.batch.column("value").to_pylist() == [10.0]

    def test_serve_pipe_on_log(self) -> None:
        """serve_pipe forwards log messages to the on_log callback."""
        logs: list[Message] = []
        with serve_pipe(RpcFixtureService, RpcFixtureServiceImpl(), on_log=logs.append) as svc:
            svc.greet_with_logs(name="Alice")
        assert len(logs) == 2
        assert logs[0].level == Level.INFO

    def test_connect_unary(self) -> None:
        """Connect yields a working proxy for unary calls over subprocess."""
        with connect(RpcFixtureService, _worker_cmd()) as svc:
            assert svc.add(a=3.0, b=4.0) == pytest.approx(7.0)

    def test_connect_on_log(self) -> None:
        """Connect forwards log messages to the on_log callback."""
        logs: list[Message] = []
        with connect(RpcFixtureService, _worker_cmd(), on_log=logs.append) as svc:
            svc.greet_with_logs(name="Bob")
        assert len(logs) == 2

    def test_run_server_type_errors(self) -> None:
        """run_server raises TypeError on invalid argument combinations."""
        with pytest.raises(TypeError, match="implementation is required"):
            run_server(RpcFixtureService)

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        with pytest.raises(TypeError, match="implementation must be None"):
            run_server(server, RpcFixtureServiceImpl())

        with pytest.raises(TypeError, match="Expected a Protocol class or RpcServer"):
            run_server("not a class")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Tests: Malformed input handling
# ---------------------------------------------------------------------------


class TestMalformedInput:
    """Tests for graceful handling of malformed Arrow IPC data.

    Uses ``serve_one`` directly with ``BytesIO`` buffers so the tests are
    transport-agnostic — the same code path runs for pipe, subprocess, and
    HTTP transports.
    """

    def test_garbage_bytes_returns_rpc_error(self) -> None:
        """serve_one writes an RpcError response when given garbage bytes."""
        from io import BytesIO

        from pyarrow import ipc

        from vgi_rpc.rpc import PipeTransport, _dispatch_log_or_error, _drain_stream

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        req_buf = BytesIO(b"garbage bytes here")
        resp_buf = BytesIO()
        transport = PipeTransport(req_buf, resp_buf)

        with pytest.raises(pa.ArrowInvalid):
            server.serve_one(transport)

        # Error response was written before the raise
        resp_buf.seek(0)
        reader = ipc.open_stream(resp_buf)
        with pytest.raises(RpcError, match="ArrowInvalid"):
            while True:
                batch, cm = reader.read_next_batch_with_custom_metadata()
                _dispatch_log_or_error(batch, cm)
        _drain_stream(reader)

    def test_empty_body_returns_rpc_error(self) -> None:
        """serve_one writes an RpcError response when given an empty body."""
        from io import BytesIO

        from pyarrow import ipc

        from vgi_rpc.rpc import PipeTransport, _dispatch_log_or_error, _drain_stream

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        req_buf = BytesIO(b"")
        resp_buf = BytesIO()
        transport = PipeTransport(req_buf, resp_buf)

        with pytest.raises(pa.ArrowInvalid):
            server.serve_one(transport)

        resp_buf.seek(0)
        reader = ipc.open_stream(resp_buf)
        with pytest.raises(RpcError):
            while True:
                batch, cm = reader.read_next_batch_with_custom_metadata()
                _dispatch_log_or_error(batch, cm)
        _drain_stream(reader)


# ---------------------------------------------------------------------------
# Tests: Request version validation
# ---------------------------------------------------------------------------


class TestRequestVersion:
    """Tests for request version metadata validation."""

    def _write_request_with_metadata(
        self,
        method_name: str,
        params_schema: pa.Schema,
        kwargs: dict[str, object],
        custom_metadata: pa.KeyValueMetadata,
    ) -> bytes:
        """Write a request IPC stream with custom metadata, returning raw bytes."""
        from io import BytesIO

        from vgi_rpc.rpc import _convert_for_arrow

        buf = BytesIO()
        arrays: list[pa.Array[Any]] = []
        for f in params_schema:
            val = _convert_for_arrow(kwargs.get(f.name))
            arrays.append(pa.array([val], type=f.type))
        batch = pa.RecordBatch.from_arrays(arrays, schema=params_schema)
        with pa.ipc.new_stream(buf, params_schema) as writer:
            writer.write_batch(batch, custom_metadata=custom_metadata)
        return buf.getvalue()

    def test_wrong_version_raises(self) -> None:
        """Request with wrong version is rejected with VersionError."""
        from io import BytesIO

        from vgi_rpc.metadata import REQUEST_VERSION_KEY, RPC_METHOD_KEY
        from vgi_rpc.rpc import _dispatch_log_or_error, _drain_stream

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        methods = rpc_methods(RpcFixtureService)
        info = methods["add"]

        req_bytes = self._write_request_with_metadata(
            "add",
            info.params_schema,
            {"a": 1.0, "b": 2.0},
            pa.KeyValueMetadata({RPC_METHOD_KEY: b"add", REQUEST_VERSION_KEY: b"999"}),
        )
        req_buf = BytesIO(req_bytes)
        resp_buf = BytesIO()
        transport = PipeTransport(req_buf, resp_buf)
        server.serve_one(transport)

        resp_buf.seek(0)
        reader = pa.ipc.open_stream(resp_buf)
        with pytest.raises(RpcError, match="Unsupported request version") as exc_info:
            while True:
                batch, cm = reader.read_next_batch_with_custom_metadata()
                _dispatch_log_or_error(batch, cm)
        _drain_stream(reader)
        assert exc_info.value.error_type == "VersionError"

    def test_missing_version_raises(self) -> None:
        """Request with missing version is rejected with VersionError."""
        from io import BytesIO

        from vgi_rpc.metadata import RPC_METHOD_KEY
        from vgi_rpc.rpc import _dispatch_log_or_error, _drain_stream

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        methods = rpc_methods(RpcFixtureService)
        info = methods["add"]

        # Only include method key, no version key
        req_bytes = self._write_request_with_metadata(
            "add",
            info.params_schema,
            {"a": 1.0, "b": 2.0},
            pa.KeyValueMetadata({RPC_METHOD_KEY: b"add"}),
        )
        req_buf = BytesIO(req_bytes)
        resp_buf = BytesIO()
        transport = PipeTransport(req_buf, resp_buf)
        server.serve_one(transport)

        resp_buf.seek(0)
        reader = pa.ipc.open_stream(resp_buf)
        with pytest.raises(RpcError, match=r"Missing vgi_rpc\.request_version") as exc_info:
            while True:
                batch, cm = reader.read_next_batch_with_custom_metadata()
                _dispatch_log_or_error(batch, cm)
        _drain_stream(reader)
        assert exc_info.value.error_type == "VersionError"


# ---------------------------------------------------------------------------
# Tests: Invalid bidi state (HTTP transport)
# ---------------------------------------------------------------------------


class TestInvalidBidiState:
    """Tests for corrupted bidi state over HTTP transport."""

    def test_corrupted_state_raises(self, http_server_port: int) -> None:
        """Corrupted bidi state bytes cause RpcError on next exchange."""
        with http_conn(http_server_port) as proxy:
            session = proxy.transform(factor=2.0)

            # First exchange succeeds — proves session is valid
            out = session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0]})))
            assert out.batch.column("value").to_pylist() == [2.0]

            # Corrupt the state bytes
            session._state_bytes = b"garbage"

            with pytest.raises(RpcError, match=r"Malformed state token|signature verification"):
                session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [2.0]})))
