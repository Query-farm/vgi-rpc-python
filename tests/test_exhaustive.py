"""Exhaustive edge-case tests for the vgi-rpc framework.

Covers protocol introspection boundaries, implementation validation, wire
protocol corner cases, transport lifecycle, deserialization edge cases, and
the internal dispatch/log machinery.
"""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from io import BytesIO
from typing import Annotated, NewType, Protocol

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import encode_metadata
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
    _build_params_schema,
    _build_result_schema,
    _classify_return_type,
    _convert_for_arrow,
    _deserialize_params,
    _deserialize_value,
    _dispatch_log_or_error,
    _drain_stream,
    _LogSink,
    _RpcProxy,
    _validate_params,
    _validate_result,
    _write_request,
    describe_rpc,
    make_pipe_pair,
    rpc_methods,
    run_server,
    serve_pipe,
)
from vgi_rpc.utils import ArrowSerializableDataclass, _infer_arrow_type, _is_optional_type, empty_batch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _ModuleColor(Enum):
    """Module-level enum so get_type_hints can resolve it."""

    RED = "red"


class _EnumDefaultProtocol(Protocol):
    """Protocol with enum default (module-level for get_type_hints)."""

    def method(self, color: _ModuleColor = _ModuleColor.RED) -> None: ...


@contextlib.contextmanager
def _pipe_conn(
    protocol: type,
    impl: object,
    on_log: Callable[[Message], None] | None = None,
) -> Iterator[_RpcProxy]:
    """Start a pipe server, yield a proxy, clean up on exit."""
    with serve_pipe(protocol, impl, on_log=on_log) as proxy:
        yield proxy


# ===================================================================
# 1. Protocol introspection edge cases
# ===================================================================


class TestRpcMethodsIntrospection:
    """Edge cases for rpc_methods() protocol introspection."""

    def test_empty_protocol(self) -> None:
        """Protocol with no methods yields empty mapping."""

        class Empty(Protocol):
            """Empty protocol."""

            ...

        methods = rpc_methods(Empty)
        assert len(methods) == 0

    def test_skips_private_methods(self) -> None:
        """Methods starting with _ are skipped."""

        class WithPrivate(Protocol):
            """Protocol with private method."""

            def _internal(self) -> None: ...
            def public(self) -> None: ...

        methods = rpc_methods(WithPrivate)
        assert "public" in methods
        assert "_internal" not in methods

    def test_skips_class_variables(self) -> None:
        """Non-callable attributes are skipped."""

        class WithClassVar(Protocol):
            """Protocol with a class variable."""

            some_value: int  # noqa: D105

            def method(self) -> None: ...

        methods = rpc_methods(WithClassVar)
        # class vars are not callable, so they're skipped
        assert "method" in methods
        assert "some_value" not in methods

    def test_method_with_no_return_annotation(self) -> None:
        """Method without return type defaults to no-return unary."""

        class NoReturn(Protocol):
            """Protocol with unannotated return."""

            def do_thing(self, x: int) -> None: ...

        methods = rpc_methods(NoReturn)
        info = methods["do_thing"]
        assert info.method_type == MethodType.UNARY
        assert info.has_return is False

    def test_method_docstring_captured(self) -> None:
        """Method docstring is captured in RpcMethodInfo.doc."""

        class WithDoc(Protocol):
            """Protocol with docstring."""

            def documented(self) -> int:
                """Return a documented value."""
                ...

        methods = rpc_methods(WithDoc)
        assert methods["documented"].doc == "Return a documented value."

    def test_method_without_docstring(self) -> None:
        """Method without docstring has None doc."""

        class NoDoc(Protocol):
            """Protocol without docstring on method."""

            def bare(self) -> int: ...

        methods = rpc_methods(NoDoc)
        assert methods["bare"].doc is None

    def test_optional_param_nullable(self) -> None:
        """Optional parameter produces nullable Arrow field."""

        class WithOpt(Protocol):
            """Protocol with optional param."""

            def method(self, x: int, y: str | None = None) -> None: ...

        methods = rpc_methods(WithOpt)
        schema = methods["method"].params_schema
        assert schema.field("x").nullable is False
        assert schema.field("y").nullable is True

    def test_param_defaults_captured(self) -> None:
        """Default parameter values are captured."""

        class WithDefaults(Protocol):
            """Protocol with default values."""

            def method(self, x: int, y: str = "hello", z: float = 3.14) -> None: ...

        methods = rpc_methods(WithDefaults)
        defaults = methods["method"].param_defaults
        assert defaults["y"] == "hello"
        assert defaults["z"] == pytest.approx(3.14)
        assert "x" not in defaults

    def test_enum_default_captured(self) -> None:
        """Enum default value is captured."""
        methods = rpc_methods(_EnumDefaultProtocol)
        assert methods["method"].param_defaults["color"] is _ModuleColor.RED


# ===================================================================
# 2. _classify_return_type edge cases
# ===================================================================


class TestClassifyReturnType:
    """Edge cases for return type classification."""

    def test_none_return(self) -> None:
        """None return type is unary with no return."""
        mt, rt, has_ret = _classify_return_type(type(None))
        assert mt == MethodType.UNARY
        assert has_ret is False

    def test_bare_server_stream(self) -> None:
        """Bare ServerStream (no type param) classifies correctly."""
        mt, _, has_ret = _classify_return_type(ServerStream)
        assert mt == MethodType.SERVER_STREAM
        assert has_ret is False

    def test_bare_bidi_stream(self) -> None:
        """Bare BidiStream (no type param) classifies correctly."""
        mt, _, has_ret = _classify_return_type(BidiStream)
        assert mt == MethodType.BIDI_STREAM
        assert has_ret is False

    def test_generic_server_stream(self) -> None:
        """ServerStream[S] classifies correctly."""
        mt, _, has_ret = _classify_return_type(ServerStream[ServerStreamState])
        assert mt == MethodType.SERVER_STREAM
        assert has_ret is False

    def test_generic_bidi_stream(self) -> None:
        """BidiStream[S] classifies correctly."""
        mt, _, has_ret = _classify_return_type(BidiStream[BidiStreamState])
        assert mt == MethodType.BIDI_STREAM
        assert has_ret is False

    def test_primitive_return(self) -> None:
        """Primitive return types are unary with has_return=True."""
        for t in (int, str, float, bool):
            mt, _, has_ret = _classify_return_type(t)
            assert mt == MethodType.UNARY
            assert has_ret is True


# ===================================================================
# 3. _build_params_schema / _build_result_schema
# ===================================================================


class TestBuildSchemas:
    """Edge cases for schema construction."""

    def test_no_params(self) -> None:
        """Empty hints (no params) produces empty schema."""
        schema = _build_params_schema({"self": None, "return": int})
        assert len(schema) == 0

    def test_dataclass_param_binary(self) -> None:
        """ArrowSerializableDataclass param maps to pa.binary()."""

        @dataclass(frozen=True)
        class DC(ArrowSerializableDataclass):
            """Simple dataclass."""

            x: int

        schema = _build_params_schema({"self": None, "data": DC, "return": None})
        assert schema.field("data").type == pa.binary()

    def test_result_schema_none(self) -> None:
        """None result type produces empty schema."""
        schema = _build_result_schema(type(None))
        assert len(schema) == 0

    def test_result_schema_primitive(self) -> None:
        """Primitive result type produces single 'result' field."""
        schema = _build_result_schema(str)
        assert len(schema) == 1
        assert schema.field("result").type == pa.string()

    def test_result_schema_optional(self) -> None:
        """Optional result type produces nullable result field."""
        schema = _build_result_schema(int | None)
        assert schema.field("result").nullable is True
        assert schema.field("result").type == pa.int64()

    def test_result_schema_dataclass_binary(self) -> None:
        """ArrowSerializableDataclass result maps to pa.binary()."""

        @dataclass(frozen=True)
        class DC(ArrowSerializableDataclass):
            """Simple dataclass."""

            x: int

        schema = _build_result_schema(DC)
        assert schema.field("result").type == pa.binary()


# ===================================================================
# 4. Implementation validation
# ===================================================================


class TestValidateImplementation:
    """Edge cases for implementation validation."""

    def test_missing_method_raises(self) -> None:
        """Missing implementation method raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def do_it(self, x: int) -> None: ...

        class Impl:
            """Incomplete implementation."""

            pass

        with pytest.raises(TypeError, match="missing method"):
            RpcServer(P, Impl())

    def test_non_callable_attribute_raises(self) -> None:
        """Non-callable attribute where method expected raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> None: ...

        class Impl:
            """Implementation with attribute instead of method."""

            method = 42  # not callable  # noqa: D105

        with pytest.raises(TypeError, match="not callable"):
            RpcServer(P, Impl())

    def test_missing_parameter_raises(self) -> None:
        """Implementation missing a required parameter raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int, y: str) -> None: ...

        class Impl:
            """Implementation missing param y."""

            def method(self, x: int) -> None: ...

        with pytest.raises(TypeError, match="missing parameter"):
            RpcServer(P, Impl())

    def test_extra_required_param_raises(self) -> None:
        """Implementation with extra required param raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> None: ...

        class Impl:
            """Implementation with extra required parameter."""

            def method(self, x: int, extra: str) -> None: ...

        with pytest.raises(TypeError, match="not defined in"):
            RpcServer(P, Impl())

    def test_extra_optional_param_allowed(self) -> None:
        """Implementation with extra optional param is allowed."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> None: ...

        class Impl:
            """Implementation with extra optional parameter."""

            def method(self, x: int, extra: str = "default") -> None: ...

        # Should not raise
        RpcServer(P, Impl())

    def test_emit_log_param_allowed(self) -> None:
        """Implementation with emit_log param is allowed without Protocol declaring it."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> int: ...

        class Impl:
            """Implementation with emit_log."""

            def method(self, x: int, emit_log: EmitLog | None = None) -> int:
                """Return x."""
                return x

        server = RpcServer(P, Impl())
        assert "method" in server.emit_log_methods

    def test_multiple_validation_errors(self) -> None:
        """All validation errors are reported in one exception."""

        class P(Protocol):
            """Protocol with two methods."""

            def a(self) -> None: ...
            def b(self, x: int) -> None: ...

        class Impl:
            """Missing both methods."""

            pass

        with pytest.raises(TypeError, match="missing method.*\n.*missing method"):
            RpcServer(P, Impl())


# ===================================================================
# 5. _dispatch_log_or_error edge cases
# ===================================================================


class TestDispatchLogOrError:
    """Edge cases for _dispatch_log_or_error."""

    def _make_zero_row_batch(self) -> pa.RecordBatch:
        """Create a zero-row batch."""
        return empty_batch(pa.schema([pa.field("x", pa.int64())]))

    def _make_data_batch(self) -> pa.RecordBatch:
        """Create a single-row data batch."""
        return pa.RecordBatch.from_pydict({"x": [42]})

    def test_none_metadata_returns_false(self) -> None:
        """No metadata => not consumed."""
        assert _dispatch_log_or_error(self._make_zero_row_batch(), None) is False

    def test_nonzero_rows_returns_false(self) -> None:
        """Data batch with rows => not consumed even if metadata present."""
        md = encode_metadata({
            "vgi_rpc.log_level": "INFO",
            "vgi_rpc.log_message": "test",
        })
        assert _dispatch_log_or_error(self._make_data_batch(), md) is False

    def test_missing_level_returns_false(self) -> None:
        """Metadata with message but no level => not consumed."""
        md = encode_metadata({"vgi_rpc.log_message": "test"})
        assert _dispatch_log_or_error(self._make_zero_row_batch(), md) is False

    def test_missing_message_returns_false(self) -> None:
        """Metadata with level but no message => not consumed."""
        md = encode_metadata({"vgi_rpc.log_level": "INFO"})
        assert _dispatch_log_or_error(self._make_zero_row_batch(), md) is False

    def test_exception_level_raises(self) -> None:
        """EXCEPTION level raises RpcError."""
        extra = json.dumps({"exception_type": "ValueError", "traceback": "tb here"})
        md = encode_metadata({
            "vgi_rpc.log_level": "EXCEPTION",
            "vgi_rpc.log_message": "boom",
            "vgi_rpc.log_extra": extra,
        })
        with pytest.raises(RpcError, match="boom") as exc_info:
            _dispatch_log_or_error(self._make_zero_row_batch(), md)
        assert exc_info.value.error_type == "ValueError"
        assert exc_info.value.remote_traceback == "tb here"

    def test_exception_without_extra_uses_defaults(self) -> None:
        """EXCEPTION with no extra uses level as error_type."""
        md = encode_metadata({
            "vgi_rpc.log_level": "EXCEPTION",
            "vgi_rpc.log_message": "err",
        })
        with pytest.raises(RpcError) as exc_info:
            _dispatch_log_or_error(self._make_zero_row_batch(), md)
        assert exc_info.value.error_type == "EXCEPTION"
        assert exc_info.value.remote_traceback == ""

    def test_malformed_json_extra_ignored(self) -> None:
        """Malformed JSON in log_extra is silently ignored."""
        md = encode_metadata({
            "vgi_rpc.log_level": "INFO",
            "vgi_rpc.log_message": "test",
            "vgi_rpc.log_extra": "not valid json{{{",
        })
        logs: list[Message] = []
        result = _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=logs.append)
        assert result is True
        assert len(logs) == 1
        assert logs[0].message == "test"

    def test_log_without_callback_silently_consumed(self) -> None:
        """Log batch with no on_log callback is consumed (returns True)."""
        md = encode_metadata({
            "vgi_rpc.log_level": "DEBUG",
            "vgi_rpc.log_message": "debug msg",
        })
        result = _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=None)
        assert result is True

    def test_log_extra_pid_stripped(self) -> None:
        """Internal 'pid' key is stripped from the reconstructed Message.extra."""
        extra = json.dumps({"pid": 12345, "custom_key": "value"})
        md = encode_metadata({
            "vgi_rpc.log_level": "INFO",
            "vgi_rpc.log_message": "msg",
            "vgi_rpc.log_extra": extra,
        })
        logs: list[Message] = []
        _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=logs.append)
        assert logs[0].extra is not None
        assert "pid" not in logs[0].extra
        assert logs[0].extra["custom_key"] == "value"


# ===================================================================
# 6. _convert_for_arrow / _deserialize_value
# ===================================================================


class TestConvertForArrow:
    """Edge cases for _convert_for_arrow."""

    def test_enum_to_name(self) -> None:
        """Enum converts to its .name string."""

        class E(Enum):
            """Test enum."""

            FOO = "foo_val"

        assert _convert_for_arrow(E.FOO) == "FOO"

    def test_frozenset_to_list(self) -> None:
        """Frozenset converts to list."""
        result = _convert_for_arrow(frozenset({1, 2, 3}))
        assert isinstance(result, list)
        assert set(result) == {1, 2, 3}

    def test_dict_to_tuple_list(self) -> None:
        """Dict converts to list of tuples."""
        result = _convert_for_arrow({"a": 1})
        assert result == [("a", 1)]

    def test_passthrough_types(self) -> None:
        """Primitives pass through unchanged."""
        assert _convert_for_arrow(42) == 42
        assert _convert_for_arrow("hello") == "hello"
        assert _convert_for_arrow(None) is None

    def test_dataclass_to_bytes(self) -> None:
        """ArrowSerializableDataclass converts to bytes."""

        @dataclass(frozen=True)
        class DC(ArrowSerializableDataclass):
            """Test dataclass."""

            x: int

        result = _convert_for_arrow(DC(x=5))
        assert isinstance(result, bytes)


class TestDeserializeValue:
    """Edge cases for _deserialize_value."""

    def test_passthrough_unknown_type(self) -> None:
        """Unknown types pass through unchanged."""
        assert _deserialize_value(42, int) == 42
        assert _deserialize_value("hi", str) == "hi"

    def test_none_passthrough(self) -> None:
        """None is not deserialized even for complex types."""
        assert _deserialize_value(None, dict[str, int]) is None

    def test_dict_from_list(self) -> None:
        """List of tuples converts to dict when hint is dict."""
        result = _deserialize_value([("a", 1), ("b", 2)], dict[str, int])
        assert result == {"a": 1, "b": 2}

    def test_frozenset_from_list(self) -> None:
        """List converts to frozenset when hint is frozenset."""
        result = _deserialize_value([1, 2, 3], frozenset[int])
        assert result == frozenset({1, 2, 3})

    def test_enum_from_name(self) -> None:
        """String converts to enum member by name."""

        class Color(Enum):
            """Test enum."""

            RED = "red"

        result = _deserialize_value("RED", Color)
        assert result is Color.RED

    def test_optional_type_unwraps(self) -> None:
        """Optional[dict[str, int]] deserializes inner dict correctly."""
        result = _deserialize_value([("k", 1)], dict[str, int] | None)
        assert result == {"k": 1}


# ===================================================================
# 7. _deserialize_params / _validate_params / _validate_result
# ===================================================================


class TestDeserializeParams:
    """Edge cases for parameter deserialization."""

    def test_none_values_skipped(self) -> None:
        """None param values are not deserialized."""
        kwargs: dict[str, object] = {"x": None}
        _deserialize_params(kwargs, {"x": dict[str, int]})
        assert kwargs["x"] is None

    def test_unknown_param_skipped(self) -> None:
        """Params not in param_types are left unchanged."""
        kwargs: dict[str, object] = {"unknown": [("a", 1)]}
        _deserialize_params(kwargs, {})
        assert kwargs["unknown"] == [("a", 1)]


class TestValidateParams:
    """Edge cases for parameter validation."""

    def test_multiple_none_non_optional(self) -> None:
        """Multiple None values on non-optional params all raise."""
        with pytest.raises(TypeError, match="not optional"):
            _validate_params("method", {"a": None, "b": 1}, {"a": int, "b": int})

    def test_none_on_optional_allowed(self) -> None:
        """None on Optional param does not raise."""
        _validate_params("method", {"a": None}, {"a": int | None})

    def test_none_unknown_param_type_skipped(self) -> None:
        """None on param with no type info is skipped."""
        _validate_params("method", {"a": None}, {})


class TestValidateResult:
    """Edge cases for result validation."""

    def test_falsy_values_pass(self) -> None:
        """Falsy non-None values (0, '', False) are valid returns."""
        _validate_result("method", 0, int)
        _validate_result("method", "", str)
        _validate_result("method", False, bool)

    def test_none_on_none_type_passes(self) -> None:
        """None return on None return type is valid."""
        _validate_result("method", None, type(None))
        _validate_result("method", None, None)

    def test_none_on_non_optional_raises(self) -> None:
        """None return on non-optional type raises TypeError."""
        with pytest.raises(TypeError, match="non-None return"):
            _validate_result("method", None, int)

    def test_none_on_optional_passes(self) -> None:
        """None return on Optional type is valid."""
        _validate_result("method", None, int | None)


# ===================================================================
# 8. _LogSink
# ===================================================================


class TestLogSink:
    """Edge cases for _LogSink buffering."""

    def test_buffer_before_activate(self) -> None:
        """Messages are buffered before activate() is called."""
        sink = _LogSink()
        sink(Message.info("msg1"))
        sink(Message.debug("msg2"))
        assert len(sink._buffer) == 2

    def test_activate_flushes_buffer(self) -> None:
        """Buffered messages are flushed on activate()."""
        sink = _LogSink()
        sink(Message.info("buffered"))

        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            sink.activate(writer, schema)
            # Buffer should be empty after activation
            assert len(sink._buffer) == 0

    def test_direct_write_after_activate(self) -> None:
        """Messages after activate() are written directly, not buffered."""
        sink = _LogSink()
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            sink.activate(writer, schema)
            sink(Message.info("direct"))
            assert len(sink._buffer) == 0


# ===================================================================
# 9. OutputCollector edge cases
# ===================================================================


class TestOutputCollector:
    """Edge cases for OutputCollector."""

    def test_double_emit_raises(self) -> None:
        """Emitting two data batches in one call raises."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit(pa.RecordBatch.from_pydict({"x": [1]}, schema=schema))
        with pytest.raises(RuntimeError, match="Only one data batch"):
            out.emit(pa.RecordBatch.from_pydict({"x": [2]}, schema=schema))

    def test_multiple_logs_before_data(self) -> None:
        """Multiple log messages before a data batch are all collected."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.log(Level.INFO, "log1")
        out.log(Level.DEBUG, "log2")
        out.log(Level.WARN, "log3")
        out.emit_pydict({"x": [1]})
        # 3 log batches + 1 data batch
        assert len(out.batches) == 4

    def test_emit_with_metadata(self) -> None:
        """Emit with custom metadata attaches it to the batch."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.emit(
            pa.RecordBatch.from_pydict({"x": [1]}, schema=schema),
            metadata={"key": "value"},
        )
        assert out.batches[0].custom_metadata is not None

    def test_finish_flag(self) -> None:
        """finish() sets the finished flag."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        assert out.finished is False
        out.finish()
        assert out.finished is True

    def test_output_schema_property(self) -> None:
        """output_schema property returns the schema."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        assert out.output_schema.equals(schema)

    def test_emit_arrays(self) -> None:
        """emit_arrays builds batch from arrays and schema."""
        fields: list[pa.Field[pa.DataType]] = [pa.field("x", pa.int64()), pa.field("y", pa.string())]
        schema = pa.schema(fields)
        out = OutputCollector(schema)
        out.emit_arrays([pa.array([1]), pa.array(["a"])])
        assert out.batches[0].batch.num_rows == 1
        assert out.batches[0].batch.to_pydict() == {"x": [1], "y": ["a"]}


# ===================================================================
# 10. Transport lifecycle edge cases
# ===================================================================


class TestPipeTransportLifecycle:
    """Edge cases for PipeTransport."""

    def test_close_twice(self) -> None:
        """Closing transport twice does not raise."""
        client, server = make_pipe_pair()
        server.close()
        client.close()
        # Second close should not raise — IOBase.close() is idempotent
        client.close()
        server.close()


class TestBidiSessionLifecycle:
    """Edge cases for BidiSession."""

    def test_close_without_exchange(self) -> None:
        """Closing a session without any exchanges does not hang."""

        @dataclass
        class State(BidiStreamState):
            """Passthrough state."""

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Pass through."""
                out.emit(input.batch)

        class P(Protocol):
            """Bidi protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Bidi implementation."""

            def bidi(self) -> BidiStream[State]:
                """Return bidi stream."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return BidiStream(output_schema=schema, state=State())

        with _pipe_conn(P, Impl()) as proxy:
            session = proxy.bidi()
            session.close()

    def test_close_idempotent(self) -> None:
        """Calling close() multiple times is safe."""

        @dataclass
        class State(BidiStreamState):
            """Passthrough state."""

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Pass through."""
                out.emit(input.batch)

        class P(Protocol):
            """Bidi protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Bidi implementation."""

            def bidi(self) -> BidiStream[State]:
                """Return bidi stream."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return BidiStream(output_schema=schema, state=State())

        with _pipe_conn(P, Impl()) as proxy:
            session = proxy.bidi()
            session.close()
            session.close()  # second close should be a no-op

    def test_context_manager(self) -> None:
        """BidiSession works as context manager."""

        @dataclass
        class State(BidiStreamState):
            """Passthrough state."""

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Pass through."""
                out.emit(input.batch)

        class P(Protocol):
            """Bidi protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Bidi implementation."""

            def bidi(self) -> BidiStream[State]:
                """Return bidi stream."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return BidiStream(output_schema=schema, state=State())

        with _pipe_conn(P, Impl()) as proxy, proxy.bidi() as session:
            batch = AnnotatedBatch.from_pydict({"x": [1]})
            result = session.exchange(batch)
            assert result.batch.to_pydict() == {"x": [1]}


# ===================================================================
# 11. Wire protocol edge cases (end-to-end over pipes)
# ===================================================================


class TestWireProtocolEdgeCases:
    """End-to-end edge cases over the pipe transport."""

    def test_empty_server_stream(self) -> None:
        """Server stream that finishes immediately yields no batches."""

        @dataclass
        class EmptyState(ServerStreamState):
            """Immediately finishes."""

            def produce(self, out: OutputCollector) -> None:
                """Finish immediately."""
                out.finish()

        class P(Protocol):
            """Protocol."""

            def stream(self) -> ServerStream[ServerStreamState]: ...

        class Impl:
            """Implementation."""

            def stream(self) -> ServerStream[EmptyState]:
                """Return empty stream."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return ServerStream(output_schema=schema, state=EmptyState())

        with _pipe_conn(P, Impl()) as proxy:
            batches = list(proxy.stream())
            assert batches == []

    def test_unary_error_recovery(self) -> None:
        """After a unary error, the next call succeeds."""

        class P(Protocol):
            """Protocol."""

            def fail(self) -> str: ...
            def ok(self) -> str: ...

        class Impl:
            """Implementation."""

            def fail(self) -> str:
                """Raise."""
                raise ValueError("boom")

            def ok(self) -> str:
                """Return ok."""
                return "ok"

        with _pipe_conn(P, Impl()) as proxy:
            with pytest.raises(RpcError):
                proxy.fail()
            assert proxy.ok() == "ok"

    def test_stream_error_recovery(self) -> None:
        """After a stream error, the next call succeeds."""

        @dataclass
        class FailState(ServerStreamState):
            """Fails immediately."""

            def produce(self, out: OutputCollector) -> None:
                """Raise."""
                raise RuntimeError("stream fail")

        class P(Protocol):
            """Protocol."""

            def stream(self) -> ServerStream[ServerStreamState]: ...
            def ok(self) -> str: ...

        class Impl:
            """Implementation."""

            def stream(self) -> ServerStream[FailState]:
                """Return failing stream."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return ServerStream(output_schema=schema, state=FailState())

            def ok(self) -> str:
                """Return ok."""
                return "ok"

        with _pipe_conn(P, Impl()) as proxy:
            with pytest.raises(RpcError):
                list(proxy.stream())
            assert proxy.ok() == "ok"

    def test_bidi_error_recovery(self) -> None:
        """After a bidi error, the next call succeeds."""

        @dataclass
        class FailState(BidiStreamState):
            """Fails on first process."""

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Raise."""
                raise RuntimeError("bidi fail")

        class P(Protocol):
            """Protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...
            def ok(self) -> str: ...

        class Impl:
            """Implementation."""

            def bidi(self) -> BidiStream[FailState]:
                """Return failing bidi."""
                schema = pa.schema([pa.field("x", pa.int64())])
                return BidiStream(output_schema=schema, state=FailState())

            def ok(self) -> str:
                """Return ok."""
                return "ok"

        with _pipe_conn(P, Impl()) as proxy:
            with proxy.bidi() as session, pytest.raises(RpcError):
                session.exchange(AnnotatedBatch.from_pydict({"x": [1]}))
            assert proxy.ok() == "ok"

    def test_unknown_method_returns_error(self) -> None:
        """Calling a method not in the protocol raises AttributeError on proxy."""

        class P(Protocol):
            """Protocol."""

            def real(self) -> str: ...

        class Impl:
            """Implementation."""

            def real(self) -> str:
                """Return real."""
                return "real"

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(AttributeError, match="no RPC method"):
            proxy.nonexistent()

    def test_falsy_return_values(self) -> None:
        """Falsy values (0, empty string, False) round-trip correctly."""

        class P(Protocol):
            """Protocol."""

            def zero(self) -> int: ...
            def empty(self) -> str: ...
            def false(self) -> bool: ...

        class Impl:
            """Implementation."""

            def zero(self) -> int:
                """Return 0."""
                return 0

            def empty(self) -> str:
                """Return empty string."""
                return ""

            def false(self) -> bool:
                """Return False."""
                return False

        with _pipe_conn(P, Impl()) as proxy:
            assert proxy.zero() == 0
            assert proxy.empty() == ""
            assert proxy.false() is False

    def test_none_return_method(self) -> None:
        """Method returning None works correctly."""

        class P(Protocol):
            """Protocol."""

            def noop(self) -> None: ...

        class Impl:
            """Implementation."""

            def noop(self) -> None:
                """Do nothing."""

        with _pipe_conn(P, Impl()) as proxy:
            assert proxy.noop() is None

    def test_optional_return_none(self) -> None:
        """Method with optional return type can return None."""

        class P(Protocol):
            """Protocol."""

            def maybe(self) -> int | None: ...

        class Impl:
            """Implementation."""

            def maybe(self) -> int | None:
                """Return None."""
                return None

        with _pipe_conn(P, Impl()) as proxy:
            assert proxy.maybe() is None

    def test_optional_return_value(self) -> None:
        """Method with optional return type can return a value."""

        class P(Protocol):
            """Protocol."""

            def maybe(self) -> int | None: ...

        class Impl:
            """Implementation."""

            def maybe(self) -> int | None:
                """Return a value."""
                return 42

        with _pipe_conn(P, Impl()) as proxy:
            assert proxy.maybe() == 42

    def test_server_error_during_init_propagates(self) -> None:
        """Error in server-stream method init (not produce) propagates."""

        class P(Protocol):
            """Protocol."""

            def stream(self) -> ServerStream[ServerStreamState]: ...

        class Impl:
            """Implementation."""

            def stream(self) -> ServerStream[ServerStreamState]:
                """Raise during init."""
                raise ValueError("init boom")

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(RpcError, match="init boom"):
            list(proxy.stream())

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_bidi_error_during_init_propagates(self) -> None:
        """Error in bidi method init (not process) propagates.

        Note: the server thread receives a leftover close stream after the
        error, which triggers an expected PytestUnhandledThreadExceptionWarning.
        """

        class P(Protocol):
            """Protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Implementation."""

            def bidi(self) -> BidiStream[BidiStreamState]:
                """Raise during init."""
                raise ValueError("bidi init boom")

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(RpcError, match="bidi init boom"):
            session = proxy.bidi()
            session.exchange(AnnotatedBatch.from_pydict({"x": [1]}))

    def test_bidi_input_schema_validation(self) -> None:
        """Input schema mismatch in bidi raises error."""

        @dataclass
        class State(BidiStreamState):
            """State."""

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Pass through."""
                out.emit(input.batch)

        class P(Protocol):
            """Protocol."""

            def bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Implementation."""

            def bidi(self) -> BidiStream[State]:
                """Return bidi with strict input schema."""
                out_schema = pa.schema([pa.field("x", pa.int64())])
                in_schema = pa.schema([pa.field("x", pa.int64())])
                return BidiStream(output_schema=out_schema, state=State(), input_schema=in_schema)

        with _pipe_conn(P, Impl()) as proxy, proxy.bidi() as session:
            # Send wrong schema
            wrong_batch = AnnotatedBatch.from_pydict({"y": ["wrong"]})
            with pytest.raises(RpcError, match="schema mismatch"):
                session.exchange(wrong_batch)


# ===================================================================
# 12. emit_log injection
# ===================================================================


class TestEmitLogInjection:
    """Tests for emit_log parameter injection by the framework."""

    def test_emit_log_injected(self) -> None:
        """Framework injects emit_log when method signature includes it."""

        class P(Protocol):
            """Protocol."""

            def greet(self, name: str) -> str: ...

        class Impl:
            """Implementation."""

            def greet(self, name: str, emit_log: EmitLog | None = None) -> str:
                """Greet with logging."""
                if emit_log:
                    emit_log(Message.info(f"hi {name}"))
                return f"Hello, {name}!"

        logs: list[Message] = []
        with _pipe_conn(P, Impl(), on_log=logs.append) as proxy:
            result = proxy.greet(name="World")
            assert result == "Hello, World!"
            assert len(logs) == 1
            assert logs[0].message == "hi World"

    def test_emit_log_not_injected_when_absent(self) -> None:
        """Framework does not inject emit_log when method signature lacks it."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> int: ...

        class Impl:
            """Implementation."""

            def method(self, x: int) -> int:
                """Return x."""
                return x

        server = RpcServer(P, Impl())
        assert "method" not in server.emit_log_methods

    def test_emit_log_buffered_before_stream(self) -> None:
        """Logs emitted before stream opens are buffered and sent."""

        @dataclass
        class State(ServerStreamState):
            """State that finishes immediately."""

            def produce(self, out: OutputCollector) -> None:
                """Finish."""
                out.finish()

        class P(Protocol):
            """Protocol."""

            def stream(self) -> ServerStream[ServerStreamState]: ...

        class Impl:
            """Implementation."""

            def stream(self, emit_log: EmitLog | None = None) -> ServerStream[State]:
                """Stream with pre-log."""
                if emit_log:
                    emit_log(Message.info("before stream"))
                schema = pa.schema([pa.field("x", pa.int64())])
                return ServerStream(output_schema=schema, state=State())

        logs: list[Message] = []
        with _pipe_conn(P, Impl(), on_log=logs.append) as proxy:
            list(proxy.stream())
            assert any(m.message == "before stream" for m in logs)


# ===================================================================
# 13. run_server argument validation
# ===================================================================


class TestRunServerValidation:
    """Edge cases for run_server() argument checking."""

    def test_rpc_server_with_implementation_raises(self) -> None:
        """Passing RpcServer + implementation raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def m(self) -> None: ...

        class Impl:
            """Implementation."""

            def m(self) -> None: ...

        server = RpcServer(P, Impl())
        with pytest.raises(TypeError, match="implementation must be None"):
            run_server(server, Impl())

    def test_protocol_without_implementation_raises(self) -> None:
        """Passing Protocol without implementation raises TypeError."""

        class P(Protocol):
            """Protocol."""

            def m(self) -> None: ...

        with pytest.raises(TypeError, match="implementation is required"):
            run_server(P)

    def test_non_type_non_server_raises(self) -> None:
        """Passing neither type nor RpcServer raises TypeError."""
        with pytest.raises(TypeError, match="Expected a Protocol class or RpcServer"):
            run_server("not a type")  # type: ignore[arg-type]


# ===================================================================
# 14. describe_rpc
# ===================================================================


class TestDescribeRpc:
    """Edge cases for describe_rpc."""

    def test_empty_protocol(self) -> None:
        """Empty protocol produces minimal output."""

        class Empty(Protocol):
            """Empty."""

            ...

        desc = describe_rpc(Empty)
        assert "Empty" in desc

    def test_includes_all_method_types(self) -> None:
        """describe_rpc shows unary, server_stream, and bidi_stream methods."""

        class P(Protocol):
            """Protocol with all types."""

            def unary(self) -> int: ...
            def stream(self) -> ServerStream[ServerStreamState]: ...
            def bidi(self) -> BidiStream[BidiStreamState]: ...

        desc = describe_rpc(P)
        assert "unary" in desc
        assert "server_stream" in desc
        assert "bidi_stream" in desc

    def test_shows_docstring(self) -> None:
        """describe_rpc includes method docstrings."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> int:
                """Compute a value."""
                ...

        desc = describe_rpc(P)
        assert "Compute a value." in desc

    def test_custom_methods_arg(self) -> None:
        """describe_rpc uses provided methods dict instead of introspecting."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> int: ...

        methods = rpc_methods(P)
        desc = describe_rpc(P, methods=methods)
        assert "method" in desc


# ===================================================================
# 15. RpcConnection context manager
# ===================================================================


class TestRpcConnection:
    """Edge cases for RpcConnection."""

    def test_enters_and_exits(self) -> None:
        """RpcConnection provides proxy on enter and closes on exit."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> int: ...

        class Impl:
            """Implementation."""

            def method(self) -> int:
                """Return 42."""
                return 42

        client, server = make_pipe_pair()
        rpc_server = RpcServer(P, Impl())
        thread = threading.Thread(target=rpc_server.serve, args=(server,), daemon=True)
        thread.start()
        try:
            with RpcConnection(P, client) as proxy:
                assert proxy.method() == 42
        finally:
            client.close()
            thread.join(timeout=5)


# ===================================================================
# 16. AnnotatedBatch
# ===================================================================


class TestAnnotatedBatch:
    """Edge cases for AnnotatedBatch."""

    def test_from_pydict(self) -> None:
        """from_pydict creates batch correctly."""
        ab = AnnotatedBatch.from_pydict({"x": [1, 2]})
        assert ab.batch.num_rows == 2
        assert ab.custom_metadata is None

    def test_from_pydict_with_schema(self) -> None:
        """from_pydict with explicit schema."""
        schema = pa.schema([pa.field("x", pa.int32())])
        ab = AnnotatedBatch.from_pydict({"x": [1]}, schema=schema)
        assert ab.batch.schema.field("x").type == pa.int32()

    def test_frozen(self) -> None:
        """AnnotatedBatch is frozen (immutable)."""
        ab = AnnotatedBatch.from_pydict({"x": [1]})
        with pytest.raises(AttributeError):
            ab.batch = None  # type: ignore[misc,assignment]


# ===================================================================
# 17. _infer_arrow_type edge cases
# ===================================================================


class TestInferArrowTypeEdgeCases:
    """Edge cases for _infer_arrow_type not covered elsewhere."""

    def test_nested_list_three_deep(self) -> None:
        """list[list[list[int]]] infers correctly."""
        result = _infer_arrow_type(list[list[list[int]]])
        assert result == pa.list_(pa.list_(pa.list_(pa.int64())))

    def test_dict_with_complex_value(self) -> None:
        """dict[str, list[int]] infers correctly."""
        result = _infer_arrow_type(dict[str, list[int]])
        assert isinstance(result, pa.MapType)

    def test_annotated_without_arrowtype_unwraps(self) -> None:
        """Annotated[str, 'metadata'] unwraps to string."""
        result = _infer_arrow_type(Annotated[str, "just a marker"])
        assert result == pa.string()

    def test_newtype_chain(self) -> None:
        """NewType wrapping NewType resolves to base type."""
        Base = NewType("Base", int)
        Derived = NewType("Derived", Base)
        assert _infer_arrow_type(Derived) == pa.int64()


# ===================================================================
# 18. _is_optional_type edge cases
# ===================================================================


class TestIsOptionalEdgeCases:
    """Edge cases for _is_optional_type."""

    def test_non_union_generic_not_optional(self) -> None:
        """list[int] is not optional."""
        inner, is_nullable = _is_optional_type(list[int])
        assert is_nullable is False

    def test_none_type_not_optional(self) -> None:
        """NoneType alone is not considered optional (it's just None)."""
        inner, is_nullable = _is_optional_type(type(None))
        assert is_nullable is False


# ===================================================================
# 19. Server method error propagation details
# ===================================================================


class TestErrorPropagation:
    """Detailed error propagation tests."""

    def test_rpc_error_fields(self) -> None:
        """RpcError preserves error_type, error_message, remote_traceback."""

        class P(Protocol):
            """Protocol."""

            def fail(self) -> str: ...

        class Impl:
            """Implementation."""

            def fail(self) -> str:
                """Raise custom error."""
                raise ValueError("detailed error message")

        with _pipe_conn(P, Impl()) as proxy:
            with pytest.raises(RpcError) as exc_info:
                proxy.fail()
            err = exc_info.value
            assert err.error_type == "ValueError"
            assert "detailed error message" in err.error_message
            assert len(err.remote_traceback) > 0

    def test_none_for_non_optional_param_error(self) -> None:
        """Client-side validation rejects None for non-optional parameter."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> int: ...

        class Impl:
            """Implementation."""

            def method(self, x: int) -> int:
                """Return x."""
                return x

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(TypeError, match="not optional"):
            # Validation happens on the client side before sending
            proxy.method(x=None)

    def test_none_return_for_non_optional_error(self) -> None:
        """Server rejects None return for non-optional return type."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> str: ...

        class Impl:
            """Implementation."""

            def method(self) -> str:
                """Return None illegally."""
                return None  # type: ignore[return-value]

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(RpcError, match="non-None return"):
            proxy.method()


# ===================================================================
# 20. Stream state serialization round-trip
# ===================================================================


class TestStreamStateSerialization:
    """State objects serialize for HTTP transport."""

    def test_server_stream_state_round_trip(self) -> None:
        """ServerStreamState subclass serializes and deserializes."""

        @dataclass
        class CountState(ServerStreamState):
            """Counting state."""

            count: int
            current: int = 0

            def produce(self, out: OutputCollector) -> None:
                """Produce."""
                out.finish()

        original = CountState(count=10, current=3)
        restored = CountState.deserialize_from_bytes(original.serialize_to_bytes())
        assert restored.count == 10
        assert restored.current == 3

    def test_bidi_stream_state_round_trip(self) -> None:
        """BidiStreamState subclass serializes and deserializes."""

        @dataclass
        class AccumState(BidiStreamState):
            """Accumulating state."""

            total: float
            items: list[int] = field(default_factory=list)

            def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
                """Process."""
                out.emit(input.batch)

        original = AccumState(total=99.5, items=[1, 2, 3])
        restored = AccumState.deserialize_from_bytes(original.serialize_to_bytes())
        assert restored.total == pytest.approx(99.5)
        assert restored.items == [1, 2, 3]


# ===================================================================
# 21. _drain_stream
# ===================================================================


class TestDrainStream:
    """Edge cases for _drain_stream."""

    def test_drain_already_exhausted(self) -> None:
        """Draining an already-exhausted stream is a no-op."""
        batch = pa.RecordBatch.from_pydict({"x": [1]})
        buf = BytesIO()
        with ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
        buf.seek(0)
        reader = ipc.open_stream(buf)
        reader.read_next_batch()
        # Stream is now exhausted — drain should not raise
        _drain_stream(reader)

    def test_drain_with_remaining_batches(self) -> None:
        """Draining consumes remaining batches."""
        batch = pa.RecordBatch.from_pydict({"x": [1]})
        buf = BytesIO()
        with ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
            writer.write_batch(batch)
            writer.write_batch(batch)
        buf.seek(0)
        reader = ipc.open_stream(buf)
        # Read only one batch
        reader.read_next_batch()
        # Drain should consume the remaining two without error
        _drain_stream(reader)


# ===================================================================
# 22. StreamSession edge cases
# ===================================================================


class TestStreamSession:
    """Edge cases for StreamSession iteration."""

    def test_empty_stream(self) -> None:
        """StreamSession over empty IPC stream yields nothing."""
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema):
            pass  # No batches
        buf.seek(0)
        reader = ipc.open_stream(buf)
        session = StreamSession(reader)
        assert list(session) == []

    def test_stream_with_logs_only(self) -> None:
        """StreamSession with only log batches yields nothing (logs go to callback)."""
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            msg = Message.info("test log")
            md = encode_metadata(msg.add_to_metadata())
            writer.write_batch(empty_batch(schema), custom_metadata=md)
        buf.seek(0)
        reader = ipc.open_stream(buf)
        logs: list[Message] = []
        session = StreamSession(reader, on_log=logs.append)
        data_batches = list(session)
        assert data_batches == []
        assert len(logs) == 1
        assert logs[0].message == "test log"


# ===================================================================
# 23. Multiple sequential calls stress test
# ===================================================================


class TestSequentialCallStress:
    """Stress test for sequential RPC calls."""

    def test_many_sequential_unary_calls(self) -> None:
        """100 sequential unary calls all succeed."""

        class P(Protocol):
            """Protocol."""

            def add(self, a: int, b: int) -> int: ...

        class Impl:
            """Implementation."""

            def add(self, a: int, b: int) -> int:
                """Add."""
                return a + b

        with _pipe_conn(P, Impl()) as proxy:
            for i in range(100):
                assert proxy.add(a=i, b=1) == i + 1

    def test_alternating_success_and_error(self) -> None:
        """Alternating success and error calls all work correctly."""

        class P(Protocol):
            """Protocol."""

            def maybe_fail(self, fail: bool) -> str: ...

        class Impl:
            """Implementation."""

            def maybe_fail(self, fail: bool) -> str:
                """Fail if told to."""
                if fail:
                    raise ValueError("intentional")
                return "ok"

        with _pipe_conn(P, Impl()) as proxy:
            for _ in range(20):
                with pytest.raises(RpcError):
                    proxy.maybe_fail(fail=True)
                assert proxy.maybe_fail(fail=False) == "ok"


# ===================================================================
# 24. Serve one with malformed input
# ===================================================================


class TestServeOneMalformed:
    """Edge cases for RpcServer.serve_one with bad input."""

    def test_unknown_method_returns_error(self) -> None:
        """Server returns error for unknown method name."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> None: ...

        class Impl:
            """Implementation."""

            def method(self) -> None: ...

        server = RpcServer(P, Impl())
        client, server_transport = make_pipe_pair()

        thread = threading.Thread(target=server.serve_one, args=(server_transport,), daemon=True)
        thread.start()

        # Write a valid request but with unknown method name
        schema = pa.schema([])
        _write_request(client.writer, "nonexistent", schema, {})

        # Read response — should be an error
        reader = ipc.open_stream(client.reader)
        with pytest.raises(RpcError, match="Unknown method"):
            batch, md = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, md)

        thread.join(timeout=5)
        client.close()
        server_transport.close()


# ===================================================================
# 24b. serve_one server-side error paths (same-thread for coverage)
# ===================================================================


def _serve_one_roundtrip(
    server: RpcServer,
    method_name: str,
    params_schema: pa.Schema,
    kwargs: dict[str, object],
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Write a request, call serve_one synchronously, read the response.

    Dispatches log/error batches so that server-side errors raise RpcError.
    """
    req_buf = BytesIO()
    _write_request(req_buf, method_name, params_schema, kwargs)
    req_buf.seek(0)
    resp_buf = BytesIO()
    transport = PipeTransport(req_buf, resp_buf)
    server.serve_one(transport)
    resp_buf.seek(0)
    reader = ipc.open_stream(resp_buf)
    batch, md = reader.read_next_batch_with_custom_metadata()
    _dispatch_log_or_error(batch, md)
    return batch, md


class TestServeOneServerErrors:
    """Server-side error paths in serve_one (run in-process for coverage)."""

    def test_unknown_method_error(self) -> None:
        """serve_one returns RpcError for a method name the server doesn't know."""

        class P(Protocol):
            """Protocol."""

            def real(self) -> str: ...

        class Impl:
            """Implementation."""

            def real(self) -> str:
                """Return ok."""
                return "ok"

        server = RpcServer(P, Impl())
        with pytest.raises(RpcError, match="Unknown method: bogus"):
            _serve_one_roundtrip(server, "bogus", pa.schema([]), {})

    def test_validate_params_none_for_required(self) -> None:
        """serve_one returns RpcError when a required param is None."""

        class P(Protocol):
            """Protocol."""

            def need(self, x: int) -> str: ...

        class Impl:
            """Implementation."""

            def need(self, x: int) -> str:
                """Return x as string."""
                return str(x)

        server = RpcServer(P, Impl())
        schema = pa.schema([pa.field("x", pa.int64())])
        with pytest.raises(RpcError, match="not optional"):
            _serve_one_roundtrip(server, "need", schema, {"x": None})

    def test_server_stream_init_error(self) -> None:
        """serve_one returns RpcError when a server-stream method raises during init."""

        class P(Protocol):
            """Protocol."""

            def broken_stream(self) -> ServerStream[ServerStreamState]: ...

        class Impl:
            """Implementation."""

            def broken_stream(self) -> ServerStream[ServerStreamState]:
                """Raise before returning a stream."""
                raise ValueError("stream init failed")

        server = RpcServer(P, Impl())
        with pytest.raises(RpcError, match="stream init failed"):
            _serve_one_roundtrip(server, "broken_stream", pa.schema([]), {})

    def test_bidi_stream_init_error(self) -> None:
        """serve_one returns RpcError when a bidi-stream method raises during init."""

        class P(Protocol):
            """Protocol."""

            def broken_bidi(self) -> BidiStream[BidiStreamState]: ...

        class Impl:
            """Implementation."""

            def broken_bidi(self) -> BidiStream[BidiStreamState]:
                """Raise before returning a bidi stream."""
                raise ValueError("bidi init failed")

        server = RpcServer(P, Impl())
        with pytest.raises(RpcError, match="bidi init failed"):
            _serve_one_roundtrip(server, "broken_bidi", pa.schema([]), {})


# ===================================================================
# 25. RpcError
# ===================================================================


class TestRpcError:
    """Edge cases for RpcError."""

    def test_str_representation(self) -> None:
        """RpcError str includes type and message."""
        err = RpcError("TypeError", "bad arg", "traceback here")
        assert "TypeError" in str(err)
        assert "bad arg" in str(err)

    def test_attributes(self) -> None:
        """RpcError preserves all three attributes."""
        err = RpcError("ValueError", "msg", "tb")
        assert err.error_type == "ValueError"
        assert err.error_message == "msg"
        assert err.remote_traceback == "tb"

    def test_is_exception(self) -> None:
        """RpcError is an Exception subclass."""
        err = RpcError("E", "m", "t")
        assert isinstance(err, Exception)

    def test_unicode_error_message(self) -> None:
        """RpcError handles unicode in messages."""

        class P(Protocol):
            """Protocol."""

            def fail(self) -> str: ...

        class Impl:
            """Implementation."""

            def fail(self) -> str:
                """Raise with unicode."""
                raise ValueError("エラー: 失敗しました 🔥")

        with _pipe_conn(P, Impl()) as proxy, pytest.raises(RpcError, match="エラー"):
            proxy.fail()
