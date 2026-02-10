"""Exhaustive edge-case tests for the vgi-rpc framework.

Covers protocol introspection boundaries, wire protocol corner cases,
transport lifecycle, deserialization edge cases, and the internal
dispatch/log machinery.
"""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from typing import NewType, Protocol

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import encode_metadata
from vgi_rpc.rpc import (
    BidiStream,
    BidiStreamState,
    CallContext,
    MethodType,
    OutputCollector,
    PipeTransport,
    RpcError,
    RpcServer,
    ServerStream,
    ServerStreamState,
    StreamSession,
    _build_params_schema,
    _build_result_schema,
    _classify_return_type,
    _ClientLogSink,
    _convert_for_arrow,
    _deserialize_params,
    _deserialize_value,
    _dispatch_log_or_error,
    _drain_stream,
    _RpcProxy,
    _validate_params,
    _validate_result,
    _write_request,
    describe_rpc,
    make_pipe_pair,
    rpc_methods,
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

            some_value: int

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
        mt, _rt, has_ret = _classify_return_type(type(None))
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
# 4. _dispatch_log_or_error edge cases
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
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "INFO",
                "vgi_rpc.log_message": "test",
            }
        )
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
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "EXCEPTION",
                "vgi_rpc.log_message": "boom",
                "vgi_rpc.log_extra": extra,
            }
        )
        with pytest.raises(RpcError, match="boom") as exc_info:
            _dispatch_log_or_error(self._make_zero_row_batch(), md)
        assert exc_info.value.error_type == "ValueError"
        assert exc_info.value.remote_traceback == "tb here"

    def test_exception_without_extra_uses_defaults(self) -> None:
        """EXCEPTION with no extra uses level as error_type."""
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "EXCEPTION",
                "vgi_rpc.log_message": "err",
            }
        )
        with pytest.raises(RpcError) as exc_info:
            _dispatch_log_or_error(self._make_zero_row_batch(), md)
        assert exc_info.value.error_type == "EXCEPTION"
        assert exc_info.value.remote_traceback == ""

    def test_malformed_json_extra_ignored(self) -> None:
        """Malformed JSON in log_extra is silently ignored."""
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "INFO",
                "vgi_rpc.log_message": "test",
                "vgi_rpc.log_extra": "not valid json{{{",
            }
        )
        logs: list[Message] = []
        result = _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=logs.append)
        assert result is True
        assert len(logs) == 1
        assert logs[0].message == "test"

    def test_log_without_callback_silently_consumed(self) -> None:
        """Log batch with no on_log callback is consumed (returns True)."""
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "DEBUG",
                "vgi_rpc.log_message": "debug msg",
            }
        )
        result = _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=None)
        assert result is True

    def test_server_id_extracted_from_metadata(self) -> None:
        """server_id from top-level metadata appears in reconstructed Message.extra."""
        extra = json.dumps({"custom_key": "value"})
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "INFO",
                "vgi_rpc.log_message": "msg",
                "vgi_rpc.log_extra": extra,
                "vgi_rpc.server_id": "abc123def456",
            }
        )
        logs: list[Message] = []
        _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=logs.append)
        assert logs[0].extra is not None
        assert logs[0].extra["server_id"] == "abc123def456"
        assert logs[0].extra["custom_key"] == "value"

    def test_server_id_absent_when_not_in_metadata(self) -> None:
        """When server_id is not in metadata, it does not appear in Message.extra."""
        extra = json.dumps({"custom_key": "value"})
        md = encode_metadata(
            {
                "vgi_rpc.log_level": "INFO",
                "vgi_rpc.log_message": "msg",
                "vgi_rpc.log_extra": extra,
            }
        )
        logs: list[Message] = []
        _dispatch_log_or_error(self._make_zero_row_batch(), md, on_log=logs.append)
        assert logs[0].extra is not None
        assert "server_id" not in logs[0].extra
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
# 8. _ClientLogSink (client-directed log buffering)
# ===================================================================


class TestClientLogSink:
    """Edge cases for _ClientLogSink client-log buffering."""

    def test_buffer_before_activate(self) -> None:
        """Messages are buffered before activate() is called."""
        sink = _ClientLogSink()
        sink(Message.info("msg1"))
        sink(Message.debug("msg2"))
        assert len(sink._buffer) == 2

    def test_activate_flushes_buffer(self) -> None:
        """Buffered messages are flushed on activate()."""
        sink = _ClientLogSink()
        sink(Message.info("buffered"))

        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            sink.flush_contents(writer, schema)
            # Buffer should be empty after activation
            assert len(sink._buffer) == 0

    def test_direct_write_after_activate(self) -> None:
        """Messages after activate() are written directly, not buffered."""
        sink = _ClientLogSink()
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            sink.flush_contents(writer, schema)
            sink(Message.info("direct"))
            assert len(sink._buffer) == 0


# ===================================================================
# 9. OutputCollector edge cases
# ===================================================================


class TestOutputCollector:
    """Edge cases for OutputCollector."""

    def test_multiple_logs_before_data(self) -> None:
        """Multiple log messages before a data batch are all collected."""
        schema = pa.schema([pa.field("x", pa.int64())])
        out = OutputCollector(schema)
        out.client_log(Level.INFO, "log1")
        out.client_log(Level.DEBUG, "log2")
        out.client_log(Level.WARN, "log3")
        out.emit_pydict({"x": [1]})
        # 3 log batches + 1 data batch
        assert len(out.batches) == 4


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


# ===================================================================
# 10. Wire protocol edge cases (end-to-end over pipes)
# ===================================================================


class TestWireProtocolEdgeCases:
    """End-to-end edge cases over the pipe transport."""

    def test_empty_server_stream(self) -> None:
        """Server stream that finishes immediately yields no batches."""

        @dataclass
        class EmptyState(ServerStreamState):
            """Immediately finishes."""

            def produce(self, out: OutputCollector, ctx: CallContext) -> None:
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


# ===================================================================
# 11. ctx injection
# ===================================================================


class TestCtxInjection:
    """Tests for ctx parameter injection by the framework."""

    def test_ctx_not_injected_when_absent(self) -> None:
        """Framework does not inject ctx when method signature lacks it."""

        class P(Protocol):
            """Protocol."""

            def method(self, x: int) -> int: ...

        class Impl:
            """Implementation."""

            def method(self, x: int) -> int:
                """Return x."""
                return x

        server = RpcServer(P, Impl())
        assert "method" not in server.ctx_methods


# ===================================================================
# 12. run_server argument validation
# ===================================================================


# ===================================================================
# 13. describe_rpc
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

    def test_custom_methods_arg(self) -> None:
        """describe_rpc uses provided methods dict instead of introspecting."""

        class P(Protocol):
            """Protocol."""

            def method(self) -> int: ...

        methods = rpc_methods(P)
        desc = describe_rpc(P, methods=methods)
        assert "method" in desc


# ===================================================================
# 14. AnnotatedBatch
# ===================================================================


# ===================================================================
# 15. _infer_arrow_type edge cases
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
        _inner, is_nullable = _is_optional_type(list[int])
        assert is_nullable is False

    def test_none_type_not_optional(self) -> None:
        """NoneType alone is not considered optional (it's just None)."""
        _inner, is_nullable = _is_optional_type(type(None))
        assert is_nullable is False


# ===================================================================
# 16. Stream state serialization round-trip
# ===================================================================


# ===================================================================
# 17. _drain_stream
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
# 18. Serve one with malformed input
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
