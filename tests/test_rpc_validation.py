"""Tests for RpcServer implementation validation at construction time."""

from __future__ import annotations

from typing import Protocol

import pytest

from vgi_rpc.rpc import (
    BidiStream,
    BidiStreamState,
    CallContext,
    RpcServer,
    ServerStream,
    ServerStreamState,
)

# ---------------------------------------------------------------------------
# Test protocols
# ---------------------------------------------------------------------------


class SimpleService(Protocol):
    """Protocol with two unary methods for focused validation tests."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def greet(self, name: str) -> str:
        """Greet by name."""
        ...


class StreamService(Protocol):
    """Protocol with stream and bidi methods."""

    def generate(self, count: int) -> ServerStream[ServerStreamState]:
        """Generate batches."""
        ...

    def transform(self, factor: float) -> BidiStream[BidiStreamState]:
        """Scale values."""
        ...


class MixedService(Protocol):
    """Protocol with unary, stream, bidi, and zero-param methods."""

    def noop(self) -> None:
        """Do nothing."""
        ...

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def generate(self, count: int) -> ServerStream[ServerStreamState]:
        """Generate batches."""
        ...

    def transform(self, factor: float) -> BidiStream[BidiStreamState]:
        """Scale values."""
        ...


# ---------------------------------------------------------------------------
# Tests: missing methods
# ---------------------------------------------------------------------------


class TestMissingMethod:
    """Tests for missing method detection."""

    def test_single_missing_method(self) -> None:
        """Impl missing one of two methods raises TypeError."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

        with pytest.raises(TypeError, match="missing method greet") as exc_info:
            RpcServer(SimpleService, Impl())
        assert "Impl does not implement SimpleService" in str(exc_info.value)

    def test_missing_method_signature_includes_param_types(self) -> None:
        """Error for missing method shows parameter names and types."""

        class Impl:
            def greet(self, name: str) -> str:
                return name

        with pytest.raises(TypeError, match=r"missing method add\(a: float, b: float\)"):
            RpcServer(SimpleService, Impl())

    def test_all_methods_missing(self) -> None:
        """Completely empty impl reports every method as missing."""

        class Empty:
            pass

        with pytest.raises(TypeError) as exc_info:
            RpcServer(SimpleService, Empty())

        msg = str(exc_info.value)
        assert "missing method add" in msg
        assert "missing method greet" in msg
        assert msg.count("  - ") == 2

    def test_missing_stream_method(self) -> None:
        """Missing stream/bidi methods are reported with correct signatures."""

        class Impl:
            def generate(self, count: int) -> ServerStream[ServerStreamState]:
                raise NotImplementedError

            # transform is missing

        with pytest.raises(TypeError, match=r"missing method transform\(factor: float\)"):
            RpcServer(StreamService, Impl())

    def test_missing_zero_param_method(self) -> None:
        """Zero-param method shows as 'noop()' in the error."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            def generate(self, count: int) -> ServerStream[ServerStreamState]:
                raise NotImplementedError

            def transform(self, factor: float) -> BidiStream[BidiStreamState]:
                raise NotImplementedError

        with pytest.raises(TypeError, match=r"missing method noop\(\)"):
            RpcServer(MixedService, Impl())


# ---------------------------------------------------------------------------
# Tests: not callable
# ---------------------------------------------------------------------------


class TestNotCallable:
    """Tests for attributes that exist but aren't callable."""

    def test_string_attribute(self) -> None:
        """String attribute is not callable."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            greet = "not a function"

        with pytest.raises(TypeError, match=r"'greet' .* not callable"):
            RpcServer(SimpleService, Impl())

    def test_int_attribute(self) -> None:
        """Integer attribute is not callable."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            greet = 42

        with pytest.raises(TypeError, match=r"'greet' .* not callable"):
            RpcServer(SimpleService, Impl())

    def test_none_attribute(self) -> None:
        """None attribute is treated as missing (getattr returns None)."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            greet = None

        with pytest.raises(TypeError, match="missing method greet"):
            RpcServer(SimpleService, Impl())


# ---------------------------------------------------------------------------
# Tests: parameter mismatches
# ---------------------------------------------------------------------------


class TestParameterMismatch:
    """Tests for parameter-level validation."""

    def test_missing_one_parameter(self) -> None:
        """Impl method missing one protocol parameter."""

        class Impl:
            def add(self, a: float) -> float:
                return a

            def greet(self, name: str) -> str:
                return name

        with pytest.raises(TypeError, match="'add\\(\\)' missing parameter 'b'"):
            RpcServer(SimpleService, Impl())

    def test_missing_all_parameters(self) -> None:
        """Impl method with no params (besides self) reports all missing."""

        class Impl:
            def add(self) -> float:
                return 0.0

            def greet(self, name: str) -> str:
                return name

        with pytest.raises(TypeError) as exc_info:
            RpcServer(SimpleService, Impl())

        msg = str(exc_info.value)
        assert "'add()' missing parameter 'a'" in msg
        assert "'add()' missing parameter 'b'" in msg

    def test_extra_required_positional(self) -> None:
        """Extra required positional param raises TypeError."""

        class Impl:
            def add(self, a: float, b: float, c: float) -> float:
                return a + b + c

            def greet(self, name: str) -> str:
                return name

        with pytest.raises(TypeError, match=r"'add\(\)' has required parameter 'c'.*SimpleService"):
            RpcServer(SimpleService, Impl())

    def test_extra_required_keyword_only(self) -> None:
        """Extra required keyword-only param raises TypeError."""

        class Impl:
            def add(self, a: float, b: float, *, precision: int) -> float:
                return round(a + b, precision)

            def greet(self, name: str) -> str:
                return name

        with pytest.raises(TypeError, match="'add\\(\\)' has required parameter 'precision'"):
            RpcServer(SimpleService, Impl())

    def test_extra_optional_positional_allowed(self) -> None:
        """Extra positional param with default is allowed."""

        class Impl:
            def add(self, a: float, b: float, verbose: bool = False) -> float:
                return a + b

            def greet(self, name: str, prefix: str = "Hello") -> str:
                return f"{prefix}, {name}!"

        RpcServer(SimpleService, Impl())

    def test_extra_optional_keyword_only_allowed(self) -> None:
        """Extra keyword-only param with default is allowed."""

        class Impl:
            def add(self, a: float, b: float, *, precision: int = 10) -> float:
                return round(a + b, precision)

            def greet(self, name: str) -> str:
                return name

        RpcServer(SimpleService, Impl())

    def test_var_positional_not_flagged(self) -> None:
        """*args is not flagged as an extra required param."""

        class Impl:
            def add(self, a: float, b: float, *args: float) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

        RpcServer(SimpleService, Impl())

    def test_var_keyword_not_flagged(self) -> None:
        """**kwargs is not flagged as an extra required param."""

        class Impl:
            def add(self, a: float, b: float, **kwargs: float) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

        RpcServer(SimpleService, Impl())


# ---------------------------------------------------------------------------
# Tests: ctx handling
# ---------------------------------------------------------------------------


class TestCtxHandling:
    """Tests for ctx special-case handling."""

    def test_optional_ctx_allowed(self) -> None:
        """Optional ctx with default is allowed and detected."""

        class Impl:
            def add(self, a: float, b: float, ctx: CallContext | None = None) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

        server = RpcServer(SimpleService, Impl())
        assert "add" in server.ctx_methods
        assert "greet" not in server.ctx_methods

    def test_required_ctx_allowed(self) -> None:
        """Required ctx without default is allowed (framework injects it)."""

        class Impl:
            def add(self, a: float, b: float, ctx: CallContext) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

        server = RpcServer(SimpleService, Impl())
        assert "add" in server.ctx_methods

    def test_ctx_on_all_methods(self) -> None:
        """Allow ctx on every method."""

        class Impl:
            def add(self, a: float, b: float, ctx: CallContext | None = None) -> float:
                return a + b

            def greet(self, name: str, ctx: CallContext | None = None) -> str:
                return name

        server = RpcServer(SimpleService, Impl())
        assert "add" in server.ctx_methods
        assert "greet" in server.ctx_methods


# ---------------------------------------------------------------------------
# Tests: multiple errors aggregated
# ---------------------------------------------------------------------------


class TestMultipleErrors:
    """Tests that all validation errors are collected into a single TypeError."""

    def test_missing_and_not_callable(self) -> None:
        """Missing method + not-callable attribute in same exception."""

        class Impl:
            # add is missing
            greet = 42

        with pytest.raises(TypeError) as exc_info:
            RpcServer(SimpleService, Impl())

        msg = str(exc_info.value)
        assert "missing method add" in msg
        assert "'greet'" in msg and "not callable" in msg
        assert msg.count("  - ") == 2

    def test_errors_across_different_methods(self) -> None:
        """Different error types on different methods, all collected."""

        class Impl:
            def add(self, a: float) -> float:  # missing b
                return a

            def greet(self, name: str, title: str) -> str:  # extra required
                return f"{title} {name}"

        with pytest.raises(TypeError) as exc_info:
            RpcServer(SimpleService, Impl())

        msg = str(exc_info.value)
        assert "'add()' missing parameter 'b'" in msg
        assert "'greet()' has required parameter 'title'" in msg

    def test_many_errors_on_mixed_protocol(self) -> None:
        """Multiple errors across unary, stream, and bidi methods."""

        class Impl:
            # noop is missing
            def add(self, a: float) -> float:  # missing b
                return a

            generate = "not a method"
            # transform is missing

        with pytest.raises(TypeError) as exc_info:
            RpcServer(MixedService, Impl())

        msg = str(exc_info.value)
        assert "missing method noop" in msg
        assert "'add()' missing parameter 'b'" in msg
        assert "'generate'" in msg and "not callable" in msg
        assert "missing method transform" in msg
        assert msg.count("  - ") == 4

    def test_error_header_format(self) -> None:
        """Error message header uses impl class name and protocol name."""

        class MyBrokenImpl:
            pass

        with pytest.raises(TypeError) as exc_info:
            RpcServer(SimpleService, MyBrokenImpl())

        msg = str(exc_info.value)
        assert msg.startswith("MyBrokenImpl does not implement SimpleService:")


# ---------------------------------------------------------------------------
# Tests: valid implementations
# ---------------------------------------------------------------------------


class TestValidImplementation:
    """Tests that correct implementations pass validation."""

    def test_exact_match(self) -> None:
        """Impl with exactly matching signatures passes."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

        server = RpcServer(SimpleService, Impl())
        assert len(server.methods) == 2

    def test_stream_protocol(self) -> None:
        """Impl matching a stream/bidi protocol passes."""

        class Impl:
            def generate(self, count: int) -> ServerStream[ServerStreamState]:
                raise NotImplementedError

            def transform(self, factor: float) -> BidiStream[BidiStreamState]:
                raise NotImplementedError

        server = RpcServer(StreamService, Impl())
        assert len(server.methods) == 2

    def test_mixed_protocol(self) -> None:
        """Impl matching a mixed protocol (unary + stream + bidi + noop) passes."""

        class Impl:
            def noop(self) -> None:
                pass

            def add(self, a: float, b: float) -> float:
                return a + b

            def generate(self, count: int) -> ServerStream[ServerStreamState]:
                raise NotImplementedError

            def transform(self, factor: float) -> BidiStream[BidiStreamState]:
                raise NotImplementedError

        server = RpcServer(MixedService, Impl())
        assert len(server.methods) == 4

    def test_superset_with_defaults_and_ctx(self) -> None:
        """Impl with extra defaulted params and ctx passes."""

        class Impl:
            def add(self, a: float, b: float, ctx: CallContext | None = None, debug: bool = False) -> float:
                return a + b

            def greet(self, name: str, **kwargs: str) -> str:
                return name

        server = RpcServer(SimpleService, Impl())
        assert "add" in server.ctx_methods

    def test_extra_methods_on_impl_ignored(self) -> None:
        """Extra methods on impl not in protocol are ignored."""

        class Impl:
            def add(self, a: float, b: float) -> float:
                return a + b

            def greet(self, name: str) -> str:
                return name

            def bonus(self) -> str:
                return "extra"

        RpcServer(SimpleService, Impl())
