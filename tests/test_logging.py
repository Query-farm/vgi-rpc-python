# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for server-side logging infrastructure."""

from __future__ import annotations

import json
import logging
import os
import sys
import textwrap
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol, cast

import pyarrow as pa
import pytest

from vgi_rpc import AnnotatedBatch, OutputCollector, Stream, StreamState
from vgi_rpc.logging_utils import VgiJsonFormatter
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    RpcError,
    RpcServer,
    StderrMode,
    SubprocessTransport,
    _ContextLoggerAdapter,
    _emit_access_log,
    connect,
    serve_pipe,
    serve_stdio,
)
from vgi_rpc.rpc._transport import _drain_stderr


def _extra(record: logging.LogRecord, key: str) -> Any:
    """Read a dynamic extra field from a log record without ``type: ignore``."""
    return record.__dict__[key]


# ---------------------------------------------------------------------------
# Test Protocol + Implementation for logging tests
# ---------------------------------------------------------------------------


class LoggingService(Protocol):
    """Minimal service for testing server-side logging."""

    def greet(self, name: str) -> str:
        """Return a greeting."""
        ...

    def fail(self) -> None:
        """Raise unconditionally."""
        ...


class LoggingServiceImpl:
    """Implementation that uses ctx.logger."""

    def greet(self, name: str, ctx: CallContext) -> str:
        """Return a greeting, log with ctx.logger."""
        ctx.logger.info("Processing greet", extra={"input_name": name})
        return f"Hello, {name}!"

    def fail(self) -> None:
        """Raise ValueError unconditionally."""
        raise ValueError("intentional error")


# ---------------------------------------------------------------------------
# Streaming test fixtures (module-level for annotation resolution)
# ---------------------------------------------------------------------------


@dataclass
class _CountState(StreamState):
    """Counts down from remaining to 1."""

    remaining: int

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce next countdown value."""
        if self.remaining <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.remaining]})
        self.remaining -= 1


@dataclass
class _FailProduceState(StreamState):
    """Always fails on first produce."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Raise on produce."""
        raise RuntimeError("produce exploded")


class _StreamService(Protocol):
    """Protocol for producer stream logging tests."""

    def count(self, n: int) -> Stream[StreamState]:
        """Count down."""
        ...


class _StreamServiceImpl:
    """Implementation for _StreamService."""

    def count(self, n: int) -> Stream[_CountState]:
        """Count down from n."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=_CountState(remaining=n))


class _FailStreamService(Protocol):
    """Protocol for failing producer stream test."""

    def fail_stream(self) -> Stream[StreamState]:
        """Fail unconditionally."""
        ...


class _FailStreamServiceImpl:
    """Implementation for _FailStreamService."""

    def fail_stream(self) -> Stream[_FailProduceState]:
        """Fail unconditionally."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=_FailProduceState())


# ---------------------------------------------------------------------------
# _ContextLoggerAdapter tests
# ---------------------------------------------------------------------------


class TestContextLoggerAdapter:
    """Tests for _ContextLoggerAdapter."""

    def test_returns_context_logger_adapter(self) -> None:
        """ctx.logger should return a _ContextLoggerAdapter instance."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="abc123",
            method_name="greet",
            protocol_name="LoggingService",
        )
        logger = ctx.logger
        assert isinstance(logger, _ContextLoggerAdapter)

    def test_extra_fields_present(self, caplog: pytest.LogCaptureFixture) -> None:
        """ctx.logger should include server_id and method in extra."""
        ctx = CallContext(
            auth=AuthContext(domain="jwt", authenticated=True, principal="alice"),
            emit_client_log=lambda _: None,
            server_id="srv42",
            method_name="greet",
            protocol_name="LoggingService",
        )
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.service.LoggingService"):
            ctx.logger.info("test message")

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert _extra(record, "server_id") == "srv42"
        assert _extra(record, "method") == "greet"
        assert _extra(record, "principal") == "alice"
        assert _extra(record, "auth_domain") == "jwt"

    def test_remote_addr_in_extra(self, caplog: pytest.LogCaptureFixture) -> None:
        """ctx.logger should include remote_addr when available."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            transport_metadata={"remote_addr": "10.0.0.1"},
            server_id="srv1",
            method_name="greet",
            protocol_name="LoggingService",
        )
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.service.LoggingService"):
            ctx.logger.info("test")

        record = caplog.records[0]
        assert _extra(record, "remote_addr") == "10.0.0.1"

    def test_lazy_initialization(self) -> None:
        """Logger should not be created until .logger is accessed."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="srv1",
            method_name="greet",
            protocol_name="LoggingService",
        )
        assert ctx._logger is None
        _ = ctx.logger
        assert ctx._logger is not None

    def test_cached_on_second_access(self) -> None:
        """Same logger instance should be returned on repeated access."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="srv1",
            method_name="greet",
            protocol_name="LoggingService",
        )
        logger1 = ctx.logger
        logger2 = ctx.logger
        assert logger1 is logger2

    def test_framework_fields_take_precedence(self, caplog: pytest.LogCaptureFixture) -> None:
        """Framework extra fields should override user-supplied extra with same key."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="srv1",
            method_name="greet",
            protocol_name="LoggingService",
        )
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.service.LoggingService"):
            # User tries to override server_id
            ctx.logger.info("test", extra={"server_id": "HACKED", "custom_field": "ok"})

        record = caplog.records[0]
        assert _extra(record, "server_id") == "srv1"
        assert _extra(record, "custom_field") == "ok"

    def test_logger_name(self) -> None:
        """Logger name should be vgi_rpc.service.<ProtocolName>."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="srv1",
            method_name="greet",
            protocol_name="MyService",
        )
        assert ctx.logger.logger.name == "vgi_rpc.service.MyService"

    def test_anonymous_auth_no_principal(self, caplog: pytest.LogCaptureFixture) -> None:
        """Anonymous auth should not add principal or auth_domain to extra."""
        ctx = CallContext(
            auth=AuthContext.anonymous(),
            emit_client_log=lambda _: None,
            server_id="srv1",
            method_name="greet",
            protocol_name="LoggingService",
        )
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.service.LoggingService"):
            ctx.logger.info("test")

        record = caplog.records[0]
        assert not hasattr(record, "principal")
        assert not hasattr(record, "auth_domain")


# ---------------------------------------------------------------------------
# Access log tests
# ---------------------------------------------------------------------------


class TestAccessLog:
    """Tests for automatic access logging."""

    def test_unary_success_access_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Successful unary call should emit one access log record."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(LoggingService, LoggingServiceImpl()) as proxy,
        ):
            result = proxy.greet(name="World")

        assert result == "Hello, World!"
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "ok"
        assert _extra(record, "method") == "greet"
        assert _extra(record, "method_type") == "unary"
        assert _extra(record, "duration_ms") >= 0
        assert _extra(record, "error_type") == ""

    def test_unary_error_access_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Failed unary call should emit access log with status=error."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(LoggingService, LoggingServiceImpl()) as proxy,
            pytest.raises(RpcError, match="ValueError"),
        ):
            proxy.fail()

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "error"
        assert _extra(record, "error_type") == "ValueError"
        assert _extra(record, "duration_ms") >= 0

    def test_access_log_includes_protocol_name(self, caplog: pytest.LogCaptureFixture) -> None:
        """Access log should include the protocol name."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(LoggingService, LoggingServiceImpl()) as proxy,
        ):
            proxy.greet(name="Test")

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        assert _extra(access_records[0], "protocol") == "LoggingService"

    def test_access_log_includes_auth_domain(self, caplog: pytest.LogCaptureFixture) -> None:
        """Access log should include auth_domain field."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext(domain="jwt", authenticated=True, principal="alice"),
                transport_metadata={},
                duration_ms=10.0,
                status="ok",
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert _extra(record, "auth_domain") == "jwt"


# ---------------------------------------------------------------------------
# Lifecycle log tests
# ---------------------------------------------------------------------------


class TestLifecycleLogging:
    """Tests for framework lifecycle logging."""

    def test_rpc_server_init_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """RpcServer.__init__ should emit INFO with server_id and protocol name."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.rpc"):
            _ = RpcServer(LoggingService, LoggingServiceImpl(), server_id="test123")

        init_records = [r for r in caplog.records if r.name == "vgi_rpc.rpc" and "RpcServer created" in r.message]
        assert len(init_records) == 1
        record = init_records[0]
        assert _extra(record, "server_id") == "test123"
        assert _extra(record, "protocol") == "LoggingService"
        assert _extra(record, "method_count") == 2  # greet + fail

    def test_error_method_logs_with_exc_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Method exception should emit ERROR log with exc_info."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.rpc"),
            serve_pipe(LoggingService, LoggingServiceImpl()) as proxy,
            pytest.raises(RpcError),
        ):
            proxy.fail()

        error_records = [r for r in caplog.records if r.name == "vgi_rpc.rpc" and r.levelno == logging.ERROR]
        assert len(error_records) >= 1
        record = error_records[0]
        assert record.exc_info is not None


# ---------------------------------------------------------------------------
# ctx.logger end-to-end test
# ---------------------------------------------------------------------------


class TestCtxLoggerEndToEnd:
    """Tests for ctx.logger through the full serve_pipe path."""

    def test_ctx_logger_emits_structured_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """ctx.logger.info() in a method should produce a structured log record."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.service.LoggingService"),
            serve_pipe(LoggingService, LoggingServiceImpl()) as proxy,
        ):
            proxy.greet(name="Alice")

        service_records = [r for r in caplog.records if r.name == "vgi_rpc.service.LoggingService"]
        assert len(service_records) >= 1
        record = service_records[0]
        assert record.message == "Processing greet"
        assert _extra(record, "input_name") == "Alice"
        assert hasattr(record, "server_id")
        assert _extra(record, "method") == "greet"


# ---------------------------------------------------------------------------
# StderrMode tests
# ---------------------------------------------------------------------------


class TestStderrMode:
    """Tests for SubprocessTransport stderr handling."""

    def test_devnull_no_stall(self) -> None:
        """DEVNULL should discard stderr without stalling."""
        script = textwrap.dedent("""\
            import sys
            sys.stderr.write("some error output\\n")
            sys.stderr.flush()
            # Exit cleanly
        """)
        transport = SubprocessTransport(
            [sys.executable, "-c", script],
            stderr=StderrMode.DEVNULL,
        )
        transport.close()
        assert transport._proc.returncode == 0

    def test_pipe_captures_stderr(self, caplog: pytest.LogCaptureFixture) -> None:
        """PIPE should forward child stderr to logging at INFO level."""
        script = textwrap.dedent("""\
            import sys
            sys.stderr.write("test stderr line\\n")
            sys.stderr.flush()
        """)
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.subprocess.stderr"):
            transport = SubprocessTransport(
                [sys.executable, "-c", script],
                stderr=StderrMode.PIPE,
            )
            transport.close()

        stderr_records = [r for r in caplog.records if r.name == "vgi_rpc.subprocess.stderr"]
        assert any("test stderr line" in r.message for r in stderr_records)
        # Verify lines are logged at INFO, not WARNING
        for record in stderr_records:
            assert record.levelno == logging.INFO

    def test_pipe_custom_logger(self, caplog: pytest.LogCaptureFixture) -> None:
        """PIPE should use custom logger when provided."""
        script = textwrap.dedent("""\
            import sys
            sys.stderr.write("custom logger line\\n")
            sys.stderr.flush()
        """)
        custom_logger = logging.getLogger("test.custom.stderr")
        with caplog.at_level(logging.DEBUG, logger="test.custom.stderr"):
            transport = SubprocessTransport(
                [sys.executable, "-c", script],
                stderr=StderrMode.PIPE,
                stderr_logger=custom_logger,
            )
            transport.close()

        custom_records = [r for r in caplog.records if r.name == "test.custom.stderr"]
        assert any("custom logger line" in r.message for r in custom_records)

    def test_inherit_is_default(self) -> None:
        """Default stderr mode should be INHERIT."""
        assert StderrMode.INHERIT.value == "inherit"

    def test_connect_passes_stderr_mode(self) -> None:
        """connect() should accept stderr and stderr_logger params."""
        # Just verify the function signature accepts them (no actual subprocess)
        import inspect

        sig = inspect.signature(connect)
        assert "stderr" in sig.parameters
        assert "stderr_logger" in sig.parameters


# ---------------------------------------------------------------------------
# VgiJsonFormatter tests
# ---------------------------------------------------------------------------


class TestVgiJsonFormatter:
    """Tests for VgiJsonFormatter."""

    def test_valid_json_output(self) -> None:
        """Output should be valid JSON."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test"
        assert parsed["message"] == "test message"

    def test_extra_fields_in_output(self) -> None:
        """Extra fields should appear in JSON output."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.server_id = "srv42"
        record.method = "greet"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["server_id"] == "srv42"
        assert parsed["method"] == "greet"

    def test_exception_info_included(self) -> None:
        """Exception info should be included in JSON output."""
        formatter = VgiJsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error occurred",
            args=(),
            exc_info=exc_info,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]

    def test_default_str_handles_non_serializable(self) -> None:
        """Non-serializable values should be coerced to strings."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.custom_obj = object()
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "custom_obj" in parsed

    def test_none_exc_info_tuple_excluded(self) -> None:
        """exc_info=(None, None, None) should not produce exception key."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=(None, None, None),
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" not in parsed

    def test_empty_message(self) -> None:
        """Empty message should produce empty string in JSON."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == ""

    def test_stack_info_included(self) -> None:
        """stack_info should be included in JSON output when present."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.stack_info = "Stack (most recent call last):\n  File test.py"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "stack_info" in parsed
        assert "test.py" in parsed["stack_info"]


# ---------------------------------------------------------------------------
# _emit_access_log tests
# ---------------------------------------------------------------------------


class TestEmitAccessLog:
    """Tests for _emit_access_log helper."""

    def test_emits_info_on_access_logger(self, caplog: pytest.LogCaptureFixture) -> None:
        """_emit_access_log should emit an INFO record on vgi_rpc.access."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=42.5,
                status="ok",
            )

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) == 1
        record = access_records[0]
        assert record.levelno == logging.INFO
        assert _extra(record, "duration_ms") == 42.5
        assert _extra(record, "status") == "ok"

    def test_duration_rounding(self, caplog: pytest.LogCaptureFixture) -> None:
        """_emit_access_log should round duration_ms to 2 decimal places."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=42.5555,
                status="ok",
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert _extra(record, "duration_ms") == 42.56

    def test_error_type_in_extra(self, caplog: pytest.LogCaptureFixture) -> None:
        """error_type should appear in extra when provided."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=10.0,
                status="error",
                error_type="ValueError",
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert _extra(record, "error_type") == "ValueError"
        assert _extra(record, "status") == "error"

    def test_http_status_included_when_provided(self, caplog: pytest.LogCaptureFixture) -> None:
        """http_status should appear in extra when provided."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=10.0,
                status="ok",
                http_status=200,
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert _extra(record, "http_status") == 200

    def test_no_http_status_for_pipe(self, caplog: pytest.LogCaptureFixture) -> None:
        """http_status should not be present when not provided."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=10.0,
                status="ok",
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert not hasattr(record, "http_status")


# ---------------------------------------------------------------------------
# RpcServer.protocol_name tests
# ---------------------------------------------------------------------------


class TestProtocolName:
    """Tests for RpcServer.protocol_name property."""

    def test_protocol_name_returns_class_name(self) -> None:
        """protocol_name should return the protocol class name."""
        server = RpcServer(LoggingService, LoggingServiceImpl())
        assert server.protocol_name == "LoggingService"


# ---------------------------------------------------------------------------
# HTTP transport access log test
# ---------------------------------------------------------------------------


class TestHttpAccessLog:
    """Tests for HTTP transport access and lifecycle logging."""

    def test_make_wsgi_app_logs_creation(self, caplog: pytest.LogCaptureFixture) -> None:
        """make_wsgi_app should emit an INFO log on creation."""
        from vgi_rpc.http import make_wsgi_app

        server = RpcServer(LoggingService, LoggingServiceImpl(), server_id="http_srv")
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.http"):
            make_wsgi_app(server, signing_key=b"testtesttesttesttesttesttesttest")

        http_records = [r for r in caplog.records if r.name == "vgi_rpc.http" and "WSGI app created" in r.message]
        assert len(http_records) == 1
        record = http_records[0]
        assert _extra(record, "server_id") == "http_srv"
        assert _extra(record, "protocol") == "LoggingService"

    def test_http_unary_access_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP unary call should emit access log with http_status."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(LoggingService, LoggingServiceImpl(), server_id="http_srv2")
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(LoggingService, client=client) as proxy,
        ):
            result = proxy.greet(name="HTTP")

        assert result == "Hello, HTTP!"
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "http_status") == 200
        assert _extra(record, "status") == "ok"

    def test_http_auth_middleware_error_fields(self, caplog: pytest.LogCaptureFixture) -> None:
        """Auth middleware should log error_type and auth_error fields."""
        import falcon

        from vgi_rpc.http import http_connect, make_sync_client

        def bad_auth(req: falcon.Request) -> AuthContext:
            raise ValueError("bad token")

        server = RpcServer(LoggingService, LoggingServiceImpl(), server_id="auth_srv")
        client = make_sync_client(
            server,
            signing_key=b"testtesttesttesttesttesttesttest",
            authenticate=bad_auth,
        )

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.http"),
            http_connect(LoggingService, client=client) as proxy,
            pytest.raises(RpcError, match="AuthenticationError"),
        ):
            proxy.greet(name="fail")

        warning_records = [r for r in caplog.records if r.name == "vgi_rpc.http" and r.levelno == logging.WARNING]
        assert len(warning_records) >= 1
        record = warning_records[0]
        assert _extra(record, "error_type") == "ValueError"
        assert _extra(record, "auth_error") == "bad token"


# ---------------------------------------------------------------------------
# _emit_access_log exception guard tests
# ---------------------------------------------------------------------------


class TestEmitAccessLogExceptionGuard:
    """Tests for _emit_access_log not raising when handler throws."""

    def test_handler_exception_does_not_propagate(self) -> None:
        """_emit_access_log should swallow handler exceptions."""
        access_logger = logging.getLogger("vgi_rpc.access")

        class _BrokenHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                raise RuntimeError("handler exploded")

        handler = _BrokenHandler()
        access_logger.addHandler(handler)
        old_level = access_logger.level
        access_logger.setLevel(logging.DEBUG)
        try:
            # Should NOT raise
            _emit_access_log(
                protocol_name="TestService",
                method_name="do_thing",
                method_type="unary",
                server_id="srv1",
                auth=AuthContext.anonymous(),
                transport_metadata={},
                duration_ms=1.0,
                status="ok",
            )
        finally:
            access_logger.removeHandler(handler)
            access_logger.setLevel(old_level)


# ---------------------------------------------------------------------------
# VgiJsonFormatter reserved key collision tests
# ---------------------------------------------------------------------------


class TestVgiJsonFormatterReservedKeys:
    """Tests for VgiJsonFormatter ignoring reserved key collisions."""

    def test_extra_level_does_not_overwrite(self) -> None:
        """Extra field named 'level' should not overwrite the standard level."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.__dict__["level"] = "HACKED"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "WARNING"

    def test_extra_timestamp_does_not_overwrite(self) -> None:
        """Extra field named 'timestamp' should not overwrite the standard timestamp."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.__dict__["timestamp"] = "HACKED"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["timestamp"] != "HACKED"

    def test_extra_message_does_not_overwrite(self) -> None:
        """Extra field named 'message' should not overwrite the standard message."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="original",
            args=(),
            exc_info=None,
        )
        record.__dict__["message"] = "HACKED"
        output = formatter.format(record)
        parsed = json.loads(output)
        # message should be set by getMessage(), not the injected extra
        assert parsed["message"] == "original"

    def test_non_reserved_extra_still_works(self) -> None:
        """Non-reserved extra fields should still appear in output."""
        formatter = VgiJsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.__dict__["server_id"] = "srv1"
        record.__dict__["level"] = "HACKED"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["server_id"] == "srv1"
        assert parsed["level"] == "INFO"


# ---------------------------------------------------------------------------
# HTTP producer stream access log tests
# ---------------------------------------------------------------------------


class TestHttpServerStreamAccessLog:
    """Tests for HTTP producer stream access logging."""

    def test_server_stream_continuation_emits_access_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Producer stream continuation should emit access log."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_StreamService, _StreamServiceImpl(), server_id="stream_srv")
        # Small max_stream_response_bytes to force continuation
        client = make_sync_client(
            server,
            signing_key=b"testtesttesttesttesttesttesttest",
            max_stream_response_bytes=1,
        )

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_StreamService, client=client) as proxy,
        ):
            batches = list(proxy.count(n=3))

        assert len(batches) == 3

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        # Should have at least 2 access log entries: initial + continuation(s)
        assert len(access_records) >= 2
        # Check that continuation entries have stream method type
        continuation_records = [r for r in access_records if _extra(r, "method_type") == "stream"]
        assert len(continuation_records) >= 2

    def test_server_stream_produce_error_reports_status_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Producer stream produce-loop failure should result in status='error' in access log."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_FailStreamService, _FailStreamServiceImpl(), server_id="fail_srv")
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_FailStreamService, client=client) as proxy,
            pytest.raises(RpcError, match="RuntimeError"),
        ):
            list(proxy.fail_stream())

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "error"
        assert _extra(record, "error_type") == "RuntimeError"


# ---------------------------------------------------------------------------
# _drain_stderr edge-case tests
# ---------------------------------------------------------------------------


class TestDrainStderr:
    """Tests for _drain_stderr error-handling paths."""

    def test_oserror_during_iteration(self, caplog: pytest.LogCaptureFixture) -> None:
        """OSError while reading lines should be silently caught."""
        pipe = _FaultyPipe(OSError("fd closed"))
        _drain_stderr(cast(BinaryIO, pipe), logging.getLogger("test.drain"))

    def test_valueerror_during_iteration(self) -> None:
        """ValueError while reading lines should be silently caught."""
        pipe = _FaultyPipe(ValueError("I/O on closed file"))
        _drain_stderr(cast(BinaryIO, pipe), logging.getLogger("test.drain"))

    def test_unexpected_exception_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unexpected exceptions should be logged at DEBUG level."""
        pipe = _FaultyPipe(RuntimeError("unexpected"))
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc"):
            _drain_stderr(cast(BinaryIO, pipe), logging.getLogger("test.drain"))

        debug_records = [r for r in caplog.records if r.name == "vgi_rpc.rpc" and "Unexpected error" in r.message]
        assert len(debug_records) == 1


class _FaultyPipe:
    """Fake pipe whose iterator raises a given exception."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self._closed = False

    def __iter__(self) -> _FaultyPipe:
        return self

    def __next__(self) -> bytes:
        raise self._exc

    def close(self) -> None:
        self._closed = True


# ---------------------------------------------------------------------------
# SubprocessTransport.proc and close() edge-case tests
# ---------------------------------------------------------------------------


class TestSubprocessTransportEdgeCases:
    """Tests for SubprocessTransport edge cases."""

    def test_proc_property_returns_popen(self) -> None:
        """Verify proc property returns the underlying Popen object."""
        script = "import sys; sys.stdin.buffer.read()"
        transport = SubprocessTransport([sys.executable, "-c", script])
        try:
            assert transport.proc is not None
            assert transport.proc.pid > 0
        finally:
            transport.close()

    def test_close_timeout_kills_process(self) -> None:
        """Kill the process when it doesn't exit within timeout."""
        import subprocess
        from unittest.mock import patch

        # Spawn a process that hangs
        script = "import time; time.sleep(3600)"
        transport = SubprocessTransport([sys.executable, "-c", script])

        # Make wait() always raise TimeoutExpired so close() hits the kill path
        original_wait = transport._proc.wait

        def fake_wait(timeout: float | None = None) -> int:
            if timeout is not None:
                raise subprocess.TimeoutExpired(cmd="test", timeout=timeout)
            return original_wait()

        with patch.object(transport._proc, "wait", side_effect=fake_wait):
            transport.close()

        assert transport._proc.returncode is not None

    def test_close_idempotent(self) -> None:
        """Calling close() twice should be safe."""
        script = ""
        transport = SubprocessTransport([sys.executable, "-c", script])
        transport.close()
        transport.close()  # should not raise


# ---------------------------------------------------------------------------
# serve_stdio tests
# ---------------------------------------------------------------------------


def _make_stdio_fixtures(*, tty: bool) -> tuple[Any, Any, Any]:
    """Create fake stdin/stdout and a patched fdopen for serve_stdio tests.

    Returns (fake_stdin, fake_stdout, patched_fdopen_func).
    The caller must close the underlying pipe fds after use.
    """
    r_fd, w_fd = os.pipe()
    r2_fd, w2_fd = os.pipe()
    os.close(w_fd)  # EOF on reader → server sees end-of-stream

    reader = os.fdopen(r_fd, "rb")
    writer = os.fdopen(w2_fd, "wb", buffering=0)

    def _isatty(self: Any) -> bool:
        return tty

    fake_stdin = cast(
        Any,
        type("FakeStdin", (), {"isatty": _isatty, "fileno": lambda self: reader.fileno()})(),
    )
    fake_stdout = cast(
        Any,
        type("FakeStdout", (), {"isatty": _isatty, "fileno": lambda self: writer.fileno()})(),
    )

    call_count = 0

    def patched_fdopen(fd: int, *args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return reader
        return writer

    # Stash references for cleanup
    fake_stdin._cleanup = (r2_fd, reader, writer)
    return fake_stdin, fake_stdout, patched_fdopen


class TestServeStdio:
    """Tests for serve_stdio() entry point."""

    def test_tty_warning(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Emit a warning when stdin/stdout is a terminal."""
        from unittest.mock import patch

        server = RpcServer(LoggingService, LoggingServiceImpl())
        fake_stdin, fake_stdout, patched_fdopen = _make_stdio_fixtures(tty=True)

        with (
            patch("sys.stdin", fake_stdin),
            patch("sys.stdout", fake_stdout),
            patch("os.fdopen", patched_fdopen),
        ):
            serve_stdio(server)

        captured = capsys.readouterr()
        assert "not intended to be run interactively" in captured.err

        r2_fd, reader, writer = fake_stdin._cleanup
        os.close(r2_fd)
        reader.close()
        writer.close()

    def test_non_tty_with_debug_logging(self) -> None:
        """Exercise the wire debug log path on a non-tty."""
        from unittest.mock import patch

        from vgi_rpc.rpc._debug import wire_transport_logger

        server = RpcServer(LoggingService, LoggingServiceImpl())
        fake_stdin, fake_stdout, patched_fdopen = _make_stdio_fixtures(tty=False)

        old_level = wire_transport_logger.level
        wire_transport_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        wire_transport_logger.addHandler(handler)
        try:
            with (
                patch("sys.stdin", fake_stdin),
                patch("sys.stdout", fake_stdout),
                patch("os.fdopen", patched_fdopen),
            ):
                serve_stdio(server)
        finally:
            wire_transport_logger.setLevel(old_level)
            wire_transport_logger.removeHandler(handler)

        r2_fd, reader, writer = fake_stdin._cleanup
        os.close(r2_fd)
        reader.close()
        writer.close()
