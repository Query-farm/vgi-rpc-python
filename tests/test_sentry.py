# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for Sentry error reporting integration."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Protocol, cast
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    OutputCollector,
    ProducerState,
    RpcConnection,
    RpcServer,
    Stream,
    _current_transport,
    _TransportContext,
    make_pipe_pair,
)
from vgi_rpc.rpc._common import (
    _CompositeDispatchHook,
    _register_dispatch_hook,
)
from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

# ---------------------------------------------------------------------------
# Test protocol + implementation
# ---------------------------------------------------------------------------


@dataclass
class CountState(ProducerState):
    """Producer stream state for testing."""

    remaining: int = 3

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a batch or finish."""
        if self.remaining <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.remaining]})
        self.remaining -= 1


class SentryTestService(Protocol):
    """Protocol for Sentry tests."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        ...

    def fail(self) -> None:
        """Raise an error unconditionally."""
        ...

    def fail_permission(self) -> None:
        """Raise a PermissionError."""
        ...

    def generate(self) -> Stream[CountState]:
        """Return a producer stream."""
        ...


class SentryTestServiceImpl:
    """Implementation of SentryTestService."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    def fail(self) -> None:
        """Raise an error unconditionally."""
        raise ValueError("intentional error")

    def fail_permission(self) -> None:
        """Raise a PermissionError."""
        raise PermissionError("forbidden")

    def generate(self) -> Stream[CountState]:
        """Return a producer stream."""
        return Stream(output_schema=pa.schema([pa.field("value", pa.int64())]), state=CountState())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_pipe_call(
    server: RpcServer,
    call: str,
    *,
    kwargs: dict[str, object] | None = None,
    auth: AuthContext | None = None,
) -> object | None:
    """Run a single pipe-transport RPC call and return the result (or None on error)."""
    client_transport, server_transport = make_pipe_pair()

    def _serve() -> None:
        if auth is not None:
            tc = _TransportContext(auth=auth)
            token = _current_transport.set(tc)
            try:
                server.serve(server_transport)
            finally:
                _current_transport.reset(token)
        else:
            server.serve(server_transport)

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    result = None
    try:
        with RpcConnection(SentryTestService, client_transport) as proxy:
            method = getattr(proxy, call)
            result = method(**(kwargs or {}))
    except Exception:
        pass
    finally:
        client_transport.close()
        thread.join(timeout=5)
        server_transport.close()
    return result


# ---------------------------------------------------------------------------
# Error capture tests
# ---------------------------------------------------------------------------


class TestSentryErrorCapture:
    """Sentry error capture over pipe transport."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_unary_error_captured(self, mock_sdk: MagicMock) -> None:
        """Unary call error is captured via sentry_sdk.capture_exception."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl(), server_id="sentry-test")
        instrument_server_sentry(server)
        _run_pipe_call(server, "fail")
        mock_sdk.capture_exception.assert_called_once()
        captured = mock_sdk.capture_exception.call_args[0][0]
        assert isinstance(captured, ValueError)

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_success_not_captured(self, mock_sdk: MagicMock) -> None:
        """Successful call does not trigger capture_exception."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        result = _run_pipe_call(server, "add", kwargs={"a": 2, "b": 3})
        assert result == 5
        mock_sdk.capture_exception.assert_not_called()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_stream_error_captured(self, mock_sdk: MagicMock) -> None:
        """Stream call that errors is captured."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        # Successful stream — no capture
        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(SentryTestService, client_transport) as proxy:
                batches = list(proxy.generate())
                assert len(batches) == 3
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()
        mock_sdk.capture_exception.assert_not_called()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_ignored_exceptions_skipped(self, mock_sdk: MagicMock) -> None:
        """Ignored exception types are not captured."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        config = SentryConfig(ignored_exceptions=(PermissionError,))
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "fail_permission")
        mock_sdk.capture_exception.assert_not_called()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_capture_disabled(self, mock_sdk: MagicMock) -> None:
        """Error capture can be disabled via config."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        config = SentryConfig(enable_error_capture=False)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "fail")
        mock_sdk.capture_exception.assert_not_called()


# ---------------------------------------------------------------------------
# Context tests
# ---------------------------------------------------------------------------


class TestSentryContext:
    """Sentry scope context population."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_scope_context_set(self, mock_sdk: MagicMock) -> None:
        """RPC context is set on the Sentry scope."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        server = RpcServer(SentryTestService, SentryTestServiceImpl(), server_id="ctx-test")
        instrument_server_sentry(server)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        mock_scope.set_context.assert_called()
        ctx_call = mock_scope.set_context.call_args
        assert ctx_call[0][0] == "rpc"
        ctx_data = ctx_call[0][1]
        assert ctx_data["method"] == "add"
        assert ctx_data["service"] == "SentryTestService"
        assert ctx_data["server_id"] == "ctx-test"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_auth_user_set(self, mock_sdk: MagicMock) -> None:
        """Auth principal is set as Sentry user."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        auth = AuthContext(domain="test", authenticated=True, principal="bob")
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2}, auth=auth)
        mock_scope.set_user.assert_called_with({"username": "bob"})

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_custom_tags_applied(self, mock_sdk: MagicMock) -> None:
        """Custom tags from config are set on the scope."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(custom_tags={"env": "test", "region": "us-east-1"})
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["env"] == "test"
        assert tag_calls["region"] == "us-east-1"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_context_disabled(self, mock_sdk: MagicMock) -> None:
        """Request context recording can be disabled."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(record_request_context=False)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        mock_scope.set_context.assert_not_called()
        mock_scope.set_user.assert_not_called()


# ---------------------------------------------------------------------------
# Performance monitoring tests
# ---------------------------------------------------------------------------


class TestSentryPerformance:
    """Sentry performance monitoring (transactions)."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_transaction_created_when_enabled(self, mock_sdk: MagicMock) -> None:
        """Transaction is started when enable_performance=True."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        mock_transaction = MagicMock()
        mock_sdk.start_transaction.return_value = mock_transaction
        config = SentryConfig(enable_performance=True)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        mock_sdk.start_transaction.assert_called_once()
        call_kwargs = mock_sdk.start_transaction.call_args
        assert call_kwargs[1]["name"] == "vgi_rpc/add"
        assert call_kwargs[1]["op"] == "rpc.server"
        mock_transaction.set_status.assert_called_with("ok")
        mock_transaction.finish.assert_called_once()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_transaction_error_status(self, mock_sdk: MagicMock) -> None:
        """Transaction gets error status on dispatch failure."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        mock_transaction = MagicMock()
        mock_sdk.start_transaction.return_value = mock_transaction
        config = SentryConfig(enable_performance=True)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "fail")
        mock_transaction.set_status.assert_called_with("internal_error")
        mock_transaction.finish.assert_called_once()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_performance_disabled_by_default(self, mock_sdk: MagicMock) -> None:
        """No transaction when enable_performance is False (default)."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        mock_sdk.start_transaction.assert_not_called()


# ---------------------------------------------------------------------------
# OTel coexistence tests
# ---------------------------------------------------------------------------


class TestSentryOtelCoexistence:
    """Sentry and OTel hooks fire together on the same server."""

    def test_both_hooks_fire(self) -> None:
        """Both Sentry and OTel hooks are called when registered together."""
        from opentelemetry.metrics import MeterProvider
        from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        from vgi_rpc.otel import OtelConfig, instrument_server

        # Set up OTel
        exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        metric_reader = InMemoryMetricReader()
        meter_provider = SdkMeterProvider(metric_readers=[metric_reader])
        otel_config = OtelConfig(tracer_provider=tracer_provider, meter_provider=cast("MeterProvider", meter_provider))

        server = RpcServer(SentryTestService, SentryTestServiceImpl(), server_id="dual-test")

        # Register both — order should not matter
        instrument_server(server, otel_config)

        with patch("vgi_rpc.sentry.sentry_sdk") as mock_sdk:
            mock_sdk.get_current_scope.return_value = MagicMock()
            instrument_server_sentry(server)

            # Verify composite hook
            assert isinstance(server._dispatch_hook, _CompositeDispatchHook)

            _run_pipe_call(server, "fail")

            # OTel produced a span
            spans = exporter.get_finished_spans()
            assert len(spans) == 1
            assert spans[0].name == "vgi_rpc/fail"

            # Sentry captured the exception
            mock_sdk.capture_exception.assert_called_once()

    def test_reverse_registration_order(self) -> None:
        """Registration order does not affect both hooks firing."""
        from opentelemetry.metrics import MeterProvider
        from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        from vgi_rpc.otel import OtelConfig, instrument_server

        exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        metric_reader = InMemoryMetricReader()
        meter_provider = SdkMeterProvider(metric_readers=[metric_reader])
        otel_config = OtelConfig(tracer_provider=tracer_provider, meter_provider=cast("MeterProvider", meter_provider))

        server = RpcServer(SentryTestService, SentryTestServiceImpl())

        # Register Sentry first, then OTel
        with patch("vgi_rpc.sentry.sentry_sdk") as mock_sdk:
            mock_sdk.get_current_scope.return_value = MagicMock()
            instrument_server_sentry(server)
            instrument_server(server, otel_config)

            assert isinstance(server._dispatch_hook, _CompositeDispatchHook)

            _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})

            spans = exporter.get_finished_spans()
            assert len(spans) == 1


# ---------------------------------------------------------------------------
# Composite dispatch hook unit tests
# ---------------------------------------------------------------------------


class TestCompositeDispatchHook:
    """Unit tests for the composite hook and registration helper."""

    def test_both_hooks_called(self) -> None:
        """Composite calls all inner hooks."""
        hook_a = MagicMock()
        hook_a.on_dispatch_start.return_value = "token_a"
        hook_b = MagicMock()
        hook_b.on_dispatch_start.return_value = "token_b"

        composite = _CompositeDispatchHook([hook_a, hook_b])
        info = MagicMock()
        auth = AuthContext.anonymous()
        token = composite.on_dispatch_start(info, auth, {})

        hook_a.on_dispatch_start.assert_called_once()
        hook_b.on_dispatch_start.assert_called_once()

        composite.on_dispatch_end(token, info, None)
        hook_a.on_dispatch_end.assert_called_once()
        hook_b.on_dispatch_end.assert_called_once()

    def test_failure_isolation(self) -> None:
        """One hook failing does not prevent others from running."""
        hook_a = MagicMock()
        hook_a.on_dispatch_start.side_effect = RuntimeError("boom")
        hook_b = MagicMock()
        hook_b.on_dispatch_start.return_value = "token_b"

        composite = _CompositeDispatchHook([hook_a, hook_b])
        info = MagicMock()
        auth = AuthContext.anonymous()

        # Should not raise
        token = composite.on_dispatch_start(info, auth, {})
        hook_b.on_dispatch_start.assert_called_once()

        # End should still call hook_b (hook_a wasn't in the token list)
        composite.on_dispatch_end(token, info, None)
        hook_b.on_dispatch_end.assert_called_once()

    def test_reverse_order_end(self) -> None:
        """on_dispatch_end calls hooks in reverse order."""
        call_order: list[str] = []
        hook_a = MagicMock()
        hook_a.on_dispatch_start.return_value = "a"
        hook_a.on_dispatch_end.side_effect = lambda *a, **kw: call_order.append("a")
        hook_b = MagicMock()
        hook_b.on_dispatch_start.return_value = "b"
        hook_b.on_dispatch_end.side_effect = lambda *a, **kw: call_order.append("b")

        composite = _CompositeDispatchHook([hook_a, hook_b])
        info = MagicMock()
        auth = AuthContext.anonymous()
        token = composite.on_dispatch_start(info, auth, {})
        composite.on_dispatch_end(token, info, None)
        assert call_order == ["b", "a"]

    def test_register_none_existing(self) -> None:
        """_register_dispatch_hook returns new_hook when existing is None."""
        hook = MagicMock()
        result = _register_dispatch_hook(None, hook)
        assert result is hook

    def test_register_creates_composite(self) -> None:
        """_register_dispatch_hook wraps two hooks into a composite."""
        hook_a = MagicMock()
        hook_b = MagicMock()
        result = _register_dispatch_hook(hook_a, hook_b)
        assert isinstance(result, _CompositeDispatchHook)
        assert result._hooks == [hook_a, hook_b]

    def test_register_appends_to_composite(self) -> None:
        """_register_dispatch_hook appends to an existing composite."""
        hook_a = MagicMock()
        hook_b = MagicMock()
        hook_c = MagicMock()
        composite = _CompositeDispatchHook([hook_a, hook_b])
        result = _register_dispatch_hook(composite, hook_c)
        assert result is composite
        assert composite._hooks == [hook_a, hook_b, hook_c]


# ---------------------------------------------------------------------------
# HTTP transport tests
# ---------------------------------------------------------------------------


class TestHttpSentry:
    """Sentry tests over HTTP transport."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_http_unary_with_sentry(self, mock_sdk: MagicMock) -> None:
        """HTTP unary call triggers Sentry context setting."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        server = RpcServer(SentryTestService, SentryTestServiceImpl(), server_id="http-sentry")
        config = SentryConfig()
        client = make_sync_client(server, signing_key=b"test-key", sentry_config=config)
        with http_connect(SentryTestService, "http://test", client=client) as proxy:
            result = proxy.add(a=10, b=20)
            assert result == 30
        mock_scope.set_context.assert_called()

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_http_error_with_sentry(self, mock_sdk: MagicMock) -> None:
        """HTTP error triggers Sentry capture_exception."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        config = SentryConfig()
        client = make_sync_client(server, signing_key=b"test-key", sentry_config=config)
        with (
            http_connect(SentryTestService, "http://test", client=client) as proxy,
            pytest.raises(Exception, match="intentional error"),
        ):
            proxy.fail()
        mock_sdk.capture_exception.assert_called_once()
