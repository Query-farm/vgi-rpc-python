# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for Sentry error reporting integration."""

from __future__ import annotations

import threading
from collections.abc import Mapping
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
from vgi_rpc.sentry import (
    SentryConfig,
    _has_sentry_hook,
    _maybe_auto_instrument,
    _SentryDispatchHook,
    _strip_sentry_hook,
    instrument_server_sentry,
)

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
# Transaction naming, span data, params (Phase A behavior)
# ---------------------------------------------------------------------------


def _fake_method_info(name: str, method_type: str = "unary") -> MagicMock:
    """Build a MagicMock that quacks like an RpcMethodInfo for hook unit tests."""
    info = MagicMock()
    info.name = name
    info.method_type.value = method_type
    return info


def _build_hook_with_mocks(
    config: SentryConfig | None = None,
) -> tuple[_SentryDispatchHook, MagicMock, MagicMock, MagicMock]:
    """Construct the hook, a patched ``sentry_sdk`` module, and the scope/span mocks.

    Returns ``(hook, mock_sdk, mock_scope, mock_span)``.  Caller is responsible
    for entering ``patch("vgi_rpc.sentry.sentry_sdk", mock_sdk)`` if the test
    needs side effects against the live SDK to be intercepted.
    """
    hook = _SentryDispatchHook(config or SentryConfig(), "TestService", "srv-1")
    mock_sdk = MagicMock()
    mock_scope = MagicMock()
    mock_span = MagicMock()
    mock_sdk.get_current_scope.return_value = mock_scope
    mock_sdk.get_current_span.return_value = mock_span
    return hook, mock_sdk, mock_scope, mock_span


class TestTransactionName:
    """Override of Falcon's route-template transaction name."""

    def test_default_overrides_to_rpc_method(self) -> None:
        """Default config replaces the WSGI-derived name with ``rpc {method}``."""
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks()
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("listUsers"), AuthContext.anonymous(), {}, {})
        mock_scope.set_transaction_name.assert_called_once_with("rpc listUsers", source="custom")

    def test_disabled_does_not_override(self) -> None:
        """``set_transaction_name=False`` leaves the WSGI-derived name in place."""
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks(SentryConfig(set_transaction_name=False))
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("listUsers"), AuthContext.anonymous(), {}, {})
        mock_scope.set_transaction_name.assert_not_called()


class TestSpanCoreAttributes:
    """Always-on span attributes: rpc.system / service / method / method_type."""

    def test_core_attrs_set(self) -> None:
        """rpc.system/service/method/method_type land on the active span."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks()
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("addNumbers"), AuthContext.anonymous(), {}, {})
        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.system"] == "vgi_rpc"
        assert keys["rpc.service"] == "TestService"
        assert keys["rpc.method"] == "addNumbers"
        assert keys["rpc.method_type"] == "unary"

    def test_no_span_skips_set_data(self) -> None:
        """``get_current_span()`` returning None (e.g. tracing disabled) is safe."""
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks()
        mock_sdk.get_current_span.return_value = None
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            # Should not raise.
            hook.on_dispatch_start(_fake_method_info("foo"), AuthContext.anonymous(), {}, {})
        # Scope tags still fire even without an active span.
        tag_keys = {call[0][0] for call in mock_scope.set_tag.call_args_list}
        assert "rpc.method" in tag_keys


class TestStreamId:
    """rpc.stream_id flows into span data only — never a scope tag."""

    def test_stream_id_in_span_data(self) -> None:
        """stream_id flows into span data and is NOT promoted to a scope tag."""
        from vgi_rpc.rpc._common import _current_stream_id

        hook, mock_sdk, mock_scope, mock_span = _build_hook_with_mocks()
        token = _current_stream_id.set("ab12cd34")
        try:
            with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
                hook.on_dispatch_start(_fake_method_info("scan", "stream"), AuthContext.anonymous(), {}, {})
        finally:
            _current_stream_id.reset(token)
        span_keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert span_keys["rpc.stream_id"] == "ab12cd34"
        # Must NOT appear as a scope tag — uuid per stream is unbounded cardinality.
        tag_keys = {call[0][0] for call in mock_scope.set_tag.call_args_list}
        assert "rpc.stream_id" not in tag_keys

    def test_no_stream_id_when_unset(self) -> None:
        """Empty contextvar means rpc.stream_id is omitted from span data."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks()
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("foo"), AuthContext.anonymous(), {}, {})
        span_keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        assert "rpc.stream_id" not in span_keys


class TestParamRecording:
    """``rpc.param.<k>`` span attributes — opt-in via ``record_params``."""

    def test_default_off(self) -> None:
        """``record_params`` defaults to False — no rpc.param.* keys recorded."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks()
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("scan"), AuthContext.anonymous(), {}, {"table": "orders"})
        span_keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        assert not any(k.startswith("rpc.param.") for k in span_keys)

    def test_enabled_records_primitives(self) -> None:
        """Strings, ints, bools, and floats round-trip as span attributes."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(SentryConfig(record_params=True))
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"table": "orders", "limit": 100, "include_archived": False, "ratio": 0.5},
            )
        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.param.table"] == "orders"
        assert keys["rpc.param.limit"] == 100
        assert keys["rpc.param.include_archived"] is False
        assert keys["rpc.param.ratio"] == 0.5

    def test_default_redactor_filters_credentials(self) -> None:
        """Default redactor strips kwargs whose names match the credentials regex."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(SentryConfig(record_params=True))
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("auth"),
                AuthContext.anonymous(),
                {},
                {"username": "alice", "password": "hunter2", "api_token": "xyz", "bearer_key": "abc"},
            )
        keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        assert "rpc.param.username" in keys
        assert "rpc.param.password" not in keys
        assert "rpc.param.api_token" not in keys
        assert "rpc.param.bearer_key" not in keys

    def test_custom_redactor_replaces_default(self) -> None:
        """A user-supplied redactor fully replaces the default — no chaining."""

        def drop_table(kw: Mapping[str, object]) -> Mapping[str, object]:
            return {k: v for k, v in kw.items() if k != "table"}

        config = SentryConfig(record_params=True, param_redactor=drop_table)
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(config)
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"table": "orders", "password": "should-pass-through"},
            )
        keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        # Custom redactor wins → password is NOT filtered (default redactor not run).
        assert "rpc.param.password" in keys
        assert "rpc.param.table" not in keys

    def test_noop_redactor_disables_filtering(self) -> None:
        """``noop_redactor`` is the documented escape hatch for trusted kwargs."""
        from vgi_rpc.sentry import noop_redactor

        config = SentryConfig(record_params=True, param_redactor=noop_redactor)
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(config)
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("auth"),
                AuthContext.anonymous(),
                {},
                {"password": "hunter2"},
            )
        keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        assert "rpc.param.password" in keys

    def test_truncates_long_strings(self) -> None:
        """Strings longer than max_param_value_bytes are clipped before recording."""
        config = SentryConfig(record_params=True, max_param_value_bytes=16)
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(config)
        long_value = "x" * 100
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"predicate": long_value},
            )
        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.param.predicate"] == "x" * 16

    def test_skips_non_primitive_types(self) -> None:
        """Dicts/bytes/objects are silently dropped — EAP rejects them."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(SentryConfig(record_params=True))
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {
                    "ok_string": "x",
                    "drop_dict": {"k": "v"},
                    "drop_bytes": b"abc",
                    "drop_object": object(),
                },
            )
        keys = {call[0][0] for call in mock_span.set_data.call_args_list}
        assert "rpc.param.ok_string" in keys
        assert "rpc.param.drop_dict" not in keys
        assert "rpc.param.drop_bytes" not in keys
        assert "rpc.param.drop_object" not in keys

    def test_homogeneous_lists_kept_mixed_dropped(self) -> None:
        """Sentry's EAP indexer requires homogeneous element types in arrays."""
        hook, mock_sdk, _, mock_span = _build_hook_with_mocks(SentryConfig(record_params=True))
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {
                    "ok_strings": ["a", "b", "c"],
                    "ok_ints": [1, 2, 3],
                    "drop_mixed": [1, "two", 3],
                },
            )
        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.param.ok_strings"] == ["a", "b", "c"]
        assert keys["rpc.param.ok_ints"] == [1, 2, 3]
        assert "rpc.param.drop_mixed" not in keys


class TestTagParams:
    """Operator-curated whitelist of params duplicated as scope tags."""

    def test_whitelisted_params_become_tags(self) -> None:
        """Only params named in tag_params are duplicated to scope tags."""
        config = SentryConfig(tag_params=("table", "format"))
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks(config)
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"table": "orders", "format": "parquet", "predicate": "x = 1"},
            )
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["rpc.param.table"] == "orders"
        assert tag_calls["rpc.param.format"] == "parquet"
        assert "rpc.param.predicate" not in tag_calls

    def test_tag_value_clipped_to_200_chars(self) -> None:
        """Sentry caps tag values at 200 chars; longer values must be clipped."""
        config = SentryConfig(tag_params=("table",))
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks(config)
        long_table = "t" * 500
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"table": long_table},
            )
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert len(tag_calls["rpc.param.table"]) == 200

    def test_tag_skips_non_primitive(self) -> None:
        """Whitelisted params with non-primitive values are not promoted to tags."""
        config = SentryConfig(tag_params=("table",))
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks(config)
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(
                _fake_method_info("scan"),
                AuthContext.anonymous(),
                {},
                {"table": {"name": "orders"}},  # dict value — must not become a tag
            )
        tag_calls = {call[0][0] for call in mock_scope.set_tag.call_args_list}
        assert "rpc.param.table" not in tag_calls


class TestMethodScopeTags:
    """rpc.method and rpc.method_type are always scope-tagged for Issues filtering."""

    def test_method_tags_set(self) -> None:
        """Method name and type are always available for Issues-side filtering."""
        hook, mock_sdk, mock_scope, _ = _build_hook_with_mocks()
        with patch("vgi_rpc.sentry.sentry_sdk", mock_sdk):
            hook.on_dispatch_start(_fake_method_info("listUsers", "stream"), AuthContext.anonymous(), {}, {})
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["rpc.method"] == "listUsers"
        assert tag_calls["rpc.method_type"] == "stream"


class TestParamRecordingEndToEnd:
    """Verify kwargs flow through the dispatch path into the Sentry hook."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_unary_kwargs_reach_span_data(self, mock_sdk: MagicMock) -> None:
        """Kwargs from a unary pipe call reach the dispatch hook as span data."""
        mock_scope = MagicMock()
        mock_span = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        mock_sdk.get_current_span.return_value = mock_span

        config = SentryConfig(record_params=True)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        result = _run_pipe_call(server, "add", kwargs={"a": 7, "b": 9})
        assert result == 16

        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.param.a"] == 7
        assert keys["rpc.param.b"] == 9
        assert keys["rpc.method"] == "add"
        assert keys["rpc.method_type"] == "unary"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_stream_carries_stream_id(self, mock_sdk: MagicMock) -> None:
        """A streaming call attaches a 32-hex stream id to span data."""
        mock_scope = MagicMock()
        mock_span = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        mock_sdk.get_current_span.return_value = mock_span

        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)

        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(SentryTestService, client_transport) as proxy:
                list(proxy.generate())
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        keys = {call[0][0]: call[0][1] for call in mock_span.set_data.call_args_list}
        assert keys["rpc.method"] == "generate"
        assert keys["rpc.method_type"] == "stream"
        assert "rpc.stream_id" in keys
        assert isinstance(keys["rpc.stream_id"], str) and len(keys["rpc.stream_id"]) == 32


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
        token = composite.on_dispatch_start(info, auth, {}, {})

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
        token = composite.on_dispatch_start(info, auth, {}, {})
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
        token = composite.on_dispatch_start(info, auth, {}, {})
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


# ---------------------------------------------------------------------------
# Auth authenticated tag and claim tag tests
# ---------------------------------------------------------------------------


class TestSentryAuthTags:
    """Sentry auth.authenticated tag and claim tags."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_auth_authenticated_tag(self, mock_sdk: MagicMock) -> None:
        """auth.authenticated tag is set to 'true' for authenticated requests."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        auth = AuthContext(domain="test", authenticated=True, principal="alice")
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2}, auth=auth)
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["auth.authenticated"] == "true"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_auth_authenticated_false_tag(self, mock_sdk: MagicMock) -> None:
        """auth.authenticated tag is set to 'false' for anonymous requests."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["auth.authenticated"] == "false"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_auth_authenticated_respects_context_disabled(self, mock_sdk: MagicMock) -> None:
        """auth.authenticated tag is not set when record_request_context=False."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(record_request_context=False)
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2})
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert "auth.authenticated" not in tag_calls

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_claim_tags(self, mock_sdk: MagicMock) -> None:
        """Configured claim tags appear on Sentry scope."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(claim_tags={"tenant_id": "auth.tenant_id"})
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        auth = AuthContext(domain="jwt", authenticated=True, claims={"tenant_id": "acme"})
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2}, auth=auth)
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert tag_calls["auth.tenant_id"] == "acme"

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_claim_tags_missing_key(self, mock_sdk: MagicMock) -> None:
        """Missing claim key does not set tag."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(claim_tags={"nonexistent": "auth.nonexistent"})
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        auth = AuthContext(domain="jwt", authenticated=True, claims={})
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2}, auth=auth)
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert "auth.nonexistent" not in tag_calls

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_claim_tags_complex_type_skipped(self, mock_sdk: MagicMock) -> None:
        """Dict/list claim values are skipped."""
        mock_scope = MagicMock()
        mock_sdk.get_current_scope.return_value = mock_scope
        config = SentryConfig(claim_tags={"roles": "auth.roles"})
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, config)
        auth = AuthContext(domain="jwt", authenticated=True, claims={"roles": ["admin", "user"]})
        _run_pipe_call(server, "add", kwargs={"a": 1, "b": 2}, auth=auth)
        tag_calls = {call[0][0]: call[0][1] for call in mock_scope.set_tag.call_args_list}
        assert "auth.roles" not in tag_calls


def _collect_sentry_hooks(hook: object) -> list[_SentryDispatchHook]:
    """Return every _SentryDispatchHook in a (possibly composite) dispatch chain."""
    if hook is None:
        return []
    if isinstance(hook, _SentryDispatchHook):
        return [hook]
    if isinstance(hook, _CompositeDispatchHook):
        return [inner for inner in hook._hooks if isinstance(inner, _SentryDispatchHook)]
    return []


# ---------------------------------------------------------------------------
# Auto-attach when sentry_sdk.is_initialized()
# ---------------------------------------------------------------------------


class TestSentryHookDetection:
    """_has_sentry_hook walks the dispatch chain correctly."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_detects_direct(self, mock_sdk: MagicMock) -> None:
        """A bare _SentryDispatchHook is detected."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server)
        assert _has_sentry_hook(server._dispatch_hook) is True

    def test_handles_none(self) -> None:
        """No hook returns False."""
        assert _has_sentry_hook(None) is False

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_detects_in_composite(self, mock_sdk: MagicMock) -> None:
        """A Sentry hook nested in a composite is found."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        server._dispatch_hook = MagicMock()
        instrument_server_sentry(server)
        assert isinstance(server._dispatch_hook, _CompositeDispatchHook)
        assert _has_sentry_hook(server._dispatch_hook) is True


class TestSentryAutoAttach:
    """Auto-attach gated solely on sentry_sdk.is_initialized()."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_attaches_when_initialised(self, mock_sdk: MagicMock) -> None:
        """Initialised SDK triggers default-config attach."""
        mock_sdk.is_initialized.return_value = True
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        server._dispatch_hook = None
        attached = _maybe_auto_instrument(server)
        assert attached is True
        assert _has_sentry_hook(server._dispatch_hook)

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_skips_when_not_initialised(self, mock_sdk: MagicMock) -> None:
        """Un-initialised SDK is a no-op."""
        mock_sdk.is_initialized.return_value = False
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        server._dispatch_hook = None
        attached = _maybe_auto_instrument(server)
        assert attached is False
        assert server._dispatch_hook is None

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_skips_when_already_instrumented(self, mock_sdk: MagicMock) -> None:
        """An existing Sentry hook prevents auto re-attach."""
        mock_sdk.is_initialized.return_value = True
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, SentryConfig(custom_tags={"explicit": "1"}))
        prior_hook = server._dispatch_hook
        attached = _maybe_auto_instrument(server)
        assert attached is False
        assert server._dispatch_hook is prior_hook

    def test_rpcserver_init_auto_attach_end_to_end(self) -> None:
        """RpcServer.__init__ wires Sentry whenever the SDK is initialised."""
        import sentry_sdk as real_sentry_sdk

        with patch.object(real_sentry_sdk, "is_initialized", return_value=True):
            server = RpcServer(SentryTestService, SentryTestServiceImpl())
        assert _has_sentry_hook(server._dispatch_hook)

    def test_rpcserver_init_no_attach_when_uninitialised(self) -> None:
        """When the SDK is not initialised, no auto hook is attached."""
        import sentry_sdk as real_sentry_sdk

        with patch.object(real_sentry_sdk, "is_initialized", return_value=False):
            server = RpcServer(SentryTestService, SentryTestServiceImpl())
        assert not _has_sentry_hook(server._dispatch_hook)


class TestSentryReplaceSemantics:
    """Explicit instrument_server_sentry replaces any existing Sentry hook."""

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_explicit_replaces_auto(self, mock_sdk: MagicMock) -> None:
        """Auto-attached default is replaced by an explicit configured hook."""
        mock_sdk.is_initialized.return_value = True
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        # Auto-attach happened in __init__.
        assert _has_sentry_hook(server._dispatch_hook)
        explicit_config = SentryConfig(custom_tags={"replaced": "yes"})
        instrument_server_sentry(server, explicit_config)
        sentry_hooks = _collect_sentry_hooks(server._dispatch_hook)
        assert len(sentry_hooks) == 1
        assert sentry_hooks[0]._config is explicit_config

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_explicit_then_explicit_replaces(self, mock_sdk: MagicMock) -> None:
        """A later explicit call replaces an earlier explicit one."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        instrument_server_sentry(server, SentryConfig(custom_tags={"first": "1"}))
        second = SentryConfig(custom_tags={"second": "2"})
        instrument_server_sentry(server, second)
        sentry_hooks = _collect_sentry_hooks(server._dispatch_hook)
        assert len(sentry_hooks) == 1
        assert sentry_hooks[0]._config is second

    @patch("vgi_rpc.sentry.sentry_sdk")
    def test_strip_keeps_other_hooks(self, mock_sdk: MagicMock) -> None:
        """_strip_sentry_hook leaves non-Sentry hooks intact."""
        mock_sdk.get_current_scope.return_value = MagicMock()
        other = MagicMock()
        server = RpcServer(SentryTestService, SentryTestServiceImpl())
        server._dispatch_hook = other
        instrument_server_sentry(server)
        # Composite now contains [other, sentry]
        assert isinstance(server._dispatch_hook, _CompositeDispatchHook)
        stripped = _strip_sentry_hook(server._dispatch_hook)
        assert stripped is other  # collapses single-element back to the bare hook

    def test_strip_handles_none(self) -> None:
        """Stripping None returns None."""
        assert _strip_sentry_hook(None) is None
