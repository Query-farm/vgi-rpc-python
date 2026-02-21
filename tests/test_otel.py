# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for OpenTelemetry server-side instrumentation."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
import pytest
from opentelemetry.metrics import MeterProvider
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.otel import OtelConfig, instrument_server
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


class OtelTestService(Protocol):
    """Protocol for OTel tests."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        ...

    def fail(self) -> None:
        """Raise an error unconditionally."""
        ...

    def generate(self) -> Stream[CountState]:
        """Return a producer stream."""
        ...


class OtelTestServiceImpl:
    """Implementation of OtelTestService."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    def fail(self) -> None:
        """Raise an error unconditionally."""
        raise ValueError("intentional error")

    def generate(self) -> Stream[CountState]:
        """Return a producer stream."""
        return Stream(output_schema=pa.schema([pa.field("value", pa.int64())]), state=CountState())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def otel_providers() -> tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader]:
    """Create in-memory OTel providers for testing."""
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))

    metric_reader = InMemoryMetricReader()
    meter_provider = SdkMeterProvider(metric_readers=[metric_reader])

    return tracer_provider, meter_provider, exporter, metric_reader


@pytest.fixture()
def otel_config(
    otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
) -> OtelConfig:
    """Create an OtelConfig with in-memory providers."""
    tracer_provider, meter_provider, _, _ = otel_providers
    return OtelConfig(
        tracer_provider=tracer_provider,
        meter_provider=cast("MeterProvider", meter_provider),
    )


# ---------------------------------------------------------------------------
# Pipe transport tests
# ---------------------------------------------------------------------------


class TestPipeOtel:
    """OTel tests over pipe transport."""

    def test_unary_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Unary call creates a span with correct attributes."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl(), server_id="test123")
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                result = proxy.add(a=2, b=3)
                assert result == 5
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "vgi_rpc/add"
        assert span.kind == SpanKind.SERVER
        assert span.status.status_code == StatusCode.OK
        attrs = dict(span.attributes or {})
        assert attrs["rpc.system"] == "vgi_rpc"
        assert attrs["rpc.service"] == "OtelTestService"
        assert attrs["rpc.method"] == "add"
        assert attrs["rpc.vgi_rpc.method_type"] == "unary"
        assert attrs["rpc.vgi_rpc.server_id"] == "test123"

    def test_stream_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Stream call creates a single span covering the full lifecycle."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                batches = list(proxy.generate())
                assert len(batches) == 3
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "vgi_rpc/generate"
        assert span.status.status_code == StatusCode.OK
        attrs = dict(span.attributes or {})
        assert attrs["rpc.vgi_rpc.method_type"] == "stream"

    def test_error_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Error in dispatch creates an error span with exception recorded."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with (
                RpcConnection(OtelTestService, client_transport) as proxy,
                pytest.raises(Exception, match="intentional error"),
            ):
                proxy.fail()
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.status.status_code == StatusCode.ERROR
        attrs = dict(span.attributes or {})
        assert attrs["rpc.vgi_rpc.error_type"] == "ValueError"
        # Exception should be recorded
        assert len(span.events) >= 1
        event_names = [e.name for e in span.events]
        assert "exception" in event_names

    def test_auth_attributes(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Auth context is included in span attributes."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        # Set auth context via contextvar (simulating pipe transport with auth)
        auth = AuthContext(domain="test", authenticated=True, principal="alice")
        tc = _TransportContext(auth=auth)

        def _serve_with_auth() -> None:
            token = _current_transport.set(tc)
            try:
                server.serve(server_transport)
            finally:
                _current_transport.reset(token)

        thread = threading.Thread(target=_serve_with_auth, daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["enduser.id"] == "alice"
        assert attrs["rpc.vgi_rpc.auth.domain"] == "test"
        assert attrs["rpc.vgi_rpc.auth.authenticated"] == "true"

    def test_custom_attributes(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Custom attributes from OtelConfig are merged into spans."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            custom_attributes={"deployment.environment": "test", "service.version": "1.0"},
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["deployment.environment"] == "test"
        assert attrs["service.version"] == "1.0"

    def test_request_counter(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Request counter increments per call."""
        tracer_provider, meter_provider, _exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
                proxy.add(a=3, b=4)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        counter_found = False
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.requests":
                        counter_found = True
                        total = sum(dp.value for dp in metric.data.data_points)  # type: ignore[union-attr, misc]
                        assert total == 2
        assert counter_found, "rpc.server.requests counter not found"

    def test_duration_histogram(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Duration histogram records positive values."""
        tracer_provider, meter_provider, _exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        histogram_found = False
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.duration":
                        histogram_found = True
                        for dp in metric.data.data_points:
                            assert dp.sum > 0  # type: ignore[union-attr]
                            assert dp.count == 1  # type: ignore[union-attr]
        assert histogram_found, "rpc.server.duration histogram not found"

    def test_tracing_disabled(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """No spans created when tracing is disabled."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            enable_tracing=False,
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 0

    def test_metrics_disabled(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """No metrics recorded when metrics are disabled."""
        tracer_provider, meter_provider, exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            enable_metrics=False,
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(OtelTestService, client_transport) as proxy:
                proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        # Still produces a span
        spans = exporter.get_finished_spans()
        assert len(spans) == 1

        # No metrics recorded
        metrics_data = metric_reader.get_metrics_data()
        has_counter = False
        if metrics_data is not None:
            for resource_metric in metrics_data.resource_metrics:
                for scope_metric in resource_metric.scope_metrics:
                    for metric in scope_metric.metrics:
                        if metric.name == "rpc.server.requests":
                            total = sum(dp.value for dp in metric.data.data_points)  # type: ignore[union-attr, misc]
                            if total > 0:
                                has_counter = True
        assert not has_counter

    def test_pipe_traceparent_propagation(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Span joins parent trace via IPC metadata propagation."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()

        # Create a parent span on the client side using the same tracer provider
        tracer = tracer_provider.get_tracer("test")
        try:
            with tracer.start_as_current_span("client-call") as parent_span:
                parent_ctx = parent_span.get_span_context()
                with RpcConnection(OtelTestService, client_transport) as proxy:
                    proxy.add(a=1, b=2)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        # Should have client span + server span
        assert len(spans) == 2
        server_span = next(s for s in spans if s.name == "vgi_rpc/add")
        client_span = next(s for s in spans if s.name == "client-call")
        # Server span should be a child of client span (same trace ID)
        assert server_span.context.trace_id == client_span.context.trace_id
        assert server_span.parent is not None
        assert server_span.parent.trace_id == parent_ctx.trace_id


# ---------------------------------------------------------------------------
# HTTP transport tests
# ---------------------------------------------------------------------------


class TestHttpOtel:
    """OTel tests over HTTP transport."""

    def test_http_unary_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """HTTP unary call creates a span with correct attributes."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        server = RpcServer(OtelTestService, OtelTestServiceImpl(), server_id="http123")
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config)
        with http_connect(OtelTestService, "http://test", client=client) as proxy:
            result = proxy.add(a=10, b=20)
            assert result == 30

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "vgi_rpc/add"
        assert span.kind == SpanKind.SERVER
        assert span.status.status_code == StatusCode.OK
        attrs = dict(span.attributes or {})
        assert attrs["rpc.system"] == "vgi_rpc"
        assert attrs["rpc.service"] == "OtelTestService"
        assert attrs["rpc.method"] == "add"
        assert attrs["rpc.vgi_rpc.method_type"] == "unary"
        assert attrs["rpc.vgi_rpc.server_id"] == "http123"

    def test_http_stream_init_and_exchange(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """HTTP stream init creates a span (producer gets one span per HTTP request)."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config)
        with http_connect(OtelTestService, "http://test", client=client) as proxy:
            batches = list(proxy.generate())
            assert len(batches) == 3

        spans = exporter.get_finished_spans()
        # Producer stream init creates at least one span
        assert len(spans) >= 1
        span_names = [s.name for s in spans]
        assert "vgi_rpc/generate" in span_names

    def test_http_traceparent_propagation(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """HTTP span joins parent trace from W3C traceparent header."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        server = RpcServer(OtelTestService, OtelTestServiceImpl())

        # Create a parent span and inject traceparent header
        tracer = tracer_provider.get_tracer("test")
        propagator = TraceContextTextMapPropagator()
        with tracer.start_as_current_span("http-client") as parent_span:
            parent_ctx = parent_span.get_span_context()
            # Inject trace context into headers
            headers: dict[str, str] = {}
            propagator.inject(headers)

            # Use the sync test client with injected headers
            client_with_headers = make_sync_client(
                server, signing_key=b"test-key", otel_config=config, default_headers=headers
            )
            with http_connect(OtelTestService, "http://test", client=client_with_headers) as proxy:
                proxy.add(a=1, b=2)

        spans = exporter.get_finished_spans()
        server_spans = [s for s in spans if s.name == "vgi_rpc/add"]
        assert len(server_spans) >= 1
        server_span = server_spans[-1]  # Take the latest one
        assert server_span.context.trace_id == parent_ctx.trace_id

    def test_http_error_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """HTTP error creates an error span."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config)
        with (
            http_connect(OtelTestService, "http://test", client=client) as proxy,
            pytest.raises(Exception, match="intentional error"),
        ):
            proxy.fail()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.status.status_code == StatusCode.ERROR
        attrs = dict(span.attributes or {})
        assert attrs["rpc.vgi_rpc.error_type"] == "ValueError"
