# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for OpenTelemetry server-side instrumentation."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import Protocol, cast

import pyarrow as pa
import pytest
from aioresponses import aioresponses as aioresponses_ctx
from opentelemetry.metrics import MeterProvider
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pyarrow import ipc

from vgi_rpc.external import (
    ExternalLocationConfig,
    ExternalStorage,
    make_external_location_batch,
    maybe_externalize_batch,
    resolve_external_location,
)
from vgi_rpc.external_fetch import FetchConfig
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.otel import OtelConfig, instrument_server
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    OutputCollector,
    ProducerState,
    RpcConnection,
    RpcError,
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
        assert attrs["rpc.vgi_rpc.auth.principal"] == "alice"
        assert attrs["rpc.vgi_rpc.auth.domain"] == "test"
        assert attrs["rpc.vgi_rpc.auth.authenticated"] == "true"
        assert "enduser.id" not in attrs  # deprecated semconv removed

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
# Claim attribute tests
# ---------------------------------------------------------------------------


class TestClaimAttributes:
    """Tests for configurable claim extraction in OTel spans."""

    def test_claim_attributes(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Configured claim keys appear in span attributes."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            claim_attributes={"tenant": "rpc.vgi_rpc.auth.claim.tenant"},
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, principal="alice", claims={"tenant": "acme"})
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
        assert attrs["rpc.vgi_rpc.auth.claim.tenant"] == "acme"

    def test_claim_attributes_non_string_value(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Int and bool claim values pass through correctly."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            claim_attributes={
                "level": "rpc.vgi_rpc.auth.claim.level",
                "admin": "rpc.vgi_rpc.auth.claim.admin",
            },
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, claims={"level": 42, "admin": True})
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
        assert attrs["rpc.vgi_rpc.auth.claim.level"] == "42"
        assert attrs["rpc.vgi_rpc.auth.claim.admin"] == "True"

    def test_claim_attributes_complex_type_skipped(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Dict/list claim values are silently skipped."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            claim_attributes={"roles": "rpc.vgi_rpc.auth.claim.roles"},
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, claims={"roles": ["admin", "user"]})
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
        assert "rpc.vgi_rpc.auth.claim.roles" not in attrs

    def test_claim_attributes_none_value_skipped(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """None claim value does not set attribute."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            claim_attributes={"missing": "rpc.vgi_rpc.auth.claim.missing"},
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, claims={"missing": None})
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
        assert "rpc.vgi_rpc.auth.claim.missing" not in attrs

    def test_claim_attributes_missing_key(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Absent claim key does not cause KeyError."""
        tracer_provider, meter_provider, exporter, _ = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            claim_attributes={"nonexistent": "rpc.vgi_rpc.auth.claim.nonexistent"},
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, claims={})
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
        assert "rpc.vgi_rpc.auth.claim.nonexistent" not in attrs


# ---------------------------------------------------------------------------
# Metric auth domain tests
# ---------------------------------------------------------------------------


class TestMetricAuthDomain:
    """Tests for auth domain in metrics."""

    def test_metric_auth_domain(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Metrics include rpc.vgi_rpc.auth.domain when auth has a domain."""
        tracer_provider, meter_provider, _exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        instrument_server(server, config)

        auth = AuthContext(domain="jwt", authenticated=True, principal="alice")
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

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        found_domain = False
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.requests":
                        for dp in metric.data.data_points:
                            attrs = dict(dp.attributes or {})
                            if attrs.get("rpc.vgi_rpc.auth.domain") == "jwt":
                                found_domain = True
        assert found_domain, "rpc.vgi_rpc.auth.domain not found in metrics"


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


# ---------------------------------------------------------------------------
# Auth failure counter tests
# ---------------------------------------------------------------------------


class TestAuthFailureCounter:
    """Tests for auth failure OTel counter on HTTP transport."""

    def test_auth_failure_counter(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """HTTP auth failure increments counter and produces no dispatch span."""
        tracer_provider, meter_provider, exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )

        def _bad_auth(req: object) -> AuthContext:
            raise ValueError("bad token")

        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config, authenticate=_bad_auth)
        with (
            pytest.raises(RpcError, match="bad token"),
            http_connect(OtelTestService, "http://test", client=client) as proxy,
        ):
            proxy.add(a=1, b=2)

        # No dispatch span (auth failed before dispatch)
        spans = exporter.get_finished_spans()
        assert len(spans) == 0

        # Auth failure counter should have incremented
        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        found_counter = False
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.auth_failure":
                        total = sum(dp.value for dp in metric.data.data_points)  # type: ignore[union-attr, misc]
                        assert total >= 1
                        found_counter = True
        assert found_counter, "rpc.server.auth_failure counter not found"

    def test_auth_failure_counter_distinguishes_error_types(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """ValueError and PermissionError appear with different error.type labels."""
        tracer_provider, meter_provider, _exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )

        call_count = 0

        def _alternating_auth(req: object) -> AuthContext:
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 1:
                raise ValueError("bad token")
            raise PermissionError("forbidden")

        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config, authenticate=_alternating_auth)

        # Make two failing calls
        for _ in range(2):
            with (
                pytest.raises(RpcError),
                http_connect(OtelTestService, "http://test", client=client) as proxy,
            ):
                proxy.add(a=1, b=2)

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        error_types: set[str] = set()
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.auth_failure":
                        for dp in metric.data.data_points:
                            attrs = dict(dp.attributes or {})
                            et = attrs.get("error.type")
                            if et:
                                error_types.add(str(et))
        assert "ValueError" in error_types
        assert "PermissionError" in error_types

    def test_auth_failure_counter_not_wired_without_otel(self) -> None:
        """make_wsgi_app without otel_config does not create counter."""
        import falcon

        def _bad_auth(req: falcon.Request) -> AuthContext:
            raise ValueError("bad token")

        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", authenticate=_bad_auth)
        with (
            pytest.raises(RpcError, match="bad token"),
            http_connect(OtelTestService, "http://test", client=client) as proxy,
        ):
            proxy.add(a=1, b=2)
        # No crash — counter callback was None

    def test_auth_failure_counter_includes_custom_attributes(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """custom_attributes from config appear on auth failure counter metrics."""
        tracer_provider, meter_provider, _exporter, metric_reader = otel_providers
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
            custom_attributes={"deployment.environment": "test"},
        )

        def _bad_auth(req: object) -> AuthContext:
            raise ValueError("bad token")

        server = RpcServer(OtelTestService, OtelTestServiceImpl())
        client = make_sync_client(server, signing_key=b"test-key", otel_config=config, authenticate=_bad_auth)
        with (
            pytest.raises(RpcError, match="bad token"),
            http_connect(OtelTestService, "http://test", client=client) as proxy,
        ):
            proxy.add(a=1, b=2)

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None
        found_env = False
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name == "rpc.server.auth_failure":
                        for dp in metric.data.data_points:
                            attrs = dict(dp.attributes or {})
                            if attrs.get("deployment.environment") == "test":
                                found_env = True
        assert found_env, "custom_attributes not found on auth failure counter"


# ---------------------------------------------------------------------------
# External data OTel span tests
# ---------------------------------------------------------------------------

_EXT_SCHEMA = pa.schema([pa.field("value", pa.int64())])


class _MockStorage(ExternalStorage):
    """In-memory ExternalStorage for OTel tests."""

    def __init__(self) -> None:
        """Initialize with empty data store."""
        self.data: dict[str, bytes] = {}
        self._counter = 0

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Store data and return a mock URL."""
        self._counter += 1
        url = f"https://mock.storage/{self._counter}"
        self.data[url] = data
        return url


def _serialize_ipc(
    schema: pa.Schema,
    batches: list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]],
) -> bytes:
    """Serialize a list of (batch, custom_metadata) into IPC stream bytes."""
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        for batch, cm in batches:
            if cm is not None:
                writer.write_batch(batch, custom_metadata=cm)
            else:
                writer.write_batch(batch)
    return buf.getvalue()


@contextmanager
def _mock_aio(storage: _MockStorage) -> Iterator[aioresponses_ctx]:
    """Register all MockStorage URLs in aioresponses."""
    with aioresponses_ctx() as mock:
        for url, body in storage.data.items():
            headers = {"Content-Length": str(len(body))}
            mock.head(url, headers=headers)
            mock.get(url, body=body, headers=headers)
        yield mock


class TestExternalOtel:
    """OTel span tests for external data upload and fetch."""

    @pytest.fixture(autouse=True)
    def _patch_tracer(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Patch external.py to use the test TracerProvider."""
        import vgi_rpc.external as _ext_mod

        tracer_provider = otel_providers[0]

        class _PatchedTrace:
            """Minimal trace module proxy that uses the test TracerProvider."""

            SpanKind = SpanKind
            StatusCode = StatusCode

            @staticmethod
            def get_tracer(name: str, version: str = "") -> object:
                """Return a tracer from the test provider."""
                return tracer_provider.get_tracer(name, version)

        monkeypatch.setattr(_ext_mod, "_otel_trace", _PatchedTrace)
        monkeypatch.setattr(_ext_mod, "_HAS_OTEL", True)

    def test_upload_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Upload creates a span with correct attributes."""
        _, _, exporter, _ = otel_providers
        storage = _MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=1)

        batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_EXT_SCHEMA)
        maybe_externalize_batch(batch, None, config)

        spans = exporter.get_finished_spans()
        upload_spans = [s for s in spans if s.name == "vgi_rpc.external/upload"]
        assert len(upload_spans) == 1
        span = upload_spans[0]
        assert span.kind == SpanKind.CLIENT
        attrs = dict(span.attributes or {})
        assert attrs["url"] == "https://mock.storage/1"
        assert isinstance(attrs["upload_bytes"], int) and attrs["upload_bytes"] > 0
        assert "compression" not in attrs

    def test_upload_span_with_compression(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Upload span includes compression and original_bytes when compressed."""
        _, _, exporter, _ = otel_providers
        from vgi_rpc.external import Compression

        storage = _MockStorage()
        config = ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=1,
            compression=Compression(),
        )

        batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_EXT_SCHEMA)
        maybe_externalize_batch(batch, None, config)

        spans = exporter.get_finished_spans()
        upload_spans = [s for s in spans if s.name == "vgi_rpc.external/upload"]
        assert len(upload_spans) == 1
        attrs = dict(upload_spans[0].attributes or {})
        assert attrs["compression"] == "zstd"
        assert isinstance(attrs["original_bytes"], int) and attrs["original_bytes"] > 0

    def test_fetch_span(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Fetch creates a span with correct attributes."""
        _, _, exporter, _ = otel_providers
        storage = _MockStorage()
        data_batch = pa.RecordBatch.from_pydict({"value": [99]}, schema=_EXT_SCHEMA)
        ipc_bytes = _serialize_ipc(_EXT_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/fetch1"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_EXT_SCHEMA, url)
        config = ExternalLocationConfig(
            storage=storage,
            url_validator=None,
            fetch_config=FetchConfig(parallel_threshold_bytes=len(ipc_bytes) + 1000),
        )

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 1
        spans = exporter.get_finished_spans()
        fetch_spans = [s for s in spans if s.name == "vgi_rpc.external/fetch"]
        assert len(fetch_spans) == 1
        span = fetch_spans[0]
        assert span.kind == SpanKind.CLIENT
        assert span.status.status_code == StatusCode.OK
        attrs = dict(span.attributes or {})
        assert attrs["url"] == "https://mock.storage/fetch1"
        assert isinstance(attrs["fetch_duration_ms"], float) and attrs["fetch_duration_ms"] >= 0

    def test_fetch_span_error(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Fetch span records error status on failure."""
        _, _, exporter, _ = otel_providers
        url = "https://mock.storage/fail"
        pointer, cm = make_external_location_batch(_EXT_SCHEMA, url)
        config = ExternalLocationConfig(
            url_validator=None,
            max_retries=0,
            fetch_config=FetchConfig(parallel_threshold_bytes=999999),
        )

        with aioresponses_ctx() as mock:
            mock.head(url, exception=OSError("connection refused"))
            mock.get(url, exception=OSError("connection refused"))
            with pytest.raises(RuntimeError, match="Failed to resolve"):
                resolve_external_location(pointer, cm, config)

        spans = exporter.get_finished_spans()
        fetch_spans = [s for s in spans if s.name == "vgi_rpc.external/fetch"]
        assert len(fetch_spans) == 1
        span = fetch_spans[0]
        assert span.status.status_code == StatusCode.ERROR
        event_names = [e.name for e in span.events]
        assert "exception" in event_names

    def test_upload_span_sanitizes_url(
        self,
        otel_providers: tuple[TracerProvider, SdkMeterProvider, InMemorySpanExporter, InMemoryMetricReader],
    ) -> None:
        """Upload span strips query params from URL to avoid credential leakage."""
        from vgi_rpc.external import _sanitize_url

        assert _sanitize_url("https://bucket.s3.amazonaws.com/key?X-Amz-Credential=AKIA") == (
            "https://bucket.s3.amazonaws.com/key"
        )
        assert _sanitize_url("https://storage.googleapis.com/b/o?token=secret") == (
            "https://storage.googleapis.com/b/o"
        )
