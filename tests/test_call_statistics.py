# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for CallStatistics per-call I/O counters."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Protocol, cast

import pyarrow as pa
import pytest

from vgi_rpc import AnnotatedBatch, CallStatistics, OutputCollector, Stream, StreamState
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    RpcConnection,
    RpcError,
    RpcServer,
    _emit_access_log,
    make_pipe_pair,
    serve_pipe,
)


def _extra(record: logging.LogRecord, key: str) -> Any:
    """Read a dynamic extra field from a log record."""
    return record.__dict__[key]


# ---------------------------------------------------------------------------
# Test Protocol + Implementation
# ---------------------------------------------------------------------------


@dataclass
class _CountState(StreamState):
    """Producer stream: counts down from remaining to 1."""

    remaining: int

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce next countdown value."""
        if self.remaining <= 0:
            out.finish()
            return
        out.emit_pydict({"value": [self.remaining]})
        self.remaining -= 1


@dataclass
class _EchoState(StreamState):
    """Exchange stream: echoes input back as output."""

    factor: int = 2

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo the input value multiplied by factor."""
        val = input.batch.column("x")[0].as_py()
        out.emit_pydict({"y": [val * self.factor]})


class _StatsService(Protocol):
    """Protocol for CallStatistics tests."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        ...

    def noop(self) -> None:
        """Do nothing."""
        ...

    def fail(self) -> None:
        """Raise an error."""
        ...

    def log_and_return(self, x: int) -> int:
        """Log a message and return x."""
        ...

    def count(self, n: int) -> Stream[StreamState]:
        """Return a producer stream."""
        ...

    def echo(self) -> Stream[StreamState]:
        """Return an exchange stream."""
        ...


class _StatsServiceImpl:
    """Implementation of _StatsService."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    def noop(self) -> None:
        """Do nothing."""

    def fail(self) -> None:
        """Raise an error."""
        raise ValueError("intentional error")

    def log_and_return(self, x: int, ctx: CallContext) -> int:
        """Log and return."""
        from vgi_rpc.log import Level

        ctx.client_log(Level.INFO, "test-log")
        return x

    def count(self, n: int) -> Stream[_CountState]:
        """Return a producer stream."""
        return Stream(output_schema=pa.schema([pa.field("value", pa.int64())]), state=_CountState(remaining=n))

    def echo(self) -> Stream[_EchoState]:
        """Return an exchange stream."""
        return Stream(
            output_schema=pa.schema([pa.field("y", pa.int64())]),
            state=_EchoState(),
            input_schema=pa.schema([pa.field("x", pa.int64())]),
        )


# ---------------------------------------------------------------------------
# Unit tests: CallStatistics accumulation
# ---------------------------------------------------------------------------


class TestCallStatisticsUnit:
    """Unit tests for CallStatistics dataclass."""

    def test_default_values(self) -> None:
        """All counters start at zero."""
        stats = CallStatistics()
        assert stats.input_batches == 0
        assert stats.output_batches == 0
        assert stats.input_rows == 0
        assert stats.output_rows == 0
        assert stats.input_bytes == 0
        assert stats.output_bytes == 0

    def test_record_input(self) -> None:
        """record_input increments input counters."""
        stats = CallStatistics()
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3]})
        stats.record_input(batch)
        assert stats.input_batches == 1
        assert stats.input_rows == 3
        assert stats.input_bytes > 0

    def test_record_output(self) -> None:
        """record_output increments output counters."""
        stats = CallStatistics()
        batch = pa.RecordBatch.from_pydict({"x": [10, 20]})
        stats.record_output(batch)
        assert stats.output_batches == 1
        assert stats.output_rows == 2
        assert stats.output_bytes > 0

    def test_accumulation(self) -> None:
        """Multiple records accumulate correctly."""
        stats = CallStatistics()
        b1 = pa.RecordBatch.from_pydict({"a": [1]})
        b2 = pa.RecordBatch.from_pydict({"a": [2, 3]})
        stats.record_input(b1)
        stats.record_input(b2)
        assert stats.input_batches == 2
        assert stats.input_rows == 3

    def test_empty_batch(self) -> None:
        """Zero-row batches still count as a batch."""
        stats = CallStatistics()
        batch = pa.RecordBatch.from_pydict({"a": pa.array([], type=pa.int64())})
        stats.record_output(batch)
        assert stats.output_batches == 1
        assert stats.output_rows == 0


# ---------------------------------------------------------------------------
# Access log stats tests
# ---------------------------------------------------------------------------


class TestAccessLogStats:
    """Tests for stats in the access log."""

    def test_emit_access_log_with_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """_emit_access_log includes 6 stats fields when stats is provided."""
        stats = CallStatistics()
        stats.input_batches = 1
        stats.output_batches = 2
        stats.input_rows = 10
        stats.output_rows = 20
        stats.input_bytes = 100
        stats.output_bytes = 200
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
                stats=stats,
            )

        record = next(r for r in caplog.records if r.name == "vgi_rpc.access")
        assert _extra(record, "input_batches") == 1
        assert _extra(record, "output_batches") == 2
        assert _extra(record, "input_rows") == 10
        assert _extra(record, "output_rows") == 20
        assert _extra(record, "input_bytes") == 100
        assert _extra(record, "output_bytes") == 200

    def test_emit_access_log_without_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """_emit_access_log omits stats fields when stats is None."""
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
        assert not hasattr(record, "input_batches")


# ---------------------------------------------------------------------------
# Pipe transport integration tests
# ---------------------------------------------------------------------------


class TestPipeStats:
    """Stats through pipe transport end-to-end."""

    def test_unary_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unary call produces stats with 1 input batch and >= 1 output batch."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(_StatsService, _StatsServiceImpl()) as proxy,
        ):
            result = proxy.add(a=2, b=3)

        assert result == 5
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "input_batches") == 1
        assert _extra(record, "output_batches") >= 1
        assert _extra(record, "input_rows") == 1
        assert _extra(record, "output_rows") >= 1
        assert _extra(record, "input_bytes") > 0
        assert _extra(record, "output_bytes") >= 0

    def test_unary_with_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unary call with client_log counts log batch as output."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(_StatsService, _StatsServiceImpl()) as proxy,
        ):
            result = proxy.log_and_return(x=42)

        assert result == 42
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        # Should have at least 2 output batches: log batch + result batch
        assert _extra(record, "output_batches") >= 2

    def test_unary_error_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """Error path includes error batch in output stats."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(_StatsService, _StatsServiceImpl()) as proxy,
            pytest.raises(RpcError, match="ValueError"),
        ):
            proxy.fail()

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "error"
        # Error batch is counted as output
        assert _extra(record, "output_batches") >= 1
        assert _extra(record, "input_batches") == 1

    def test_producer_stream_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """Producer stream counts tick batches as input and data batches as output."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            serve_pipe(_StatsService, _StatsServiceImpl()) as proxy,
        ):
            batches = list(proxy.count(n=3))

        assert len(batches) == 3
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "ok"
        # 3 ticks + 1 final tick (for finish) = 4 input batches (ticks)
        assert _extra(record, "input_batches") >= 4  # params + ticks
        # 3 data batches from producer
        assert _extra(record, "output_batches") >= 3

    def test_exchange_stream_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """Exchange stream counts exchange batches in stats."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(_StatsService, _StatsServiceImpl())
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with (
                caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
                RpcConnection(_StatsService, client_transport) as proxy,
            ):
                session = proxy.echo()
                results = []
                for val in [10, 20]:
                    ab = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"x": [val]}))
                    batch = session.exchange(ab)
                    results.append(batch.batch.column("y")[0].as_py())
                session.close()
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        assert results == [20, 40]
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        # params + 2 exchange input batches
        assert _extra(record, "input_batches") >= 3
        # 2 exchange output batches (each with data from _flush_collector)
        assert _extra(record, "output_batches") >= 2

    def test_describe_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """__describe__ path produces stats with 1 output batch."""
        from vgi_rpc.introspect import introspect

        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(_StatsService, _StatsServiceImpl(), enable_describe=True)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
                desc = introspect(client_transport)
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        assert desc is not None
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        describe_records = [r for r in access_records if _extra(r, "method") == "__describe__"]
        assert len(describe_records) >= 1
        record = describe_records[0]
        assert _extra(record, "input_batches") == 1  # params batch
        assert _extra(record, "output_batches") == 1  # describe batch


# ---------------------------------------------------------------------------
# HTTP transport integration tests
# ---------------------------------------------------------------------------


class TestHttpStats:
    """Stats through HTTP transport."""

    def test_http_unary_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP unary call produces stats in access log."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_StatsService, _StatsServiceImpl())
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_StatsService, client=client) as proxy,
        ):
            result = proxy.add(a=5, b=7)

        assert result == 12
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "input_batches") == 1
        assert _extra(record, "output_batches") >= 1
        assert _extra(record, "input_rows") == 1

    def test_http_error_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP error path includes partial stats."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_StatsService, _StatsServiceImpl())
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_StatsService, client=client) as proxy,
            pytest.raises(RpcError, match="ValueError"),
        ):
            proxy.fail()

        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert len(access_records) >= 1
        record = access_records[0]
        assert _extra(record, "status") == "error"
        assert _extra(record, "output_batches") >= 1

    def test_http_describe_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP __describe__ produces stats."""
        from vgi_rpc.http import http_introspect, make_sync_client

        server = RpcServer(_StatsService, _StatsServiceImpl(), enable_describe=True)
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"):
            desc = http_introspect("http://test", client=client)

        assert desc is not None
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        describe_records = [r for r in access_records if _extra(r, "method") == "__describe__"]
        assert len(describe_records) >= 1
        record = describe_records[0]
        assert _extra(record, "output_batches") == 1

    def test_http_exchange_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP exchange per-request stats."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_StatsService, _StatsServiceImpl())
        client = make_sync_client(server, signing_key=b"testtesttesttesttesttesttesttest")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_StatsService, client=client) as proxy,
        ):
            session = proxy.echo()
            results = []
            for val in [10, 20]:
                ab = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"x": [val]}))
                batch = session.exchange(ab)
                results.append(batch.batch.column("y")[0].as_py())
            session.close()

        assert results == [20, 40]
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        # Should have init + exchange records
        assert len(access_records) >= 3  # init + 2 exchanges
        # Exchange records should have stats
        exchange_records = [r for r in access_records if _extra(r, "method_type") == "stream"]
        for record in exchange_records:
            assert hasattr(record, "input_batches")
            assert hasattr(record, "output_batches")

    def test_http_producer_continuation_stats(self, caplog: pytest.LogCaptureFixture) -> None:
        """HTTP producer continuation counts continuation token batch as output."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_StatsService, _StatsServiceImpl())
        # Small max to force continuation
        client = make_sync_client(
            server,
            signing_key=b"testtesttesttesttesttesttesttest",
            max_stream_response_bytes=1,
        )

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.access"),
            http_connect(_StatsService, client=client) as proxy,
        ):
            batches = list(proxy.count(n=3))

        assert len(batches) == 3
        access_records = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        # Should have multiple access logs (init + continuations)
        assert len(access_records) >= 2
        # All records should have stats
        for record in access_records:
            assert hasattr(record, "output_batches")
        # At least one record should have output batches (data or continuation token)
        total_output = sum(_extra(r, "output_batches") for r in access_records)
        assert total_output >= 3  # at least 3 data batches across all requests


# ---------------------------------------------------------------------------
# OTel span attribute tests
# ---------------------------------------------------------------------------


class TestOtelStats:
    """Tests for CallStatistics in OTel span attributes."""

    def test_otel_unary_span_has_stats(self) -> None:
        """OTel span should include 6 stats attributes for unary call."""
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
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )

        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(_StatsService, _StatsServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with RpcConnection(_StatsService, client_transport) as proxy:
                result = proxy.add(a=1, b=2)
                assert result == 3
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["rpc.vgi_rpc.input_batches"] == 1
        assert int(attrs["rpc.vgi_rpc.output_batches"]) >= 1  # type: ignore[arg-type]
        assert attrs["rpc.vgi_rpc.input_rows"] == 1
        assert int(attrs["rpc.vgi_rpc.output_rows"]) >= 1  # type: ignore[arg-type]
        assert int(attrs["rpc.vgi_rpc.input_bytes"]) > 0  # type: ignore[arg-type]
        assert int(attrs["rpc.vgi_rpc.output_bytes"]) >= 0  # type: ignore[arg-type]

    def test_otel_error_span_has_stats(self) -> None:
        """OTel error span should still include stats attributes."""
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
        config = OtelConfig(
            tracer_provider=tracer_provider,
            meter_provider=cast("MeterProvider", meter_provider),
        )

        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(_StatsService, _StatsServiceImpl())
        instrument_server(server, config)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with (
                RpcConnection(_StatsService, client_transport) as proxy,
                pytest.raises(RpcError, match="ValueError"),
            ):
                proxy.fail()
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        # Stats should be present even on error
        assert "rpc.vgi_rpc.input_batches" in attrs
        assert "rpc.vgi_rpc.output_batches" in attrs
        assert int(attrs["rpc.vgi_rpc.output_batches"]) >= 1  # type: ignore[arg-type]  # error batch
