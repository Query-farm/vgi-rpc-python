#!/usr/bin/env python3
"""Cross-implementation OTel consistency tests for vgi-rpc.

Collects real OTel span data from Python (in-process), Go (subprocess + file
export), and TypeScript (subprocess + file export), then compares span names,
attributes, and error handling.

Run: uv run pytest test_otel_consistency.py -v --timeout=60
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import httpx
import pytest

REPOS_DIR = Path.home() / "Development"
GO_REPO = REPOS_DIR / "vgi-rpc-go"
TS_REPO = REPOS_DIR / "vgi-rpc-typescript"

EXPECTED_CORE_ATTRS = {"rpc.system", "rpc.service", "rpc.method", "rpc.vgi_rpc.method_type"}


# ---------------------------------------------------------------------------
# Normalized span data
# ---------------------------------------------------------------------------


@dataclass
class NormalizedSpan:
    name: str
    status_ok: bool
    attributes: dict[str, str] = field(default_factory=dict)


def _normalize_go_spans(path: Path) -> list[NormalizedSpan]:
    """Parse Go stdout OTel exporter JSON lines into normalized spans."""
    spans = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            if "Name" not in d:
                continue  # metric line, skip
            attrs = {}
            for attr in d.get("Attributes", []):
                attrs[attr["Key"]] = str(attr["Value"]["Value"])
            status = d.get("Status", {})
            status_ok = status.get("Code", "") == "Ok"
            spans.append(NormalizedSpan(name=d["Name"], status_ok=status_ok, attributes=attrs))
    return spans


def _normalize_ts_spans(path: Path) -> list[NormalizedSpan]:
    """Parse TypeScript JSON export into normalized spans."""
    with open(path) as f:
        d = json.load(f)
    spans = []
    for s in d.get("spans", []):
        attrs = {k: str(v) for k, v in s.get("attributes", {}).items()}
        # OTel SpanStatusCode: 0=UNSET, 1=OK, 2=ERROR
        status_code = s.get("status", {}).get("code", 0)
        spans.append(NormalizedSpan(name=s["name"], status_ok=(status_code != 2), attributes=attrs))
    return spans


def _normalize_python_spans(exporter) -> list[NormalizedSpan]:
    """Extract spans from Python InMemorySpanExporter."""
    from opentelemetry.trace import StatusCode

    spans = []
    for s in exporter.get_finished_spans():
        attrs = {k: str(v) for k, v in (s.attributes or {}).items()}
        spans.append(NormalizedSpan(name=s.name, status_ok=(s.status.status_code != StatusCode.ERROR), attributes=attrs))
    return spans


# ---------------------------------------------------------------------------
# Python OTel (in-process)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def python_spans() -> list[NormalizedSpan]:
    """Collect OTel spans from the Python conformance service via HTTP (in-process)."""
    from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.http import http_connect, make_sync_client
    from vgi_rpc.otel import OtelConfig, instrument_server
    from vgi_rpc.rpc import RpcError, RpcServer

    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    metric_reader = InMemoryMetricReader()
    meter_provider = SdkMeterProvider(metric_readers=[metric_reader])

    config = OtelConfig(tracer_provider=tracer_provider, meter_provider=cast("any", meter_provider))
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    instrument_server(server, config)

    # Use HTTP test client (in-process, no real HTTP server needed)
    client = make_sync_client(server)

    with http_connect(ConformanceService, client=client) as proxy:
        proxy.echo_string(value="hello")
        try:
            proxy.raise_value_error(message="test error")
        except RpcError:
            pass

    return _normalize_python_spans(exporter)


# ---------------------------------------------------------------------------
# Go OTel (subprocess + file export)
# ---------------------------------------------------------------------------


def _start_go_otel(export_path: str) -> tuple[subprocess.Popen, int] | None:
    """Start Go conformance worker with OTel export. Returns (proc, port) or None."""
    worker = GO_REPO / "conformance-worker"
    if not worker.exists():
        result = subprocess.run(
            ["go", "build", "-o", "conformance-worker", "./conformance/cmd/vgi-rpc-conformance-go"],
            cwd=GO_REPO, capture_output=True,
        )
        if result.returncode != 0:
            return None

    proc = subprocess.Popen(
        [str(worker), "--http", "--otel-export", export_path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=GO_REPO,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    if not line.startswith("PORT:"):
        proc.terminate()
        return None
    port = int(line.split(":", 1)[1])
    # Wait for readiness
    for _ in range(20):
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
            break
        except Exception:
            time.sleep(0.1)
    return proc, port


@pytest.fixture(scope="module")
def go_spans() -> list[NormalizedSpan]:
    """Collect OTel spans from Go conformance worker."""
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.http import http_connect
    from vgi_rpc.rpc import RpcError

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        export_path = f.name

    try:
        result = _start_go_otel(export_path)
        if result is None:
            pytest.skip("Go conformance worker not available")
        proc, port = result

        try:
            with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
                proxy.echo_string(value="hello")
                try:
                    proxy.raise_value_error(message="test error")
                except RpcError:
                    pass
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=10)

        return _normalize_go_spans(Path(export_path))
    finally:
        os.unlink(export_path)


@pytest.fixture(scope="module")
def ts_spans() -> list[NormalizedSpan]:
    """Collect OTel spans from TypeScript conformance worker."""
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.http import http_connect
    from vgi_rpc.rpc import RpcError

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        export_path = f.name

    try:
        env = {**os.environ, "VGI_OTEL_FILE": export_path}
        proc = subprocess.Popen(
            ["bun", "run", "examples/conformance-http.ts"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, cwd=str(TS_REPO),
        )
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith("PORT:"):
            proc.terminate()
            pytest.skip(f"TS conformance worker failed to start: {line!r}")
        port = int(line.split(":", 1)[1])

        for _ in range(20):
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
                break
            except Exception:
                time.sleep(0.1)

        try:
            with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
                proxy.echo_string(value="hello")
                try:
                    proxy.raise_value_error(message="test error")
                except RpcError:
                    pass
        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        return _normalize_ts_spans(Path(export_path))
    finally:
        os.unlink(export_path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSpanNames:
    def test_python_span_names(self, python_spans: list[NormalizedSpan]) -> None:
        names = {s.name for s in python_spans}
        assert "vgi_rpc/echo_string" in names
        assert "vgi_rpc/raise_value_error" in names

    def test_go_span_names(self, go_spans: list[NormalizedSpan]) -> None:
        names = {s.name for s in go_spans}
        assert "vgi_rpc/echo_string" in names
        assert "vgi_rpc/raise_value_error" in names

    def test_ts_span_names(self, ts_spans: list[NormalizedSpan]) -> None:
        names = {s.name for s in ts_spans}
        assert "vgi_rpc/echo_string" in names
        assert "vgi_rpc/raise_value_error" in names

    def test_span_name_format_consistent(
        self, python_spans: list[NormalizedSpan], go_spans: list[NormalizedSpan], ts_spans: list[NormalizedSpan],
    ) -> None:
        """All implementations use the same vgi_rpc/{method} format."""
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                assert s.name.startswith("vgi_rpc/"), f"{lang} span name doesn't start with 'vgi_rpc/': {s.name}"


class TestSpanAttributes:
    def test_python_core_attrs(self, python_spans: list[NormalizedSpan]) -> None:
        for s in python_spans:
            assert EXPECTED_CORE_ATTRS.issubset(s.attributes.keys()), f"Python span {s.name} missing attrs: {EXPECTED_CORE_ATTRS - s.attributes.keys()}"

    def test_go_core_attrs(self, go_spans: list[NormalizedSpan]) -> None:
        for s in go_spans:
            assert EXPECTED_CORE_ATTRS.issubset(s.attributes.keys()), f"Go span {s.name} missing attrs: {EXPECTED_CORE_ATTRS - s.attributes.keys()}"

    def test_ts_core_attrs(self, ts_spans: list[NormalizedSpan]) -> None:
        for s in ts_spans:
            assert EXPECTED_CORE_ATTRS.issubset(s.attributes.keys()), f"TS span {s.name} missing attrs: {EXPECTED_CORE_ATTRS - s.attributes.keys()}"

    def test_rpc_system_value(
        self, python_spans: list[NormalizedSpan], go_spans: list[NormalizedSpan], ts_spans: list[NormalizedSpan],
    ) -> None:
        """All implementations set rpc.system to 'vgi_rpc'."""
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                assert s.attributes.get("rpc.system") == "vgi_rpc", f"{lang} rpc.system != 'vgi_rpc': {s.attributes.get('rpc.system')}"

    def test_method_type_value(
        self, python_spans: list[NormalizedSpan], go_spans: list[NormalizedSpan], ts_spans: list[NormalizedSpan],
    ) -> None:
        """All echo_string spans have method_type 'unary'."""
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            echo_spans = [s for s in spans if s.name == "vgi_rpc/echo_string"]
            for s in echo_spans:
                assert s.attributes.get("rpc.vgi_rpc.method_type") == "unary", f"{lang} method_type != 'unary'"


class TestErrorSpans:
    def test_python_error_span(self, python_spans: list[NormalizedSpan]) -> None:
        error_spans = [s for s in python_spans if s.name == "vgi_rpc/raise_value_error"]
        assert len(error_spans) == 1
        assert not error_spans[0].status_ok, "Python error span should have error status"

    def test_go_error_span(self, go_spans: list[NormalizedSpan]) -> None:
        error_spans = [s for s in go_spans if s.name == "vgi_rpc/raise_value_error"]
        assert len(error_spans) == 1
        assert not error_spans[0].status_ok, "Go error span should have error status"

    @pytest.mark.xfail(reason="TS HTTP handler doesn't pass errors to dispatch hook onDispatchEnd")
    def test_ts_error_span(self, ts_spans: list[NormalizedSpan]) -> None:
        error_spans = [s for s in ts_spans if s.name == "vgi_rpc/raise_value_error"]
        assert len(error_spans) == 1
        assert not error_spans[0].status_ok, "TS error span should have error status"

    def test_error_type_attribute(
        self, python_spans: list[NormalizedSpan], go_spans: list[NormalizedSpan], ts_spans: list[NormalizedSpan],
    ) -> None:
        """Python and Go error spans should have an error_type attribute.
        TypeScript is excluded (xfail) since errors aren't passed to the dispatch hook yet.
        """
        for spans, lang in [(python_spans, "python"), (go_spans, "go")]:
            error_spans = [s for s in spans if s.name == "vgi_rpc/raise_value_error"]
            for s in error_spans:
                assert "rpc.vgi_rpc.error_type" in s.attributes, f"{lang} error span missing error_type attribute"


class TestStaticConsistency:
    """Verify all three source files use the same OTel string constants."""

    def _read_sources(self) -> dict[str, str]:
        sources = {}
        py_path = REPOS_DIR / "vgi-rpc" / "vgi_rpc" / "otel.py"
        go_path = GO_REPO / "vgirpc" / "otel" / "otel.go"
        ts_path = TS_REPO / "src" / "otel.ts"
        for name, path in [("python", py_path), ("go", go_path), ("typescript", ts_path)]:
            if path.exists():
                sources[name] = path.read_text()
        return sources

    def test_span_name_prefix(self) -> None:
        for name, source in self._read_sources().items():
            assert "vgi_rpc/" in source, f"{name} otel source missing span name prefix 'vgi_rpc/'"

    def test_metric_names(self) -> None:
        for name, source in self._read_sources().items():
            assert "rpc.server.requests" in source, f"{name} missing metric name rpc.server.requests"
            assert "rpc.server.duration" in source, f"{name} missing metric name rpc.server.duration"

    def test_core_attribute_keys(self) -> None:
        for name, source in self._read_sources().items():
            for attr in ["rpc.system", "rpc.service", "rpc.method", "rpc.vgi_rpc.method_type"]:
                assert attr in source, f"{name} missing attribute key {attr}"
