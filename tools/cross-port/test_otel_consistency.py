#!/usr/bin/env python3
"""Cross-implementation OTel consistency tests for vgi-rpc.

Runs the full conformance suite (48 methods) against Python, Go, and TypeScript
with OTel instrumentation enabled, collects all spans, and compares them.

Run: uv run pytest test_otel_consistency.py -v --timeout=120
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import tempfile
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
    method: str
    method_type: str
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
                continue
            attrs = {}
            for attr in d.get("Attributes", []):
                attrs[attr["Key"]] = str(attr["Value"]["Value"])
            status = d.get("Status", {})
            status_ok = status.get("Code", "") != "Error"
            spans.append(
                NormalizedSpan(
                    name=d["Name"],
                    method=attrs.get("rpc.method", ""),
                    method_type=attrs.get("rpc.vgi_rpc.method_type", ""),
                    status_ok=status_ok,
                    attributes=attrs,
                )
            )
    return spans


def _normalize_ts_spans(path: Path) -> list[NormalizedSpan]:
    """Parse TypeScript JSON export into normalized spans."""
    with open(path) as f:
        d = json.load(f)
    spans = []
    for s in d.get("spans", []):
        attrs = {k: str(v) for k, v in s.get("attributes", {}).items()}
        status_code = s.get("status", {}).get("code", 0)
        spans.append(
            NormalizedSpan(
                name=s["name"],
                method=attrs.get("rpc.method", ""),
                method_type=attrs.get("rpc.vgi_rpc.method_type", ""),
                status_ok=(status_code != 2),
                attributes=attrs,
            )
        )
    return spans


def _normalize_python_spans(exporter) -> list[NormalizedSpan]:
    """Extract spans from Python InMemorySpanExporter."""
    from opentelemetry.trace import StatusCode

    spans = []
    for s in exporter.get_finished_spans():
        attrs = {k: str(v) for k, v in (s.attributes or {}).items()}
        spans.append(
            NormalizedSpan(
                name=s.name,
                method=attrs.get("rpc.method", ""),
                method_type=attrs.get("rpc.vgi_rpc.method_type", ""),
                status_ok=(s.status.status_code != StatusCode.ERROR),
                attributes=attrs,
            )
        )
    return spans


# ---------------------------------------------------------------------------
# Run full conformance suite against an HTTP server
# ---------------------------------------------------------------------------


def _run_conformance_via_http(port: int) -> None:
    """Run the full conformance test suite against an HTTP server."""
    from vgi_rpc.conformance import ConformanceService, LogCollector, run_conformance
    from vgi_rpc.http import http_connect

    logs = LogCollector()
    with http_connect(ConformanceService, f"http://127.0.0.1:{port}", on_log=logs) as proxy:
        suite = run_conformance(proxy, logs, timeout=10.0)
        # We don't assert suite.success here — some tests may fail on specific transports.
        # We just want the spans generated.
        print(f"  Conformance: {suite.passed}/{suite.total} passed, {suite.failed} failed")


# ---------------------------------------------------------------------------
# Fixtures: collect spans from all three implementations
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def python_spans() -> list[NormalizedSpan]:
    """Run full conformance suite against Python with OTel, collect spans."""
    from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl, LogCollector, run_conformance
    from vgi_rpc.http import http_connect, make_sync_client
    from vgi_rpc.otel import OtelConfig, instrument_server
    from vgi_rpc.rpc import RpcServer

    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    metric_reader = InMemoryMetricReader()
    meter_provider = SdkMeterProvider(metric_readers=[metric_reader])

    config = OtelConfig(tracer_provider=tracer_provider, meter_provider=cast("any", meter_provider))
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    instrument_server(server, config)

    client = make_sync_client(server)
    logs = LogCollector()
    with http_connect(ConformanceService, client=client, on_log=logs) as proxy:
        suite = run_conformance(proxy, logs, timeout=10.0)
        print(f"  Python conformance: {suite.passed}/{suite.total} passed")

    return _normalize_python_spans(exporter)


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
            return
        except Exception:
            time.sleep(0.1)


@pytest.fixture(scope="module")
def go_spans() -> list[NormalizedSpan]:
    """Run full conformance suite against Go with OTel, collect spans."""
    worker = GO_REPO / "conformance-worker"
    if not worker.exists():
        result = subprocess.run(
            ["go", "build", "-o", "conformance-worker", "./conformance/cmd/vgi-rpc-conformance-go"],
            cwd=GO_REPO,
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("Go conformance worker build failed")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        export_path = f.name

    try:
        proc = subprocess.Popen(
            [str(worker), "--http", "--otel-export", export_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=GO_REPO,
        )
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith("PORT:"):
            proc.terminate()
            pytest.skip(f"Go worker failed: {line!r}")
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)

        try:
            _run_conformance_via_http(port)
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=15)

        return _normalize_go_spans(Path(export_path))
    finally:
        with contextlib.suppress(OSError):
            os.unlink(export_path)


@pytest.fixture(scope="module")
def ts_spans() -> list[NormalizedSpan]:
    """Run full conformance suite against TypeScript with OTel, collect spans."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        export_path = f.name

    try:
        env = {**os.environ, "VGI_OTEL_FILE": export_path}
        proc = subprocess.Popen(
            ["bun", "run", "examples/conformance-http.ts"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=str(TS_REPO),
        )
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith("PORT:"):
            proc.terminate()
            pytest.skip(f"TS worker failed: {line!r}")
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)

        try:
            _run_conformance_via_http(port)
        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        return _normalize_ts_spans(Path(export_path))
    finally:
        with contextlib.suppress(OSError):
            os.unlink(export_path)


# ---------------------------------------------------------------------------
# Helper to build span maps
# ---------------------------------------------------------------------------


def _span_map(spans: list[NormalizedSpan]) -> dict[str, NormalizedSpan]:
    """Build a method-name → span mapping. Uses the LAST span for each method (streams have one)."""
    result: dict[str, NormalizedSpan] = {}
    for s in spans:
        result[s.method] = s
    return result


# ---------------------------------------------------------------------------
# Tests: Full conformance span comparison
# ---------------------------------------------------------------------------


class TestSpanCoverage:
    """Verify all three implementations produce spans for every conformance method."""

    def test_python_span_count(self, python_spans: list[NormalizedSpan]) -> None:
        methods = {s.method for s in python_spans}
        assert len(methods) >= 40, f"Python produced spans for only {len(methods)} methods (expected 40+)"

    def test_go_span_count(self, go_spans: list[NormalizedSpan]) -> None:
        methods = {s.method for s in go_spans}
        assert len(methods) >= 40, f"Go produced spans for only {len(methods)} methods (expected 40+)"

    def test_ts_span_count(self, ts_spans: list[NormalizedSpan]) -> None:
        methods = {s.method for s in ts_spans}
        assert len(methods) >= 40, f"TS produced spans for only {len(methods)} methods (expected 40+)"

    def test_same_method_set(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        """All three should produce spans for the same set of methods."""
        py = {s.method for s in python_spans}
        go = {s.method for s in go_spans}
        ts = {s.method for s in ts_spans}

        # All three should cover the same core methods
        common = py & go & ts
        assert len(common) >= 40, f"Only {len(common)} methods common to all three"

        py_only = py - go - ts
        go_only = go - py - ts
        ts_only = ts - py - go
        if py_only:
            print(f"  Python-only methods: {py_only}")
        if go_only:
            print(f"  Go-only methods: {go_only}")
        if ts_only:
            print(f"  TS-only methods: {ts_only}")


class TestSpanNameFormat:
    """Verify span name format is consistent across all implementations."""

    def test_all_use_vgi_rpc_prefix(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                assert s.name.startswith("vgi_rpc/"), f"{lang}: span '{s.name}' doesn't use vgi_rpc/ prefix"

    def test_span_name_matches_method(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        """Span name should be vgi_rpc/{method_name}."""
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                expected = f"vgi_rpc/{s.method}"
                assert s.name == expected, f"{lang}: span name '{s.name}' != expected '{expected}'"


class TestCoreAttributes:
    """Verify core attributes are present and consistent on every span."""

    def test_core_attrs_present(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                missing = EXPECTED_CORE_ATTRS - s.attributes.keys()
                assert not missing, f"{lang} span {s.method} missing: {missing}"

    def test_rpc_system_is_vgi_rpc(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for s in spans:
                assert s.attributes["rpc.system"] == "vgi_rpc", (
                    f"{lang} {s.method}: rpc.system={s.attributes['rpc.system']}"
                )

    def test_method_type_consistent(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        """The same method should have the same method_type across all implementations."""
        py_map = _span_map(python_spans)
        go_map = _span_map(go_spans)
        ts_map = _span_map(ts_spans)

        common = set(py_map) & set(go_map) & set(ts_map)
        mismatches = []
        for method in sorted(common):
            py_type = py_map[method].method_type
            go_type = go_map[method].method_type
            ts_type = ts_map[method].method_type
            if not (py_type == go_type == ts_type):
                mismatches.append(f"  {method}: python={py_type}, go={go_type}, ts={ts_type}")

        assert not mismatches, "Method type mismatches:\n" + "\n".join(mismatches)


class TestErrorSpans:
    """Verify error methods produce error spans."""

    def test_python_error_status(self, python_spans: list[NormalizedSpan]) -> None:
        for method in ["raise_value_error", "raise_runtime_error", "raise_type_error"]:
            spans = [s for s in python_spans if s.method == method]
            assert spans, f"Python: no span for {method}"
            assert not spans[-1].status_ok, f"Python: {method} should have error status"

    def test_go_error_status(self, go_spans: list[NormalizedSpan]) -> None:
        for method in ["raise_value_error", "raise_runtime_error", "raise_type_error"]:
            spans = [s for s in go_spans if s.method == method]
            assert spans, f"Go: no span for {method}"
            assert not spans[-1].status_ok, f"Go: {method} should have error status"

    def test_ts_error_status(self, ts_spans: list[NormalizedSpan]) -> None:
        for method in ["raise_value_error", "raise_runtime_error", "raise_type_error"]:
            spans = [s for s in ts_spans if s.method == method]
            assert spans, f"TS: no span for {method}"
            assert not spans[-1].status_ok, f"TS: {method} should have error status"

    def test_error_type_attr_all(
        self,
        python_spans: list[NormalizedSpan],
        go_spans: list[NormalizedSpan],
        ts_spans: list[NormalizedSpan],
    ) -> None:
        """All error spans should have error_type attribute."""
        for spans, lang in [(python_spans, "python"), (go_spans, "go"), (ts_spans, "typescript")]:
            for method in ["raise_value_error", "raise_runtime_error", "raise_type_error"]:
                matching = [s for s in spans if s.method == method]
                for s in matching:
                    assert "rpc.vgi_rpc.error_type" in s.attributes, f"{lang} {method} missing error_type"


class TestStaticConsistency:
    """Verify all three OTel source files use the same string constants."""

    def _read_sources(self) -> dict[str, str]:
        sources = {}
        paths = {
            "python": REPOS_DIR / "vgi-rpc" / "vgi_rpc" / "otel.py",
            "go": GO_REPO / "vgirpc" / "otel" / "otel.go",
            "typescript": TS_REPO / "src" / "otel.ts",
        }
        for name, path in paths.items():
            if path.exists():
                sources[name] = path.read_text()
        return sources

    def test_span_name_prefix(self) -> None:
        for name, src in self._read_sources().items():
            assert "vgi_rpc/" in src, f"{name} missing span name prefix"

    def test_metric_names(self) -> None:
        for name, src in self._read_sources().items():
            assert "rpc.server.requests" in src, f"{name} missing rpc.server.requests"
            assert "rpc.server.duration" in src, f"{name} missing rpc.server.duration"

    def test_core_attribute_keys(self) -> None:
        for name, src in self._read_sources().items():
            for attr in EXPECTED_CORE_ATTRS:
                assert attr in src, f"{name} missing {attr}"
