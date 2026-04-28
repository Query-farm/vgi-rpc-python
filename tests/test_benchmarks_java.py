# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""End-to-end RPC benchmarks against the Java vgi-rpc worker.

Mirrors TestRpcBenchmarks in test_benchmarks.py but routes calls through the
Java ``benchmark-worker`` (built via ``./gradlew installDist`` in the
vgi-rpc-java repo).  Same test names so pytest-benchmark JSON output sits
side-by-side with the Python results for direct comparison.

Run with::

    JAVA_BENCHMARK_WORKER=/path/to/benchmark-worker \
      uv run pytest tests/test_benchmarks_java.py \
      --benchmark-enable --benchmark-only -o "addopts=" --timeout=300
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from vgi_rpc.http import http_connect
from vgi_rpc.rpc import AnnotatedBatch, unix_connect

from .test_rpc import Color, RpcFixtureService

pytestmark = pytest.mark.benchmark

JAVA_BENCHMARK_WORKER = os.environ.get(
    "JAVA_BENCHMARK_WORKER",
    str(Path.home() / "Development/vgi-rpc-java/benchmark-worker/build/install/benchmark-worker/bin/benchmark-worker"),
)


def _wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ = httpx.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"Java HTTP worker on port {port} did not start within {timeout}s")


def _wait_for_unix(path: str, timeout: float = 10.0) -> None:
    af_unix = getattr(socket, "AF_UNIX", None)
    if af_unix is None:
        raise RuntimeError("Unix domain sockets are not supported on this platform")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.socket(af_unix, socket.SOCK_STREAM) as s:
                s.connect(path)
                return
        except (FileNotFoundError, ConnectionRefusedError):
            time.sleep(0.05)
    raise TimeoutError(f"Java unix worker at {path} did not start within {timeout}s")


@pytest.fixture(scope="session")
def java_http_port() -> Iterator[int]:
    """Start the Java benchmark worker in HTTP mode and yield its port."""
    if not Path(JAVA_BENCHMARK_WORKER).exists():
        pytest.skip(f"Java benchmark worker not built: {JAVA_BENCHMARK_WORKER}")
    proc = subprocess.Popen(
        [JAVA_BENCHMARK_WORKER, "--http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)


@pytest.fixture(scope="session")
def java_unix_path() -> Iterator[str]:
    """Start the Java benchmark worker on a Unix socket and yield its path."""
    if not Path(JAVA_BENCHMARK_WORKER).exists():
        pytest.skip(f"Java benchmark worker not built: {JAVA_BENCHMARK_WORKER}")
    tmp = tempfile.mkdtemp(prefix="vgi-java-bench-")
    sock_path = str(Path(tmp) / "worker.sock")
    proc = subprocess.Popen(
        [JAVA_BENCHMARK_WORKER, "--unix", sock_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_unix(sock_path)
        yield sock_path
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sock_path)


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


@pytest.fixture(scope="session", autouse=True)
def _java_jit_warmup(java_http_port: int, java_unix_path: str) -> None:
    """Warm the JVM so it hits steady-state before pytest-benchmark starts timing.

    Without this the first benchmark sees C1-tier code; subsequent ones see C2;
    pytest-benchmark's adaptive round-counting can settle before the hot path is
    fully optimized.
    """
    for url_or_path, connect in (
        (f"http://127.0.0.1:{java_http_port}", http_connect),
        (java_unix_path, unix_connect),
    ):
        with connect(RpcFixtureService, url_or_path) as proxy:
            for _ in range(2000):
                proxy.noop()
                proxy.add(a=1.0, b=2.0)
                proxy.greet(name="warmup")


@pytest.fixture(params=["java_http", "java_unix"])
def make_conn(
    request: pytest.FixtureRequest,
) -> ConnFactory:
    """Route connections through the Java benchmark worker (http or unix)."""

    def factory(on_log: Callable[..., None] | None = None) -> contextlib.AbstractContextManager[Any]:
        if request.param == "java_http":
            port: int = request.getfixturevalue("java_http_port")
            return http_connect(RpcFixtureService, f"http://127.0.0.1:{port}", on_log=on_log)
        path: str = request.getfixturevalue("java_unix_path")
        return unix_connect(RpcFixtureService, path, on_log=on_log)

    return factory


# ---------------------------------------------------------------------------
# Mirrors TestRpcBenchmarks in test_benchmarks.py
# ---------------------------------------------------------------------------


class TestJavaRpcBenchmarks:
    """End-to-end RPC benchmarks against the Java worker."""

    def test_unary_noop(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark a no-op unary call."""
        with make_conn() as proxy:
            benchmark(proxy.noop)

    def test_unary_add(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark an add unary call."""
        with make_conn() as proxy:
            benchmark(proxy.add, a=1.0, b=2.0)

    def test_unary_greet(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark a greet unary call."""
        with make_conn() as proxy:
            benchmark(proxy.greet, name="benchmark")

    def test_unary_roundtrip_types(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark a roundtrip-types unary call."""
        with make_conn() as proxy:
            benchmark(
                proxy.roundtrip_types,
                color=Color.GREEN,
                mapping={"x": 1},
                tags=frozenset({7}),
            )

    def test_stream_producer(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark a producer streaming call."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                return list(proxy.generate(count=50))

        benchmark(run)

    def test_stream_exchange(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark an exchange streaming call."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                session = proxy.transform(factor=2.0)
                results = []
                for i in range(20):
                    batch = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [float(i)]}))
                    results.append(session.exchange(batch))
                session.close()
                return results

        benchmark(run)
