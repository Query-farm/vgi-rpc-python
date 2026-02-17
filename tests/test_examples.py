"""Tests that verify every example in the examples/ directory runs successfully."""

from __future__ import annotations

import subprocess
import sys

import pytest

from .conftest import _wait_for_http

# ---------------------------------------------------------------------------
# Self-contained examples: just call main()
# ---------------------------------------------------------------------------


class TestSelfContainedExamples:
    """Examples that run entirely in-process via serve_pipe."""

    def test_hello_world(self, capsys: pytest.CaptureFixture[str]) -> None:
        """hello_world.py: basic unary calls over in-process pipe."""
        from examples.hello_world import main

        main()
        out = capsys.readouterr().out
        assert "Hello, World!" in out
        assert "6.0" in out

    def test_streaming(self, capsys: pytest.CaptureFixture[str]) -> None:
        """streaming.py: producer and exchange streams."""
        from examples.streaming import main

        main()
        out = capsys.readouterr().out
        assert "n=4  n^2=16" in out
        assert "output=[10.0, 20.0, 30.0]" in out

    def test_structured_types(self, capsys: pytest.CaptureFixture[str]) -> None:
        """structured_types.py: ArrowSerializableDataclass params and returns."""
        from examples.structured_types import main

        main()
        out = capsys.readouterr().out
        assert "TASK-0" in out
        assert "Total tasks:    2" in out
        assert "High priority:  1" in out


# ---------------------------------------------------------------------------
# Subprocess example: client spawns worker
# ---------------------------------------------------------------------------


class TestSubprocessExample:
    """Example that spawns a subprocess worker."""

    def test_subprocess_client(self, capsys: pytest.CaptureFixture[str]) -> None:
        """subprocess_client.py: connect() spawns subprocess_worker.py."""
        from examples.subprocess_client import main

        main()
        out = capsys.readouterr().out
        assert "add(2, 3)      = 5.0" in out
        assert "multiply(4, 5) = 20.0" in out
        assert "Division by zero" in out


# ---------------------------------------------------------------------------
# HTTP example: server + client pair
# ---------------------------------------------------------------------------


class TestHttpExample:
    """HTTP server + client example pair."""

    def test_http_server_and_client(self) -> None:
        """Start http_server.py, then run http_client.py against it."""
        proc = subprocess.Popen(
            [sys.executable, "examples/http_server.py", "0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line.startswith("Serving DemoService on http://127.0.0.1:"), f"Unexpected output: {line}"
            # Extract port from "Serving DemoService on http://127.0.0.1:<port>"
            port = int(line.rsplit(":", maxsplit=1)[1])

            _wait_for_http(port)

            from examples.http_client import DemoService
            from vgi_rpc.http import http_connect

            with http_connect(DemoService, f"http://127.0.0.1:{port}") as svc:
                assert svc.echo(message="test") == "test"

                batches = list(svc.fibonacci(limit=10))
                fibs = [row["fib"] for b in batches for row in b.batch.to_pylist()]
                assert fibs == [0, 1, 1, 2, 3, 5, 8]
        finally:
            proc.terminate()
            proc.wait(timeout=5)
