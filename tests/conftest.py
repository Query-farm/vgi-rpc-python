"""Shared test fixtures for vgi-rpc tests."""

from __future__ import annotations

import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

_SERVE_FIXTURE_HTTP = str(Path(__file__).parent / "serve_fixture_http.py")


def _http_worker_cmd() -> list[str]:
    """Return the command to launch the test HTTP RPC worker subprocess."""
    entry_point = shutil.which("vgi-rpc-test-http-worker")
    if entry_point:
        return [entry_point]
    return [sys.executable, _SERVE_FIXTURE_HTTP]


def _wait_for_http(port: int, timeout: float = 5.0) -> None:
    """Poll until the HTTP server is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(f"http://127.0.0.1:{port}/", timeout=5.0)
            del resp
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server on port {port} did not start within {timeout}s")


@pytest.fixture(scope="session")
def http_server_port() -> Iterator[int]:
    """Spawn a single HTTP server subprocess for the entire test session."""
    proc = subprocess.Popen(
        _http_worker_cmd(),
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
        proc.wait(timeout=5)
