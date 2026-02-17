"""Shared test fixtures for vgi-rpc tests."""

from __future__ import annotations

import contextlib
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pipe.py")
_SERVE_FIXTURE_HTTP = str(Path(__file__).parent / "serve_fixture_http.py")

ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]
"""Type alias for the ``make_conn`` fixture return type."""


def _worker_cmd() -> list[str]:
    """Return the command to launch the test RPC worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE]


def _http_worker_cmd() -> list[str]:
    """Return the command to launch the test HTTP RPC worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE_HTTP]


def _wait_for_http(port: int, timeout: float = 5.0) -> None:
    """Poll until the HTTP server is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ = httpx.get(f"http://127.0.0.1:{port}/", timeout=5.0)
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


@pytest.fixture(scope="session")
def subprocess_worker() -> Iterator[SubprocessTransport]:
    """Spawn a single subprocess worker for the entire test session."""
    transport = SubprocessTransport(_worker_cmd())
    yield transport
    transport.close()


# ---------------------------------------------------------------------------
# Fixture: make_conn — parametrized over pipe, subprocess, and http transports
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pipe", "shm_pipe", "subprocess", "http"])
def make_conn(
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
) -> ConnFactory:
    """Return a factory that creates an RPC connection context manager.

    Parametrized over pipe, shm_pipe, subprocess, and http transports so
    tests automatically run against all four.
    """
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import RpcServer, ShmPipeTransport, make_pipe_pair, serve_pipe
    from vgi_rpc.shm import ShmSegment

    from .test_rpc import RpcFixtureService, RpcFixtureServiceImpl

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":
            return serve_pipe(RpcFixtureService, RpcFixtureServiceImpl(), on_log=on_log)
        elif request.param == "shm_pipe":

            @contextlib.contextmanager
            def _shm_conn() -> Iterator[_RpcProxy]:
                shm = ShmSegment.create(4 * 1024 * 1024)  # 4 MB
                try:
                    client_pipe, server_pipe = make_pipe_pair()
                    client_transport = ShmPipeTransport(client_pipe, shm)
                    server_transport = ShmPipeTransport(server_pipe, shm)
                    rpc_server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
                    thread = threading.Thread(target=rpc_server.serve, args=(server_transport,), daemon=True)
                    thread.start()
                    try:
                        yield _RpcProxy(RpcFixtureService, client_transport, on_log)
                    finally:
                        client_transport.close()
                        thread.join(timeout=5)
                finally:
                    shm.unlink()
                    with contextlib.suppress(BufferError):
                        shm.close()

            return _shm_conn()
        elif request.param == "subprocess":

            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(RpcFixtureService, subprocess_worker, on_log)

            return _conn()
        else:
            return http_connect(RpcFixtureService, f"http://127.0.0.1:{http_server_port}", on_log=on_log)

    return factory
