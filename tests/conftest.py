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

from vgi_rpc.pool import WorkerPool
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pipe.py")
_SERVE_FIXTURE_HTTP = str(Path(__file__).parent / "serve_fixture_http.py")
_CONFORMANCE_PIPE = str(Path(__file__).parent / "serve_conformance_pipe.py")
_CONFORMANCE_HTTP = str(Path(__file__).parent / "serve_conformance_http.py")

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


@pytest.fixture(scope="session")
def worker_pool() -> Iterator[WorkerPool]:
    """Session-scoped WorkerPool for pool transport tests."""
    pool = WorkerPool(max_idle=4)
    yield pool
    pool.close()


# ---------------------------------------------------------------------------
# Fixture: make_conn — parametrized over pipe, subprocess, pool, and http transports
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pipe", "shm_pipe", "subprocess", "pool", "http"])
def make_conn(
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
    worker_pool: WorkerPool,
) -> ConnFactory:
    """Return a factory that creates an RPC connection context manager.

    Parametrized over pipe, shm_pipe, subprocess, pool, and http transports
    so tests automatically run against all five.
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
        elif request.param == "pool":
            return worker_pool.connect(RpcFixtureService, _worker_cmd(), on_log=on_log)
        else:
            return http_connect(RpcFixtureService, f"http://127.0.0.1:{http_server_port}", on_log=on_log)

    return factory


# ---------------------------------------------------------------------------
# Conformance fixtures
# ---------------------------------------------------------------------------


def _conformance_pipe_cmd() -> list[str]:
    """Return the command to launch the conformance pipe worker."""
    return [sys.executable, _CONFORMANCE_PIPE]


def _conformance_http_cmd() -> list[str]:
    """Return the command to launch the conformance HTTP worker."""
    return [sys.executable, _CONFORMANCE_HTTP]


@pytest.fixture(scope="session")
def conformance_http_port() -> Iterator[int]:
    """Spawn a single conformance HTTP server subprocess for the session."""
    proc = subprocess.Popen(
        _conformance_http_cmd(),
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
def conformance_subprocess() -> Iterator[SubprocessTransport]:
    """Spawn a single conformance subprocess worker for the session."""
    transport = SubprocessTransport(_conformance_pipe_cmd())
    yield transport
    transport.close()


@pytest.fixture(params=["pipe", "subprocess", "http"])
def conformance_conn(
    request: pytest.FixtureRequest,
    conformance_http_port: int,
    conformance_subprocess: SubprocessTransport,
) -> ConnFactory:
    """Return a factory for conformance service connections.

    Parametrized over pipe, subprocess, and http transports.
    """
    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import serve_pipe

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":
            return serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=on_log)
        elif request.param == "subprocess":

            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, conformance_subprocess, on_log)

            return _conn()
        else:
            return http_connect(
                ConformanceService, f"http://127.0.0.1:{conformance_http_port}", on_log=on_log
            )

    return factory
