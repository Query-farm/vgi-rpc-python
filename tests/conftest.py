# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

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

_SKIP_UNIX = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets not available on Windows")

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pipe.py")
_SERVE_FIXTURE_HTTP = str(Path(__file__).parent / "serve_fixture_http.py")
_SERVE_FIXTURE_UNIX = str(Path(__file__).parent / "serve_fixture_unix.py")
_SERVE_FIXTURE_UNIX_THREADED = str(Path(__file__).parent / "serve_fixture_unix_threaded.py")
_CONFORMANCE_PIPE = str(Path(__file__).parent / "serve_conformance_pipe.py")
_CONFORMANCE_HTTP = str(Path(__file__).parent / "serve_conformance_http.py")
_CONFORMANCE_HTTP_SHARED = str(Path(__file__).parent / "serve_conformance_http_shared.py")
_CONFORMANCE_HTTP_AUTH = str(Path(__file__).parent / "serve_conformance_http_auth.py")
_CONFORMANCE_HTTP_STRICT = str(Path(__file__).parent / "serve_conformance_http_strict.py")
_CONFORMANCE_UNIX = str(Path(__file__).parent / "serve_conformance_unix.py")
_CONFORMANCE_UNIX_THREADED = str(Path(__file__).parent / "serve_conformance_unix_threaded.py")

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


def _wait_for_unix(path: str, timeout: float = 5.0) -> None:
    """Poll until a Unix domain socket is accepting connections."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # type: ignore[attr-defined, unused-ignore]
            try:
                sock.connect(path)
                return
            finally:
                sock.close()
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"Unix socket at {path} did not start within {timeout}s")


def _short_unix_path(name: str) -> str:
    """Return a short /tmp path for a Unix domain socket (macOS 104-byte limit)."""
    import os
    import tempfile

    # Use /tmp directly to keep under the 104-byte AF_UNIX limit on macOS
    fd, path = tempfile.mkstemp(prefix=f"vgi-{name}-", suffix=".sock", dir="/tmp")
    os.close(fd)
    os.unlink(path)  # remove file; server will create the socket
    return path


@pytest.fixture(scope="session")
def unix_socket_server() -> Iterator[str]:
    """Spawn a single Unix socket server subprocess for the entire test session."""
    path = _short_unix_path("fix")
    proc = subprocess.Popen(
        [sys.executable, _SERVE_FIXTURE_UNIX, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_unix_path() -> Iterator[str]:
    """Spawn a single conformance Unix socket server for the session."""
    path = _short_unix_path("conf")
    proc = subprocess.Popen(
        [sys.executable, _CONFORMANCE_UNIX, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def unix_threaded_socket_server() -> Iterator[str]:
    """Spawn a threaded Unix socket server subprocess for the entire test session."""
    path = _short_unix_path("fixt")
    proc = subprocess.Popen(
        [sys.executable, _SERVE_FIXTURE_UNIX_THREADED, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_unix_threaded_path() -> Iterator[str]:
    """Spawn a threaded conformance Unix socket server for the session."""
    path = _short_unix_path("cont")
    proc = subprocess.Popen(
        [sys.executable, _CONFORMANCE_UNIX_THREADED, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Fixture: make_conn — parametrized over pipe, subprocess, pool, http, unix, and unix_threaded
# ---------------------------------------------------------------------------


@pytest.fixture(
    params=[
        "pipe",
        "shm_pipe",
        "subprocess",
        "pool",
        "http",
        pytest.param("unix", marks=_SKIP_UNIX),
        pytest.param("unix_threaded", marks=_SKIP_UNIX),
    ]
)
def make_conn(
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
    worker_pool: WorkerPool,
) -> ConnFactory:
    """Return a factory that creates an RPC connection context manager.

    Parametrized over pipe, shm_pipe, subprocess, pool, http, unix, and
    unix_threaded transports so tests automatically run against all seven.
    """
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import RpcServer, ShmPipeTransport, make_pipe_pair, serve_pipe, unix_connect
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
        elif request.param == "unix":
            path: str = request.getfixturevalue("unix_socket_server")
            return unix_connect(RpcFixtureService, path, on_log=on_log)
        elif request.param == "unix_threaded":
            path = request.getfixturevalue("unix_threaded_socket_server")
            return unix_connect(RpcFixtureService, path, on_log=on_log)
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
    return [sys.executable, _CONFORMANCE_HTTP, "--http"]


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
def conformance_http_strict_cap_port() -> Iterator[int]:
    """Spawn a strict-cap conformance HTTP server for HTTP-only strict-fail tests.

    The server is booted with tight ``max_response_bytes`` and
    ``max_externalized_response_bytes`` so the strict-fail conformance
    tests can deliberately overshoot the caps via ``produce_oversized_batch``,
    ``oversized_unary``, and ``exchange_oversized``.
    """
    proc = subprocess.Popen(
        [sys.executable, _CONFORMANCE_HTTP_STRICT],
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
def conformance_fake_storage() -> Iterator[str]:
    """Spawn the in-process fake storage service for external-location tests.

    Yields the base URL (e.g. ``http://127.0.0.1:<port>``).  The service
    runs on a daemon thread inside the pytest process — no subprocess
    needed because the storage state can be safely scoped to the pytest
    process for reads/writes from the conformance worker subprocess.
    """
    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


@pytest.fixture(scope="session")
def conformance_http_with_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a conformance HTTP worker wired against the fake storage service.

    Uses a small (4 KiB) ``externalize_threshold_bytes`` so tests can
    deliberately trigger externalization without producing megabytes of
    payload.
    """
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            _CONFORMANCE_HTTP,
            "--port",
            str(port),
            "--fake-storage",
            conformance_fake_storage,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        actual_port = int(line.split(":", 1)[1])
        _wait_for_http(actual_port)
        yield actual_port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_externalize_always_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a conformance HTTP worker that externalizes EVERY non-empty batch.

    Sets ``--externalize-threshold 1`` so every data-bearing batch (any
    batch with > 0 rows) goes through the upload-URL flow.  Used as a
    transport variant in ``conformance_conn`` so the entire conformance
    suite double-checks that externalization is observationally
    indistinguishable from inline transmission.

    Zero-row batches (logs, EOS markers, void returns) are exempt from
    externalization at the framework level — those still flow inline.
    """
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            _CONFORMANCE_HTTP,
            "--port",
            str(port),
            "--fake-storage",
            conformance_fake_storage,
            # Server externalizes EVERY non-empty response batch.
            "--externalize-threshold",
            "1",
            # Keep the inline-request cap loose so normal client calls
            # (whose bodies are typically a few hundred bytes) don't get
            # 413-rejected — this variant exercises *response*-side
            # externalization across the full method matrix.
            "--max-request-bytes",
            "1048576",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        actual_port = int(line.split(":", 1)[1])
        _wait_for_http(actual_port)
        yield actual_port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a conformance HTTP worker with the fake storage and zstd compression on."""
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            _CONFORMANCE_HTTP,
            "--port",
            str(port),
            "--fake-storage",
            conformance_fake_storage,
            "--compression",
            "zstd",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        actual_port = int(line.split(":", 1)[1])
        _wait_for_http(actual_port)
        yield actual_port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Spawn a conformance HTTP worker with a reject-all auth callback.

    Used by the ``TestHealth`` conformance suite to verify ``GET /health``
    is exempt from authentication: RPC endpoints on this server return
    401, but the health probe must still succeed.
    """
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, _CONFORMANCE_HTTP_AUTH, "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"PORT:{port}", f"Expected PORT:{port}, got: {line!r}"
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


@pytest.fixture(
    params=[
        "pipe",
        "subprocess",
        "http",
        pytest.param(
            "http_roundrobin",
            marks=pytest.mark.skip(reason="flaky under full-suite load; tracked separately"),
        ),
        "http_externalize_always",
        pytest.param("unix", marks=_SKIP_UNIX),
        pytest.param("unix_threaded", marks=_SKIP_UNIX),
    ]
)
def conformance_conn(
    request: pytest.FixtureRequest,
    conformance_http_port: int,
    conformance_subprocess: SubprocessTransport,
) -> ConnFactory:
    """Return a factory for conformance service connections.

    Parametrized over pipe, subprocess, http, unix, and unix_threaded transports.
    """
    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import serve_pipe, unix_connect

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
        elif request.param == "unix":
            path: str = request.getfixturevalue("conformance_unix_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        elif request.param == "unix_threaded":
            path = request.getfixturevalue("conformance_unix_threaded_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        elif request.param == "http_roundrobin":
            ports: tuple[int, int] = request.getfixturevalue("conformance_http_two_servers")
            client = _make_roundrobin_client(ports)
            return http_connect(ConformanceService, client=client, on_log=on_log)
        elif request.param == "http_externalize_always":
            from vgi_rpc.external import ExternalLocationConfig

            ext_port = request.getfixturevalue("conformance_http_externalize_always_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ext_port}",
                on_log=on_log,
                # Server uses http://127.0.0.1 download URLs from the
                # in-process fake storage; disable the HTTPS-only validator.
                external_location=ExternalLocationConfig(url_validator=None),
            )
        else:
            return http_connect(ConformanceService, f"http://127.0.0.1:{conformance_http_port}", on_log=on_log)

    return factory


def _free_port() -> int:
    """Return a free TCP port on localhost."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="session")
def conformance_http_two_servers() -> Iterator[tuple[int, int]]:
    """Spawn two conformance HTTP workers sharing one HMAC signing key.

    Exercises the protocol's "state lives in the signed token" contract:
    state tokens minted by either server must verify and resume on the
    other.  Each server gets a distinct auto-generated ``server_id`` so
    responses can reveal which backend handled each exchange.
    """
    import os
    import tempfile

    key_hex = os.urandom(32).hex()
    port_a = _free_port()
    port_b = _free_port()
    probe_fd, probe_path = tempfile.mkstemp(prefix="vgi-rpc-cancel-probe-", suffix=".json")
    os.close(probe_fd)
    os.unlink(probe_path)
    env = {**os.environ, "VGI_RPC_CONFORMANCE_PROBE_FILE": probe_path}

    def _spawn(port: int) -> subprocess.Popen[bytes]:
        proc = subprocess.Popen(
            [sys.executable, _CONFORMANCE_HTTP_SHARED, "--port", str(port), "--key", key_hex],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"PORT:{port}", f"Expected PORT:{port}, got: {line!r}"
        _wait_for_http(port)
        return proc

    proc_a = _spawn(port_a)
    proc_b = _spawn(port_b)
    try:
        yield (port_a, port_b)
    finally:
        for proc in (proc_a, proc_b):
            proc.terminate()
            proc.wait(timeout=5)
        if os.path.exists(probe_path):
            os.unlink(probe_path)


def _make_roundrobin_client(ports: tuple[int, int]) -> httpx.Client:
    """Build an ``httpx.Client`` that alternates between two ports per request."""
    import itertools

    counter = itertools.count()
    lock = threading.Lock()

    class _RoundRobinTransport(httpx.BaseTransport):
        def __init__(self) -> None:
            self._inner = httpx.HTTPTransport()

        def handle_request(self, request: httpx.Request) -> httpx.Response:
            with lock:
                idx = next(counter) % 2
            port = ports[idx]
            request.url = request.url.copy_with(host="127.0.0.1", port=port)
            return self._inner.handle_request(request)

        def close(self) -> None:
            self._inner.close()

    # base_url is required by httpx but the transport rewrites host:port on every request
    return httpx.Client(
        base_url=f"http://127.0.0.1:{ports[0]}", transport=_RoundRobinTransport(), follow_redirects=True
    )
