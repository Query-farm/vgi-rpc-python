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
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx2
import pytest

from vgi_rpc.introspect import ServiceDescription
from vgi_rpc.pool import WorkerPool
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_SKIP_UNIX = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets not available on Windows")

# Windows + waitress + httpx2 race: under load, waitress closes the response
# socket faster than httpx2 can drain the body, surfacing as WinError 10053
# (connection aborted by software) instead of the actual error/response. The
# ``http_externalize_always`` fixture multiplies request/response cycles via
# upload-URL bootstrap, making the race deterministic on Windows. Pre-existing
# (passed pre-v0.18 CI by luck); skipping the whole variant on Windows until
# we can drain-before-close at the WSGI handler layer.
_SKIP_WIN_EXTERNALIZE = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows + waitress + httpx2 + externalize: pre-existing TCP race (WinError 10053)",
)

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pipe.py")
_SERVE_FIXTURE_HTTP = str(Path(__file__).parent / "serve_fixture_http.py")
_SERVE_FIXTURE_UNIX = str(Path(__file__).parent / "serve_fixture_unix.py")
_SERVE_FIXTURE_UNIX_THREADED = str(Path(__file__).parent / "serve_fixture_unix_threaded.py")
_CONFORMANCE_PIPE = str(Path(__file__).parent / "serve_conformance_pipe.py")
_CONFORMANCE_HTTP = str(Path(__file__).parent / "serve_conformance_http.py")
_CONFORMANCE_HTTP_SHARED = str(Path(__file__).parent / "serve_conformance_http_shared.py")
_CONFORMANCE_HTTP_AUTH = str(Path(__file__).parent / "serve_conformance_http_auth.py")
_CONFORMANCE_HTTP_PROOF = str(Path(__file__).parent / "serve_conformance_http_proof.py")
_CONFORMANCE_HTTP_STRICT = str(Path(__file__).parent / "serve_conformance_http_strict.py")
_CONFORMANCE_UNIX = str(Path(__file__).parent / "serve_conformance_unix.py")
_CONFORMANCE_UNIX_THREADED = str(Path(__file__).parent / "serve_conformance_unix_threaded.py")
_CONFORMANCE_TCP = str(Path(__file__).parent / "serve_conformance_tcp.py")

ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]
"""Type alias for the ``make_conn`` fixture return type."""


def _worker_cmd() -> list[str]:
    """Return the command to launch the test RPC worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE]


def _http_worker_cmd() -> list[str]:
    """Return the command to launch the test HTTP RPC worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE_HTTP]


def _wait_for_http(port: int, timeout: float = 30.0) -> None:
    """Poll until the HTTP server is accepting connections.

    Retries on :class:`httpx2.TransportError` rather than on connect errors
    alone. "Not ready yet" has more than one shape: a server that has bound
    its socket but has not begun serving *accepts* the connection and then
    closes it, which arrives as ``RemoteProtocolError("Server disconnected
    without sending a response")`` -- a sibling of ``ConnectError``, not a
    subclass. Catching only connect errors let that escape the loop and fail
    the fixture rather than retry. The window is narrow enough to be invisible
    when servers start one at a time, and routine under ``-n auto``, where
    several start at once on a loaded box.

    The per-attempt timeout is deliberately much shorter than the deadline:
    with both at 5s a single slow attempt consumed the entire budget, so the
    loop got one try rather than many.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ = httpx2.get(f"http://127.0.0.1:{port}/", timeout=2.0)
            return
        except httpx2.TransportError:
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


def _wait_for_tcp(host: str, port: int, timeout: float = 5.0) -> None:
    """Poll until a TCP socket is accepting connections."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"TCP server at {host}:{port} did not start within {timeout}s")


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
def fixture_tcp_addr() -> Iterator[tuple[str, int]]:
    """Serve the RPC fixture service over TCP for the session.

    In-process on a daemon thread rather than a subprocess: the benchmarks
    that use it are measuring transport cost, and a spawn per parametrisation
    would be pure noise against that. ``serve_tcp`` reports the bound port
    through ``on_bound``, so nothing has to guess a free one.
    """
    from vgi_rpc.rpc import RpcServer, serve_tcp

    from .test_rpc import RpcFixtureService, RpcFixtureServiceImpl

    bound: dict[str, Any] = {}
    ready = threading.Event()

    def _on_bound(host: str, port: int) -> None:
        bound["host"], bound["port"] = host, port
        ready.set()

    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
    thread = threading.Thread(
        target=lambda: serve_tcp(server, "127.0.0.1", 0, threaded=True, on_bound=_on_bound),
        daemon=True,
    )
    thread.start()
    assert ready.wait(10), "serve_tcp did not bind within 10s"
    _wait_for_tcp(bound["host"], bound["port"])
    yield bound["host"], bound["port"]


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


_LAUNCHER_CONFORMANCE_RUNNER = str(Path(__file__).parent / "_launcher_conformance_run_server.py")


@pytest.fixture(scope="session")
def conformance_unix_launcher_path() -> Iterator[str]:
    """Bring up a conformance worker via ``vgi_rpc.launcher.launch``.

    Exercises the full launcher path (flock coordination, deterministic
    socket path, ``run_server --unix`` flag, ``UNIX:<path>`` discovery line,
    idle-timeout supervision) through the existing conformance test matrix.
    """
    import shutil
    import tempfile
    import uuid

    from vgi_rpc.launcher import LaunchConfig, launch

    state_dir = Path(tempfile.gettempdir()) / f"vgi-conf-launcher-{uuid.uuid4().hex[:8]}"
    state_dir.mkdir(mode=0o700)
    config = LaunchConfig(
        worker_argv=(sys.executable, _LAUNCHER_CONFORMANCE_RUNNER),
        # Long enough to outlast the conformance suite session, short enough
        # that a forgotten worker eventually self-cleans.
        idle_timeout=600.0,
        connect_timeout=15.0,
        worker_startup_timeout=30.0,
        state_dir=str(state_dir),
    )
    path = launch(config)
    _wait_for_unix(path)
    try:
        yield path
    finally:
        # The launched worker self-terminates via idle timeout once we stop
        # connecting; we don't need to track its PID.  Clean up the state dir.
        shutil.rmtree(state_dir, ignore_errors=True)


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


@pytest.fixture(scope="session")
def conformance_tcp_addr() -> Iterator[tuple[str, int]]:
    """Spawn a threaded conformance TCP server for the session.

    Binds ``127.0.0.1:0`` (OS auto-selects the port) and reads the
    ``TCP:<host>:<port>`` discovery line to learn the bound address.

    Yields:
        The ``(host, port)`` the server is listening on.

    """
    proc = subprocess.Popen(
        [sys.executable, _CONFORMANCE_TCP, "127.0.0.1", "0"],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("TCP:"), f"Expected TCP:<host>:<port>, got: {line!r}"
        host, _, port_str = line[len("TCP:") :].rpartition(":")
        port = int(port_str)
        _wait_for_tcp(host, port)
        yield (host, port)
    finally:
        proc.terminate()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Fixture: make_conn — parametrized over pipe, subprocess, pool, http, unix, and unix_threaded
# ---------------------------------------------------------------------------


#: Transports every ``make_conn`` test runs against.
#:
#: TCP is deliberately absent. It is covered for *correctness* by the
#: conformance fixtures, and adding an eighth parametrisation here multiplies
#: across every test using this fixture — against a suite budget that is
#: already most of the way spent. Benchmarks, which need the transport
#: comparison and are deselected by default, use ``_BENCH_TRANSPORTS`` below.
_CONN_TRANSPORTS = [
    "pipe",
    "shm_pipe",
    "subprocess",
    "pool",
    "http",
    pytest.param("unix", marks=_SKIP_UNIX),
    pytest.param("unix_threaded", marks=_SKIP_UNIX),
]

#: Transports the benchmarks run against: everything above, plus TCP.
#:
#: A benchmark's whole job is comparing transports, so leaving one out is a
#: hole in the result rather than a saving — and TCP is the one whose numbers
#: are least predictable from the others, having neither a shared page cache
#: nor a local socket's short path.
_BENCH_TRANSPORTS = [*_CONN_TRANSPORTS, "tcp"]


def _build_conn_factory(
    param: str,
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
    worker_pool: WorkerPool,
) -> ConnFactory:
    """Build the connection factory for one transport *param*.

    Shared by :func:`make_conn` and :func:`make_bench_conn` so the two cannot
    drift into testing and benchmarking different things.

    Args:
        param: Transport name.
        request: The requesting fixture context, for lazy server fixtures.
        http_server_port: Port of the session HTTP worker.
        subprocess_worker: The session subprocess worker.
        worker_pool: The session worker pool.

    Returns:
        A callable taking an optional ``on_log`` and returning a connection
        context manager.

    """
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import RpcServer, ShmPipeTransport, make_pipe_pair, serve_pipe, tcp_connect, unix_connect
    from vgi_rpc.shm import ShmSegment

    from .test_rpc import RpcFixtureService, RpcFixtureServiceImpl

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if param == "pipe":
            return serve_pipe(RpcFixtureService, RpcFixtureServiceImpl(), on_log=on_log)
        if param == "shm_pipe":

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
        if param == "subprocess":

            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(RpcFixtureService, subprocess_worker, on_log)

            return _conn()
        if param == "pool":
            return worker_pool.connect(RpcFixtureService, _worker_cmd(), on_log=on_log)
        if param == "unix":
            path: str = request.getfixturevalue("unix_socket_server")
            return unix_connect(RpcFixtureService, path, on_log=on_log)
        if param == "unix_threaded":
            path = request.getfixturevalue("unix_threaded_socket_server")
            return unix_connect(RpcFixtureService, path, on_log=on_log)
        if param == "tcp":
            tcp_host, tcp_port = request.getfixturevalue("fixture_tcp_addr")
            return tcp_connect(RpcFixtureService, tcp_host, tcp_port, on_log=on_log)
        return http_connect(RpcFixtureService, f"http://127.0.0.1:{http_server_port}", on_log=on_log)

    return factory


@pytest.fixture(params=_CONN_TRANSPORTS)
def make_conn(
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
    worker_pool: WorkerPool,
) -> ConnFactory:
    """Return a connection factory, parametrized over the seven core transports."""
    return _build_conn_factory(request.param, request, http_server_port, subprocess_worker, worker_pool)


@pytest.fixture(params=_BENCH_TRANSPORTS)
def make_bench_conn(
    request: pytest.FixtureRequest,
    http_server_port: int,
    subprocess_worker: SubprocessTransport,
    worker_pool: WorkerPool,
) -> ConnFactory:
    """Return a connection factory over every transport, TCP included.

    Benchmarks only; see :data:`_BENCH_TRANSPORTS`.
    """
    return _build_conn_factory(request.param, request, http_server_port, subprocess_worker, worker_pool)


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


@contextmanager
def _spawn_conformance_http(*extra_args: str) -> Iterator[int]:
    """Spawn a conformance HTTP worker with *extra_args*, yielding its port."""
    proc = subprocess.Popen(
        [*_conformance_http_cmd(), *extra_args],
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


# Shared AEAD key for the sticky worker pair.  Both peers can decrypt each
# other's tokens, which is the point: the rejection under test must come
# from the server_id comparison, not from a failed decrypt.
_STICKY_PEER_TOKEN_KEY = "5f" * 32


@pytest.fixture(scope="session")
def conformance_http_sticky_short_ttl_port() -> Iterator[int]:
    """Spawn a sticky conformance HTTP worker with a ~1s default session TTL.

    Backs the canonical ``TestSticky::test_expired_session_surfaces_session_lost``.
    The main ``conformance_http_port`` worker uses the framework's 300s
    default, which no test can outwait.

    Passed as an integer, not ``1.0``: ``VGI-Sticky-Default-TTL`` is advertised
    in whole seconds, and cross-language workers parse the flag as an integer.
    """
    with _spawn_conformance_http("--sticky-ttl", "1") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_sticky_peer_ports() -> Iterator[tuple[int, int]]:
    """Spawn two sticky conformance HTTP workers that share one AEAD token key.

    Backs the canonical ``TestSticky::test_token_from_other_worker_rejected``.
    Sharing the key is deliberate — it isolates the ``server_id`` check from
    the decryption failure a per-process key would produce anyway.
    """
    with (
        _spawn_conformance_http("--token-key", _STICKY_PEER_TOKEN_KEY) as port_a,
        _spawn_conformance_http("--token-key", _STICKY_PEER_TOKEN_KEY) as port_b,
    ):
        yield port_a, port_b


@pytest.fixture(scope="session")
def conformance_http_sticky_auth_port() -> Iterator[int]:
    """Spawn a sticky conformance HTTP worker that authenticates by header.

    The worker maps ``X-Conformance-Principal: <name>`` to an authenticated
    principal, so ``TestSticky::test_cross_principal_replay_rejected`` can
    present one principal's session token as another principal.
    """
    with _spawn_conformance_http("--sticky-auth") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_cold_call_cache_port() -> Iterator[int]:
    """Spawn a conformance HTTP server with the call-state cache disabled.

    Backs the shared ``TestColdCallStateCache`` group.  The cache is a pure
    accelerator, so with it warm a client that never echoes the call token
    still works — the bug only surfaces once a continuation lands on a
    process that has no cached entry.  Booting a worker with the cache off
    makes every continuation take that path, so the client's obligation is
    checked deterministically instead of by luck.
    """
    with _spawn_conformance_http("--no-call-state-cache") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_access_log(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[int, Path]]:
    """Spawn a conformance HTTP worker writing an access log, yielding (port, path).

    Backs the shared ``TestRequestId`` correlation case.  Asserting that the
    ``X-Request-ID`` on a response matches the ``request_id`` in the record
    requires reading back what the server logged for a request the suite made,
    which no amount of poking at the wire can substitute for.
    """
    log_path = tmp_path_factory.mktemp("accesslog") / "conformance.log"
    with _spawn_conformance_http("--access-log", str(log_path)) as port:
        yield port, log_path


@pytest.fixture(scope="session")
def conformance_http_introspect_port() -> Iterator[int]:
    """Spawn a conformance HTTP worker with token introspection enabled.

    Backs the shared ``TestTokenIntrospection`` group.  It needs its own
    worker because the route is absent unless explicitly enabled -- which
    ``TestTokenIntrospectionOffMode`` asserts against the default worker.
    """
    with _spawn_conformance_http("--introspect") as port:
        yield port


#: Origin the CORS conformance worker is configured to allow. Shared with the
#: canonical ``TestCors`` group, which sends it as the ``Origin`` request
#: header; a runner supplying its own worker must allow this exact value.
CONFORMANCE_CORS_ORIGIN = "https://conformance.example"


@pytest.fixture(scope="session")
def conformance_http_cors_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a conformance HTTP server with CORS *and* storage enabled.

    Backs the shared ``TestCors`` group.  It needs its own worker because
    CORS is a *server configuration* no request can induce, and because the
    default conformance worker deliberately serves no CORS headers -- which
    ``TestCorsOffMode`` asserts.

    Storage is on deliberately.  The exposure check derives what it expects
    from what the worker advertises, so a *plain* worker under-tests it:
    the conditional capability headers -- upload URLs, the size caps,
    externalisation -- are never advertised, so a server that forgets to
    expose them still passes.  Those are the likeliest to be missed
    precisely because a default worker never exercises them.
    """
    with _spawn_conformance_http(
        "--cors-origin",
        CONFORMANCE_CORS_ORIGIN,
        "--fake-storage",
        conformance_fake_storage,
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_no_compression_port() -> Iterator[int]:
    """Spawn a conformance HTTP server with response compression disabled.

    Backs the shared ``test_empty_advertisement_means_never_compressed``
    case.  It needs its own server because the state under test is a
    *server configuration* -- "I can produce no codecs" -- which no client
    request can induce.  ``identity`` covers the client-side ability to
    demand an uncompressed body; only a server booted this way emits the
    present-but-empty ``VGI-Supported-Encodings`` that distinguishes
    "speaks no compression" from an absent header on a legacy server.
    """
    proc = subprocess.Popen(
        [*_conformance_http_cmd(), "--no-compression"],
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
def conformance_http_small_request_cap_port() -> Iterator[int]:
    """Spawn the canonical 4 KiB encoded/decoded request-cap worker."""
    with _spawn_conformance_http("--max-request-bytes", "4096") as port:
        yield port


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
def conformance_http_externalized_cap_port(conformance_fake_storage: str) -> Iterator[int]:
    """Spawn a worker whose *external-channel* cap is the one that bites.

    Backs the shared ``TestExternalizedResponseCap`` group.  Two settings
    make this fixture mean what it says:

    * ``--max-externalized-response-bytes`` is tight (64 KiB) so an
      externalised response overshoots it.
    * ``--max-response-bytes`` is deliberately *generous* (8 MiB).  An
      externalised payload leaves only a pointer batch on the wire, so the
      body cap should never be what fails here — if it were tight too, the
      test would pass while proving nothing about the external cap.

    ``--externalize-threshold`` stays at its 4 KiB default so a modest
    payload still externalises, which is what lets the under-cap control
    exercise the same channel without tripping the cap.
    """
    proc = subprocess.Popen(
        [
            sys.executable,
            _CONFORMANCE_HTTP_STRICT,
            "--fake-storage",
            conformance_fake_storage,
            "--max-externalized-response-bytes",
            str(64 * 1024),
            "--max-response-bytes",
            str(8 * 1024 * 1024),
        ],
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
def proof_worker_factory() -> Iterator[Callable[..., Any]]:
    """Spawn conformance workers gated on proxy proof.

    This is the reference implementation of the fixture every other-language
    runner supplies so the shared ``TestProxyProof`` group can drive it. The
    factory shape, rather than one fixture per configuration, is what lets the
    suite add a case without touching five repositories.
    """
    from vgi_rpc.conformance.proof_harness import ProofWorker, ProofWorkerConfig

    @contextlib.contextmanager
    def spawn(config: ProofWorkerConfig) -> Iterator[ProofWorker]:
        port = _free_port()
        cmd = [
            sys.executable,
            _CONFORMANCE_HTTP_PROOF,
            "--port",
            str(port),
            "--proof-mode",
            config.mode,
            "--proof-origin-id",
            config.origin_id,
            "--proof-secrets",
            config.secrets,
            "--proof-skew",
            str(config.skew_seconds),
            "--prefix",
            "/vgi",
        ]
        if not config.replay_cache:
            cmd.append("--proof-no-replay-cache")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line == f"PORT:{port}", f"Expected PORT:{port}, got: {line!r}"
            _wait_for_http(port)
            yield ProofWorker(port=port, prefix="/vgi", config=config)
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    yield spawn


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
def conformance_http_auth_reason_port(conformance_http_auth_port: int) -> int:
    """Return a worker whose ``authenticate`` honours ``X-Conformance-Auth-Reason``.

    Optional across ports: supplying this fixture is how a runner declares
    that its worker maps the header onto the matching reason code, which is
    what makes ``TestUnauthorized``'s discrimination tests meaningful. A port
    that omits it skips those tests instead of passing them vacuously.

    The reject-all worker already implements the header, so this is an alias
    rather than a second process.
    """
    return conformance_http_auth_port


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
        pytest.param("http_externalize_always", marks=_SKIP_WIN_EXTERNALIZE),
        pytest.param("unix", marks=_SKIP_UNIX),
        pytest.param("unix_threaded", marks=_SKIP_UNIX),
        pytest.param("unix_launcher", marks=_SKIP_UNIX),
        "tcp",
    ]
)
def conformance_conn(
    request: pytest.FixtureRequest,
    conformance_http_port: int,
    conformance_subprocess: SubprocessTransport,
) -> ConnFactory:
    """Return a factory for conformance service connections.

    Parametrized over pipe, subprocess, http, unix, unix_threaded,
    unix_launcher, and tcp transports.
    """
    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import serve_pipe, tcp_connect, unix_connect

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":
            return serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=on_log)
        if request.param == "subprocess":

            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, conformance_subprocess, on_log)

            return _conn()
        if request.param == "unix":
            path: str = request.getfixturevalue("conformance_unix_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        if request.param == "unix_threaded":
            path = request.getfixturevalue("conformance_unix_threaded_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        if request.param == "unix_launcher":
            path = request.getfixturevalue("conformance_unix_launcher_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        if request.param == "tcp":
            host, tcp_port = request.getfixturevalue("conformance_tcp_addr")
            return tcp_connect(ConformanceService, host, tcp_port, on_log=on_log)
        if request.param == "http_roundrobin":
            ports: tuple[int, int] = request.getfixturevalue("conformance_http_two_servers")
            client = _make_roundrobin_client(ports)
            return http_connect(ConformanceService, client=client, on_log=on_log)
        if request.param == "http_externalize_always":
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
        return http_connect(ConformanceService, f"http://127.0.0.1:{conformance_http_port}", on_log=on_log)

    return factory


@pytest.fixture(
    params=[
        "pipe",
        "subprocess",
        "http",
        pytest.param("http_externalize_always", marks=_SKIP_WIN_EXTERNALIZE),
        pytest.param("unix", marks=_SKIP_UNIX),
        pytest.param("unix_threaded", marks=_SKIP_UNIX),
        pytest.param("unix_launcher", marks=_SKIP_UNIX),
        "tcp",
    ]
)
def conformance_describe(
    request: pytest.FixtureRequest,
    conformance_http_port: int,
    conformance_subprocess: SubprocessTransport,
) -> ServiceDescription:
    """Return a ``ServiceDescription`` obtained by calling ``__describe__`` over the wire.

    Parallels ``conformance_conn`` — same transport matrix — but instead of a
    proxy it sends a real ``__describe__`` request to the worker under test and
    parses the response.  This is what lets ``TestDescribeConformance`` validate
    introspection against the *actual* worker (subprocess / HTTP / Unix-socket
    server) rather than a throwaway in-process Python server.  Every conformance
    worker entry point is launched with ``enable_describe=True`` so the method is
    available on the wire.

    For the ``pipe`` variant there is no separate worker process — the faithful
    equivalent is a fresh in-process pipe server with describe enabled.
    """
    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect
    from vgi_rpc.rpc import RpcServer, TcpTransport, UnixTransport, make_pipe_pair

    param = request.param
    if param == "pipe":
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            return introspect(client_transport)
        finally:
            client_transport.close()
            thread.join(timeout=5)
    if param == "subprocess":
        return introspect(conformance_subprocess)
    if param in ("unix", "unix_threaded", "unix_launcher"):
        import socket as _socket

        if sys.platform == "win32":  # pragma: no cover - unix params skip on Windows
            raise RuntimeError("AF_UNIX is not available on Windows")
        path_fixture = {
            "unix": "conformance_unix_path",
            "unix_threaded": "conformance_unix_threaded_path",
            "unix_launcher": "conformance_unix_launcher_path",
        }[param]
        path: str = request.getfixturevalue(path_fixture)
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        try:
            sock.connect(path)
        except BaseException:
            sock.close()
            raise
        transport = UnixTransport(sock)
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "tcp":
        import socket as _socket

        host, tcp_port = request.getfixturevalue("conformance_tcp_addr")
        sock = _socket.create_connection((host, tcp_port))
        tcp_transport = TcpTransport(sock)
        try:
            return introspect(tcp_transport)
        finally:
            tcp_transport.close()
    if param == "http_externalize_always":
        ext_port: int = request.getfixturevalue("conformance_http_externalize_always_port")
        return http_introspect(base_url=f"http://127.0.0.1:{ext_port}")
    return http_introspect(base_url=f"http://127.0.0.1:{conformance_http_port}")


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


def _make_roundrobin_client(ports: tuple[int, int]) -> httpx2.Client:
    """Build an ``httpx2.Client`` that alternates between two ports per request."""
    import itertools

    counter = itertools.count()
    lock = threading.Lock()

    class _RoundRobinTransport(httpx2.BaseTransport):
        def __init__(self) -> None:
            self._inner = httpx2.HTTPTransport()

        def handle_request(self, request: httpx2.Request) -> httpx2.Response:
            with lock:
                idx = next(counter) % 2
            port = ports[idx]
            request.url = request.url.copy_with(host="127.0.0.1", port=port)
            return self._inner.handle_request(request)

        def close(self) -> None:
            self._inner.close()

    # base_url is required by httpx2 but the transport rewrites host:port on every request
    return httpx2.Client(
        base_url=f"http://127.0.0.1:{ports[0]}", transport=_RoundRobinTransport(), follow_redirects=True
    )
