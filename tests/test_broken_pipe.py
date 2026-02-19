"""Tests for broken pipe and premature worker exit handling."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from tests.test_pool import PoolTestService, PoolTestServiceImpl
from vgi_rpc import WorkerPool
from vgi_rpc.rpc import (
    AnnotatedBatch,
    RpcConnection,
    RpcError,
    RpcServer,
    make_pipe_pair,
)
from vgi_rpc.rpc._types import rpc_methods
from vgi_rpc.rpc._wire import _send_request

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pool.py")


def _pool_worker_cmd() -> list[str]:
    """Return the command to launch the pool test worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE]


# SIGKILL is instant, but the child stays a zombie until the parent (Popen)
# calls waitpid.  The pipes *are* broken immediately, so a short sleep is
# sufficient to let the kernel tear down the child's file descriptors.
# 0.5 s gives comfortable margin for overloaded CI machines.
_KILL_SETTLE = 0.5


# ---------------------------------------------------------------------------
# Server-side: serve() loop exits cleanly on broken pipe
# ---------------------------------------------------------------------------


class TestServerBrokenPipe:
    """Server serve() loop handles broken pipe gracefully."""

    def test_serve_loop_exits_on_client_disconnect(self) -> None:
        """Server serve() loop exits cleanly when client closes transport."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(PoolTestService, PoolTestServiceImpl())

        serve_done = threading.Event()

        def run_server() -> None:
            server.serve(server_transport)
            serve_done.set()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()

        # Immediately close client transport — server should exit gracefully
        client_transport.close()
        assert serve_done.wait(timeout=5), "Server serve() loop did not exit after client disconnect"
        thread.join(timeout=5)
        server_transport.close()

    def test_serve_loop_survives_then_client_disconnect(self) -> None:
        """One good call, then client closes — server exits cleanly."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(PoolTestService, PoolTestServiceImpl())

        serve_done = threading.Event()

        def run_server() -> None:
            server.serve(server_transport)
            serve_done.set()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()

        # Make one successful call
        with RpcConnection(PoolTestService, client_transport) as svc:
            result = svc.add(a=1.0, b=2.0)
            assert result == 3.0

        # Client transport closed by RpcConnection.__exit__ — server should exit
        assert serve_done.wait(timeout=5), "Server serve() loop did not exit after client disconnect"
        thread.join(timeout=5)
        server_transport.close()

    def test_serve_loop_exits_on_write_to_broken_pipe(self) -> None:
        """Server serve() exits when response write hits BrokenPipeError.

        Closes the client reader *before* sending a request so the server
        reads the request successfully but fails to write the response.
        """
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(PoolTestService, PoolTestServiceImpl())

        serve_done = threading.Event()

        def run_server() -> None:
            server.serve(server_transport)
            serve_done.set()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()

        # Close the client reader FIRST — server can still read requests
        # but its writes will hit BrokenPipeError (read end of s2c pipe closed).
        client_transport.reader.close()

        # Now send a request — server will read it and try to respond.
        info = rpc_methods(PoolTestService)["add"]
        _send_request(client_transport.writer, info, {"a": 1.0, "b": 2.0})

        # Server: reads request OK → computes result → writes response → BrokenPipeError
        assert serve_done.wait(timeout=5), "Server did not exit after broken pipe on write"
        thread.join(timeout=5)
        client_transport.writer.close()
        server_transport.close()


# ---------------------------------------------------------------------------
# Client-side: transport errors become RpcError(TransportError)
# ---------------------------------------------------------------------------


class TestClientTransportError:
    """Client wraps transport errors as RpcError(TransportError)."""

    # -- Unary calls --------------------------------------------------------

    def test_unary_dead_worker_raises_transport_error(self) -> None:
        """Kill worker mid-borrow, next unary call raises TransportError."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    svc.add(a=1.0, b=2.0)

    def test_unary_transport_error_details(self) -> None:
        """TransportError has correct error_type attr and includes method name."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError) as exc_info:
                    svc.add(a=1.0, b=2.0)
                err = exc_info.value
                assert err.error_type == "TransportError"
                assert "add" in err.error_message

    # -- Producer streams (tick) --------------------------------------------

    def test_stream_dead_worker_raises_transport_error(self) -> None:
        """Kill worker during producer stream, tick raises TransportError."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                session = svc.generate(count=100)
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    for _ in session:
                        pass

    # -- Exchange streams ---------------------------------------------------

    def test_exchange_dead_worker_raises_transport_error(self) -> None:
        """Kill worker during exchange stream, exchange() raises TransportError."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                session = svc.echo(dummy=0)
                # One successful exchange
                ab = AnnotatedBatch.from_pydict({"value": [1.0]})
                result = session.exchange(ab)
                assert result.batch.column("value").to_pylist() == [1.0]
                # Kill worker
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))

    # -- Stream init --------------------------------------------------------

    def test_stream_init_dead_worker_raises_transport_error(self) -> None:
        """Kill worker before stream init, caller raises TransportError."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError) as exc_info:
                    svc.generate(count=5)
                assert exc_info.value.error_type == "TransportError"
                assert "generate" in exc_info.value.error_message

    # -- Recovery -----------------------------------------------------------

    def test_pool_recovery_after_unary_crash(self) -> None:
        """Kill worker, catch error, next borrow gets fresh worker."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
                os.kill(pid1, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    svc.add(a=1.0, b=2.0)

            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
                result = svc.add(a=3.0, b=4.0)
                assert result == 7.0
                assert pid2 != pid1

    def test_pool_recovery_after_stream_crash(self) -> None:
        """Stream crash, recovery, next borrow succeeds."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
                session = svc.generate(count=100)
                os.kill(pid1, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    for _ in session:
                        pass

            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
                batches = list(svc.generate(count=3))
                assert len(batches) == 3
                assert pid2 != pid1

    # -- Truncated response (pipe-based) ------------------------------------

    def test_truncated_unary_response_raises_transport_error(self) -> None:
        """Server closes after one call, next unary call gets TransportError."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(PoolTestService, PoolTestServiceImpl())

        def run_one_then_close() -> None:
            server.serve_one(server_transport)
            server_transport.close()

        thread = threading.Thread(target=run_one_then_close, daemon=True)
        thread.start()

        with RpcConnection(PoolTestService, client_transport) as svc:
            result = svc.add(a=1.0, b=2.0)
            assert result == 3.0
            thread.join(timeout=5)

            with pytest.raises(RpcError, match="TransportError"):
                svc.add(a=3.0, b=4.0)

        server_transport.close()

    def test_truncated_stream_response_raises_transport_error(self) -> None:
        """Server closes mid-stream, client read fails with TransportError."""
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(PoolTestService, PoolTestServiceImpl())

        def run_one_then_close() -> None:
            server.serve_one(server_transport)
            server_transport.close()

        thread = threading.Thread(target=run_one_then_close, daemon=True)
        thread.start()

        with RpcConnection(PoolTestService, client_transport) as svc:
            # Start a producer stream — server serves it
            session = svc.generate(count=2)
            # Consume the stream fully
            batches = list(session)
            assert len(batches) == 2
            thread.join(timeout=5)

            # Server is gone — next stream init should fail
            with pytest.raises(RpcError, match="TransportError"):
                svc.generate(count=1)

        server_transport.close()

    # -- Session close() idempotency after transport error ------------------

    def test_close_idempotent_after_transport_error(self) -> None:
        """Calling close() after a TransportError is a safe no-op."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                session = svc.generate(count=100)
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    session.tick()
                # close() after transport error should not raise
                session.close()
                session.close()  # Double-close also safe


# ---------------------------------------------------------------------------
# Bad worker commands — spawn failures must not corrupt pool state
# ---------------------------------------------------------------------------


class TestBadWorkerCommand:
    """Pool handles bad worker commands without corrupting internal state."""

    def test_nonexistent_command_raises_file_not_found(self) -> None:
        """connect() with a missing executable raises FileNotFoundError."""
        with (
            WorkerPool(max_idle=4) as pool,
            pytest.raises(FileNotFoundError),
            pool.connect(PoolTestService, ["/nonexistent/path/worker"]),
        ):
            pass  # pragma: no cover

    def test_pool_recovery_after_file_not_found(self) -> None:
        """Bad command fails, next connect with good command works."""
        with WorkerPool(max_idle=4) as pool:
            with pytest.raises(FileNotFoundError), pool.connect(PoolTestService, ["/nonexistent/path/worker"]):
                pass  # pragma: no cover

            # Pool state is clean — active_count back to 0
            assert pool.active_count == 0

            # Good command works fine after the failure
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                result = svc.add(a=1.0, b=2.0)
                assert result == 3.0

            assert pool.active_count == 0

    def test_exit_code_in_pool_warning_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Killed worker return logs a WARNING containing the exit code."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with caplog.at_level(logging.WARNING, logger="vgi_rpc.pool"), pool.connect(PoolTestService, cmd) as svc:
                pid = svc.get_pid()
                os.kill(pid, signal.SIGKILL)
                time.sleep(_KILL_SETTLE)
                with pytest.raises(RpcError, match="TransportError"):
                    svc.add(a=1.0, b=2.0)

            # Pool return path should log exit_code for the dead worker
            exit_code_logged = any("exit_code=" in rec.message for rec in caplog.records)
            assert exit_code_logged, f"No exit_code in WARNING logs: {[r.message for r in caplog.records]}"
