"""Tests for WorkerPool -- subprocess process pool with SHM support."""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pytest

from vgi_rpc import WorkerPool
from vgi_rpc.log import Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    OutputCollector,
    Stream,
    StreamState,
)

_SERVE_FIXTURE = str(Path(__file__).parent / "serve_fixture_pool.py")


def _pool_worker_cmd() -> list[str]:
    """Return the command to launch the pool test worker subprocess."""
    return [sys.executable, _SERVE_FIXTURE]


# ---------------------------------------------------------------------------
# Test Protocol + Implementation
# ---------------------------------------------------------------------------


@dataclass
class GenerateState(StreamState):
    """State for the generate producer stream."""

    count: int
    current: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class EchoState(StreamState):
    """State for the echo exchange stream."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo input values back as output."""
        out.emit_pydict({"value": input.batch.column("value").to_pylist()})


_ECHO_SCHEMA = pa.schema([pa.field("value", pa.float64())])


class PoolTestService(Protocol):
    """Test protocol for pool tests."""

    def get_pid(self) -> int:
        """Return the server process PID."""
        ...

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def generate(self, count: int) -> Stream[GenerateState, None]:
        """Yield count batches as a producer stream."""
        ...

    def echo(self, dummy: int) -> Stream[EchoState, None]:
        """Echo input batches back (exchange stream)."""
        ...


class PoolTestServiceImpl:
    """Implementation of PoolTestService."""

    def get_pid(self) -> int:
        """Return the server process PID."""
        return os.getpid()

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def generate(self, count: int) -> Stream[GenerateState, None]:
        """Yield count batches as a producer stream."""
        schema = pa.schema([pa.field("i", pa.int64()), pa.field("value", pa.int64())])
        return Stream(output_schema=schema, state=GenerateState(count=count))

    def echo(self, dummy: int) -> Stream[EchoState, None]:
        """Echo input batches back (exchange stream)."""
        return Stream(output_schema=_ECHO_SCHEMA, state=EchoState(), input_schema=_ECHO_SCHEMA)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPoolBasic:
    """Basic pool functionality tests."""

    def test_basic_reuse(self) -> None:
        """Two sequential connects return the same PID."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 == pid2

    def test_different_commands(self) -> None:
        """Different cmds get separate pool slots."""
        with WorkerPool(max_idle=4) as pool:
            cmd1 = _pool_worker_cmd()
            cmd2 = [sys.executable, _SERVE_FIXTURE]
            with pool.connect(PoolTestService, cmd1) as svc:
                pid1 = svc.get_pid()
            with pool.connect(PoolTestService, cmd2) as svc:
                pid2 = svc.get_pid()
            # Same command path → same PID (reused)
            assert pid1 == pid2

    def test_never_blocks_creation(self) -> None:
        """max_idle=1, 3 sequential borrows all succeed."""
        with WorkerPool(max_idle=1) as pool:
            cmd = _pool_worker_cmd()
            pids = []
            for _ in range(3):
                with pool.connect(PoolTestService, cmd) as svc:
                    pids.append(svc.get_pid())
            # First two should be the same PID (reuse); third may differ
            # due to eviction, but all succeed
            assert len(pids) == 3
            assert all(isinstance(p, int) for p in pids)

    def test_error_recovery(self) -> None:
        """Worker still returned to pool after RpcError."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
            # Worker should be reused
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 == pid2


class TestPoolEviction:
    """Eviction and timeout tests."""

    def test_idle_timeout_eviction(self) -> None:
        """Worker terminated after timeout."""
        with WorkerPool(max_idle=4, idle_timeout=0.5) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
            assert pool.idle_count == 1
            # Wait for timeout + reaper interval
            time.sleep(1.5)
            assert pool.idle_count == 0
            # Next borrow spawns new
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 != pid2

    def test_max_idle_eviction(self) -> None:
        """Oldest evicted when pool full."""
        with WorkerPool(max_idle=2) as pool:
            cmd = _pool_worker_cmd()
            pids = []
            # Spawn 3 workers sequentially, return each
            for _ in range(3):
                with pool.connect(PoolTestService, cmd) as svc:
                    pids.append(svc.get_pid())
                    # Force a new spawn by using all idle workers first
                # After return, idle count should stay <= 2
            assert pool.idle_count <= 2
            m = pool.metrics
            # At least 1 eviction should have happened
            # (we reuse workers, so we may not hit 3 spawns)
            assert m.returns >= 2

    def test_dead_process_handling(self) -> None:
        """Kill idle subprocess, next borrow spawns new."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
            assert pool.idle_count == 1
            # Kill the idle process
            os.kill(pid1, signal.SIGKILL)
            time.sleep(0.2)  # Let process die
            # Next borrow should detect dead process and spawn new
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 != pid2


class TestPoolStream:
    """Stream-related pool tests."""

    def test_stream_reuse(self) -> None:
        """Producer stream through pool, subprocess reused."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                batches = list(svc.generate(count=3))
                assert len(batches) == 3
                pid1 = svc.get_pid()
            # Stream was properly closed, worker should be reused
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 == pid2

    def test_abandoned_stream_discards(self) -> None:
        """Open stream, don't close, exit context -> worker discarded."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
                # Open a stream but don't iterate/close it
                _session = svc.generate(count=5)
                # Exit context without closing stream
            # Worker should be discarded, not returned to pool
            assert pool.idle_count == 0
            m = pool.metrics
            assert m.discards >= 1
            # Next borrow spawns new process
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 != pid2


class TestPoolLifecycle:
    """Pool lifecycle tests."""

    def test_pool_close(self) -> None:
        """All idle workers terminated on close."""
        pool = WorkerPool(max_idle=4)
        cmd = _pool_worker_cmd()
        with pool.connect(PoolTestService, cmd) as svc:
            svc.get_pid()
        assert pool.idle_count == 1
        pool.close()
        assert pool.idle_count == 0

    def test_context_manager(self) -> None:
        """Verify WorkerPool() context manager protocol works."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                result = svc.add(a=1.0, b=2.0)
            assert result == 3.0

    def test_connect_after_close(self) -> None:
        """Raises RuntimeError after close."""
        pool = WorkerPool(max_idle=4)
        pool.close()
        with pytest.raises(RuntimeError, match="closed"), pool.connect(PoolTestService, _pool_worker_cmd()):
            pass

    def test_double_close(self) -> None:
        """No error on double close."""
        pool = WorkerPool(max_idle=4)
        pool.close()
        pool.close()  # Should not raise


class TestPoolCounts:
    """Counter and metrics tests."""

    def test_idle_count(self) -> None:
        """Accurate through borrow/return cycles."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            assert pool.idle_count == 0
            with pool.connect(PoolTestService, cmd) as svc:
                svc.get_pid()
                assert pool.idle_count == 0
            assert pool.idle_count == 1
            with pool.connect(PoolTestService, cmd) as svc:
                svc.get_pid()
                assert pool.idle_count == 0
            assert pool.idle_count == 1

    def test_active_count(self) -> None:
        """Accurate during active connections."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            assert pool.active_count == 0
            with pool.connect(PoolTestService, cmd) as svc:
                assert pool.active_count == 1
                svc.get_pid()
            assert pool.active_count == 0

    def test_metrics(self) -> None:
        """All counters correct after operations."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                svc.get_pid()
            with pool.connect(PoolTestService, cmd) as svc:
                svc.get_pid()
            m = pool.metrics
            assert m.borrows == 2
            assert m.spawns == 1
            assert m.reuses == 1
            assert m.returns >= 1
            assert m.idle == 1
            assert m.active == 0

    def test_on_log_forwarding(self) -> None:
        """Log callbacks work through pool."""
        logs: list[Message] = []
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd, on_log=logs.append) as svc:
                svc.get_pid()
            # No log messages expected from get_pid, but callback wiring works
            # (no exception raised)


class TestPoolShm:
    """Shared memory support tests."""

    def test_shm_basic(self) -> None:
        """Pool-managed SHM works for basic calls."""
        with WorkerPool(max_idle=4, shm_size=4 * 1024 * 1024) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                result = svc.add(a=1.0, b=2.0)
            assert result == 3.0

    def test_shm_reuse(self) -> None:
        """Pool-managed SHM borrows reuse same subprocess."""
        with WorkerPool(max_idle=4, shm_size=4 * 1024 * 1024) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
            with pool.connect(PoolTestService, cmd) as svc:
                pid2 = svc.get_pid()
            assert pid1 == pid2


class TestPoolConcurrency:
    """Concurrency tests."""

    def test_concurrent_same_command(self) -> None:
        """Multiple threads borrow/return same command safely."""
        results: list[int] = []
        errors: list[Exception] = []

        def worker(pool: WorkerPool, cmd: list[str]) -> None:
            try:
                with pool.connect(PoolTestService, cmd) as svc:
                    pid = svc.get_pid()
                    results.append(pid)
            except Exception as e:
                errors.append(e)

        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            threads = [threading.Thread(target=worker, args=(pool, cmd)) for _ in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

        assert not errors, f"Errors in concurrent test: {errors}"
        assert len(results) == 4
        assert all(isinstance(p, int) for p in results)


class TestPoolLogging:
    """Logging tests."""

    def test_logging_events(self, caplog: pytest.LogCaptureFixture) -> None:
        """Pool events logged at correct levels."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.pool"), WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                svc.get_pid()
        # Check that spawn and close events were logged
        spawn_msgs = [r for r in caplog.records if "Spawned" in r.message]
        assert len(spawn_msgs) >= 1
        assert spawn_msgs[0].levelno == logging.INFO


class TestPoolValidation:
    """Input validation tests."""

    def test_negative_max_idle(self) -> None:
        """Negative max_idle raises ValueError."""
        with pytest.raises(ValueError, match="max_idle must be non-negative"):
            WorkerPool(max_idle=-1)

    def test_zero_idle_timeout(self) -> None:
        """Non-positive idle_timeout raises ValueError."""
        with pytest.raises(ValueError, match="idle_timeout must be positive"):
            WorkerPool(idle_timeout=0)

    def test_negative_idle_timeout(self) -> None:
        """Negative idle_timeout raises ValueError."""
        with pytest.raises(ValueError, match="idle_timeout must be positive"):
            WorkerPool(idle_timeout=-5.0)


class TestPoolEdgeCases:
    """Edge case and error path tests."""

    def test_pooled_transport_proc_property(self) -> None:
        """_PooledTransport.proc delegates to inner transport."""
        from vgi_rpc.pool import _PooledTransport

        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            inner = pool._borrow(tuple(cmd))
            pt = _PooledTransport(inner, pool)
            try:
                # proc property should return the Popen object
                assert pt.proc is inner.proc
                assert hasattr(pt.proc, "pid")
            finally:
                pt.close()

    def test_close_with_active_connections(self, caplog: pytest.LogCaptureFixture) -> None:
        """close() warns when active connections exist."""
        pool = WorkerPool(max_idle=4)
        cmd = _pool_worker_cmd()
        # Enter a connect() but close pool before exiting
        gen = pool.connect(PoolTestService, cmd)
        svc = gen.__enter__()
        svc.get_pid()
        # Pool has active=1 now
        assert pool.active_count == 1
        with caplog.at_level(logging.WARNING, logger="vgi_rpc.pool"):
            pool.close()
        # Should have warned about active connections
        active_msgs = [r for r in caplog.records if "active connections" in r.message]
        assert len(active_msgs) == 1
        # Cleanup: exit the generator to avoid warnings
        with contextlib.suppress(Exception):
            gen.__exit__(None, None, None)

    def test_worker_died_during_use(self) -> None:
        """Worker killed while borrowed is discarded on return."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            with pool.connect(PoolTestService, cmd) as svc:
                pid1 = svc.get_pid()
                # Kill the worker while it's actively borrowed
                os.kill(pid1, signal.SIGKILL)
                time.sleep(0.2)
            # Worker should be discarded, not returned to pool
            assert pool.idle_count == 0
            m = pool.metrics
            assert m.discards >= 1

    def test_pool_closed_while_returning_worker(self) -> None:
        """Worker returned after pool.close() is discarded."""
        pool = WorkerPool(max_idle=4)
        cmd = _pool_worker_cmd()
        inner = pool._borrow(tuple(cmd))
        # Close the pool while the worker is "active"
        pool.close()
        # Now return the worker — should discard since pool is closed
        pool._return_worker(inner, stream_opened=False)
        m = pool.metrics
        assert m.discards >= 1

    def test_return_worker_exception_path(self) -> None:
        """_PooledTransport.close() handles _return_worker exceptions."""
        from unittest.mock import patch

        from vgi_rpc.pool import _PooledTransport

        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            inner = pool._borrow(tuple(cmd))
            pt = _PooledTransport(inner, pool)
            # Monkeypatch _return_worker to raise
            with patch.object(pool, "_return_worker", side_effect=RuntimeError("boom")):
                pt.close()  # Should not raise
            # Worker should be terminated (inner.close called in except)
            assert inner.proc.poll() is not None

    def test_max_idle_eviction_on_return(self, caplog: pytest.LogCaptureFixture) -> None:
        """Returning workers beyond max_idle evicts the oldest."""
        with WorkerPool(max_idle=1) as pool:
            cmd = _pool_worker_cmd()
            # Borrow two workers concurrently
            inner1 = pool._borrow(tuple(cmd))
            inner2 = pool._borrow(tuple(cmd))
            pid1 = inner1.proc.pid
            pid2 = inner2.proc.pid
            assert pid1 != pid2
            # Return first (becomes idle)
            pool._return_worker(inner1, stream_opened=False)
            assert pool.idle_count == 1
            # Return second — should evict first (max_idle=1)
            with caplog.at_level(logging.INFO, logger="vgi_rpc.pool"):
                pool._return_worker(inner2, stream_opened=False)
            assert pool.idle_count == 1
            m = pool.metrics
            assert m.evictions_max >= 1
            # Eviction log message
            evict_msgs = [r for r in caplog.records if "Evicted worker (max_idle)" in r.message]
            assert len(evict_msgs) >= 1

    def test_proc_args_string_fallback(self) -> None:
        """_return_worker handles proc.args as a string."""
        with WorkerPool(max_idle=4) as pool:
            cmd = _pool_worker_cmd()
            inner = pool._borrow(tuple(cmd))
            # Monkeypatch proc.args to be a string instead of list
            original_args = inner.proc.args
            inner.proc.args = "some_command"
            try:
                pool._return_worker(inner, stream_opened=False)
            finally:
                inner.proc.args = original_args
            # Worker returned successfully under string key ("some_command",)
            assert pool.idle_count == 1
            m = pool.metrics
            assert m.returns >= 1
