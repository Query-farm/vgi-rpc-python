# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for client-initiated stream cancellation via ``cancel()``."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa
import pytest

from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    OutputCollector,
    ProducerState,
    RpcError,
    Stream,
)

from .conftest import ConnFactory
from .test_rpc import (
    RpcFixtureService,
    _CancelProbe,
    rpc_conn,
    rpc_server_transport,
)


@dataclass
class _OnCancelBoomState(ProducerState):
    """Producer whose on_cancel() raises; used to verify server isolation."""

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit a single row."""
        out.emit_pydict({"i": [0]})

    def on_cancel(self, ctx: CallContext) -> None:
        """Raise to verify the server catches on_cancel exceptions."""
        raise RuntimeError("on_cancel boom")


class _BoomSvc(Protocol):
    """Protocol exercising on_cancel exception handling."""

    def run(self) -> Stream[ProducerState]:
        """Run the producer whose on_cancel raises."""
        ...

    def ping(self) -> int:
        """Unary liveness probe."""
        ...


class _BoomImpl:
    """Implementation paired with :class:`_BoomSvc`."""

    def run(self) -> Stream[_OnCancelBoomState]:
        """Build a stream that emits one row then waits."""
        schema = pa.schema([pa.field("i", pa.int64())])
        return Stream(output_schema=schema, state=_OnCancelBoomState())

    def ping(self) -> int:
        """Return a constant to prove the transport is still alive."""
        return 42


def _consume(stream: object, n: int) -> list[AnnotatedBatch]:
    """Consume the first *n* batches from *stream* without exhausting it."""
    it = iter(stream)  # type: ignore[call-overload]
    return [next(it) for _ in range(n)]


class TestCancelCrossTransport:
    """Cancel semantics verified over every supported transport."""

    def test_cancel_exchange_closes_session(self, make_conn: ConnFactory) -> None:
        """After cancel(), subsequent exchange() raises."""
        with make_conn() as proxy:
            session = proxy.cancellable_exchange()
            session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1]})))
            session.cancel()
            with pytest.raises(RpcError):
                session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [2]})))

    def test_cancel_before_any_exchange(self, make_conn: ConnFactory) -> None:
        """Cancel before sending any input succeeds and leaves the transport reusable."""
        with make_conn() as proxy:
            session = proxy.cancellable_exchange()
            session.cancel()
            # Reuse the same proxy for a follow-up call: transport is clean.
            assert proxy.add(a=1.0, b=2.0) == pytest.approx(3.0)

    def test_cancel_idempotent(self, make_conn: ConnFactory) -> None:
        """cancel()+cancel(), close()+cancel(), and cancel()+close() are all safe."""
        with make_conn() as proxy:
            s1 = proxy.cancellable_exchange()
            s1.cancel()
            s1.cancel()  # no-op

            s2 = proxy.cancellable_exchange()
            s2.close()
            s2.cancel()  # no-op

            s3 = proxy.cancellable_exchange()
            s3.cancel()
            s3.close()  # no-op

    def test_transport_reusable_after_cancel(self, make_conn: ConnFactory) -> None:
        """Cancelling a producer mid-stream leaves the transport usable for the next call."""
        with make_conn() as proxy:
            stream = proxy.cancellable_producer()
            _consume(stream, 2)
            stream.cancel()
            assert proxy.add(a=5.0, b=7.0) == pytest.approx(12.0)


class TestCancelInProcessPipe:
    """Server-side observations that require in-process server state visibility."""

    def test_on_cancel_hook_fires_producer(self) -> None:
        """The server's on_cancel() runs when the client cancels a producer mid-stream."""
        _CancelProbe.reset()
        with rpc_conn() as proxy:
            stream = proxy.cancellable_producer()
            _consume(stream, 3)
            stream.cancel()
        assert _CancelProbe.on_cancel_calls == 1

    def test_on_cancel_hook_fires_exchange(self) -> None:
        """The server's on_cancel() runs when the client cancels an exchange stream."""
        _CancelProbe.reset()
        with rpc_conn() as proxy:
            session = proxy.cancellable_exchange()
            session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1]})))
            session.cancel()
        assert _CancelProbe.on_cancel_calls == 1

    def test_produce_stops_after_cancel(self) -> None:
        """produce() is not invoked after the server observes cancel."""
        _CancelProbe.reset()
        with rpc_conn() as proxy:
            stream = proxy.cancellable_producer()
            _consume(stream, 2)
            # Consumed 2 → produce_calls is at least 2, maybe 3 (one already buffered).
            calls_at_cancel = _CancelProbe.produce_calls
            stream.cancel()
        # No further produce() invocations fired after cancel.
        assert _CancelProbe.produce_calls == calls_at_cancel

    def test_exchange_after_cancel_raises(self) -> None:
        """Using an exchange session after cancel raises RpcError."""
        with rpc_conn() as proxy:
            session = proxy.cancellable_exchange()
            session.cancel()
            with pytest.raises(RpcError):
                session.exchange(AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [9]})))

    def test_access_log_emits_cancelled_field(self, caplog: pytest.LogCaptureFixture) -> None:
        """The access log record for a cancelled stream carries cancelled=True."""
        with (
            caplog.at_level(logging.INFO, logger="vgi_rpc.access"),
            rpc_server_transport() as transport,
        ):
            from vgi_rpc.rpc import RpcConnection

            with RpcConnection(RpcFixtureService, transport) as svc:
                stream = svc.cancellable_producer()
                _consume(stream, 1)
                stream.cancel()

        cancel_records = [r for r in caplog.records if getattr(r, "cancelled", False)]
        assert cancel_records, "expected at least one access log record with cancelled=True"
        assert cancel_records[0].method == "cancellable_producer"  # type: ignore[attr-defined]


class TestCancelHookFailureIsolated:
    """Exceptions from on_cancel() must not crash the server."""

    def test_on_cancel_exception_does_not_break_transport(self) -> None:
        """If on_cancel() raises, the server logs and continues serving."""
        with rpc_conn(_BoomSvc, _BoomImpl()) as proxy:
            stream = proxy.run()
            _consume(stream, 1)
            stream.cancel()
            assert proxy.ping() == 42
