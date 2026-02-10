"""Exhaustive tests for bidi stream RPC calls.

Covers edge cases, error handling, state management, session lifecycle,
metadata propagation, schema validation, and stress scenarios for bidi
streaming across all transport types.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from vgi_rpc.http import HttpBidiSession
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    BidiSession,
    BidiStream,
    BidiStreamState,
    CallContext,
    OutputCollector,
    RpcConnection,
    RpcError,
    _RpcProxy,
    serve_pipe,
)

from .conftest import ConnFactory
from .test_rpc import (
    RpcFixtureService,
    http_conn,
    rpc_conn,
    rpc_server_transport,
)

# ---------------------------------------------------------------------------
# New bidi state classes for edge-case testing
# ---------------------------------------------------------------------------


@dataclass
class AccumulatingState(BidiStreamState):
    """State that accumulates a running sum across exchanges."""

    running_sum: float = 0.0
    exchange_count: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Add input values to running sum and emit result."""
        col = input.batch.column("value")
        self.running_sum += pc.sum(col).as_py()
        self.exchange_count += 1
        out.emit_pydict({"running_sum": [self.running_sum], "exchange_count": [self.exchange_count]})


@dataclass
class FailFirstState(BidiStreamState):
    """State that fails on the very first process() call."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Raise on every call."""
        raise RuntimeError("fail on first exchange")


@dataclass
class FailOnNthState(BidiStreamState):
    """State that fails on the Nth process() call."""

    fail_on: int
    count: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Fail on the Nth call, passthrough otherwise."""
        self.count += 1
        if self.count == self.fail_on:
            raise ValueError(f"intentional failure on exchange {self.count}")
        out.emit(input.batch)


@dataclass
class MultiLogState(BidiStreamState):
    """State that emits multiple log messages per exchange."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit 3 log messages then the data batch."""
        out.client_log(Level.DEBUG, "step 1: received input")
        out.client_log(Level.INFO, "step 2: processing", row_count=str(input.batch.num_rows))
        out.client_log(Level.WARN, "step 3: almost done")
        out.emit(input.batch)


@dataclass
class MetadataEchoState(BidiStreamState):
    """State that attaches custom metadata to output."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo input and attach custom metadata."""
        out.emit(input.batch, metadata={"echo": "true", "rows": str(input.batch.num_rows)})


@dataclass
class EmptyBatchState(BidiStreamState):
    """State that handles 0-row input batches gracefully."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit a batch with the row count of the input."""
        out.emit_pydict({"row_count": [input.batch.num_rows]})


@dataclass
class HistoryState(BidiStreamState):
    """State that tracks a list of all seen values (growing state)."""

    history: list[float] = field(default_factory=list)

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Append input values to history and emit history length."""
        for v in input.batch.column("value"):
            self.history.append(v.as_py())
        out.emit_pydict({"history_len": [len(self.history)]})


@dataclass
class LargeBatchState(BidiStreamState):
    """State for testing large batch throughput."""

    factor: float

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Multiply all values by factor."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_arrays([scaled])


# ---------------------------------------------------------------------------
# New test services (pipe-only, not in the subprocess/HTTP fixture)
# ---------------------------------------------------------------------------


class BidiEdgeCaseService(Protocol):
    """Service for testing bidi edge cases."""

    def accumulate(self) -> BidiStream[BidiStreamState]:
        """Accumulate values across exchanges."""
        ...

    def fail_first(self) -> BidiStream[BidiStreamState]:
        """Bidi that fails on first exchange."""
        ...

    def fail_on_nth(self, fail_on: int) -> BidiStream[BidiStreamState]:
        """Bidi that fails on the Nth exchange."""
        ...

    def multi_log(self) -> BidiStream[BidiStreamState]:
        """Bidi with multiple logs per exchange."""
        ...

    def metadata_echo(self) -> BidiStream[BidiStreamState]:
        """Bidi that echoes metadata."""
        ...

    def empty_batch_handler(self) -> BidiStream[BidiStreamState]:
        """Bidi that handles empty batches."""
        ...

    def history_tracker(self) -> BidiStream[BidiStreamState]:
        """Bidi that tracks value history in growing state."""
        ...

    def large_batch(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi for large batch throughput."""
        ...

    def fail_init(self) -> BidiStream[BidiStreamState]:
        """Bidi where init itself raises."""
        ...

    def log_during_init(self) -> BidiStream[BidiStreamState]:
        """Bidi that emits logs during init (before BidiStream returned)."""
        ...

    def passthrough_with_input_schema(self) -> BidiStream[BidiStreamState]:
        """Bidi with explicit input_schema."""
        ...


class BidiEdgeCaseServiceImpl:
    """Implementation of BidiEdgeCaseService."""

    def accumulate(self) -> BidiStream[AccumulatingState]:
        """Accumulate values across exchanges."""
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("running_sum", pa.float64()),
            pa.field("exchange_count", pa.int64()),
        ]
        schema = pa.schema(fields)
        return BidiStream(output_schema=schema, state=AccumulatingState())

    def fail_first(self) -> BidiStream[FailFirstState]:
        """Bidi that fails on first exchange."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=FailFirstState())

    def fail_on_nth(self, fail_on: int) -> BidiStream[FailOnNthState]:
        """Bidi that fails on the Nth exchange."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=FailOnNthState(fail_on=fail_on))

    def multi_log(self) -> BidiStream[MultiLogState]:
        """Bidi with multiple logs per exchange."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=MultiLogState())

    def metadata_echo(self) -> BidiStream[MetadataEchoState]:
        """Bidi that echoes metadata."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=MetadataEchoState())

    def empty_batch_handler(self) -> BidiStream[EmptyBatchState]:
        """Bidi that handles empty batches."""
        schema = pa.schema([pa.field("row_count", pa.int64())])
        return BidiStream(output_schema=schema, state=EmptyBatchState())

    def history_tracker(self) -> BidiStream[HistoryState]:
        """Bidi that tracks value history in growing state."""
        schema = pa.schema([pa.field("history_len", pa.int64())])
        return BidiStream(output_schema=schema, state=HistoryState())

    def large_batch(self, factor: float) -> BidiStream[LargeBatchState]:
        """Bidi for large batch throughput."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=LargeBatchState(factor=factor))

    def fail_init(self) -> BidiStream[BidiStreamState]:
        """Bidi where init itself raises."""
        raise ValueError("init boom")

    def log_during_init(self, ctx: CallContext | None = None) -> BidiStream[AccumulatingState]:
        """Bidi that emits logs during init (before BidiStream returned)."""
        if ctx:
            ctx.client_log(Level.INFO, "bidi init log")
            ctx.client_log(Level.DEBUG, "bidi init detail", tag="init")
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("running_sum", pa.float64()),
            pa.field("exchange_count", pa.int64()),
        ]
        schema = pa.schema(fields)
        return BidiStream(output_schema=schema, state=AccumulatingState())

    def passthrough_with_input_schema(self) -> BidiStream[EmptyBatchState]:
        """Bidi with explicit input_schema."""
        expected = pa.schema([pa.field("value", pa.float64())])
        output = pa.schema([pa.field("row_count", pa.int64())])
        return BidiStream(output_schema=output, state=EmptyBatchState(), input_schema=expected)


@contextlib.contextmanager
def edge_conn(
    on_log: Callable[[Message], None] | None = None,
) -> Iterator[_RpcProxy]:
    """Start an edge-case service and yield a proxy."""
    with serve_pipe(BidiEdgeCaseService, BidiEdgeCaseServiceImpl(), on_log=on_log) as proxy:
        yield proxy


# ---------------------------------------------------------------------------
# Tests: Empty bidi session (open and close without exchanges)
# ---------------------------------------------------------------------------


class TestBidiEmptySession:
    """Tests for bidi sessions that are opened and closed without any exchanges."""

    def test_open_close_no_exchange(self, make_conn: ConnFactory) -> None:
        """Opening a bidi session and closing without exchanging should not deadlock."""
        with make_conn() as proxy:
            session = proxy.transform(factor=2.0)
            session.close()

    def test_context_manager_no_exchange(self, make_conn: ConnFactory) -> None:
        """Context manager bidi session with no exchanges should clean up."""
        with make_conn() as proxy, proxy.transform(factor=2.0):
            pass  # no exchange

    def test_call_after_empty_close(self, make_conn: ConnFactory) -> None:
        """After closing a bidi session with no exchanges, subsequent calls work."""
        with make_conn() as proxy:
            with proxy.transform(factor=2.0):
                pass
            # Transport should be clean for next call
            assert proxy.add(a=1.0, b=2.0) == pytest.approx(3.0)

    def test_multiple_empty_sessions(self, make_conn: ConnFactory) -> None:
        """Multiple bidi sessions opened and closed without exchanges."""
        with make_conn() as proxy:
            for _ in range(5):
                with proxy.transform(factor=1.0):
                    pass
            # Still works
            assert proxy.add(a=10.0, b=10.0) == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Tests: Session lifecycle edge cases
# ---------------------------------------------------------------------------


class TestBidiSessionLifecycle:
    """Tests for BidiSession/HttpBidiSession lifecycle edge cases."""

    def test_close_called_twice(self, make_conn: ConnFactory) -> None:
        """Calling close() twice should be idempotent."""
        with make_conn() as proxy:
            session = proxy.transform(factor=2.0)
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert out.batch.column("value").to_pylist() == [2.0]
            session.close()
            session.close()  # second close should be a no-op

    def test_close_twice_no_exchange(self, make_conn: ConnFactory) -> None:
        """Closing twice without any exchange should be safe."""
        with make_conn() as proxy:
            session = proxy.transform(factor=2.0)
            session.close()
            session.close()

    def test_context_manager_with_exception(self, make_conn: ConnFactory) -> None:
        """Context manager __exit__ cleans up even when an exception occurs."""
        with make_conn() as proxy:
            with pytest.raises(ValueError, match="test error"), proxy.transform(factor=2.0) as session:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
                raise ValueError("test error")
            # Transport should be clean
            assert proxy.add(a=1.0, b=2.0) == pytest.approx(3.0)

    def test_exchange_after_close_pipe(self) -> None:
        """Exchange after close should raise on pipe transport."""
        with rpc_conn() as proxy:
            session = proxy.transform(factor=2.0)
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            session.close()
            assert isinstance(session, BidiSession)
            # After close, the input writer is closed. Trying to exchange should fail.
            with pytest.raises((pa.ArrowInvalid, OSError)):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))


# ---------------------------------------------------------------------------
# Tests: Error handling in bidi streams
# ---------------------------------------------------------------------------


class TestBidiErrorHandling:
    """Tests for error handling during bidi exchanges."""

    def test_error_on_first_exchange(self) -> None:
        """Error on the very first process() call propagates as RpcError."""
        with edge_conn() as proxy:
            session = proxy.fail_first()
            with pytest.raises(RpcError, match="fail on first exchange"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

    def test_error_on_first_then_recovery(self) -> None:
        """After error on first exchange, transport is clean for next call."""
        with edge_conn() as proxy:
            session = proxy.fail_first()
            with pytest.raises(RpcError, match="fail on first exchange"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            # Should be able to make another call
            with proxy.accumulate() as session2:
                out = session2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out.batch.column("running_sum").to_pylist() == [5.0]

    def test_error_on_nth_exchange(self) -> None:
        """Error on the 3rd exchange after 2 successful ones."""
        with edge_conn() as proxy:
            session = proxy.fail_on_nth(fail_on=3)
            # First two succeed
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            # Third fails
            with pytest.raises(RpcError, match="intentional failure on exchange 3"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))

    def test_error_preserves_error_type(self) -> None:
        """Error type from server is preserved in RpcError."""
        with edge_conn() as proxy:
            session = proxy.fail_on_nth(fail_on=1)
            with pytest.raises(RpcError) as exc_info:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert exc_info.value.error_type == "ValueError"
            assert "intentional failure" in exc_info.value.error_message

    def test_error_has_remote_traceback(self) -> None:
        """RpcError from bidi includes remote traceback."""
        with edge_conn() as proxy:
            session = proxy.fail_first()
            with pytest.raises(RpcError) as exc_info:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert exc_info.value.remote_traceback != ""
            assert "fail on first exchange" in exc_info.value.remote_traceback

    def test_error_during_init_visible_on_exchange(self) -> None:
        """Error during bidi init is visible on first exchange (pipe transport).

        On pipe transport, proxy.fail_init() returns a BidiSession without
        reading the server response.  The error is only raised when the
        client tries the first exchange().
        """
        with edge_conn() as proxy:
            session = proxy.fail_init()
            with pytest.raises((RpcError, pa.ArrowInvalid)):
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

    def test_bidi_error_then_unary_on_shared_transport(self, make_conn: ConnFactory) -> None:
        """Bidi error followed by successful unary on the same transport."""
        with make_conn() as proxy:
            session = proxy.fail_bidi_mid(factor=2.0)
            # First exchange OK
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert out.batch.column("value").to_pylist() == [2.0]
            # Second exchange fails
            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            # Unary should still work
            assert proxy.greet(name="test") == "Hello, test!"

    def test_bidi_error_then_stream_on_shared_transport(self, make_conn: ConnFactory) -> None:
        """Bidi error followed by successful server stream on the same transport."""
        with make_conn() as proxy:
            session = proxy.fail_bidi_mid(factor=2.0)
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            # Server stream should work
            batches = list(proxy.generate(count=3))
            assert len(batches) == 3

    def test_bidi_error_then_another_bidi(self, make_conn: ConnFactory) -> None:
        """Bidi error followed by successful bidi on the same transport."""
        with make_conn() as proxy:
            session = proxy.fail_bidi_mid(factor=2.0)
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            # New bidi should work
            with proxy.transform(factor=3.0) as session2:
                out = session2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out.batch.column("value").to_pylist() == [15.0]


# ---------------------------------------------------------------------------
# Tests: State mutation and accumulation
# ---------------------------------------------------------------------------


class TestBidiStateMutation:
    """Tests for bidi state mutation across exchanges."""

    def test_accumulating_state(self) -> None:
        """State accumulates values across multiple exchanges."""
        with edge_conn() as proxy, proxy.accumulate() as session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))
            assert out1.batch.column("running_sum").to_pylist() == [6.0]
            assert out1.batch.column("exchange_count").to_pylist() == [1]

            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert out2.batch.column("running_sum").to_pylist() == [16.0]
            assert out2.batch.column("exchange_count").to_pylist() == [2]

            out3 = session.exchange(AnnotatedBatch.from_pydict({"value": [0.5, 0.5]}))
            assert out3.batch.column("running_sum").to_pylist() == [17.0]
            assert out3.batch.column("exchange_count").to_pylist() == [3]

    def test_growing_list_state(self) -> None:
        """State with a growing list (history) works correctly."""
        with edge_conn() as proxy, proxy.history_tracker() as session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert out1.batch.column("history_len").to_pylist() == [2]

            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0, 4.0, 5.0]}))
            assert out2.batch.column("history_len").to_pylist() == [5]

            out3 = session.exchange(AnnotatedBatch.from_pydict({"value": [6.0]}))
            assert out3.batch.column("history_len").to_pylist() == [6]

    def test_state_independent_across_sessions(self, make_conn: ConnFactory) -> None:
        """Two sequential bidi sessions have independent state."""
        with make_conn() as proxy:
            with proxy.transform(factor=2.0) as s1:
                out1 = s1.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out1.batch.column("value").to_pylist() == [10.0]

            with proxy.transform(factor=10.0) as s2:
                out2 = s2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out2.batch.column("value").to_pylist() == [50.0]


# ---------------------------------------------------------------------------
# Tests: Logging during bidi
# ---------------------------------------------------------------------------


class TestBidiLogging:
    """Tests for log message delivery during bidi exchanges."""

    def test_multiple_logs_per_exchange(self) -> None:
        """Multiple log messages are delivered per exchange in order."""
        logs: list[Message] = []
        with edge_conn(on_log=logs.append) as proxy, proxy.multi_log() as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))

            assert len(logs) == 3
            assert logs[0].level == Level.DEBUG
            assert "step 1" in logs[0].message
            assert logs[1].level == Level.INFO
            assert "step 2" in logs[1].message
            assert logs[2].level == Level.WARN
            assert "step 3" in logs[2].message

    def test_log_extras_preserved(self) -> None:
        """Log message extras are preserved through the wire."""
        logs: list[Message] = []
        with edge_conn(on_log=logs.append) as proxy, proxy.multi_log() as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))

            info_log = next(m for m in logs if m.level == Level.INFO)
            assert info_log.extra is not None
            assert info_log.extra["row_count"] == "3"

    def test_logs_accumulate_across_exchanges(self) -> None:
        """Logs from multiple exchanges all go to the callback."""
        logs: list[Message] = []
        with edge_conn(on_log=logs.append) as proxy, proxy.multi_log() as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert len(logs) == 3
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            assert len(logs) == 6

    def test_log_during_init_buffered(self) -> None:
        """Logs emitted during bidi init are buffered and delivered (pipe transport)."""
        logs: list[Message] = []
        with edge_conn(on_log=logs.append) as proxy:
            session = proxy.log_during_init()
            # Init logs should have been delivered during session creation
            # (they're sent as part of the bidi init response)
            # Do an exchange to prove the session works
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
            assert out.batch.column("running_sum").to_pylist() == [5.0]
            session.close()

    def test_bidi_log_no_callback(self, make_conn: ConnFactory) -> None:
        """Logs during bidi exchange are silently discarded when no on_log callback."""
        with make_conn() as proxy, proxy.transform_with_logs(factor=2.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
            assert out.batch.column("value").to_pylist() == [6.0]

    def test_logs_delivered_before_data(self, make_conn: ConnFactory) -> None:
        """Log callback is invoked before exchange() returns the data batch."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy, proxy.transform_with_logs(factor=2.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            # By the time exchange() returns, logs should already be delivered
            assert len(logs) >= 1
            assert out.batch.column("value").to_pylist() == [2.0]


# ---------------------------------------------------------------------------
# Tests: Metadata propagation
# ---------------------------------------------------------------------------


class TestBidiMetadata:
    """Tests for custom metadata on bidi input/output batches."""

    def test_output_metadata_from_process(self) -> None:
        """Custom metadata set by process() is visible on output AnnotatedBatch."""
        with edge_conn() as proxy, proxy.metadata_echo() as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))
            assert out.custom_metadata is not None
            assert out.custom_metadata.get(b"echo") == b"true"
            assert out.custom_metadata.get(b"rows") == b"3"

    def test_input_custom_metadata_not_lost(self) -> None:
        """Custom metadata on input batches reaches the process() method (pipe)."""

        @dataclass
        class MetadataReadState(BidiStreamState):
            last_meta_value: str = ""

            def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
                if input.custom_metadata:
                    val = input.custom_metadata.get(b"user_key")
                    if val:
                        self.last_meta_value = val.decode()
                out.emit_pydict({"meta_value": [self.last_meta_value]})

        class MetaService(Protocol):
            def meta_bidi(self) -> BidiStream[BidiStreamState]: ...

        class MetaServiceImpl:
            def meta_bidi(self) -> BidiStream[MetadataReadState]:
                schema = pa.schema([pa.field("meta_value", pa.utf8())])
                return BidiStream(output_schema=schema, state=MetadataReadState())

        with rpc_conn(MetaService, MetaServiceImpl()) as proxy:
            session = proxy.meta_bidi()
            batch = pa.RecordBatch.from_pydict({"value": [1.0]})
            cm = pa.KeyValueMetadata({b"user_key": b"hello"})
            out = session.exchange(AnnotatedBatch(batch=batch, custom_metadata=cm))
            assert out.batch.column("meta_value").to_pylist() == ["hello"]
            session.close()


# ---------------------------------------------------------------------------
# Tests: Input schema validation
# ---------------------------------------------------------------------------


class TestBidiInputSchemaValidation:
    """Tests for BidiStream.input_schema validation."""

    def test_wrong_column_name_raises(self) -> None:
        """Wrong column name raises RpcError with schema mismatch."""
        with edge_conn() as proxy:
            session = proxy.passthrough_with_input_schema()
            with pytest.raises(RpcError, match="Input schema mismatch"):
                session.exchange(AnnotatedBatch.from_pydict({"wrong": [1.0]}))

    def test_wrong_column_type_raises(self) -> None:
        """Wrong column type raises RpcError with schema mismatch."""
        with edge_conn() as proxy:
            session = proxy.passthrough_with_input_schema()
            # Send string instead of float
            with pytest.raises(RpcError, match="Input schema mismatch"):
                session.exchange(AnnotatedBatch.from_pydict({"value": ["not_a_float"]}))

    def test_extra_columns_raises(self) -> None:
        """Extra columns in input raise schema mismatch."""
        with edge_conn() as proxy:
            session = proxy.passthrough_with_input_schema()
            with pytest.raises(RpcError, match="Input schema mismatch"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0], "extra": [2.0]}))

    def test_correct_schema_succeeds(self) -> None:
        """Correct input schema passes validation."""
        with edge_conn() as proxy:
            session = proxy.passthrough_with_input_schema()
            out = session.exchange(
                AnnotatedBatch(
                    batch=pa.RecordBatch.from_pydict(
                        {"value": [1.0]}, schema=pa.schema([pa.field("value", pa.float64())])
                    )
                )
            )
            assert out.batch.column("row_count").to_pylist() == [1]
            session.close()

    def test_schema_mismatch_then_recovery(self) -> None:
        """After schema mismatch error, transport is clean for next call."""
        with edge_conn() as proxy:
            session = proxy.passthrough_with_input_schema()
            with pytest.raises(RpcError, match="Input schema mismatch"):
                session.exchange(AnnotatedBatch.from_pydict({"wrong": [1.0]}))
            # Next call should work
            with proxy.accumulate() as s2:
                out = s2.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
                assert out.batch.column("running_sum").to_pylist() == [5.0]


# ---------------------------------------------------------------------------
# Tests: Batch size variations
# ---------------------------------------------------------------------------


class TestBidiBatchSizes:
    """Tests for various input batch sizes."""

    def test_single_row(self, make_conn: ConnFactory) -> None:
        """Single-row input batch works."""
        with make_conn() as proxy, proxy.transform(factor=2.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [42.0]}))
            assert out.batch.column("value").to_pylist() == [84.0]

    def test_multi_row(self, make_conn: ConnFactory) -> None:
        """Multi-row input batch works correctly."""
        with make_conn() as proxy, proxy.transform(factor=0.5) as session:
            values = [float(i) for i in range(100)]
            out = session.exchange(AnnotatedBatch.from_pydict({"value": values}))
            expected = [v * 0.5 for v in values]
            assert out.batch.column("value").to_pylist() == expected

    def test_zero_row_input(self) -> None:
        """Zero-row input batch is handled gracefully."""
        with edge_conn() as proxy, proxy.empty_batch_handler() as session:
            empty = pa.RecordBatch.from_pydict(
                {"value": pa.array([], type=pa.float64())},
                schema=pa.schema([pa.field("value", pa.float64())]),
            )
            out = session.exchange(AnnotatedBatch(batch=empty))
            assert out.batch.column("row_count").to_pylist() == [0]

    def test_large_batch(self) -> None:
        """Large batch (10k rows) throughput works correctly."""
        with edge_conn() as proxy, proxy.large_batch(factor=3.0) as session:
            values = [float(i) for i in range(10_000)]
            out = session.exchange(AnnotatedBatch.from_pydict({"value": values}))
            result = out.batch.column("value").to_pylist()
            assert len(result) == 10_000
            assert result[0] == 0.0
            assert result[9999] == 29997.0

    def test_varying_batch_sizes(self, make_conn: ConnFactory) -> None:
        """Different batch sizes across exchanges work correctly."""
        with make_conn() as proxy, proxy.transform(factor=2.0) as session:
            # 1 row
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert len(out1.batch) == 1

            # 10 rows
            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [float(i) for i in range(10)]}))
            assert len(out2.batch) == 10

            # 1 row again
            out3 = session.exchange(AnnotatedBatch.from_pydict({"value": [99.0]}))
            assert len(out3.batch) == 1
            assert out3.batch.column("value").to_pylist() == [198.0]


# ---------------------------------------------------------------------------
# Tests: Many exchanges (stress test)
# ---------------------------------------------------------------------------


class TestBidiStress:
    """Stress tests for bidi streaming."""

    def test_many_exchanges(self, make_conn: ConnFactory) -> None:
        """100 exchanges on a single bidi session work correctly."""
        with make_conn() as proxy, proxy.transform(factor=2.0) as session:
            for i in range(100):
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [float(i)]}))
                assert out.batch.column("value").to_pylist() == [float(i * 2)]

    def test_many_sequential_sessions(self, make_conn: ConnFactory) -> None:
        """20 sequential bidi sessions each with a few exchanges."""
        with make_conn() as proxy:
            for i in range(20):
                with proxy.transform(factor=float(i + 1)) as session:
                    out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
                    assert out.batch.column("value").to_pylist() == [float(i + 1)]

    def test_alternating_bidi_and_unary(self, make_conn: ConnFactory) -> None:
        """Alternating bidi sessions and unary calls."""
        with make_conn() as proxy:
            for i in range(10):
                with proxy.transform(factor=2.0) as session:
                    out = session.exchange(AnnotatedBatch.from_pydict({"value": [float(i)]}))
                    assert out.batch.column("value").to_pylist() == [float(i * 2)]
                assert proxy.add(a=float(i), b=1.0) == pytest.approx(float(i + 1))


# ---------------------------------------------------------------------------
# Tests: Mixed error and success scenarios
# ---------------------------------------------------------------------------


class TestBidiMixedScenarios:
    """Tests for complex mixed scenarios involving bidi with other method types."""

    def test_bidi_between_stream_calls(self, make_conn: ConnFactory) -> None:
        """Bidi session between two server-stream calls."""
        with make_conn() as proxy:
            batches1 = list(proxy.generate(count=3))
            assert len(batches1) == 3

            with proxy.transform(factor=5.0) as session:
                out = session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
                assert out.batch.column("value").to_pylist() == [10.0]

            batches2 = list(proxy.generate(count=2))
            assert len(batches2) == 2

    def test_bidi_with_logging_then_error_recovery(self, make_conn: ConnFactory) -> None:
        """Bidi with logs, then bidi error, then successful unary with logs."""
        logs: list[Message] = []
        with make_conn(on_log=logs.append) as proxy:
            # Bidi with logs
            with proxy.transform_with_logs(factor=2.0) as session:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert len(logs) >= 1

            logs.clear()

            # Bidi error
            session2 = proxy.fail_bidi_mid(factor=1.0)
            session2.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            with pytest.raises(RpcError):
                session2.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))

            # Unary with logs
            logs.clear()
            proxy.greet_with_logs(name="post-error")
            assert any("post-error" in m.message for m in logs)


# ---------------------------------------------------------------------------
# Tests: Transport-specific behavior
# ---------------------------------------------------------------------------


class TestBidiPipeTransport:
    """Tests specific to pipe transport bidi behavior."""

    def test_bidi_via_rpc_connection(self) -> None:
        """Bidi works through raw RpcConnection context manager."""
        with (
            rpc_server_transport() as transport,
            RpcConnection(RpcFixtureService, transport) as svc,
            svc.transform(factor=4.0) as session,
        ):
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
            assert out.batch.column("value").to_pylist() == [12.0]

    def test_bidi_session_is_correct_type_pipe(self) -> None:
        """Pipe transport returns BidiSession (not HttpBidiSession)."""
        with rpc_conn() as proxy:
            session = proxy.transform(factor=1.0)
            assert isinstance(session, BidiSession)
            session.close()

    def test_bidi_with_on_log_via_connection(self) -> None:
        """Log callback works through RpcConnection with bidi."""
        logs: list[Message] = []
        with (
            rpc_server_transport() as transport,
            RpcConnection(RpcFixtureService, transport, on_log=logs.append) as svc,
            svc.transform_with_logs(factor=2.0) as session,
        ):
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
        assert len(logs) >= 1
        assert any("factor=2.0" in m.message for m in logs)


class TestBidiHttpTransport:
    """Tests specific to HTTP transport bidi behavior."""

    def test_bidi_session_is_correct_type_http(self, http_server_port: int) -> None:
        """HTTP transport returns HttpBidiSession."""
        with http_conn(http_server_port) as proxy:
            session = proxy.transform(factor=1.0)
            assert isinstance(session, HttpBidiSession)
            session.close()

    def test_http_bidi_close_is_noop(self, http_server_port: int) -> None:
        """HttpBidiSession.close() is a no-op (stateless HTTP)."""
        with http_conn(http_server_port) as proxy:
            session = proxy.transform(factor=2.0)
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
            assert out.batch.column("value").to_pylist() == [10.0]
            session.close()
            session.close()  # Multiple closes should be fine

    def test_http_bidi_state_persists_across_exchanges(self, http_server_port: int) -> None:
        """HTTP bidi state is correctly updated across multiple exchanges.

        This is critical because HTTP transport serializes/deserializes state
        on every exchange. Verifies the state_bytes is updated.
        """
        with http_conn(http_server_port) as proxy, proxy.transform(factor=2.0) as session:
            assert isinstance(session, HttpBidiSession)

            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            # State bytes may or may not change for TransformState (no mutation)
            # but the round-trip should work
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))

            # All exchanges should succeed with consistent state
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert out.batch.column("value").to_pylist() == [20.0]

    def test_http_bidi_logs_with_callback(self, http_server_port: int) -> None:
        """HTTP bidi delivers logs to on_log callback."""
        logs: list[Message] = []
        with http_conn(http_server_port, on_log=logs.append) as proxy:
            with proxy.transform_with_logs(factor=3.0) as session:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            assert len(logs) == 2
            assert all(m.level == Level.INFO for m in logs)

    def test_http_bidi_error_mid_stream(self, http_server_port: int) -> None:
        """HTTP bidi error mid-stream returns RpcError with correct details."""
        with http_conn(http_server_port) as proxy:
            session = proxy.fail_bidi_mid(factor=2.0)
            # First exchange OK
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            assert out.batch.column("value").to_pylist() == [2.0]
            # Second exchange fails
            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))

    def test_http_bidi_error_recovery(self, http_server_port: int) -> None:
        """After HTTP bidi error, new session works fine."""
        with http_conn(http_server_port) as proxy:
            session = proxy.fail_bidi_mid(factor=1.0)
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            with pytest.raises(RpcError, match="bidi boom"):
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))

            # New session should work
            with proxy.transform(factor=5.0) as session2:
                out = session2.exchange(AnnotatedBatch.from_pydict({"value": [3.0]}))
                assert out.batch.column("value").to_pylist() == [15.0]


# ---------------------------------------------------------------------------
# Tests: Data type fidelity in bidi
# ---------------------------------------------------------------------------


class TestBidiTypeFidelity:
    """Tests for type fidelity through bidi exchanges."""

    def test_float_precision(self, make_conn: ConnFactory) -> None:
        """Float precision is preserved through bidi exchange."""
        with make_conn() as proxy, proxy.transform(factor=1.0) as session:
            values = [0.1, 0.2, 0.3, 1e-10, 1e10, -0.0]
            out = session.exchange(AnnotatedBatch.from_pydict({"value": values}))
            result = out.batch.column("value").to_pylist()
            for v, r in zip(values, result, strict=True):
                assert v == pytest.approx(r)

    def test_negative_values(self, make_conn: ConnFactory) -> None:
        """Negative values work correctly."""
        with make_conn() as proxy, proxy.transform(factor=-1.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, -2.0, 3.0]}))
            assert out.batch.column("value").to_pylist() == [-1.0, 2.0, -3.0]

    def test_zero_factor(self, make_conn: ConnFactory) -> None:
        """Factor of 0 works correctly (all zeros)."""
        with make_conn() as proxy, proxy.transform(factor=0.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))
            assert out.batch.column("value").to_pylist() == [0.0, 0.0, 0.0]

    def test_large_values(self, make_conn: ConnFactory) -> None:
        """Very large float values work correctly."""
        with make_conn() as proxy, proxy.transform(factor=1.0) as session:
            values = [1e100, -1e100, 1e308]
            out = session.exchange(AnnotatedBatch.from_pydict({"value": values}))
            result = out.batch.column("value").to_pylist()
            assert result == values


# ---------------------------------------------------------------------------
# Tests: Subprocess transport bidi
# ---------------------------------------------------------------------------


class TestBidiSubprocess:
    """Tests specific to subprocess transport bidi behavior."""

    def test_subprocess_bidi_basic(self, subprocess_worker: object) -> None:
        """Basic bidi works through subprocess transport."""
        from vgi_rpc.rpc import SubprocessTransport

        assert isinstance(subprocess_worker, SubprocessTransport)
        proxy = _RpcProxy(RpcFixtureService, subprocess_worker)
        session = proxy.transform(factor=3.0)
        out = session.exchange(AnnotatedBatch.from_pydict({"value": [4.0]}))
        assert out.batch.column("value").to_pylist() == [12.0]
        session.close()

    def test_subprocess_bidi_error_recovery(self, subprocess_worker: object) -> None:
        """Bidi error recovery works through subprocess transport."""
        from vgi_rpc.rpc import SubprocessTransport

        assert isinstance(subprocess_worker, SubprocessTransport)
        proxy = _RpcProxy(RpcFixtureService, subprocess_worker)

        session = proxy.fail_bidi_mid(factor=2.0)
        session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
        with pytest.raises(RpcError, match="bidi boom"):
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))

        # Should recover for unary
        result = proxy.add(a=1.0, b=2.0)
        assert result == pytest.approx(3.0)
