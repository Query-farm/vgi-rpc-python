# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for wire protocol debug logging infrastructure."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa
import pytest

from vgi_rpc.rpc import (
    CallContext,
    OutputCollector,
    ProducerState,
    RpcError,
    Stream,
    StreamState,
    serve_pipe,
)
from vgi_rpc.rpc._debug import (
    fmt_batch,
    fmt_kwargs,
    fmt_metadata,
    fmt_schema,
)

# ---------------------------------------------------------------------------
# Unit tests for formatting helpers
# ---------------------------------------------------------------------------


class TestFmtSchema:
    """Tests for fmt_schema."""

    def test_empty_schema(self) -> None:
        """Empty schema returns '(empty)'."""
        fields: list[pa.Field[Any]] = []
        assert fmt_schema(pa.schema(fields)) == "(empty)"

    def test_single_field(self) -> None:
        """Single field includes Arrow type name."""
        result = fmt_schema(pa.schema([pa.field("x", pa.float64())]))
        assert "x: double" in result

    def test_multiple_fields(self) -> None:
        """Multiple fields are comma-separated."""
        fields: list[pa.Field[Any]] = [pa.field("a", pa.float64()), pa.field("b", pa.int32()), pa.field("c", pa.utf8())]
        schema = pa.schema(fields)
        result = fmt_schema(schema)
        assert "a: double" in result
        assert "b: int32" in result
        assert "c: string" in result

    def test_parenthesised(self) -> None:
        """Result is wrapped in parentheses."""
        result = fmt_schema(pa.schema([pa.field("x", pa.int64())]))
        assert result.startswith("(")
        assert result.endswith(")")


class TestFmtMetadata:
    """Tests for fmt_metadata."""

    def test_none(self) -> None:
        """None metadata returns 'None'."""
        assert fmt_metadata(None) == "None"

    def test_empty(self) -> None:
        """Empty metadata returns '{}'."""
        assert fmt_metadata(pa.KeyValueMetadata({})) == "{}"

    def test_bytes_keys(self) -> None:
        """Bytes keys/values are decoded to strings."""
        md = pa.KeyValueMetadata({b"vgi_rpc.method": b"add", b"vgi_rpc.request_version": b"1"})
        result = fmt_metadata(md)
        assert "vgi_rpc.method='add'" in result
        assert "vgi_rpc.request_version='1'" in result

    def test_long_value_truncated(self) -> None:
        """Long values are truncated with ellipsis."""
        md = pa.KeyValueMetadata({b"key": b"x" * 200})
        result = fmt_metadata(md)
        assert "..." in result
        assert len(result) < 200


class TestFmtBatch:
    """Tests for fmt_batch."""

    def test_empty_batch(self) -> None:
        """Empty batch shows rows=0, cols=0, (empty)."""
        batch = pa.RecordBatch.from_pydict({}, schema=pa.schema([]))
        result = fmt_batch(batch)
        assert "rows=0" in result
        assert "cols=0" in result
        assert "(empty)" in result

    def test_data_batch(self) -> None:
        """Data batch includes row/column counts and schema."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_arrays([pa.array([1.0]), pa.array([2.0])], schema=schema)
        result = fmt_batch(batch)
        assert "rows=1" in result
        assert "cols=2" in result
        assert "a: double" in result
        assert "bytes=" in result


class TestFmtKwargs:
    """Tests for fmt_kwargs."""

    def test_empty(self) -> None:
        """Empty kwargs returns empty string."""
        assert fmt_kwargs({}) == ""

    def test_simple(self) -> None:
        """Simple kwargs are formatted as key=value pairs."""
        result = fmt_kwargs({"a": 1.0, "b": 2.0})
        assert result == "a=1.0, b=2.0"

    def test_long_value_truncated(self) -> None:
        """Long repr values are truncated with ellipsis."""
        result = fmt_kwargs({"data": "x" * 200})
        assert "..." in result


# ---------------------------------------------------------------------------
# Integration test: debug logging emits records during unary + stream calls
# ---------------------------------------------------------------------------


@dataclass
class _CountState(ProducerState):
    """State for the count producer stream."""

    current: int = 0
    limit: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce one batch per tick, finish when limit reached."""
        if self.current >= self.limit:
            out.finish()
            return
        out.emit_pydict({"value": [self.current]})
        self.current += 1


class _DebugProtocol(Protocol):
    """Test protocol for debug logging integration test."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def count(self, n: int) -> Stream[StreamState]:
        """Count from 0 to n-1 as a producer stream."""
        ...


_COUNT_SCHEMA = pa.schema([pa.field("value", pa.int64())])


class _DebugImpl:
    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def count(self, n: int) -> Stream[_CountState]:
        """Return a producer stream counting from 0 to n-1."""
        return Stream(output_schema=_COUNT_SCHEMA, state=_CountState(current=0, limit=n))


def _wire_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return all log records under the vgi_rpc.wire.* hierarchy."""
    return [r for r in caplog.records if r.name.startswith("vgi_rpc.wire")]


def _messages_for(records: list[logging.LogRecord], logger_name: str) -> list[str]:
    """Return formatted messages for a specific logger."""
    return [r.message for r in records if r.name == logger_name]


class TestDebugLoggingUnary:
    """Integration tests for debug logging during unary RPC calls."""

    def test_request_logger_fires(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.request logs method name and kwargs for unary call."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            result = svc.add(a=1.0, b=2.0)

        assert result == 3.0
        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.request")
        assert any("method=add" in m for m in msgs), f"Expected method=add: {msgs}"
        # _send_request logs defaults_applied
        assert any("Send request" in m and "method=add" in m for m in msgs), f"Expected Send request log: {msgs}"
        # _read_request (server-side) logs parsed kwargs
        assert any("Parsed request" in m and "a=" in m for m in msgs), f"Expected parsed kwargs: {msgs}"

    def test_response_logger_fires(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.response logs result batch and return type for unary call."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            svc.add(a=1.0, b=2.0)

        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.response")
        assert any("Write result batch" in m for m in msgs), f"Expected Write result batch: {msgs}"
        assert any("Read unary response" in m and "method=add" in m for m in msgs), (
            f"Expected Read unary response: {msgs}"
        )

    def test_batch_logger_fires(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.batch logs batch classification during unary call."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            svc.add(a=1.0, b=2.0)

        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.batch")
        assert any("Classify batch" in m for m in msgs), f"Expected Classify batch: {msgs}"

    def test_transport_logger_fires(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.transport logs connection open/close for pipe transport."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            svc.add(a=1.0, b=2.0)

        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.transport")
        assert any("make_pipe_pair" in m for m in msgs), f"Expected make_pipe_pair: {msgs}"
        assert any("RpcConnection open" in m for m in msgs), f"Expected RpcConnection open: {msgs}"
        assert any("RpcConnection close" in m for m in msgs), f"Expected RpcConnection close: {msgs}"


class TestDebugLoggingStream:
    """Integration tests for debug logging during stream RPC calls."""

    def test_stream_lifecycle(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.stream logs init, tick, and close for producer stream."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            batches = list(svc.count(n=2))

        assert len(batches) == 2
        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.stream")
        assert any("Stream init" in m and "method=count" in m for m in msgs), f"Expected Stream init: {msgs}"
        assert any("Stream tick" in m for m in msgs), f"Expected Stream tick: {msgs}"
        assert any("Stream close" in m for m in msgs), f"Expected Stream close: {msgs}"

    def test_stream_batch_logger(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.batch logs batch classification for stream output batches."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            list(svc.count(n=1))

        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.batch")
        assert any("Classify batch" in m for m in msgs), f"Expected Classify batch: {msgs}"

    def test_stream_response_logger(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.response logs flushed batches for stream output."""
        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), serve_pipe(_DebugProtocol, _DebugImpl()) as svc:
            list(svc.count(n=1))

        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.response")
        assert any("Flush collector" in m for m in msgs), f"Expected Flush collector: {msgs}"
        assert any("Read batch" in m for m in msgs), f"Expected Read batch: {msgs}"


class TestDebugLoggingError:
    """Integration tests for debug logging during error paths."""

    def test_server_error_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.response logs error batch when server raises an exception."""

        class _ErrorProtocol(Protocol):
            """Protocol with a method that raises."""

            def fail(self) -> float:
                """Fail unconditionally."""
                ...

        class _ErrorImpl:
            def fail(self) -> float:
                """Raise an error."""
                raise ValueError("deliberate test error")

        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"),
            serve_pipe(_ErrorProtocol, _ErrorImpl()) as svc,
            pytest.raises(RpcError, match="deliberate test error"),
        ):
            svc.fail()

        # Server side: _write_error_batch logs the error
        resp_msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.response")
        assert any("Write error batch" in m and "ValueError" in m for m in resp_msgs), (
            f"Expected Write error batch with ValueError: {resp_msgs}"
        )

        # Client side: _dispatch_log_or_error classifies the error batch
        batch_msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.batch")
        assert any("EXCEPTION" in m for m in batch_msgs), f"Expected EXCEPTION classification: {batch_msgs}"


class TestDebugLoggingHttp:
    """Integration tests for wire.http logger via in-process HTTP transport."""

    def test_http_unary_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.http logs request and response for HTTP unary call."""
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import make_sync_client
        from vgi_rpc.rpc import RpcServer

        server = RpcServer(_DebugProtocol, _DebugImpl())
        client = make_sync_client(server, signing_key=b"test-key")

        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), http_connect(_DebugProtocol, client=client) as svc:
            result = svc.add(a=1.0, b=2.0)

        assert result == 3.0
        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.http")
        assert any("HTTP unary call" in m for m in msgs), f"Expected HTTP unary call: {msgs}"
        assert any("HTTP unary response" in m and "status=" in m for m in msgs), f"Expected HTTP unary response: {msgs}"

    def test_http_stream_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """wire.http logs init and response for HTTP producer stream call."""
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import make_sync_client
        from vgi_rpc.rpc import RpcServer

        server = RpcServer(_DebugProtocol, _DebugImpl())
        client = make_sync_client(server, signing_key=b"test-key")

        with caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire"), http_connect(_DebugProtocol, client=client) as svc:
            batches = list(svc.count(n=2))

        assert len(batches) == 2
        msgs = _messages_for(_wire_records(caplog), "vgi_rpc.wire.http")
        # HTTP producer streams bundle all output in the init response
        assert any("HTTP stream init" in m for m in msgs), f"Expected HTTP stream init: {msgs}"
        assert any("HTTP stream init response" in m and "status=" in m for m in msgs), (
            f"Expected HTTP stream init response: {msgs}"
        )
