"""Tests for IPC validation feature (IpcValidation, ValidatedReader, validate_batch)."""

from __future__ import annotations

from typing import Protocol
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.rpc import RpcConnection, RpcServer, make_pipe_pair, serve_pipe
from vgi_rpc.utils import (
    IPCError,
    IpcValidation,
    ValidatedReader,
    serialize_record_batch_bytes,
    validate_batch,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIMPLE_SCHEMA = pa.schema([pa.field("x", pa.int64())])


def _make_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=_SIMPLE_SCHEMA)


def _make_ipc_bytes(batch: pa.RecordBatch | None = None) -> bytes:
    if batch is None:
        batch = _make_batch()
    return serialize_record_batch_bytes(batch)


# ---------------------------------------------------------------------------
# validate_batch
# ---------------------------------------------------------------------------


class TestValidateBatch:
    """Tests for the validate_batch helper."""

    def test_none_is_noop(self) -> None:
        """IpcValidation.NONE performs no validation."""
        batch = _make_batch()
        validate_batch(batch, IpcValidation.NONE)  # should not raise

    def test_standard_valid_batch(self) -> None:
        """Valid batch passes STANDARD validation."""
        batch = _make_batch()
        validate_batch(batch, IpcValidation.STANDARD)

    def test_full_valid_batch(self) -> None:
        """Valid batch passes FULL validation."""
        batch = _make_batch()
        validate_batch(batch, IpcValidation.FULL)

    def test_standard_calls_validate(self) -> None:
        """STANDARD calls batch.validate(full=False)."""
        batch = MagicMock(spec=pa.RecordBatch)
        validate_batch(batch, IpcValidation.STANDARD)
        batch.validate.assert_called_once_with(full=False)

    def test_full_calls_validate_full(self) -> None:
        """FULL calls batch.validate(full=True)."""
        batch = MagicMock(spec=pa.RecordBatch)
        validate_batch(batch, IpcValidation.FULL)
        batch.validate.assert_called_once_with(full=True)

    def test_raises_ipc_error_on_invalid(self) -> None:
        """ArrowInvalid from batch.validate() is wrapped as IPCError."""
        batch = MagicMock(spec=pa.RecordBatch)
        batch.validate.side_effect = pa.ArrowInvalid("corrupt data")
        with pytest.raises(IPCError, match="IPC batch validation failed"):
            validate_batch(batch, IpcValidation.FULL)


# ---------------------------------------------------------------------------
# ValidatedReader
# ---------------------------------------------------------------------------


class TestValidatedReader:
    """Tests for the ValidatedReader wrapper."""

    def test_validates_each_batch(self) -> None:
        """ValidatedReader calls validate_batch on every read_next_batch."""
        data = _make_ipc_bytes()
        inner = ipc.open_stream(pa.BufferReader(data))
        reader = ValidatedReader(inner, IpcValidation.FULL)
        batch = reader.read_next_batch()
        assert batch.num_rows == 3

    def test_validates_each_batch_with_custom_metadata(self) -> None:
        """ValidatedReader validates on read_next_batch_with_custom_metadata."""
        data = _make_ipc_bytes()
        inner = ipc.open_stream(pa.BufferReader(data))
        reader = ValidatedReader(inner, IpcValidation.FULL)
        batch, _ = reader.read_next_batch_with_custom_metadata()
        assert batch.num_rows == 3

    def test_context_manager(self) -> None:
        """ValidatedReader delegates context manager protocol."""
        data = _make_ipc_bytes()
        inner = ipc.open_stream(pa.BufferReader(data))
        with ValidatedReader(inner, IpcValidation.NONE) as reader:
            batch = reader.read_next_batch()
            assert batch.num_rows == 3

    def test_schema_property(self) -> None:
        """ValidatedReader exposes the inner reader's schema."""
        data = _make_ipc_bytes()
        inner = ipc.open_stream(pa.BufferReader(data))
        reader = ValidatedReader(inner, IpcValidation.NONE)
        assert reader.schema == _SIMPLE_SCHEMA


# ---------------------------------------------------------------------------
# Default validation levels
# ---------------------------------------------------------------------------


class TestDefaults:
    """Tests for default IpcValidation levels on RpcServer and RpcConnection."""

    class _Svc(Protocol):
        def add(self, a: float, b: float) -> float: ...

    class _Impl:
        def add(self, a: float, b: float) -> float:
            return a + b

    def test_rpc_server_default_full(self) -> None:
        """RpcServer defaults to IpcValidation.FULL."""
        server = RpcServer(self._Svc, self._Impl())
        assert server.ipc_validation is IpcValidation.FULL

    def test_rpc_connection_default_full(self) -> None:
        """RpcConnection defaults to IpcValidation.FULL."""
        c, s = make_pipe_pair()
        try:
            conn = RpcConnection(self._Svc, c)
            assert conn._ipc_validation is IpcValidation.FULL
        finally:
            c.close()
            s.close()


# ---------------------------------------------------------------------------
# serve_pipe defaults
# ---------------------------------------------------------------------------


class TestServePipeDefaults:
    """Tests for serve_pipe() default validation levels."""

    class _Svc(Protocol):
        def add(self, a: float, b: float) -> float: ...

    class _Impl:
        def add(self, a: float, b: float) -> float:
            return a + b

    def test_default_none_uses_full_for_both(self) -> None:
        """serve_pipe(ipc_validation=None) creates server=FULL, client=FULL."""
        with serve_pipe(self._Svc, self._Impl()) as proxy:
            # The proxy works correctly with valid data
            result = proxy.add(a=1.0, b=2.0)
            assert result == 3.0

    def test_explicit_full_propagates(self) -> None:
        """serve_pipe(ipc_validation=FULL) propagates to both components."""
        with serve_pipe(self._Svc, self._Impl(), ipc_validation=IpcValidation.FULL) as proxy:
            result = proxy.add(a=1.0, b=2.0)
            assert result == 3.0


# ---------------------------------------------------------------------------
# End-to-end validation
# ---------------------------------------------------------------------------


class TestEndToEndValidation:
    """End-to-end tests for IPC validation through serve_pipe."""

    class _Svc(Protocol):
        def add(self, a: float, b: float) -> float: ...

        def greet(self, name: str) -> str: ...

    class _Impl:
        def add(self, a: float, b: float) -> float:
            return a + b

        def greet(self, name: str) -> str:
            return f"Hello, {name}!"

    def test_round_trip_with_full_validation(self) -> None:
        """Valid data round-trips cleanly with FULL validation on both sides."""
        with serve_pipe(self._Svc, self._Impl(), ipc_validation=IpcValidation.FULL) as proxy:
            assert proxy.add(a=10.0, b=20.0) == 30.0
            assert proxy.greet(name="Alice") == "Hello, Alice!"

    def test_round_trip_with_none_validation(self) -> None:
        """Valid data round-trips cleanly with NONE validation."""
        with serve_pipe(self._Svc, self._Impl(), ipc_validation=IpcValidation.NONE) as proxy:
            assert proxy.add(a=5.0, b=3.0) == 8.0

    def test_round_trip_with_standard_validation(self) -> None:
        """Valid data round-trips cleanly with STANDARD validation."""
        with serve_pipe(self._Svc, self._Impl(), ipc_validation=IpcValidation.STANDARD) as proxy:
            assert proxy.add(a=7.0, b=2.0) == 9.0
