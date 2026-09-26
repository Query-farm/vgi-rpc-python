# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Stock unary Arrow batches retain their binary envelopes and error prefixes."""

from io import BytesIO
from typing import Protocol

import pyarrow as pa
import pytest

from vgi_rpc import RpcServer, serve_pipe
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._wire import _write_error_batch
from vgi_rpc.utils import new_ipc_stream


class BatchService(Protocol):
    """Declare a stock binary-serialized Arrow batch result."""

    def fetch(self, count: int) -> pa.RecordBatch:
        """Return an arbitrary Arrow batch inside the scalar envelope."""
        ...


class BatchServiceImpl:
    """Return ordinary Arrow values through the existing transport contract."""

    def fetch(self, count: int) -> pa.RecordBatch:
        """Build a batch using the requested number of rows."""
        return pa.record_batch([list(range(count))], names=["value"])


def test_bare_batch_retains_binary_wrapper() -> None:
    """Unannotated RecordBatch return values use the stock binary result field."""
    expected = pa.schema([pa.field("result", pa.binary(), nullable=False)])
    assert rpc_methods(BatchService)["fetch"].result_schema.equals(expected)


@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_bare_batch_roundtrips(transport: str) -> None:
    """Both transports preserve a batch carried inside the binary result field."""
    implementation = BatchServiceImpl()
    if transport == "pipe":
        with serve_pipe(BatchService, implementation) as client:
            assert client.fetch(count=2).equals(implementation.fetch(2))
    else:
        http = make_sync_client(RpcServer(BatchService, implementation), token_key=b"test")
        with http_connect(BatchService, client=http) as client:
            assert client.fetch(count=2).equals(implementation.fetch(2))


def test_wire_error_retains_exception_type_prefix() -> None:
    """The transport keeps Message.from_exception's existing error representation."""
    schema = pa.schema([pa.field("result", pa.binary(), nullable=False)])
    buffer = BytesIO()
    with new_ipc_stream(buffer, schema) as writer:
        _write_error_batch(writer, schema, ValueError("fixture failure"))
    _, metadata = pa.ipc.open_stream(buffer.getvalue()).read_next_batch_with_custom_metadata()
    assert metadata is not None
    assert metadata[b"vgi_rpc.log_message"] == b"ValueError: fixture failure"
