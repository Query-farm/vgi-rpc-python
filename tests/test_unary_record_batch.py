# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Explicit unary Arrow schemas interoperate with non-Python services."""

from __future__ import annotations

from io import BytesIO
from typing import Annotated, Protocol

import pyarrow as pa
import pytest

from vgi_rpc import RpcError, RpcServer, serve_pipe
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._wire import _write_error_batch
from vgi_rpc.utils import new_ipc_stream

_FIELDS: list[pa.Field[pa.DataType]] = [
    pa.field("handle", pa.string(), nullable=False),
    pa.field("count", pa.int64()),
]
SCHEMA = pa.schema(_FIELDS)
Batch = Annotated[pa.RecordBatch, SCHEMA]


class BatchService(Protocol):
    """Service with a fixed response schema."""

    def fetch(self, count: int) -> Batch:
        """Return the requested number of rows."""
        ...


class BatchServiceImpl:
    """Implement the fixed-schema service."""

    def fetch(self, count: int) -> pa.RecordBatch:
        """Build a batch without a scalar result wrapper."""
        return pa.record_batch([["handle"] * count, list(range(count))], schema=SCHEMA)


@pytest.mark.parametrize("count", [0, 1, 3])
@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_explicit_batch_roundtrip(count: int, transport: str) -> None:
    """Preserve field names, nullability, empty results and multiple rows."""
    impl = BatchServiceImpl()
    if transport == "pipe":
        with serve_pipe(BatchService, impl) as client:
            result = client.fetch(count=count)
    else:
        client_http = make_sync_client(RpcServer(BatchService, impl), token_key=b"test")
        with http_connect(BatchService, client=client_http) as client:
            result = client.fetch(count=count)
    assert result.equals(impl.fetch(count))
    assert result.schema.equals(SCHEMA, check_metadata=True)


def test_bare_batch_retains_binary_wrapper() -> None:
    """Existing bare RecordBatch return annotations keep their binary envelope."""

    class Missing(Protocol):
        def fetch(self) -> pa.RecordBatch: ...

    assert rpc_methods(Missing)["fetch"].result_schema.field("result").type == pa.binary()


@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_bare_batch_still_roundtrips(transport: str) -> None:
    """The opt-in schema feature does not change existing binary batch results."""

    class Legacy(Protocol):
        def fetch(self, count: int) -> pa.RecordBatch: ...

    impl = BatchServiceImpl()
    if transport == "pipe":
        with serve_pipe(Legacy, impl) as client:
            assert client.fetch(count=2).equals(impl.fetch(2))
    else:
        client_http = make_sync_client(RpcServer(Legacy, impl), token_key=b"test")
        with http_connect(Legacy, client=client_http) as client:
            assert client.fetch(count=2).equals(impl.fetch(2))


@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_batch_schema_mismatch_rejected(transport: str) -> None:
    """Both transports reject invalid return schemas before writing data."""

    class Wrong:
        def fetch(self, count: int) -> pa.RecordBatch:
            return pa.record_batch([[count]], names=["wrong"])

    if transport == "pipe":
        with serve_pipe(BatchService, Wrong()) as client, pytest.raises(RpcError, match="declared schema"):
            client.fetch(count=1)
    else:
        client_http = make_sync_client(RpcServer(BatchService, Wrong()), token_key=b"test")
        with http_connect(BatchService, client=client_http) as client, pytest.raises(RpcError, match="declared schema"):
            client.fetch(count=1)


def test_wire_error_message_preserves_structured_payload() -> None:
    """Cross-language clients can parse raw JSON without stripping type names."""
    payload = '{"status":"not_implemented","message":"unsupported"}'
    buf = BytesIO()
    with new_ipc_stream(buf, SCHEMA) as writer:
        _write_error_batch(writer, SCHEMA, ValueError(payload))
    reader = pa.ipc.open_stream(buf.getvalue())
    _, metadata = reader.read_next_batch_with_custom_metadata()
    assert metadata is not None
    assert metadata[b"vgi_rpc.log_message"].decode() == payload
