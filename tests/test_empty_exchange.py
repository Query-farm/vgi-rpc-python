# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""An explicitly declared exchange may legitimately have a zero-column schema."""

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa
import pytest

from vgi_rpc import AnnotatedBatch, CallContext, ExchangeState, OutputCollector, RpcServer, Stream, serve_pipe
from vgi_rpc.http import http_connect, make_sync_client


@dataclass
class EmptyExchange(ExchangeState):
    """Count actual client turns while preserving their application metadata."""

    count: int = 0

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one acknowledgement for one client-supplied zero-column batch."""
        self.count += 1
        marker = input.custom_metadata.get(b"marker") if input.custom_metadata is not None else None
        out.emit_pydict({"turn": [self.count], "marker": [marker]})


class EmptyProtocol(Protocol):
    """Declare exchange direction independently of the input field count."""

    def upload(self) -> Stream[EmptyExchange]:
        """Start an exchange with a zero-column input schema."""
        ...


class EmptyService:
    """Construct the declared zero-column exchange."""

    def upload(self) -> Stream[EmptyExchange]:
        """Return a stream that waits for input before advancing its counter."""
        fields: list[tuple[str, pa.DataType]] = [("turn", pa.int64()), ("marker", pa.binary())]
        return Stream(pa.schema(fields), EmptyExchange())


def _exercise(client: EmptyProtocol) -> None:
    empty = pa.RecordBatch.from_pydict({}, schema=pa.schema([]))
    with client.upload() as stream:
        for turn in (1, 2):
            marker = str(turn).encode()
            response = stream.exchange(AnnotatedBatch(empty, pa.KeyValueMetadata({b"marker": marker})))
            assert response.batch.column("turn")[0].as_py() == turn
            assert response.batch.column("marker")[0].as_py() == marker


@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_empty_schema_exchange_waits_for_input(transport: str) -> None:
    """Both transports advance once per explicit input and preserve its metadata."""
    if transport == "pipe":
        with serve_pipe(EmptyProtocol, EmptyService()) as client:
            _exercise(client)
    else:
        http = make_sync_client(RpcServer(EmptyProtocol, EmptyService()), token_key=b"test")
        with http_connect(EmptyProtocol, client=http) as client:
            _exercise(client)
