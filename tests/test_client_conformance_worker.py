# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Self-tests for the native-client conformance reference worker."""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import time
from collections.abc import Callable
from decimal import Decimal
from io import BytesIO
from typing import cast
from wsgiref.types import StartResponse, WSGIEnvironment

import pyarrow as pa
import pytest

from vgi_rpc.conformance.client_worker import (
    TYPED_EXCHANGE_SCHEMA,
    ClientConformanceService,
    ClientConformanceServiceImpl,
    StrictExchangeSchemaMiddleware,
)
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import AnnotatedBatch, RpcError, serve_pipe


def _exchange(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Round-trip one batch through the reference worker."""
    with (
        serve_pipe(ClientConformanceService, ClientConformanceServiceImpl()) as proxy,
        proxy.typed_exchange() as session,
    ):
        return session.exchange(AnnotatedBatch(batch=batch)).batch


def test_all_null_batch_preserves_declared_schema() -> None:
    """All-null runtime values cannot erase the client's declared types."""
    arrays = [pa.array([None], type=field.type) for field in TYPED_EXCHANGE_SCHEMA]
    batch = pa.RecordBatch.from_arrays(arrays, schema=TYPED_EXCHANGE_SCHEMA)
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.to_pylist() == batch.to_pylist()


def test_zero_row_batch_preserves_declared_schema() -> None:
    """An empty exchange still carries every declared field and logical type."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
        schema=TYPED_EXCHANGE_SCHEMA,
    )
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.num_rows == 0


def test_populated_batch_round_trips_nested_logical_types() -> None:
    """Dictionary, temporal, decimal, list, and struct values survive intact."""
    batch = pa.RecordBatch.from_pylist(
        [
            {
                "nullable_float": 1.5,
                "tags": ["alpha", None, "omega"],
                "category": "blue",
                "event_time": dt.datetime(2026, 8, 18, 12, 34, 56, tzinfo=dt.UTC),
                "amount": Decimal("1234.5000"),
                "nested": {"name": "sample", "scores": [1, None, 3]},
            }
        ],
        schema=TYPED_EXCHANGE_SCHEMA,
    )
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.to_pylist() == batch.to_pylist()


def test_strict_http_worker_rejects_inferred_all_null_schema() -> None:
    """The HTTP worker inspects the pre-cast wire schema and rejects inference."""
    inferred = pa.RecordBatch.from_pylist(
        [
            {
                "nullable_float": None,
                "tags": None,
                "category": None,
                "event_time": None,
                "amount": None,
                "nested": None,
            }
        ]
    )
    body = BytesIO()
    with pa.ipc.new_stream(body, inferred.schema) as writer:
        writer.write_batch(inferred)

    delegated = False

    def delegate(environ: WSGIEnvironment, start_response: StartResponse) -> list[bytes]:
        nonlocal delegated
        delegated = True
        return [b""]

    status = ""

    def start_response(
        value: str,
        headers: list[tuple[str, str]],
        exc_info: object = None,
    ) -> Callable[[bytes], object]:
        nonlocal status
        status = value

        def write(data: bytes) -> object:
            return None

        return write

    environ = cast(
        "WSGIEnvironment",
        {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/typed_exchange/exchange",
            "CONTENT_LENGTH": str(len(body.getvalue())),
            "wsgi.input": BytesIO(body.getvalue()),
        },
    )
    result = StrictExchangeSchemaMiddleware(delegate)(environ, start_response)
    assert not delegated
    assert status == "400 Bad Request"
    assert b"schema mismatch" in b"".join(cast("list[bytes]", result))


def test_http_worker_enforces_wire_schema_end_to_end() -> None:
    """The documented module entry point accepts exact and rejects inferred IPC."""
    import httpx2

    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.client_worker", "--http", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), line
        base_url = f"http://127.0.0.1:{int(line.split(':', 1)[1])}"
        deadline = time.monotonic() + 5
        while True:
            try:
                httpx2.get(f"{base_url}/health", timeout=0.5)
                break
            except httpx2.TransportError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

        exact = pa.RecordBatch.from_arrays(
            [pa.array([None], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
            schema=TYPED_EXCHANGE_SCHEMA,
        )
        inferred = pa.RecordBatch.from_pylist([{field.name: None for field in TYPED_EXCHANGE_SCHEMA}])
        with http_connect(ClientConformanceService, base_url) as proxy:
            with proxy.typed_exchange() as session:
                assert session.exchange(AnnotatedBatch(exact)).batch.schema == TYPED_EXCHANGE_SCHEMA
            with proxy.typed_exchange() as session, pytest.raises(RpcError):
                session.exchange(AnnotatedBatch(inferred))
    finally:
        proc.terminate()
        proc.wait(timeout=5)
