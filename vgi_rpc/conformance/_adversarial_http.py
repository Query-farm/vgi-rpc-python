# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Adversarial HTTP request-contract conformance tests.

The payloads in this module are valid Arrow IPC. Only their RPC contract is
wrong, so a conformant HTTP worker must reject them as typed 400 responses
without dispatching the method or damaging the worker.
"""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.metadata import PROTOCOL_VERSION_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY
from vgi_rpc.rpc import RpcError, _dispatch_log_or_error, _drain_stream, rpc_methods
from vgi_rpc.utils import IpcValidation, ValidatedReader

if TYPE_CHECKING:
    import httpx2

pytestmark = pytest.mark.timeout(5)

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
_PROTOCOL_VERSION = vars(ConformanceService)["protocol_version"]

_METHOD_VALUES: dict[str, dict[str, object]] = {
    "add_floats": {"a": 1.0, "b": 2.0},
    "produce_dynamic_schema": {
        "seed": 7,
        "count": 1,
        "include_strings": True,
        "include_floats": True,
    },
}

_SCHEMA_MUTATIONS = (
    "missing_field",
    "extra_field",
    "wrong_name",
    "wrong_order",
    "wrong_type",
    "wrong_nullability",
    "zero_rows",
    "two_rows",
)

_METADATA_MUTATIONS = (
    "missing_method",
    "missing_request_version",
    "wrong_request_version",
    "non_utf8_method",
)


def _request_body(
    method_name: str,
    schema: pa.Schema,
    batch: pa.RecordBatch,
    *,
    metadata_mutation: str | None = None,
) -> bytes:
    """Serialize one complete request with optionally malformed RPC metadata."""
    metadata = {
        RPC_METHOD_KEY: method_name.encode(),
        REQUEST_VERSION_KEY: REQUEST_VERSION,
        PROTOCOL_VERSION_KEY: str(_PROTOCOL_VERSION).encode(),
    }
    if metadata_mutation == "missing_method":
        del metadata[RPC_METHOD_KEY]
    elif metadata_mutation == "missing_request_version":
        del metadata[REQUEST_VERSION_KEY]
    elif metadata_mutation == "wrong_request_version":
        metadata[REQUEST_VERSION_KEY] = b"999"
    elif metadata_mutation == "non_utf8_method":
        metadata[RPC_METHOD_KEY] = b"\xff\xfe"
    elif metadata_mutation is not None:
        raise AssertionError(f"unknown metadata mutation: {metadata_mutation}")

    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        writer.write_batch(batch, custom_metadata=pa.KeyValueMetadata(metadata))
    return buf.getvalue()


def _valid_request(method_name: str) -> bytes:
    """Build a valid one-row request for a method in ``_METHOD_VALUES``."""
    info = rpc_methods(ConformanceService)[method_name]
    values = _METHOD_VALUES[method_name]
    arrays = [pa.array([values[field.name]], type=field.type) for field in info.params_schema]
    batch = pa.RecordBatch.from_arrays(arrays, schema=info.params_schema)
    return _request_body(method_name, info.params_schema, batch)


def _schema_mutation_body(method_name: str, mutation: str) -> bytes:
    """Build a structurally valid request whose parameter contract is wrong."""
    info = rpc_methods(ConformanceService)[method_name]
    fields = list(info.params_schema)
    values = [_METHOD_VALUES[method_name][field.name] for field in fields]
    rows = 1

    if mutation == "missing_field":
        fields.pop()
        values.pop()
    elif mutation == "extra_field":
        fields.append(pa.field("unexpected", pa.utf8(), nullable=False))
        values.append("extra")
    elif mutation == "wrong_name":
        field = fields[0]
        fields[0] = pa.field("wrong_name", field.type, nullable=field.nullable)
    elif mutation == "wrong_order":
        fields.reverse()
        values.reverse()
    elif mutation == "wrong_type":
        field = fields[0]
        fields[0] = pa.field(field.name, pa.utf8(), nullable=field.nullable)
        values[0] = "wrong type"
    elif mutation == "wrong_nullability":
        field = fields[0]
        fields[0] = pa.field(field.name, field.type, nullable=not field.nullable)
    elif mutation == "zero_rows":
        rows = 0
    elif mutation == "two_rows":
        rows = 2
    else:
        raise AssertionError(f"unknown schema mutation: {mutation}")

    schema = pa.schema(fields)
    arrays = [pa.array([value] * rows, type=field.type) for field, value in zip(fields, values, strict=True)]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    return _request_body(method_name, schema, batch)


def _metadata_mutation_body(method_name: str, mutation: str) -> bytes:
    """Build a request with a valid parameter batch and malformed RPC metadata."""
    info = rpc_methods(ConformanceService)[method_name]
    values = _METHOD_VALUES[method_name]
    arrays = [pa.array([values[field.name]], type=field.type) for field in info.params_schema]
    batch = pa.RecordBatch.from_arrays(arrays, schema=info.params_schema)
    return _request_body(method_name, info.params_schema, batch, metadata_mutation=mutation)


def _reset_probe_body(*, invalid_schema: bool = False, missing_method: bool = False) -> bytes:
    """Build a reset request used to prove invalid calls never dispatch."""
    info = rpc_methods(ConformanceService)["reset_cancel_probe"]
    if invalid_schema:
        schema = pa.schema([pa.field("unexpected", pa.int64(), nullable=False)])
        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], schema=schema)
    else:
        schema = info.params_schema
        batch = pa.RecordBatch.from_arrays([], schema=schema)
    return _request_body(
        "reset_cancel_probe",
        schema,
        batch,
        metadata_mutation="missing_method" if missing_method else None,
    )


def _post(port: int, method_name: str, body: bytes, *, suffix: str = "") -> httpx2.Response:
    """Post to either supported conformance route prefix."""
    import httpx2

    response = None
    for prefix in ("", "/vgi"):
        response = httpx2.post(
            f"http://127.0.0.1:{port}{prefix}/{method_name}{suffix}",
            content=body,
            headers={"Content-Type": _ARROW_CONTENT_TYPE, "Accept-Encoding": "identity"},
            timeout=5.0,
        )
        if response.status_code != 404:
            return response
    assert response is not None
    return response


def _extract_error(content: bytes) -> RpcError:
    """Extract the typed RPC error from an Arrow response body."""
    reader = ValidatedReader(ipc.open_stream(BytesIO(content)), IpcValidation.NONE)
    try:
        while True:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, metadata)
    except RpcError as exc:
        _drain_stream(reader)
        return exc
    except StopIteration:
        pass
    raise AssertionError("HTTP 400 response did not contain a typed RPC error")


def _assert_rejected(port: int, method_name: str, body: bytes, *, suffix: str = "") -> None:
    """Assert typed rejection and verify the worker remains reusable."""
    response = _post(port, method_name, body, suffix=suffix)
    assert response.status_code == 400, (
        f"invalid request reached {method_name!r}: expected HTTP 400, got {response.status_code}"
    )
    error = _extract_error(response.content)
    assert error.error_type, "HTTP 400 must carry a typed RPC error"

    recovery = _post(port, "add_floats", _valid_request("add_floats"))
    assert recovery.status_code == 200, (
        f"worker did not recover after rejecting {method_name!r}: follow-up call returned HTTP {recovery.status_code}"
    )
    reader = ipc.open_stream(BytesIO(recovery.content))
    result = reader.read_next_batch()
    assert result.column("result")[0].as_py() == pytest.approx(3.0)


class TestAdversarialHttpRequestContract:
    """Valid Arrow IPC with an invalid RPC contract is rejected before dispatch."""

    @pytest.mark.parametrize("mutation", _SCHEMA_MUTATIONS)
    def test_unary_parameter_contract(self, conformance_http_port: int, mutation: str) -> None:
        """Unary parameter schemas, nullability, and row count are exact."""
        _assert_rejected(
            conformance_http_port,
            "add_floats",
            _schema_mutation_body("add_floats", mutation),
        )

    @pytest.mark.parametrize("mutation", _SCHEMA_MUTATIONS)
    def test_stream_init_parameter_contract(self, conformance_http_port: int, mutation: str) -> None:
        """Stream-init requests enforce the same parameter contract as unary calls."""
        _assert_rejected(
            conformance_http_port,
            "produce_dynamic_schema",
            _schema_mutation_body("produce_dynamic_schema", mutation),
            suffix="/init",
        )

    @pytest.mark.parametrize("mutation", _METADATA_MUTATIONS)
    @pytest.mark.parametrize(
        ("method_name", "suffix"),
        (("add_floats", ""), ("produce_dynamic_schema", "/init")),
        ids=("unary", "stream_init"),
    )
    def test_dispatch_metadata_contract(
        self,
        conformance_http_port: int,
        mutation: str,
        method_name: str,
        suffix: str,
    ) -> None:
        """Required dispatch metadata is validated consistently on both endpoints."""
        _assert_rejected(
            conformance_http_port,
            method_name,
            _metadata_mutation_body(method_name, mutation),
            suffix=suffix,
        )

    @pytest.mark.parametrize(
        ("invalid_schema", "missing_method"),
        ((True, False), (False, True)),
        ids=("schema", "metadata"),
    )
    def test_rejection_occurs_before_dispatch(
        self,
        conformance_http_port: int,
        invalid_schema: bool,
        missing_method: bool,
    ) -> None:
        """Representative schema and metadata failures cannot run a handler."""
        from vgi_rpc.http import http_connect

        base_url = f"http://127.0.0.1:{conformance_http_port}"
        with http_connect(ConformanceService, base_url) as proxy:  # type: ignore[type-abstract]
            proxy.reset_cancel_probe()
            session = proxy.cancellable_exchange()
            session.cancel()
            assert proxy.cancel_probe_counters()[2] == 1

            _assert_rejected(
                conformance_http_port,
                "reset_cancel_probe",
                _reset_probe_body(invalid_schema=invalid_schema, missing_method=missing_method),
            )
            assert proxy.cancel_probe_counters()[2] == 1, "invalid request dispatched reset_cancel_probe"
