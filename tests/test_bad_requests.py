# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Robustness tests for adversarial / malformed HTTP requests.

Verifies that the server returns proper error responses (never crashes,
never hangs) when clients send malformed, unexpected, or deliberately
hostile payloads.
"""

from __future__ import annotations

from collections.abc import Iterator
from io import BytesIO

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.http import (
    _ARROW_CONTENT_TYPE,
    _SyncTestClient,
    make_sync_client,
)
from vgi_rpc.metadata import REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY
from vgi_rpc.rpc import RpcError, RpcServer, _dispatch_log_or_error, _drain_stream
from vgi_rpc.utils import IpcValidation, ValidatedReader, empty_batch

from .test_rpc import RpcFixtureService, RpcFixtureServiceImpl

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_URL = "http://test"
_CT = {"Content-Type": _ARROW_CONTENT_TYPE}


@pytest.fixture
def client() -> Iterator[_SyncTestClient]:
    """Create a sync Falcon test client."""
    c = make_sync_client(RpcServer(RpcFixtureService, RpcFixtureServiceImpl()), signing_key=b"test-key")
    yield c
    c.close()


def _post(
    client: _SyncTestClient, path: str, body: bytes, *, content_type: str = _ARROW_CONTENT_TYPE
) -> tuple[int, bytes]:
    """Send a POST and return (status_code, body)."""
    resp = client.post(f"{_BASE_URL}{path}", content=body, headers={"Content-Type": content_type})
    return resp.status_code, resp.content


def _extract_error(content: bytes) -> RpcError:
    """Parse an Arrow IPC error stream, return the RpcError."""
    reader = ValidatedReader(ipc.open_stream(BytesIO(content)), IpcValidation.NONE)
    try:
        while True:
            batch, cm = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, cm)
    except RpcError as exc:
        _drain_stream(reader)
        return exc
    except StopIteration:
        pass
    raise AssertionError("No RpcError in response body")


def _craft_request(
    method: str,
    schema: pa.Schema,
    batch: pa.RecordBatch,
    *,
    version: bytes = REQUEST_VERSION,
    include_method: bool = True,
    include_version: bool = True,
) -> bytes:
    """Build a raw IPC request stream with full control over metadata."""
    md: dict[bytes, bytes] = {}
    if include_method:
        md[RPC_METHOD_KEY] = method.encode()
    if include_version:
        md[REQUEST_VERSION_KEY] = version
    cm = pa.KeyValueMetadata(md) if md else None
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        if cm is not None:
            writer.write_batch(batch, custom_metadata=cm)
        else:
            writer.write_batch(batch)
    return buf.getvalue()


def _craft_valid_add(a: float = 1.0, b: float = 2.0) -> bytes:
    """Build a valid 'add' request."""
    schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
    batch = pa.RecordBatch.from_pydict({"a": [a], "b": [b]}, schema=schema)
    return _craft_request("add", schema, batch)


# ===========================================================================
# Garbage / non-IPC payloads
# ===========================================================================


class TestGarbagePayloads:
    """Server handles completely invalid payloads gracefully."""

    def test_empty_body(self, client: _SyncTestClient) -> None:
        """Empty request body returns 400."""
        status, _ = _post(client, "/vgi/add", b"")
        assert status == 400

    def test_single_null_byte(self, client: _SyncTestClient) -> None:
        """Single null byte is not valid IPC."""
        status, _ = _post(client, "/vgi/add", b"\x00")
        assert status == 400

    def test_random_garbage(self, client: _SyncTestClient) -> None:
        """Random bytes are not valid IPC."""
        status, _ = _post(client, "/vgi/add", b"this is not arrow ipc data at all")
        assert status == 400

    def test_truncated_ipc(self, client: _SyncTestClient) -> None:
        """Truncated IPC stream (cut mid-schema) returns 400."""
        valid = _craft_valid_add()
        truncated = valid[: len(valid) // 2]
        status, _ = _post(client, "/vgi/add", truncated)
        assert status == 400

    def test_json_body(self, client: _SyncTestClient) -> None:
        """JSON body is not valid IPC even with correct Content-Type."""
        status, _ = _post(client, "/vgi/add", b'{"a": 1, "b": 2}')
        assert status == 400

    def test_very_large_garbage(self, client: _SyncTestClient) -> None:
        """Large garbage payload doesn't crash the server."""
        status, _ = _post(client, "/vgi/add", b"X" * 100_000)
        assert status == 400


# ===========================================================================
# Bad method names
# ===========================================================================


class TestBadMethodNames:
    """Server handles malicious or malformed method names."""

    def test_empty_method_name_in_ipc(self, client: _SyncTestClient) -> None:
        """Empty string method name in IPC metadata mismatches URL — returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0]}, schema=schema)
        body = _craft_request("", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        # IPC metadata says "" but URL says "add" — method name mismatch
        assert status == 400
        err = _extract_error(content)
        assert "mismatch" in err.error_message.lower()

    def test_unknown_method_url(self, client: _SyncTestClient) -> None:
        """Non-existent method in URL returns 404."""
        status, content = _post(client, "/vgi/nonexistent", _craft_valid_add())
        assert status == 404
        err = _extract_error(content)
        assert "nonexistent" in err.error_message

    def test_path_traversal_attempt(self, client: _SyncTestClient) -> None:
        """Path traversal in method name is treated as unknown method."""
        status, _ = _post(client, "/vgi/../../etc/passwd", _craft_valid_add())
        assert status in (400, 404)

    def test_very_long_method_name(self, client: _SyncTestClient) -> None:
        """Very long method name doesn't crash or hang."""
        long_name = "a" * 10_000
        status, _ = _post(client, f"/vgi/{long_name}", _craft_valid_add())
        assert status == 404

    def test_method_with_null_bytes(self, client: _SyncTestClient) -> None:
        """Method name with null bytes in URL is handled."""
        status, _ = _post(client, "/vgi/add%00extra", _craft_valid_add())
        # Falcon may treat this as a different path
        assert status in (400, 404)

    def test_method_with_unicode(self, client: _SyncTestClient) -> None:
        """Unicode method name is treated as unknown."""
        status, _ = _post(client, "/vgi/\u2603snowman", _craft_valid_add())
        assert status == 404

    def test_method_with_slashes(self, client: _SyncTestClient) -> None:
        """Slashes in method name are handled by routing."""
        status, _ = _post(client, "/vgi/add/extra/path", _craft_valid_add())
        # Falcon routes: /vgi/{method}, /vgi/{method}/init, /vgi/{method}/exchange
        # /vgi/add/extra/path won't match any route
        assert status in (400, 404)


# ===========================================================================
# Bad Content-Type
# ===========================================================================


class TestBadContentType:
    """Server rejects wrong Content-Type."""

    def test_text_plain(self, client: _SyncTestClient) -> None:
        """text/plain is rejected."""
        status, content = _post(client, "/vgi/add", _craft_valid_add(), content_type="text/plain")
        assert status == 415
        err = _extract_error(content)
        assert "Content-Type" in err.error_message

    def test_application_json(self, client: _SyncTestClient) -> None:
        """application/json is rejected."""
        status, _ = _post(client, "/vgi/add", b'{"a":1}', content_type="application/json")
        assert status == 415

    def test_empty_content_type(self, client: _SyncTestClient) -> None:
        """Empty Content-Type header is rejected."""
        status, _ = _post(client, "/vgi/add", _craft_valid_add(), content_type="")
        assert status == 415

    def test_almost_correct_content_type(self, client: _SyncTestClient) -> None:
        """Slightly wrong Content-Type (missing 'stream') is rejected."""
        status, _ = _post(client, "/vgi/add", _craft_valid_add(), content_type="application/vnd.apache.arrow")
        assert status == 415


# ===========================================================================
# Wrong HTTP method
# ===========================================================================


class TestWrongHttpMethod:
    """Server rejects non-POST methods."""

    def test_get_on_rpc_endpoint(self, client: _SyncTestClient) -> None:
        """GET on an RPC endpoint returns 405."""
        resp = client._client.simulate_get("/vgi/add")
        assert resp.status_code == 405

    def test_put_on_rpc_endpoint(self, client: _SyncTestClient) -> None:
        """PUT on an RPC endpoint returns 405."""
        resp = client._client.simulate_put("/vgi/add", body=b"")
        assert resp.status_code == 405

    def test_delete_on_rpc_endpoint(self, client: _SyncTestClient) -> None:
        """DELETE on an RPC endpoint returns 405."""
        resp = client._client.simulate_delete("/vgi/add")
        assert resp.status_code == 405


# ===========================================================================
# Valid IPC but bad metadata
# ===========================================================================


class TestBadMetadata:
    """Valid Arrow IPC but with missing or wrong metadata."""

    def test_missing_method_key(self, client: _SyncTestClient) -> None:
        """Request batch without vgi_rpc.method metadata returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0]}, schema=schema)
        body = _craft_request("add", schema, batch, include_method=False)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_missing_version_key(self, client: _SyncTestClient) -> None:
        """Request batch without vgi_rpc.request_version returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0]}, schema=schema)
        body = _craft_request("add", schema, batch, include_version=False)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_wrong_version(self, client: _SyncTestClient) -> None:
        """Wrong request version returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0]}, schema=schema)
        body = _craft_request("add", schema, batch, version=b"999")
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert "version" in err.error_message.lower() or "Version" in err.error_message

    def test_no_metadata_at_all(self, client: _SyncTestClient) -> None:
        """Request batch with no custom metadata at all returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0]}, schema=schema)
        body = _craft_request("add", schema, batch, include_method=False, include_version=False)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert err is not None


# ===========================================================================
# Schema mismatches
# ===========================================================================


class TestSchemaMismatch:
    """Valid IPC but schema doesn't match the target method."""

    def test_wrong_column_names(self, client: _SyncTestClient) -> None:
        """Columns named differently than method parameters return 400."""
        schema = pa.schema([pa.field("x", pa.float64()), pa.field("y", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"x": [1.0], "y": [2.0]}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        # The server reads whatever columns are in the batch as kwargs.
        # "x" and "y" aren't "a" and "b" — the method call will fail with TypeError.
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_wrong_column_types(self, client: _SyncTestClient) -> None:
        """Column types don't match method parameter types (strings instead of floats)."""
        schema = pa.schema([pa.field("a", pa.utf8()), pa.field("b", pa.utf8())])
        batch = pa.RecordBatch.from_pydict({"a": ["one"], "b": ["two"]}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        # Method runs with wrong types; result serialization fails with ArrowInvalid.
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_extra_columns(self, client: _SyncTestClient) -> None:
        """Extra columns beyond what the method expects return 400."""
        schema = pa.schema(
            [
                pa.field("a", pa.float64()),
                pa.field("b", pa.float64()),
                pa.field("c", pa.float64()),
            ]
        )
        batch = pa.RecordBatch.from_pydict({"a": [1.0], "b": [2.0], "c": [3.0]}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        # Extra param 'c' will be passed as kwarg — method doesn't accept it
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_missing_columns(self, client: _SyncTestClient) -> None:
        """Missing required columns (only 'a', missing 'b') returns 400."""
        schema = pa.schema([pa.field("a", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0]}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert err is not None

    def test_empty_schema(self, client: _SyncTestClient) -> None:
        """Empty schema (no columns) for a method that expects parameters returns 400."""
        schema = pa.schema([])
        batch = pa.RecordBatch.from_pydict({}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert err is not None


# ===========================================================================
# Bad batch row counts
# ===========================================================================


class TestBadRowCounts:
    """Valid IPC with unexpected number of rows."""

    def test_zero_rows(self, client: _SyncTestClient) -> None:
        """Zero-row batch for a method that expects parameters returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = empty_batch(schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert "row" in err.error_message.lower()

    def test_multiple_rows(self, client: _SyncTestClient) -> None:
        """Multiple rows when exactly 1 is expected returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]}, schema=schema)
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert "1 row" in err.error_message or "row" in err.error_message.lower()

    def test_many_rows(self, client: _SyncTestClient) -> None:
        """Large number of rows (1000) when 1 is expected returns 400."""
        schema = pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())])
        batch = pa.RecordBatch.from_pydict(
            {"a": [float(i) for i in range(1000)], "b": [float(i) for i in range(1000)]},
            schema=schema,
        )
        body = _craft_request("add", schema, batch)
        status, content = _post(client, "/vgi/add", body)
        assert status == 400
        err = _extract_error(content)
        assert "row" in err.error_message.lower()


# ===========================================================================
# Endpoint routing abuse
# ===========================================================================


class TestEndpointRouting:
    """Tests for wrong endpoint/method-type combinations."""

    def test_stream_method_on_unary_endpoint(self, client: _SyncTestClient) -> None:
        """Stream method on the /vgi/{method} endpoint returns 400."""
        schema = pa.schema([pa.field("factor", pa.float64())])
        batch = pa.RecordBatch.from_pydict({"factor": [2.0]}, schema=schema)
        body = _craft_request("transform", schema, batch)
        status, content = _post(client, "/vgi/transform", body)
        assert status == 400
        err = _extract_error(content)
        assert "stream" in err.error_message.lower()

    def test_unary_method_on_init_endpoint(self, client: _SyncTestClient) -> None:
        """Unary method on /init endpoint returns 400."""
        body = _craft_valid_add()
        status, content = _post(client, "/vgi/add/init", body)
        assert status == 400
        err = _extract_error(content)
        assert "not a stream" in err.error_message

    def test_unary_method_on_exchange_endpoint(self, client: _SyncTestClient) -> None:
        """Unary method on /exchange endpoint returns 400."""
        body = _craft_valid_add()
        status, content = _post(client, "/vgi/add/exchange", body)
        assert status == 400
        err = _extract_error(content)
        assert "does not support /exchange" in err.error_message

    def test_unknown_method_on_init_endpoint(self, client: _SyncTestClient) -> None:
        """Unknown method on /init endpoint returns 404."""
        body = _craft_valid_add()
        status, _content = _post(client, "/vgi/nonexistent/init", body)
        assert status == 404

    def test_unknown_method_on_exchange_endpoint(self, client: _SyncTestClient) -> None:
        """Unknown method on /exchange endpoint returns 404."""
        body = _craft_valid_add()
        status, _content = _post(client, "/vgi/nonexistent/exchange", body)
        assert status == 404

    def test_garbage_on_stream_init(self, client: _SyncTestClient) -> None:
        """Garbage bytes on stream init returns 400."""
        status, _ = _post(client, "/vgi/transform/init", b"not ipc")
        assert status == 400

    def test_garbage_on_exchange(self, client: _SyncTestClient) -> None:
        """Garbage bytes on exchange returns 400."""
        status, _ = _post(client, "/vgi/transform/exchange", b"not ipc")
        assert status == 400

    def test_empty_body_on_stream_init(self, client: _SyncTestClient) -> None:
        """Empty body on stream init returns 400."""
        status, _ = _post(client, "/vgi/transform/init", b"")
        assert status == 400

    def test_empty_body_on_exchange(self, client: _SyncTestClient) -> None:
        """Empty body on exchange returns 400."""
        status, _ = _post(client, "/vgi/transform/exchange", b"")
        assert status == 400


# ===========================================================================
# State token tampering
# ===========================================================================


class TestStateTokenTampering:
    """Tests for tampered or forged state tokens on /exchange."""

    def _make_exchange_request(self, token: bytes) -> bytes:
        """Build an exchange request with a custom state token."""
        from vgi_rpc.metadata import STATE_KEY

        schema = pa.schema([])
        batch = empty_batch(schema)
        cm = pa.KeyValueMetadata({STATE_KEY: token})
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            writer.write_batch(batch, custom_metadata=cm)
        return buf.getvalue()

    def test_empty_token(self, client: _SyncTestClient) -> None:
        """Empty bytes as state token."""
        body = self._make_exchange_request(b"")
        status, _content = _post(client, "/vgi/transform/exchange", body)
        assert status == 400

    def test_short_token(self, client: _SyncTestClient) -> None:
        """Token shorter than minimum length."""
        body = self._make_exchange_request(b"\x00" * 10)
        status, _content = _post(client, "/vgi/transform/exchange", body)
        assert status == 400

    def test_random_token(self, client: _SyncTestClient) -> None:
        """Random bytes as state token (wrong HMAC)."""
        import os

        body = self._make_exchange_request(os.urandom(100))
        status, _content = _post(client, "/vgi/transform/exchange", body)
        assert status == 400

    def test_all_zeros_token(self, client: _SyncTestClient) -> None:
        """All-zero token."""
        body = self._make_exchange_request(b"\x00" * 100)
        status, _content = _post(client, "/vgi/transform/exchange", body)
        assert status == 400

    def test_no_state_token_in_metadata(self, client: _SyncTestClient) -> None:
        """Exchange request with no STATE_KEY in metadata."""
        schema = pa.schema([])
        batch = empty_batch(schema)
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            writer.write_batch(batch)
        status, _content = _post(client, "/vgi/transform/exchange", buf.getvalue())
        assert status == 400


# ===========================================================================
# Server stays healthy after bad requests
# ===========================================================================


class TestServerRecovery:
    """Verify the server can handle good requests after bad ones."""

    def test_good_after_garbage(self, client: _SyncTestClient) -> None:
        """Server works normally after receiving garbage."""
        # Send garbage
        _post(client, "/vgi/add", b"garbage")
        # Send a valid request
        body = _craft_valid_add(a=3.0, b=4.0)
        status, content = _post(client, "/vgi/add", body)
        assert status == 200
        reader = ipc.open_stream(BytesIO(content))
        batch = reader.read_next_batch()
        assert batch.num_rows == 1
        assert batch.column(0)[0].as_py() == 7.0

    def test_good_after_wrong_content_type(self, client: _SyncTestClient) -> None:
        """Server works after wrong Content-Type."""
        _post(client, "/vgi/add", b"hello", content_type="text/plain")
        body = _craft_valid_add(a=10.0, b=20.0)
        status, content = _post(client, "/vgi/add", body)
        assert status == 200
        reader = ipc.open_stream(BytesIO(content))
        batch = reader.read_next_batch()
        assert batch.column(0)[0].as_py() == 30.0

    def test_good_after_schema_mismatch(self, client: _SyncTestClient) -> None:
        """Server works after schema mismatch error."""
        schema = pa.schema([pa.field("x", pa.utf8())])
        batch = pa.RecordBatch.from_pydict({"x": ["bad"]}, schema=schema)
        body = _craft_request("add", schema, batch)
        _post(client, "/vgi/add", body)
        # Now send a valid request
        body = _craft_valid_add(a=5.0, b=5.0)
        status, content = _post(client, "/vgi/add", body)
        assert status == 200
        reader = ipc.open_stream(BytesIO(content))
        batch = reader.read_next_batch()
        assert batch.column(0)[0].as_py() == 10.0

    def test_good_after_bad_token(self, client: _SyncTestClient) -> None:
        """Server works after tampered state token."""
        from vgi_rpc.metadata import STATE_KEY

        schema = pa.schema([])
        batch = empty_batch(schema)
        cm = pa.KeyValueMetadata({STATE_KEY: b"bad-token"})
        buf = BytesIO()
        with ipc.new_stream(buf, schema) as writer:
            writer.write_batch(batch, custom_metadata=cm)
        _post(client, "/vgi/transform/exchange", buf.getvalue())
        # Now valid request
        body = _craft_valid_add(a=7.0, b=8.0)
        status, content = _post(client, "/vgi/add", body)
        assert status == 200
        reader = ipc.open_stream(BytesIO(content))
        batch = reader.read_next_batch()
        assert batch.column(0)[0].as_py() == 15.0

    def test_multiple_bad_then_good(self, client: _SyncTestClient) -> None:
        """Server survives a barrage of bad requests then handles a good one."""
        # Various bad requests
        _post(client, "/vgi/add", b"")
        _post(client, "/vgi/add", b"\x00" * 1000)
        _post(client, "/vgi/nonexistent", _craft_valid_add())
        _post(client, "/vgi/add", _craft_valid_add(), content_type="text/html")
        _post(client, "/vgi/add/init", _craft_valid_add())
        # Valid request succeeds
        body = _craft_valid_add(a=100.0, b=200.0)
        status, content = _post(client, "/vgi/add", body)
        assert status == 200
        reader = ipc.open_stream(BytesIO(content))
        batch = reader.read_next_batch()
        assert batch.column(0)[0].as_py() == 300.0
