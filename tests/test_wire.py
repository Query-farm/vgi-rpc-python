"""Tests for the public wire-framing helpers (:mod:`vgi_rpc.wire`)."""

from __future__ import annotations

from io import BytesIO

import pyarrow as pa
import pyarrow.ipc as ipc

from vgi_rpc.metadata import PROTOCOL_VERSION_KEY, STATE_KEY
from vgi_rpc.wire import build_error_stream, find_state_token, read_request, write_request

_SCHEMA = pa.schema([pa.field("request", pa.binary())])


def _stream(schema: pa.Schema, *, token: bytes | None = None) -> bytes:
    buf = BytesIO()
    md = pa.KeyValueMetadata({STATE_KEY: token}) if token is not None else None
    with ipc.new_stream(buf, schema) as w:
        w.write_batch(pa.record_batch([[] for _ in schema], schema=schema), custom_metadata=md)
    return buf.getvalue()


def test_write_read_request_round_trip() -> None:
    """A request framed by write_request reads back with the same method + kwargs."""
    body = write_request("bind", _SCHEMA, {"request": b"payload-bytes"})
    method, kwargs = read_request(body)
    assert method == "bind"
    assert kwargs["request"] == b"payload-bytes"


def test_write_request_preserves_protocol_version() -> None:
    """A supplied protocol_version is stamped on the request batch metadata."""
    body = write_request("init", _SCHEMA, {"request": b"x"}, protocol_version="2.3")
    rb = ipc.open_stream(BytesIO(body)).read_next_batch_with_custom_metadata()
    assert rb.custom_metadata is not None
    assert rb.custom_metadata.get(PROTOCOL_VERSION_KEY) == b"2.3"


def test_write_request_omits_protocol_version_when_none() -> None:
    """Without a protocol_version the key is absent (exempt from the dispatch check)."""
    body = write_request("init", _SCHEMA, {"request": b"x"})
    rb = ipc.open_stream(BytesIO(body)).read_next_batch_with_custom_metadata()
    md = rb.custom_metadata
    assert md is None or md.get(PROTOCOL_VERSION_KEY) is None


def test_build_error_stream_encodes_exception() -> None:
    """build_error_stream carries the EXCEPTION marker + message the client decodes."""
    body = build_error_stream(PermissionError("denied: nope"))
    rb = ipc.open_stream(BytesIO(body)).read_next_batch_with_custom_metadata()
    md = rb.custom_metadata
    assert md is not None
    assert md.get(b"vgi_rpc.log_level") == b"EXCEPTION"
    assert b"denied: nope" in md.get(b"vgi_rpc.log_message", b"")


def test_build_error_stream_defaults_to_empty_schema() -> None:
    """The error stream uses an empty schema when none is supplied."""
    body = build_error_stream(ValueError("boom"))
    assert ipc.open_stream(BytesIO(body)).schema.names == []


def test_find_state_token_in_single_stream_request() -> None:
    """An exchange request's single batch carries the token."""
    assert find_state_token(_stream(pa.schema([]), token=b"TOK")) == b"TOK"


def test_find_state_token_walks_concatenated_response_streams() -> None:
    """A producer response is header-stream ++ data-stream; the token is in the latter."""
    header = _stream(pa.schema([pa.field("execution_id", pa.string())]))  # no token
    data = _stream(pa.schema([pa.field("v", pa.int64())]), token=b"TOK2")
    assert find_state_token(header + data) == b"TOK2"


def test_find_state_token_absent_or_unparseable() -> None:
    """Missing token or junk bytes return None rather than raising."""
    assert find_state_token(b"") is None
    assert find_state_token(_stream(pa.schema([]))) is None
    assert find_state_token(b"not-an-ipc-stream") is None
