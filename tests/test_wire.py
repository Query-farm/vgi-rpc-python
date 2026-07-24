"""Tests for the public wire-framing helpers (:mod:`vgi_rpc.wire`)."""

from __future__ import annotations

from io import BytesIO

import pyarrow as pa
import pyarrow.ipc as ipc

from vgi_rpc.metadata import PROTOCOL_VERSION_KEY, STATE_KEY
from vgi_rpc.wire import (
    build_error_stream,
    find_protocol_version,
    find_state_token,
    read_request,
    read_unary_result,
    write_request,
    write_unary_result,
)

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


def test_write_request_carries_extra_metadata() -> None:
    """extra_metadata rides the request batch so an intermediary can preserve it.

    A proxy that re-serializes a request (e.g. stripping an argument) passes the
    client's application metadata — notably the ``vgi.cache.*`` conditional-
    revalidation validators — through extra_metadata so the worker still sees it.
    """
    extra = {b"vgi.cache.if_none_match": b'"etag-123"', b"vgi.cache.if_modified_since": b"Mon"}
    body = write_request("init", _SCHEMA, {"request": b"x"}, protocol_version="2.3", extra_metadata=extra)
    rb = ipc.open_stream(BytesIO(body)).read_next_batch_with_custom_metadata()
    md = dict(rb.custom_metadata)
    assert md.get(b"vgi.cache.if_none_match") == b'"etag-123"'
    assert md.get(b"vgi.cache.if_modified_since") == b"Mon"
    # Framework keys still win / are present.
    assert md.get(PROTOCOL_VERSION_KEY) == b"2.3"


def test_write_request_extra_metadata_cannot_override_framework_keys() -> None:
    """A caller's extra_metadata never clobbers the method / version framing keys."""
    from vgi_rpc.metadata import RPC_METHOD_KEY

    body = write_request(
        "init",
        _SCHEMA,
        {"request": b"x"},
        protocol_version="2.3",
        extra_metadata={RPC_METHOD_KEY: b"forged", PROTOCOL_VERSION_KEY: b"9.9"},
    )
    rb = ipc.open_stream(BytesIO(body)).read_next_batch_with_custom_metadata()
    md = dict(rb.custom_metadata)
    assert md.get(RPC_METHOD_KEY) == b"init"
    assert md.get(PROTOCOL_VERSION_KEY) == b"2.3"


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


def test_find_protocol_version() -> None:
    """The stamped protocol_version is recovered; absent/junk yields None."""
    assert find_protocol_version(write_request("bind", _SCHEMA, {"request": b"x"}, protocol_version="3.1")) == "3.1"
    assert find_protocol_version(write_request("bind", _SCHEMA, {"request": b"x"})) is None
    assert find_protocol_version(b"junk") is None


def test_unary_result_round_trip() -> None:
    """write_unary_result + read_unary_result preserve the raw result payload."""
    envelope = pa.schema([pa.field("result", pa.binary())])
    parsed = read_unary_result(write_unary_result(envelope, b"serialized-response"))
    assert parsed is not None
    schema, result_bytes = parsed
    assert schema.names == ["result"]
    assert result_bytes == b"serialized-response"


def test_read_unary_result_none_for_non_result_stream() -> None:
    """A data batch without a 'result' column is not a unary result (None)."""
    schema = pa.schema([pa.field("x", pa.int64())])
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as w:
        w.write_batch(pa.record_batch({"x": [1]}, schema=schema))
    assert read_unary_result(buf.getvalue()) is None
