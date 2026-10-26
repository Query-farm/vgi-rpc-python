# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bounded typed IPC values must consume their complete single-record envelope."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import ClassVar, Protocol, Self

import falcon.testing
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from vgi_rpc.http import make_wsgi_app
from vgi_rpc.rpc import PipeTransport, RpcConnection, RpcError, RpcServer, rpc_methods
from vgi_rpc.rpc._wire import _deserialize_value
from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    IPCError,
    IpcValidation,
    deserialize_record_batch,
    read_single_record_batch,
    serialize_record_batch_bytes,
)
from vgi_rpc.wire import write_request, write_unary_result

_EOS = b"\xff\xff\xff\xff\0\0\0\0"


@dataclass(frozen=True)
class _Request(ArrowSerializableDataclass):
    """A minimal typed request whose first batch would otherwise be accepted."""

    value: int


class _FramingProtocol(Protocol):
    """Expose the bounded value through public HTTP and pipe dispatch."""

    protocol_name: ClassVar[str] = "test.Framing"

    def accept(self, request: _Request) -> int:
        """Return the accepted request value."""
        ...


class _ResultProtocol(Protocol):
    """Exercise the same typed decoder on the receiving client."""

    def retrieve(self) -> _Request:
        """Return a typed record."""
        ...


class _FramingImpl:
    """Record whether malformed inputs reached application code."""

    def __init__(self) -> None:
        """Start with no dispatched calls."""
        self.calls: list[int] = []

    def accept(self, request: _Request) -> int:
        """Record and return the accepted value."""
        self.calls.append(request.value)
        return request.value


def _payload(*, batches: int = 1) -> bytes:
    """Construct the inner typed value without involving the RPC decoder."""
    batch = pa.record_batch([[42]], schema=_Request.ARROW_SCHEMA)
    out = BytesIO()
    with ipc.new_stream(out, batch.schema) as writer:
        for _ in range(batches):
            writer.write_batch(batch, custom_metadata={b"tag": b"preserved"})
    return out.getvalue()


def _ending(data: bytes, ending: str) -> bytes:
    """Replace a modern EOS with another supported Arrow stream ending."""
    assert data.endswith(_EOS)
    return data[:-8] + {"modern": _EOS, "legacy": b"\0\0\0\0", "eof": b""}[ending]


_MALFORMED = (
    "extra_batch",
    "concatenated_stream",
    "trailing_byte",
    "legacy_trailing_byte",
    "repeated_eos",
    "truncated_schema",
    "truncated_body",
    "truncated_next_message",
    *(f"truncated_eos_{length}" for length in range(1, 8)),
    *(f"truncated_legacy_eos_{length}" for length in range(1, 4)),
)


def _malformed(kind: str) -> bytes:
    """Build independent corruptions of a valid single-batch envelope."""
    data = _payload()
    if kind.startswith("truncated_eos_"):
        return data[:-8] + _EOS[: int(kind.rsplit("_", 1)[1])]
    if kind.startswith("truncated_legacy_eos_"):
        return data[:-8] + b"\0" * int(kind.rsplit("_", 1)[1])
    return {
        "extra_batch": _payload(batches=2),
        "concatenated_stream": data + data,
        "trailing_byte": data + b"x",
        "legacy_trailing_byte": _ending(data, "legacy") + b"x",
        "repeated_eos": data + _EOS,
        "truncated_schema": data[:12],
        "truncated_body": data[:-9],
        "truncated_next_message": data[:-8] + b"\xff\xff\xff\xff\x08\0\0\0x",
    }[kind]


@pytest.mark.parametrize("validation", list(IpcValidation))
@pytest.mark.parametrize("ending", ["modern", "legacy", "eof"])
def test_single_batch_accepts_arrow_endings(ending: str, validation: IpcValidation) -> None:
    """Complete valid streams preserve values and record custom metadata."""
    data = _ending(_payload(), ending)
    batch, metadata = deserialize_record_batch(data, validation)
    assert batch.to_pylist() == [{"value": 42}]
    assert metadata is not None and metadata[b"tag"] == b"preserved"
    assert _deserialize_value(data, _Request, validation) == _Request(42)


@pytest.mark.parametrize("validation", list(IpcValidation))
@pytest.mark.parametrize("kind", _MALFORMED)
def test_single_batch_rejects_incomplete_or_surplus_framing(kind: str, validation: IpcValidation) -> None:
    """Disabling buffer validation must never disable envelope framing checks."""
    data = _malformed(kind)
    with pytest.raises((IPCError, pa.ArrowException, ValueError)):
        deserialize_record_batch(data, validation)
    with pytest.raises((TypeError, ValueError)):
        _deserialize_value(data, _Request, validation)


@pytest.mark.parametrize("ending", ["modern", "legacy", "eof"])
def test_schema_only_and_empty_utility_values_remain_supported(ending: str) -> None:
    """The generic helper retains the cross-language empty-value convention."""
    empty, metadata = deserialize_record_batch(b"")
    assert empty.num_rows == empty.num_columns == 0
    assert metadata is None
    data = _ending(_payload(batches=0), ending)
    batch, metadata = deserialize_record_batch(data)
    assert batch.num_rows == 0 and batch.schema == _Request.ARROW_SCHEMA
    assert metadata is None
    with pytest.raises((IPCError, pa.ArrowException, ValueError)):
        deserialize_record_batch(data + b"x")


def test_dictionary_messages_do_not_count_as_extra_batches() -> None:
    """A dictionary and its single record batch remain a valid typed value."""
    dictionary = pa.array(["red", "green", "red"]).dictionary_encode()
    original = pa.record_batch([dictionary], names=["color"])
    result, _ = deserialize_record_batch(serialize_record_batch_bytes(original))
    assert result.equals(original)


def test_legacy_format_messages_and_eos_are_supported() -> None:
    """Legacy prefixes on schema and batch messages remain interoperable."""
    batch = pa.record_batch([[42]], schema=_Request.ARROW_SCHEMA)
    out = BytesIO()
    with ipc.new_stream(out, batch.schema, options=ipc.IpcWriteOptions(use_legacy_format=True)) as writer:
        writer.write_batch(batch)
    data = out.getvalue()
    assert data[:4] != b"\xff\xff\xff\xff"
    assert _deserialize_value(data, _Request) == _Request(42)


@pytest.mark.parametrize("validation", list(IpcValidation))
def test_parameter_decoder_honors_bytes_override_before_arrow(
    validation: IpcValidation, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A request type can reject its raw byte size before any Arrow decoding."""
    seen: list[IpcValidation] = []

    @dataclass(frozen=True)
    class BoundedRequest(_Request):
        """Apply an application-specific raw cap using the existing bytes hook."""

        @classmethod
        def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
            """Check size before delegating to the generic Arrow decoder."""
            seen.append(ipc_validation)
            if len(data) > 1024:
                raise ValueError("request exceeds raw byte limit")
            return super().deserialize_from_bytes(data, ipc_validation)

    def unexpected_decode(*args: object, **kwargs: object) -> None:
        """Fail immediately if the raw bound is checked too late."""
        pytest.fail("Arrow decoder ran before the request's raw byte limit")

    monkeypatch.setattr(ipc, "open_stream", unexpected_decode)
    with pytest.raises(ValueError, match="raw byte limit"):
        _deserialize_value(b"x" * 1025, BoundedRequest, validation)
    assert seen == [validation]


def test_stream_reader_leaves_following_transport_frame_available() -> None:
    """Strict bounded-value decoding must not consume the next pipe RPC frame."""
    stream = BytesIO(_payload() + _payload())
    first, _ = read_single_record_batch(stream)
    second, _ = read_single_record_batch(stream)
    assert first.equals(second)
    assert stream.tell() == len(stream.getvalue())


def _request(data: bytes) -> bytes:
    """Wrap arbitrary IPC bytes in an otherwise valid public RPC request."""
    schema = rpc_methods(_FramingProtocol)["accept"].params_schema
    return write_request("accept", schema, {"request": data}, protocol=_FramingProtocol.protocol_name)


@pytest.mark.parametrize("kind", _MALFORMED)
def test_pipe_rejects_bad_typed_value_then_dispatches_next_request(kind: str) -> None:
    """Reject before the handler and preserve the next request on the same pipe."""
    impl = _FramingImpl()
    server = RpcServer(_FramingProtocol, impl)
    output = BytesIO()
    transport = PipeTransport(BytesIO(_request(_malformed(kind)) + _request(_payload())), output)
    server.serve_one(transport)
    assert impl.calls == []
    server.serve_one(transport)
    assert impl.calls == [42]
    output.seek(0)
    with ipc.open_stream(output) as errors:
        first = list(errors.iter_batches_with_custom_metadata())
    assert any(md is not None and md.get(b"vgi_rpc.log_level") == b"EXCEPTION" for _, md in first)
    with ipc.open_stream(output) as results:
        assert results.read_next_batch().column("result")[0].as_py() == 42


@pytest.mark.parametrize("kind", _MALFORMED)
def test_http_rejects_bad_typed_value_as_client_error(kind: str) -> None:
    """Malformed wrapped IPC is HTTP 400, never handler execution or HTTP 500."""
    impl = _FramingImpl()
    client = falcon.testing.TestClient(make_wsgi_app(RpcServer(_FramingProtocol, impl), token_key=b"framing-test"))
    headers = {"content-type": "application/vnd.apache.arrow.stream"}
    path = "/test.Framing/accept"
    bad = client.simulate_post(path, body=_request(_malformed(kind)), headers=headers)
    assert bad.status_code == 400
    assert impl.calls == []
    good = client.simulate_post(path, body=_request(_payload()), headers=headers)
    assert good.status_code == 200
    assert impl.calls == [42]


@pytest.mark.parametrize("kind", ["extra_batch", "trailing_byte", "truncated_eos_4"])
def test_client_classifies_malformed_typed_result_as_transport_error(kind: str) -> None:
    """Typed result corruption keeps the client's established RpcError contract."""
    schema = rpc_methods(_ResultProtocol)["retrieve"].result_schema
    response = write_unary_result(schema, _malformed(kind))
    transport = PipeTransport(BytesIO(response), BytesIO())
    with RpcConnection(_ResultProtocol, transport) as client, pytest.raises(RpcError) as caught:
        client.retrieve()
    assert caught.value.error_type == "TransportError"
