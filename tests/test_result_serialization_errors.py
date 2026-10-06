"""A value the server cannot serialize is answered with an error, not a dead connection.

Covers issue #55 and the failures of the same shape found alongside it. An
implementation can hand the framework something that fails only when the
framework serializes it: a unary result that does not fit its declared type
(``-> int`` returning a ``str``), a stream header that does not fit its header
type, a stream method that returns no ``Stream``, a ``call_state`` that is not
serializable, a ``ctx.client_log`` extra that is not JSON, an ``ExternalRef``
whose URL cannot be encoded, an exception whose ``__str__`` raises, or an
invalid cookie. Each is the method's error: the client must receive a typed
``RpcError`` and the same connection must keep serving.

Before the fix each of these was serialized outside the dispatcher's guard. On
the pipe transport the response closed early (the client read a bare
``StopIteration`` or blocked forever) and the exception ended the serve loop;
over HTTP the client got an unhandled Falcon 500 with a non-Arrow body. Every
test therefore follows the failing call with an ordinary call on the same
connection -- the assertion the original bugs fail.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol

import httpx2
import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc import (
    ArrowSerializableDataclass,
    OutputCollector,
    RpcError,
    RpcServer,
    Stream,
    StreamState,
    serve_pipe,
)
from vgi_rpc.external import ExternalRef
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.log import Level
from vgi_rpc.rpc import AnnotatedBatch, CallContext, rpc_methods
from vgi_rpc.rpc._wire import _read_unary_response
from vgi_rpc.utils import IpcValidation, ValidatedReader, new_ipc_stream

_SCHEMA = pa.schema([pa.field("value", pa.int64())])


@dataclass(frozen=True)
class _Header(ArrowSerializableDataclass):
    total: int


@dataclass
class _OneBatchState(StreamState):
    """Emits a single row, then finishes."""

    done: bool = False

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one row on the first tick, finish on the second."""
        if self.done:
            out.finish()
            return
        self.done = True
        out.emit_pydict({"value": [1]})


@dataclass
class _EchoState(StreamState):
    """Exchange state that echoes its input."""

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Echo the input batch."""
        out.emit(input.batch)


class _UnprintableError(Exception):
    """An exception whose ``__str__`` itself raises."""

    def __str__(self) -> str:
        raise RuntimeError("__str__ failed")


class _Service(Protocol):
    def wrong_scalar(self) -> int: ...

    def wrong_list(self) -> list[int]: ...

    def bad_ref(self) -> int: ...

    def unprintable_error(self) -> int: ...

    def bad_header(self) -> Stream[StreamState, _Header]: ...

    def none_header(self) -> Stream[StreamState, _Header]: ...

    def not_a_stream(self) -> Stream[StreamState]: ...

    def bad_output_schema(self) -> Stream[StreamState]: ...

    def init_log_producer(self) -> Stream[StreamState]: ...

    def init_log_with_header(self) -> Stream[StreamState, _Header]: ...

    def init_log_exchange(self) -> Stream[StreamState]: ...

    def bad_call_state(self) -> Stream[StreamState]: ...

    def bad_same_site(self) -> int: ...

    def bad_cookie_name(self) -> int: ...

    def ok(self) -> int: ...


class _BrokenImpl:
    """Each method breaks its own contract, except ``ok``."""

    def wrong_scalar(self) -> int:
        return "not_an_integer"  # type: ignore[return-value]

    def wrong_list(self) -> list[int]:
        return [1, "two", 3]  # type: ignore[list-item]

    def bad_ref(self) -> int:
        return ExternalRef(url="https://storage.test/\ud800")  # type: ignore[return-value]

    def unprintable_error(self) -> int:
        raise _UnprintableError

    def bad_header(self) -> Stream[_OneBatchState, _Header]:
        return Stream(output_schema=_SCHEMA, state=_OneBatchState(), header=_Header(total="x"))  # type: ignore[arg-type]

    def none_header(self) -> Stream[_OneBatchState, _Header]:
        return Stream(output_schema=_SCHEMA, state=_OneBatchState(), header=None)

    def not_a_stream(self) -> Stream[_OneBatchState]:
        return None  # type: ignore[return-value]

    def bad_output_schema(self) -> Stream[_OneBatchState]:
        return Stream(output_schema="not a schema", state=_OneBatchState())  # type: ignore[arg-type]

    def init_log_producer(self, ctx: CallContext) -> Stream[_OneBatchState]:
        ctx.client_log(Level.INFO, "starting", payload=object())  # type: ignore[arg-type]
        return Stream(output_schema=_SCHEMA, state=_OneBatchState())

    def init_log_with_header(self, ctx: CallContext) -> Stream[_OneBatchState, _Header]:
        ctx.client_log(Level.INFO, "starting", payload=object())  # type: ignore[arg-type]
        return Stream(output_schema=_SCHEMA, state=_OneBatchState(), header=_Header(total=1))

    def init_log_exchange(self, ctx: CallContext) -> Stream[_EchoState]:
        ctx.client_log(Level.INFO, "starting", payload=object())  # type: ignore[arg-type]
        return Stream(output_schema=_SCHEMA, state=_EchoState(), input_schema=_SCHEMA)

    def bad_call_state(self) -> Stream[_OneBatchState]:
        return Stream(output_schema=_SCHEMA, state=_OneBatchState(), call_state=object())  # type: ignore[arg-type]

    def bad_same_site(self, ctx: CallContext) -> int:
        ctx.set_cookie("session", "abc", same_site="bogus")
        return 1

    def bad_cookie_name(self, ctx: CallContext) -> int:
        ctx.set_cookie("bad name;", "abc")
        return 1

    def ok(self) -> int:
        return 7


ConnFactory = Callable[[], contextlib.AbstractContextManager[Any]]


@contextlib.contextmanager
def _pipe() -> Iterator[Any]:
    with serve_pipe(_Service, _BrokenImpl()) as proxy:
        yield proxy


@contextlib.contextmanager
def _http() -> Iterator[Any]:
    client = make_sync_client(RpcServer(_Service, _BrokenImpl()), token_key=b"test")
    with http_connect(_Service, client=client) as proxy:
        yield proxy


@pytest.fixture(params=["pipe", "http"])
def connect(request: pytest.FixtureRequest) -> ConnFactory:
    """Open a ``_Service`` proxy over the parametrised transport."""
    return _pipe if request.param == "pipe" else _http


def _drain(proxy: Any, method: str) -> None:
    """Call a stream method and consume it to the end."""
    if method == "init_log_exchange":
        with getattr(proxy, method)() as session:
            session.exchange(AnnotatedBatch(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)))
        return
    for _ in getattr(proxy, method)():
        pass


class TestWronglyTypedUnaryResult:
    """A unary result that does not fit the declared return type."""

    @pytest.mark.parametrize("method", ["wrong_scalar", "wrong_list"])
    def test_raises_rpc_error_and_connection_survives(self, connect: ConnFactory, method: str) -> None:
        """The client gets a typed RpcError and the next call on the same connection succeeds."""
        with connect() as proxy:
            with pytest.raises(RpcError) as exc_info:
                getattr(proxy, method)()
            assert exc_info.value.error_type == "ArrowInvalid"
            assert proxy.ok() == 7

    def test_repeated_failures_do_not_wedge_the_connection(self, connect: ConnFactory) -> None:
        """Alternating failing and passing calls all answer correctly."""
        with connect() as proxy:
            for _ in range(3):
                with pytest.raises(RpcError):
                    proxy.wrong_scalar()
                assert proxy.ok() == 7

    def test_access_log_records_the_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """The failed call is logged as an error, not as ``ok``.

        The passing call is the positive control: it proves records were
        captured at all, so the error assertion cannot pass on an empty log.
        """
        with caplog.at_level(logging.INFO, logger="vgi_rpc.access"), serve_pipe(_Service, _BrokenImpl()) as proxy:
            assert proxy.ok() == 7
            with pytest.raises(RpcError):
                proxy.wrong_scalar()
        status = {r.__dict__["method"]: r.__dict__["status"] for r in caplog.records if r.name == "vgi_rpc.access"}
        assert status == {"ok": "ok", "wrong_scalar": "error"}


class TestUnaryErrorPath:
    """Failures while writing a unary answer other than a mistyped result."""

    def test_unwritable_external_ref(self, connect: ConnFactory) -> None:
        """An ExternalRef whose URL cannot be encoded is answered as an error."""
        with connect() as proxy:
            with pytest.raises(RpcError) as exc_info:
                proxy.bad_ref()
            assert exc_info.value.error_type == "UnicodeEncodeError"
            assert proxy.ok() == 7

    def test_exception_whose_str_raises(self, connect: ConnFactory) -> None:
        """Reporting an exception does not depend on its ``__str__`` succeeding."""
        with connect() as proxy:
            with pytest.raises(RpcError) as exc_info:
                proxy.unprintable_error()
            assert exc_info.value.error_type == "_UnprintableError"
            assert "<exception str() failed>" in exc_info.value.error_message
            assert proxy.ok() == 7


class TestStreamInitFailures:
    """Failures between a stream method returning and its first output batch."""

    @pytest.mark.parametrize(
        ("method", "error_type"),
        [
            ("bad_header", "ArrowInvalid"),
            ("none_header", "TypeError"),
            ("not_a_stream", "TypeError"),
            ("bad_output_schema", "TypeError"),
            ("init_log_producer", "TypeError"),
            ("init_log_with_header", "TypeError"),
            ("init_log_exchange", "TypeError"),
        ],
    )
    def test_raises_rpc_error_and_connection_survives(self, connect: ConnFactory, method: str, error_type: str) -> None:
        """Opening the stream raises a typed RpcError; the connection keeps serving."""
        with connect() as proxy:
            with pytest.raises(RpcError) as exc_info:
                _drain(proxy, method)
            assert exc_info.value.error_type == error_type
            assert proxy.ok() == 7

    def test_unserializable_call_state_over_http(self) -> None:
        """HTTP seals ``call_state`` into the call token; failing to is the method's error.

        Pipe never serializes ``call_state``, so the case is HTTP-only.
        """
        with _http() as proxy:
            with pytest.raises(RpcError) as exc_info:
                _drain(proxy, "bad_call_state")
            assert exc_info.value.error_type == "AttributeError"
            assert proxy.ok() == 7


class TestInvalidCookie:
    """``ctx.set_cookie`` rejects a cookie the HTTP response could not carry."""

    @pytest.mark.parametrize(
        ("method", "match"),
        [("bad_same_site", "same_site must be"), ("bad_cookie_name", "Illegal cookie name")],
    )
    def test_raises_rpc_error_and_connection_survives(self, method: str, match: str) -> None:
        """The bad cookie raises inside the method, so the call answers a typed error."""
        with _http() as proxy:
            with pytest.raises(RpcError) as exc_info:
                getattr(proxy, method)()
            assert exc_info.value.error_type == "ValueError"
            assert match in exc_info.value.error_message
            assert proxy.ok() == 7


class TestClientScreensMalformedResponses:
    """The client raises RpcError, never a raw low-level exception, for a malformed response."""

    def test_empty_unary_response_raises_protocol_error(self) -> None:
        """A schema-then-EOS response raises RpcError, never a bare StopIteration."""
        info = rpc_methods(_Service)["ok"]
        buf = BytesIO()
        with new_ipc_stream(buf, info.result_schema):
            pass
        buf.seek(0)
        reader = ValidatedReader(ipc.open_stream(buf), IpcValidation.FULL)
        with pytest.raises(RpcError) as exc_info:
            _read_unary_response(reader, info, None)
        assert exc_info.value.error_type == "ProtocolError"
        assert "ended without a result batch" in exc_info.value.error_message

    def test_non_arrow_error_body_on_header_stream_init(self) -> None:
        """A proxy's HTML 502 on a header-declaring stream init raises HttpError, not ArrowInvalid."""

        def handler(_request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(502, headers={"Content-Type": "text/html"}, content=b"<html>Bad Gateway</html>")

        with (
            httpx2.Client(base_url="http://worker.test", transport=httpx2.MockTransport(handler)) as client,
            http_connect(_Service, client=client, accepted_max_response_bytes=None) as proxy,
            pytest.raises(RpcError) as exc_info,
        ):
            _drain(proxy, "bad_header")
        assert exc_info.value.error_type == "HttpError"
        assert "HTTP 502" in exc_info.value.error_message
