"""Tests for vgi_rpc.http — HTTP transport for Arrow IPC RPC."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from vgi_rpc.http import (
    HttpBidiSession,
    _HttpProxy,
    _SyncASGIClient,
    http_connect,
    make_asgi_app,
    make_sync_client,
)
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    RpcError,
    RpcServer,
    StreamSession,
)

from .test_rpc import (
    Color,
    DescribeResult,
    RpcFixtureService,
    RpcFixtureServiceImpl,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_URL = "http://test"


@pytest.fixture
def client() -> Iterator[_SyncASGIClient]:
    """Create a sync ASGI test client with proper cleanup."""
    c = make_sync_client(RpcServer(RpcFixtureService, RpcFixtureServiceImpl()), base_url=_BASE_URL)
    yield c
    c.close()


@pytest.fixture
def svc(client: _SyncASGIClient) -> Iterator[_HttpProxy]:
    """Create an RPC proxy over HTTP with proper cleanup."""
    with http_connect(RpcFixtureService, _BASE_URL, client=client) as proxy:
        yield proxy


# ---------------------------------------------------------------------------
# Tests: make_asgi_app
# ---------------------------------------------------------------------------


class TestMakeAsgiApp:
    """Tests for make_asgi_app factory."""

    def test_returns_starlette_app(self) -> None:
        """make_asgi_app returns a Starlette application."""
        from starlette.applications import Starlette

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_asgi_app(server)
        assert isinstance(app, Starlette)


# ---------------------------------------------------------------------------
# Tests: Unary calls over HTTP
# ---------------------------------------------------------------------------


class TestHttpUnary:
    """Tests for unary RPC calls over HTTP transport."""

    def test_add(self, svc: _HttpProxy) -> None:
        """Unary float addition roundtrips through HTTP."""
        assert svc.add(a=1.5, b=2.5) == pytest.approx(4.0)

    def test_greet(self, svc: _HttpProxy) -> None:
        """Unary string return roundtrips through HTTP."""
        assert svc.greet(name="World") == "Hello, World!"

    def test_noop(self, svc: _HttpProxy) -> None:
        """Unary None-returning method returns None."""
        assert svc.noop() is None

    def test_dataclass_result(self, svc: _HttpProxy) -> None:
        """Unary method returning ArrowSerializableDataclass roundtrips correctly."""
        result = svc.describe()
        assert isinstance(result, DescribeResult)
        assert result.output_schema == pa.schema([pa.field("x", pa.int64())])
        assert result.sample_batch.to_pydict() == {"x": [1, 2, 3]}

    def test_dataclass_param(self, svc: _HttpProxy) -> None:
        """Unary method accepting ArrowSerializableDataclass param roundtrips correctly."""
        desc = DescribeResult(
            output_schema=pa.schema([pa.field("a", pa.float64()), pa.field("b", pa.float64())]),
            sample_batch=pa.RecordBatch.from_pydict({"x": [10, 20]}),
        )
        assert svc.inspect(result=desc) == "2:2"

    def test_error_propagation(self, svc: _HttpProxy) -> None:
        """Errors from unary methods are raised as RpcError."""
        with pytest.raises(RpcError, match="unary boom") as exc_info:
            svc.fail_unary()
        assert exc_info.value.error_type == "ValueError"

    def test_multiple_calls(self, svc: _HttpProxy) -> None:
        """Multiple unary calls on the same connection work."""
        assert svc.add(a=1.0, b=2.0) == pytest.approx(3.0)
        assert svc.add(a=10.0, b=20.0) == pytest.approx(30.0)
        assert svc.greet(name="Alice") == "Hello, Alice!"

    def test_error_then_success(self, svc: _HttpProxy) -> None:
        """Error followed by successful call works."""
        with pytest.raises(RpcError, match="unary boom"):
            svc.fail_unary()
        assert svc.add(a=1.0, b=2.0) == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Tests: Type fidelity over HTTP
# ---------------------------------------------------------------------------


class TestHttpTypeFidelity:
    """Tests for type fidelity through HTTP transport."""

    def test_enum_roundtrip(self, svc: _HttpProxy) -> None:
        """Enum result roundtrips through HTTP."""
        assert svc.echo_color(color=Color.BLUE) is Color.BLUE

    def test_dict_roundtrip(self, svc: _HttpProxy) -> None:
        """Dict result roundtrips through HTTP."""
        result = svc.echo_mapping(mapping={"x": 42, "y": 99})
        assert isinstance(result, dict)
        assert result == {"x": 42, "y": 99}

    def test_frozenset_roundtrip(self, svc: _HttpProxy) -> None:
        """Frozenset result roundtrips through HTTP."""
        result = svc.echo_tags(tags=frozenset({1, 2, 3}))
        assert isinstance(result, frozenset)
        assert result == frozenset({1, 2, 3})


# ---------------------------------------------------------------------------
# Tests: Server stream over HTTP
# ---------------------------------------------------------------------------


class TestHttpServerStream:
    """Tests for server-stream RPC calls over HTTP transport."""

    def test_basic_stream(self, svc: _HttpProxy) -> None:
        """Server stream yields expected batches over HTTP."""
        batches = list(svc.generate(count=3))
        assert len(batches) == 3
        assert batches[0].batch.column("i")[0].as_py() == 0
        assert batches[1].batch.column("value")[0].as_py() == 10
        assert batches[2].batch.column("i")[0].as_py() == 2

    def test_empty_stream(self, svc: _HttpProxy) -> None:
        """Server stream with count=0 yields no batches."""
        assert list(svc.generate(count=0)) == []

    def test_stream_returns_stream_session(self, svc: _HttpProxy) -> None:
        """Server stream returns a StreamSession."""
        stream = svc.generate(count=2)
        assert isinstance(stream, StreamSession)
        assert len(list(stream)) == 2

    def test_stream_error_propagates(self, svc: _HttpProxy) -> None:
        """Errors from server stream methods propagate as RpcError."""
        with pytest.raises(RpcError, match="stream boom"):
            list(svc.fail_stream())

    def test_stream_with_logs(self, client: _SyncASGIClient) -> None:
        """Server stream delivers log messages to on_log callback."""
        logs: list[Message] = []
        with http_connect(RpcFixtureService, _BASE_URL, client=client, on_log=logs.append) as svc:
            batches = list(svc.generate_with_logs(count=2))
            assert len(batches) == 2
            # 1 pre-stream log + 2 per-batch logs
            assert len(logs) == 3
            assert logs[0].message == "pre-stream log"


# ---------------------------------------------------------------------------
# Tests: Bidi stream over HTTP
# ---------------------------------------------------------------------------


class TestHttpBidiStream:
    """Tests for bidi-stream RPC calls over HTTP transport."""

    def test_init_and_single_exchange(self, svc: _HttpProxy) -> None:
        """Bidi init + single exchange works over HTTP."""
        session = svc.transform(factor=2.0)
        assert isinstance(session, HttpBidiSession)

        input1 = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))
        output1 = session.exchange(input1)
        assert output1.batch.column("value").to_pylist() == [2.0, 4.0, 6.0]
        session.close()

    def test_multi_exchange(self, svc: _HttpProxy) -> None:
        """Multiple exchanges on the same bidi session work."""
        with svc.transform(factor=3.0) as session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert out1.batch.column("value").to_pylist() == [3.0, 6.0]

            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert out2.batch.column("value").to_pylist() == [30.0]

            out3 = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
            assert out3.batch.column("value").to_pylist() == [15.0]

    def test_state_mutation_across_exchanges(self, svc: _HttpProxy) -> None:
        """Bidi state is carried across exchanges (FailBidiMidState has a counter)."""
        session = svc.fail_bidi_mid(factor=2.0)

        # First exchange succeeds (count=0 -> count=1)
        out = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
        assert out.batch.column("value").to_pylist() == [10.0]

        # Second exchange fails (count=1 -> error)
        with pytest.raises(RpcError, match="bidi boom"):
            session.exchange(AnnotatedBatch.from_pydict({"value": [6.0]}))

    def test_bidi_context_manager(self, svc: _HttpProxy) -> None:
        """HttpBidiSession works as a context manager."""
        with svc.transform(factor=2.0) as session:
            output = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
            assert output.batch.column("value").to_pylist() == [10.0]

    def test_bidi_with_logs(self, client: _SyncASGIClient) -> None:
        """Bidi stream delivers log messages during exchange."""
        logs: list[Message] = []
        with http_connect(RpcFixtureService, _BASE_URL, client=client, on_log=logs.append) as svc:
            with svc.transform_with_logs(factor=2.0) as session:
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
                session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            assert len(logs) == 2
            assert all(m.level == Level.INFO for m in logs)
            assert "factor=2.0" in logs[0].message


# ---------------------------------------------------------------------------
# Tests: Error cases
# ---------------------------------------------------------------------------


class TestHttpErrorCases:
    """Tests for HTTP-specific error cases."""

    def test_unknown_method_404(self, client: _SyncASGIClient) -> None:
        """Unknown method returns 404."""
        resp = client.post(
            f"{_BASE_URL}/vgi/nonexistent",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 404

    def test_non_bidi_on_bidi_endpoint_400(self, client: _SyncASGIClient) -> None:
        """Non-bidi method on the /bidi endpoint returns 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/bidi",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Tests: on_log callback for all method types
# ---------------------------------------------------------------------------


class TestHttpLogDelivery:
    """Tests for on_log callback delivery across all method types."""

    def test_unary_log_delivery(self, client: _SyncASGIClient) -> None:
        """Unary method emits logs that are delivered via on_log callback."""
        logs: list[Message] = []
        with http_connect(RpcFixtureService, _BASE_URL, client=client, on_log=logs.append) as svc:
            result = svc.greet_with_logs(name="Alice")
            assert result == "Hello, Alice!"
            assert len(logs) == 2
            assert logs[0].level == Level.INFO
            assert logs[0].message == "greeting Alice"
            assert logs[1].level == Level.DEBUG
            assert logs[1].message == "debug detail"

    def test_unary_log_with_extras(self, client: _SyncASGIClient) -> None:
        """Message.extra kwargs are preserved through HTTP."""
        logs: list[Message] = []
        with http_connect(RpcFixtureService, _BASE_URL, client=client, on_log=logs.append) as svc:
            svc.greet_with_logs(name="Bob")
            debug_msg = [m for m in logs if m.level == Level.DEBUG][0]
            assert debug_msg.extra is not None
            assert debug_msg.extra["detail"] == "extra-info"

    def test_logs_silently_discarded_without_on_log(self, svc: _HttpProxy) -> None:
        """Log-emitting methods work fine when no on_log callback is provided."""
        result = svc.greet_with_logs(name="Charlie")
        assert result == "Hello, Charlie!"
