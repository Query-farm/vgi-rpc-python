"""Tests for HTTP-specific behavior in vgi_rpc.http.

Functional correctness (unary, stream, bidi, type fidelity, logs, errors)
is already covered by the ``[http]`` parametrization in ``test_rpc.py``.
This file tests only things unique to the HTTP transport layer.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from io import BytesIO
from typing import Any

import falcon
import pyarrow as pa
import pytest
from aioresponses import CallbackResult
from aioresponses import aioresponses as aioresponses_ctx
from pyarrow import ipc

from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.http import (
    _ARROW_CONTENT_TYPE,
    HttpStreamSession,
    _SyncTestClient,
    _SyncTestResponse,
    http_connect,
    make_sync_client,
    make_wsgi_app,
)
from vgi_rpc.log import Message
from vgi_rpc.rpc import AnnotatedBatch, RpcError, RpcServer, _dispatch_log_or_error, _drain_stream

from .test_external import (
    MockStorage,
    _ExternalService,
    _ExternalServiceImpl,
)
from .test_rpc import (
    RpcFixtureService,
    RpcFixtureServiceImpl,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_URL = "http://test"


@pytest.fixture
def client() -> Iterator[_SyncTestClient]:
    """Create a sync Falcon test client with proper cleanup."""
    c = make_sync_client(RpcServer(RpcFixtureService, RpcFixtureServiceImpl()), signing_key=b"test-key")
    yield c
    c.close()


@pytest.fixture
def resumable_client() -> Iterator[_SyncTestClient]:
    """Create a sync client with resumable streaming enabled (small limit to force continuations)."""
    c = make_sync_client(
        RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
        signing_key=b"test-key",
        max_stream_response_bytes=200,
    )
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Tests: make_wsgi_app
# ---------------------------------------------------------------------------


class TestMakeWsgiApp:
    """Tests for make_wsgi_app factory."""

    def test_returns_falcon_app(self) -> None:
        """make_wsgi_app returns a Falcon application."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server, signing_key=b"test-key")
        assert isinstance(app, falcon.App)


# ---------------------------------------------------------------------------
# Tests: HTTP-specific error cases
# ---------------------------------------------------------------------------


def _extract_rpc_error(resp: _SyncTestResponse) -> RpcError:
    """Parse an Arrow IPC error stream from a response body, return the RpcError."""
    reader = ipc.open_stream(BytesIO(resp.content))
    try:
        while True:
            batch, cm = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, cm)
    except RpcError as exc:
        _drain_stream(reader)
        return exc
    except StopIteration:
        pass
    raise AssertionError("Response body did not contain an RpcError")


class TestHttpErrorCases:
    """Tests for HTTP-specific error cases (status codes + Arrow IPC error bodies)."""

    def test_unknown_method_404(self, client: _SyncTestClient) -> None:
        """Unknown method returns 404 with a parseable Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/nonexistent",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 404
        err = _extract_rpc_error(resp)
        assert err.error_type == "AttributeError"
        assert "nonexistent" in err.error_message

    def test_non_bidi_on_bidi_endpoint_400(self, client: _SyncTestClient) -> None:
        """Non-bidi method on the /bidi endpoint returns 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/bidi",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "not a bidi stream" in err.error_message

    def test_malformed_body_bidi_init_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a bidi init endpoint return 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/bidi",
            content=b"garbage bytes",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "ArrowInvalid" in err.error_type

    def test_malformed_body_bidi_exchange_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a bidi exchange endpoint return 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/exchange",
            content=b"garbage bytes",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "ArrowInvalid" in err.error_type

    def test_wrong_content_type_415(self, client: _SyncTestClient) -> None:
        """Wrong Content-Type returns 415 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add",
            content=b"hello",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 415
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "Content-Type" in err.error_message

    def test_unary_on_exchange_400(self, client: _SyncTestClient) -> None:
        """Unary method on /exchange endpoint returns 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/exchange",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "does not support /exchange" in err.error_message


# ---------------------------------------------------------------------------
# Tests: Resumable server-stream over HTTP
# ---------------------------------------------------------------------------


class TestResumableServerStream:
    """Tests for resumable server-stream with continuation tokens."""

    def test_basic_resumable(self, resumable_client: _SyncTestClient) -> None:
        """All batches received across continuation boundaries."""
        with http_connect(RpcFixtureService, client=resumable_client) as proxy:
            stream = proxy.generate(count=10)
            assert isinstance(stream, HttpStreamSession)
            batches = list(stream)
            assert len(batches) == 10
            values = [ab.batch.column("i")[0].as_py() for ab in batches]
            assert values == list(range(10))

    def test_resumable_with_logs(self, resumable_client: _SyncTestClient) -> None:
        """Log messages are delivered across continuation boundaries."""
        logs: list[Message] = []
        with http_connect(RpcFixtureService, client=resumable_client, on_log=logs.append) as proxy:
            batches = list(proxy.generate_with_logs(count=5))
            assert len(batches) == 5
            # Pre-stream log + per-batch logs
            assert len(logs) >= 5
            # Check that log messages reference batch numbers
            batch_log_msgs = [m for m in logs if "generating batch" in m.message]
            assert len(batch_log_msgs) == 5

    def test_error_during_continuation(self, resumable_client: _SyncTestClient) -> None:
        """Error in produce() during continuation propagates as RpcError."""
        with (
            http_connect(RpcFixtureService, client=resumable_client) as proxy,
            pytest.raises(RpcError, match="stream boom"),
        ):
            list(proxy.fail_stream())

    def test_empty_stream_resumable(self, resumable_client: _SyncTestClient) -> None:
        """Empty stream (count=0) with resumable enabled returns no batches."""
        with http_connect(RpcFixtureService, client=resumable_client) as proxy:
            batches = list(proxy.generate(count=0))
            assert len(batches) == 0

    def test_disabled_by_default(self, client: _SyncTestClient) -> None:
        """Without max_stream_response_bytes, no continuation tokens are used."""
        with http_connect(RpcFixtureService, client=client) as proxy:
            stream = proxy.generate(count=5)
            assert isinstance(stream, HttpStreamSession)
            batches = list(stream)
            assert len(batches) == 5

    def test_tampered_token_400(self, resumable_client: _SyncTestClient) -> None:
        """Tampered continuation token returns error."""
        import pyarrow as pa

        from vgi_rpc.metadata import STATE_KEY
        from vgi_rpc.rpc import _EMPTY_SCHEMA
        from vgi_rpc.utils import empty_batch

        # Send a fake exchange request with a tampered token
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({STATE_KEY: b"tampered-token-data"})
        with ipc.new_stream(req_buf, _EMPTY_SCHEMA) as writer:
            writer.write_batch(empty_batch(_EMPTY_SCHEMA), custom_metadata=state_md)

        resp = resumable_client.post(
            f"{_BASE_URL}/vgi/generate/exchange",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "token" in err.error_message.lower() or "Malformed" in err.error_message

    def test_single_batch_no_continuation(self) -> None:
        """A stream with one batch doesn't need continuation even with small limit."""
        c = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test-key",
            max_stream_response_bytes=200,
        )
        with http_connect(RpcFixtureService, client=c) as proxy:
            batches = list(proxy.generate(count=1))
            assert len(batches) == 1
            assert batches[0].batch.column("i")[0].as_py() == 0
        c.close()


# ---------------------------------------------------------------------------
# Tests: ExternalStorage over HTTP transport
# ---------------------------------------------------------------------------


class TestHttpExternalStorage:
    """Integration tests for ExternalLocation over HTTP transport."""

    def _make_config(self, storage: MockStorage, threshold: int = 100) -> ExternalLocationConfig:
        """Create an ExternalLocationConfig with low threshold for testing."""
        return ExternalLocationConfig(
            storage=storage,
            threshold_bytes=threshold,
            max_retries=0,
            retry_delay_seconds=0.0,
        )

    def _make_client(
        self, config: ExternalLocationConfig
    ) -> _SyncTestClient:
        """Create a _SyncTestClient wrapping an RpcServer with external storage."""
        server = RpcServer(_ExternalService, _ExternalServiceImpl(), external_location=config)
        return make_sync_client(server, signing_key=b"test-key")

    def _mock_aio_dynamic(self, storage: MockStorage, mock: aioresponses_ctx) -> None:
        """Register pattern-based HEAD + GET callbacks that serve from MockStorage dynamically."""
        pattern = re.compile(r"^https://mock\.storage/.*$")

        def _head_callback(url_: Any, **kwargs: Any) -> CallbackResult:
            url_str = str(url_)
            if url_str not in storage.data:
                return CallbackResult(status=404)
            body = storage.data[url_str]
            return CallbackResult(status=200, headers={"Content-Length": str(len(body))})

        def _get_callback(url_: Any, **kwargs: Any) -> CallbackResult:
            url_str = str(url_)
            if url_str not in storage.data:
                return CallbackResult(status=404)
            body = storage.data[url_str]
            return CallbackResult(status=200, body=body, headers={"Content-Length": str(len(body))})

        for _ in range(50):
            mock.head(pattern, callback=_head_callback)
            mock.get(pattern, callback=_get_callback)

    def test_unary_large_externalized(self) -> None:
        """Large unary result is externalized by server and resolved by client."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10)
        client = self._make_client(config)

        with aioresponses_ctx() as mock:
            self._mock_aio_dynamic(storage, mock)
            with http_connect(_ExternalService, client=client, external_location=config) as proxy:
                result = proxy.echo_large(data="x" * 200)

        assert result == "x" * 200
        assert len(storage.data) >= 1
        client.close()

    def test_unary_small_inline(self) -> None:
        """Small unary result stays inline (not externalized)."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10_000_000)
        client = self._make_client(config)

        with http_connect(_ExternalService, client=client, external_location=config) as proxy:
            result = proxy.echo_large(data="hello")

        assert result == "hello"
        assert len(storage.data) == 0
        client.close()

    def test_server_stream_large_externalized(self) -> None:
        """Large server-stream batches are externalized and resolved by client."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)
        client = self._make_client(config)

        received_logs: list[Message] = []

        with aioresponses_ctx() as mock:
            self._mock_aio_dynamic(storage, mock)
            with http_connect(
                _ExternalService, client=client, on_log=received_logs.append, external_location=config
            ) as proxy:
                batches = list(proxy.stream_large(count=3, size=50))

        assert len(batches) == 3
        for ab in batches:
            assert ab.batch.num_rows == 50
        assert len(received_logs) == 3
        assert all(m.level.name == "INFO" for m in received_logs)
        client.close()

    def test_server_stream_with_logs(self) -> None:
        """Log messages from externalized stream batches are dispatched via on_log."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)
        client = self._make_client(config)

        received_logs: list[Message] = []

        with aioresponses_ctx() as mock:
            self._mock_aio_dynamic(storage, mock)
            with http_connect(
                _ExternalService, client=client, on_log=received_logs.append, external_location=config
            ) as proxy:
                batches = list(proxy.stream_large(count=2, size=50))

        assert len(batches) == 2
        assert len(received_logs) == 2
        assert "producing batch 0" in received_logs[0].message
        assert "producing batch 1" in received_logs[1].message
        client.close()

    def test_bidi_large_output_externalized(self) -> None:
        """Large bidi output is externalized by server and resolved by client."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)
        client = self._make_client(config)

        with aioresponses_ctx() as mock:
            self._mock_aio_dynamic(storage, mock)
            with http_connect(_ExternalService, client=client, external_location=config) as proxy:
                bidi = proxy.bidi_large(factor=2.0)
                with bidi:
                    input_batch = AnnotatedBatch.from_pydict(
                        {"value": [float(i) for i in range(50)]},
                        schema=pa.schema([pa.field("value", pa.float64())]),
                    )
                    result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 50
        assert result.batch.column("value")[0].as_py() == 0.0
        assert result.batch.column("value")[1].as_py() == 2.0
        client.close()

    def test_bidi_large_input_externalized(self) -> None:
        """Large bidi input is externalized by client and resolved by server."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)
        client = self._make_client(config)

        with aioresponses_ctx() as mock:
            self._mock_aio_dynamic(storage, mock)
            with http_connect(_ExternalService, client=client, external_location=config) as proxy:
                bidi = proxy.bidi_large(factor=3.0)
                with bidi:
                    input_batch = AnnotatedBatch.from_pydict(
                        {"value": [float(i) for i in range(50)]},
                        schema=pa.schema([pa.field("value", pa.float64())]),
                    )
                    result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 50
        assert result.batch.column("value")[2].as_py() == 6.0
        client.close()

    def test_small_batches_inline(self) -> None:
        """Small batches remain inline (not externalized)."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10_000_000)
        client = self._make_client(config)

        with http_connect(_ExternalService, client=client, external_location=config) as proxy:
            batches = list(proxy.stream_large(count=2, size=2))

        assert len(batches) == 2
        for ab in batches:
            assert ab.batch.num_rows == 2
        assert len(storage.data) == 0
        client.close()
