"""Tests for HTTP-specific behavior in vgi_rpc.http.

Functional correctness (unary, stream, bidi, type fidelity, logs, errors)
is already covered by the ``[http]`` parametrization in ``test_rpc.py``.
This file tests only things unique to the HTTP transport layer.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol, cast

import falcon
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from aioresponses import CallbackResult
from aioresponses import aioresponses as aioresponses_ctx
from pyarrow import ipc

from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.http import (
    _ARROW_CONTENT_TYPE,
    MAX_REQUEST_BYTES_HEADER,
    HttpServerCapabilities,
    HttpStreamSession,
    _SyncTestClient,
    _SyncTestResponse,
    http_capabilities,
    http_connect,
    make_sync_client,
    make_wsgi_app,
)
from vgi_rpc.log import Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    AuthContext,
    CallContext,
    OutputCollector,
    RpcError,
    RpcServer,
    Stream,
    StreamState,
    _dispatch_log_or_error,
    _drain_stream,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader

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
    reader = ValidatedReader(ipc.open_stream(BytesIO(resp.content)), IpcValidation.NONE)
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

    def test_non_stream_on_init_endpoint_400(self, client: _SyncTestClient) -> None:
        """Non-stream method on the /init endpoint returns 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/init",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "not a stream" in err.error_message

    def test_malformed_body_stream_init_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a stream init endpoint return 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/init",
            content=b"garbage bytes",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "ArrowInvalid" in err.error_type

    def test_malformed_body_stream_exchange_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a stream exchange endpoint return 400."""
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
# Tests: Resumable producer stream over HTTP
# ---------------------------------------------------------------------------


class TestResumableServerStream:
    """Tests for resumable producer stream with continuation tokens."""

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
            externalize_threshold_bytes=threshold,
            max_retries=0,
            retry_delay_seconds=0.0,
        )

    def _make_client(self, config: ExternalLocationConfig) -> _SyncTestClient:
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
        """Large producer stream batches are externalized and resolved by client."""
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
        """Log messages from externalized producer stream batches are dispatched via on_log."""
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


# ---------------------------------------------------------------------------
# Auth test protocol + implementation
# ---------------------------------------------------------------------------


@dataclass
class _AuthStreamState(StreamState):
    """Stream state that checks auth in process()."""

    count: int
    current: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a batch containing the caller's principal."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "principal": [ctx.auth.principal or "anonymous"]})
        self.current += 1


@dataclass
class _AuthBidiState(StreamState):
    """Stream state that checks auth in process()."""

    factor: float

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process input and tag output with caller's principal."""
        scaled = cast("pa.Array[Any]", pc.multiply(input.batch.column("value"), self.factor))  # type: ignore[redundant-cast]
        out.emit_pydict({"value": scaled.to_pylist(), "principal": [ctx.auth.principal or "anon"] * len(scaled)})


class _AuthService(Protocol):
    """Protocol for auth integration tests."""

    def whoami(self) -> str:
        """Return the caller's principal."""
        ...

    def public(self) -> str:
        """Return a constant string."""
        ...

    def guarded(self) -> str:
        """Require authentication and return a secret."""
        ...

    def auth_stream(self, count: int) -> Stream[StreamState]:
        """Stream that includes principal in output."""
        ...

    def auth_bidi(self, factor: float) -> Stream[StreamState]:
        """Stream that includes principal in output."""
        ...


class _AuthServiceImpl:
    """Implementation for auth integration tests."""

    def whoami(self, ctx: CallContext) -> str:
        """Return the caller's principal."""
        return ctx.auth.principal or "anonymous"

    def public(self) -> str:
        """No ctx — always works regardless of auth."""
        return "ok"

    def guarded(self, ctx: CallContext) -> str:
        """Require authentication."""
        ctx.auth.require_authenticated()
        return f"secret for {ctx.auth.principal}"

    def auth_stream(self, count: int, ctx: CallContext) -> Stream[_AuthStreamState]:
        """Stream that passes principal to state."""
        fields: list[pa.Field[Any]] = [pa.field("i", pa.int64()), pa.field("principal", pa.utf8())]
        schema = pa.schema(fields)
        return Stream(
            output_schema=schema,
            state=_AuthStreamState(count=count),
        )

    def auth_bidi(self, factor: float) -> Stream[_AuthBidiState]:
        """Stream that tags output with principal."""
        fields: list[pa.Field[Any]] = [pa.field("value", pa.float64()), pa.field("principal", pa.utf8())]
        schema = pa.schema(fields)
        return Stream(
            output_schema=schema,
            state=_AuthBidiState(factor=factor),
            input_schema=pa.schema([pa.field("value", pa.float64())]),
        )


def _test_authenticate(req: falcon.Request) -> AuthContext:
    """Test authenticate callback — expects 'Bearer <principal>' header."""
    auth_header = req.get_header("Authorization") or ""
    if not auth_header.startswith("Bearer "):
        msg = "Missing or invalid Authorization header"
        raise ValueError(msg)
    principal = auth_header.removeprefix("Bearer ")
    return AuthContext(domain="jwt", authenticated=True, principal=principal)


def _make_auth_client(
    authenticate: Callable[[falcon.Request], AuthContext] = _test_authenticate,
    principal: str | None = None,
) -> _SyncTestClient:
    """Create a sync test client with authentication enabled.

    When *principal* is given, the client auto-injects a
    ``Bearer <principal>`` header on every request via ``default_headers``.
    """
    server = RpcServer(_AuthService, _AuthServiceImpl())
    default_headers = {"Authorization": f"Bearer {principal}"} if principal is not None else None
    return make_sync_client(
        server,
        signing_key=b"auth-test-key",
        authenticate=authenticate,
        default_headers=default_headers,
    )


# ---------------------------------------------------------------------------
# Tests: Authentication over HTTP
# ---------------------------------------------------------------------------


class TestAuthentication:
    """Tests for HTTP authentication via _AuthMiddleware."""

    def test_authenticated_unary(self) -> None:
        """Authenticated unary call populates ctx.auth correctly."""
        client = _make_auth_client(principal="alice")
        with http_connect(_AuthService, client=client) as proxy:
            result = proxy.whoami()
        assert result == "alice"
        client.close()

    def test_missing_auth_401(self) -> None:
        """Missing auth header returns 401."""
        client = _make_auth_client()
        # No auth header → authenticate callback raises → 401
        resp = client.post(
            "http://test/vgi/whoami",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 401
        client.close()

    def test_auth_callback_exception_401(self) -> None:
        """ValueError in authenticate callback returns 401."""

        def failing_auth(req: falcon.Request) -> AuthContext:
            msg = "Token expired"
            raise ValueError(msg)

        client = _make_auth_client(authenticate=failing_auth)
        resp = client.post(
            "http://test/vgi/public",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE, "Authorization": "Bearer whatever"},
        )
        assert resp.status_code == 401
        client.close()

    def test_unexpected_auth_error_propagates_500(self) -> None:
        """Non-ValueError/PermissionError in authenticate propagates as 500."""

        def buggy_auth(req: falcon.Request) -> AuthContext:
            msg = "oops"
            raise KeyError(msg)

        client = _make_auth_client(authenticate=buggy_auth)
        resp = client.post(
            "http://test/vgi/public",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE, "Authorization": "Bearer whatever"},
        )
        assert resp.status_code == 500
        client.close()

    def test_client_receives_rpc_error_on_401(self) -> None:
        """Client-side 401 is raised as RpcError('AuthenticationError')."""
        client = _make_auth_client()
        with (
            pytest.raises(RpcError) as exc_info,
            http_connect(_AuthService, client=client) as proxy,
        ):
            proxy.whoami()  # no auth header → 401
        assert exc_info.value.error_type == "AuthenticationError"
        client.close()

    def test_method_without_ctx_works_with_auth(self) -> None:
        """Methods not declaring ctx work even when authenticate is configured."""
        client = _make_auth_client(principal="bob")
        with http_connect(_AuthService, client=client) as proxy:
            result = proxy.public()
        assert result == "ok"
        client.close()

    def test_guarded_method_passes_when_authenticated(self) -> None:
        """Method calling require_authenticated succeeds with valid auth."""
        client = _make_auth_client(principal="charlie")
        with http_connect(_AuthService, client=client) as proxy:
            result = proxy.guarded()
        assert result == "secret for charlie"
        client.close()

    def test_authenticated_server_stream(self) -> None:
        """Auth context is available in producer stream StreamState.process over HTTP."""
        client = _make_auth_client(principal="dave")
        with http_connect(_AuthService, client=client) as proxy:
            batches = list(proxy.auth_stream(count=3))
        assert len(batches) == 3
        for ab in batches:
            assert ab.batch.column("principal")[0].as_py() == "dave"
        client.close()

    def test_authenticated_bidi(self) -> None:
        """Auth context is available in StreamState.process over HTTP."""
        client = _make_auth_client(principal="eve")
        with http_connect(_AuthService, client=client) as proxy:
            bidi = proxy.auth_bidi(factor=2.0)
            with bidi:
                input_batch = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [1.0, 2.0]}))
                result = bidi.exchange(input_batch)
        assert result.batch.column("value").to_pylist() == [2.0, 4.0]
        assert result.batch.column("principal")[0].as_py() == "eve"
        client.close()

    def test_transport_metadata_populated(self) -> None:
        """ctx.transport_metadata includes remote_addr from HTTP request."""
        captured: list[dict[str, Any]] = []

        class MetaService(Protocol):
            def meta(self) -> str: ...

        class MetaServiceImpl:
            def meta(self, ctx: CallContext) -> str:
                captured.append(dict(ctx.transport_metadata))
                return "ok"

        def always_auth(req: falcon.Request) -> AuthContext:
            return AuthContext(domain="jwt", authenticated=True, principal="test")

        server = RpcServer(MetaService, MetaServiceImpl())
        client = make_sync_client(
            server,
            signing_key=b"test",
            authenticate=always_auth,
            default_headers={"Authorization": "Bearer test"},
        )
        with http_connect(MetaService, client=client) as proxy:
            proxy.meta()
        assert len(captured) == 1
        # Falcon test client sets remote_addr
        assert "remote_addr" in captured[0]
        client.close()

    def test_no_auth_middleware_when_authenticate_is_none(self) -> None:
        """Without authenticate parameter, no auth middleware is added."""
        server = RpcServer(_AuthService, _AuthServiceImpl())
        client = make_sync_client(server, signing_key=b"test")
        # whoami should work — ctx.auth will be anonymous
        with http_connect(_AuthService, client=client) as proxy:
            result = proxy.whoami()
        assert result == "anonymous"
        client.close()


# ---------------------------------------------------------------------------
# Tests: CORS
# ---------------------------------------------------------------------------


class TestCors:
    """Tests for CORS support via cors_origins parameter."""

    def test_cors_wildcard_adds_headers(self) -> None:
        """cors_origins='*' adds Access-Control-Allow-Origin to responses."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server, signing_key=b"test", cors_origins="*")
        tc = falcon.testing.TestClient(app)
        resp = tc.simulate_options("/vgi/add", headers={"Origin": "http://example.com"})
        assert resp.headers.get("access-control-allow-origin") == "*"

    def test_cors_specific_origin(self) -> None:
        """cors_origins with a specific origin only allows that origin."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server, signing_key=b"test", cors_origins="http://example.com")
        tc = falcon.testing.TestClient(app)
        resp = tc.simulate_options("/vgi/add", headers={"Origin": "http://example.com"})
        assert resp.headers.get("access-control-allow-origin") == "http://example.com"

    def test_no_cors_by_default(self) -> None:
        """Without cors_origins, no CORS headers are added."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server, signing_key=b"test")
        tc = falcon.testing.TestClient(app)
        resp = tc.simulate_options("/vgi/add", headers={"Origin": "http://example.com"})
        assert "access-control-allow-origin" not in resp.headers


# ---------------------------------------------------------------------------
# Tests: Max request bytes header
# ---------------------------------------------------------------------------


class TestMaxRequestBytes:
    """Tests for VGI-Max-Request-Bytes header advertisement."""

    def test_header_present_on_post_when_configured(self) -> None:
        """POST response includes VGI-Max-Request-Bytes when configured."""
        client = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test",
            max_request_bytes=10_000_000,
        )
        resp = client.post(
            f"{_BASE_URL}/vgi/add",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        # Falcon test client lowercases header names
        assert resp.headers.get(MAX_REQUEST_BYTES_HEADER.lower()) == "10000000"
        client.close()

    def test_header_absent_when_not_configured(self) -> None:
        """POST response does not include VGI-Max-Request-Bytes by default."""
        client = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test",
        )
        resp = client.post(
            f"{_BASE_URL}/vgi/add",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert MAX_REQUEST_BYTES_HEADER not in resp.headers
        assert MAX_REQUEST_BYTES_HEADER.lower() not in resp.headers
        client.close()

    def test_options_returns_header(self) -> None:
        """OPTIONS response includes VGI-Max-Request-Bytes when configured."""
        client = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test",
            max_request_bytes=5_000_000,
        )
        resp = client.options(f"{_BASE_URL}/vgi/__capabilities__")
        assert resp.headers.get(MAX_REQUEST_BYTES_HEADER.lower()) == "5000000"
        client.close()

    def test_cors_exposes_header(self) -> None:
        """With cors_origins, VGI-Max-Request-Bytes is in Access-Control-Expose-Headers."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server, signing_key=b"test", cors_origins="*", max_request_bytes=1_000_000)
        tc = falcon.testing.TestClient(app)
        resp = tc.simulate_options("/vgi/add", headers={"Origin": "http://example.com"})
        expose = resp.headers.get("access-control-expose-headers", "")
        assert MAX_REQUEST_BYTES_HEADER in expose


# ---------------------------------------------------------------------------
# Tests: HTTP capabilities discovery
# ---------------------------------------------------------------------------


class TestHttpCapabilities:
    """Tests for http_capabilities() discovery function."""

    def test_discovers_max_request_bytes(self) -> None:
        """http_capabilities() reads VGI-Max-Request-Bytes from OPTIONS response."""
        client = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test",
            max_request_bytes=8_000_000,
        )
        caps = http_capabilities(client=client)
        assert isinstance(caps, HttpServerCapabilities)
        assert caps.max_request_bytes == 8_000_000
        client.close()

    def test_none_when_not_configured(self) -> None:
        """http_capabilities() returns None max_request_bytes when not set."""
        client = make_sync_client(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            signing_key=b"test",
        )
        caps = http_capabilities(client=client)
        assert caps.max_request_bytes is None
        client.close()
