"""HTTP transport for vgi-rpc using Falcon (server) and httpx (client).

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application, and ``http_connect`` to call it from Python with ``httpx``.

HTTP Wire Protocol
------------------
All endpoints use ``Content-Type: application/vnd.apache.arrow.stream``.

- **Unary / Server Stream**: ``POST /vgi/{method}``
- **Bidi Init**: ``POST /vgi/{method}/bidi``
- **Bidi Exchange**: ``POST /vgi/{method}/exchange``

Bidi streaming is implemented statelessly: each exchange is a separate HTTP
POST carrying serialized ``BidiStreamState`` in Arrow custom metadata
(``vgi_rpc.bidi_state``).

Optional dependencies: ``pip install vgi-rpc[http]``
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Iterator
from io import BytesIO
from types import TracebackType
from typing import Any
from urllib.parse import urlparse

import falcon
import falcon.testing
import httpx
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.log import Message
from vgi_rpc.metadata import BIDI_STATE_KEY, merge_metadata, strip_keys
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    AnnotatedBatch,
    BidiStream,
    BidiStreamState,
    MethodType,
    OutputCollector,
    PipeTransport,
    RpcError,
    RpcMethodInfo,
    RpcServer,
    StreamSession,
    _deserialize_params,
    _drain_stream,
    _LogSink,
    _read_batch_with_log_check,
    _read_request,
    _read_stream_response,
    _read_unary_response,
    _send_request,
    _validate_params,
    _write_error_batch,
    rpc_methods,
)
from vgi_rpc.utils import empty_batch

__all__ = [
    "HttpBidiSession",
    "http_connect",
    "make_wsgi_app",
]

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


def _check_content_type(req: falcon.Request) -> bytes | None:
    """Return error bytes if Content-Type is not Arrow IPC stream, else ``None``."""
    content_type = req.content_type or ""
    if content_type != _ARROW_CONTENT_TYPE:
        return _error_response_bytes(
            TypeError(f"Expected Content-Type: {_ARROW_CONTENT_TYPE}, got {content_type!r}"),
        )
    return None


def _error_response_bytes(exc: BaseException, schema: pa.Schema = _EMPTY_SCHEMA) -> bytes:
    """Serialize an exception as a complete Arrow IPC error stream."""
    resp_buf = BytesIO()
    with ipc.new_stream(resp_buf, schema) as writer:
        _write_error_batch(writer, schema, exc)
    return resp_buf.getvalue()


def _set_error_response(
    resp: falcon.Response,
    exc: BaseException,
    *,
    status_code: int = 200,
    schema: pa.Schema = _EMPTY_SCHEMA,
) -> None:
    """Set a Falcon response to an Arrow IPC error stream."""
    resp.content_type = _ARROW_CONTENT_TYPE
    resp.data = _error_response_bytes(exc, schema)
    resp.status = str(status_code)


# ---------------------------------------------------------------------------
# Server — Falcon WSGI resources
# ---------------------------------------------------------------------------


class _HttpRpcApp:
    """Internal helper that wraps an RpcServer and manages bidi state."""

    __slots__ = ("_server", "_bidi_state_types", "_bidi_output_schemas", "_lock")

    def __init__(self, server: RpcServer) -> None:
        self._server = server
        self._bidi_state_types: dict[str, type[BidiStreamState]] = {}
        self._bidi_output_schemas: dict[str, pa.Schema] = {}
        self._lock = threading.Lock()

    def _bidi_init_sync(self, method_name: str, info: RpcMethodInfo, body: bytes) -> bytes:
        """Run bidi init synchronously."""
        req_buf = BytesIO(body)
        _, kwargs = _read_request(req_buf)
        _deserialize_params(kwargs, info.param_types)

        try:
            _validate_params(info.name, kwargs, info.param_types)
        except TypeError as exc:
            return _error_response_bytes(exc)

        # Inject emit_log if the implementation accepts it
        sink = _LogSink()
        if method_name in self._server.emit_log_methods:
            kwargs["emit_log"] = sink

        try:
            result: BidiStream[BidiStreamState] = getattr(self._server.implementation, method_name)(**kwargs)
        except Exception as exc:
            return _error_response_bytes(exc)

        state = result.state
        output_schema = result.output_schema

        # Cache state type and output schema for exchange calls
        with self._lock:
            self._bidi_state_types[method_name] = type(state)
            self._bidi_output_schemas[method_name] = output_schema

        # Serialize state
        state_bytes = state.serialize_to_bytes()

        # Write response: log batches + zero-row batch with state metadata
        resp_buf = BytesIO()
        with ipc.new_stream(resp_buf, output_schema) as writer:
            sink.activate(writer, output_schema)
            state_metadata = pa.KeyValueMetadata({BIDI_STATE_KEY: state_bytes})
            zero_batch = empty_batch(output_schema)
            writer.write_batch(zero_batch, custom_metadata=state_metadata)

        return resp_buf.getvalue()

    def _bidi_exchange_sync(self, method_name: str, info: RpcMethodInfo, body: bytes) -> bytes:
        """Run bidi exchange synchronously."""
        with self._lock:
            state_cls = self._bidi_state_types.get(method_name)
            output_schema = self._bidi_output_schemas.get(method_name)

        if state_cls is None:
            return _error_response_bytes(RuntimeError(f"No bidi session initialized for method '{method_name}'"))

        # Read the input batch + extract state from metadata
        req_reader = ipc.open_stream(BytesIO(body))
        input_batch, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
        _drain_stream(req_reader)

        if custom_metadata is None or custom_metadata.get(BIDI_STATE_KEY) is None:
            return _error_response_bytes(RuntimeError("Missing vgi_rpc.bidi_state in exchange request"))

        state_bytes = custom_metadata.get(BIDI_STATE_KEY)
        assert state_bytes is not None  # already checked above
        try:
            state = state_cls.deserialize_from_bytes(state_bytes)
        except Exception as exc:
            return _error_response_bytes(RuntimeError(f"Failed to deserialize bidi state: {exc}"))

        if output_schema is None:
            return _error_response_bytes(RuntimeError(f"No output schema cached for method '{method_name}'"))

        # Strip vgi_rpc.bidi_state from metadata visible to process()
        user_cm = strip_keys(custom_metadata, BIDI_STATE_KEY)

        ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
        out = OutputCollector(output_schema)

        try:
            state.process(ab_in, out)
        except Exception as exc:
            return _error_response_bytes(exc, output_schema)

        # Serialize updated state
        updated_state_bytes = state.serialize_to_bytes()

        # Write response: log batches + data batch with state metadata
        resp_buf = BytesIO()
        with ipc.new_stream(resp_buf, output_schema) as writer:
            for ab in out.batches:
                if ab.batch.num_rows == 0 and ab.custom_metadata is not None:
                    # Log batch — write as-is
                    writer.write_batch(ab.batch, custom_metadata=ab.custom_metadata)
                else:
                    # Data batch — attach state metadata
                    state_md = pa.KeyValueMetadata({BIDI_STATE_KEY: updated_state_bytes})
                    merged = merge_metadata(ab.custom_metadata, state_md)
                    writer.write_batch(ab.batch, custom_metadata=merged)

        return resp_buf.getvalue()


class _RpcResource:
    """Falcon resource for unary and server-stream calls: ``POST {prefix}/{method}``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle unary and server-stream RPC calls."""
        if (ct_err := _check_content_type(req)) is not None:
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.data = ct_err
            resp.status = "415"
            return

        info = self._app._server.methods.get(method)
        if info is None:
            _set_error_response(resp, AttributeError(f"Unknown method: {method}"), status_code=404)
            return
        if info.method_type == MethodType.BIDI_STREAM:
            _set_error_response(
                resp,
                TypeError(f"Bidi method '{method}' requires /bidi and /exchange endpoints"),
                status_code=400,
            )
            return

        body = req.bounded_stream.read()
        req_buf = BytesIO(body)
        resp_buf = BytesIO()

        transport = PipeTransport(req_buf, resp_buf)
        with contextlib.suppress(pa.lib.ArrowInvalid):
            self._app._server.serve_one(transport)

        resp.content_type = _ARROW_CONTENT_TYPE
        resp.data = resp_buf.getvalue()


class _BidiInitResource:
    """Falcon resource for bidi init: ``POST {prefix}/{method}/bidi``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle bidi stream initialization."""
        if (ct_err := _check_content_type(req)) is not None:
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.data = ct_err
            resp.status = "415"
            return

        info = self._app._server.methods.get(method)
        if info is None:
            _set_error_response(resp, AttributeError(f"Unknown method: {method}"), status_code=404)
            return
        if info.method_type != MethodType.BIDI_STREAM:
            _set_error_response(
                resp,
                TypeError(f"Method '{method}' is not a bidi stream"),
                status_code=400,
            )
            return

        body = req.bounded_stream.read()
        try:
            result_bytes = self._app._bidi_init_sync(method, info, body)
        except pa.lib.ArrowInvalid as exc:
            _set_error_response(resp, exc, status_code=400)
            return
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.data = result_bytes


class _BidiExchangeResource:
    """Falcon resource for bidi exchange: ``POST {prefix}/{method}/exchange``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle bidi stream exchange."""
        if (ct_err := _check_content_type(req)) is not None:
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.data = ct_err
            resp.status = "415"
            return

        info = self._app._server.methods.get(method)
        if info is None:
            _set_error_response(resp, AttributeError(f"Unknown method: {method}"), status_code=404)
            return
        if info.method_type != MethodType.BIDI_STREAM:
            _set_error_response(
                resp,
                TypeError(f"Method '{method}' is not a bidi stream"),
                status_code=400,
            )
            return

        body = req.bounded_stream.read()
        try:
            result_bytes = self._app._bidi_exchange_sync(method, info, body)
        except pa.lib.ArrowInvalid as exc:
            _set_error_response(resp, exc, status_code=400)
            return
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.data = result_bytes


def make_wsgi_app(server: RpcServer, *, prefix: str = "/vgi") -> falcon.App:
    """Create a Falcon WSGI app that serves RPC requests over HTTP.

    Args:
        server: The RpcServer instance to serve.
        prefix: URL prefix for all RPC endpoints (default ``/vgi``).

    Returns:
        A Falcon application with routes for unary, server-stream,
        and bidi-stream RPC calls.

    """
    app_handler = _HttpRpcApp(server)
    app = falcon.App()
    app.add_route(f"{prefix}/{{method}}", _RpcResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/bidi", _BidiInitResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/exchange", _BidiExchangeResource(app_handler))
    return app


# ---------------------------------------------------------------------------
# Sync test client
# ---------------------------------------------------------------------------


class _SyncTestResponse:
    """Minimal response object matching what _HttpProxy expects from httpx.Response."""

    __slots__ = ("status_code", "content")

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class _SyncTestClient:
    """Sync HTTP client that calls a Falcon WSGI app directly via falcon.testing.TestClient."""

    __slots__ = ("_client",)

    def __init__(self, app: falcon.App) -> None:
        self._client = falcon.testing.TestClient(app)

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Send a synchronous POST using the Falcon test client."""
        # Strip scheme+host if present (test_http.py passes full URLs)
        path = urlparse(url).path
        result = self._client.simulate_post(path, body=content, headers=headers)
        return _SyncTestResponse(result.status_code, result.content)

    def close(self) -> None:
        """Close the client (no-op for test client)."""


def make_sync_client(server: RpcServer, *, prefix: str = "/vgi") -> _SyncTestClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``falcon.testing.TestClient`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        prefix: URL prefix for RPC endpoints (default ``/vgi``).

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_wsgi_app(server, prefix=prefix)
    return _SyncTestClient(app)


# ---------------------------------------------------------------------------
# Client — http_connect + HttpBidiSession
# ---------------------------------------------------------------------------


class HttpBidiSession:
    """Client-side handle for a bidi stream over HTTP.

    Each ``exchange()`` call is a separate HTTP POST carrying the input batch
    and serialized state.  The server returns the output batch with updated
    state.

    Supports context manager protocol for convenience.
    """

    __slots__ = ("_client", "_url_prefix", "_method", "_state_bytes", "_output_schema", "_on_log")

    def __init__(
        self,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        method: str,
        state_bytes: bytes,
        output_schema: pa.Schema,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        """Initialize with HTTP client, method details, and initial state."""
        self._client = client
        self._url_prefix = url_prefix
        self._method = method
        self._state_bytes = state_bytes
        self._output_schema = output_schema
        self._on_log = on_log

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch and receive the output batch.

        Args:
            input: The input batch to send.

        Returns:
            The output batch from the server.

        Raises:
            RpcError: If the server reports an error.

        """
        # Write input batch with state in metadata
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({BIDI_STATE_KEY: self._state_bytes})
        merged = merge_metadata(input.custom_metadata, state_md)
        with ipc.new_stream(req_buf, input.batch.schema) as writer:
            writer.write_batch(input.batch, custom_metadata=merged)

        resp = self._client.post(
            f"{self._url_prefix}/{self._method}/exchange",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )

        # Read response — log batches + data batch with state
        reader = ipc.open_stream(BytesIO(resp.content))
        try:
            ab = _read_batch_with_log_check(reader, self._on_log)
        except RpcError:
            _drain_stream(reader)
            raise

        # Extract updated state from metadata
        if ab.custom_metadata is not None:
            new_state = ab.custom_metadata.get(BIDI_STATE_KEY)
            if new_state is not None:
                self._state_bytes = new_state

        # Strip vgi_rpc.bidi_state from user-visible metadata
        user_cm = strip_keys(ab.custom_metadata, BIDI_STATE_KEY)

        _drain_stream(reader)
        return AnnotatedBatch(batch=ab.batch, custom_metadata=user_cm)

    def close(self) -> None:
        """Close the session (no-op for HTTP — stateless)."""

    def __enter__(self) -> HttpBidiSession:
        """Enter the context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context."""
        self.close()


@contextlib.contextmanager
def http_connect(
    protocol: type,
    base_url: str,
    *,
    prefix: str = "/vgi",
    on_log: Callable[[Message], None] | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
) -> Iterator[_HttpProxy]:
    """Connect to an HTTP RPC server and yield a typed proxy.

    Args:
        protocol: The Protocol class defining the RPC interface.
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        on_log: Optional callback for log messages from the server.
        client: Optional HTTP client — ``httpx.Client`` for production,
            or a ``_SyncTestClient`` from ``make_sync_client()`` for testing.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    own_client = client is None
    if client is None:
        client = httpx.Client(base_url=base_url)

    url_prefix = prefix
    try:
        yield _HttpProxy(protocol, client, url_prefix, on_log)
    finally:
        if own_client:
            client.close()


class _HttpProxy:
    """Dynamic proxy that implements RPC method calls over HTTP."""

    def __init__(
        self,
        protocol: type,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        self._protocol = protocol
        self._client = client
        self._url_prefix = url_prefix
        self._methods = rpc_methods(protocol)
        self._on_log = on_log

    def __getattr__(self, name: str) -> Any:  # noqa: D105
        info = self._methods.get(name)
        if info is None:
            raise AttributeError(f"{self._protocol.__name__} has no RPC method '{name}'")

        if info.method_type == MethodType.UNARY:
            caller = self._make_unary_caller(info)
        elif info.method_type == MethodType.SERVER_STREAM:
            caller = self._make_stream_caller(info)
        elif info.method_type == MethodType.BIDI_STREAM:
            caller = self._make_bidi_caller(info)
        else:
            raise AttributeError(f"Unknown method type for '{name}'")

        self.__dict__[name] = caller
        return caller

    def _make_unary_caller(self, info: RpcMethodInfo) -> Callable[..., object]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log

        def caller(**kwargs: object) -> object:
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            reader = ipc.open_stream(BytesIO(resp.content))
            return _read_unary_response(reader, info, on_log)

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., StreamSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log

        def caller(**kwargs: object) -> StreamSession:
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            reader = ipc.open_stream(BytesIO(resp.content))
            return _read_stream_response(reader, on_log)

        return caller

    def _make_bidi_caller(self, info: RpcMethodInfo) -> Callable[..., HttpBidiSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log

        def caller(**kwargs: object) -> HttpBidiSession:
            # Send init request
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}/bidi",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            # Read response — log batches + zero-row batch with state
            reader = ipc.open_stream(BytesIO(resp.content))
            output_schema = reader.schema
            state_bytes: bytes | None = None

            try:
                ab = _read_batch_with_log_check(reader, on_log)
                # This is the zero-row batch with state
                if ab.custom_metadata is not None:
                    state_bytes = ab.custom_metadata.get(BIDI_STATE_KEY)
            except StopIteration:
                pass

            _drain_stream(reader)

            if state_bytes is None:
                raise RpcError("ProtocolError", "Missing vgi_rpc.bidi_state in bidi init response", "")

            return HttpBidiSession(
                client=client,
                url_prefix=url_prefix,
                method=info.name,
                state_bytes=state_bytes,
                output_schema=output_schema,
                on_log=on_log,
            )

        return caller
