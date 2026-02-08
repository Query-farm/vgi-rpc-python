"""HTTP transport for vgi-rpc using Starlette (server) and httpx (client).

Provides ``make_asgi_app`` to expose an ``RpcServer`` as a Starlette ASGI
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

import asyncio
import contextlib
from collections.abc import Callable, Iterator
from io import BytesIO
from types import TracebackType
from typing import Any

import httpx
import pyarrow as pa
from pyarrow import ipc
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Mount, Route

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
    _deserialize_value,
    _dispatch_log_or_error,
    _drain_stream,
    _LogSink,
    _read_batch_with_log_check,
    _read_request,
    _send_request,
    _validate_params,
    _validate_result,
    _write_error_batch,
    rpc_methods,
)
from vgi_rpc.utils import empty_batch

__all__ = [
    "HttpBidiSession",
    "http_connect",
    "make_asgi_app",
]

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


# ---------------------------------------------------------------------------
# Server — ASGI app
# ---------------------------------------------------------------------------


class _HttpRpcApp:
    """Internal helper that wraps an RpcServer and provides Starlette route handlers."""

    __slots__ = ("_server", "_bidi_state_types", "_bidi_output_schemas")

    def __init__(self, server: RpcServer) -> None:
        self._server = server
        self._bidi_state_types: dict[str, type[BidiStreamState]] = {}
        self._bidi_output_schemas: dict[str, pa.Schema] = {}

    # --- Route handlers ---

    async def rpc_endpoint(self, request: Request) -> Response:
        """Handle unary and server-stream calls via ``POST /vgi/{method}``."""
        method_name: str = request.path_params["method"]
        info = self._server.methods.get(method_name)
        if info is None:
            return Response(f"Unknown method: {method_name}", status_code=404)
        if info.method_type == MethodType.BIDI_STREAM:
            return Response(
                f"Bidi method '{method_name}' requires /bidi and /exchange endpoints",
                status_code=400,
            )

        body = await request.body()
        req_buf = BytesIO(body)
        resp_buf = BytesIO()

        transport = PipeTransport(req_buf, resp_buf)
        try:
            await asyncio.to_thread(self._server.serve_one, transport)
        except pa.lib.ArrowInvalid:
            pass  # Error already written to resp_buf by serve_one

        return Response(content=resp_buf.getvalue(), media_type=_ARROW_CONTENT_TYPE)

    async def bidi_init_endpoint(self, request: Request) -> Response:
        """Handle bidi init via ``POST /vgi/{method}/bidi``."""
        method_name: str = request.path_params["method"]
        info = self._server.methods.get(method_name)
        if info is None:
            return Response(f"Unknown method: {method_name}", status_code=404)
        if info.method_type != MethodType.BIDI_STREAM:
            return Response(f"Method '{method_name}' is not a bidi stream", status_code=400)

        body = await request.body()

        def _do_init() -> bytes:
            return self._bidi_init_sync(method_name, info, body)

        try:
            result_bytes = await asyncio.to_thread(_do_init)
        except pa.lib.ArrowInvalid as exc:
            return Response(f"Invalid Arrow IPC payload: {exc}", status_code=400)
        return Response(content=result_bytes, media_type=_ARROW_CONTENT_TYPE)

    async def bidi_exchange_endpoint(self, request: Request) -> Response:
        """Handle bidi exchange via ``POST /vgi/{method}/exchange``."""
        method_name: str = request.path_params["method"]
        info = self._server.methods.get(method_name)
        if info is None:
            return Response(f"Unknown method: {method_name}", status_code=404)
        if info.method_type != MethodType.BIDI_STREAM:
            return Response(f"Method '{method_name}' is not a bidi stream", status_code=400)

        body = await request.body()

        def _do_exchange() -> bytes:
            return self._bidi_exchange_sync(method_name, info, body)

        try:
            result_bytes = await asyncio.to_thread(_do_exchange)
        except pa.lib.ArrowInvalid as exc:
            return Response(f"Invalid Arrow IPC payload: {exc}", status_code=400)
        return Response(content=result_bytes, media_type=_ARROW_CONTENT_TYPE)

    # --- Sync helpers (run in thread) ---

    def _bidi_init_sync(self, method_name: str, info: RpcMethodInfo, body: bytes) -> bytes:
        """Run bidi init synchronously — called via asyncio.to_thread."""
        req_buf = BytesIO(body)
        _, kwargs = _read_request(req_buf)
        _deserialize_params(kwargs, info.param_types)

        try:
            _validate_params(info.name, kwargs, info.param_types)
        except TypeError as exc:
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, exc)
            return resp_buf.getvalue()

        # Inject emit_log if the implementation accepts it
        sink = _LogSink()
        if method_name in self._server.emit_log_methods:
            kwargs["emit_log"] = sink

        try:
            result: BidiStream[BidiStreamState] = getattr(self._server.implementation, method_name)(**kwargs)
        except Exception as exc:
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, exc)
            return resp_buf.getvalue()

        state = result.state
        output_schema = result.output_schema

        # Cache state type and output schema for exchange calls
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
        """Run bidi exchange synchronously — called via asyncio.to_thread."""
        state_cls = self._bidi_state_types.get(method_name)
        if state_cls is None:
            resp_buf = BytesIO()
            exc = RuntimeError(f"No bidi session initialized for method '{method_name}'")
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, exc)
            return resp_buf.getvalue()

        # Read the input batch + extract state from metadata
        req_reader = ipc.open_stream(BytesIO(body))
        input_batch, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
        _drain_stream(req_reader)

        if custom_metadata is None or custom_metadata.get(BIDI_STATE_KEY) is None:
            resp_buf = BytesIO()
            exc = RuntimeError("Missing vgi_rpc.bidi_state in exchange request")
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, exc)
            return resp_buf.getvalue()

        state_bytes = custom_metadata.get(BIDI_STATE_KEY)
        assert state_bytes is not None  # already checked above
        try:
            state = state_cls.deserialize_from_bytes(state_bytes)
        except Exception as exc:
            resp_buf = BytesIO()
            err = RuntimeError(f"Failed to deserialize bidi state: {exc}")
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, err)
            return resp_buf.getvalue()

        output_schema = self._bidi_output_schemas.get(method_name)
        if output_schema is None:
            resp_buf = BytesIO()
            exc = RuntimeError(f"No output schema cached for method '{method_name}'")
            with ipc.new_stream(resp_buf, _EMPTY_SCHEMA) as writer:
                _write_error_batch(writer, _EMPTY_SCHEMA, exc)
            return resp_buf.getvalue()

        # Strip vgi_rpc.bidi_state from metadata visible to process()
        user_cm = strip_keys(custom_metadata, BIDI_STATE_KEY)

        ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
        out = OutputCollector(output_schema)

        try:
            state.process(ab_in, out)
        except Exception as exc:
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, output_schema) as writer:
                _write_error_batch(writer, output_schema, exc)
            return resp_buf.getvalue()

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


def make_asgi_app(server: RpcServer, *, prefix: str = "/vgi") -> Starlette:
    """Create a Starlette ASGI app that serves RPC requests over HTTP.

    Args:
        server: The RpcServer instance to serve.
        prefix: URL prefix for all RPC endpoints (default ``/vgi``).

    Returns:
        A Starlette application with routes for unary, server-stream,
        and bidi-stream RPC calls.

    """
    app_handler = _HttpRpcApp(server)
    return Starlette(
        routes=[
            Mount(
                prefix,
                routes=[
                    Route("/{method}", app_handler.rpc_endpoint, methods=["POST"]),
                    Route("/{method}/bidi", app_handler.bidi_init_endpoint, methods=["POST"]),
                    Route("/{method}/exchange", app_handler.bidi_exchange_endpoint, methods=["POST"]),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Sync ASGI bridge for testing
# ---------------------------------------------------------------------------


class _SyncASGIResponse:
    """Minimal response object matching what _HttpProxy expects from httpx.Response."""

    __slots__ = ("status_code", "content")

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        """Raise httpx.HTTPStatusError if status >= 400."""
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=httpx.Request("POST", "http://test"),
                response=httpx.Response(self.status_code, content=self.content),
            )


class _SyncASGIClient:
    """Sync HTTP client that calls a Starlette ASGI app directly.

    Uses a private event loop to run ``httpx.AsyncClient`` with
    ``ASGITransport`` — no real server needed.
    """

    __slots__ = ("_loop", "_client")

    def __init__(self, app: Starlette, base_url: str) -> None:
        self._loop = asyncio.new_event_loop()
        transport = httpx.ASGITransport(app=app)
        self._client = httpx.AsyncClient(transport=transport, base_url=base_url)

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncASGIResponse:
        """Send a synchronous POST by bridging to the async client."""
        resp = self._loop.run_until_complete(self._client.post(url, content=content, headers=headers))
        return _SyncASGIResponse(resp.status_code, resp.content)

    def close(self) -> None:
        """Close the async client and event loop."""
        self._loop.run_until_complete(self._client.aclose())
        self._loop.close()


def make_sync_client(server: RpcServer, *, base_url: str = "http://test", prefix: str = "/vgi") -> _SyncASGIClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``httpx.ASGITransport`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        base_url: Fake base URL (default ``http://test``).
        prefix: URL prefix for RPC endpoints (default ``/vgi``).

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_asgi_app(server, prefix=prefix)
    return _SyncASGIClient(app, base_url)


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
        client: httpx.Client | _SyncASGIClient,
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
        resp.raise_for_status()

        # Read response — log batches + data batch with state
        reader = ipc.open_stream(BytesIO(resp.content))
        try:
            while True:
                batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                if _dispatch_log_or_error(batch, custom_metadata, self._on_log):
                    continue

                # Extract updated state from metadata
                if custom_metadata is not None:
                    new_state = custom_metadata.get(BIDI_STATE_KEY)
                    if new_state is not None:
                        self._state_bytes = new_state

                # Strip vgi_rpc.bidi_state from user-visible metadata
                user_cm = strip_keys(custom_metadata, BIDI_STATE_KEY)

                _drain_stream(reader)
                return AnnotatedBatch(batch=batch, custom_metadata=user_cm)
        except RpcError:
            _drain_stream(reader)
            raise

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
    client: httpx.Client | _SyncASGIClient | None = None,
) -> Iterator[_HttpProxy]:
    """Connect to an HTTP RPC server and yield a typed proxy.

    Args:
        protocol: The Protocol class defining the RPC interface.
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        on_log: Optional callback for log messages from the server.
        client: Optional HTTP client — ``httpx.Client`` for production,
            or a ``_SyncASGIClient`` from ``make_sync_client()`` for testing.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    own_client = client is None
    if client is None:
        client = httpx.Client(base_url=base_url)

    url_prefix = f"{base_url}{prefix}"
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
        client: httpx.Client | _SyncASGIClient,
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
            resp.raise_for_status()

            reader = ipc.open_stream(BytesIO(resp.content))
            try:
                batch = _read_batch_with_log_check(reader, on_log)
            except RpcError:
                _drain_stream(reader)
                raise
            _drain_stream(reader)
            if not info.has_return:
                return None
            value = batch.column("result")[0].as_py()
            _validate_result(info.name, value, info.result_type)
            if value is None:
                return None
            return _deserialize_value(value, info.result_type)

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
            resp.raise_for_status()

            reader = ipc.open_stream(BytesIO(resp.content))
            return StreamSession(reader, on_log)

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
            resp.raise_for_status()

            # Read response — log batches + zero-row batch with state
            reader = ipc.open_stream(BytesIO(resp.content))
            output_schema = reader.schema
            state_bytes: bytes | None = None

            try:
                while True:
                    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                    if _dispatch_log_or_error(batch, custom_metadata, on_log):
                        continue
                    # This is the zero-row batch with state
                    if custom_metadata is not None:
                        state_bytes = custom_metadata.get(BIDI_STATE_KEY)
                    break
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
