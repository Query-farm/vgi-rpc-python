"""HTTP transport for vgi-rpc using Falcon (server) and httpx (client).

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application, and ``http_connect`` to call it from Python with ``httpx``.

HTTP Wire Protocol
------------------
All endpoints use ``Content-Type: application/vnd.apache.arrow.stream``.

- **Unary / Server Stream**: ``POST /vgi/{method}``
- **Bidi Init**: ``POST /vgi/{method}/bidi``
- **State Exchange**: ``POST /vgi/{method}/exchange``

Bidi streaming and resumable server-streaming are implemented statelessly:
each exchange is a separate HTTP POST carrying serialized state in Arrow
custom metadata (``vgi_rpc.bidi_state``).  When ``max_stream_response_bytes``
is set, server-stream responses are split across multiple exchanges; the
client transparently resumes via ``POST /{method}/exchange``.

Optional dependencies: ``pip install vgi-rpc[http]``
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import os
import struct
import warnings
from collections.abc import Callable, Iterator, Mapping
from http import HTTPStatus
from io import BytesIO, IOBase
from types import TracebackType
from typing import Any, get_args, get_origin, get_type_hints
from urllib.parse import urlparse

import falcon
import falcon.testing
import httpx
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import (
    ExternalLocationConfig,
    maybe_externalize_batch,
    resolve_external_location,
)
from vgi_rpc.log import Message
from vgi_rpc.metadata import STATE_KEY, merge_metadata, strip_keys
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    AnnotatedBatch,
    AuthContext,
    BidiStream,
    BidiStreamState,
    CallContext,
    MethodType,
    OutputCollector,
    RpcError,
    RpcMethodInfo,
    RpcServer,
    ServerStream,
    ServerStreamState,
    VersionError,
    _ClientLogSink,
    _current_transport,
    _deserialize_params,
    _dispatch_log_or_error,
    _drain_stream,
    _flush_collector,
    _get_auth_and_metadata,
    _read_batch_with_log_check,
    _read_request,
    _read_unary_response,
    _send_request,
    _TransportContext,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_result_batch,
    rpc_methods,
)
from vgi_rpc.utils import empty_batch

__all__ = [
    "HttpBidiSession",
    "HttpStreamSession",
    "http_connect",
    "make_sync_client",
    "make_wsgi_app",
]

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


class _RpcHttpError(Exception):
    """Internal exception for HTTP-layer errors with status codes."""

    __slots__ = ("cause", "schema", "status_code")

    def __init__(self, cause: BaseException, *, status_code: HTTPStatus, schema: pa.Schema = _EMPTY_SCHEMA) -> None:
        self.cause = cause
        self.status_code = status_code
        self.schema = schema


def _check_content_type(req: falcon.Request) -> None:
    """Raise ``_RpcHttpError`` if Content-Type is not Arrow IPC stream."""
    content_type = req.content_type or ""
    if content_type != _ARROW_CONTENT_TYPE:
        raise _RpcHttpError(
            TypeError(f"Expected Content-Type: {_ARROW_CONTENT_TYPE}, got {content_type!r}"),
            status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
        )


def _error_response_stream(
    exc: BaseException, schema: pa.Schema = _EMPTY_SCHEMA, server_id: str | None = None
) -> BytesIO:
    """Serialize an exception as a complete Arrow IPC error stream.

    Args:
        exc: The exception to serialize.
        schema: Arrow schema for the error stream (default empty).
        server_id: Optional server identifier injected into error metadata.

    Returns:
        A ``BytesIO`` positioned at the start, containing the IPC stream.

    """
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        _write_error_batch(writer, schema, exc, server_id=server_id)
    buf.seek(0)
    return buf


def _set_error_response(
    resp: falcon.Response,
    exc: BaseException,
    *,
    status_code: HTTPStatus = HTTPStatus.BAD_REQUEST,
    schema: pa.Schema = _EMPTY_SCHEMA,
    server_id: str | None = None,
) -> None:
    """Set a Falcon response to an Arrow IPC error stream."""
    resp.content_type = _ARROW_CONTENT_TYPE
    resp.stream = _error_response_stream(exc, schema, server_id=server_id)
    resp.status = str(status_code.value)


# ---------------------------------------------------------------------------
# Signed state token helpers
# ---------------------------------------------------------------------------

_HMAC_LEN = 32  # SHA-256 digest size
_HEADER_LEN = 4  # uint32 LE prefix for each segment
_MIN_TOKEN_LEN = _HEADER_LEN + _HEADER_LEN + _HMAC_LEN  # two length prefixes + HMAC


def _pack_state_token(state_bytes: bytes, schema_bytes: bytes, signing_key: bytes) -> bytes:
    """Pack state and schema bytes into a signed token.

    Wire format::

        [4 bytes: state_len  (uint32 LE)]
        [state_len bytes: state_bytes]
        [4 bytes: schema_len (uint32 LE)]
        [schema_len bytes: schema_bytes]
        [32 bytes: HMAC-SHA256(key, state_bytes + schema_bytes)]

    Args:
        state_bytes: Serialized state (Arrow IPC).
        schema_bytes: Serialized ``pa.Schema``.
        signing_key: HMAC signing key.

    Returns:
        The opaque signed token.

    """
    payload = struct.pack("<I", len(state_bytes)) + state_bytes + struct.pack("<I", len(schema_bytes)) + schema_bytes
    mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return payload + mac


def _unpack_state_token(token: bytes, signing_key: bytes) -> tuple[bytes, bytes]:
    """Unpack and verify a signed state token.

    Args:
        token: The opaque token produced by ``_pack_state_token``.
        signing_key: HMAC signing key (must match the one used to pack).

    Returns:
        ``(state_bytes, schema_bytes)``

    Raises:
        _RpcHttpError: On malformed or tampered tokens (HTTP 400).

    """
    if len(token) < _MIN_TOKEN_LEN:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # Parse lengths and extract segments
    state_len = struct.unpack_from("<I", token, 0)[0]
    offset = _HEADER_LEN + state_len
    if offset + _HEADER_LEN + _HMAC_LEN > len(token):
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    schema_len = struct.unpack_from("<I", token, offset)[0]
    payload_end = offset + _HEADER_LEN + schema_len
    if payload_end + _HMAC_LEN != len(token):
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    payload = token[:payload_end]
    received_mac = token[payload_end:]
    expected_mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(received_mac, expected_mac):
        raise _RpcHttpError(
            RuntimeError("State token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    state_bytes = token[_HEADER_LEN : _HEADER_LEN + state_len]
    schema_bytes = token[offset + _HEADER_LEN : offset + _HEADER_LEN + schema_len]
    return state_bytes, schema_bytes


def _resolve_state_types(
    server: RpcServer,
) -> dict[str, type[BidiStreamState] | type[ServerStreamState]]:
    """Introspect server implementation to map method names to concrete state types.

    Examines the return type hints of each bidi-stream and server-stream
    method on the implementation (not the protocol) to extract the concrete
    ``BidiStreamState`` or ``ServerStreamState`` subclass.

    Args:
        server: The ``RpcServer`` whose implementation to introspect.

    Returns:
        Mapping of method name to concrete state subclass.

    """
    result: dict[str, type[BidiStreamState] | type[ServerStreamState]] = {}
    for name, info in server.methods.items():
        if info.method_type not in (MethodType.BIDI_STREAM, MethodType.SERVER_STREAM):
            continue
        impl_method = getattr(server.implementation, name, None)
        if impl_method is None:
            continue
        try:
            hints = get_type_hints(impl_method)
        except (NameError, AttributeError):
            continue
        return_hint = hints.get("return")
        if return_hint is None:
            continue
        origin = get_origin(return_hint)
        if origin is BidiStream:
            args = get_args(return_hint)
            if args and isinstance(args[0], type) and issubclass(args[0], BidiStreamState):
                result[name] = args[0]
        elif origin is ServerStream:
            args = get_args(return_hint)
            if args and isinstance(args[0], type) and issubclass(args[0], ServerStreamState):
                result[name] = args[0]
    return result


# ---------------------------------------------------------------------------
# Server — Falcon WSGI resources
# ---------------------------------------------------------------------------


class _HttpRpcApp:
    """Internal helper that wraps an RpcServer and manages stream/bidi state."""

    __slots__ = ("_max_stream_response_bytes", "_server", "_signing_key", "_state_types")

    def __init__(self, server: RpcServer, signing_key: bytes, max_stream_response_bytes: int | None = None) -> None:
        self._server = server
        self._signing_key = signing_key
        self._state_types = _resolve_state_types(server)
        self._max_stream_response_bytes = max_stream_response_bytes

    def _resolve_method(self, req: falcon.Request, method: str) -> RpcMethodInfo:
        """Validate content type and resolve method info.

        Raises:
            _RpcHttpError: If content type is wrong or method is unknown.

        """
        _check_content_type(req)
        info = self._server.methods.get(method)
        if info is None:
            raise _RpcHttpError(AttributeError(f"Unknown method: {method}"), status_code=HTTPStatus.NOT_FOUND)
        return info

    def _unary_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> tuple[BytesIO, HTTPStatus]:
        """Run a unary method synchronously.

        Returns:
            ``(response_buf, http_status)`` — the IPC response stream
            and the HTTP status code to use.

        Raises:
            _RpcHttpError: For protocol-level errors (bad IPC, missing
                metadata, param validation failures).

        """
        try:
            ipc_method, kwargs = _read_request(stream)
            if ipc_method != method_name:
                raise TypeError(f"Method mismatch: URL '{method_name}' vs IPC metadata '{ipc_method}'")
            _deserialize_params(kwargs, info.param_types)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        server_id = self._server.server_id
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in self._server.ctx_methods:
            kwargs["ctx"] = CallContext(auth=auth, emit_client_log=sink, transport_metadata=transport_metadata)

        schema = info.result_schema
        resp_buf = BytesIO()
        http_status = HTTPStatus.OK
        with ipc.new_stream(resp_buf, schema) as writer:
            sink.flush_contents(writer, schema)
            try:
                result = getattr(self._server.implementation, method_name)(**kwargs)
                _validate_result(info.name, result, info.result_type)
                _write_result_batch(writer, schema, result, self._server.external_config)
            except (TypeError, pa.ArrowInvalid) as exc:
                _write_error_batch(writer, schema, exc, server_id=server_id)
                http_status = HTTPStatus.BAD_REQUEST
            except Exception as exc:
                _write_error_batch(writer, schema, exc, server_id=server_id)
                http_status = HTTPStatus.INTERNAL_SERVER_ERROR

        resp_buf.seek(0)
        return resp_buf, http_status

    def _bidi_init_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> BytesIO:
        """Run bidi init synchronously."""
        try:
            ipc_method, kwargs = _read_request(stream)
            if ipc_method != method_name:
                raise TypeError(f"Method mismatch: URL '{method_name}' vs IPC metadata '{ipc_method}'")
            _deserialize_params(kwargs, info.param_types)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Inject ctx if the implementation accepts it
        sink = _ClientLogSink(server_id=self._server.server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in self._server.ctx_methods:
            kwargs["ctx"] = CallContext(auth=auth, emit_client_log=sink, transport_metadata=transport_metadata)

        try:
            result: BidiStream[BidiStreamState] = getattr(self._server.implementation, method_name)(**kwargs)
        except (TypeError, pa.ArrowInvalid) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc
        except Exception as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

        try:
            state = result.state
            output_schema = result.output_schema

            # Pack state + schema into a signed token
            state_bytes = state.serialize_to_bytes()
            schema_bytes = output_schema.serialize().to_pybytes()
            token = _pack_state_token(state_bytes, schema_bytes, self._signing_key)

            # Write response: log batches + zero-row batch with token in metadata
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, output_schema) as writer:
                sink.flush_contents(writer, output_schema)
                state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                zero_batch = empty_batch(output_schema)
                writer.write_batch(zero_batch, custom_metadata=state_metadata)
        except Exception as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

        resp_buf.seek(0)
        return resp_buf

    def _bidi_exchange_sync(self, method_name: str, stream: IOBase) -> BytesIO:
        """Run bidi exchange synchronously."""
        state_cls = self._state_types.get(method_name)
        if state_cls is None:
            raise _RpcHttpError(
                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        # Read the input batch + extract token from metadata
        try:
            req_reader = ipc.open_stream(stream)
            input_batch, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
            _drain_stream(req_reader)
        except pa.ArrowInvalid as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Extract state token before resolution — resolve_external_location
        # replaces metadata with what was stored in the external IPC stream.
        token = custom_metadata.get(STATE_KEY) if custom_metadata is not None else None

        # Resolve ExternalLocation on input batch
        try:
            input_batch, resolved_cm = resolve_external_location(
                input_batch, custom_metadata, self._server.external_config
            )
        except Exception as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

        if token is None:
            raise _RpcHttpError(
                RuntimeError("Missing state token in exchange request"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        # Unpack and verify the signed token, recover state + schema
        state_obj, output_schema = self._unpack_and_recover_state(token, state_cls)

        if not isinstance(state_obj, BidiStreamState):
            raise _RpcHttpError(
                RuntimeError(f"Expected BidiStreamState, got {type(state_obj).__name__}"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        try:
            # Strip state token from metadata visible to process()
            user_cm = strip_keys(resolved_cm, STATE_KEY)

            ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
            out = OutputCollector(output_schema, server_id=self._server.server_id)

            auth, transport_md = _get_auth_and_metadata()
            process_ctx = CallContext(
                auth=auth,
                emit_client_log=out.emit_client_log_message,
                transport_metadata=transport_md,
            )
            state_obj.process(ab_in, out, process_ctx)
            out.validate()

            # Repack updated state with same schema into new signed token
            updated_state_bytes = state_obj.serialize_to_bytes()
            schema_bytes = output_schema.serialize().to_pybytes()
            updated_token = _pack_state_token(updated_state_bytes, schema_bytes, self._signing_key)
            out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

            # Write response batches (log + data, in order)
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, output_schema) as writer:
                _flush_collector(writer, out, self._server.external_config)
        except Exception as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, schema=output_schema) from exc

        resp_buf.seek(0)
        return resp_buf

    def _produce_stream_response(
        self,
        schema: pa.Schema,
        state: ServerStreamState,
        sink: _ClientLogSink | None = None,
        *,
        auth: AuthContext | None = None,
        transport_metadata: Mapping[str, Any] | None = None,
    ) -> BytesIO:
        """Run the produce loop for a server stream, with optional size-based continuation.

        Args:
            schema: The output schema for the stream.
            state: The server-stream state object.
            sink: Optional log sink to flush before producing (initial request only).
            auth: Auth context; falls back to contextvar when ``None`` (continuation path).
            transport_metadata: Transport metadata; falls back to contextvar when ``None``.

        Returns:
            A ``BytesIO`` containing the IPC response stream.

        """
        if auth is None or transport_metadata is None:
            cv_auth, cv_md = _get_auth_and_metadata()
            auth = auth if auth is not None else cv_auth
            transport_metadata = transport_metadata if transport_metadata is not None else cv_md

        server_id = self._server.server_id
        resp_buf = BytesIO()
        max_bytes = self._max_stream_response_bytes
        with ipc.new_stream(resp_buf, schema) as writer:
            if sink is not None:
                sink.flush_contents(writer, schema)
            cumulative_bytes = 0
            try:
                while True:
                    out = OutputCollector(schema, prior_data_bytes=cumulative_bytes, server_id=server_id)
                    produce_ctx = CallContext(
                        auth=auth,
                        emit_client_log=out.emit_client_log_message,
                        transport_metadata=transport_metadata,
                    )
                    state.produce(out, produce_ctx)
                    if not out.finished:
                        out.validate()
                    _flush_collector(writer, out, self._server.external_config)
                    if out.finished:
                        break
                    cumulative_bytes = out.total_data_bytes
                    # Check size limit after flushing each produce cycle
                    if max_bytes is not None and resp_buf.tell() >= max_bytes:
                        # Serialize state into a continuation token
                        state_bytes = state.serialize_to_bytes()
                        schema_bytes = schema.serialize().to_pybytes()
                        token = _pack_state_token(state_bytes, schema_bytes, self._signing_key)
                        state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                        writer.write_batch(empty_batch(schema), custom_metadata=state_metadata)
                        break
            except Exception as exc:
                _write_error_batch(writer, schema, exc, server_id=server_id)
        resp_buf.seek(0)
        return resp_buf

    def _server_stream_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> BytesIO:
        """Run a server-stream method synchronously with resumable support."""
        try:
            ipc_method, kwargs = _read_request(stream)
            if ipc_method != method_name:
                raise TypeError(f"Method mismatch: URL '{method_name}' vs IPC metadata '{ipc_method}'")
            _deserialize_params(kwargs, info.param_types)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Inject ctx if the implementation accepts it
        sink = _ClientLogSink(server_id=self._server.server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in self._server.ctx_methods:
            kwargs["ctx"] = CallContext(auth=auth, emit_client_log=sink, transport_metadata=transport_metadata)

        try:
            result: ServerStream[ServerStreamState] = getattr(self._server.implementation, method_name)(**kwargs)
        except (TypeError, pa.ArrowInvalid) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc
        except Exception as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

        return self._produce_stream_response(
            result.output_schema,
            result.state,
            sink,
            auth=auth,
            transport_metadata=transport_metadata,
        )

    def _server_stream_continue_sync(self, method_name: str, stream: IOBase) -> BytesIO:
        """Resume a server-stream from a continuation token."""
        state_cls = self._state_types.get(method_name)
        if state_cls is None:
            raise _RpcHttpError(
                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        # Read the request batch + extract token from metadata
        try:
            req_reader = ipc.open_stream(stream)
            _, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
            _drain_stream(req_reader)
        except pa.ArrowInvalid as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        token = custom_metadata.get(STATE_KEY) if custom_metadata is not None else None
        if token is None:
            raise _RpcHttpError(
                RuntimeError("Missing state token in exchange request"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        # Unpack and verify the signed token, recover state + schema
        state, output_schema = self._unpack_and_recover_state(token, state_cls)

        if not isinstance(state, ServerStreamState):
            raise _RpcHttpError(
                RuntimeError(f"State type mismatch: expected ServerStreamState, got {type(state).__name__}"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        return self._produce_stream_response(output_schema, state)

    def _unpack_and_recover_state(
        self,
        token: bytes,
        state_cls: type[BidiStreamState] | type[ServerStreamState],
    ) -> tuple[BidiStreamState | ServerStreamState, pa.Schema]:
        """Unpack a signed state token and recover the state object and schema.

        Args:
            token: The signed state token bytes.
            state_cls: The concrete state class to deserialize into.

        Returns:
            Tuple of (state_object, output_schema).

        Raises:
            _RpcHttpError: On malformed tokens, failed deserialization, or
                signature verification failure.

        """
        state_bytes, schema_bytes = _unpack_state_token(token, self._signing_key)

        try:
            output_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize output schema: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        try:
            state_obj = state_cls.deserialize_from_bytes(state_bytes)
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize state: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        return state_obj, output_schema


class _RpcResource:
    """Falcon resource for unary and server-stream calls: ``POST {prefix}/{method}``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle unary and server-stream RPC calls."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type == MethodType.BIDI_STREAM:
                raise _RpcHttpError(
                    TypeError(f"Bidi method '{method}' requires /bidi and /exchange endpoints"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            if info.method_type == MethodType.UNARY:
                result_stream, http_status = self._app._unary_sync(method, info, req.bounded_stream)
                resp.content_type = _ARROW_CONTENT_TYPE
                resp.stream = result_stream
                resp.status = str(http_status.value)
                return

            # SERVER_STREAM
            result_stream = self._app._server_stream_sync(method, info, req.bounded_stream)
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
            )


class _BidiInitResource:
    """Falcon resource for bidi init: ``POST {prefix}/{method}/bidi``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle bidi stream initialization."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type != MethodType.BIDI_STREAM:
                raise _RpcHttpError(
                    TypeError(f"Method '{method}' is not a bidi stream"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )
            result_stream = self._app._bidi_init_sync(method, info, req.bounded_stream)
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
            )
            return
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.stream = result_stream


class _ExchangeResource:
    """Falcon resource for state exchange: ``POST {prefix}/{method}/exchange``.

    Dispatches to bidi exchange or server-stream continuation based on method type.
    """

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle bidi exchange or server-stream continuation."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type == MethodType.BIDI_STREAM:
                result_stream = self._app._bidi_exchange_sync(method, req.bounded_stream)
            elif info.method_type == MethodType.SERVER_STREAM and self._app._max_stream_response_bytes is not None:
                result_stream = self._app._server_stream_continue_sync(method, req.bounded_stream)
            else:
                raise _RpcHttpError(
                    TypeError(f"Method '{method}' does not support /exchange"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
            )
            return
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.stream = result_stream


class _AuthMiddleware:
    """Falcon middleware that runs an ``authenticate`` callback on each request.

    On success, sets a ``_TransportContext`` in ``_current_transport`` for
    the duration of the request so that ``CallContext`` picks up the real
    ``AuthContext`` and transport metadata.

    The ``authenticate`` callback is expected to raise ``ValueError`` (bad
    credentials) or ``PermissionError`` (forbidden) on failure.  Other
    exceptions propagate as 500s so that bugs in the callback are not
    silently swallowed as 401s.
    """

    __slots__ = ("_authenticate",)

    def __init__(self, authenticate: Callable[[falcon.Request], AuthContext]) -> None:
        self._authenticate = authenticate

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Authenticate the request and populate the transport contextvar.

        Only ``ValueError`` and ``PermissionError`` are caught and mapped to
        HTTP 401.  Other exceptions propagate as 500 so that bugs in the
        authenticate callback surface loudly rather than masquerading as
        auth failures.

        The 401 response is plain text (not Arrow IPC) because at this
        stage no method has been resolved and the output schema is unknown.
        """
        try:
            auth = self._authenticate(req)
        except (ValueError, PermissionError) as exc:
            raise falcon.HTTPUnauthorized(description=str(exc)) from exc
        transport_metadata: dict[str, str] = {}
        if req.remote_addr:
            transport_metadata["remote_addr"] = req.remote_addr
        ua = req.user_agent
        if ua:
            transport_metadata["user_agent"] = ua
        tc = _TransportContext(auth=auth, transport_metadata=transport_metadata)
        req.context.transport_token = _current_transport.set(tc)

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Reset the transport contextvar after each request."""
        token = getattr(req.context, "transport_token", None)
        if token is not None:
            _current_transport.reset(token)


def make_wsgi_app(
    server: RpcServer,
    *,
    prefix: str = "/vgi",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
) -> falcon.App[falcon.Request, falcon.Response]:
    """Create a Falcon WSGI app that serves RPC requests over HTTP.

    Args:
        server: The RpcServer instance to serve.
        prefix: URL prefix for all RPC endpoints (default ``/vgi``).
        signing_key: HMAC key for signing state tokens.  When ``None``
            (the default), a random 32-byte key is generated **per process**.
            This means state tokens issued by one worker are invalid in
            another — you **must** provide a shared key for multi-process
            deployments (e.g. gunicorn with multiple workers).
        max_stream_response_bytes: When set, server-stream responses are
            broken into multiple HTTP exchanges once the response body
            exceeds this size.  The client transparently resumes via
            ``POST /{method}/exchange``.  ``None`` (default) disables
            resumable streaming.
        authenticate: Optional callback that extracts an :class:`AuthContext`
            from a Falcon ``Request``.  When provided, every request is
            authenticated before dispatch.  The callback should raise
            ``ValueError`` (bad credentials) or ``PermissionError``
            (forbidden) on failure — these are mapped to HTTP 401.
            Other exceptions propagate as 500.

    Returns:
        A Falcon application with routes for unary, server-stream,
        and bidi-stream RPC calls.

    """
    if signing_key is None:
        warnings.warn(
            "No signing_key provided; generating a random per-process key. "
            "State tokens will be invalid across workers — pass a shared key "
            "for multi-process deployments.",
            stacklevel=2,
        )
        signing_key = os.urandom(32)
    app_handler = _HttpRpcApp(server, signing_key, max_stream_response_bytes)
    app = falcon.App(middleware=[_AuthMiddleware(authenticate)] if authenticate is not None else None)
    app.add_route(f"{prefix}/{{method}}", _RpcResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/bidi", _BidiInitResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/exchange", _ExchangeResource(app_handler))
    return app


# ---------------------------------------------------------------------------
# Sync test client
# ---------------------------------------------------------------------------


class _SyncTestResponse:
    """Minimal response object matching what _HttpProxy expects from httpx.Response."""

    __slots__ = ("content", "status_code")

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class _SyncTestClient:
    """Sync HTTP client that calls a Falcon WSGI app directly via falcon.testing.TestClient."""

    __slots__ = ("_client", "_default_headers")

    def __init__(
        self,
        app: falcon.App[falcon.Request, falcon.Response],
        default_headers: dict[str, str] | None = None,
    ) -> None:
        self._client = falcon.testing.TestClient(app)
        self._default_headers: dict[str, str] = default_headers or {}

    def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> _SyncTestResponse:
        """Send a synchronous POST using the Falcon test client."""
        merged = {**self._default_headers, **headers}
        # Strip scheme+host if present (test_http.py passes full URLs)
        path = urlparse(url).path
        result = self._client.simulate_post(path, body=content, headers=merged)
        return _SyncTestResponse(result.status_code, result.content)

    def close(self) -> None:
        """Close the client (no-op for test client)."""


def make_sync_client(
    server: RpcServer,
    *,
    prefix: str = "/vgi",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    default_headers: dict[str, str] | None = None,
) -> _SyncTestClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``falcon.testing.TestClient`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        prefix: URL prefix for RPC endpoints (default ``/vgi``).
        signing_key: HMAC key for signing state tokens (see
            ``make_wsgi_app`` for details).
        max_stream_response_bytes: See ``make_wsgi_app``.
        authenticate: See ``make_wsgi_app``.
        default_headers: Headers merged into every request (e.g. auth tokens).

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_wsgi_app(
        server,
        prefix=prefix,
        signing_key=signing_key,
        max_stream_response_bytes=max_stream_response_bytes,
        authenticate=authenticate,
    )
    return _SyncTestClient(app, default_headers=default_headers)


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------


def _open_response_stream(content: bytes, status_code: int) -> ipc.RecordBatchStreamReader:
    """Open an Arrow IPC stream from HTTP response bytes.

    Args:
        content: Response body bytes.
        status_code: HTTP status code (used in error messages).

    Returns:
        An open IPC stream reader.

    Raises:
        RpcError: If the server returns 401 (``AuthenticationError``) or
            the response is not a valid Arrow IPC stream.

    """
    # 401 responses are plain text (not Arrow IPC) because they are produced
    # by _AuthMiddleware before any method is resolved, so no output schema
    # is available.  We surface them as RpcError("AuthenticationError").
    if status_code == HTTPStatus.UNAUTHORIZED:
        raise RpcError("AuthenticationError", content.decode(errors="replace"), "")
    try:
        return ipc.open_stream(BytesIO(content))
    except pa.ArrowInvalid:
        raise RpcError(
            "HttpError",
            f"HTTP {status_code}: response is not a valid Arrow IPC stream",
            "",
        ) from None


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

    __slots__ = ("_client", "_external_config", "_method", "_on_log", "_output_schema", "_state_bytes", "_url_prefix")

    def __init__(
        self,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        method: str,
        state_bytes: bytes,
        output_schema: pa.Schema,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
    ) -> None:
        """Initialize with HTTP client, method details, and initial state."""
        self._client = client
        self._url_prefix = url_prefix
        self._method = method
        self._state_bytes = state_bytes
        self._output_schema = output_schema
        self._on_log = on_log
        self._external_config = external_config

    def exchange(self, input_batch: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch and receive the output batch.

        Args:
            input_batch: The input batch to send.

        Returns:
            The output batch from the server.

        Raises:
            RpcError: If the server reports an error.

        """
        batch_to_write = input_batch.batch
        cm_to_write = input_batch.custom_metadata

        # Client-side production for large bidi inputs
        if self._external_config is not None:
            batch_to_write, cm_to_write = maybe_externalize_batch(batch_to_write, cm_to_write, self._external_config)

        # Write input batch with state in metadata
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({STATE_KEY: self._state_bytes})
        merged = merge_metadata(cm_to_write, state_md)
        with ipc.new_stream(req_buf, batch_to_write.schema) as writer:
            writer.write_batch(batch_to_write, custom_metadata=merged)

        resp = self._client.post(
            f"{self._url_prefix}/{self._method}/exchange",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )

        # Read response — log batches + data batch with state
        reader = _open_response_stream(resp.content, resp.status_code)
        try:
            ab = _read_batch_with_log_check(reader, self._on_log, self._external_config)
        except RpcError:
            _drain_stream(reader)
            raise

        # Extract updated state from metadata
        if ab.custom_metadata is not None:
            new_state = ab.custom_metadata.get(STATE_KEY)
            if new_state is not None:
                self._state_bytes = new_state

        # Strip state token from user-visible metadata
        user_cm = strip_keys(ab.custom_metadata, STATE_KEY)

        _drain_stream(reader)
        return AnnotatedBatch(batch=ab.batch, custom_metadata=user_cm)

    def close(self) -> None:
        """Close the session (no-op for HTTP — stateless)."""

    def __enter__(self) -> HttpBidiSession:
        """Enter the context."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context."""
        self.close()


class HttpStreamSession:
    """Client-side handle for a server stream over HTTP with transparent continuation.

    Iterates over ``AnnotatedBatch`` objects from the server.  When the server
    sends a zero-row batch with a ``STATE_KEY`` continuation token, the session
    transparently sends a ``POST /{method}/exchange`` to resume the stream.
    """

    __slots__ = ("_client", "_external_config", "_method", "_on_log", "_reader", "_url_prefix")

    def __init__(
        self,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        method: str,
        reader: ipc.RecordBatchStreamReader,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
    ) -> None:
        """Initialize with HTTP client, method details, and initial response reader."""
        self._client = client
        self._url_prefix = url_prefix
        self._method = method
        self._reader = reader
        self._on_log = on_log
        self._external_config = external_config

    def _send_continuation(self, token: bytes) -> ipc.RecordBatchStreamReader:
        """Send a continuation request and return the new response reader."""
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({STATE_KEY: token})
        with ipc.new_stream(req_buf, _EMPTY_SCHEMA) as writer:
            writer.write_batch(empty_batch(_EMPTY_SCHEMA), custom_metadata=state_md)

        resp = self._client.post(
            f"{self._url_prefix}/{self._method}/exchange",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        return _open_response_stream(resp.content, resp.status_code)

    def __iter__(self) -> Iterator[AnnotatedBatch]:  # noqa: D105
        try:
            while True:
                try:
                    batch, custom_metadata = self._reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break

                # Check for continuation token (zero-row batch with STATE_KEY)
                if batch.num_rows == 0 and custom_metadata is not None:
                    token = custom_metadata.get(STATE_KEY)
                    if token is not None:
                        if not isinstance(token, bytes):
                            raise TypeError(f"Expected bytes for state token, got {type(token).__name__}")
                        _drain_stream(self._reader)
                        self._reader = self._send_continuation(token)
                        continue

                # Dispatch log/error batches
                if _dispatch_log_or_error(batch, custom_metadata, self._on_log):
                    continue

                resolved_batch, resolved_cm = resolve_external_location(
                    batch, custom_metadata, self._external_config, self._on_log
                )
                yield AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm)
        except RpcError:
            _drain_stream(self._reader)
            raise


@contextlib.contextmanager
def http_connect(
    protocol: type,
    base_url: str | None = None,
    *,
    prefix: str = "/vgi",
    on_log: Callable[[Message], None] | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    external_location: ExternalLocationConfig | None = None,
) -> Iterator[_HttpProxy]:
    """Connect to an HTTP RPC server and yield a typed proxy.

    Args:
        protocol: The Protocol class defining the RPC interface.
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``; ignored when a pre-built
            *client* is provided.  The internally-created client follows
            redirects (307/308) transparently.
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        on_log: Optional callback for log messages from the server.
        client: Optional HTTP client — ``httpx.Client`` for production,
            or a ``_SyncTestClient`` from ``make_sync_client()`` for testing.
        external_location: Optional ExternalLocation configuration for
            resolving and producing externalized batches.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    Raises:
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)

    url_prefix = prefix
    try:
        yield _HttpProxy(protocol, client, url_prefix, on_log, external_config=external_location)
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
        *,
        external_config: ExternalLocationConfig | None = None,
    ) -> None:
        self._protocol = protocol
        self._client = client
        self._url_prefix = url_prefix
        self._methods = rpc_methods(protocol)
        self._on_log = on_log
        self._external_config = external_config

    def __getattr__(self, name: str) -> Any:
        """Resolve RPC method names to callable proxies, caching on first access.

        Returns ``Any`` because each method name maps to a different callable
        signature (unary, server-stream, or bidi), so no single static return
        type can represent all of them.
        """
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
        ext_cfg = self._external_config

        def caller(**kwargs: object) -> object:
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            reader = _open_response_stream(resp.content, resp.status_code)
            return _read_unary_response(reader, info, on_log, ext_cfg)

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., HttpStreamSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config

        def caller(**kwargs: object) -> HttpStreamSession:
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            reader = _open_response_stream(resp.content, resp.status_code)
            return HttpStreamSession(
                client=client,
                url_prefix=url_prefix,
                method=info.name,
                reader=reader,
                on_log=on_log,
                external_config=ext_cfg,
            )

        return caller

    def _make_bidi_caller(self, info: RpcMethodInfo) -> Callable[..., HttpBidiSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config

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
            reader = _open_response_stream(resp.content, resp.status_code)
            output_schema = reader.schema
            state_bytes: bytes | None = None

            try:
                ab = _read_batch_with_log_check(reader, on_log)
                # This is the zero-row batch with state
                if ab.custom_metadata is not None:
                    state_bytes = ab.custom_metadata.get(STATE_KEY)
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
                external_config=ext_cfg,
            )

        return caller
