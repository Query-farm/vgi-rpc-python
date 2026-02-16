"""HTTP transport for vgi-rpc using Falcon (server) and httpx (client).

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application, and ``http_connect`` to call it from Python with ``httpx``.

HTTP Wire Protocol
------------------
All endpoints use ``Content-Type: application/vnd.apache.arrow.stream``.

- **Unary**: ``POST /vgi/{method}``
- **Stream Init**: ``POST /vgi/{method}/init``
- **Stream Exchange**: ``POST /vgi/{method}/exchange``

Streaming is implemented statelessly: each exchange is a separate HTTP POST
carrying serialized state in Arrow custom metadata (``vgi_rpc.stream_state``).
When ``max_stream_response_bytes`` is set, producer-stream responses are
split across multiple exchanges; the client transparently resumes via
``POST /{method}/exchange``.

Optional dependencies: ``pip install vgi-rpc[http]``
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import logging
import os
import struct
import time
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from io import BytesIO, IOBase
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, cast, get_args, get_origin, get_type_hints
from urllib.parse import urlparse

import falcon
import falcon.testing
import httpx
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import (
    ExternalLocationConfig,
    UploadUrl,
    UploadUrlProvider,
    maybe_externalize_batch,
    resolve_external_location,
)
from vgi_rpc.log import Message
from vgi_rpc.metadata import STATE_KEY, merge_metadata, strip_keys
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    _TICK_BATCH,
    AnnotatedBatch,
    AuthContext,
    CallContext,
    MethodType,
    OutputCollector,
    RpcError,
    RpcMethodInfo,
    RpcServer,
    Stream,
    StreamState,
    VersionError,
    _ClientLogSink,
    _current_transport,
    _deserialize_params,
    _dispatch_log_or_error,
    _drain_stream,
    _emit_access_log,
    _flush_collector,
    _get_auth_and_metadata,
    _log_method_error,
    _read_batch_with_log_check,
    _read_request,
    _read_unary_response,
    _send_request,
    _TransportContext,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_request,
    _write_result_batch,
    rpc_methods,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader, empty_batch

if TYPE_CHECKING:
    from vgi_rpc.introspect import ServiceDescription

__all__ = [
    "HttpServerCapabilities",
    "HttpStreamSession",
    "MAX_REQUEST_BYTES_HEADER",
    "MAX_UPLOAD_BYTES_HEADER",
    "UPLOAD_URL_HEADER",
    "http_capabilities",
    "http_connect",
    "http_introspect",
    "make_sync_client",
    "make_wsgi_app",
    "request_upload_urls",
]

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes"
UPLOAD_URL_HEADER = "VGI-Upload-URL-Support"
MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes"
_logger = logging.getLogger(__name__)

_MAX_UPLOAD_URL_COUNT = 100

_UPLOAD_URL_METHOD = "__upload_url__"
_upload_url_params_fields: list[pa.Field[Any]] = [pa.field("count", pa.int64())]
_UPLOAD_URL_PARAMS_SCHEMA = pa.schema(_upload_url_params_fields)
_upload_url_fields: list[pa.Field[Any]] = [
    pa.field("upload_url", pa.utf8()),
    pa.field("download_url", pa.utf8()),
    pa.field("expires_at", pa.timestamp("us", tz="UTC")),
]
_UPLOAD_URL_SCHEMA = pa.schema(_upload_url_fields)


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
_MIN_TOKEN_LEN = _HEADER_LEN * 3 + _HMAC_LEN  # three length prefixes + HMAC


def _pack_state_token(state_bytes: bytes, schema_bytes: bytes, input_schema_bytes: bytes, signing_key: bytes) -> bytes:
    """Pack state, output schema, and input schema bytes into a signed token.

    Wire format::

        [4 bytes: state_len  (uint32 LE)]
        [state_len bytes: state_bytes]
        [4 bytes: schema_len (uint32 LE)]
        [schema_len bytes: schema_bytes]
        [4 bytes: input_schema_len (uint32 LE)]
        [input_schema_len bytes: input_schema_bytes]
        [32 bytes: HMAC-SHA256(key, all above)]

    Args:
        state_bytes: Serialized state (Arrow IPC).
        schema_bytes: Serialized output ``pa.Schema``.
        input_schema_bytes: Serialized input ``pa.Schema``.
        signing_key: HMAC signing key.

    Returns:
        The opaque signed token.

    """
    payload = (
        struct.pack("<I", len(state_bytes))
        + state_bytes
        + struct.pack("<I", len(schema_bytes))
        + schema_bytes
        + struct.pack("<I", len(input_schema_bytes))
        + input_schema_bytes
    )
    mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return payload + mac


def _unpack_state_token(token: bytes, signing_key: bytes) -> tuple[bytes, bytes, bytes]:
    """Unpack and verify a signed state token.

    Args:
        token: The opaque token produced by ``_pack_state_token``.
        signing_key: HMAC signing key (must match the one used to pack).

    Returns:
        ``(state_bytes, schema_bytes, input_schema_bytes)``

    Raises:
        _RpcHttpError: On malformed or tampered tokens (HTTP 400).

    """
    if len(token) < _MIN_TOKEN_LEN:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    def _read_segment(data: bytes, pos: int) -> tuple[bytes, int]:
        if pos + _HEADER_LEN > len(data):
            raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)
        seg_len = struct.unpack_from("<I", data, pos)[0]
        seg_end = pos + _HEADER_LEN + seg_len
        if seg_end > len(data):
            raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)
        return data[pos + _HEADER_LEN : seg_end], seg_end

    state_bytes, pos = _read_segment(token, 0)
    schema_bytes, pos = _read_segment(token, pos)
    input_schema_bytes, payload_end = _read_segment(token, pos)

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

    return state_bytes, schema_bytes, input_schema_bytes


def _resolve_state_types(
    server: RpcServer,
) -> dict[str, type[StreamState]]:
    """Introspect server implementation to map method names to concrete state types.

    Examines the return type hints of each stream method on the
    implementation (not the protocol) to extract the concrete
    ``StreamState`` subclass.

    Args:
        server: The ``RpcServer`` whose implementation to introspect.

    Returns:
        Mapping of method name to concrete state subclass.

    """
    result: dict[str, type[StreamState]] = {}
    for name, info in server.methods.items():
        if info.method_type != MethodType.STREAM:
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
        if origin is Stream:
            args = get_args(return_hint)
            if args and isinstance(args[0], type) and issubclass(args[0], StreamState):
                result[name] = args[0]
    return result


# ---------------------------------------------------------------------------
# Server — Falcon WSGI resources
# ---------------------------------------------------------------------------


class _HttpRpcApp:
    """Internal helper that wraps an RpcServer and manages stream state."""

    __slots__ = (
        "_max_request_bytes",
        "_max_stream_response_bytes",
        "_max_upload_bytes",
        "_server",
        "_signing_key",
        "_state_types",
        "_upload_url_provider",
    )

    def __init__(
        self,
        server: RpcServer,
        signing_key: bytes,
        max_stream_response_bytes: int | None = None,
        max_request_bytes: int | None = None,
        upload_url_provider: UploadUrlProvider | None = None,
        max_upload_bytes: int | None = None,
    ) -> None:
        self._server = server
        self._signing_key = signing_key
        self._state_types = _resolve_state_types(server)
        self._max_stream_response_bytes = max_stream_response_bytes
        self._max_request_bytes = max_request_bytes
        self._upload_url_provider = upload_url_provider
        self._max_upload_bytes = max_upload_bytes

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
            ipc_method, kwargs = _read_request(stream, self._server.ipc_validation)
            if ipc_method != method_name:
                raise TypeError(f"Method mismatch: URL '{method_name}' vs IPC metadata '{ipc_method}'")
            _deserialize_params(kwargs, info.param_types, self._server.ipc_validation)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Pre-built __describe__ batch — write directly, skip implementation call.
        # __describe__ is metadata introspection — no access log.
        describe_batch = self._server._describe_batch
        if describe_batch is not None and method_name == "__describe__":
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, describe_batch.schema) as writer:
                writer.write_batch(describe_batch)
            resp_buf.seek(0)
            return resp_buf, HTTPStatus.OK

        server_id = self._server.server_id
        protocol_name = self._server.protocol_name
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in self._server.ctx_methods:
            kwargs["ctx"] = CallContext(
                auth=auth,
                emit_client_log=sink,
                transport_metadata=transport_metadata,
                server_id=server_id,
                method_name=method_name,
                protocol_name=protocol_name,
            )

        schema = info.result_schema
        resp_buf = BytesIO()
        http_status = HTTPStatus.OK
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        try:
            with ipc.new_stream(resp_buf, schema) as writer:
                sink.flush_contents(writer, schema)
                try:
                    result = getattr(self._server.implementation, method_name)(**kwargs)
                    _validate_result(info.name, result, info.result_type)
                    _write_result_batch(writer, schema, result, self._server.external_config)
                except (TypeError, pa.ArrowInvalid) as exc:
                    status = "error"
                    error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    _write_error_batch(writer, schema, exc, server_id=server_id)
                    http_status = HTTPStatus.BAD_REQUEST
                except Exception as exc:
                    status = "error"
                    error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    _write_error_batch(writer, schema, exc, server_id=server_id)
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                method_name,
                info.method_type.value,
                server_id,
                auth,
                transport_metadata,
                duration_ms,
                status,
                error_type,
                http_status=http_status.value,
            )

        resp_buf.seek(0)
        return resp_buf, http_status

    def _stream_init_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> BytesIO:
        """Run stream init synchronously.

        For producer streams (input_schema == _EMPTY_SCHEMA), produces data
        immediately via _produce_stream_response.
        For exchange streams, returns a state token for subsequent exchanges.
        """
        try:
            ipc_method, kwargs = _read_request(stream, self._server.ipc_validation)
            if ipc_method != method_name:
                raise TypeError(f"Method mismatch: URL '{method_name}' vs IPC metadata '{ipc_method}'")
            _deserialize_params(kwargs, info.param_types, self._server.ipc_validation)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Inject ctx if the implementation accepts it
        server_id = self._server.server_id
        protocol_name = self._server.protocol_name
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in self._server.ctx_methods:
            kwargs["ctx"] = CallContext(
                auth=auth,
                emit_client_log=sink,
                transport_metadata=transport_metadata,
                server_id=server_id,
                method_name=method_name,
                protocol_name=protocol_name,
            )

        start = time.monotonic()
        http_status = HTTPStatus.OK
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        try:
            try:
                result: Stream[StreamState] = getattr(self._server.implementation, method_name)(**kwargs)
            except (TypeError, pa.ArrowInvalid) as exc:
                status = "error"
                error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                http_status = HTTPStatus.BAD_REQUEST
                raise _RpcHttpError(exc, status_code=http_status) from exc
            except Exception as exc:
                status = "error"
                error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                raise _RpcHttpError(exc, status_code=http_status) from exc

            is_producer = result.input_schema == _EMPTY_SCHEMA

            if is_producer:
                # Producer stream — run produce loop directly
                resp_buf, produce_error_type = self._produce_stream_response(
                    result.output_schema,
                    result.state,
                    result.input_schema,
                    sink,
                    method_name=method_name,
                    auth=auth,
                    transport_metadata=transport_metadata,
                )
                if produce_error_type is not None:
                    status = "error"
                    error_type = produce_error_type
                return resp_buf
            else:
                # Exchange stream — return state token
                try:
                    state = result.state
                    output_schema = result.output_schema
                    input_schema = result.input_schema

                    # Pack state + output schema + input schema into a signed token
                    state_bytes = state.serialize_to_bytes()
                    schema_bytes = output_schema.serialize().to_pybytes()
                    input_schema_bytes = input_schema.serialize().to_pybytes()
                    token = _pack_state_token(state_bytes, schema_bytes, input_schema_bytes, self._signing_key)

                    # Write response: log batches + zero-row batch with token in metadata
                    resp_buf = BytesIO()
                    with ipc.new_stream(resp_buf, output_schema) as writer:
                        sink.flush_contents(writer, output_schema)
                        state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                        zero_batch = empty_batch(output_schema)
                        writer.write_batch(zero_batch, custom_metadata=state_metadata)
                except Exception as exc:
                    status = "error"
                    error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                    raise _RpcHttpError(exc, status_code=http_status) from exc

                resp_buf.seek(0)
                return resp_buf
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                method_name,
                info.method_type.value,
                server_id,
                auth,
                transport_metadata,
                duration_ms,
                status,
                error_type,
                http_status=http_status.value,
            )

    def _stream_exchange_sync(self, method_name: str, stream: IOBase) -> BytesIO:
        """Run stream exchange synchronously.

        Dispatches to producer continuation or exchange based on input_schema
        recovered from the state token.
        """
        state_cls = self._state_types.get(method_name)
        if state_cls is None:
            raise _RpcHttpError(
                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        # Read the input batch + extract token from metadata
        try:
            req_reader = ValidatedReader(ipc.open_stream(stream), self._server.ipc_validation)
            input_batch, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
            _drain_stream(req_reader)
        except pa.ArrowInvalid as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Extract state token before resolution — resolve_external_location
        # replaces metadata with what was stored in the external IPC stream.
        token = custom_metadata.get(STATE_KEY) if custom_metadata is not None else None

        if token is None:
            raise _RpcHttpError(
                RuntimeError("Missing state token in exchange request"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        # Unpack and verify the signed token, recover state + schema + input_schema
        state_obj, output_schema, input_schema = self._unpack_and_recover_state(token, state_cls)

        is_producer = input_schema == _EMPTY_SCHEMA

        if is_producer:
            # Producer continuation
            server_id = self._server.server_id
            protocol_name = self._server.protocol_name
            auth, transport_metadata = _get_auth_and_metadata()
            start = time.monotonic()
            http_status = HTTPStatus.OK
            status: Literal["ok", "error"] = "ok"
            error_type = ""
            try:
                resp_buf, produce_error_type = self._produce_stream_response(
                    output_schema,
                    state_obj,
                    input_schema,
                    method_name=method_name,
                )
                if produce_error_type is not None:
                    status = "error"
                    error_type = produce_error_type
                return resp_buf
            finally:
                duration_ms = (time.monotonic() - start) * 1000
                _emit_access_log(
                    protocol_name,
                    method_name,
                    "stream",
                    server_id,
                    auth,
                    transport_metadata,
                    duration_ms,
                    status,
                    error_type,
                    http_status=http_status.value,
                )
        else:
            # Exchange — resolve external locations on real input data
            try:
                input_batch, resolved_cm = resolve_external_location(
                    input_batch,
                    custom_metadata,
                    self._server.external_config,
                    ipc_validation=self._server.ipc_validation,
                )
            except Exception as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

            server_id = self._server.server_id
            protocol_name = self._server.protocol_name
            auth, transport_md = _get_auth_and_metadata()
            start = time.monotonic()
            http_status = HTTPStatus.OK
            status_str: Literal["ok", "error"] = "ok"
            error_type_str = ""
            try:
                # Strip state token from metadata visible to process()
                user_cm = strip_keys(resolved_cm, STATE_KEY)

                ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
                out = OutputCollector(output_schema, server_id=server_id)

                process_ctx = CallContext(
                    auth=auth,
                    emit_client_log=out.emit_client_log_message,
                    transport_metadata=transport_md,
                    server_id=server_id,
                    method_name=method_name,
                    protocol_name=protocol_name,
                )
                state_obj.process(ab_in, out, process_ctx)
                if not out.finished:
                    out.validate()

                # Repack updated state with same schemas into new signed token
                updated_state_bytes = state_obj.serialize_to_bytes()
                schema_bytes = output_schema.serialize().to_pybytes()
                input_schema_bytes_upd = input_schema.serialize().to_pybytes()
                updated_token = _pack_state_token(
                    updated_state_bytes, schema_bytes, input_schema_bytes_upd, self._signing_key
                )
                out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

                # Write response batches (log + data, in order)
                resp_buf = BytesIO()
                with ipc.new_stream(resp_buf, output_schema) as writer:
                    _flush_collector(writer, out, self._server.external_config)
            except Exception as exc:
                status_str = "error"
                error_type_str = _log_method_error(protocol_name, method_name, server_id, exc)
                http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                raise _RpcHttpError(exc, status_code=http_status, schema=output_schema) from exc
            finally:
                duration_ms = (time.monotonic() - start) * 1000
                _emit_access_log(
                    protocol_name,
                    method_name,
                    "stream",
                    server_id,
                    auth,
                    transport_md,
                    duration_ms,
                    status_str,
                    error_type_str,
                    http_status=http_status.value,
                )

            resp_buf.seek(0)
            return resp_buf

    def _produce_stream_response(
        self,
        schema: pa.Schema,
        state: StreamState,
        input_schema: pa.Schema,
        sink: _ClientLogSink | None = None,
        *,
        method_name: str,
        auth: AuthContext | None = None,
        transport_metadata: Mapping[str, Any] | None = None,
    ) -> tuple[BytesIO, str | None]:
        """Run the produce loop for a producer stream, with optional size-based continuation.

        Args:
            schema: The output schema for the stream.
            state: The stream state object.
            input_schema: The input schema (stored in continuation tokens).
            sink: Optional log sink to flush before producing (initial request only).
            method_name: The RPC method name (for logging context).
            auth: Auth context; falls back to contextvar when ``None`` (continuation path).
            transport_metadata: Transport metadata; falls back to contextvar when ``None``.

        Returns:
            A ``(BytesIO, error_type)`` tuple — the IPC response stream and the
            exception class name on produce-loop failure (``None`` on success).

        """
        if auth is None or transport_metadata is None:
            cv_auth, cv_md = _get_auth_and_metadata()
            auth = auth if auth is not None else cv_auth
            transport_metadata = transport_metadata if transport_metadata is not None else cv_md

        server_id = self._server.server_id
        protocol_name = self._server.protocol_name
        resp_buf = BytesIO()
        max_bytes = self._max_stream_response_bytes
        produce_error_type: str | None = None
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
                        server_id=server_id,
                        method_name=method_name,
                        protocol_name=protocol_name,
                    )
                    state.process(_TICK_BATCH, out, produce_ctx)
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
                        input_schema_bytes = input_schema.serialize().to_pybytes()
                        token = _pack_state_token(state_bytes, schema_bytes, input_schema_bytes, self._signing_key)
                        state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                        writer.write_batch(empty_batch(schema), custom_metadata=state_metadata)
                        break
            except Exception as exc:
                produce_error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                _write_error_batch(writer, schema, exc, server_id=server_id)
        resp_buf.seek(0)
        return resp_buf, produce_error_type

    def _unpack_and_recover_state(
        self,
        token: bytes,
        state_cls: type[StreamState],
    ) -> tuple[StreamState, pa.Schema, pa.Schema]:
        """Unpack a signed state token and recover state, output schema, and input schema.

        Args:
            token: The signed state token bytes.
            state_cls: The concrete state class to deserialize into.

        Returns:
            Tuple of (state_object, output_schema, input_schema).

        Raises:
            _RpcHttpError: On malformed tokens, failed deserialization, or
                signature verification failure.

        """
        state_bytes, schema_bytes, input_schema_bytes = _unpack_state_token(token, self._signing_key)

        try:
            output_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize output schema: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        try:
            input_schema = pa.ipc.read_schema(pa.py_buffer(input_schema_bytes))
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize input schema: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        try:
            state_obj = state_cls.deserialize_from_bytes(state_bytes, self._server.ipc_validation)
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize state: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        return state_obj, output_schema, input_schema


class _RpcResource:
    """Falcon resource for unary calls: ``POST {prefix}/{method}``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle unary and __describe__ RPC calls."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type == MethodType.STREAM:
                raise _RpcHttpError(
                    TypeError(f"Stream method '{method}' requires /init and /exchange endpoints"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            result_stream, http_status = self._app._unary_sync(method, info, req.bounded_stream)
            resp.content_type = _ARROW_CONTENT_TYPE
            resp.stream = result_stream
            resp.status = str(http_status.value)
        except _RpcHttpError as e:
            _set_error_response(
                resp,
                e.cause,
                status_code=e.status_code,
                schema=e.schema,
                server_id=self._app._server.server_id,
            )


class _StreamInitResource:
    """Falcon resource for stream init: ``POST {prefix}/{method}/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle stream initialization (both producer and exchange)."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type != MethodType.STREAM:
                raise _RpcHttpError(
                    TypeError(f"Method '{method}' is not a stream"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )
            result_stream = self._app._stream_init_sync(method, info, req.bounded_stream)
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
    """Falcon resource for state exchange: ``POST {prefix}/{method}/exchange``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response, method: str) -> None:
        """Handle stream exchange or producer continuation."""
        try:
            info = self._app._resolve_method(req, method)
            if info.method_type != MethodType.STREAM:
                raise _RpcHttpError(
                    TypeError(f"Method '{method}' does not support /exchange"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )
            result_stream = self._app._stream_exchange_sync(method, req.bounded_stream)
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


class _UploadUrlResource:
    """Falcon resource for upload URL generation: ``POST {prefix}/__upload_url__/init``."""

    def __init__(self, app: _HttpRpcApp) -> None:
        self._app = app

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Handle upload URL generation requests."""
        try:
            _check_content_type(req)
            # Route is only registered when upload_url_provider is set.
            provider = self._app._upload_url_provider
            assert provider is not None

            # Read request using standard wire protocol
            try:
                ipc_method, kwargs = _read_request(req.bounded_stream, self._app._server.ipc_validation)
                if ipc_method != _UPLOAD_URL_METHOD:
                    raise TypeError(f"Method mismatch: expected '{_UPLOAD_URL_METHOD}', got '{ipc_method}'")
            except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

            count = kwargs.get("count", 1)
            if not isinstance(count, int):
                count = 1
            count = max(1, min(count, _MAX_UPLOAD_URL_COUNT))
        except _RpcHttpError as e:
            _set_error_response(resp, e.cause, status_code=e.status_code, server_id=self._app._server.server_id)
            return

        # Execute: follows the same response pattern as _unary_sync —
        # log sink, inline error batches, access logging.
        server_id = self._app._server.server_id
        protocol_name = self._app._server.protocol_name
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()

        resp_buf = BytesIO()
        http_status = HTTPStatus.OK
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        try:
            with ipc.new_stream(resp_buf, _UPLOAD_URL_SCHEMA) as writer:
                sink.flush_contents(writer, _UPLOAD_URL_SCHEMA)
                try:
                    urls = [provider.generate_upload_url(pa.schema([])) for _ in range(count)]
                    result_batch = pa.RecordBatch.from_pydict(
                        {
                            "upload_url": [u.upload_url for u in urls],
                            "download_url": [u.download_url for u in urls],
                            "expires_at": [u.expires_at for u in urls],
                        },
                        schema=_UPLOAD_URL_SCHEMA,
                    )
                    writer.write_batch(result_batch)
                except Exception as exc:
                    status = "error"
                    error_type = _log_method_error(protocol_name, _UPLOAD_URL_METHOD, server_id, exc)
                    _write_error_batch(writer, _UPLOAD_URL_SCHEMA, exc, server_id=server_id)
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                _UPLOAD_URL_METHOD,
                "unary",
                server_id,
                auth,
                transport_metadata,
                duration_ms,
                status,
                error_type,
                http_status=http_status.value,
            )

        resp_buf.seek(0)
        resp.content_type = _ARROW_CONTENT_TYPE
        resp.stream = resp_buf
        resp.status = str(http_status.value)


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
            _logger.warning(
                "Auth failure from %s: %s",
                req.remote_addr,
                exc,
                extra={
                    "remote_addr": req.remote_addr or "",
                    "error_type": type(exc).__name__,
                    "auth_error": str(exc),
                },
            )
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


class _CapabilitiesMiddleware:
    """Falcon middleware that sets capability headers on every response."""

    __slots__ = ("_headers",)

    def __init__(self, headers: dict[str, str]) -> None:
        self._headers = headers

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Set capability headers on every response."""
        for name, value in self._headers.items():
            resp.set_header(name, value)


def make_wsgi_app(
    server: RpcServer,
    *,
    prefix: str = "/vgi",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    cors_origins: str | Iterable[str] | None = None,
    upload_url_provider: UploadUrlProvider | None = None,
    max_upload_bytes: int | None = None,
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
        max_stream_response_bytes: When set, producer stream responses are
            broken into multiple HTTP exchanges once the response body
            exceeds this size.  The client transparently resumes via
            ``POST /{method}/exchange``.  ``None`` (default) disables
            resumable streaming.
        max_request_bytes: When set, the value is advertised via the
            ``VGI-Max-Request-Bytes`` response header on every response
            (including OPTIONS).  Clients can use ``http_capabilities()``
            to discover this limit and decide whether to use external
            storage for large payloads.  Advertisement only — no
            server-side enforcement.  ``None`` (default) omits the header.
        authenticate: Optional callback that extracts an :class:`AuthContext`
            from a Falcon ``Request``.  When provided, every request is
            authenticated before dispatch.  The callback should raise
            ``ValueError`` (bad credentials) or ``PermissionError``
            (forbidden) on failure — these are mapped to HTTP 401.
            Other exceptions propagate as 500.
        cors_origins: Allowed origins for CORS.  Pass ``"*"`` to allow all
            origins, a single origin string like ``"https://example.com"``,
            or an iterable of origin strings.  ``None`` (the default)
            disables CORS headers.  Uses Falcon's built-in
            ``CORSMiddleware`` which also handles preflight OPTIONS
            requests automatically.
        upload_url_provider: Optional provider for generating pre-signed
            upload URLs.  When set, the ``__upload_url__/init`` endpoint
            is enabled and ``VGI-Upload-URL-Support: true`` is advertised
            on every response.
        max_upload_bytes: When set (and ``upload_url_provider`` is set),
            advertised via the ``VGI-Max-Upload-Bytes`` header.  Informs
            clients of the maximum size they may upload to vended URLs.
            Advertisement only — no server-side enforcement.

    Returns:
        A Falcon application with routes for unary and stream RPC calls.

    """
    if signing_key is None:
        warnings.warn(
            "No signing_key provided; generating a random per-process key. "
            "State tokens will be invalid across workers — pass a shared key "
            "for multi-process deployments.",
            stacklevel=2,
        )
        signing_key = os.urandom(32)
    app_handler = _HttpRpcApp(
        server, signing_key, max_stream_response_bytes, max_request_bytes, upload_url_provider, max_upload_bytes
    )
    middleware: list[Any] = []
    cors_expose: list[str] = []

    # Build capability headers
    capability_headers: dict[str, str] = {}
    if max_request_bytes is not None:
        capability_headers[MAX_REQUEST_BYTES_HEADER] = str(max_request_bytes)
        cors_expose.append(MAX_REQUEST_BYTES_HEADER)
    if upload_url_provider is not None:
        capability_headers[UPLOAD_URL_HEADER] = "true"
        cors_expose.append(UPLOAD_URL_HEADER)
        if max_upload_bytes is not None:
            capability_headers[MAX_UPLOAD_BYTES_HEADER] = str(max_upload_bytes)
            cors_expose.append(MAX_UPLOAD_BYTES_HEADER)

    if cors_origins is not None:
        cors_kwargs: dict[str, Any] = {"allow_origins": cors_origins}
        if cors_expose:
            cors_kwargs["expose_headers"] = cors_expose
        middleware.append(falcon.CORSMiddleware(**cors_kwargs))
    if authenticate is not None:
        middleware.append(_AuthMiddleware(authenticate))
    if capability_headers:
        middleware.append(_CapabilitiesMiddleware(capability_headers))
    app: falcon.App[falcon.Request, falcon.Response] = falcon.App(middleware=middleware or None)
    app.add_route(f"{prefix}/{{method}}", _RpcResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/init", _StreamInitResource(app_handler))
    app.add_route(f"{prefix}/{{method}}/exchange", _ExchangeResource(app_handler))
    if upload_url_provider is not None:
        app.add_route(f"{prefix}/__upload_url__/init", _UploadUrlResource(app_handler))

    _logger.info(
        "WSGI app created for %s (server_id=%s, prefix=%s, auth=%s)",
        server.protocol_name,
        server.server_id,
        prefix,
        "enabled" if authenticate is not None else "disabled",
        extra={
            "server_id": server.server_id,
            "protocol": server.protocol_name,
            "prefix": prefix,
            "auth_enabled": authenticate is not None,
        },
    )

    return app


# ---------------------------------------------------------------------------
# Sync test client
# ---------------------------------------------------------------------------


class _SyncTestResponse:
    """Minimal response object matching what _HttpProxy expects from httpx.Response."""

    __slots__ = ("content", "headers", "status_code")

    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers: dict[str, str] = headers or {}


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
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def options(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
        """Send a synchronous OPTIONS using the Falcon test client."""
        merged = {**self._default_headers, **(headers or {})}
        path = urlparse(url).path
        result = self._client.simulate_options(path, headers=merged)
        return _SyncTestResponse(result.status_code, result.content, headers=dict(result.headers))

    def close(self) -> None:
        """Close the client (no-op for test client)."""


def make_sync_client(
    server: RpcServer,
    *,
    prefix: str = "/vgi",
    signing_key: bytes | None = None,
    max_stream_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    default_headers: dict[str, str] | None = None,
    upload_url_provider: UploadUrlProvider | None = None,
    max_upload_bytes: int | None = None,
) -> _SyncTestClient:
    """Create a synchronous test client for an RpcServer.

    Uses ``falcon.testing.TestClient`` internally — no real HTTP server needed.

    Args:
        server: The RpcServer to test.
        prefix: URL prefix for RPC endpoints (default ``/vgi``).
        signing_key: HMAC key for signing state tokens (see
            ``make_wsgi_app`` for details).
        max_stream_response_bytes: See ``make_wsgi_app``.
        max_request_bytes: See ``make_wsgi_app``.
        authenticate: See ``make_wsgi_app``.
        default_headers: Headers merged into every request (e.g. auth tokens).
        upload_url_provider: See ``make_wsgi_app``.
        max_upload_bytes: See ``make_wsgi_app``.

    Returns:
        A sync client that can be passed to ``http_connect(client=...)``.

    """
    app = make_wsgi_app(
        server,
        prefix=prefix,
        signing_key=signing_key,
        max_stream_response_bytes=max_stream_response_bytes,
        max_request_bytes=max_request_bytes,
        authenticate=authenticate,
        upload_url_provider=upload_url_provider,
        max_upload_bytes=max_upload_bytes,
    )
    return _SyncTestClient(app, default_headers=default_headers)


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------


def _open_response_stream(
    content: bytes,
    status_code: int,
    ipc_validation: IpcValidation = IpcValidation.NONE,
) -> ValidatedReader:
    """Open an Arrow IPC stream from HTTP response bytes.

    Args:
        content: Response body bytes.
        status_code: HTTP status code (used in error messages).
        ipc_validation: Validation level for batches read from the stream.

    Returns:
        A ``ValidatedReader`` wrapping the IPC stream.

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
        return ValidatedReader(ipc.open_stream(BytesIO(content)), ipc_validation)
    except pa.ArrowInvalid:
        raise RpcError(
            "HttpError",
            f"HTTP {status_code}: response is not a valid Arrow IPC stream",
            "",
        ) from None


# ---------------------------------------------------------------------------
# Client — http_connect + HttpStreamSession
# ---------------------------------------------------------------------------


class HttpStreamSession:
    """Client-side handle for a stream over HTTP (both producer and exchange patterns).

    For producer streams, use ``__iter__()`` — yields batches from batched
    responses and follows continuation tokens transparently.
    For exchange streams, use ``exchange()`` — sends an input batch and
    receives an output batch.

    Supports context manager protocol for convenience.
    """

    __slots__ = (
        "_client",
        "_external_config",
        "_finished",
        "_ipc_validation",
        "_method",
        "_on_log",
        "_output_schema",
        "_pending_batches",
        "_state_bytes",
        "_url_prefix",
    )

    def __init__(
        self,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        method: str,
        state_bytes: bytes | None,
        output_schema: pa.Schema,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.NONE,
        pending_batches: list[AnnotatedBatch] | None = None,
        finished: bool = False,
    ) -> None:
        """Initialize with HTTP client, method details, and initial state."""
        self._client = client
        self._url_prefix = url_prefix
        self._method = method
        self._state_bytes = state_bytes
        self._output_schema = output_schema
        self._on_log = on_log
        self._external_config = external_config
        self._ipc_validation = ipc_validation
        self._pending_batches: list[AnnotatedBatch] = pending_batches or []
        self._finished = finished

    def exchange(self, input_batch: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch and receive the output batch.

        Args:
            input_batch: The input batch to send.

        Returns:
            The output batch from the server.

        Raises:
            RpcError: If the server reports an error or the stream has finished.

        """
        if self._state_bytes is None:
            raise RpcError("ProtocolError", "Stream has finished — no state token available", "")

        batch_to_write = input_batch.batch
        cm_to_write = input_batch.custom_metadata

        # Client-side externalization for large inputs
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
        reader = _open_response_stream(resp.content, resp.status_code, self._ipc_validation)
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

    def _send_continuation(self, token: bytes) -> ValidatedReader:
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
        return _open_response_stream(resp.content, resp.status_code, self._ipc_validation)

    def __iter__(self) -> Iterator[AnnotatedBatch]:
        """Iterate over output batches from a producer stream.

        Yields pre-loaded batches from init, then follows continuation tokens.
        """
        # Yield pre-loaded batches from init response
        yield from self._pending_batches
        self._pending_batches.clear()

        if self._finished:
            return

        # Follow continuation tokens
        if self._state_bytes is None:
            return

        reader: ValidatedReader | None = None
        try:
            reader = self._send_continuation(self._state_bytes)
            while True:
                try:
                    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break

                # Check for continuation token (zero-row batch with STATE_KEY)
                if batch.num_rows == 0 and custom_metadata is not None:
                    token = custom_metadata.get(STATE_KEY)
                    if token is not None:
                        if not isinstance(token, bytes):
                            raise TypeError(f"Expected bytes for state token, got {type(token).__name__}")
                        _drain_stream(reader)
                        reader = self._send_continuation(token)
                        continue

                # Dispatch log/error batches
                if _dispatch_log_or_error(batch, custom_metadata, self._on_log):
                    continue

                resolved_batch, resolved_cm = resolve_external_location(
                    batch, custom_metadata, self._external_config, self._on_log, reader.ipc_validation
                )
                yield AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm)
        except RpcError:
            if reader is not None:
                _drain_stream(reader)
            raise

    def close(self) -> None:
        """Close the session (no-op for HTTP — stateless)."""

    def __enter__(self) -> HttpStreamSession:
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


@contextlib.contextmanager
def http_connect[P](
    protocol: type[P],
    base_url: str | None = None,
    *,
    prefix: str = "/vgi",
    on_log: Callable[[Message], None] | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.NONE,
) -> Iterator[P]:
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
        ipc_validation: Validation level for incoming IPC batches.

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
        yield cast(
            P,
            _HttpProxy(
                protocol, client, url_prefix, on_log, external_config=external_location, ipc_validation=ipc_validation
            ),
        )
    finally:
        if own_client:
            client.close()


def http_introspect(
    base_url: str | None = None,
    *,
    prefix: str = "/vgi",
    client: httpx.Client | _SyncTestClient | None = None,
    ipc_validation: IpcValidation = IpcValidation.NONE,
) -> ServiceDescription:
    """Send a ``__describe__`` request over HTTP and return a ``ServiceDescription``.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).
        ipc_validation: Validation level for incoming IPC batches.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        RpcError: If the server does not support introspection or returns
            an error.
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    from vgi_rpc.introspect import DESCRIBE_METHOD_NAME, parse_describe_batch

    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)

    try:
        # Build a minimal request: empty params with __describe__ method name
        req_buf = BytesIO()
        request_metadata = pa.KeyValueMetadata(
            {
                b"vgi_rpc.method": DESCRIBE_METHOD_NAME.encode(),
                b"vgi_rpc.request_version": b"1",
            }
        )
        with ipc.new_stream(req_buf, _EMPTY_SCHEMA) as writer:
            writer.write_batch(
                pa.RecordBatch.from_pydict({}, schema=_EMPTY_SCHEMA),
                custom_metadata=request_metadata,
            )

        resp = client.post(
            f"{prefix}/{DESCRIBE_METHOD_NAME}",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )

        reader = _open_response_stream(resp.content, resp.status_code, ipc_validation)
        # Skip log batches
        while True:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            if not _dispatch_log_or_error(batch, custom_metadata):
                break
        _drain_stream(reader)

        # Reconstruct batch with schema metadata from the IPC stream
        schema_with_md = reader.schema.with_metadata(reader.schema.metadata or {})
        batch_with_md = pa.RecordBatch.from_arrays(
            [batch.column(i) for i in range(batch.num_columns)],
            schema=schema_with_md,
        )
        return parse_describe_batch(batch_with_md)
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
        ipc_validation: IpcValidation = IpcValidation.NONE,
    ) -> None:
        self._protocol = protocol
        self._client = client
        self._url_prefix = url_prefix
        self._methods = rpc_methods(protocol)
        self._on_log = on_log
        self._external_config = external_config
        self._ipc_validation = ipc_validation

    def __getattr__(self, name: str) -> Any:
        """Resolve RPC method names to callable proxies, caching on first access.

        Returns ``Any`` because each method name maps to a different callable
        signature (unary or stream), so no single static return type can
        represent all of them.
        """
        info = self._methods.get(name)
        if info is None:
            raise AttributeError(f"{self._protocol.__name__} has no RPC method '{name}'")

        if info.method_type == MethodType.UNARY:
            caller = self._make_unary_caller(info)
        elif info.method_type == MethodType.STREAM:
            caller = self._make_stream_caller(info)
        else:
            raise AttributeError(f"Unknown method type for '{name}'")

        self.__dict__[name] = caller
        return caller

    def _make_unary_caller(self, info: RpcMethodInfo) -> Callable[..., object]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation

        def caller(**kwargs: object) -> object:
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            reader = _open_response_stream(resp.content, resp.status_code, ipc_validation)
            return _read_unary_response(reader, info, on_log, ext_cfg)

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., HttpStreamSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation

        def caller(**kwargs: object) -> HttpStreamSession:
            # Send init request
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = client.post(
                f"{url_prefix}/{info.name}/init",
                content=req_buf.getvalue(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )

            # Read response
            reader = _open_response_stream(resp.content, resp.status_code, ipc_validation)
            output_schema = reader.schema
            state_bytes: bytes | None = None
            pending_batches: list[AnnotatedBatch] = []
            finished = False

            try:
                while True:
                    try:
                        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                    except StopIteration:
                        finished = True
                        break

                    # Check for state token (zero-row batch with STATE_KEY)
                    if batch.num_rows == 0 and custom_metadata is not None:
                        token = custom_metadata.get(STATE_KEY)
                        if token is not None:
                            if isinstance(token, bytes):
                                state_bytes = token
                            break

                    # Dispatch log/error batches
                    if _dispatch_log_or_error(batch, custom_metadata, on_log):
                        continue

                    # Data batch from producer init
                    resolved_batch, resolved_cm = resolve_external_location(
                        batch, custom_metadata, ext_cfg, on_log, reader.ipc_validation
                    )
                    pending_batches.append(AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm))
            except RpcError:
                _drain_stream(reader)
                raise

            _drain_stream(reader)

            return HttpStreamSession(
                client=client,
                url_prefix=url_prefix,
                method=info.name,
                state_bytes=state_bytes,
                output_schema=output_schema,
                on_log=on_log,
                external_config=ext_cfg,
                ipc_validation=ipc_validation,
                pending_batches=pending_batches,
                finished=finished,
            )

        return caller


# ---------------------------------------------------------------------------
# Server capabilities discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HttpServerCapabilities:
    """Capabilities advertised by an HTTP RPC server.

    Attributes:
        max_request_bytes: Maximum request body size the server accepts,
            or ``None`` if the server does not advertise a limit.
        upload_url_support: Whether the server supports the
            ``__upload_url__`` endpoint for client-side uploads.
        max_upload_bytes: Maximum upload size the server accepts for
            client-vended URLs, or ``None`` if not advertised.

    """

    max_request_bytes: int | None = None
    upload_url_support: bool = False
    max_upload_bytes: int | None = None


def http_capabilities(
    base_url: str | None = None,
    *,
    prefix: str = "/vgi",
    client: httpx.Client | _SyncTestClient | None = None,
) -> HttpServerCapabilities:
    """Discover server capabilities via an OPTIONS request.

    Sends ``OPTIONS {prefix}/__capabilities__`` and reads capability
    headers (``VGI-Max-Request-Bytes``, ``VGI-Upload-URL-Support``,
    ``VGI-Max-Upload-Bytes``) from the response.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).

    Returns:
        An ``HttpServerCapabilities`` with discovered values.

    Raises:
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)

    try:
        url = f"{prefix}/__capabilities__"
        if isinstance(client, _SyncTestClient):
            resp = client.options(url)
            headers = resp.headers
        else:
            httpx_resp = client.options(url)
            headers = dict(httpx_resp.headers)

        max_req: int | None = None
        raw = headers.get(MAX_REQUEST_BYTES_HEADER) or headers.get(MAX_REQUEST_BYTES_HEADER.lower())
        if raw is not None:
            with contextlib.suppress(ValueError):
                max_req = int(raw)

        upload_raw = headers.get(UPLOAD_URL_HEADER) or headers.get(UPLOAD_URL_HEADER.lower())
        upload_support = upload_raw == "true" if upload_raw is not None else False

        max_upload: int | None = None
        upload_bytes_raw = headers.get(MAX_UPLOAD_BYTES_HEADER) or headers.get(MAX_UPLOAD_BYTES_HEADER.lower())
        if upload_bytes_raw is not None:
            with contextlib.suppress(ValueError):
                max_upload = int(upload_bytes_raw)

        return HttpServerCapabilities(
            max_request_bytes=max_req,
            upload_url_support=upload_support,
            max_upload_bytes=max_upload,
        )
    finally:
        if own_client:
            client.close()


def request_upload_urls(
    base_url: str | None = None,
    *,
    count: int = 1,
    prefix: str = "/vgi",
    client: httpx.Client | _SyncTestClient | None = None,
) -> list[UploadUrl]:
    """Request pre-signed upload URLs from the server's ``__upload_url__`` endpoint.

    The server must have been configured with an ``upload_url_provider``
    in ``make_wsgi_app()``.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        count: Number of upload URLs to request (default 1, max 100).
        prefix: URL prefix matching the server's prefix (default ``/vgi``).
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).

    Returns:
        A list of ``UploadUrl`` objects with pre-signed PUT and GET URLs.

    Raises:
        RpcError: If the server does not support upload URLs (404) or
            returns an error.
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)

    try:
        # Build request IPC with standard wire protocol metadata
        req_buf = BytesIO()
        _write_request(req_buf, _UPLOAD_URL_METHOD, _UPLOAD_URL_PARAMS_SCHEMA, {"count": count})

        resp = client.post(
            f"{prefix}/__upload_url__/init",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )

        # Without an upload_url_provider the route doesn't exist and the
        # request falls through to _StreamInitResource → 404.
        if resp.status_code == HTTPStatus.NOT_FOUND:
            raise RpcError("NotSupported", "Server does not support upload URLs", "")

        reader = _open_response_stream(resp.content, resp.status_code)
        urls: list[UploadUrl] = []
        try:
            while True:
                try:
                    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break

                if _dispatch_log_or_error(batch, custom_metadata):
                    continue

                for i in range(batch.num_rows):
                    upload_url = batch.column("upload_url")[i].as_py()
                    download_url = batch.column("download_url")[i].as_py()
                    expires_at = batch.column("expires_at")[i].as_py()
                    urls.append(UploadUrl(upload_url=upload_url, download_url=download_url, expires_at=expires_at))
        except RpcError:
            _drain_stream(reader)
            raise
        _drain_stream(reader)
        return urls
    finally:
        if own_client:
            client.close()
