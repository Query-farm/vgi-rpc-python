# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP server implementation using Falcon/WSGI.

Provides ``make_wsgi_app`` to expose an ``RpcServer`` as a Falcon WSGI
application.  All server-side Falcon resources, middleware, state token
helpers, and the app factory live here.
"""

from __future__ import annotations

import hashlib
import hmac
import html as _html
import importlib.metadata
import logging
import os
import struct
import time
import types as _types
import warnings
from collections.abc import Callable, Iterable, Mapping
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import Any, Literal, get_args, get_origin, get_type_hints

import falcon
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import (
    UploadUrlProvider,
    resolve_external_location,
)
from vgi_rpc.introspect import MethodDescription, parse_describe_batch
from vgi_rpc.metadata import STATE_KEY, strip_keys
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
    _current_request_id,
    _current_transport,
    _deserialize_params,
    _drain_stream,
    _emit_access_log,
    _flush_collector,
    _generate_request_id,
    _get_auth_and_metadata,
    _log_method_error,
    _read_request,
    _TransportContext,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_result_batch,
    _write_stream_header,
)
from vgi_rpc.rpc._common import (
    CallStatistics,
    HookToken,
    _current_call_stats,
    _DispatchHook,
    _record_input,
    _record_output,
)
from vgi_rpc.utils import ValidatedReader, empty_batch

from ._common import (
    _ARROW_CONTENT_TYPE,
    _MAX_UPLOAD_URL_COUNT,
    _UPLOAD_URL_METHOD,
    _UPLOAD_URL_SCHEMA,
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    UPLOAD_URL_HEADER,
    _compress_body,
    _decompress_body,
    _RpcHttpError,
)

_logger = logging.getLogger("vgi_rpc.http")


def _vgi_version() -> str:
    """Return the installed vgi-rpc package version."""
    try:
        return importlib.metadata.version("vgi-rpc")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


def _check_content_type(req: falcon.Request) -> None:
    """Raise ``_RpcHttpError`` if Content-Type is not Arrow IPC stream."""
    content_type = req.content_type or ""
    if content_type != _ARROW_CONTENT_TYPE:
        raise _RpcHttpError(
            TypeError(
                f"Expected Content-Type: '{_ARROW_CONTENT_TYPE}', got {content_type!r}. "
                f"All vgi-rpc HTTP requests must use Content-Type: {_ARROW_CONTENT_TYPE}"
            ),
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
_TOKEN_VERSION = 2  # bump when the token wire format changes
_TOKEN_VERSION_LEN = 1  # single byte
_TIMESTAMP_LEN = 8  # uint64 LE, seconds since epoch
_MIN_TOKEN_LEN = _TOKEN_VERSION_LEN + _TIMESTAMP_LEN + _HEADER_LEN * 3 + _HMAC_LEN


def _pack_state_token(
    state_bytes: bytes,
    schema_bytes: bytes,
    input_schema_bytes: bytes,
    signing_key: bytes,
    created_at: int,
) -> bytes:
    """Pack state, output schema, and input schema bytes into a signed token.

    Wire format (v2)::

        [1 byte:  version=2          (uint8)]
        [8 bytes: created_at         (uint64 LE, seconds since epoch)]
        [4 bytes: state_len          (uint32 LE)]
        [state_len bytes: state_bytes]
        [4 bytes: schema_len         (uint32 LE)]
        [schema_len bytes: schema_bytes]
        [4 bytes: input_schema_len   (uint32 LE)]
        [input_schema_len bytes: input_schema_bytes]
        [32 bytes: HMAC-SHA256(key, all above)]

    Args:
        state_bytes: Serialized state (Arrow IPC).
        schema_bytes: Serialized output ``pa.Schema``.
        input_schema_bytes: Serialized input ``pa.Schema``.
        signing_key: HMAC signing key.
        created_at: Token creation time as seconds since epoch.

    Returns:
        The opaque signed token.

    """
    payload = (
        struct.pack("B", _TOKEN_VERSION)
        + struct.pack("<Q", created_at)
        + struct.pack("<I", len(state_bytes))
        + state_bytes
        + struct.pack("<I", len(schema_bytes))
        + schema_bytes
        + struct.pack("<I", len(input_schema_bytes))
        + input_schema_bytes
    )
    mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return payload + mac


def _unpack_state_token(token: bytes, signing_key: bytes, token_ttl: int = 0) -> tuple[bytes, bytes, bytes]:
    """Unpack and verify a signed state token.

    Args:
        token: The opaque token produced by ``_pack_state_token``.
        signing_key: HMAC signing key (must match the one used to pack).
        token_ttl: Maximum token age in seconds.  ``0`` disables expiry
            checking.

    Returns:
        ``(state_bytes, schema_bytes, input_schema_bytes)``

    Raises:
        _RpcHttpError: On malformed, tampered, or expired tokens (HTTP 400).

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

    segments_start = _TOKEN_VERSION_LEN + _TIMESTAMP_LEN
    state_bytes, pos = _read_segment(token, segments_start)
    schema_bytes, pos = _read_segment(token, pos)
    input_schema_bytes, payload_end = _read_segment(token, pos)

    if payload_end + _HMAC_LEN != len(token):
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # Verify HMAC before inspecting any payload fields (including version)
    # to avoid leaking information about the token format to unauthenticated callers.
    payload = token[:payload_end]
    received_mac = token[payload_end:]
    expected_mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(received_mac, expected_mac):
        raise _RpcHttpError(
            RuntimeError("State token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    version = token[0]
    if version != _TOKEN_VERSION:
        raise _RpcHttpError(
            RuntimeError(f"Unsupported state token version {version} (expected {_TOKEN_VERSION})"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # Enforce token TTL when configured
    if token_ttl > 0:
        created_at = struct.unpack_from("<Q", token, _TOKEN_VERSION_LEN)[0]
        if int(time.time()) - created_at > token_ttl:
            raise _RpcHttpError(
                RuntimeError("State token expired"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

    return state_bytes, schema_bytes, input_schema_bytes


# Type alias: a single concrete class or an ordered tuple for unions.
_StateInfo = type[StreamState] | tuple[type[StreamState], ...]

# Arrow IPC streams always start with 0xFF (continuation indicator).
# We use 0x00 as a discriminator byte for union-tagged state envelopes.
_UNION_STATE_MARKER = b"\x00"


def _serialize_state_bytes(state: StreamState, state_info: _StateInfo) -> bytes:
    r"""Serialize state bytes for state token payload.

    Single-state methods store raw serialized state bytes.
    Union-state methods store: ``\x00`` + uint16-LE tag + raw bytes.
    """
    state_bytes = state.serialize_to_bytes()
    if isinstance(state_info, tuple):
        try:
            tag = state_info.index(type(state))
        except ValueError as exc:
            msg = (
                f"State type {type(state).__name__!r} is not valid for union method; "
                f"expected one of {[t.__name__ for t in state_info]}"
            )
            raise RuntimeError(msg) from exc
        return _UNION_STATE_MARKER + struct.pack("<H", tag) + state_bytes
    return state_bytes


def _resolve_state_cls(
    data: bytes,
    state_info: _StateInfo,
) -> tuple[type[StreamState], bytes]:
    """Resolve the concrete state class from token state bytes.

    Returns:
        ``(state_cls, raw_state_bytes)``

    """
    if isinstance(state_info, tuple):
        if data[:1] != _UNION_STATE_MARKER or len(data) < 3:
            msg = "Cannot deserialize union state from untagged token"
            raise RuntimeError(msg)
        tag = struct.unpack("<H", data[1:3])[0]
        if tag >= len(state_info):
            msg = f"Unknown union state tag {tag}; expected 0..{len(state_info) - 1}"
            raise RuntimeError(msg)
        return state_info[tag], data[3:]
    return state_info, data


def _resolve_state_types(
    server: RpcServer,
) -> dict[str, _StateInfo]:
    """Introspect server implementation to map method names to concrete state types.

    Examines the return type hints of each stream method on the
    implementation (not the protocol) to extract the concrete
    ``StreamState`` subclass.

    For union return types (``Stream[A | B, ...]``), stores an
    ordered tuple of classes so token state can carry a compact
    numeric tag instead of class names.

    Args:
        server: The ``RpcServer`` whose implementation to introspect.

    Returns:
        Mapping of method name to state info (single class or union dict).

    """
    result: dict[str, _StateInfo] = {}
    for name, info in server.methods.items():
        if info.method_type != MethodType.STREAM:
            continue
        impl_method = getattr(server.implementation, name, None)
        if impl_method is None:
            continue
        try:
            hints = get_type_hints(impl_method)
        except (NameError, AttributeError) as exc:
            msg = f"Cannot resolve type hints for stream method {name!r}: {exc}"
            raise TypeError(msg) from exc
        return_hint = hints.get("return")
        if return_hint is None:
            continue
        origin = get_origin(return_hint)
        if origin is Stream:
            args = get_args(return_hint)
            if not args:
                continue
            state_arg = args[0]
            if isinstance(state_arg, type) and issubclass(state_arg, StreamState):
                result[name] = state_arg
            elif isinstance(state_arg, _types.UnionType):
                members = tuple(t for t in get_args(state_arg) if isinstance(t, type) and issubclass(t, StreamState))
                if len(members) == 1:
                    result[name] = members[0]
                elif members:
                    result[name] = members
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
        "_token_ttl",
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
        token_ttl: int = 3600,
    ) -> None:
        self._server = server
        self._signing_key = signing_key
        self._state_types = _resolve_state_types(server)
        self._max_stream_response_bytes = max_stream_response_bytes
        self._max_request_bytes = max_request_bytes
        self._upload_url_provider = upload_url_provider
        self._max_upload_bytes = max_upload_bytes
        self._token_ttl = token_ttl

    def _resolve_method(self, req: falcon.Request, method: str) -> RpcMethodInfo:
        """Validate content type and resolve method info.

        Raises:
            _RpcHttpError: If content type is wrong or method is unknown.

        """
        _check_content_type(req)
        info = self._server.methods.get(method)
        if info is None:
            available = sorted(self._server.methods.keys())
            raise _RpcHttpError(
                AttributeError(f"Unknown method: '{method}'. Available methods: {available}"),
                status_code=HTTPStatus.NOT_FOUND,
            )
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
        stats = CallStatistics()
        stats_token = _current_call_stats.set(stats)
        try:
            try:
                ipc_method, kwargs = _read_request(stream, self._server.ipc_validation)
                if ipc_method != method_name:
                    raise TypeError(
                        f"Method name mismatch: URL path has '{method_name}' but Arrow IPC "
                        f"custom_metadata 'vgi_rpc.method' has '{ipc_method}'. These must match."
                    )
                _deserialize_params(kwargs, info.param_types, self._server.ipc_validation)
                _validate_params(info.name, kwargs, info.param_types)
            except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

            # Pre-built __describe__ batch — write directly, skip implementation call.
            describe_batch = self._server._describe_batch
            if describe_batch is not None and method_name == "__describe__":
                _record_output(describe_batch)
                resp_buf = BytesIO()
                with ipc.new_stream(resp_buf, describe_batch.schema) as writer:
                    writer.write_batch(describe_batch, custom_metadata=self._server._describe_metadata)
                resp_buf.seek(0)
                auth, transport_metadata = _get_auth_and_metadata()
                _emit_access_log(
                    self._server.protocol_name,
                    method_name,
                    info.method_type.value,
                    self._server.server_id,
                    auth,
                    transport_metadata,
                    0.0,
                    "ok",
                    http_status=HTTPStatus.OK.value,
                    stats=stats,
                )
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
            hook: _DispatchHook | None = self._server._dispatch_hook
            hook_token: HookToken = None
            if hook is not None:
                try:
                    hook_token = hook.on_dispatch_start(info, auth, transport_metadata)
                except Exception:
                    _logger.debug("Dispatch hook start failed", exc_info=True)
                    hook = None
            _hook_exc: BaseException | None = None
            try:
                with ipc.new_stream(resp_buf, schema) as writer:
                    sink.flush_contents(writer, schema)
                    try:
                        result = getattr(self._server.implementation, method_name)(**kwargs)
                        _validate_result(info.name, result, info.result_type)
                        _write_result_batch(writer, schema, result, self._server.external_config)
                    except (TypeError, pa.ArrowInvalid) as exc:
                        _hook_exc = exc
                        status = "error"
                        error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                        _write_error_batch(writer, schema, exc, server_id=server_id)
                        http_status = HTTPStatus.BAD_REQUEST
                    except Exception as exc:
                        _hook_exc = exc
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
                    stats=stats,
                )
                if hook is not None:
                    try:
                        hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                    except Exception:
                        _logger.debug("Dispatch hook end failed", exc_info=True)

            resp_buf.seek(0)
            return resp_buf, http_status
        finally:
            _current_call_stats.reset(stats_token)

    def _stream_init_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> BytesIO:
        """Run stream init synchronously.

        For producer streams (input_schema == _EMPTY_SCHEMA), produces data
        immediately via _produce_stream_response.
        For exchange streams, returns a state token for subsequent exchanges.
        """
        stats = CallStatistics()
        stats_token = _current_call_stats.set(stats)
        try:
            state_info = self._state_types.get(method_name)
            if state_info is None:
                raise _RpcHttpError(
                    RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            try:
                ipc_method, kwargs = _read_request(stream, self._server.ipc_validation)
                if ipc_method != method_name:
                    raise TypeError(
                        f"Method name mismatch: URL path has '{method_name}' but Arrow IPC "
                        f"custom_metadata 'vgi_rpc.method' has '{ipc_method}'. These must match."
                    )
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
            hook: _DispatchHook | None = self._server._dispatch_hook
            hook_token_init: HookToken = None
            if hook is not None:
                try:
                    hook_token_init = hook.on_dispatch_start(info, auth, transport_metadata)
                except Exception:
                    _logger.debug("Dispatch hook start failed", exc_info=True)
                    hook = None
            _hook_exc: BaseException | None = None
            try:
                try:
                    result: Stream[StreamState, Any] = getattr(self._server.implementation, method_name)(**kwargs)
                except (TypeError, pa.ArrowInvalid) as exc:
                    _hook_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    http_status = HTTPStatus.BAD_REQUEST
                    raise _RpcHttpError(exc, status_code=http_status) from exc
                except Exception as exc:
                    _hook_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                    raise _RpcHttpError(exc, status_code=http_status) from exc

                is_producer = result.input_schema == _EMPTY_SCHEMA

                if is_producer:
                    # Producer stream — write header (if declared) then run produce loop
                    resp_buf = BytesIO()
                    if info.header_type is not None:
                        _write_stream_header(
                            resp_buf, result.header, self._server.external_config, sink=sink, method_name=method_name
                        )
                    produce_buf, produce_error_type = self._produce_stream_response(
                        result.output_schema,
                        result.state,
                        result.input_schema,
                        sink,
                        method_name=method_name,
                        auth=auth,
                        transport_metadata=transport_metadata,
                    )
                    resp_buf.write(produce_buf.getvalue())
                    resp_buf.seek(0)
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
                        state_bytes = _serialize_state_bytes(state, state_info)
                        schema_bytes = output_schema.serialize().to_pybytes()
                        input_schema_bytes = input_schema.serialize().to_pybytes()
                        token = _pack_state_token(
                            state_bytes, schema_bytes, input_schema_bytes, self._signing_key, int(time.time())
                        )

                        # Write response: header (if declared) + log batches + zero-row batch with token in metadata
                        resp_buf = BytesIO()
                        if info.header_type is not None:
                            _write_stream_header(
                                resp_buf,
                                result.header,
                                self._server.external_config,
                                sink=sink,
                                method_name=method_name,
                            )
                        with ipc.new_stream(resp_buf, output_schema) as writer:
                            sink.flush_contents(writer, output_schema)
                            state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                            zero_batch = empty_batch(output_schema)
                            _record_output(zero_batch)
                            writer.write_batch(zero_batch, custom_metadata=state_metadata)
                    except Exception as exc:
                        _hook_exc = exc
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
                    stats=stats,
                )
                if hook is not None:
                    try:
                        hook.on_dispatch_end(hook_token_init, info, _hook_exc, stats=stats)
                    except Exception:
                        _logger.debug("Dispatch hook end failed", exc_info=True)
        finally:
            _current_call_stats.reset(stats_token)

    def _stream_exchange_sync(self, method_name: str, stream: IOBase) -> BytesIO:
        """Run stream exchange synchronously.

        Dispatches to producer continuation or exchange based on input_schema
        recovered from the state token.

        Note: headers are only sent in the init response — continuations
        and exchanges never re-send headers (state is recovered from the
        signed token, which does not include header data).
        """
        stats = CallStatistics()
        stats_token = _current_call_stats.set(stats)
        try:
            state_info = self._state_types.get(method_name)
            if state_info is None:
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

            # Record input batch for stats
            _record_input(input_batch)

            # Extract state token before resolution — resolve_external_location
            # replaces metadata with what was stored in the external IPC stream.
            token = custom_metadata.get(STATE_KEY) if custom_metadata is not None else None

            if token is None:
                raise _RpcHttpError(
                    RuntimeError("Missing state token in exchange request"),
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            # Unpack and verify the signed token, recover state + schema + input_schema
            state_obj, output_schema, input_schema = self._unpack_and_recover_state(token, state_info)

            is_producer = input_schema == _EMPTY_SCHEMA

            # Resolve method info for hook (may be None for unknown methods, but
            # _stream_exchange_sync is only reached for known stream methods)
            info = self._server.methods.get(method_name)

            if is_producer:
                # Producer continuation
                server_id = self._server.server_id
                protocol_name = self._server.protocol_name
                auth, transport_metadata = _get_auth_and_metadata()
                start = time.monotonic()
                http_status = HTTPStatus.OK
                status: Literal["ok", "error"] = "ok"
                error_type = ""
                hook: _DispatchHook | None = self._server._dispatch_hook if info is not None else None
                hook_token_exch: HookToken = None
                if hook is not None and info is not None:
                    try:
                        hook_token_exch = hook.on_dispatch_start(info, auth, transport_metadata)
                    except Exception:
                        _logger.debug("Dispatch hook start failed", exc_info=True)
                        hook = None
                _hook_exc: BaseException | None = None
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
                        stats=stats,
                    )
                    if hook is not None and info is not None:
                        try:
                            hook.on_dispatch_end(hook_token_exch, info, _hook_exc, stats=stats)
                        except Exception:
                            _logger.debug("Dispatch hook end failed", exc_info=True)
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
                hook_e: _DispatchHook | None = self._server._dispatch_hook if info is not None else None
                hook_token_e: HookToken = None
                if hook_e is not None and info is not None:
                    try:
                        hook_token_e = hook_e.on_dispatch_start(info, auth, transport_md)
                    except Exception:
                        _logger.debug("Dispatch hook start failed", exc_info=True)
                        hook_e = None
                _hook_exc_e: BaseException | None = None
                try:
                    # Strip state token from metadata visible to process()
                    user_cm = strip_keys(resolved_cm, STATE_KEY)

                    ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
                    out = OutputCollector(output_schema, server_id=server_id, producer_mode=False)

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
                    updated_state_bytes = _serialize_state_bytes(state_obj, state_info)
                    schema_bytes = output_schema.serialize().to_pybytes()
                    input_schema_bytes_upd = input_schema.serialize().to_pybytes()
                    updated_token = _pack_state_token(
                        updated_state_bytes, schema_bytes, input_schema_bytes_upd, self._signing_key, int(time.time())
                    )
                    out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

                    # Write response batches (log + data, in order)
                    resp_buf = BytesIO()
                    with ipc.new_stream(resp_buf, output_schema) as writer:
                        _flush_collector(writer, out, self._server.external_config)
                except Exception as exc:
                    _hook_exc_e = exc
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
                        stats=stats,
                    )
                    if hook_e is not None and info is not None:
                        try:
                            hook_e.on_dispatch_end(hook_token_e, info, _hook_exc_e, stats=stats)
                        except Exception:
                            _logger.debug("Dispatch hook end failed", exc_info=True)

                resp_buf.seek(0)
                return resp_buf
        finally:
            _current_call_stats.reset(stats_token)

    def _produce_stream_response(
        self,
        schema: pa.Schema,
        state: StreamState,
        input_schema: pa.Schema,
        sink: _ClientLogSink | None = None,
        *,
        method_name: str,
        auth: AuthContext | None = None,
        transport_metadata: Mapping[str, str] | None = None,
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
                    out = OutputCollector(
                        schema, prior_data_bytes=cumulative_bytes, server_id=server_id, producer_mode=True
                    )
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
                        state_info = self._state_types.get(method_name)
                        if state_info is None:
                            raise _RpcHttpError(
                                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                            )
                        state_bytes = _serialize_state_bytes(state, state_info)
                        schema_bytes = schema.serialize().to_pybytes()
                        input_schema_bytes = input_schema.serialize().to_pybytes()
                        token = _pack_state_token(
                            state_bytes, schema_bytes, input_schema_bytes, self._signing_key, int(time.time())
                        )
                        state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                        continuation_batch = empty_batch(schema)
                        _record_output(continuation_batch)
                        writer.write_batch(continuation_batch, custom_metadata=state_metadata)
                        break
            except Exception as exc:
                produce_error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                _write_error_batch(writer, schema, exc, server_id=server_id)
        resp_buf.seek(0)
        return resp_buf, produce_error_type

    def _unpack_and_recover_state(
        self,
        token: bytes,
        state_info: _StateInfo,
    ) -> tuple[StreamState, pa.Schema, pa.Schema]:
        """Unpack a signed state token and recover state, output schema, and input schema.

        Args:
            token: The signed state token bytes.
            state_info: A single concrete state class, or an ordered tuple of
                concrete classes for union types.  When a tuple is provided, the
                concrete class is resolved from the numeric tag embedded in
                ``state_bytes``.

        Returns:
            Tuple of (state_object, output_schema, input_schema).

        Raises:
            _RpcHttpError: On malformed tokens, expired tokens, failed
                deserialization, or signature verification failure.

        """
        state_bytes, schema_bytes, input_schema_bytes = _unpack_state_token(token, self._signing_key, self._token_ttl)

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
            state_cls, raw_state_bytes = _resolve_state_cls(state_bytes, state_info)
            state_obj = state_cls.deserialize_from_bytes(raw_state_bytes, self._server.ipc_validation)
            state_obj.rehydrate(self._server.implementation)
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize state: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

        return state_obj, output_schema, input_schema


# ---------------------------------------------------------------------------
# 404 sink for unmatched routes
# ---------------------------------------------------------------------------

_NOT_FOUND_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>404 &mdash; vgi-rpc endpoint</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 60px auto; padding: 0 20px; color: #333; text-align: center; }}
  .logo {{ margin-bottom: 24px; }}
  .logo img {{ width: 120px; height: 120px; }}
  h1 {{ color: #555; }}
  code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-size: 0.95em; }}
  a {{ color: #0066cc; }}
  p {{ line-height: 1.6; }}
</style>
</head>
<body>
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>
<h1>404 &mdash; Not Found</h1>
<p>This is a <code>vgi-rpc</code> service endpoint{protocol_fragment}.</p>
<p>RPC methods are available under <code>{prefix}/&lt;method&gt;</code>.</p>
<p>Learn more at <a href="https://vgi-rpc.query.farm">vgi-rpc.query.farm</a>.</p>
</body>
</html>"""


def _make_not_found_sink(
    prefix: str,
    protocol_name: str,
) -> Callable[..., None]:
    """Create a Falcon sink that returns a 404 HTML page for unmatched routes.

    The HTML is pre-rendered at creation time so there is zero per-request
    template overhead.

    Args:
        prefix: The URL prefix for RPC endpoints (e.g. ``/vgi``).
        protocol_name: The protocol name from the ``RpcServer``.

    Returns:
        A Falcon sink callable.

    """
    protocol_fragment = f" serving <strong>{_html.escape(protocol_name)}</strong>" if protocol_name else ""
    body_bytes = _NOT_FOUND_HTML_TEMPLATE.format(
        prefix=_html.escape(prefix),
        protocol_fragment=protocol_fragment,
    ).encode("utf-8")

    def _not_found_sink(req: falcon.Request, resp: falcon.Response, **kwargs: Any) -> None:
        """Return an HTML 404 page for requests that do not match any RPC route."""
        resp.status = "404"
        resp.content_type = "text/html; charset=utf-8"
        resp.data = body_bytes

    return _not_found_sink


# ---------------------------------------------------------------------------
# Landing page at GET {prefix}
# ---------------------------------------------------------------------------

_FONT_IMPORTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">'  # noqa: E501
)

_LANDING_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{protocol_name} &mdash; vgi-rpc</title>
"""
    + _FONT_IMPORTS
    + """
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 0 auto; padding: 60px 20px 0; color: #2c2c1e; text-align: center;
         background: #faf8f0; }}
  .logo {{ margin-bottom: 24px; }}
  .logo img {{ width: 140px; height: 140px; border-radius: 50%;
               box-shadow: 0 4px 24px rgba(0,0,0,0.12); }}
  h1 {{ color: #2d5016; margin-bottom: 8px; font-weight: 700; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.9em; color: #2c2c1e; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  p {{ line-height: 1.7; color: #6b6b5a; }}
  .meta {{ font-size: 0.9em; color: #6b6b5a; }}
  .links {{ margin-top: 28px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }}
  .links a {{ display: inline-block; padding: 8px 18px; border-radius: 6px;
              border: 1px solid #4a7c23; color: #2d5016; font-weight: 600;
              font-size: 0.9em; transition: all 0.2s ease; }}
  .links a:hover {{ background: #4a7c23; color: #fff; }}
  .links a.primary {{ background: #2d5016; color: #fff; border-color: #2d5016; }}
  .links a.primary:hover {{ background: #4a7c23; border-color: #4a7c23; }}
  footer {{ margin-top: 48px; padding: 20px 0; border-top: 1px solid #f0ece0;
            color: #6b6b5a; font-size: 0.85em; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>
</head>
<body>
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>
<h1>{protocol_name}</h1>
<p class="meta">Powered by <code>vgi-rpc</code> v{version} &middot; server <code>{server_id}</code></p>
<p>This is a <code>vgi-rpc</code> service endpoint.</p>
<div class="links">
{describe_link}
{repo_link}
<a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
</div>
<footer>
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>"""
)


def _build_landing_html(
    prefix: str,
    protocol_name: str,
    server_id: str,
    describe_path: str | None,
    repo_url: str | None,
) -> bytes:
    """Pre-render the landing page HTML.

    Args:
        prefix: URL prefix for RPC endpoints.
        protocol_name: Protocol name from the ``RpcServer``.
        server_id: Server identity string.
        describe_path: Path to the describe page, or ``None`` to omit the link.
        repo_url: URL to the service's source repository, or ``None``.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    describe_link = (
        f'<a class="primary" href="{_html.escape(describe_path)}">View service API</a>'
        if describe_path is not None
        else ""
    )
    repo_link = f'<a href="{_html.escape(repo_url)}">Source repository</a>' if repo_url else ""
    return _LANDING_HTML_TEMPLATE.format(
        protocol_name=_html.escape(protocol_name),
        server_id=_html.escape(server_id),
        version=_html.escape(_vgi_version()),
        describe_link=describe_link,
        repo_link=repo_link,
    ).encode("utf-8")


class _LandingPageResource:
    """Falcon resource for the landing page at ``GET {prefix}``."""

    __slots__ = ("_body",)

    def __init__(self, body: bytes) -> None:
        self._body = body

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the landing page HTML."""
        resp.content_type = "text/html; charset=utf-8"
        resp.data = self._body


# ---------------------------------------------------------------------------
# Describe page at GET {prefix}/describe
# ---------------------------------------------------------------------------

_DESCRIBE_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{protocol_name} API Reference &mdash; vgi-rpc</title>
"""
    + _FONT_IMPORTS
    + """
<style>
  body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; max-width: 900px;
         margin: 0 auto; padding: 40px 20px 0; color: #2c2c1e; background: #faf8f0; }}
  .header {{ text-align: center; margin-bottom: 40px; }}
  .header .logo img {{ width: 80px; height: 80px; border-radius: 50%;
                       box-shadow: 0 3px 16px rgba(0,0,0,0.10); }}
  .header h1 {{ margin-bottom: 4px; color: #2d5016; font-weight: 700; }}
  .header .subtitle {{ color: #6b6b5a; font-size: 1.1em; margin-top: 0; }}
  .header .meta {{ color: #6b6b5a; font-size: 0.9em; }}
  .header .meta a {{ color: #2d5016; font-weight: 600; }}
  .header .meta a:hover {{ color: #4a7c23; }}
  code {{ font-family: 'JetBrains Mono', monospace; background: #f0ece0;
          padding: 2px 6px; border-radius: 3px; font-size: 0.85em; color: #2c2c1e; }}
  a {{ color: #2d5016; text-decoration: none; }}
  a:hover {{ color: #4a7c23; }}
  .card {{ border: 1px solid #f0ece0; border-radius: 8px; padding: 20px;
           margin-bottom: 16px; background: #fff; }}
  .card:hover {{ border-color: #c8a43a; }}
  .card-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }}
  .method-name {{ font-family: 'JetBrains Mono', monospace; font-size: 1.1em; font-weight: 600;
                  color: #2d5016; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 0.75em; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.03em; }}
  .badge-unary {{ background: #e8f5e0; color: #2d5016; }}
  .badge-stream {{ background: #e0ecf5; color: #1a4a6b; }}
  .badge-exchange {{ background: #f5e6f0; color: #6b234a; }}
  .badge-producer {{ background: #e0f0f5; color: #1a5a6b; }}
  .badge-header {{ background: #f5eee0; color: #6b4423; }}
  .docstring {{ color: #6b6b5a; margin-bottom: 12px; line-height: 1.5; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
  th {{ text-align: left; padding: 8px 10px; background: #f0ece0; color: #2c2c1e;
        font-weight: 600; border-bottom: 2px solid #e0dcd0; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #f0ece0; }}
  td code {{ font-size: 0.85em; }}
  .no-params {{ color: #6b6b5a; font-style: italic; font-size: 0.9em; }}
  .section-label {{ font-size: 0.8em; font-weight: 600; text-transform: uppercase;
                    letter-spacing: 0.05em; color: #6b6b5a; margin-top: 14px;
                    margin-bottom: 6px; }}
  footer {{ text-align: center; margin-top: 48px; padding: 20px 0;
            border-top: 1px solid #f0ece0; color: #6b6b5a; font-size: 0.85em; }}
  footer a {{ color: #2d5016; font-weight: 600; }}
  footer a:hover {{ color: #4a7c23; }}
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
  </div>
  <h1>{protocol_name}</h1>
  <p class="subtitle">API Reference</p>
  <p class="meta">Powered by <code>vgi-rpc</code> v{version} &middot; server <code>{server_id}</code>
{repo_link}</p>
</div>
{method_cards}
<footer>
  <a href="https://vgi-rpc.query.farm">Learn more about <code>vgi-rpc</code></a>
  &middot;
  &copy; 2026 &#x1F69C; <a href="https://query.farm">Query.Farm LLC</a>
</footer>
</body>
</html>"""
)


def _build_method_card(md: MethodDescription) -> str:
    """Build the HTML card for a single RPC method.

    All user-supplied values are HTML-escaped.

    Args:
        md: Parsed method description from introspection.

    Returns:
        An HTML string for the method card.

    """
    badge_cls = "badge-unary" if md.method_type.value == "UNARY" else "badge-stream"
    parts: list[str] = [
        '<div class="card">',
        '<div class="card-header">',
        f'<span class="method-name">{_html.escape(md.name)}</span>',
        f'<span class="badge {badge_cls}">{_html.escape(md.method_type.value)}</span>',
    ]
    if md.is_exchange is True:
        parts.append('<span class="badge badge-exchange">exchange</span>')
    elif md.is_exchange is False:
        parts.append('<span class="badge badge-producer">producer</span>')
    if md.has_header:
        parts.append('<span class="badge badge-header">header</span>')
    parts.append("</div>")

    if md.doc:
        parts.append(f'<p class="docstring">{_html.escape(md.doc)}</p>')

    # Parameters table
    if md.param_types:
        parts.append('<div class="section-label">Parameters</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>")
        for pname, ptype in md.param_types.items():
            default_val = md.param_defaults.get(pname)
            default_str = _html.escape(repr(default_val)) if default_val is not None else "&mdash;"
            desc_str = _html.escape(md.param_docs[pname]) if pname in md.param_docs else "&mdash;"
            parts.append(
                f"<tr><td><code>{_html.escape(pname)}</code></td>"
                f"<td><code>{_html.escape(ptype)}</code></td>"
                f"<td>{default_str}</td>"
                f"<td>{desc_str}</td></tr>"
            )
        parts.append("</table>")
    else:
        parts.append('<p class="no-params">No parameters</p>')

    # Returns table for unary methods with return values
    if md.has_return and md.result_schema and len(md.result_schema) > 0:
        parts.append('<div class="section-label">Returns</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        parts.extend(
            f"<tr><td><code>{_html.escape(field.name)}</code></td>"
            f"<td><code>{_html.escape(str(field.type))}</code></td></tr>"
            for field in md.result_schema
        )
        parts.append("</table>")

    # Header schema for stream methods with headers
    if md.has_header and md.header_schema and len(md.header_schema) > 0:
        parts.append('<div class="section-label">Stream Header</div>')
        parts.append("<table><tr><th>Name</th><th>Type</th></tr>")
        parts.extend(
            f"<tr><td><code>{_html.escape(field.name)}</code></td>"
            f"<td><code>{_html.escape(str(field.type))}</code></td></tr>"
            for field in md.header_schema
        )
        parts.append("</table>")

    parts.append("</div>")
    return "\n".join(parts)


def _build_describe_html(server: RpcServer, prefix: str, repo_url: str | None) -> bytes:
    """Pre-render the describe page HTML from the server's introspection data.

    Args:
        server: The ``RpcServer`` instance (must have ``describe_enabled``).
        prefix: URL prefix for RPC endpoints.
        repo_url: URL to the service's source repository, or ``None``.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    assert server._describe_batch is not None
    assert server._describe_metadata is not None
    desc = parse_describe_batch(server._describe_batch, server._describe_metadata)

    cards: list[str] = []
    for name in sorted(desc.methods):
        if name == "__describe__":
            continue
        cards.append(_build_method_card(desc.methods[name]))

    repo_link = f'&middot; <a href="{_html.escape(repo_url)}">Source repository</a>' if repo_url else ""
    return _DESCRIBE_HTML_TEMPLATE.format(
        protocol_name=_html.escape(desc.protocol_name),
        server_id=_html.escape(desc.server_id),
        version=_html.escape(_vgi_version()),
        method_cards="\n".join(cards),
        repo_link=repo_link,
    ).encode("utf-8")


class _DescribePageResource:
    """Falcon resource for the describe page at ``GET {prefix}/describe``."""

    __slots__ = ("_body",)

    def __init__(self, body: bytes) -> None:
        self._body = body

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Return the describe page HTML."""
        resp.content_type = "text/html; charset=utf-8"
        resp.data = self._body


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

            result_stream, http_status = self._app._unary_sync(method, info, _get_request_stream(req))
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
            result_stream = self._app._stream_init_sync(method, info, _get_request_stream(req))
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
            result_stream = self._app._stream_exchange_sync(method, _get_request_stream(req))
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
                ipc_method, kwargs = _read_request(_get_request_stream(req), self._app._server.ipc_validation)
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


_REQUEST_ID_HEADER = "X-Request-ID"


class _RequestIdMiddleware:
    """Falcon middleware that sets a per-request correlation ID.

    Reads ``X-Request-ID`` from the incoming request header or generates a
    new 16-char hex ID.  The value is stored in ``req.context.request_id``,
    set on the ``_current_request_id`` contextvar, and echoed back on the
    response as the ``X-Request-ID`` header.
    """

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Set request ID from header or generate one; populate contextvar."""
        request_id = req.get_header(_REQUEST_ID_HEADER) or _generate_request_id()
        req.context.request_id = request_id
        req.context.request_id_token = _current_request_id.set(request_id)

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Echo request ID on response header and reset contextvar."""
        request_id = getattr(req.context, "request_id", None)
        if request_id is not None:
            resp.set_header(_REQUEST_ID_HEADER, request_id)
        token = getattr(req.context, "request_id_token", None)
        if token is not None:
            _current_request_id.reset(token)


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


def _get_request_stream(req: falcon.Request) -> IOBase:
    """Return the request body stream, using the decompressed stream if available.

    When ``_CompressionMiddleware`` is active and the request body was
    compressed, the decompressed bytes are stored in ``req.context.decompressed_stream``.
    This helper returns that stream when present, falling back to Falcon's
    ``req.bounded_stream``.

    Args:
        req: The Falcon request.

    Returns:
        A readable binary stream for the request body.

    """
    stream: IOBase | None = getattr(req.context, "decompressed_stream", None)
    if stream is not None:
        return stream
    return req.bounded_stream


class _CompressionMiddleware:
    """Falcon middleware for transparent zstd request/response compression.

    On requests: if ``Content-Encoding: zstd`` is present, the request body
    is decompressed and stored in ``req.context.decompressed_stream`` so that
    resource handlers read uncompressed data.

    On responses: if the client sent ``Accept-Encoding`` containing ``zstd``
    and the response has Arrow content, the response body is compressed and
    ``Content-Encoding: zstd`` is set.
    """

    __slots__ = ("_level",)

    def __init__(self, level: int) -> None:
        self._level = level

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Decompress zstd request bodies; record client Accept-Encoding."""
        # Check if the client accepts zstd responses
        accept_encoding = req.get_header("Accept-Encoding") or ""
        req.context.client_accepts_zstd = "zstd" in accept_encoding

        # Decompress request body if Content-Encoding: zstd
        content_encoding = req.get_header("Content-Encoding") or ""
        if "zstd" in content_encoding:
            try:
                compressed = req.bounded_stream.read()
                decompressed = _decompress_body(compressed)
                req.context.decompressed_stream = BytesIO(decompressed)
            except Exception as exc:
                raise falcon.HTTPBadRequest(
                    title="Decompression Error",
                    description=f"Failed to decompress zstd request body: {exc}",
                ) from exc

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Compress Arrow IPC response bodies with zstd when client supports it."""
        if not getattr(req.context, "client_accepts_zstd", False):
            return
        if resp.content_type != _ARROW_CONTENT_TYPE:
            return
        stream = resp.stream
        if stream is None:
            return
        if not isinstance(stream, IOBase):
            return

        body = stream.read()
        if not body:
            return

        compressed = _compress_body(body, self._level)
        resp.data = compressed
        resp.stream = None
        resp.set_header("Content-Encoding", "zstd")


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
    otel_config: object | None = None,
    sentry_config: object | None = None,
    token_ttl: int = 3600,
    compression_level: int | None = 3,
    enable_not_found_page: bool = True,
    enable_landing_page: bool = True,
    enable_describe_page: bool = True,
    repo_url: str | None = None,
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
        otel_config: Optional ``OtelConfig`` for OpenTelemetry instrumentation.
            When provided, ``instrument_server()`` is called and
            ``_OtelFalconMiddleware`` is prepended for W3C trace propagation.
            Requires ``pip install vgi-rpc[otel]``.
        sentry_config: Optional ``SentryConfig`` for Sentry error reporting.
            When provided, ``instrument_server_sentry()`` is called.
            Requires ``pip install vgi-rpc[sentry]``.
        token_ttl: Maximum age of stream state tokens in seconds.  Tokens
            older than this are rejected with HTTP 400.  Default is 3600
            (1 hour).  Set to ``0`` to disable expiry checking.
        compression_level: Zstandard compression level for HTTP request/
            response bodies.  ``3`` (the default) installs
            ``_CompressionMiddleware`` at level 3.  Valid range is 1-22.
            ``None`` disables compression entirely.
        enable_not_found_page: When ``True`` (the default), requests to
            paths that do not match any RPC route receive a friendly HTML
            404 page.  Set to ``False`` to use Falcon's default 404
            behaviour instead.
        enable_landing_page: When ``True`` (the default), ``GET {prefix}``
            returns a friendly HTML landing page showing the protocol name,
            server ID, and links.  Set to ``False`` to disable.
        enable_describe_page: When ``True`` (the default) **and** the server
            has ``enable_describe=True``, ``GET {prefix}/describe`` returns
            an HTML page listing all methods, parameters, and types.  The
            path ``{prefix}/describe`` is reserved when active — an RPC
            method named ``describe`` would need the page disabled.
        repo_url: Optional URL to the service's source repository (e.g. a
            GitHub URL).  When provided, a "Source repository" link appears
            on the landing page and describe page.

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
    # OpenTelemetry instrumentation (optional)
    if otel_config is not None:
        from vgi_rpc.otel import OtelConfig, _OtelFalconMiddleware, instrument_server

        if not isinstance(otel_config, OtelConfig):
            raise TypeError(f"otel_config must be an OtelConfig instance, got {type(otel_config).__name__}")
        instrument_server(server, otel_config)

    # Sentry error reporting (optional)
    if sentry_config is not None:
        from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

        if not isinstance(sentry_config, SentryConfig):
            raise TypeError(f"sentry_config must be a SentryConfig instance, got {type(sentry_config).__name__}")
        instrument_server_sentry(server, sentry_config)

    app_handler = _HttpRpcApp(
        server,
        signing_key,
        max_stream_response_bytes,
        max_request_bytes,
        upload_url_provider,
        max_upload_bytes,
        token_ttl,
    )
    middleware: list[Any] = [_RequestIdMiddleware()]

    # Compression middleware decompresses request bodies and compresses
    # responses — must come before auth so handlers read plaintext bodies.
    if compression_level is not None:
        middleware.append(_CompressionMiddleware(compression_level))

    # OTel middleware must come before auth so spans cover the full request
    if otel_config is not None:
        middleware.append(_OtelFalconMiddleware())

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

    # Describe page — GET {prefix}/describe (requires both flags and server support)
    describe_page_active = enable_describe_page and server.describe_enabled
    if describe_page_active:
        app.add_route(f"{prefix}/describe", _DescribePageResource(_build_describe_html(server, prefix, repo_url)))

    # Landing page — GET {prefix}
    if enable_landing_page:
        describe_path = f"{prefix}/describe" if describe_page_active else None
        landing_body = _build_landing_html(prefix, server.protocol_name, server.server_id, describe_path, repo_url)
        app.add_route(prefix, _LandingPageResource(landing_body))

    if enable_not_found_page:
        app.add_sink(_make_not_found_sink(prefix, server.protocol_name))

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
