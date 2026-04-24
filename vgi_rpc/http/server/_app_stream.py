# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Stream-call dispatch paths for the HTTP server (init, exchange, producer)."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Mapping
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import resolve_external_location
from vgi_rpc.metadata import CANCEL_KEY, STATE_KEY, strip_keys
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    _TICK_BATCH,
    AnnotatedBatch,
    AuthContext,
    CallContext,
    OutputCollector,
    RpcError,
    RpcMethodInfo,
    Stream,
    StreamState,
    VersionError,
    _ClientLogSink,
    _deserialize_params,
    _emit_access_log,
    _flush_collector,
    _get_auth_and_metadata,
    _log_method_error,
    _read_request,
    _validate_params,
    _write_error_batch,
    _write_stream_header,
)
from vgi_rpc.rpc._common import (
    CallStatistics,
    HookToken,
    _current_call_stats,
    _current_stream_id,
    _DispatchHook,
    _record_input,
    _record_output,
)
from vgi_rpc.utils import ValidatedReader, empty_batch

from .._common import _RpcHttpError
from ._state_token import (
    _pack_state_token,
    _resolve_state_cls,
    _serialize_state_bytes,
    _StateInfo,
    _unpack_state_token,
)

if TYPE_CHECKING:
    from ._app import _HttpRpcApp

_logger = logging.getLogger("vgi_rpc.http")


def _run_stream_init_sync(
    app: _HttpRpcApp,
    method_name: str,
    info: RpcMethodInfo,
    stream: IOBase,
) -> BytesIO:
    """Run stream init synchronously.

    For producer streams (input_schema == _EMPTY_SCHEMA), produces data
    immediately via _produce_stream_response.
    For exchange streams, returns a state token for subsequent exchanges.
    """
    stats = CallStatistics()
    stats_token = _current_call_stats.set(stats)
    try:
        state_info = app._state_types.get(method_name)
        if state_info is None:
            raise _RpcHttpError(
                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        try:
            ipc_method, kwargs = _read_request(stream, app._server.ipc_validation)
            if ipc_method != method_name:
                raise TypeError(
                    f"Method name mismatch: URL path has '{method_name}' but Arrow IPC "
                    f"custom_metadata 'vgi_rpc.method' has '{ipc_method}'. These must match."
                )
            _deserialize_params(kwargs, info.param_types, app._server.ipc_validation)
            _validate_params(info.name, kwargs, info.param_types)
        except (pa.ArrowInvalid, TypeError, StopIteration, RpcError, VersionError) as exc:
            raise _RpcHttpError(exc, status_code=HTTPStatus.BAD_REQUEST) from exc

        # Inject ctx if the implementation accepts it
        server_id = app._server.server_id
        protocol_name = app._server.protocol_name
        sink = _ClientLogSink(server_id=server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if method_name in app._server.ctx_methods:
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
        error_message = ""
        _response_state_bytes: bytes | None = None
        hook: _DispatchHook | None = app._server._dispatch_hook
        hook_token_init: HookToken = None
        if hook is not None:
            try:
                hook_token_init = hook.on_dispatch_start(info, auth, transport_metadata)
            except Exception:
                _logger.debug("Dispatch hook start failed", exc_info=True)
                hook = None
        _hook_exc: BaseException | None = None
        _current_stream_id.set(uuid.uuid4().hex)
        try:
            try:
                result: Stream[StreamState, Any] = getattr(app._server.implementation, method_name)(**kwargs)
            except (TypeError, pa.ArrowInvalid) as exc:
                _hook_exc = exc
                status = "error"
                error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                error_message = str(exc)[:500]
                http_status = HTTPStatus.BAD_REQUEST
                raise _RpcHttpError(exc, status_code=http_status) from exc
            except Exception as exc:
                _hook_exc = exc
                status = "error"
                error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                error_message = str(exc)[:500]
                http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                raise _RpcHttpError(exc, status_code=http_status) from exc

            is_producer = result.input_schema == _EMPTY_SCHEMA

            if is_producer:
                # Producer stream — write header (if declared) then run produce loop
                resp_buf = BytesIO()
                if info.header_type is not None:
                    _write_stream_header(
                        resp_buf, result.header, app._server.external_config, sink=sink, method_name=method_name
                    )
                produce_buf, produce_error_type, produce_error_msg = _produce_stream_response(
                    app,
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
                    error_message = produce_error_msg
                return resp_buf
            else:
                # Exchange stream — return state token
                try:
                    state = result.state
                    output_schema = result.output_schema
                    input_schema = result.input_schema

                    # Pack state + output schema + input schema into a signed token
                    state_bytes = _serialize_state_bytes(state, state_info)
                    _response_state_bytes = state_bytes
                    schema_bytes = output_schema.serialize().to_pybytes()
                    input_schema_bytes = input_schema.serialize().to_pybytes()
                    token = _pack_state_token(
                        state_bytes,
                        schema_bytes,
                        input_schema_bytes,
                        app._signing_key,
                        int(time.time()),
                        stream_id=_current_stream_id.get(),
                    )

                    # Write response: header (if declared) + log batches + zero-row batch with token in metadata
                    resp_buf = BytesIO()
                    if info.header_type is not None:
                        _write_stream_header(
                            resp_buf,
                            result.header,
                            app._server.external_config,
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
                    error_message = str(exc)[:500]
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
                server_version=app._server.server_version,
                error_message=error_message,
                response_state=_response_state_bytes,
            )
            if hook is not None:
                try:
                    hook.on_dispatch_end(hook_token_init, info, _hook_exc, stats=stats)
                except Exception:
                    _logger.debug("Dispatch hook end failed", exc_info=True)
    finally:
        _current_call_stats.reset(stats_token)


def _run_stream_exchange_sync(
    app: _HttpRpcApp,
    method_name: str,
    stream: IOBase,
) -> BytesIO:
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
        state_info = app._state_types.get(method_name)
        if state_info is None:
            raise _RpcHttpError(
                RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        # Read the input batch + extract token from metadata.
        # Note: unlike pipe transport we do not drain the stream here —
        # each HTTP request is independent, so there is no shared pipe to
        # keep in sync.  _DrainRequestMiddleware handles draining any
        # unconsumed body in process_response.
        try:
            req_reader = ValidatedReader(ipc.open_stream(stream), app._server.ipc_validation)
            input_batch, custom_metadata = req_reader.read_next_batch_with_custom_metadata()
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
        state_obj, output_schema, input_schema = _unpack_and_recover_state(app, token, state_info)

        is_producer = input_schema == _EMPTY_SCHEMA

        cancel_flag = custom_metadata is not None and custom_metadata.get(CANCEL_KEY) is not None

        # Record input batch for stats — skip tick batches on producer
        # continuations (zero-row, empty-schema protocol artifacts) and
        # skip cancel batches (zero-row, protocol artifacts).
        if not is_producer and not cancel_flag:
            _record_input(input_batch)

        # Resolve method info for hook (may be None for unknown methods, but
        # _stream_exchange_sync is only reached for known stream methods)
        info = app._server.methods.get(method_name)

        if cancel_flag:
            server_id = app._server.server_id
            protocol_name = app._server.protocol_name
            auth, transport_metadata = _get_auth_and_metadata()
            start = time.monotonic()
            cancel_sink = _ClientLogSink(server_id=server_id)
            cancel_ctx = CallContext(
                auth=auth,
                emit_client_log=cancel_sink,
                transport_metadata=transport_metadata,
                server_id=server_id,
                method_name=method_name,
                protocol_name=protocol_name,
            )
            try:
                state_obj.on_cancel(cancel_ctx)
            except Exception:
                _logger.debug("on_cancel hook failed", exc_info=True)
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, output_schema):
                pass
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                method_name,
                "stream",
                server_id,
                auth,
                transport_metadata,
                duration_ms,
                "ok",
                http_status=HTTPStatus.OK.value,
                stats=stats,
                server_version=app._server.server_version,
                request_state=token if isinstance(token, bytes) else token.encode() if token else None,
                cancelled=True,
            )
            resp_buf.seek(0)
            return resp_buf

        if is_producer:
            # Producer continuation
            server_id = app._server.server_id
            protocol_name = app._server.protocol_name
            auth, transport_metadata = _get_auth_and_metadata()
            start = time.monotonic()
            http_status = HTTPStatus.OK
            status: Literal["ok", "error"] = "ok"
            error_type = ""
            hook: _DispatchHook | None = app._server._dispatch_hook if info is not None else None
            hook_token_exch: HookToken = None
            if hook is not None and info is not None:
                try:
                    hook_token_exch = hook.on_dispatch_start(info, auth, transport_metadata)
                except Exception:
                    _logger.debug("Dispatch hook start failed", exc_info=True)
                    hook = None
            _hook_exc: BaseException | None = None
            produce_error_msg = ""
            try:
                resp_buf, produce_error_type, produce_error_msg = _produce_stream_response(
                    app,
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
                    error_message=produce_error_msg,
                    http_status=http_status.value,
                    stats=stats,
                    server_version=app._server.server_version,
                    request_state=token if isinstance(token, bytes) else token.encode() if token else None,
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
                    app._server.external_config,
                    ipc_validation=app._server.ipc_validation,
                )
            except Exception as exc:
                raise _RpcHttpError(exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR) from exc

            server_id = app._server.server_id
            protocol_name = app._server.protocol_name
            auth, transport_md = _get_auth_and_metadata()
            start = time.monotonic()
            http_status = HTTPStatus.OK
            status_str: Literal["ok", "error"] = "ok"
            error_type_str = ""
            hook_e: _DispatchHook | None = app._server._dispatch_hook if info is not None else None
            hook_token_e: HookToken = None
            if hook_e is not None and info is not None:
                try:
                    hook_token_e = hook_e.on_dispatch_start(info, auth, transport_md)
                except Exception:
                    _logger.debug("Dispatch hook start failed", exc_info=True)
                    hook_e = None
            _hook_exc_e: BaseException | None = None
            _response_state_bytes: bytes | None = None
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
                _response_state_bytes = updated_state_bytes
                schema_bytes = output_schema.serialize().to_pybytes()
                input_schema_bytes_upd = input_schema.serialize().to_pybytes()
                updated_token = _pack_state_token(
                    updated_state_bytes,
                    schema_bytes,
                    input_schema_bytes_upd,
                    app._signing_key,
                    int(time.time()),
                    stream_id=_current_stream_id.get(),
                )
                out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

                # Write response batches (log + data, in order)
                resp_buf = BytesIO()
                with ipc.new_stream(resp_buf, output_schema) as writer:
                    _flush_collector(writer, out, app._server.external_config)
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
                    server_version=app._server.server_version,
                    error_message=str(_hook_exc_e)[:500] if _hook_exc_e is not None else "",
                    request_state=token if isinstance(token, bytes) else token.encode() if token else None,
                    response_state=_response_state_bytes,
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
    app: _HttpRpcApp,
    schema: pa.Schema,
    state: StreamState,
    input_schema: pa.Schema,
    sink: _ClientLogSink | None = None,
    *,
    method_name: str,
    auth: AuthContext | None = None,
    transport_metadata: Mapping[str, str] | None = None,
) -> tuple[BytesIO, str | None, str]:
    """Run the produce loop for a producer stream, with optional size-based continuation.

    Args:
        app: The HTTP app holding server + limit configuration.
        schema: The output schema for the stream.
        state: The stream state object.
        input_schema: The input schema (stored in continuation tokens).
        sink: Optional log sink to flush before producing (initial request only).
        method_name: The RPC method name (for logging context).
        auth: Auth context; falls back to contextvar when ``None`` (continuation path).
        transport_metadata: Transport metadata; falls back to contextvar when ``None``.

    Returns:
        A ``(BytesIO, error_type, error_message)`` tuple — the IPC response
        stream, the exception class name on failure (``None`` on success),
        and the error message (empty on success).

    """
    if auth is None or transport_metadata is None:
        cv_auth, cv_md = _get_auth_and_metadata()
        auth = auth if auth is not None else cv_auth
        transport_metadata = transport_metadata if transport_metadata is not None else cv_md

    server_id = app._server.server_id
    protocol_name = app._server.protocol_name
    resp_buf = BytesIO()
    max_bytes = app._max_stream_response_bytes
    max_time = app._max_stream_response_time
    produce_error_type: str | None = None
    produce_error_message: str = ""
    with ipc.new_stream(resp_buf, schema) as writer:
        if sink is not None:
            sink.flush_contents(writer, schema)
        cumulative_bytes = 0
        start_time = time.monotonic()
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
                _flush_collector(writer, out, app._server.external_config)
                if out.finished:
                    break
                cumulative_bytes = out.total_data_bytes
                # Decide whether to continue producing or break with a
                # continuation token.  By default (no limits configured),
                # break after every produce cycle so the client receives
                # data incrementally.  When limits are configured, buffer
                # multiple batches until a limit is reached.
                should_continue = max_bytes is not None or max_time is not None
                if max_bytes is not None and resp_buf.tell() >= max_bytes:
                    should_continue = False
                if max_time is not None and (time.monotonic() - start_time) >= max_time:
                    should_continue = False
                if not should_continue:
                    # Serialize state into a continuation token
                    state_info = app._state_types.get(method_name)
                    if state_info is None:
                        raise _RpcHttpError(
                            RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                        )
                    state_bytes = _serialize_state_bytes(state, state_info)
                    schema_bytes = schema.serialize().to_pybytes()
                    input_schema_bytes = input_schema.serialize().to_pybytes()
                    token = _pack_state_token(
                        state_bytes,
                        schema_bytes,
                        input_schema_bytes,
                        app._signing_key,
                        int(time.time()),
                        stream_id=_current_stream_id.get(),
                    )
                    state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                    continuation_batch = empty_batch(schema)
                    _record_output(continuation_batch)
                    writer.write_batch(continuation_batch, custom_metadata=state_metadata)
                    break
        except Exception as exc:
            produce_error_type = _log_method_error(protocol_name, method_name, server_id, exc)
            produce_error_message = str(exc)[:500]
            _write_error_batch(writer, schema, exc, server_id=server_id)
    resp_buf.seek(0)
    return resp_buf, produce_error_type, produce_error_message


def _unpack_and_recover_state(
    app: _HttpRpcApp,
    token: bytes,
    state_info: _StateInfo,
) -> tuple[StreamState, pa.Schema, pa.Schema]:
    """Unpack a signed state token and recover state, output schema, and input schema.

    Args:
        app: The HTTP app providing signing key, TTL, and server implementation.
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
    state_bytes, schema_bytes, input_schema_bytes, stream_id = _unpack_state_token(
        token, app._signing_key, app._token_ttl
    )
    if stream_id:
        _current_stream_id.set(stream_id)

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
        state_obj = state_cls.deserialize_from_bytes(raw_state_bytes, app._server.ipc_validation)
        state_obj.rehydrate(app._server.implementation)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError(f"Failed to deserialize state: {exc}"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    return state_obj, output_schema, input_schema
