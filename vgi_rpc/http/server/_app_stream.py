# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Stream-call dispatch paths for the HTTP server (init, exchange, producer)."""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import resolve_external_location
from vgi_rpc.log import Message
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
    _coerce_input_batch,
    _deserialize_params,
    _emit_access_log,
    _flush_collector,
    _get_auth_and_metadata,
    _log_method_error,
    _read_request,
    _truncate_error_message,
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
    _derive_signing_key,
    _pack_state_token,
    _resolve_state_cls,
    _serialize_state_bytes,
    _StateInfo,
    _unpack_state_token,
)

if TYPE_CHECKING:
    from ._app import _HttpRpcApp

_logger = logging.getLogger("vgi_rpc.http")


@dataclass
class _DispatchOutcome:
    """Mutable telemetry fields populated by a dispatch shell.

    The shell runs its body inside ``with _dispatch_telemetry(...) as outcome:``
    and writes to these fields as work progresses (e.g. on error paths,
    after token mints, on cancel).  The context manager reads them on exit
    to emit the access log and dispatch-hook bookkeeping uniformly across
    all four stream-call shells.

    The fields intentionally cover both producer-loop turns (which may
    accumulate batches before returning) and lockstep exchange turns
    (which run ``state.process()`` exactly once); the shell decides how
    to drive its work, the outcome only records what the access log needs
    to know about the result.
    """

    status: Literal["ok", "error"] = "ok"
    error_type: str = ""
    error_message: str = ""
    http_status: HTTPStatus = HTTPStatus.OK
    response_state_bytes: bytes | None = None
    request_state_bytes: bytes | None = None
    cancelled: bool = False


@contextlib.contextmanager
def _dispatch_telemetry(
    app: _HttpRpcApp,
    *,
    info: RpcMethodInfo | None,
    method_name: str,
    method_type: str,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
) -> Iterator[_DispatchOutcome]:
    """Wrap a stream-call shell with shared start/end telemetry.

    On enter: optionally invokes ``hook.on_dispatch_start`` (when an
    ``RpcMethodInfo`` and a registered dispatch hook are both present)
    and captures the wall-clock start time.

    On exit (always — the ``finally`` runs whether the body returned
    normally or raised): emits the access-log record using the fields
    in the yielded :class:`_DispatchOutcome`, then invokes
    ``hook.on_dispatch_end`` with the captured exception (or ``None``
    on success).  Any exception raised by the body propagates to the
    caller after telemetry has been recorded.

    Pass ``info=None`` for code paths that are not regular dispatch —
    e.g. the ``/exchange`` cancel handler, which should still produce
    an access-log record but does not run a method (and so should not
    fire dispatch hooks).
    """
    server_id = app._server.server_id
    protocol_name = app._server.protocol_name
    outcome = _DispatchOutcome()
    start = time.monotonic()
    hook: _DispatchHook | None = app._server._dispatch_hook if info is not None else None
    hook_token: HookToken = None
    if hook is not None and info is not None:
        try:
            hook_token = hook.on_dispatch_start(info, auth, transport_metadata)
        except Exception:
            _logger.debug("Dispatch hook start failed", exc_info=True)
            hook = None
    hook_exc: BaseException | None = None
    try:
        try:
            yield outcome
        except BaseException as exc:
            hook_exc = exc
            raise
    finally:
        duration_ms = (time.monotonic() - start) * 1000
        _emit_access_log(
            protocol_name,
            method_name,
            method_type,
            server_id,
            auth,
            transport_metadata,
            duration_ms,
            outcome.status,
            outcome.error_type,
            http_status=outcome.http_status.value,
            stats=_current_call_stats.get(),
            server_version=app._server.server_version,
            protocol_hash=app._server.protocol_hash,
            protocol_version=app._server.protocol_version,
            error_message=outcome.error_message,
            request_state=outcome.request_state_bytes,
            response_state=outcome.response_state_bytes,
            cancelled=outcome.cancelled,
        )
        if hook is not None and info is not None:
            try:
                hook.on_dispatch_end(hook_token, info, hook_exc, stats=_current_call_stats.get())
            except Exception:
                _logger.debug("Dispatch hook end failed", exc_info=True)


def _run_stream_init_sync(
    app: _HttpRpcApp,
    method_name: str,
    info: RpcMethodInfo,
    stream: IOBase,
) -> BytesIO:
    """Run stream init synchronously.

    For producer streams (input_schema == _EMPTY_SCHEMA), produces data
    immediately via _run_http_producer_turn.
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
            ipc_method, kwargs = _read_request(stream, app._server.ipc_validation, app._server.external_config)
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

        # The chain-correlation id for all HTTP turns of this stream.  Carried
        # in every continuation token; observers (logger formatters, OTel)
        # also see it via ``_current_stream_id`` so they can tag log lines
        # without threading the value through every helper signature.
        stream_id = uuid.uuid4().hex
        _current_stream_id.set(stream_id)
        with _dispatch_telemetry(
            app,
            info=info,
            method_name=method_name,
            method_type=info.method_type.value,
            auth=auth,
            transport_metadata=transport_metadata,
        ) as outcome:
            try:
                result: Stream[StreamState, Any] = getattr(app._server.implementation, method_name)(**kwargs)
            except (TypeError, pa.ArrowInvalid) as exc:
                outcome.status = "error"
                outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                outcome.error_message = _truncate_error_message(exc)
                outcome.http_status = HTTPStatus.BAD_REQUEST
                raise _RpcHttpError(exc, status_code=outcome.http_status) from exc
            except Exception as exc:
                outcome.status = "error"
                outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                outcome.error_message = _truncate_error_message(exc)
                outcome.http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                raise _RpcHttpError(exc, status_code=outcome.http_status) from exc

            is_producer = result.input_schema == _EMPTY_SCHEMA

            if is_producer:
                # Producer stream — write header (if declared) then run produce loop
                resp_buf = BytesIO()
                if info.header_type is not None:
                    _write_stream_header(
                        resp_buf, result.header, app._server.external_config, sink=sink, method_name=method_name
                    )
                produce_buf, produce_error_type, produce_error_msg = _run_http_producer_turn(
                    app,
                    result.output_schema,
                    result.state,
                    result.input_schema,
                    sink,
                    method_name=method_name,
                    stream_id=stream_id,
                    auth=auth,
                    transport_metadata=transport_metadata,
                )
                resp_buf.write(produce_buf.getvalue())
                resp_buf.seek(0)
                if produce_error_type is not None:
                    outcome.status = "error"
                    outcome.error_type = produce_error_type
                    outcome.error_message = produce_error_msg
                return resp_buf
            else:
                # Exchange stream — return state token
                try:
                    state = result.state
                    output_schema = result.output_schema
                    input_schema = result.input_schema

                    # Pack state + output schema + input schema into a signed token
                    state_bytes = _serialize_state_bytes(state, state_info)
                    outcome.response_state_bytes = state_bytes
                    schema_bytes = output_schema.serialize().to_pybytes()
                    input_schema_bytes = input_schema.serialize().to_pybytes()
                    token = _pack_state_token(
                        state_bytes,
                        schema_bytes,
                        input_schema_bytes,
                        _derive_signing_key(app._signing_key, auth),
                        int(time.time()),
                        stream_id=stream_id,
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
                    outcome.status = "error"
                    outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    outcome.error_message = _truncate_error_message(exc)
                    outcome.http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                    raise _RpcHttpError(exc, status_code=outcome.http_status) from exc

                resp_buf.seek(0)
                return resp_buf
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

        # Resolve auth up front: the state token is signed with a key derived
        # from the caller's identity, so we need auth to verify it.
        auth, transport_metadata = _get_auth_and_metadata()

        # Unpack and verify the signed token, recover state + schema + input_schema
        state_obj, output_schema, input_schema, stream_id = _unpack_and_recover_state(app, token, state_info, auth)

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

        request_state_bytes = token if isinstance(token, bytes) else token.encode() if token else None

        if cancel_flag:
            # Cancel is not a method dispatch — pass info=None to suppress
            # dispatch-hook calls; access log still records cancelled=True.
            server_id = app._server.server_id
            protocol_name = app._server.protocol_name
            with _dispatch_telemetry(
                app,
                info=None,
                method_name=method_name,
                method_type="stream",
                auth=auth,
                transport_metadata=transport_metadata,
            ) as outcome:
                outcome.cancelled = True
                outcome.request_state_bytes = request_state_bytes
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
                resp_buf.seek(0)
                return resp_buf

        if is_producer:
            # Producer continuation — multi-batch capable; the producer-turn
            # helper buffers up to ``max_response_bytes`` before
            # emitting a continuation token.
            with _dispatch_telemetry(
                app,
                info=info,
                method_name=method_name,
                method_type="stream",
                auth=auth,
                transport_metadata=transport_metadata,
            ) as outcome:
                outcome.request_state_bytes = request_state_bytes
                resp_buf, produce_error_type, produce_error_msg = _run_http_producer_turn(
                    app,
                    output_schema,
                    state_obj,
                    input_schema,
                    method_name=method_name,
                    stream_id=stream_id,
                )
                if produce_error_type is not None:
                    outcome.status = "error"
                    outcome.error_type = produce_error_type
                    outcome.error_message = produce_error_msg
                return resp_buf
        else:
            # Exchange — lockstep one input batch in, one output batch out.
            # External-location resolution must happen before we hand the
            # batch to ``state.process``; if resolution fails we surface a
            # 500 directly (no telemetry shell — the failure pre-dates the
            # method dispatch).
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
            with _dispatch_telemetry(
                app,
                info=info,
                method_name=method_name,
                method_type="stream",
                auth=auth,
                transport_metadata=transport_metadata,
            ) as outcome:
                outcome.request_state_bytes = request_state_bytes
                try:
                    # Reconcile the inbound batch's schema against the declared
                    # input schema (strict on field set, tolerant of order/type).
                    # Same helper used by the pipe transport so callers see a
                    # uniform TypeError on mismatch regardless of transport.
                    input_batch = _coerce_input_batch(input_batch, input_schema)

                    # Strip state token from metadata visible to process()
                    user_cm = strip_keys(resolved_cm, STATE_KEY)

                    ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)
                    out = OutputCollector(output_schema, server_id=server_id, producer_mode=False)

                    process_ctx = CallContext(
                        auth=auth,
                        emit_client_log=out.emit_client_log_message,
                        transport_metadata=transport_metadata,
                        server_id=server_id,
                        method_name=method_name,
                        protocol_name=protocol_name,
                    )
                    state_obj.process(ab_in, out, process_ctx)
                    if not out.finished:
                        out.validate()

                    # Repack updated state with same schemas into new signed token
                    updated_state_bytes = _serialize_state_bytes(state_obj, state_info)
                    outcome.response_state_bytes = updated_state_bytes
                    schema_bytes = output_schema.serialize().to_pybytes()
                    input_schema_bytes_upd = input_schema.serialize().to_pybytes()
                    updated_token = _pack_state_token(
                        updated_state_bytes,
                        schema_bytes,
                        input_schema_bytes_upd,
                        _derive_signing_key(app._signing_key, auth),
                        int(time.time()),
                        stream_id=stream_id,
                    )
                    out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

                    # Write response batches (log + data, in order)
                    resp_buf = BytesIO()
                    with ipc.new_stream(resp_buf, output_schema) as writer:
                        _flush_collector(writer, out, app._server.external_config)
                except Exception as exc:
                    outcome.status = "error"
                    outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
                    outcome.error_message = _truncate_error_message(exc)
                    outcome.http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                    raise _RpcHttpError(exc, status_code=outcome.http_status, schema=output_schema) from exc

                resp_buf.seek(0)
                return resp_buf
    finally:
        _current_call_stats.reset(stats_token)


def _run_http_producer_turn(
    app: _HttpRpcApp,
    schema: pa.Schema,
    state: StreamState,
    input_schema: pa.Schema,
    sink: _ClientLogSink | None = None,
    *,
    method_name: str,
    stream_id: str,
    auth: AuthContext | None = None,
    transport_metadata: Mapping[str, str] | None = None,
) -> tuple[BytesIO, str | None, str]:
    """Run one HTTP turn of a producer stream, returning an IPC body + error fields.

    A "turn" here means a single HTTP request/response cycle:

    - ``state.process()`` is invoked one or more times against ``_TICK_BATCH``;
      each call may emit a data batch, log batches, and/or call ``finish()``.
    - All emitted batches are written to a single IPC stream that becomes the
      HTTP response body.
    - The loop exits when the stream finishes (no continuation token) or when
      the wire-effective response size (HTTP body + externalized payload)
      reaches ``app._max_response_bytes``, in which case a continuation
      token is appended as a zero-row sentinel and the client resumes via
      ``POST /{method}/exchange``.

    HTTP-specific responsibilities — the reason this is its own helper rather
    than shared with the pipe transport:

    - Mints a signed continuation token carrying serialised state + schemas
      (HTTP is stateless across requests).
    - Charges externalised payload bytes against the wire-size cap so
      ``ExternalLocationConfig`` cannot silently bypass it.

    The pipe-family producer loop in :mod:`vgi_rpc.rpc._server` looks similar
    on the surface but holds ``state`` in-process for the duration of the
    connection, never serialises it, and never mints tokens.

    Args:
        app: The HTTP app holding server + limit configuration.
        schema: The output schema for the stream.
        state: The stream state object.
        input_schema: The input schema (stored in continuation tokens).
        sink: Optional log sink to flush before producing (initial request only).
        method_name: The RPC method name (for logging context).
        stream_id: The chain-correlation id baked into the continuation
            token.  Generated fresh by the init turn and recovered from
            the inbound token by exchange/continuation turns; passed in
            explicitly so this helper does not depend on the
            ``_current_stream_id`` contextvar (the contextvar is kept
            only for ambient telemetry observers).
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
    max_bytes = app._max_response_bytes
    produce_error_type: str | None = None
    produce_error_message: str = ""
    with ipc.new_stream(resp_buf, schema) as writer:
        if sink is not None:
            sink.flush_contents(writer, schema)
        cumulative_bytes = 0
        # Bytes uploaded to external storage across this HTTP turn.  Tracked
        # separately from the HTTP body cap (``max_response_bytes`` measures
        # ``resp_buf.tell()`` only — externalised payloads do not occupy the
        # wire body, so charging them against the body cap would conflate
        # two different operator concerns).  External payload size is
        # governed by ``max_externalized_response_bytes`` in Phase B.
        cumulative_external_bytes = 0
        # The CallContext is hoisted out of the loop — its non-collector
        # fields (auth, transport_metadata, ids) are constant for the whole
        # turn.  ``emit_client_log`` is the only thing that needs to follow
        # the current iteration's ``OutputCollector``; we route it through a
        # tiny mutable proxy that the loop body re-points each turn.
        current_out: list[OutputCollector | None] = [None]

        def _emit_to_current(msg: Message) -> None:
            collector = current_out[0]
            if collector is not None:
                collector.emit_client_log_message(msg)

        produce_ctx = CallContext(
            auth=auth,
            emit_client_log=_emit_to_current,
            transport_metadata=transport_metadata,
            server_id=server_id,
            method_name=method_name,
            protocol_name=protocol_name,
        )
        try:
            while True:
                out = OutputCollector(
                    schema, prior_data_bytes=cumulative_bytes, server_id=server_id, producer_mode=True
                )
                current_out[0] = out
                state.process(_TICK_BATCH, out, produce_ctx)
                if not out.finished:
                    out.validate()
                cumulative_external_bytes += _flush_collector(writer, out, app._server.external_config)
                if out.finished:
                    break
                cumulative_bytes = out.total_data_bytes
                # Decide whether to continue producing or break with a
                # continuation token.  By default (no limit configured),
                # break after every produce cycle so the client receives
                # data incrementally.  When ``max_bytes`` is configured,
                # buffer multiple batches until the HTTP body fills the cap.
                # Externalised payloads do *not* count toward this cap —
                # only what's literally on the wire (``resp_buf.tell()``)
                # does.  External-channel volume has its own cap (Phase B).
                should_continue = max_bytes is not None and resp_buf.tell() < max_bytes
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
                        _derive_signing_key(app._signing_key, auth),
                        int(time.time()),
                        stream_id=stream_id,
                    )
                    state_metadata = pa.KeyValueMetadata({STATE_KEY: token})
                    continuation_batch = empty_batch(schema)
                    _record_output(continuation_batch)
                    writer.write_batch(continuation_batch, custom_metadata=state_metadata)
                    break
        except Exception as exc:
            produce_error_type = _log_method_error(protocol_name, method_name, server_id, exc)
            produce_error_message = _truncate_error_message(exc)
            _write_error_batch(writer, schema, exc, server_id=server_id)
    resp_buf.seek(0)
    return resp_buf, produce_error_type, produce_error_message


def _unpack_and_recover_state(
    app: _HttpRpcApp,
    token: bytes,
    state_info: _StateInfo,
    auth: AuthContext | None,
) -> tuple[StreamState, pa.Schema, pa.Schema, str]:
    """Unpack a signed state token and recover state, output schema, and input schema.

    Args:
        app: The HTTP app providing signing key, TTL, and server implementation.
        token: The signed state token bytes.
        state_info: A single concrete state class, or an ordered tuple of
            concrete classes for union types.  When a tuple is provided, the
            concrete class is resolved from the numeric tag embedded in
            ``state_bytes``.
        auth: Authenticated identity for the current request.  The token
            signing key is derived from this identity so state tokens
            cannot be replayed across users.

    Returns:
        Tuple of ``(state_object, output_schema, input_schema, stream_id)``.
        ``stream_id`` carries the chain identifier from the unpacked token
        and must be threaded back into the next continuation token by
        callers; the contextvar ``_current_stream_id`` is also updated as
        a convenience for ambient telemetry observers (logger formatters,
        OTel hooks).

    Raises:
        _RpcHttpError: On malformed tokens, expired tokens, failed
            deserialization, or signature verification failure.

    """
    state_bytes, schema_bytes, input_schema_bytes, stream_id = _unpack_state_token(
        token, _derive_signing_key(app._signing_key, auth), app._token_ttl
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

    return state_obj, output_schema, input_schema, stream_id
