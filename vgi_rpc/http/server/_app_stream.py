# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Stream-call dispatch paths for the HTTP server (init, exchange, producer)."""

from __future__ import annotations

import contextlib
import logging
import secrets
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import predict_externalize_bytes_for_collector, resolve_external_location
from vgi_rpc.log import Message
from vgi_rpc.metadata import CALL_STATE_KEY, CANCEL_KEY, PROTOCOL_VERSION_KEY, STATE_KEY, strip_keys
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
    _current_body_precompressed,
    _current_call_stats,
    _current_request_metadata,
    _current_response_codec,
    _current_stream_id,
    _DispatchHook,
    _record_input,
    _record_output,
)
from vgi_rpc.utils import ArrowSerializableDataclass, ValidatedReader, empty_batch, new_ipc_stream

from .._common import _RpcHttpError
from ._responses import _current_response_status, _enforce_response_budgets
from ._state_token import (
    _compute_aad,
    _compute_call_aad,
    _mint_call_token,
    _mint_cursor_token,
    _open_call_token,
    _open_cursor_token,
    _resolve_state_cls,
    _ResolvedCall,
    _StateInfo,
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
    kwargs: Mapping[str, Any] | None = None,
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
    fire dispatch hooks).  ``kwargs`` is forwarded to the hook so
    observability backends can attach RPC parameters to traces; pass
    ``None`` (the default) when no kwargs are available (cancel paths).
    """
    server_id = app._server.server_id
    protocol_name = app._server.protocol_name
    outcome = _DispatchOutcome()
    start = time.monotonic()
    hook: _DispatchHook | None = app._server._dispatch_hook if info is not None else None
    hook_token: HookToken = None
    if hook is not None and info is not None:
        try:
            hook_token = hook.on_dispatch_start(info, auth, transport_metadata, kwargs or {})
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
            # Application-protocol-version gate (mirror of RpcServer.serve_one). HTTP
            # stream init dispatches directly here; the check has to be wired in
            # independently. Stream methods include no synthetic ``__describe__``,
            # but the same exemption is kept for consistency with the unary path.
            # Server opts out by not declaring ``protocol_version`` on its Protocol.
            if app._server._protocol_version_parts is not None and method_name != "__describe__":
                md = _current_request_metadata.get()
                app._server._check_protocol_version(md.get(PROTOCOL_VERSION_KEY) if md is not None else None)
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
                kind=app._server.transport_kind,
                implementation=app._server.implementation,
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
            kwargs=kwargs,
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

            # Mint the stream's call token once, here.  Everything it carries —
            # the call state, both schemas, the stream id — is fixed for the
            # life of the stream, so this is the only time any of it is
            # serialized or sealed.  Continuations echo the token back and the
            # server resolves it from cache; see ``_state_token`` for why that
            # lookup is safe.
            call_token, call_id, call_state_bytes = _mint_call_token(
                result.call_state,
                result.output_schema,
                result.input_schema,
                app._token_key,
                auth,
                stream_id,
            )
            # Warm the cache with the objects we already hold, so this stream's
            # first continuation does not have to open the token it was just
            # handed.
            app._call_state_cache.put(
                call_id,
                auth,
                _ResolvedCall(result.call_state, result.output_schema, result.input_schema, stream_id),
                time.time(),
            )

            if result.input_schema == _EMPTY_SCHEMA:
                return _run_http_producer_init(
                    app,
                    info=info,
                    result=result,
                    sink=sink,
                    method_name=method_name,
                    stream_id=stream_id,
                    call_id=call_id,
                    call_token=call_token,
                    call_state_bytes=call_state_bytes,
                    auth=auth,
                    transport_metadata=transport_metadata,
                    outcome=outcome,
                )
            return _run_http_exchange_init(
                app,
                info=info,
                result=result,
                state_info=state_info,
                sink=sink,
                method_name=method_name,
                call_id=call_id,
                call_token=call_token,
                auth=auth,
                transport_metadata=transport_metadata,
                outcome=outcome,
            )
    finally:
        _current_call_stats.reset(stats_token)


def _run_http_producer_init(
    app: _HttpRpcApp,
    *,
    info: RpcMethodInfo,
    result: Stream[StreamState, Any],
    sink: _ClientLogSink,
    method_name: str,
    stream_id: str,
    call_id: bytes,
    call_token: bytes,
    call_state_bytes: bytes,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
    outcome: _DispatchOutcome,
) -> BytesIO:
    """Init for a producer stream — header (if declared) then producer turn.

    The header lives in its own IPC stream (separate schema), so the
    response body is two concatenated IPC streams: the header stream
    followed by the producer turn's data stream.  ``_run_http_producer_turn``
    handles the producer-loop semantics; this helper just stitches the
    optional header onto the front.
    """
    resp_buf = BytesIO()
    if info.header_type is not None:
        _write_stream_header(resp_buf, result.header, app._server.external_config, sink=sink, method_name=method_name)
    # Over HTTP a producer's first turn runs INSIDE the /init request, so the init
    # request's Arrow metadata IS the first tick's metadata — surface it to the
    # producer's first process() call. Without this the first turn sees the empty
    # _TICK_BATCH and never observes first-tick metadata (e.g. the result-cache
    # conditional-revalidation validators vgi.cache.if_none_match), which on the pipe
    # transport ride a real first tick. Only the init turn carries it; continuation
    # /exchange turns pass their own client tick metadata as usual.
    init_request_metadata = _current_request_metadata.get()
    produce_buf = _run_http_producer_turn(
        app,
        schema=result.output_schema,
        state=result.state,
        input_schema=result.input_schema,
        method_name=method_name,
        stream_id=stream_id,
        call_id=call_id,
        call_token=call_token,
        call_state_bytes=call_state_bytes,
        auth=auth,
        transport_metadata=transport_metadata,
        outcome=outcome,
        sink=sink,
        init_request_metadata=init_request_metadata,
    )
    # `produce_buf` is a native BufferReader now; read it out to splice into the
    # init response, which still carries a header written ahead of it.
    resp_buf.write(produce_buf.read())
    resp_buf.seek(0)
    return resp_buf


def _run_http_exchange_init(
    app: _HttpRpcApp,
    *,
    info: RpcMethodInfo,
    result: Stream[StreamState, Any],
    state_info: _StateInfo,
    sink: _ClientLogSink,
    method_name: str,
    call_id: bytes,
    call_token: bytes,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
    outcome: _DispatchOutcome,
) -> BytesIO:
    """Init for an exchange stream — header (if declared) + the stream's two tokens.

    Writes a zero-row sentinel batch carrying both the call token (issued
    exactly once, here) and the first cursor token.  The client echoes both
    on each subsequent ``POST /{method}/exchange`` call; only the cursor
    token is refreshed in the responses that follow.
    """
    server_id = app._server.server_id
    protocol_name = app._server.protocol_name
    try:
        state = result.state
        output_schema = result.output_schema

        token, state_bytes = _mint_cursor_token(
            state,
            state_info,
            call_id,
            app._token_key,
            auth,
        )
        outcome.response_state_bytes = state_bytes

        # Response: header (if declared) + zero-row batch carrying the tokens.
        resp_buf = BytesIO()
        if info.header_type is not None:
            _write_stream_header(
                resp_buf,
                result.header,
                app._server.external_config,
                sink=sink,
                method_name=method_name,
            )
        with new_ipc_stream(resp_buf, output_schema) as writer:
            sink.flush_contents(writer, output_schema)
            state_metadata = pa.KeyValueMetadata({STATE_KEY: token, CALL_STATE_KEY: call_token})
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

        # Extract both tokens before resolution — resolve_external_location
        # replaces metadata with what was stored in the external IPC stream.
        token = custom_metadata.get(STATE_KEY) if custom_metadata is not None else None
        call_token = custom_metadata.get(CALL_STATE_KEY) if custom_metadata is not None else None

        if token is None:
            raise _RpcHttpError(
                RuntimeError("Missing state token in exchange request"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

        # Resolve auth up front: both tokens' AAD is bound to the caller's
        # identity, so we need auth to open either.
        auth, transport_metadata = _get_auth_and_metadata()

        # Open the cursor token, resolve the call it belongs to (cache hit in
        # the common case), and rebuild the state object.
        (
            state_obj,
            resolved_call,
            call_id,
            request_state_bytes,
        ) = _unpack_and_recover_state(app, token, call_token, state_info, auth)
        output_schema = resolved_call.output_schema
        input_schema = resolved_call.input_schema
        stream_id = resolved_call.stream_id

        is_producer = input_schema == _EMPTY_SCHEMA

        cancel_flag = custom_metadata is not None and custom_metadata.get(CANCEL_KEY) is not None

        # Record input batch for stats — skip tick batches on producer
        # continuations (zero-row, empty-schema protocol artifacts) and
        # skip cancel batches (zero-row, protocol artifacts).
        if not is_producer and not cancel_flag:
            _record_input(input_batch)

        # Resolve method info for hook.  ``_stream_exchange_sync`` is only
        # reached for known stream methods (``_resources.py`` rejects
        # unknown methods with 404 before dispatch), so this is always
        # populated; the cancel branch passes ``info=None`` to its
        # telemetry shell deliberately, not because info is missing.
        info = app._server.methods.get(method_name)

        # ``request_state_bytes`` holds the *decrypted plaintext* state — so
        # the access log records a useful audit artifact (and not the
        # opaque AEAD ciphertext the client supplied on the wire).

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
                    kind=app._server.transport_kind,
                    implementation=app._server.implementation,
                )
                try:
                    state_obj.on_cancel(cancel_ctx)
                except Exception:
                    _logger.debug("on_cancel hook failed", exc_info=True)
                resp_buf = BytesIO()
                with new_ipc_stream(resp_buf, output_schema):
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
                return _run_http_producer_turn(
                    app,
                    owns_response_body=True,
                    schema=output_schema,
                    state=state_obj,
                    input_schema=input_schema,
                    method_name=method_name,
                    stream_id=stream_id,
                    call_id=call_id,
                    auth=auth,
                    transport_metadata=transport_metadata,
                    outcome=outcome,
                )
        # Exchange — lockstep one input batch in, one output batch out.
        # External resolution + coercion + state.process + token mint +
        # cap enforcement live in ``_run_http_exchange_turn``; this
        # branch is just the telemetry envelope.
        with _dispatch_telemetry(
            app,
            info=info,
            method_name=method_name,
            method_type="stream",
            auth=auth,
            transport_metadata=transport_metadata,
        ) as outcome:
            outcome.request_state_bytes = request_state_bytes
            return _run_http_exchange_turn(
                app,
                state=state_obj,
                state_info=state_info,
                output_schema=output_schema,
                input_schema=input_schema,
                input_batch=input_batch,
                custom_metadata=custom_metadata,
                method_name=method_name,
                call_id=call_id,
                auth=auth,
                transport_metadata=transport_metadata,
                outcome=outcome,
            )
    finally:
        _current_call_stats.reset(stats_token)


def _run_http_exchange_turn(
    app: _HttpRpcApp,
    *,
    state: StreamState,
    state_info: _StateInfo,
    output_schema: pa.Schema,
    input_schema: pa.Schema,
    input_batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    method_name: str,
    call_id: bytes,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
    outcome: _DispatchOutcome,
) -> BytesIO:
    """Run one HTTP exchange turn — lockstep input → output → refreshed cursor token.

    Mirrors :func:`_run_http_producer_turn` for the exchange shape:

    - Resolves any external-location pointer in the inbound batch.
    - Coerces the inbound batch's schema against the declared input schema.
    - Runs ``state.process()`` exactly once with the budget snapshots
      surfaced on ``OutputCollector``.
    - Mints a refreshed cursor token via :func:`_mint_cursor_token`.  The
      call token is not re-issued; nothing it carries can have changed.
    - Pre-flights ``max_externalized_response_bytes`` so an oversize
      upload is refused before the storage round-trip.
    - Writes the response IPC stream and post-flush enforces the
      wire-body cap.

    Mutates ``outcome`` for telemetry: ``response_state_bytes`` on
    success, ``status``/``error_type``/``error_message`` on either kind
    of overshoot.  Unexpected exceptions during ``state.process`` raise
    :class:`_RpcHttpError` (status 500) so the telemetry shell records
    them via the ``hook_exc`` path.
    """
    server_id = app._server.server_id
    protocol_name = app._server.protocol_name

    # External-location resolution on inbound input.  Failures pre-date
    # the method dispatch but still need to surface as 500 to the client.
    try:
        input_batch, resolved_cm = resolve_external_location(
            input_batch,
            custom_metadata,
            app._server.external_config,
            ipc_validation=app._server.ipc_validation,
        )
    except Exception as exc:
        outcome.status = "error"
        outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
        outcome.error_message = _truncate_error_message(exc)
        outcome.http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        raise _RpcHttpError(exc, status_code=outcome.http_status) from exc

    try:
        # Reconcile the inbound batch's schema against the declared
        # input schema (strict on field set, tolerant of order/type).
        input_batch = _coerce_input_batch(input_batch, input_schema)

        user_cm = strip_keys(resolved_cm, STATE_KEY, CALL_STATE_KEY)
        ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=user_cm)

        # Exchange is lockstep — one process() call, one output batch,
        # one HTTP response.  The whole budget is available to this
        # single emit; there's no prior accumulation.
        ext_enabled = app._server.external_config is not None and app._server.external_config.storage is not None
        out = OutputCollector(
            output_schema,
            server_id=server_id,
            producer_mode=False,
            remaining_response_bytes=app._max_response_bytes,
            remaining_externalized_response_bytes=(app._max_externalized_response_bytes if ext_enabled else None),
            externalization_enabled=ext_enabled,
        )

        process_ctx = CallContext(
            auth=auth,
            emit_client_log=out.emit_client_log_message,
            transport_metadata=transport_metadata,
            server_id=server_id,
            method_name=method_name,
            protocol_name=protocol_name,
            kind=app._server.transport_kind,
            implementation=app._server.implementation,
        )
        state.process(ab_in, out, process_ctx)
        if not out.finished:
            out.validate()

        # Refresh the cursor token.  The call token is not re-issued: nothing
        # it carries can have changed, and the client still holds it.
        updated_token, updated_state_bytes = _mint_cursor_token(
            state,
            state_info,
            call_id,
            app._token_key,
            auth,
        )
        outcome.response_state_bytes = updated_state_bytes
        out.merge_data_metadata(pa.KeyValueMetadata({STATE_KEY: updated_token}))

        # Pre-flight the external cap BEFORE flushing — refuse a violating
        # upload without paying for the storage round-trip.
        ext_cfg = app._server.external_config
        predicted_external = predict_externalize_bytes_for_collector(out, ext_cfg) if ext_cfg is not None else 0
        if (
            app._max_externalized_response_bytes is not None
            and predicted_external > app._max_externalized_response_bytes
        ):
            overshoot = RuntimeError(
                f"Externalised payload exceeds max_externalized_response_bytes "
                f"({predicted_external} > {app._max_externalized_response_bytes}) "
                f"for method {method_name!r}"
            )
            return _exchange_error_response(output_schema, server_id, protocol_name, method_name, overshoot, outcome)

        # Write response batches (log + data, in order).
        resp_buf = BytesIO()
        exchange_external_bytes = 0
        with new_ipc_stream(resp_buf, output_schema) as writer:
            exchange_external_bytes = _flush_collector(writer, out, app._server.external_config)

        # Wire body cap — checked post-flush since BytesIO writes are free.
        try:
            _enforce_response_budgets(
                method_name=method_name,
                wire_bytes=resp_buf.tell(),
                external_bytes=exchange_external_bytes,
                wire_cap=app._max_response_bytes,
                external_cap=app._max_externalized_response_bytes,
            )
        except RuntimeError as overshoot:
            return _exchange_error_response(output_schema, server_id, protocol_name, method_name, overshoot, outcome)

        resp_buf.seek(0)
        return resp_buf
    except Exception as exc:
        outcome.status = "error"
        outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
        outcome.error_message = _truncate_error_message(exc)
        outcome.http_status = HTTPStatus.INTERNAL_SERVER_ERROR
        raise _RpcHttpError(exc, status_code=outcome.http_status, schema=output_schema) from exc


def _exchange_error_response(
    output_schema: pa.Schema,
    server_id: str,
    protocol_name: str,
    method_name: str,
    exc: BaseException,
    outcome: _DispatchOutcome,
) -> BytesIO:
    """Build a fresh response IPC stream containing only an EXCEPTION batch.

    Used by :func:`_run_http_exchange_turn` on either kind of cap
    overshoot — the worker emitted a successful batch, but the response
    would violate an operator cap, so we discard the oversize body and
    deliver an :class:`RpcError` round-trip instead.
    """
    outcome.status = "error"
    outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
    outcome.error_message = _truncate_error_message(exc)
    # Signal the resource layer to surface this as 200 + X-VGI-RPC-Error: true
    # so the documented hard-cap contract holds for stream-exchange.
    _current_response_status.set(HTTPStatus.INTERNAL_SERVER_ERROR)
    resp_buf = BytesIO()
    with new_ipc_stream(resp_buf, output_schema) as err_writer:
        _write_error_batch(err_writer, output_schema, exc, server_id=server_id)
    resp_buf.seek(0)
    return resp_buf


def _run_http_producer_turn(
    app: _HttpRpcApp,
    *,
    schema: pa.Schema,
    state: StreamState,
    input_schema: pa.Schema,
    method_name: str,
    stream_id: str,
    call_id: bytes,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
    outcome: _DispatchOutcome,
    sink: _ClientLogSink | None = None,
    init_request_metadata: pa.KeyValueMetadata | None = None,
    owns_response_body: bool = False,
    call_token: bytes | None = None,
    call_state_bytes: bytes | None = None,
) -> pa.BufferReader:
    """Run one HTTP turn of a producer stream.

    A "turn" here means a single HTTP request/response cycle:

    - ``state.process()`` is invoked one or more times against ``_TICK_BATCH``;
      each call may emit a data batch, log batches, and/or call ``finish()``.
    - All emitted batches are written to a single IPC stream that becomes the
      HTTP response body.
    - The loop exits when the stream finishes (no continuation token) or when
      the wire body reaches ``app._max_response_bytes``, in which case a
      continuation token is appended as a zero-row sentinel and the client
      resumes via ``POST /{method}/exchange``.

    HTTP-specific responsibilities — the reason this is its own helper rather
    than shared with the pipe transport:

    - Mints a signed continuation token carrying serialised state + schemas
      (HTTP is stateless across requests).  When minted,
      ``outcome.response_state_bytes`` is set so the access log records the
      issued state.
    - Pre-flights ``max_externalized_response_bytes`` so a violating upload
      is refused before incurring the storage round-trip.

    The pipe-family producer loop in :mod:`vgi_rpc.rpc._server` looks similar
    on the surface but holds ``state`` in-process for the duration of the
    connection, never serialises it, and never mints tokens.

    Args:
        app: The HTTP app holding server + limit configuration.
        schema: The output schema for the stream.
        state: The stream state object.
        input_schema: The input schema (stored in the call token).
        method_name: The RPC method name (for logging context).
        stream_id: The chain-correlation id for this stream.  Generated
            fresh by the init turn and recovered from the inbound call
            token by continuation turns; passed in explicitly so this
            helper does not depend on the ``_current_stream_id``
            contextvar.
        call_id: The stream's call id, baked into every cursor token this
            turn mints so the next request can resolve its call state.
        auth: Authenticated identity for this request.
        transport_metadata: Transport metadata for the CallContext.
        outcome: The shared :class:`_DispatchOutcome` from the surrounding
            telemetry shell.  Mutated on continuation-token mint
            (``response_state_bytes``) and on any error path
            (``status``, ``error_type``, ``error_message``).
        sink: Optional log sink to flush before producing (initial request
            only).  Continuation calls pass ``None``.
        init_request_metadata: The init request's Arrow ``custom_metadata``,
            passed only by the init turn.  Delivered to the producer's FIRST
            ``process()`` call as the tick's ``custom_metadata`` (HTTP folds the
            first producer turn into ``/init``, so this is the first tick), then
            reset to the empty tick.  ``None`` on continuation turns.
        owns_response_body: True when this turn's output IS the whole HTTP
            body, which is what makes it safe to compress into the IPC
            stream (see the codec note below).  False for the init turn,
            whose output is spliced after a plaintext header.
        call_token: The stream's sealed call token, passed only by the init
            turn — the one response that hands it to the client.  ``None``
            on continuation turns, which must not re-issue it.
        call_state_bytes: The plaintext call state, passed alongside
            ``call_token`` so the init turn's access-log record shows the
            state that was issued.  ``None`` on continuation turns.

    Returns:
        The IPC response body as a native ``pa.BufferReader``, positioned at the
        start.

        Native on purpose. ``ipc.new_stream`` over a Python file object wraps it
        in pyarrow's ``PythonFile`` adapter, so every buffer Arrow emits during
        ``write_batch`` calls back into Python and the C++ serializer cannot keep
        the GIL released — which made that one ``write_batch`` 45% of a worker's
        GIL-held time while being only 14% of its wall time. Writing into a
        ``pa.BufferOutputStream`` and handing back a zero-copy reader over the
        result keeps the whole serialize off the GIL, so concurrent stream turns
        actually overlap. Falcon only needs ``read()``/``seek()`` from
        ``resp.stream``, both of which ``BufferReader`` provides natively.

    """
    server_id = app._server.server_id
    protocol_name = app._server.protocol_name
    # Native sink — see the note in this function's docstring. `tell()` (used for
    # the max_bytes check below) is supported; `seek()` is not needed on a
    # write-only stream.
    resp_buf = pa.BufferOutputStream()
    # Compress INTO the IPC stream when a codec was negotiated, instead of
    # building the whole plaintext body and squeezing it afterwards. Arrow's
    # codec runs in C++ with the GIL released, and the uncompressed body never
    # exists as a whole: benchmarked at 81 MB of transient per 50k-row batch
    # versus 0, for the same output bytes and ~9% less CPU. The middleware skips
    # its own pass when `_current_body_precompressed` is set.
    # Only when this turn's output IS the whole response body. The init turn's
    # output is spliced into a response that already carries a plaintext header
    # (see the caller), so compressing here would yield a plaintext header
    # followed by a compressed body — not a decodable frame.
    codec = _current_response_codec.get() if owns_response_body else None
    write_sink: Any = resp_buf
    if codec is not None:
        try:
            write_sink = pa.CompressedOutputStream(resp_buf, codec)
            _current_body_precompressed.set(True)
        except Exception:
            write_sink = resp_buf
    max_bytes = app._max_response_bytes
    max_external_bytes = app._max_externalized_response_bytes
    externalization_enabled = (
        app._server.external_config is not None and app._server.external_config.storage is not None
    )
    with new_ipc_stream(write_sink, schema) as writer:
        if sink is not None:
            sink.flush_contents(writer, schema)
        cumulative_bytes = 0
        # Bytes uploaded to external storage across this HTTP turn.  Tracked
        # separately from the HTTP body cap (``max_response_bytes`` measures
        # ``resp_buf.tell()`` only — externalised payloads do not occupy the
        # wire body).  External payload size is governed by
        # ``max_externalized_response_bytes``.
        cumulative_external_bytes = 0
        # The CallContext is hoisted out of the loop — its non-collector
        # fields are constant for the whole turn.  ``emit_client_log`` is the
        # only thing that needs to follow the current iteration's
        # ``OutputCollector``; we route it through a mutable proxy.
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
            kind=app._server.transport_kind,
            implementation=app._server.implementation,
        )
        # The producer's FIRST tick carries the init request's metadata when this is
        # the init turn (HTTP folds the first producer turn into /init), so a producer
        # that reads first-tick metadata — e.g. the result-cache revalidation
        # validators vgi.cache.if_none_match — observes it exactly as on the pipe
        # transport. Later ticks (and continuation /exchange turns) use the empty
        # _TICK_BATCH.
        first_tick = (
            AnnotatedBatch(batch=_TICK_BATCH.batch, custom_metadata=init_request_metadata)
            if init_request_metadata is not None
            else _TICK_BATCH
        )
        try:
            while True:
                # Snapshot the budgets remaining at the start of this iteration.
                remaining_wire = None if max_bytes is None else max(0, max_bytes - resp_buf.tell())
                remaining_external = (
                    None
                    if max_external_bytes is None or not externalization_enabled
                    else max(0, max_external_bytes - cumulative_external_bytes)
                )
                out = OutputCollector(
                    schema,
                    prior_data_bytes=cumulative_bytes,
                    server_id=server_id,
                    producer_mode=True,
                    remaining_response_bytes=remaining_wire,
                    remaining_externalized_response_bytes=remaining_external,
                    externalization_enabled=externalization_enabled,
                )
                current_out[0] = out
                state.process(first_tick, out, produce_ctx)
                first_tick = _TICK_BATCH  # only the first process() sees init metadata
                if not out.finished:
                    out.validate()
                # Pre-flight the external cap BEFORE flushing — predicting the
                # upload size from the data batch's buffer size lets us refuse
                # a violating upload without paying the storage round-trip.
                if max_external_bytes is not None and externalization_enabled and not out.finished:
                    ext_cfg = app._server.external_config
                    assert ext_cfg is not None  # narrowed by externalization_enabled
                    predicted = predict_externalize_bytes_for_collector(out, ext_cfg)
                    if predicted and cumulative_external_bytes + predicted > max_external_bytes:
                        projected = cumulative_external_bytes + predicted
                        overshoot = RuntimeError(
                            f"Externalised payload exceeds max_externalized_response_bytes "
                            f"({projected} > {max_external_bytes}) for method {method_name!r}"
                        )
                        outcome.status = "error"
                        outcome.error_type = _log_method_error(protocol_name, method_name, server_id, overshoot)
                        outcome.error_message = _truncate_error_message(overshoot)
                        # Hard cap (external channel has no escape valve) — signal
                        # the resource layer to flip 200 → 200 + X-VGI-RPC-Error: true.
                        _current_response_status.set(HTTPStatus.INTERNAL_SERVER_ERROR)
                        _write_error_batch(writer, schema, overshoot, server_id=server_id)
                        break
                cumulative_external_bytes += _flush_collector(writer, out, app._server.external_config)
                if out.finished:
                    break
                cumulative_bytes = out.total_data_bytes
                # Decide whether to continue producing or break with a
                # continuation token.  By default (no limit configured),
                # break after every produce cycle so the client receives
                # data incrementally.  When ``max_bytes`` is configured,
                # buffer multiple batches until the HTTP body fills the cap.
                should_continue = max_bytes is not None and resp_buf.tell() < max_bytes
                if not should_continue:
                    # Serialize the cursor into a continuation token.  Only the
                    # cursor: the call token was minted at /init and either the
                    # client already holds it (continuation turns) or it rides
                    # this same sentinel batch (the init turn, below).
                    state_info = app._state_types.get(method_name)
                    if state_info is None:
                        raise _RpcHttpError(
                            RuntimeError(f"Cannot resolve state type for method '{method_name}'"),
                            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                        )
                    token, state_bytes = _mint_cursor_token(
                        state,
                        state_info,
                        call_id,
                        app._token_key,
                        auth,
                    )
                    outcome.response_state_bytes = state_bytes
                    token_md: dict[bytes, bytes] = {STATE_KEY: token}
                    if call_token is not None:
                        # Init turn — hand the client its one and only copy of
                        # the call token, alongside the first cursor.
                        token_md[CALL_STATE_KEY] = call_token
                        if call_state_bytes:
                            outcome.response_state_bytes = call_state_bytes + state_bytes
                    state_metadata = pa.KeyValueMetadata(token_md)
                    continuation_batch = empty_batch(schema)
                    _record_output(continuation_batch)
                    writer.write_batch(continuation_batch, custom_metadata=state_metadata)
                    break
        except Exception as exc:
            outcome.status = "error"
            outcome.error_type = _log_method_error(protocol_name, method_name, server_id, exc)
            outcome.error_message = _truncate_error_message(exc)
            _write_error_batch(writer, schema, exc, server_id=server_id)
    # Close the codec BEFORE getvalue(): the compressed frame is only complete
    # once the stream is finalised.
    if write_sink is not resp_buf:
        write_sink.close()
    # Zero-copy: BufferReader wraps the accumulated buffer, it does not copy it.
    return pa.BufferReader(resp_buf.getvalue())


def _unpack_and_recover_state(
    app: _HttpRpcApp,
    token: bytes,
    call_token: bytes | None,
    state_info: _StateInfo,
    auth: AuthContext | None,
) -> tuple[StreamState, _ResolvedCall, bytes, bytes]:
    """Open a cursor token, resolve its call, and rebuild the state object.

    Order matters here, and it is the whole security argument for the
    cache.  The cursor token is opened *first*; its AEAD tag covers the
    ``call_id`` and its AAD covers the caller's identity.  Only then is
    that authenticated ``call_id`` used as a cache key.  A client cannot
    name a call id the server did not mint for it, so a cache hit can never
    hand back another principal's call state — and on a hit the presented
    call token is not consulted at all, which is exactly the work we are
    trying to avoid.

    On a miss (cold process, evicted entry, or a request load-balanced to a
    node that never saw this stream's ``/init``) the client-supplied call
    token is opened and verified, and its embedded ``call_id`` must match
    the one the cursor named.

    Args:
        app: The HTTP app providing the AEAD key, TTL, cache, and server
            implementation.
        token: The sealed cursor token bytes.
        call_token: The sealed call token echoed by the client.  May be
            ``None`` when the cache is expected to hit; a miss then fails.
        state_info: A single concrete state class, or an ordered tuple of
            concrete classes for union types.  When a tuple is provided, the
            concrete class is resolved from the numeric tag embedded in
            ``state_bytes``.
        auth: Authenticated identity for the current request.

    Returns:
        ``(state_object, resolved_call, call_id, state_bytes)``.
        ``state_bytes`` is the decrypted plaintext cursor payload —
        returned so callers can surface it to the access log rather than
        the on-the-wire ciphertext token.  The contextvar
        ``_current_stream_id`` is set as a convenience for ambient
        telemetry observers (logger formatters, OTel hooks).

    Raises:
        _RpcHttpError: On malformed tokens, expired tokens, a cursor whose
            call cannot be resolved, mismatched call ids, failed
            deserialization, or AEAD authenticity failure (which covers
            both tampering and cross-principal replay).

    """
    state_bytes, call_id = _open_cursor_token(token, app._token_key, _compute_aad(auth), app._token_ttl)

    now = time.time()
    resolved = app._call_state_cache.get(call_id, auth, now)
    if resolved is None:
        resolved = _resolve_call_from_token(app, call_token, call_id, state_info, auth)
        app._call_state_cache.put(call_id, auth, resolved, now)

    if resolved.stream_id:
        _current_stream_id.set(resolved.stream_id)

    try:
        state_cls, raw_state_bytes = _resolve_state_cls(state_bytes, state_info)
        state_obj = state_cls.deserialize_from_bytes(raw_state_bytes, app._server.ipc_validation)
        state_obj.bind_call_state(resolved.call_state)
        state_obj.rehydrate(app._server.implementation)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError(f"Failed to deserialize state: {exc}"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    return state_obj, resolved, call_id, state_bytes


def _declared_call_state_types(state_info: _StateInfo) -> dict[str, type[ArrowSerializableDataclass]]:
    """Map class name → call-state class, over everything a method may declare.

    A union stream method can mix members that carry call state with members
    that do not — VGI's ``init`` returns exactly such a union, since a table
    scan carries an init request while a buffered-finalize stream carries
    only a cursor.  So the reader resolves the token's type name against
    *this* map and nothing else: an unrecognised name is rejected rather
    than looked up, which keeps a client-supplied string from selecting a
    class.
    """
    members = state_info if isinstance(state_info, tuple) else (state_info,)
    return {m.CALL_STATE_TYPE.__name__: m.CALL_STATE_TYPE for m in members if m.CALL_STATE_TYPE is not None}


def _resolve_call_from_token(
    app: _HttpRpcApp,
    call_token: bytes | None,
    expected_call_id: bytes,
    state_info: _StateInfo,
    auth: AuthContext | None,
) -> _ResolvedCall:
    """Open a client-supplied call token — the cache-miss path.

    Args:
        app: The HTTP app providing the AEAD key, TTL, and state types.
        call_token: The sealed call token from ``CALL_STATE_KEY``.
        expected_call_id: The call id the cursor token named.
        state_info: The method's state class (or union tuple), which
            declares the call-state type to deserialize into.
        auth: Authenticated identity for the current request.

    Returns:
        The parsed :class:`_ResolvedCall`.

    Raises:
        _RpcHttpError: If the token is absent, fails to open, names a
            different call, or carries schemas/call state that will not
            deserialize.

    """
    if call_token is None:
        raise _RpcHttpError(
            RuntimeError("Missing call token in exchange request"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    (
        call_state_bytes,
        call_state_type,
        schema_bytes,
        input_schema_bytes,
        token_call_id,
        stream_id,
    ) = _open_call_token(call_token, app._token_key, _compute_call_aad(auth), app._token_ttl)
    # Constant-time compare: the ids are both server-minted and already
    # authenticated, so this is belt-and-braces against a client pairing two
    # of its own tokens from different streams.
    if not secrets.compare_digest(token_call_id, expected_call_id):
        raise _RpcHttpError(
            RuntimeError("State token does not belong to the supplied call token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

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

    call_state: Any = None
    if call_state_bytes:
        call_state_cls = _declared_call_state_types(state_info).get(call_state_type)
        if call_state_cls is None:
            raise _RpcHttpError(
                RuntimeError(f"Call token declares call-state type {call_state_type!r}, which this method does not"),
                status_code=HTTPStatus.BAD_REQUEST,
            )
        try:
            call_state = call_state_cls.deserialize_from_bytes(call_state_bytes, app._server.ipc_validation)
        except Exception as exc:
            raise _RpcHttpError(
                RuntimeError(f"Failed to deserialize call state: {exc}"),
                status_code=HTTPStatus.BAD_REQUEST,
            ) from exc

    return _ResolvedCall(call_state, output_schema, input_schema, stream_id)
