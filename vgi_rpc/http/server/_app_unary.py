# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Unary-call dispatch path for the HTTP server."""

from __future__ import annotations

import logging
import time
from http import HTTPStatus
from io import BytesIO, IOBase
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.rpc import (
    CallContext,
    RpcError,
    RpcMethodInfo,
    VersionError,
    _ClientLogSink,
    _deserialize_params,
    _emit_access_log,
    _get_auth_and_metadata,
    _log_method_error,
    _read_request,
    _truncate_error_message,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_result_batch,
)
from vgi_rpc.rpc._common import (
    CallStatistics,
    HookToken,
    _current_call_stats,
    _DispatchHook,
    _record_output,
)

from .._common import _RpcHttpError

if TYPE_CHECKING:
    from ._app import _HttpRpcApp

_logger = logging.getLogger("vgi_rpc.http")


def _run_unary_sync(
    app: _HttpRpcApp,
    method_name: str,
    info: RpcMethodInfo,
    stream: IOBase,
) -> tuple[BytesIO, HTTPStatus]:
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

        # Pre-built __describe__ batch — write directly, skip implementation call.
        describe_batch = app._server._describe_batch
        if describe_batch is not None and method_name == "__describe__":
            _record_output(describe_batch)
            resp_buf = BytesIO()
            with ipc.new_stream(resp_buf, describe_batch.schema) as writer:
                writer.write_batch(describe_batch, custom_metadata=app._server._describe_metadata)
            resp_buf.seek(0)
            auth, transport_metadata = _get_auth_and_metadata()
            _emit_access_log(
                app._server.protocol_name,
                method_name,
                info.method_type.value,
                app._server.server_id,
                auth,
                transport_metadata,
                0.0,
                "ok",
                http_status=HTTPStatus.OK.value,
                stats=stats,
                server_version=app._server.server_version,
                protocol_hash=app._server.protocol_hash,
                protocol_version=app._server.protocol_version,
            )
            return resp_buf, HTTPStatus.OK

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

        schema = info.result_schema
        resp_buf = BytesIO()
        http_status = HTTPStatus.OK
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        hook: _DispatchHook | None = app._server._dispatch_hook
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
                    result = getattr(app._server.implementation, method_name)(**kwargs)
                    _validate_result(info.name, result, info.result_type)
                    _write_result_batch(writer, schema, result, app._server.external_config)
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
                server_version=app._server.server_version,
                protocol_hash=app._server.protocol_hash,
                error_message=_truncate_error_message(_hook_exc),
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
