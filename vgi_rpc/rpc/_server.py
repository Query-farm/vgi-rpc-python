"""RPC server dispatch."""

from __future__ import annotations

import contextlib
import inspect
import logging
import time
import uuid
from collections.abc import Mapping
from typing import Any, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import ExternalLocationConfig, resolve_external_location
from vgi_rpc.metadata import SHM_SEGMENT_NAME_KEY, SHM_SEGMENT_SIZE_KEY
from vgi_rpc.rpc._common import (
    _EMPTY_SCHEMA,
    AuthContext,
    CallContext,
    CallStatistics,
    HookToken,
    MethodType,
    RpcError,
    VersionError,
    _access_logger,
    _current_call_stats,
    _current_request_id,
    _current_request_metadata,
    _DispatchHook,
    _generate_request_id,
    _get_auth_and_metadata,
    _logger,
    _record_input,
    _record_output,
)
from vgi_rpc.rpc._transport import RpcTransport, ShmPipeTransport
from vgi_rpc.rpc._types import (
    AnnotatedBatch,
    OutputCollector,
    RpcMethodInfo,
    Stream,
    StreamState,
    _validate_implementation,
    rpc_methods,
)
from vgi_rpc.rpc._wire import (
    _ClientLogSink,
    _deserialize_params,
    _drain_stream,
    _flush_collector,
    _read_request,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_error_stream,
    _write_result_batch,
    _write_stream_header,
)
from vgi_rpc.shm import ShmSegment, resolve_shm_batch
from vgi_rpc.utils import IpcValidation, ValidatedReader

# ---------------------------------------------------------------------------
# Server helpers
# ---------------------------------------------------------------------------


def _log_method_error(protocol_name: str, method_name: str, server_id: str, exc: BaseException) -> str:
    """Log an RPC method error and return the exception class name.

    Returns:
        The exception class name (for use as ``error_type``).

    """
    error_type = type(exc).__name__
    extra: dict[str, object] = {"server_id": server_id, "method": method_name, "error_type": error_type}
    request_id = _current_request_id.get()
    if request_id:
        extra["request_id"] = request_id
    _logger.error(
        "Error in %s.%s: %s",
        protocol_name,
        method_name,
        exc,
        exc_info=True,
        extra=extra,
    )
    return error_type


def _emit_access_log(
    protocol_name: str,
    method_name: str,
    method_type: str,
    server_id: str,
    auth: AuthContext,
    transport_metadata: Mapping[str, Any],
    duration_ms: float,
    status: Literal["ok", "error"],
    error_type: str = "",
    http_status: int | None = None,
    stats: CallStatistics | None = None,
) -> None:
    """Emit a structured access log record for a completed RPC call."""
    if not _access_logger.isEnabledFor(logging.INFO):
        return
    try:
        extra: dict[str, object] = {
            "server_id": server_id,
            "protocol": protocol_name,
            "method": method_name,
            "method_type": method_type,
            "principal": auth.principal or "",
            "auth_domain": auth.domain or "",
            "remote_addr": transport_metadata.get("remote_addr", ""),
            "duration_ms": round(duration_ms, 2),
            "status": status,
            "error_type": error_type,
        }
        request_id = _current_request_id.get()
        if request_id:
            extra["request_id"] = request_id
        if http_status is not None:
            extra["http_status"] = http_status
        if stats is not None:
            extra["input_batches"] = stats.input_batches
            extra["output_batches"] = stats.output_batches
            extra["input_rows"] = stats.input_rows
            extra["output_rows"] = stats.output_rows
            extra["input_bytes"] = stats.input_bytes
            extra["output_bytes"] = stats.output_bytes
        _access_logger.info(
            "%s.%s %s",
            protocol_name,
            method_name,
            status,
            extra=extra,
        )
    except Exception:
        _logger.debug("Access log emission failed", exc_info=True)


def _maybe_attach_shm(req_md: pa.KeyValueMetadata | None) -> ShmSegment | None:
    """Attach to a client-owned SHM segment advertised in request metadata.

    Returns ``None`` if the metadata doesn't contain SHM segment keys or
    if the values are malformed.  The caller owns unlinking; the returned
    segment uses ``track=False`` so the resource tracker won't interfere.
    """
    if req_md is None:
        return None
    shm_name_bytes = req_md.get(SHM_SEGMENT_NAME_KEY)
    if shm_name_bytes is None:
        return None
    shm_size_bytes = req_md.get(SHM_SEGMENT_SIZE_KEY)
    if shm_size_bytes is None:
        return None
    try:
        shm_name = shm_name_bytes if isinstance(shm_name_bytes, str) else shm_name_bytes.decode()
        shm_size = int(shm_size_bytes)
    except (ValueError, UnicodeDecodeError):
        _logger.warning("Ignoring malformed SHM metadata: name=%r, size=%r", shm_name_bytes, shm_size_bytes)
        return None
    return ShmSegment.attach(shm_name, shm_size, track=False)


# ---------------------------------------------------------------------------
# RpcServer
# ---------------------------------------------------------------------------


class RpcServer:
    """Dispatches RPC requests to an implementation over IO-stream transports."""

    __slots__ = (
        "_ctx_methods",
        "_describe_batch",
        "_describe_metadata",
        "_dispatch_hook",
        "_external_config",
        "_impl",
        "_ipc_validation",
        "_methods",
        "_protocol",
        "_server_id",
    )

    def __init__(
        self,
        protocol: type,
        implementation: object,
        *,
        external_location: ExternalLocationConfig | None = None,
        server_id: str | None = None,
        enable_describe: bool = False,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> None:
        """Initialize with a protocol type and its implementation.

        Args:
            protocol: The Protocol class defining the RPC interface.
            implementation: Object implementing all methods from *protocol*.
            external_location: Optional ExternalLocation configuration.
            server_id: Optional server identifier; auto-generated if ``None``.
            enable_describe: When ``True``, the server handles ``__describe__``
                requests returning machine-readable method metadata.
            ipc_validation: Validation level for incoming IPC batches.
                Defaults to ``FULL`` for maximum safety.

        """
        self._protocol = protocol
        self._impl = implementation
        self._ipc_validation = ipc_validation
        self._methods = rpc_methods(protocol)
        self._external_config = external_location
        self._server_id = server_id if server_id is not None else uuid.uuid4().hex[:12]
        self._dispatch_hook: _DispatchHook | None = None
        _validate_implementation(protocol, implementation, self._methods)

        if enable_describe:
            from vgi_rpc.introspect import DESCRIBE_METHOD_NAME, build_describe_batch

            self._describe_batch: pa.RecordBatch | None
            self._describe_metadata: pa.KeyValueMetadata | None
            self._describe_batch, self._describe_metadata = build_describe_batch(
                protocol.__name__, self._methods, self._server_id
            )
            # Register __describe__ as a synthetic unary method so normal dispatch handles it.
            self._methods = {
                **self._methods,
                DESCRIBE_METHOD_NAME: RpcMethodInfo(
                    name=DESCRIBE_METHOD_NAME,
                    params_schema=_EMPTY_SCHEMA,
                    result_schema=self._describe_batch.schema,
                    result_type=type(None),
                    method_type=MethodType.UNARY,
                    has_return=True,
                    doc="Return machine-readable metadata about all server methods.",
                ),
            }
        else:
            self._describe_batch = None
            self._describe_metadata = None

        # Detect which impl methods accept a `ctx` parameter.
        self._ctx_methods: frozenset[str] = frozenset(
            name
            for name in self._methods
            if (method := getattr(implementation, name, None)) is not None
            and "ctx" in inspect.signature(method).parameters
        )

        _logger.info(
            "RpcServer created for %s (server_id=%s, methods=%d)",
            protocol.__name__,
            self._server_id,
            len(self._methods),
            extra={"server_id": self._server_id, "protocol": protocol.__name__, "method_count": len(self._methods)},
        )

    @property
    def methods(self) -> Mapping[str, RpcMethodInfo]:
        """Return method metadata for this server's protocol."""
        return self._methods

    @property
    def implementation(self) -> object:
        """The implementation object."""
        return self._impl

    @property
    def external_config(self) -> ExternalLocationConfig | None:
        """The ExternalLocation configuration, if any."""
        return self._external_config

    @property
    def server_id(self) -> str:
        """Short random identifier for this server instance."""
        return self._server_id

    @property
    def protocol_name(self) -> str:
        """Name of the Protocol class this server implements."""
        return self._protocol.__name__

    @property
    def ctx_methods(self) -> frozenset[str]:
        """Method names whose implementations accept a ctx parameter."""
        return self._ctx_methods

    @property
    def describe_enabled(self) -> bool:
        """Whether ``__describe__`` introspection is enabled."""
        return self._describe_batch is not None

    @property
    def ipc_validation(self) -> IpcValidation:
        """Validation level for incoming IPC batches."""
        return self._ipc_validation

    def serve(self, transport: RpcTransport) -> None:
        """Serve RPC requests in a loop until the transport is closed."""
        while True:
            try:
                self.serve_one(transport)
            except (EOFError, StopIteration):
                break
            except pa.ArrowInvalid:
                _logger.warning(
                    "serve loop ending due to ArrowInvalid",
                    exc_info=True,
                    extra={"server_id": self._server_id},
                )
                break

    def serve_one(self, transport: RpcTransport) -> None:
        """Handle a single RPC call (any method type) over the given transport.

        Protocol-level errors (``VersionError``, ``RpcError`` from missing
        metadata) are caught, written back as error responses, and the
        method returns normally so the serve loop can continue.

        Raises:
            pa.ArrowInvalid: If the incoming data is not valid Arrow IPC.
                An error response is written to *transport* before raising so
                the client can read a structured ``RpcError``.

        """
        token = _current_request_id.set(_generate_request_id())
        stats = CallStatistics()
        stats_token = _current_call_stats.set(stats)
        md_token = _current_request_metadata.set(None)
        dynamic_shm: ShmSegment | None = None
        try:
            try:
                method_name, kwargs = _read_request(transport.reader, self._ipc_validation)
            except pa.ArrowInvalid as exc:
                with contextlib.suppress(BrokenPipeError, OSError):
                    _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc, server_id=self._server_id)
                raise
            except (VersionError, RpcError) as exc:
                with contextlib.suppress(BrokenPipeError, OSError):
                    _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc, server_id=self._server_id)
                return

            info = self._methods.get(method_name)
            if info is None:
                available = sorted(self._methods.keys())
                _write_error_stream(
                    transport.writer,
                    _EMPTY_SCHEMA,
                    AttributeError(f"Unknown method: '{method_name}'. Available methods: {available}"),
                    server_id=self._server_id,
                )
                return

            _deserialize_params(kwargs, info.param_types, self._ipc_validation)

            try:
                _validate_params(info.name, kwargs, info.param_types)
            except TypeError as exc:
                err_schema = info.result_schema if info.method_type == MethodType.UNARY else _EMPTY_SCHEMA
                _write_error_stream(transport.writer, err_schema, exc, server_id=self._server_id)
                return

            # Determine SHM segment: prefer transport-level, fall back to request metadata
            shm = transport.shm if isinstance(transport, ShmPipeTransport) else None
            if shm is None:
                dynamic_shm = _maybe_attach_shm(_current_request_metadata.get())
                shm = dynamic_shm

            if info.method_type == MethodType.UNARY:
                self._serve_unary(transport, info, kwargs, stats=stats, shm=shm)
            elif info.method_type == MethodType.STREAM:
                self._serve_stream(transport, info, kwargs, stats=stats, shm=shm)
        finally:
            if dynamic_shm is not None:
                with contextlib.suppress(BufferError):
                    dynamic_shm.close()
            _current_request_metadata.reset(md_token)
            _current_call_stats.reset(stats_token)
            _current_request_id.reset(token)

    def _prepare_method_call(
        self, info: RpcMethodInfo, kwargs: dict[str, Any]
    ) -> tuple[_ClientLogSink, AuthContext, Mapping[str, Any]]:
        """Create a log sink + read auth; wire a :class:`CallContext` into *kwargs* if the method accepts ``ctx``."""
        sink = _ClientLogSink(server_id=self._server_id)
        auth, transport_metadata = _get_auth_and_metadata()
        if info.name in self._ctx_methods:
            kwargs["ctx"] = CallContext(
                auth=auth,
                emit_client_log=sink,
                transport_metadata=transport_metadata,
                server_id=self._server_id,
                method_name=info.name,
                protocol_name=self.protocol_name,
            )
        return sink, auth, transport_metadata

    def _serve_unary(
        self,
        transport: RpcTransport,
        info: RpcMethodInfo,
        kwargs: dict[str, Any],
        *,
        stats: CallStatistics | None = None,
        shm: ShmSegment | None = None,
    ) -> None:
        # Pre-built __describe__ batch — write directly, skip implementation call.
        if self._describe_batch is not None and info.name == "__describe__":
            _record_output(self._describe_batch)
            with ipc.new_stream(transport.writer, self._describe_batch.schema) as writer:
                writer.write_batch(self._describe_batch, custom_metadata=self._describe_metadata)
            auth, transport_md = _get_auth_and_metadata()
            _emit_access_log(
                self.protocol_name,
                info.name,
                info.method_type.value,
                self._server_id,
                auth,
                transport_md,
                0.0,
                "ok",
                stats=stats,
            )
            return

        schema = info.result_schema
        sink, auth, transport_md = self._prepare_method_call(info, kwargs)
        protocol_name = self.protocol_name
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        hook = self._dispatch_hook
        hook_token: HookToken = None
        if hook is not None:
            try:
                hook_token = hook.on_dispatch_start(info, auth, transport_md)
            except Exception:
                _logger.debug("Dispatch hook start failed", exc_info=True)
                hook = None
        _hook_exc: BaseException | None = None
        try:
            with ipc.new_stream(transport.writer, schema) as writer:
                sink.flush_contents(writer, schema)
                try:
                    result = getattr(self._impl, info.name)(**kwargs)
                    _validate_result(info.name, result, info.result_type)
                except Exception as exc:
                    _hook_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, info.name, self._server_id, exc)
                    _write_error_batch(writer, schema, exc, server_id=self._server_id)
                    return
                _write_result_batch(writer, info.result_schema, result, self._external_config, shm=shm)
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                info.name,
                info.method_type.value,
                self._server_id,
                auth,
                transport_md,
                duration_ms,
                status,
                error_type,
                stats=stats,
            )
            if hook is not None:
                try:
                    hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                except Exception:
                    _logger.debug("Dispatch hook end failed", exc_info=True)

    def _serve_stream(
        self,
        transport: RpcTransport,
        info: RpcMethodInfo,
        kwargs: dict[str, Any],
        *,
        stats: CallStatistics | None = None,
        shm: ShmSegment | None = None,
    ) -> None:
        sink, auth, transport_md = self._prepare_method_call(info, kwargs)
        protocol_name = self.protocol_name
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        hook = self._dispatch_hook
        hook_token: HookToken = None
        if hook is not None:
            try:
                hook_token = hook.on_dispatch_start(info, auth, transport_md)
            except Exception:
                _logger.debug("Dispatch hook start failed", exc_info=True)
                hook = None
        _hook_exc: BaseException | None = None

        # NOTE: Two finally blocks — the inner one handles init errors (with early return),
        # the outer one handles streaming errors.  Only one access log fires per call.
        try:
            result: Stream[StreamState, Any] = getattr(self._impl, info.name)(**kwargs)
        except Exception as exc:
            _hook_exc = exc
            status = "error"
            error_type = _log_method_error(protocol_name, info.name, self._server_id, exc)
            with contextlib.suppress(BrokenPipeError, OSError):
                _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc, server_id=self._server_id)
            return
        finally:
            if status == "error":
                duration_ms = (time.monotonic() - start) * 1000
                _emit_access_log(
                    protocol_name,
                    info.name,
                    info.method_type.value,
                    self._server_id,
                    auth,
                    transport_md,
                    duration_ms,
                    status,
                    error_type,
                    stats=stats,
                )
                if hook is not None:
                    try:
                        hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                    except Exception:
                        _logger.debug("Dispatch hook end failed", exc_info=True)

        output_schema = result.output_schema
        input_schema = result.input_schema
        state = result.state

        # Write header IPC stream before the main output stream
        if info.header_type is not None:
            _write_stream_header(
                transport.writer, result.header, self._external_config, sink=sink, method_name=info.name
            )

        input_reader = ValidatedReader(ipc.open_stream(transport.reader), self._ipc_validation)

        prev_input: AnnotatedBatch | None = None
        try:
            with ipc.new_stream(transport.writer, output_schema) as output_writer:
                sink.flush_contents(output_writer, output_schema)
                cumulative_bytes = 0
                try:
                    while True:
                        try:
                            input_batch, custom_metadata = input_reader.read_next_batch_with_custom_metadata()
                        except StopIteration:
                            break

                        # Record input batch for stats (including producer ticks)
                        _record_input(input_batch)

                        # Resolve ExternalLocation on input batch
                        input_batch, resolved_cm = resolve_external_location(
                            input_batch,
                            custom_metadata,
                            self._external_config,
                            ipc_validation=self._ipc_validation,
                        )

                        # Resolve SHM pointer on input batch
                        input_batch, resolved_cm, release_fn = resolve_shm_batch(input_batch, resolved_cm, shm)

                        if input_batch.schema != input_schema:
                            raise TypeError(f"Input schema mismatch: expected {input_schema}, got {input_batch.schema}")

                        ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=resolved_cm, _release_fn=release_fn)
                        if prev_input is not None:
                            prev_input.release()
                        prev_input = ab_in
                        is_producer = input_schema == _EMPTY_SCHEMA
                        out = OutputCollector(
                            output_schema,
                            prior_data_bytes=cumulative_bytes,
                            server_id=self._server_id,
                            producer_mode=is_producer,
                        )
                        process_ctx = CallContext(
                            auth=auth,
                            emit_client_log=out.emit_client_log_message,
                            transport_metadata=transport_md,
                            server_id=self._server_id,
                            method_name=info.name,
                            protocol_name=protocol_name,
                        )
                        state.process(ab_in, out, process_ctx)
                        if not out.finished:
                            out.validate()
                        _flush_collector(output_writer, out, self._external_config, shm=shm)
                        if out.finished:
                            break
                        cumulative_bytes = out.total_data_bytes
                except Exception as exc:
                    _hook_exc = exc
                    status = "error"
                    error_type = _log_method_error(protocol_name, info.name, self._server_id, exc)
                    with contextlib.suppress(BrokenPipeError, OSError):
                        _write_error_batch(output_writer, output_schema, exc, server_id=self._server_id)
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            _emit_access_log(
                protocol_name,
                info.name,
                info.method_type.value,
                self._server_id,
                auth,
                transport_md,
                duration_ms,
                status,
                error_type,
                stats=stats,
            )
            if hook is not None:
                try:
                    hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                except Exception:
                    _logger.debug("Dispatch hook end failed", exc_info=True)

        # Drain remaining input so transport is clean for next request
        with contextlib.suppress(pa.ArrowInvalid, OSError):
            _drain_stream(input_reader)
