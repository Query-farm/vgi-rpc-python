# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""RPC server dispatch."""

from __future__ import annotations

import base64
import contextlib
import inspect
import logging
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from typing import Any, Literal

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import ExternalLocationConfig, resolve_external_location
from vgi_rpc.metadata import CANCEL_KEY, SHM_SEGMENT_NAME_KEY, SHM_SEGMENT_SIZE_KEY
from vgi_rpc.rpc._common import (
    _EMPTY_SCHEMA,
    AuthContext,
    CallContext,
    CallStatistics,
    HookToken,
    MethodType,
    RpcError,
    TransportKind,
    VersionError,
    _access_logger,
    _current_call_stats,
    _current_request_batch,
    _current_request_id,
    _current_request_metadata,
    _current_stream_id,
    _DispatchHook,
    _generate_request_id,
    _get_auth_and_metadata,
    _logger,
    _record_input,
    _record_output,
)
from vgi_rpc.rpc._debug import wire_stream_logger
from vgi_rpc.rpc._transport import PipeTransport, RpcTransport, ShmPipeTransport, UnixTransport
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
    _coerce_input_batch,
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


_ACCESS_LOG_ERROR_MESSAGE_LIMIT = 500
"""Cap for ``error_message`` fields surfaced via the access log.

Long exception messages (typically with embedded tracebacks or repeated
context) bloat each JSONL record without adding signal — the full traceback
is logged separately by ``_log_method_error``.  The cap matches the
historical inline truncation used at every dispatch site.
"""


def _truncate_error_message(exc: BaseException | None, limit: int = _ACCESS_LOG_ERROR_MESSAGE_LIMIT) -> str:
    """Render an exception's message for the access-log ``error_message`` field.

    Returns ``""`` for ``None`` (the no-error case).  Otherwise returns
    ``str(exc)`` truncated to ``limit`` characters.  Centralises the
    historically duplicated ``str(exc)[:500]`` pattern across the unary
    and stream dispatch shells so the truncation policy is one knob.
    """
    if exc is None:
        return ""
    return str(exc)[:limit]


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
    error_message: str = "",
    http_status: int | None = None,
    stats: CallStatistics | None = None,
    server_version: str = "",
    protocol_hash: str = "",
    protocol_version: str = "",
    request_state: bytes | None = None,
    response_state: bytes | None = None,
    cancelled: bool = False,
) -> None:
    """Emit a structured access log record for a completed RPC call."""
    if not _access_logger.isEnabledFor(logging.INFO):
        return
    try:
        extra: dict[str, object] = {
            "server_id": server_id,
            "protocol": protocol_name,
            "protocol_hash": protocol_hash,
            "method": method_name,
            "method_type": method_type,
            "principal": auth.principal or "",
            "auth_domain": auth.domain or "",
            "authenticated": auth.authenticated,
            "remote_addr": transport_metadata.get("remote_addr", ""),
            "duration_ms": round(duration_ms, 2),
            "status": status,
            "error_type": error_type,
        }
        if protocol_version:
            extra["protocol_version"] = protocol_version
        if cancelled:
            extra["cancelled"] = True
        if error_message:
            extra["error_message"] = error_message
        if server_version:
            extra["server_version"] = server_version
        request_id = _current_request_id.get()
        if request_id:
            extra["request_id"] = request_id
        if http_status is not None:
            extra["http_status"] = http_status
        # Raw request batch bytes (set by _read_request for unary/stream-init).
        # Only emit the full base64 payload when the access logger is at
        # DEBUG. At INFO this field is by far the heaviest in the record
        # (an init RPC commonly logs 8+ KiB of base64 per call), and audit
        # consumers rarely need the bytes — they care about who/what/when.
        # When omitted, mark the record `truncated: true` and surface the
        # original size so the access-log schema's "unary requires
        # request_data unless truncated" invariant holds.
        request_data = _current_request_batch.get()
        if request_data is not None:
            encoded = base64.b64encode(request_data).decode()
            if _access_logger.isEnabledFor(logging.DEBUG):
                extra["request_data"] = encoded
            else:
                extra["original_request_bytes"] = len(encoded)
                extra["truncated"] = True
        # Stream correlation ID
        stream_id = _current_stream_id.get()
        if stream_id:
            extra["stream_id"] = stream_id
        # Auth claims
        if auth.claims:
            extra["claims"] = dict(auth.claims)
        # State tokens (HTTP transport only). Same DEBUG-gating as
        # request_data above: these are base64'd opaque blobs encoding the
        # worker's serialized cross-POST context — typically 8-12 KiB per
        # record on streaming methods. Useful for replay/audit at DEBUG;
        # at INFO they dominate the log volume without giving operators
        # anything they can read. Schema makes both fields optional, so we
        # can simply omit (no truncated marker needed).
        if _access_logger.isEnabledFor(logging.DEBUG):
            if request_state is not None:
                extra["request_state"] = base64.b64encode(request_state).decode()
            if response_state is not None:
                extra["response_state"] = base64.b64encode(response_state).decode()
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


def _maybe_attach_shm(
    req_md: pa.KeyValueMetadata | None,
    transport_kind: TransportKind | None,
) -> ShmSegment | None:
    """Attach to a client-owned SHM segment advertised in request metadata.

    Only honoured for local pipe / Unix-socket transports where the
    client and server share a kernel and the client legitimately owns
    the named segment.  HTTP transport is explicitly rejected: a remote
    client could otherwise supply an arbitrary POSIX shm name and have
    the server attach to it, leading to information disclosure or
    crashes on multi-tenant hosts.  HTTP dispatch does not currently
    flow through ``serve_one``, so this guard is defence-in-depth
    against future refactors that might wire it differently.

    Returns ``None`` if the transport is HTTP, the metadata doesn't
    contain SHM segment keys, or the values are malformed.  The caller
    owns unlinking; the returned segment uses ``track=False`` so the
    resource tracker won't interfere.
    """
    if transport_kind == TransportKind.HTTP:
        if req_md is not None and req_md.get(SHM_SEGMENT_NAME_KEY) is not None:
            _logger.warning(
                "Refusing dynamic SHM attach over HTTP transport (client supplied %r)",
                req_md.get(SHM_SEGMENT_NAME_KEY),
            )
        return None
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
        "_protocol_hash",
        "_protocol_version",
        "_server_id",
        "_server_version",
        "_transport_capabilities",
        "_transport_kind",
        "_transport_lock",
    )

    def __init__(
        self,
        protocol: type,
        implementation: object,
        *,
        external_location: ExternalLocationConfig | None = None,
        server_id: str | None = None,
        server_version: str = "",
        protocol_version: str = "",
        enable_describe: bool = False,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> None:
        """Initialize with a protocol type and its implementation.

        Args:
            protocol: The Protocol class defining the RPC interface.
            implementation: Object implementing all methods from *protocol*.
            external_location: Optional ExternalLocation configuration.
            server_id: Optional server identifier; auto-generated if ``None``.
            server_version: Build version string included in access log entries
                (separate from *protocol_version*).
            protocol_version: Operator-supplied free-form protocol contract version
                included in access log entries; complements ``protocol_hash`` for
                humans.
            enable_describe: When ``True``, the server handles ``__describe__``
                requests returning machine-readable method metadata.
            ipc_validation: Validation level for incoming IPC batches.
                Defaults to ``FULL`` for maximum safety.

        """
        self._protocol = protocol
        self._impl = implementation
        self._server_version = server_version
        self._protocol_version = protocol_version
        self._ipc_validation = ipc_validation
        self._methods = rpc_methods(protocol)
        self._external_config = external_location
        self._server_id = server_id if server_id is not None else uuid.uuid4().hex[:12]
        self._dispatch_hook: _DispatchHook | None = None
        self._transport_kind: TransportKind | None = None
        self._transport_capabilities: frozenset[str] = frozenset()
        self._transport_lock = threading.Lock()
        _validate_implementation(protocol, implementation, self._methods)

        # Compute protocol_hash regardless of describe flag — it is needed for
        # access-log records on every dispatch.
        from vgi_rpc.introspect import build_describe_batch

        _hash_batch, _hash_md = build_describe_batch(protocol.__name__, self._methods, self._server_id)
        from vgi_rpc.metadata import PROTOCOL_HASH_KEY

        self._protocol_hash: str = _hash_md.get(PROTOCOL_HASH_KEY, b"").decode()

        if enable_describe:
            from vgi_rpc.introspect import DESCRIBE_METHOD_NAME

            self._describe_batch: pa.RecordBatch | None = _hash_batch
            self._describe_metadata: pa.KeyValueMetadata | None = _hash_md
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

        # Auto-attach Sentry instrumentation when the SDK is initialised in
        # this process.  We only consult sentry_sdk when it is already
        # imported, so this never forces the optional dependency on users
        # who have not opted into Sentry.
        if "sentry_sdk" in sys.modules:
            try:
                from vgi_rpc.sentry import _maybe_auto_instrument

                _maybe_auto_instrument(self)
            except ImportError:
                _logger.debug("sentry_sdk imported but vgi_rpc.sentry unavailable", exc_info=True)

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
    def server_version(self) -> str:
        """Version string passed at construction (empty if not set)."""
        return self._server_version

    @property
    def protocol_version(self) -> str:
        """Operator-supplied free-form protocol version label."""
        return self._protocol_version

    @property
    def protocol_hash(self) -> str:
        """SHA-256 hex digest of the canonical __describe__ payload."""
        return self._protocol_hash

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

    @property
    def transport_kind(self) -> TransportKind | None:
        """Coarse identifier of the bound transport, or ``None`` before serving begins.

        Set by the framework right before the first request is dispatched
        (lazy on HTTP for fork-safety).  Workers may read this directly,
        or rely on the ``on_serve_start`` lifecycle hook for one-shot
        startup work.
        """
        return self._transport_kind

    @property
    def transport_capabilities(self) -> frozenset[str]:
        """Capabilities advertised by the bound transport.

        Currently includes ``"shm"`` when a :class:`ShmPipeTransport` is
        bound.  Empty before a transport is bound and for kinds without
        special capabilities.
        """
        return self._transport_capabilities

    def _notify_transport(self, kind: TransportKind, capabilities: frozenset[str]) -> None:
        """Bind the server to a transport and fire ``on_serve_start`` once.

        Idempotent for the same ``(kind, capabilities)``.  Re-firing with a
        different value (e.g. tests that exercise multiple transports
        against the same server) updates the public attributes and calls
        the hook again — it is **not** an error.

        Bind state is committed only after the hook returns successfully,
        so a transient hook failure on the first request leaves
        ``transport_kind`` unset and the next request re-fires the hook
        rather than silently skipping it.

        Hook exceptions are logged via ``_logger.exception`` and propagate.
        """
        with self._transport_lock:
            if self._transport_kind == kind and self._transport_capabilities == capabilities:
                return
            hook = getattr(self._impl, "on_serve_start", None)
            if callable(hook):
                try:
                    hook(kind)
                except Exception:
                    _logger.exception(
                        "on_serve_start hook raised; aborting serve",
                        extra={"server_id": self._server_id, "transport_kind": kind.value},
                    )
                    raise
            self._transport_kind = kind
            self._transport_capabilities = capabilities

    def serve(self, transport: RpcTransport) -> None:
        """Serve RPC requests in a loop until the transport is closed."""
        capabilities: frozenset[str] = frozenset()
        if isinstance(transport, ShmPipeTransport):
            kind = TransportKind.PIPE
            capabilities = frozenset({"shm"})
        elif isinstance(transport, UnixTransport):
            kind = TransportKind.UNIX
        elif isinstance(transport, PipeTransport):
            kind = TransportKind.PIPE
        else:
            kind = TransportKind.PIPE
        self._notify_transport(kind, capabilities)
        while True:
            try:
                self.serve_one(transport)
            except (EOFError, StopIteration):
                break
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                _logger.debug(
                    "serve loop ending due to broken pipe",
                    exc_info=True,
                    extra={"server_id": self._server_id},
                )
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
        rb_token = _current_request_batch.set(None)
        sid_token = _current_stream_id.set("")
        dynamic_shm: ShmSegment | None = None
        try:
            try:
                method_name, kwargs = _read_request(transport.reader, self._ipc_validation, self._external_config)
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

            # Determine SHM segment: prefer transport-level, fall back to request metadata.
            # ``_maybe_attach_shm`` rejects the dynamic path for HTTP — defence-in-depth
            # since HTTP doesn't currently invoke ``serve_one``.
            shm = transport.shm if isinstance(transport, ShmPipeTransport) else None
            if shm is None:
                dynamic_shm = _maybe_attach_shm(_current_request_metadata.get(), self._transport_kind)
                shm = dynamic_shm

            if info.method_type == MethodType.UNARY:
                self._serve_unary(transport, info, kwargs, stats=stats, shm=shm)
            elif info.method_type == MethodType.STREAM:
                self._serve_stream(transport, info, kwargs, stats=stats, shm=shm)
        finally:
            if dynamic_shm is not None:
                with contextlib.suppress(BufferError):
                    dynamic_shm.close()
            _current_stream_id.reset(sid_token)
            _current_request_batch.reset(rb_token)
            _current_request_metadata.reset(md_token)
            _current_call_stats.reset(stats_token)
            _current_request_id.reset(token)

    def _prepare_method_call(
        self, info: RpcMethodInfo, kwargs: dict[str, object]
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
                kind=self._transport_kind,
            )
        return sink, auth, transport_metadata

    def _serve_unary(
        self,
        transport: RpcTransport,
        info: RpcMethodInfo,
        kwargs: dict[str, object],
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
                server_version=self._server_version,
                protocol_hash=self._protocol_hash,
                protocol_version=self._protocol_version,
            )
            return

        schema = info.result_schema
        sink, auth, transport_md = self._prepare_method_call(info, kwargs)
        protocol_name = self.protocol_name
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        error_message = ""
        hook = self._dispatch_hook
        hook_token: HookToken = None
        if hook is not None:
            try:
                hook_token = hook.on_dispatch_start(info, auth, transport_md, kwargs)
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
                    error_message = str(exc)
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
                error_message=error_message,
                stats=stats,
                server_version=self._server_version,
                protocol_hash=self._protocol_hash,
                protocol_version=self._protocol_version,
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
        kwargs: dict[str, object],
        *,
        stats: CallStatistics | None = None,
        shm: ShmSegment | None = None,
    ) -> None:
        _current_stream_id.set(uuid.uuid4().hex)
        sink, auth, transport_md = self._prepare_method_call(info, kwargs)
        protocol_name = self.protocol_name
        start = time.monotonic()
        status: Literal["ok", "error"] = "ok"
        error_type = ""
        error_message = ""
        hook = self._dispatch_hook
        hook_token: HookToken = None
        if hook is not None:
            try:
                hook_token = hook.on_dispatch_start(info, auth, transport_md, kwargs)
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
            error_message = str(exc)
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
                    error_message=error_message,
                    stats=stats,
                    server_version=self._server_version,
                    protocol_hash=self._protocol_hash,
                    protocol_version=self._protocol_version,
                )
                if hook is not None:
                    try:
                        hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                    except Exception:
                        _logger.debug("Dispatch hook end failed", exc_info=True)

        output_schema = result.output_schema
        input_schema = result.input_schema
        state = result.state
        cancelled = False

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

                        if custom_metadata is not None and custom_metadata.get(CANCEL_KEY) is not None:
                            if wire_stream_logger.isEnabledFor(logging.DEBUG):
                                wire_stream_logger.debug("Stream cancel received: method=%s", info.name)
                            cancelled = True
                            cancel_ctx = CallContext(
                                auth=auth,
                                emit_client_log=sink,
                                transport_metadata=transport_md,
                                server_id=self._server_id,
                                method_name=info.name,
                                protocol_name=protocol_name,
                                kind=self._transport_kind,
                            )
                            try:
                                state.on_cancel(cancel_ctx)
                            except Exception:
                                _logger.debug("on_cancel hook failed", exc_info=True)
                            break

                        # Record input batch for stats — skip tick batches on
                        # producer streams (zero-row, empty-schema protocol artifacts).
                        if input_schema != _EMPTY_SCHEMA:
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

                        input_batch = _coerce_input_batch(input_batch, input_schema)

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
                            kind=self._transport_kind,
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
                    error_message = str(exc)
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
                error_message=error_message,
                stats=stats,
                server_version=self._server_version,
                protocol_hash=self._protocol_hash,
                protocol_version=self._protocol_version,
                cancelled=cancelled,
            )
            if hook is not None:
                try:
                    hook.on_dispatch_end(hook_token, info, _hook_exc, stats=stats)
                except Exception:
                    _logger.debug("Dispatch hook end failed", exc_info=True)

        # Drain remaining input so transport is clean for next request
        with contextlib.suppress(pa.ArrowInvalid, OSError):
            _drain_stream(input_reader)
