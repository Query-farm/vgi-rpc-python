# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport-agnostic RPC framework using Arrow IPC serialization.

Defines RPC interfaces as Python Protocol classes, derives Arrow schemas from
type annotations, and provides typed client proxies with automatic
serialization.

Method Types (derived from return type annotation)
--------------------------------------------------
- **Unary**: ``def method(self, ...) -> T`` — single request, single response
- **Stream**: ``def method(self, ...) -> Stream`` — stateful streaming

Wire Protocol
-------------
Multiple IPC streams are written/read sequentially on the same pipe.  Each
``ipc.open_stream()`` reads one complete IPC stream (schema + batches + EOS)
and stops.  The next ``ipc.open_stream()`` picks up where the last left off.

Every request batch carries ``vgi_rpc.request_version`` in its custom metadata.
The server validates this before dispatching and rejects requests with a
missing or incompatible version (``VersionError``).

Errors and log messages are signaled as zero-row batches with
``vgi_rpc.log_level``, ``vgi_rpc.log_message``, and ``vgi_rpc.log_extra`` custom metadata
on the batch.

- **EXCEPTION** level → error (client raises ``RpcError``)
- **Other levels** (ERROR, WARN, INFO, DEBUG, TRACE) → log message
  (client invokes ``on_log`` callback, or silently discards if no callback)

**Unary**::

    Client→Server: [IPC stream: params_schema + 1 request batch + EOS]
    Server→Client: [IPC stream: result_schema + 0..N log batches + 1 result/error batch + EOS]

**Stream** (pipe transport — lockstep)::

    Phase 1 — request params (same as unary):
      Client→Server: [IPC stream: params_schema + 1 request batch + EOS]

    Phase 2 — lockstep exchange (both within a single IPC stream per direction):
      Client→Server: [IPC stream: input batch₁ + input batch₂ + ... + EOS]
      Server→Client: [IPC stream: (log_batch* + output_batch)* + EOS]

    Producer streams (input_schema == _EMPTY_SCHEMA): client sends tick
    batches, server calls process(tick, out, ctx) and may call out.finish().
    Exchange streams: client sends real data, server calls process(input,
    out, ctx) and always returns a data batch.

Over HTTP, streaming is stateless: each exchange is a separate
``POST /vgi/{method}/exchange`` carrying the input batch and serialized
``StreamState`` in Arrow custom metadata (``vgi_rpc.stream_state``).

State-Based Stream Model
-------------------------
Stream methods return ``Stream[S]`` where ``S`` is a state object with an
explicit ``process()`` method.  State objects extend
``ArrowSerializableDataclass`` so they can be serialized between requests —
this enables stateless HTTP exchanges and resumable producer-stream
continuations.

Call Context
------------
Server method implementations can accept an optional ``ctx`` parameter
(type ``CallContext``) to access authentication, logging, and transport
metadata.  The parameter is injected by the framework when present in
the method signature — it does **not** appear in the Protocol definition.

"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable, Iterator, Mapping

from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.log import Message
from vgi_rpc.rpc._client import (
    RpcConnection,
    StreamSession,
    _RpcProxy,
)
from vgi_rpc.rpc._common import (
    _ANONYMOUS,
    _EMPTY_SCHEMA,
    _EMPTY_TRANSPORT_METADATA,
    AuthContext,
    CallContext,
    CallStatistics,
    ClientLog,
    HookToken,
    MethodType,
    RpcError,
    VersionError,
    _access_logger,
    _ContextLoggerAdapter,
    _current_call_stats,
    _current_request_id,
    _current_request_metadata,
    _current_trace_headers,
    _current_transport,
    _DispatchHook,
    _generate_request_id,
    _get_auth_and_metadata,
    _logger,
    _record_input,
    _record_output,
    _TransportContext,
)
from vgi_rpc.rpc._server import (
    RpcServer,
    _emit_access_log,
    _log_method_error,
)
from vgi_rpc.rpc._transport import (
    PipeTransport,
    RpcTransport,
    ShmPipeTransport,
    StderrMode,
    SubprocessTransport,
    UnixTransport,
    _drain_stderr,
    make_pipe_pair,
    make_unix_pair,
    serve_stdio,
    serve_unix,
)
from vgi_rpc.rpc._types import (
    _TICK_BATCH,
    AnnotatedBatch,
    ExchangeState,
    OutputCollector,
    ProducerState,
    RpcMethodInfo,
    Stream,
    StreamState,
    _build_params_schema,
    _build_result_schema,
    _classify_return_type,
    _format_signature,
    _get_param_defaults,
    _unwrap_annotated,
    _validate_implementation,
    _validate_protocol_params,
    rpc_methods,
)
from vgi_rpc.rpc._wire import (
    _ClientLogSink,
    _convert_for_arrow,
    _deserialize_params,
    _deserialize_value,
    _dispatch_log_or_error,
    _drain_stream,
    _flush_collector,
    _read_batch_with_log_check,
    _read_raw_stream_header,
    _read_request,
    _read_stream_header,
    _read_unary_response,
    _send_request,
    _validate_params,
    _validate_result,
    _write_error_batch,
    _write_error_stream,
    _write_message_batch,
    _write_request,
    _write_result_batch,
    _write_stream_header,
)
from vgi_rpc.utils import IpcValidation

__all__ = [
    # Public API
    "AnnotatedBatch",
    "AuthContext",
    "CallContext",
    "CallStatistics",
    "ClientLog",
    "ExchangeState",
    "MethodType",
    "OutputCollector",
    "PipeTransport",
    "ProducerState",
    "RpcConnection",
    "RpcError",
    "RpcMethodInfo",
    "RpcServer",
    "RpcTransport",
    "ShmPipeTransport",
    "StderrMode",
    "Stream",
    "StreamSession",
    "StreamState",
    "SubprocessTransport",
    "UnixTransport",
    "VersionError",
    "connect",
    "describe_rpc",
    "make_pipe_pair",
    "make_unix_pair",
    "rpc_methods",
    "run_server",
    "serve_pipe",
    "serve_stdio",
    "serve_unix",
    "serve_unix_pipe",
    "unix_connect",
    # Internal — used by vgi_rpc.http, vgi_rpc.introspect, vgi_rpc.external, and tests
    "HookToken",
    "_ANONYMOUS",
    "_ClientLogSink",
    "_ContextLoggerAdapter",
    "_DispatchHook",
    "_EMPTY_SCHEMA",
    "_EMPTY_TRANSPORT_METADATA",
    "_current_call_stats",
    "_record_input",
    "_record_output",
    "_RpcProxy",
    "_TICK_BATCH",
    "_TransportContext",
    "_access_logger",
    "_build_params_schema",
    "_build_result_schema",
    "_classify_return_type",
    "_convert_for_arrow",
    "_current_request_id",
    "_current_request_metadata",
    "_current_trace_headers",
    "_current_transport",
    "_deserialize_params",
    "_deserialize_value",
    "_dispatch_log_or_error",
    "_drain_stderr",
    "_drain_stream",
    "_emit_access_log",
    "_flush_collector",
    "_format_signature",
    "_generate_request_id",
    "_get_auth_and_metadata",
    "_get_param_defaults",
    "_log_method_error",
    "_logger",
    "_read_batch_with_log_check",
    "_read_raw_stream_header",
    "_read_request",
    "_read_stream_header",
    "_read_unary_response",
    "_send_request",
    "_unwrap_annotated",
    "_validate_implementation",
    "_validate_params",
    "_validate_protocol_params",
    "_validate_result",
    "_write_error_batch",
    "_write_error_stream",
    "_write_message_batch",
    "_write_request",
    "_write_result_batch",
    "_write_stream_header",
]


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


def run_server(protocol_or_server: type | RpcServer, implementation: object | None = None) -> None:
    """Serve RPC requests over stdin/stdout.

    This is the recommended entry point for subprocess workers.  Accepts
    either a ``(protocol, implementation)`` pair or a pre-built ``RpcServer``.

    Args:
        protocol_or_server: A Protocol class (requires *implementation*) or
            an already-constructed ``RpcServer``.
        implementation: The implementation object.  Required when
            *protocol_or_server* is a Protocol class; must be ``None`` when
            passing an ``RpcServer``.

    Raises:
        TypeError: On invalid argument combinations.

    """
    if isinstance(protocol_or_server, RpcServer):
        if implementation is not None:
            raise TypeError("implementation must be None when passing an RpcServer")
        server = protocol_or_server
    elif isinstance(protocol_or_server, type):
        if implementation is None:
            raise TypeError("implementation is required when passing a Protocol class")
        server = RpcServer(protocol_or_server, implementation)
    else:
        raise TypeError(f"Expected a Protocol class or RpcServer, got {type(protocol_or_server).__name__}")
    serve_stdio(server)


@contextlib.contextmanager
def connect[P](
    protocol: type[P],
    cmd: list[str],
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    stderr: StderrMode = StderrMode.INHERIT,
    stderr_logger: logging.Logger | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> Iterator[P]:
    """Connect to a subprocess RPC server.

    Context manager that spawns a subprocess, yields a typed proxy, and
    cleans up on exit.

    Args:
        protocol: The Protocol class defining the RPC interface.
        cmd: Command to spawn the subprocess worker.
        on_log: Optional callback for log messages from the server.
        external_location: Optional ExternalLocation configuration for
            resolving and producing externalized batches.
        stderr: How to handle the child's stderr stream (see :class:`StderrMode`).
        stderr_logger: Logger for ``StderrMode.PIPE`` output; ignored for
            other modes.  Defaults to
            ``logging.getLogger("vgi_rpc.subprocess.stderr")``.
        ipc_validation: Validation level for incoming IPC batches.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    transport = SubprocessTransport(cmd, stderr=stderr, stderr_logger=stderr_logger)
    try:
        with RpcConnection(
            protocol, transport, on_log=on_log, external_location=external_location, ipc_validation=ipc_validation
        ) as proxy:
            yield proxy
    finally:
        transport.close()


@contextlib.contextmanager
def serve_pipe[P](
    protocol: type[P],
    implementation: object,
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation | None = None,
) -> Iterator[P]:
    """Start an in-process pipe server and yield a typed client proxy.

    Useful for tests and demos — no subprocess needed.  A background thread
    runs ``RpcServer.serve()`` on the server side of a pipe pair.

    Args:
        protocol: The Protocol class defining the RPC interface.
        implementation: The implementation object.
        on_log: Optional callback for log messages from the server.
        external_location: Optional ExternalLocation configuration for
            resolving and producing externalized batches.
        ipc_validation: Validation level for incoming IPC batches.
            When ``None`` (the default), both components use
            ``IpcValidation.FULL``.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    client_transport, server_transport = make_pipe_pair()
    server = RpcServer(
        protocol,
        implementation,
        external_location=external_location,
        ipc_validation=ipc_validation if ipc_validation is not None else IpcValidation.FULL,
    )
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        with RpcConnection(
            protocol,
            client_transport,
            on_log=on_log,
            external_location=external_location,
            ipc_validation=ipc_validation if ipc_validation is not None else IpcValidation.FULL,
        ) as proxy:
            yield proxy
    finally:
        client_transport.close()
        thread.join(timeout=5)
        server_transport.close()


@contextlib.contextmanager
def unix_connect[P](
    protocol: type[P],
    path: str,
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> Iterator[P]:
    """Connect to a Unix domain socket RPC server and yield a typed proxy.

    Args:
        protocol: The Protocol class defining the RPC interface.
        path: Filesystem path of the Unix domain socket.
        on_log: Optional callback for log messages from the server.
        external_location: Optional ExternalLocation configuration for
            resolving and producing externalized batches.
        ipc_validation: Validation level for incoming IPC batches.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    import socket as _socket

    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    try:
        sock.connect(path)
    except BaseException:
        sock.close()
        raise
    transport = UnixTransport(sock)
    try:
        with RpcConnection(
            protocol, transport, on_log=on_log, external_location=external_location, ipc_validation=ipc_validation
        ) as proxy:
            yield proxy
    finally:
        transport.close()


@contextlib.contextmanager
def serve_unix_pipe[P](
    protocol: type[P],
    implementation: object,
    *,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation | None = None,
) -> Iterator[P]:
    """Start an in-process Unix socket server and yield a typed client proxy.

    Like :func:`serve_pipe` but uses a Unix ``socketpair()`` instead of
    ``os.pipe()`` pairs.  Useful for tests and demos — no subprocess needed.
    A background thread runs ``RpcServer.serve()`` on the server side.

    Args:
        protocol: The Protocol class defining the RPC interface.
        implementation: The implementation object.
        on_log: Optional callback for log messages from the server.
        external_location: Optional ExternalLocation configuration for
            resolving and producing externalized batches.
        ipc_validation: Validation level for incoming IPC batches.
            When ``None`` (the default), both components use
            ``IpcValidation.FULL``.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    client_transport, server_transport = make_unix_pair()
    server = RpcServer(
        protocol,
        implementation,
        external_location=external_location,
        ipc_validation=ipc_validation if ipc_validation is not None else IpcValidation.FULL,
    )
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        with RpcConnection(
            protocol,
            client_transport,
            on_log=on_log,
            external_location=external_location,
            ipc_validation=ipc_validation if ipc_validation is not None else IpcValidation.FULL,
        ) as proxy:
            yield proxy
    finally:
        client_transport.close()
        thread.join(timeout=5)
        server_transport.close()


# ---------------------------------------------------------------------------
# describe_rpc
# ---------------------------------------------------------------------------


def describe_rpc(protocol: type, *, methods: Mapping[str, RpcMethodInfo] | None = None) -> str:
    """Return a human-readable description of an RPC protocol's methods."""
    if methods is None:
        methods = rpc_methods(protocol)
    lines: list[str] = [f"RPC Protocol: {protocol.__name__}", ""]

    for name, info in sorted(methods.items()):
        lines.append(f"  {name}({info.method_type.value})")
        lines.append(f"    params: {info.params_schema}")
        if info.method_type == MethodType.UNARY:
            lines.append(f"    result: {info.result_schema}")
        if info.doc:
            lines.append(f"    doc: {info.doc.strip()}")
        lines.append("")

    return "\n".join(lines)
