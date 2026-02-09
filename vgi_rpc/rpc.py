"""Transport-agnostic RPC framework using Arrow IPC serialization.

Defines RPC interfaces as Python Protocol classes, derives Arrow schemas from
type annotations, and provides typed client proxies with automatic
serialization.

Method Types (derived from return type annotation)
--------------------------------------------------
- **Unary**: ``def method(self, ...) -> T`` — single request, single response
- **Server stream**: ``def method(self, ...) -> ServerStream`` — request, stream of batches
- **Bidi stream**: ``def method(self, ...) -> BidiStream`` — bidirectional

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

**Server stream**::

    Client→Server: [IPC stream: params_schema + 1 request batch + EOS]
    Server→Client: [IPC stream: output_schema + (log_batch | data_batch)* + EOS]
                 On error, the final batch is 0-row with error metadata.

**Bidi stream** (state + process)::

    Phase 1 — request params (same as unary/stream):
      Client→Server: [IPC stream: params_schema + 1 request batch + EOS]

    Phase 2 — lockstep exchange (both within a single IPC stream per direction):
      Client→Server: [IPC stream: input batch₁ + input batch₂ + ... + EOS]
      Server→Client: [IPC stream: (log_batch* + output_batch)* + EOS]

    Each input batch produces one output batch (1:1 lockstep).
    Log batches may appear before each output batch.
    Client closes its input stream (EOS) to signal end of exchange.
    On error, the final output batch is 0-row with error metadata.

State-Based Stream Model
-------------------------
Server stream and bidi stream methods return ``ServerStream[S]`` or
``BidiStream[S]`` where ``S`` is a state object with explicit ``produce()``
or ``process()`` methods.  State objects extend ``ArrowSerializableDataclass``
so they can be serialized between requests (required for HTTP transport).

Out-of-Band Logging
-------------------
Server method implementations can accept an optional ``emit_log`` parameter
(type ``EmitLog``) to emit log messages during processing.  The parameter is
injected by the framework when present in the method signature — it does **not**
appear in the Protocol definition.

"""

from __future__ import annotations

import abc
import contextlib
import functools
import inspect
import json
import logging
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from io import IOBase
from types import MappingProxyType, TracebackType
from typing import (
    Annotated,
    Any,
    Protocol,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import (
    LOG_EXTRA_KEY,
    LOG_LEVEL_KEY,
    LOG_MESSAGE_KEY,
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
    encode_metadata,
    merge_metadata,
)
from vgi_rpc.utils import ArrowSerializableDataclass, _infer_arrow_type, _is_optional_type, empty_batch

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EMPTY_SCHEMA = pa.schema([])
_logger = logging.getLogger(__name__)

EmitLog = Callable[[Message], None]
"""Callback type for emitting log messages from RPC method implementations."""

__all__ = [
    "AnnotatedBatch",
    "BidiSession",
    "BidiStream",
    "BidiStreamState",
    "EmitLog",
    "MethodType",
    "OutputCollector",
    "PipeTransport",
    "RpcConnection",
    "RpcError",
    "RpcMethodInfo",
    "RpcServer",
    "RpcTransport",
    "ServerStream",
    "ServerStreamState",
    "StreamSession",
    "SubprocessTransport",
    "connect",
    "describe_rpc",
    "make_pipe_pair",
    "rpc_methods",
    "run_server",
    "serve_pipe",
    "serve_stdio",
]


# ---------------------------------------------------------------------------
# MethodType enum
# ---------------------------------------------------------------------------


class MethodType(Enum):
    """Classification of RPC method patterns."""

    UNARY = "unary"
    SERVER_STREAM = "server_stream"
    BIDI_STREAM = "bidi_stream"


# ---------------------------------------------------------------------------
# Stream return types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AnnotatedBatch:
    """A RecordBatch paired with its custom metadata.

    Used as both input and output for all batch I/O in stream methods.
    """

    batch: pa.RecordBatch
    custom_metadata: pa.KeyValueMetadata | None = None

    @classmethod
    def from_pydict(
        cls,
        data: dict[str, Any],
        schema: pa.Schema | None = None,
    ) -> AnnotatedBatch:
        """Create from a Python dict, optionally with a schema."""
        batch = pa.RecordBatch.from_pydict(data, schema=schema)
        return cls(batch=batch)


class OutputCollector:
    """Accumulates output batches during a produce/process call.

    Enforces that exactly one data batch is emitted per call (plus any number
    of log batches).  Batches are stored in a single ordered list because
    interleaving order matters for the wire protocol (logs must precede the
    data batch they annotate).
    """

    __slots__ = ("_output_schema", "_batches", "_finished", "_data_batch_idx")

    def __init__(self, output_schema: pa.Schema) -> None:
        """Initialize with the output schema for this stream."""
        self._output_schema = output_schema
        self._batches: list[AnnotatedBatch] = []
        self._finished: bool = False
        self._data_batch_idx: int | None = None

    @property
    def output_schema(self) -> pa.Schema:
        """The output schema for this stream."""
        return self._output_schema

    @property
    def finished(self) -> bool:
        """Whether finish() has been called."""
        return self._finished

    @property
    def batches(self) -> list[AnnotatedBatch]:
        """The accumulated batches."""
        return self._batches

    # --- Data emission (exactly one per call) ---

    @property
    def data_batch(self) -> AnnotatedBatch:
        """Return the single data batch, or raise if none was emitted."""
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")
        return self._batches[self._data_batch_idx]

    def validate(self) -> None:
        """Assert that exactly one data batch was emitted.

        Raises:
            RuntimeError: If no data batch was emitted.

        """
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")

    def merge_data_metadata(self, metadata: pa.KeyValueMetadata) -> None:
        """Merge extra metadata into the data batch.

        Raises:
            RuntimeError: If no data batch was emitted.

        """
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")
        ab = self._batches[self._data_batch_idx]
        self._batches[self._data_batch_idx] = AnnotatedBatch(
            batch=ab.batch,
            custom_metadata=merge_metadata(ab.custom_metadata, metadata),
        )

    def emit(
        self,
        batch: pa.RecordBatch,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Emit a pre-built data batch. Raises if a data batch was already emitted."""
        if self._data_batch_idx is not None:
            raise RuntimeError("Only one data batch may be emitted per call")
        self._data_batch_idx = len(self._batches)
        custom_metadata = encode_metadata(metadata) if metadata else None
        self._batches.append(AnnotatedBatch(batch=batch, custom_metadata=custom_metadata))

    def emit_arrays(
        self,
        arrays: Sequence[pa.Array[Any]],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Build a RecordBatch from arrays using output_schema and emit it."""
        batch = pa.RecordBatch.from_arrays(arrays, schema=self._output_schema)
        self.emit(batch, metadata=metadata)

    def emit_pydict(
        self,
        data: dict[str, Any],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Build a RecordBatch from a Python dict using output_schema and emit it."""
        batch = pa.RecordBatch.from_pydict(data, schema=self._output_schema)
        self.emit(batch, metadata=metadata)

    # --- Logging (zero or more per call) ---

    def log(self, level: Level, message: str, **extra: str) -> None:
        """Emit a zero-row log batch with log metadata."""
        msg = Message(level, message, **extra)
        custom_metadata = encode_metadata(msg.add_to_metadata())
        self._batches.append(AnnotatedBatch(batch=empty_batch(self._output_schema), custom_metadata=custom_metadata))

    # --- Stream completion (ServerStream only) ---

    def finish(self) -> None:
        """Signal stream completion. Emits a zero-row non-log batch."""
        self._finished = True


class ServerStreamState(ArrowSerializableDataclass, abc.ABC):
    """Base class for server-stream state objects.

    Subclasses must be dataclasses that define ``produce(out)`` which is
    called repeatedly to generate output batches.  Call ``out.finish()``
    to signal stream end.

    Extends ``ArrowSerializableDataclass`` so that state can be serialized
    between requests (required for HTTP transport).
    """

    @abc.abstractmethod
    def produce(self, out: OutputCollector) -> None:
        """Produce output batches into the collector."""
        ...


class BidiStreamState(ArrowSerializableDataclass, abc.ABC):
    """Base class for bidi-stream state objects.

    Subclasses must be dataclasses that define ``process(input, out)``
    which is called once per input batch.  State is mutated in-place
    across calls.

    Extends ``ArrowSerializableDataclass`` so that state can be serialized
    between requests (required for HTTP transport).
    """

    @abc.abstractmethod
    def process(self, input: AnnotatedBatch, out: OutputCollector) -> None:
        """Process an input batch and emit output into the collector."""
        ...


@dataclass(frozen=True)
class ServerStream[S: ServerStreamState]:
    """Return type for server-stream RPC methods.

    Bundles the output schema with a state object whose ``produce(out)`` method
    is called repeatedly to generate output batches.  The state object calls
    ``out.finish()`` to signal stream completion.
    """

    output_schema: pa.Schema
    state: S


@dataclass(frozen=True)
class BidiStream[S: BidiStreamState]:
    """Return type for bidirectional-stream RPC methods.

    Bundles the output schema with a state object whose ``process(input, out)``
    method is called once per input batch.  State is mutated in-place across calls.

    If ``input_schema`` is provided, the framework validates each input batch
    against it before passing to ``process()``.  When ``None`` (the default),
    the input schema is implicit from the client's first batch.
    """

    output_schema: pa.Schema
    state: S
    input_schema: pa.Schema | None = None


# ---------------------------------------------------------------------------
# RpcError
# ---------------------------------------------------------------------------


class RpcError(Exception):
    """Raised on the client side when the server reports an error."""

    def __init__(self, error_type: str, error_message: str, remote_traceback: str) -> None:
        """Initialize with error details from the remote side."""
        self.error_type = error_type
        self.error_message = error_message
        self.remote_traceback = remote_traceback
        super().__init__(f"{error_type}: {error_message}")


class VersionError(Exception):
    """Raised when a request has a missing or incompatible protocol version."""


# ---------------------------------------------------------------------------
# RpcMethodInfo
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RpcMethodInfo:
    """Metadata for a single RPC method, derived from Protocol type hints."""

    name: str
    params_schema: pa.Schema
    result_schema: pa.Schema
    result_type: Any
    method_type: MethodType
    has_return: bool
    doc: str | None
    param_defaults: dict[str, Any] = field(default_factory=dict)
    param_types: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Protocol introspection
# ---------------------------------------------------------------------------


def _unwrap_annotated(hint: Any) -> Any:
    """Unwrap Annotated[T, ...] to T, or return hint unchanged."""
    if get_origin(hint) is Annotated:
        return get_args(hint)[0]
    return hint


def _classify_return_type(hint: Any) -> tuple[MethodType, Any, bool]:
    """Classify a return type hint into a MethodType.

    Returns (method_type, result_type, has_return).
    Handles both bare ``ServerStream`` / ``BidiStream`` and generic forms
    like ``ServerStream[MyState]``.
    """
    # Bare class reference
    if hint is ServerStream:
        return MethodType.SERVER_STREAM, hint, False
    if hint is BidiStream:
        return MethodType.BIDI_STREAM, hint, False

    # Generic form: ServerStream[S] / BidiStream[S]
    origin = get_origin(hint)
    if origin is ServerStream:
        return MethodType.SERVER_STREAM, hint, False
    if origin is BidiStream:
        return MethodType.BIDI_STREAM, hint, False

    # Everything else -> UNARY
    has_return = hint is not type(None) and hint is not None
    return MethodType.UNARY, hint, has_return


def _build_params_schema(hints: dict[str, Any]) -> pa.Schema:
    """Build an Arrow schema from method parameter type hints (excluding 'self' and 'return')."""
    fields: list[pa.Field[pa.DataType]] = []
    for name, hint in hints.items():
        if name in ("self", "return"):
            continue
        inner, is_nullable = _is_optional_type(hint)
        base = _unwrap_annotated(inner)
        if isinstance(base, type) and issubclass(base, ArrowSerializableDataclass):
            fields.append(pa.field(name, pa.binary(), nullable=is_nullable))
        else:
            arrow_type = _infer_arrow_type(inner)  # handles Annotated natively
            fields.append(pa.field(name, arrow_type, nullable=is_nullable))
    return pa.schema(fields)


def _build_result_schema(result_type: Any) -> pa.Schema:
    """Build a single-field Arrow schema for a unary result type."""
    if result_type is type(None) or result_type is None:
        return _EMPTY_SCHEMA

    # ArrowSerializableDataclass — serialize whole dataclass as binary blob
    base = _unwrap_annotated(result_type)
    if isinstance(base, type) and issubclass(base, ArrowSerializableDataclass):
        return pa.schema([pa.field("result", pa.binary())])

    inner, is_nullable = _is_optional_type(result_type)
    arrow_type = _infer_arrow_type(inner)  # handles Annotated natively
    return pa.schema([pa.field("result", arrow_type, nullable=is_nullable)])


def _get_param_defaults(protocol: type, method_name: str) -> dict[str, Any]:
    """Extract default values for method parameters."""
    method = getattr(protocol, method_name, None)
    if method is None:
        return {}
    sig = inspect.signature(method)
    defaults: dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if param.default is not inspect.Parameter.empty:
            defaults[name] = param.default
    return defaults


@functools.lru_cache(maxsize=64)
def rpc_methods(protocol: type) -> Mapping[str, RpcMethodInfo]:
    """Introspect a Protocol class and return RpcMethodInfo for each method.

    Skips underscore-prefixed names and non-callable attributes.
    """
    result: dict[str, RpcMethodInfo] = {}

    # Get method names from Protocol — look at annotations and callables
    for name in dir(protocol):
        if name.startswith("_"):
            continue
        attr = getattr(protocol, name, None)
        if attr is None or not callable(attr):
            continue

        try:
            method_hints = get_type_hints(attr, include_extras=True)
        except (NameError, AttributeError):
            continue

        return_hint = method_hints.get("return", type(None))
        method_type, result_type, has_return = _classify_return_type(return_hint)

        # For unary methods, build result schema from the result type
        result_schema = _build_result_schema(result_type) if method_type == MethodType.UNARY else _EMPTY_SCHEMA

        param_defaults = _get_param_defaults(protocol, name)
        params_schema = _build_params_schema(method_hints)
        param_types = {k: v for k, v in method_hints.items() if k not in ("self", "return")}

        doc = getattr(attr, "__doc__", None)

        result[name] = RpcMethodInfo(
            name=name,
            params_schema=params_schema,
            result_schema=result_schema,
            result_type=result_type,
            method_type=method_type,
            has_return=has_return,
            doc=doc,
            param_defaults=param_defaults,
            param_types=param_types,
        )

    return MappingProxyType(result)


# ---------------------------------------------------------------------------
# Implementation validation
# ---------------------------------------------------------------------------


def _format_signature(info: RpcMethodInfo) -> str:
    """Format a protocol method signature for error messages."""
    params = ", ".join(f"{n}: {getattr(t, '__name__', str(t))}" for n, t in info.param_types.items())
    return f"{info.name}({params})"


def _validate_implementation(
    protocol: type,
    implementation: object,
    methods: Mapping[str, RpcMethodInfo],
) -> None:
    """Validate that *implementation* conforms to *protocol*.

    Checks that every method declared in the protocol exists on the
    implementation, is callable, and has a compatible parameter list.
    The special ``emit_log`` parameter is allowed on implementations
    even when not present in the protocol.

    Raises:
        TypeError: If one or more validation errors are found.  The
            message lists every problem so the developer can fix them
            all in one pass.

    """
    errors: list[str] = []

    for name, info in methods.items():
        method = getattr(implementation, name, None)

        if method is None:
            errors.append(f"missing method {_format_signature(info)}")
            continue

        if not callable(method):
            errors.append(f"'{name}' exists but is not callable")
            continue

        impl_sig = inspect.signature(method)
        impl_params = {k: v for k, v in impl_sig.parameters.items() if k != "self"}
        proto_param_names = set(info.param_types.keys())

        for param_name in proto_param_names:
            if param_name not in impl_params:
                errors.append(f"'{name}()' missing parameter '{param_name}'")

        for param_name, param in impl_params.items():
            if param_name in proto_param_names:
                continue
            if param_name == "emit_log":
                continue
            if param.default is inspect.Parameter.empty and param.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                errors.append(f"'{name}()' has required parameter '{param_name}' not defined in {protocol.__name__}")

    if errors:
        impl_name = type(implementation).__name__
        header = f"{impl_name} does not implement {protocol.__name__}:"
        detail = "\n".join(f"  - {e}" for e in errors)
        raise TypeError(f"{header}\n{detail}")


# ---------------------------------------------------------------------------
# IPC stream helpers
# ---------------------------------------------------------------------------


def _convert_for_arrow(val: object) -> object:
    """Convert a Python value for Arrow serialization.

    Inverse of ``_deserialize_value``.  Handles types that Arrow cannot
    serialize directly:

    - Enum → .name (string)
    - frozenset → list
    - dict → list of tuples (for map types)
    - ArrowSerializableDataclass → bytes
    """
    if isinstance(val, ArrowSerializableDataclass):
        return val.serialize_to_bytes()
    if isinstance(val, Enum):
        return val.name
    if isinstance(val, frozenset):
        return list(val)
    if isinstance(val, dict):
        return list(val.items())
    return val


def _write_request(writer_stream: IOBase, method_name: str, params_schema: pa.Schema, kwargs: dict[str, Any]) -> None:
    """Write a request as a complete IPC stream (schema + 1 batch + EOS).

    The batch's custom_metadata carries ``vgi_rpc.method`` (the method name)
    and ``vgi_rpc.request_version`` (the wire-protocol version).
    """
    arrays: list[pa.Array[Any]] = []
    for f in params_schema:
        val = _convert_for_arrow(kwargs.get(f.name))
        arrays.append(pa.array([val], type=f.type))
    batch = pa.RecordBatch.from_arrays(arrays, schema=params_schema)
    custom_metadata = pa.KeyValueMetadata({RPC_METHOD_KEY: method_name.encode(), REQUEST_VERSION_KEY: REQUEST_VERSION})
    with ipc.new_stream(writer_stream, params_schema) as writer:
        writer.write_batch(batch, custom_metadata=custom_metadata)


def _write_message_batch(
    writer: ipc.RecordBatchStreamWriter,
    schema: pa.Schema,
    msg: Message,
) -> None:
    """Write a zero-row batch with Message metadata on an existing IPC stream writer."""
    custom_metadata = encode_metadata(msg.add_to_metadata())
    writer.write_batch(empty_batch(schema), custom_metadata=custom_metadata)


def _write_error_batch(
    writer: ipc.RecordBatchStreamWriter,
    schema: pa.Schema,
    exc: BaseException,
) -> None:
    """Write error as zero-row batch (convenience wrapper)."""
    _write_message_batch(writer, schema, Message.from_exception(exc))


def _write_error_stream(writer_stream: IOBase, schema: pa.Schema, exc: BaseException) -> None:
    """Write a complete IPC stream containing just an error batch."""
    with ipc.new_stream(writer_stream, schema) as writer:
        _write_error_batch(writer, schema, exc)


class _LogSink:
    """Buffers log messages until an IPC writer is available, then writes directly."""

    __slots__ = ("_buffer", "_writer", "_schema")

    def __init__(self) -> None:
        self._buffer: list[Message] = []
        self._writer: ipc.RecordBatchStreamWriter | None = None
        self._schema: pa.Schema | None = None

    def __call__(self, msg: Message) -> None:
        if self._writer is not None and self._schema is not None:
            _write_message_batch(self._writer, self._schema, msg)
        else:
            self._buffer.append(msg)

    def flush_contents(self, writer: ipc.RecordBatchStreamWriter, schema: pa.Schema) -> None:
        """Flush buffered messages and switch to direct writing."""
        self._writer = writer
        self._schema = schema
        for msg in self._buffer:
            _write_message_batch(writer, schema, msg)
        self._buffer.clear()


def _write_result_batch(writer: ipc.RecordBatchStreamWriter, result_schema: pa.Schema, value: object) -> None:
    """Write a unary result batch to an already-open IPC stream writer."""
    if len(result_schema) == 0:
        batch = pa.RecordBatch.from_pydict({}, schema=_EMPTY_SCHEMA)
    else:
        wire_value = _convert_for_arrow(value)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([wire_value], type=result_schema.field(0).type)], schema=result_schema
        )
    writer.write_batch(batch)


def _read_request(reader_stream: IOBase) -> tuple[str, dict[str, Any]]:
    """Read a request IPC stream, return (method_name, kwargs).

    Extracts ``vgi_rpc.method`` and validates ``vgi_rpc.request_version``
    from the batch's custom_metadata.

    Raises:
        RpcError: If ``vgi_rpc.method`` is missing.
        VersionError: If ``vgi_rpc.request_version`` is missing or
            does not match ``REQUEST_VERSION``.

    """
    reader = ipc.open_stream(reader_stream)
    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
    method_name_bytes = custom_metadata.get(RPC_METHOD_KEY) if custom_metadata else None
    if method_name_bytes is None:
        raise RpcError("ProtocolError", "Missing vgi_rpc.method in request batch custom_metadata", "")
    version_bytes = custom_metadata.get(REQUEST_VERSION_KEY) if custom_metadata else None
    if version_bytes is None:
        raise VersionError("Missing vgi_rpc.request_version in request metadata")
    if version_bytes != REQUEST_VERSION:
        raise VersionError(f"Unsupported request version {version_bytes!r}, expected {REQUEST_VERSION!r}")
    method_name = method_name_bytes.decode()
    _drain_stream(reader)
    kwargs = {f.name: batch.column(i)[0].as_py() for i, f in enumerate(batch.schema)}
    return method_name, kwargs


def _dispatch_log_or_error(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    on_log: Callable[[Message], None] | None = None,
) -> bool:
    """Dispatch a zero-row log/error batch; return whether the batch was consumed.

    - Data batches (num_rows > 0 or no log metadata) → return ``False``
    - EXCEPTION level → **raise** ``RpcError``
    - Other log levels → invoke *on_log* callback, return ``True``

    Callers should loop, skipping consumed batches until a data batch is found.
    """
    if custom_metadata is None:
        return False
    if batch.num_rows != 0:
        return False
    level_bytes = custom_metadata.get(LOG_LEVEL_KEY)
    message_bytes = custom_metadata.get(LOG_MESSAGE_KEY)
    if level_bytes is None or message_bytes is None:
        return False

    level_str = level_bytes.decode()
    message_str = message_bytes.decode()

    # Extract extra info (traceback, exception_type, etc.)
    extra: dict[str, object] = {}
    raw_extra = custom_metadata.get(LOG_EXTRA_KEY)
    if raw_extra is not None:
        with contextlib.suppress(json.JSONDecodeError):
            extra = json.loads(raw_extra.decode())

    # EXCEPTION level → raise RpcError (existing behaviour)
    if level_str == Level.EXCEPTION.value:
        error_type = str(extra.get("exception_type", level_str))
        traceback_str = str(extra.get("traceback", ""))
        raise RpcError(error_type, message_str, traceback_str)

    # Non-exception log message → invoke callback
    # Strip internal keys (pid) before reconstructing Message.extra
    log_extra = {k: v for k, v in extra.items() if k != "pid"}
    msg = Message(Level(level_str), message_str, **log_extra)
    if on_log is not None:
        on_log(msg)
    return True


def _deserialize_value(value: object, type_hint: Any) -> object:
    """Deserialize a single value based on its type hint.

    Inverse of ``_convert_for_arrow``.  Handles ArrowSerializableDataclass,
    Enum, dict, frozenset.  Each branch narrows the value type with
    ``isinstance`` before performing type-specific operations.
    """
    inner, _ = _is_optional_type(type_hint)
    base = _unwrap_annotated(inner)
    if isinstance(base, type) and issubclass(base, ArrowSerializableDataclass):
        if not isinstance(value, bytes):
            return value
        reader = ipc.open_stream(value)
        batch, metadata = reader.read_next_batch_with_custom_metadata()
        return base.deserialize_from_batch(batch, metadata)
    if isinstance(base, type) and issubclass(base, Enum):
        if not isinstance(value, str):
            return value
        return base[value]
    origin = get_origin(base)
    if origin is dict and isinstance(value, list):
        return dict(cast(list[tuple[Any, Any]], value))
    if origin is frozenset and isinstance(value, list):
        return frozenset(value)
    return value


def _deserialize_params(kwargs: dict[str, Any], param_types: dict[str, Any]) -> None:
    """Deserialize params that lose type fidelity through as_py() in-place.

    Handles ArrowSerializableDataclass (bytes), Enum (str→member),
    dict (list of tuples→dict), and frozenset (list→frozenset).
    """
    for name, value in kwargs.items():
        if value is None:
            continue
        ptype = param_types.get(name)
        if ptype is None:
            continue
        kwargs[name] = _deserialize_value(value, ptype)


def _validate_params(method_name: str, kwargs: dict[str, Any], param_types: dict[str, Any]) -> None:
    """Validate that non-optional parameters are not None.

    Raises TypeError if a None value is passed for a parameter whose type
    annotation does not include None (i.e., is not ``X | None``).
    """
    for name, value in kwargs.items():
        if value is not None:
            continue
        ptype = param_types.get(name)
        if ptype is None:
            continue
        _, is_nullable = _is_optional_type(ptype)
        if not is_nullable:
            raise TypeError(f"{method_name}() parameter '{name}' is not optional but got None")


def _validate_result(method_name: str, value: object, result_type: Any) -> None:
    """Validate that a non-optional return value is not None.

    Raises TypeError if the implementation returns None for a method whose
    return type annotation does not include None.
    """
    if value is not None:
        return
    if result_type is None or result_type is type(None):
        return
    _, is_nullable = _is_optional_type(result_type)
    if not is_nullable:
        raise TypeError(f"{method_name}() expected a non-None return value but got None")


def _drain_stream(reader: ipc.RecordBatchStreamReader) -> None:
    """Consume remaining batches so the IPC EOS marker is read."""
    while True:
        try:
            reader.read_next_batch()
        except StopIteration:
            return


# ---------------------------------------------------------------------------
# RpcTransport protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RpcTransport(Protocol):
    """Bidirectional byte stream transport."""

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        ...

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        ...

    def close(self) -> None:
        """Close the transport."""
        ...


# ---------------------------------------------------------------------------
# PipeTransport + make_pipe_pair
# ---------------------------------------------------------------------------


class PipeTransport:
    """Transport backed by file-like IO streams (e.g. from os.pipe())."""

    __slots__ = ("_reader", "_writer")

    def __init__(self, reader: IOBase, writer: IOBase) -> None:
        """Initialize with reader and writer streams."""
        self._reader = reader
        self._writer = writer

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._writer

    def close(self) -> None:
        """Close both streams."""
        self._reader.close()
        self._writer.close()


def make_pipe_pair() -> tuple[PipeTransport, PipeTransport]:
    """Create connected client/server transports using os.pipe().

    Returns (client_transport, server_transport).
    """
    c2s_r, c2s_w = os.pipe()
    s2c_r, s2c_w = os.pipe()
    client = PipeTransport(
        os.fdopen(s2c_r, "rb"),
        os.fdopen(c2s_w, "wb", buffering=0),
    )
    server = PipeTransport(
        os.fdopen(c2s_r, "rb"),
        os.fdopen(s2c_w, "wb", buffering=0),
    )
    return client, server


class SubprocessTransport:
    """Transport that communicates with a child process over stdin/stdout.

    Spawns a command via ``subprocess.Popen`` with ``stdin=PIPE``,
    ``stdout=PIPE``, and ``stderr=None`` (inherits the parent's stderr).

    The writer (child's stdin) is kept unbuffered (``bufsize=0``) so IPC
    data is flushed immediately.  The reader (child's stdout) is wrapped
    in a ``BufferedReader`` because Arrow IPC expects ``read(n)`` to
    return exactly *n* bytes, but raw ``FileIO.read(n)`` on a pipe may
    return fewer (POSIX short-read semantics).
    """

    __slots__ = ("_proc", "_reader", "_writer", "_closed")

    def __init__(self, cmd: list[str]) -> None:
        """Spawn the subprocess and wire up stdin/stdout as the transport."""
        self._proc = subprocess.Popen(  # noqa: S603
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            bufsize=0,
        )
        assert self._proc.stdout is not None
        assert self._proc.stdin is not None
        self._reader: IOBase = os.fdopen(self._proc.stdout.fileno(), "rb", closefd=False)
        self._writer: IOBase = cast(IOBase, self._proc.stdin)
        self._closed = False

    @property
    def proc(self) -> subprocess.Popen[bytes]:
        """The underlying Popen process."""
        return self._proc

    @property
    def reader(self) -> IOBase:
        """Readable binary stream (child's stdout, buffered)."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream (child's stdin, unbuffered)."""
        return self._writer

    def close(self) -> None:
        """Close stdin (sends EOF), wait for exit, close stdout."""
        if self._closed:
            return
        self._closed = True
        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.wait()
        self._reader.close()


def serve_stdio(server: RpcServer) -> None:
    """Serve RPC requests over stdin/stdout.

    This is the server-side entry point for subprocess mode.  The reader
    uses default buffering so that ``read(n)`` returns exactly *n* bytes
    (Arrow IPC requires this; raw ``FileIO.read(n)`` may short-read on
    pipes).  The writer is unbuffered (``buffering=0``) so IPC data is
    flushed immediately.  Uses ``closefd=False`` so the original stdio
    descriptors are not closed on exit.
    """
    reader = os.fdopen(sys.stdin.fileno(), "rb", closefd=False)
    writer = os.fdopen(sys.stdout.fileno(), "wb", buffering=0, closefd=False)
    transport = PipeTransport(reader, writer)
    server.serve(transport)


def _flush_collector(writer: ipc.RecordBatchStreamWriter, out: OutputCollector) -> None:
    """Write all accumulated batches from an OutputCollector to an IPC stream writer."""
    for ab in out.batches:
        if ab.custom_metadata is not None:
            writer.write_batch(ab.batch, custom_metadata=ab.custom_metadata)
        else:
            writer.write_batch(ab.batch)


# ---------------------------------------------------------------------------
# RpcServer
# ---------------------------------------------------------------------------


class RpcServer:
    """Dispatches RPC requests to an implementation over IO-stream transports."""

    __slots__ = ("_protocol", "_impl", "_methods", "_emit_log_methods")

    def __init__(self, protocol: type, implementation: object) -> None:
        """Initialize with a protocol type and its implementation."""
        self._protocol = protocol
        self._impl = implementation
        self._methods = rpc_methods(protocol)
        _validate_implementation(protocol, implementation, self._methods)

        # Detect which impl methods accept an `emit_log` parameter.
        self._emit_log_methods: frozenset[str] = frozenset(
            name
            for name in self._methods
            if (method := getattr(implementation, name, None)) is not None
            and "emit_log" in inspect.signature(method).parameters
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
    def emit_log_methods(self) -> frozenset[str]:
        """Method names whose implementations accept an emit_log parameter."""
        return self._emit_log_methods

    def serve(self, transport: RpcTransport) -> None:
        """Serve RPC requests in a loop until the transport is closed."""
        while True:
            try:
                self.serve_one(transport)
            except (EOFError, StopIteration):
                break
            except pa.ArrowInvalid:
                _logger.debug("serve loop ending due to ArrowInvalid", exc_info=True)
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
        try:
            method_name, kwargs = _read_request(transport.reader)
        except pa.ArrowInvalid as exc:
            with contextlib.suppress(BrokenPipeError, OSError):
                _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc)
            raise
        except (VersionError, RpcError) as exc:
            with contextlib.suppress(BrokenPipeError, OSError):
                _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc)
            return
        info = self._methods.get(method_name)
        if info is None:
            _write_error_stream(transport.writer, _EMPTY_SCHEMA, AttributeError(f"Unknown method: {method_name}"))
            return

        _deserialize_params(kwargs, info.param_types)

        try:
            _validate_params(info.name, kwargs, info.param_types)
        except TypeError as exc:
            err_schema = info.result_schema if info.method_type == MethodType.UNARY else _EMPTY_SCHEMA
            _write_error_stream(transport.writer, err_schema, exc)
            return

        if info.method_type == MethodType.UNARY:
            self._serve_unary(transport, info, kwargs)
        elif info.method_type == MethodType.SERVER_STREAM:
            self._serve_server_stream(transport, info, kwargs)
        elif info.method_type == MethodType.BIDI_STREAM:
            self._serve_bidi_stream(transport, info, kwargs)

    def _make_log_sink(self, info: RpcMethodInfo, kwargs: dict[str, Any]) -> _LogSink:
        """Create a log sink, wiring it into *kwargs* if the method accepts ``emit_log``."""
        sink = _LogSink()
        if info.name in self._emit_log_methods:
            kwargs["emit_log"] = sink
        return sink

    def _serve_unary(self, transport: RpcTransport, info: RpcMethodInfo, kwargs: dict[str, Any]) -> None:
        schema = info.result_schema
        sink = self._make_log_sink(info, kwargs)
        with ipc.new_stream(transport.writer, schema) as writer:
            sink.flush_contents(writer, schema)
            try:
                result = getattr(self._impl, info.name)(**kwargs)
                _validate_result(info.name, result, info.result_type)
            except Exception as exc:
                _write_error_batch(writer, schema, exc)
                return
            _write_result_batch(writer, info.result_schema, result)

    def _serve_server_stream(self, transport: RpcTransport, info: RpcMethodInfo, kwargs: dict[str, Any]) -> None:
        sink = self._make_log_sink(info, kwargs)
        try:
            result: ServerStream[ServerStreamState] = getattr(self._impl, info.name)(**kwargs)
        except Exception as exc:
            _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc)
            return

        schema = result.output_schema
        state = result.state
        with ipc.new_stream(transport.writer, schema) as stream_writer:
            sink.flush_contents(stream_writer, schema)
            try:
                while True:
                    out = OutputCollector(schema)
                    state.produce(out)
                    if not out.finished:
                        out.validate()
                    _flush_collector(stream_writer, out)
                    if out.finished:
                        break
            except Exception as exc:
                _write_error_batch(stream_writer, schema, exc)

    def _serve_bidi_stream(self, transport: RpcTransport, info: RpcMethodInfo, kwargs: dict[str, Any]) -> None:
        sink = self._make_log_sink(info, kwargs)

        try:
            result: BidiStream[BidiStreamState] = getattr(self._impl, info.name)(**kwargs)
        except Exception as exc:
            _write_error_stream(transport.writer, _EMPTY_SCHEMA, exc)
            return

        output_schema = result.output_schema
        expected_input_schema = result.input_schema
        state = result.state

        input_reader = ipc.open_stream(transport.reader)

        with ipc.new_stream(transport.writer, output_schema) as output_writer:
            sink.flush_contents(output_writer, output_schema)
            try:
                while True:
                    try:
                        input_batch, custom_metadata = input_reader.read_next_batch_with_custom_metadata()
                    except StopIteration:
                        break

                    if expected_input_schema is not None and input_batch.schema != expected_input_schema:
                        raise TypeError(
                            f"Input schema mismatch: expected {expected_input_schema}, got {input_batch.schema}"
                        )

                    ab_in = AnnotatedBatch(batch=input_batch, custom_metadata=custom_metadata)
                    out = OutputCollector(output_schema)
                    state.process(ab_in, out)
                    out.validate()
                    _flush_collector(output_writer, out)
            except Exception as exc:
                _write_error_batch(output_writer, output_schema, exc)

        # Drain remaining input so transport is clean for next request
        _drain_stream(input_reader)


# ---------------------------------------------------------------------------
# _RpcProxy — dynamic typed client proxy
# ---------------------------------------------------------------------------


def _read_batch_with_log_check(
    reader: ipc.RecordBatchStreamReader,
    on_log: Callable[[Message], None] | None = None,
) -> AnnotatedBatch:
    """Read the next non-log batch, dispatching log batches to *on_log*.

    Loops internally, skipping zero-row log batches (via
    ``_dispatch_log_or_error``).  Returns the first data batch as an
    ``AnnotatedBatch``.  ``StopIteration`` and ``RpcError`` propagate
    to the caller.
    """
    while True:
        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        if not _dispatch_log_or_error(batch, custom_metadata, on_log):
            return AnnotatedBatch(batch=batch, custom_metadata=custom_metadata)


def _read_unary_response(
    reader: ipc.RecordBatchStreamReader,
    info: RpcMethodInfo,
    on_log: Callable[[Message], None] | None,
) -> object:
    """Read a unary response: skip logs, extract result, deserialize."""
    try:
        batch = _read_batch_with_log_check(reader, on_log)
    except RpcError:
        _drain_stream(reader)
        raise
    _drain_stream(reader)
    if not info.has_return:
        return None
    value = batch.batch.column("result")[0].as_py()
    _validate_result(info.name, value, info.result_type)
    if value is None:
        return None
    return _deserialize_value(value, info.result_type)


def _read_stream_response(
    reader: ipc.RecordBatchStreamReader,
    on_log: Callable[[Message], None] | None,
) -> StreamSession:
    """Wrap a response reader in a StreamSession."""
    return StreamSession(reader, on_log)


class StreamSession:
    """Client-side handle for a server stream call.

    Iterates over ``AnnotatedBatch`` objects from the server.
    Log batches are delivered to the ``on_log`` callback.
    """

    __slots__ = ("_reader", "_on_log")

    def __init__(
        self,
        reader: ipc.RecordBatchStreamReader,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        """Initialize with an IPC reader and optional log callback."""
        self._reader = reader
        self._on_log = on_log

    def __iter__(self) -> Iterator[AnnotatedBatch]:  # noqa: D105
        try:
            while True:
                try:
                    yield _read_batch_with_log_check(self._reader, self._on_log)
                except StopIteration:
                    break
        except RpcError:
            _drain_stream(self._reader)
            raise


class BidiSession:
    """Client-side handle for a bidi stream call.

    Call ``exchange(input)`` to send an input batch and receive the output batch.
    Log batches are delivered to the ``on_log`` callback.
    Supports context manager for resource cleanup.
    """

    __slots__ = ("_writer_stream", "_reader_stream", "_on_log", "_input_writer", "_output_reader", "_closed")

    def __init__(
        self,
        writer_stream: IOBase,
        reader_stream: IOBase,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        """Initialize with writer/reader streams and optional log callback."""
        self._writer_stream = writer_stream
        self._reader_stream = reader_stream
        self._on_log = on_log
        self._input_writer: ipc.RecordBatchStreamWriter | None = None
        self._output_reader: ipc.RecordBatchStreamReader | None = None
        self._closed = False

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch, receive the output batch.

        Log batches are delivered to on_log callback before returning.
        On RpcError, the session is automatically closed so the transport
        is clean for the next RPC call.
        """
        if self._input_writer is None:
            self._input_writer = ipc.new_stream(self._writer_stream, input.batch.schema)
        if input.custom_metadata is not None:
            self._input_writer.write_batch(input.batch, custom_metadata=input.custom_metadata)
        else:
            self._input_writer.write_batch(input.batch)

        if self._output_reader is None:
            self._output_reader = ipc.open_stream(self._reader_stream)

        try:
            return _read_batch_with_log_check(self._output_reader, self._on_log)
        except RpcError:
            self.close()
            raise

    def close(self) -> None:
        """Close input stream (signals EOS) and drain remaining output."""
        if self._closed:
            return
        self._closed = True
        if self._input_writer is not None:
            self._input_writer.close()
        else:
            with ipc.new_stream(self._writer_stream, _EMPTY_SCHEMA):
                pass
        if self._output_reader is None:
            self._output_reader = ipc.open_stream(self._reader_stream)
        with contextlib.suppress(StopIteration, RpcError):
            while True:
                _read_batch_with_log_check(self._output_reader, self._on_log)

    def __enter__(self) -> BidiSession:  # noqa: D105
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


def _send_request(writer: IOBase, info: RpcMethodInfo, kwargs: dict[str, Any]) -> None:
    """Merge defaults, validate, and write a request IPC stream."""
    merged = {**info.param_defaults, **kwargs}
    _validate_params(info.name, merged, info.param_types)
    _write_request(writer, info.name, info.params_schema, merged)


class _RpcProxy:
    """Dynamic proxy that implements RPC method calls through a transport."""

    def __init__(
        self,
        protocol: type,
        transport: RpcTransport,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        self._protocol = protocol
        self._transport = transport
        self._methods = rpc_methods(protocol)
        self._on_log = on_log

    def __getattr__(self, name: str) -> Any:
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
        transport = self._transport
        on_log = self._on_log

        def caller(**kwargs: object) -> object:
            _send_request(transport.writer, info, kwargs)
            reader = ipc.open_stream(transport.reader)
            return _read_unary_response(reader, info, on_log)

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., StreamSession]:
        transport = self._transport
        on_log = self._on_log

        def caller(**kwargs: object) -> StreamSession:
            _send_request(transport.writer, info, kwargs)
            reader = ipc.open_stream(transport.reader)
            return _read_stream_response(reader, on_log)

        return caller

    def _make_bidi_caller(self, info: RpcMethodInfo) -> Callable[..., BidiSession]:
        transport = self._transport
        on_log = self._on_log

        def caller(**kwargs: object) -> BidiSession:
            _send_request(transport.writer, info, kwargs)
            return BidiSession(transport.writer, transport.reader, on_log)

        return caller


# ---------------------------------------------------------------------------
# RpcConnection — typed context manager
# ---------------------------------------------------------------------------


class RpcConnection:
    """Context manager that provides a typed RPC proxy over a transport.

    Usage::

        with RpcConnection(MyProtocol, transport) as svc:
            result = svc.add(a=1, b=2)

    """

    __slots__ = ("_protocol", "_transport", "_on_log")

    def __init__(
        self,
        protocol: type,
        transport: RpcTransport,
        on_log: Callable[[Message], None] | None = None,
    ) -> None:
        """Initialize with a protocol type and transport."""
        self._protocol = protocol
        self._transport = transport
        self._on_log = on_log

    def __enter__(self) -> _RpcProxy:
        """Enter the context and return a typed proxy."""
        return _RpcProxy(self._protocol, self._transport, self._on_log)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the transport."""
        self._transport.close()


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
def connect(
    protocol: type,
    cmd: list[str],
    *,
    on_log: Callable[[Message], None] | None = None,
) -> Iterator[_RpcProxy]:
    """Connect to a subprocess RPC server.

    Context manager that spawns a subprocess, yields a typed proxy, and
    cleans up on exit.

    Args:
        protocol: The Protocol class defining the RPC interface.
        cmd: Command to spawn the subprocess worker.
        on_log: Optional callback for log messages from the server.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    transport = SubprocessTransport(cmd)
    try:
        with RpcConnection(protocol, transport, on_log=on_log) as proxy:
            yield proxy
    finally:
        transport.close()


@contextlib.contextmanager
def serve_pipe(
    protocol: type,
    implementation: object,
    *,
    on_log: Callable[[Message], None] | None = None,
) -> Iterator[_RpcProxy]:
    """Start an in-process pipe server and yield a typed client proxy.

    Useful for tests and demos — no subprocess needed.  A background thread
    runs ``RpcServer.serve()`` on the server side of a pipe pair.

    Args:
        protocol: The Protocol class defining the RPC interface.
        implementation: The implementation object.
        on_log: Optional callback for log messages from the server.

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    """
    client_transport, server_transport = make_pipe_pair()
    server = RpcServer(protocol, implementation)
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        with RpcConnection(protocol, client_transport, on_log=on_log) as proxy:
            yield proxy
    finally:
        client_transport.close()
        thread.join(timeout=5)


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
