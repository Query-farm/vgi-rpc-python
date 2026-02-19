"""Wire protocol read/write helpers and serialization."""

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Callable
from enum import Enum
from io import IOBase
from typing import TYPE_CHECKING, Any, cast, get_origin

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import (
    ExternalLocationConfig,
    maybe_externalize_batch,
    maybe_externalize_collector,
    resolve_external_location,
)
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import (
    LOG_EXTRA_KEY,
    LOG_LEVEL_KEY,
    LOG_MESSAGE_KEY,
    REQUEST_ID_KEY,
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
    SERVER_ID_KEY,
    SHM_SEGMENT_NAME_KEY,
    SHM_SEGMENT_SIZE_KEY,
    TRACEPARENT_KEY,
    TRACESTATE_KEY,
    encode_metadata,
)
from vgi_rpc.rpc._common import (
    _EMPTY_SCHEMA,
    RpcError,
    VersionError,
    _current_request_id,
    _current_request_metadata,
    _current_trace_headers,
    _record_input,
    _record_output,
)
from vgi_rpc.rpc._debug import (
    fmt_batch,
    fmt_kwargs,
    fmt_metadata,
    fmt_schema,
    wire_batch_logger,
    wire_request_logger,
    wire_response_logger,
)
from vgi_rpc.rpc._types import (
    AnnotatedBatch,
    RpcMethodInfo,
    _unwrap_annotated,
)
from vgi_rpc.shm import ShmSegment, maybe_write_to_shm, resolve_shm_batch
from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    IpcValidation,
    ValidatedReader,
    _is_optional_type,
    empty_batch,
)

if TYPE_CHECKING:
    from vgi_rpc.rpc._types import OutputCollector

# Best-effort trace context injection (no-op if OTel not installed)
try:
    from vgi_rpc.otel import _inject_trace_context
except ImportError:
    _inject_trace_context = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# IPC stream helpers
# ---------------------------------------------------------------------------


def _convert_for_arrow(val: object) -> object:
    """Convert a Python value for Arrow serialization.

    Inverse of ``_deserialize_value``.  Handles types that Arrow cannot
    serialize directly:

    - ArrowSerializableDataclass → bytes
    - Enum → .name (string)
    - frozenset → list
    - dict → list of tuples (for map types)
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


def _write_request(
    writer_stream: IOBase,
    method_name: str,
    params_schema: pa.Schema,
    kwargs: dict[str, Any],
    *,
    shm: ShmSegment | None = None,
) -> None:
    """Write a request as a complete IPC stream (schema + 1 batch + EOS).

    The batch's custom_metadata carries ``vgi_rpc.method`` (the method name)
    and ``vgi_rpc.request_version`` (the wire-protocol version).

    When *shm* is provided, the segment name and size are included in the
    metadata so the server can dynamically attach to the segment.
    """
    arrays: list[pa.Array[Any]] = []
    for f in params_schema:
        val = _convert_for_arrow(kwargs.get(f.name))
        arrays.append(pa.array([val], type=f.type))
    batch = pa.RecordBatch.from_arrays(arrays, schema=params_schema)
    md: dict[bytes, bytes] = {RPC_METHOD_KEY: method_name.encode(), REQUEST_VERSION_KEY: REQUEST_VERSION}
    if shm is not None:
        md[SHM_SEGMENT_NAME_KEY] = shm.name.encode()
        md[SHM_SEGMENT_SIZE_KEY] = str(shm.size).encode()
    if _inject_trace_context is not None:
        trace_meta = _inject_trace_context()
        if trace_meta is not None:
            md.update(trace_meta)
    custom_metadata = pa.KeyValueMetadata(md)
    if wire_request_logger.isEnabledFor(logging.DEBUG):
        wire_request_logger.debug(
            "Write request: method=%s, schema=%s, kwargs={%s}, metadata=%s",
            method_name,
            fmt_schema(params_schema),
            fmt_kwargs(kwargs),
            fmt_metadata(custom_metadata),
        )
    with ipc.new_stream(writer_stream, params_schema) as writer:
        writer.write_batch(batch, custom_metadata=custom_metadata)


def _write_message_batch(
    writer: ipc.RecordBatchStreamWriter,
    schema: pa.Schema,
    msg: Message,
    server_id: str | None = None,
) -> None:
    """Write a zero-row batch with Message metadata on an existing IPC stream writer."""
    md = msg.add_to_metadata()
    if server_id is not None:
        md[SERVER_ID_KEY.decode()] = server_id
    request_id = _current_request_id.get()
    if request_id:
        md[REQUEST_ID_KEY.decode()] = request_id
    custom_metadata = encode_metadata(md)
    batch = empty_batch(schema)
    _record_output(batch)
    writer.write_batch(batch, custom_metadata=custom_metadata)


def _write_error_batch(
    writer: ipc.RecordBatchStreamWriter,
    schema: pa.Schema,
    exc: BaseException,
    server_id: str | None = None,
) -> None:
    """Write error as zero-row batch (convenience wrapper)."""
    if wire_response_logger.isEnabledFor(logging.DEBUG):
        wire_response_logger.debug(
            "Write error batch: %s: %s",
            type(exc).__name__,
            str(exc)[:200],
        )
    _write_message_batch(writer, schema, Message.from_exception(exc), server_id=server_id)


def _write_error_stream(
    writer_stream: IOBase, schema: pa.Schema, exc: BaseException, server_id: str | None = None
) -> None:
    """Write a complete IPC stream containing just an error batch."""
    with ipc.new_stream(writer_stream, schema) as writer:
        _write_error_batch(writer, schema, exc, server_id=server_id)


class _ClientLogSink:
    """Buffers client-directed log messages until an IPC writer is available, then writes directly."""

    __slots__ = ("_buffer", "_schema", "_server_id", "_writer")

    def __init__(self, server_id: str | None = None) -> None:
        self._buffer: list[Message] = []
        self._writer: ipc.RecordBatchStreamWriter | None = None
        self._schema: pa.Schema | None = None
        self._server_id = server_id

    def __call__(self, msg: Message) -> None:
        if self._writer is not None and self._schema is not None:
            _write_message_batch(self._writer, self._schema, msg, server_id=self._server_id)
        else:
            self._buffer.append(msg)

    def flush_contents(self, writer: ipc.RecordBatchStreamWriter, schema: pa.Schema) -> None:
        """Flush buffered messages and switch to direct writing."""
        self._writer = writer
        self._schema = schema
        for msg in self._buffer:
            _write_message_batch(writer, schema, msg, server_id=self._server_id)
        self._buffer.clear()

    def reset(self) -> None:
        """Clear writer/schema references, reverting to buffer mode.

        Call this after the IPC writer the sink was flushed to has been
        closed (e.g. after a header stream ends) so that any subsequent
        log messages are buffered instead of written to a stale writer.
        """
        self._writer = None
        self._schema = None


def _write_result_batch(
    writer: ipc.RecordBatchStreamWriter,
    result_schema: pa.Schema,
    value: object,
    external_config: ExternalLocationConfig | None = None,
    *,
    shm: ShmSegment | None = None,
) -> None:
    """Write a unary result batch to an already-open IPC stream writer."""
    if len(result_schema) == 0:
        batch = pa.RecordBatch.from_pydict({}, schema=_EMPTY_SCHEMA)
    else:
        wire_value = _convert_for_arrow(value)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([wire_value], type=result_schema.field(0).type)], schema=result_schema
        )
    _record_output(batch)
    if shm is not None:
        batch, cm = maybe_write_to_shm(batch, None, shm)
        if cm is not None:
            if wire_response_logger.isEnabledFor(logging.DEBUG):
                wire_response_logger.debug("Write result batch: %s, route=shm", fmt_batch(batch))
            writer.write_batch(batch, custom_metadata=cm)
            return
    elif external_config is not None:
        batch, cm = maybe_externalize_batch(batch, None, external_config)
        if cm is not None:
            if wire_response_logger.isEnabledFor(logging.DEBUG):
                wire_response_logger.debug("Write result batch: %s, route=external", fmt_batch(batch))
            writer.write_batch(batch, custom_metadata=cm)
            return
    if wire_response_logger.isEnabledFor(logging.DEBUG):
        wire_response_logger.debug("Write result batch: %s, route=inline", fmt_batch(batch))
    writer.write_batch(batch)


def _read_request(
    reader_stream: IOBase, ipc_validation: IpcValidation = IpcValidation.FULL
) -> tuple[str, dict[str, Any]]:
    """Read a request IPC stream, return (method_name, kwargs).

    Extracts ``vgi_rpc.method`` and validates ``vgi_rpc.request_version``
    from the batch's custom_metadata.

    Raises:
        RpcError: If ``vgi_rpc.method`` is missing or if the request
            batch has a non-empty schema but ``num_rows != 1``.
        VersionError: If ``vgi_rpc.request_version`` is missing or
            does not match ``REQUEST_VERSION``.

    """
    reader = ValidatedReader(ipc.open_stream(reader_stream), ipc_validation)
    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
    _current_request_metadata.set(custom_metadata)
    _record_input(batch)
    if wire_request_logger.isEnabledFor(logging.DEBUG):
        wire_request_logger.debug(
            "Read request batch: %s, metadata=%s",
            fmt_batch(batch),
            fmt_metadata(custom_metadata),
        )
    method_name_bytes = custom_metadata.get(RPC_METHOD_KEY) if custom_metadata else None
    if method_name_bytes is None:
        raise RpcError(
            "ProtocolError",
            "Missing 'vgi_rpc.method' in request batch custom_metadata. "
            "Each request batch must carry a 'vgi_rpc.method' key in its Arrow IPC custom_metadata "
            "with the method name as a UTF-8 string.",
            "",
        )
    version_bytes = custom_metadata.get(REQUEST_VERSION_KEY) if custom_metadata else None
    if version_bytes is None:
        raise VersionError(
            "Missing 'vgi_rpc.request_version' in request batch custom_metadata. "
            f"Set the 'vgi_rpc.request_version' custom_metadata value to {REQUEST_VERSION!r}."
        )
    if version_bytes != REQUEST_VERSION:
        raise VersionError(
            f"Unsupported request version {version_bytes!r}, expected {REQUEST_VERSION!r}. "
            f"Set the 'vgi_rpc.request_version' custom_metadata value to {REQUEST_VERSION!r}."
        )
    method_name = method_name_bytes.decode()
    # Store trace context in contextvar for hook consumption (pipe/subprocess transport)
    tp = custom_metadata.get(TRACEPARENT_KEY) if custom_metadata else None
    if tp is not None:
        headers: dict[str, str] = {"traceparent": tp if isinstance(tp, str) else tp.decode()}
        ts = custom_metadata.get(TRACESTATE_KEY) if custom_metadata else None
        if ts is not None:
            headers["tracestate"] = ts if isinstance(ts, str) else ts.decode()
        _current_trace_headers.set(headers)
    _drain_stream(reader)
    if len(batch.schema) > 0 and batch.num_rows != 1:
        raise RpcError(
            "ProtocolError",
            f"Expected 1 row in request batch, got {batch.num_rows}. "
            f"Each parameter is a column (not a row). The batch should have exactly 1 row with schema "
            f"{fmt_schema(batch.schema)}.",
            "",
        )
    kwargs = {f.name: batch.column(i)[0].as_py() for i, f in enumerate(batch.schema)}
    if wire_request_logger.isEnabledFor(logging.DEBUG):
        wire_request_logger.debug(
            "Parsed request: method=%s, kwargs={%s}",
            method_name,
            fmt_kwargs(kwargs),
        )
    return method_name, kwargs


def _flush_collector(
    writer: ipc.RecordBatchStreamWriter,
    out: OutputCollector,
    external_config: ExternalLocationConfig | None = None,
    *,
    shm: ShmSegment | None = None,
) -> None:
    """Write all accumulated batches from an OutputCollector to an IPC stream writer."""
    # Record all logical batches for stats before any SHM/external transforms
    for ab in out.batches:
        _record_output(ab.batch)
    if wire_response_logger.isEnabledFor(logging.DEBUG):
        route_config = "shm" if shm is not None else ("external" if external_config is not None else "inline")
        wire_response_logger.debug(
            "Flush collector: batches=%d, route_config=%s",
            len(out.batches),
            route_config,
        )
    if shm is not None:
        # Log batches go to pipe; data batch goes to SHM if possible
        for ab in out.batches:
            batch, cm = maybe_write_to_shm(ab.batch, ab.custom_metadata, shm)
            if cm is not None:
                writer.write_batch(batch, custom_metadata=cm)
            else:
                writer.write_batch(batch)
    elif external_config is not None:
        batch_list = maybe_externalize_collector(out, external_config)
        for batch, cm in batch_list:
            if cm is not None:
                writer.write_batch(batch, custom_metadata=cm)
            else:
                writer.write_batch(batch)
    else:
        for ab in out.batches:
            if ab.custom_metadata is not None:
                writer.write_batch(ab.batch, custom_metadata=ab.custom_metadata)
            else:
                writer.write_batch(ab.batch)


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
        if wire_batch_logger.isEnabledFor(logging.DEBUG):
            wire_batch_logger.debug("Classify batch: rows=%d, no metadata -> data", batch.num_rows)
        return False
    if batch.num_rows != 0:
        if wire_batch_logger.isEnabledFor(logging.DEBUG):
            wire_batch_logger.debug(
                "Classify batch: rows=%d, metadata=%s -> data",
                batch.num_rows,
                fmt_metadata(custom_metadata),
            )
        return False
    level_bytes = custom_metadata.get(LOG_LEVEL_KEY)
    message_bytes = custom_metadata.get(LOG_MESSAGE_KEY)
    if level_bytes is None or message_bytes is None:
        if wire_batch_logger.isEnabledFor(logging.DEBUG):
            wire_batch_logger.debug("Classify batch: zero-row, no log keys -> data")
        return False

    level_str = level_bytes.decode()
    message_str = message_bytes.decode()

    # Extract extra info (traceback, exception_type, etc.)
    raw_extra_data: dict[str, object] = {}
    raw_extra = custom_metadata.get(LOG_EXTRA_KEY)
    if raw_extra is not None:
        with contextlib.suppress(json.JSONDecodeError):
            raw_extra_data = json.loads(raw_extra.decode())

    # Extract request_id from batch metadata
    request_id_bytes = custom_metadata.get(REQUEST_ID_KEY)
    request_id = ""
    if request_id_bytes is not None:
        request_id = request_id_bytes.decode() if isinstance(request_id_bytes, bytes) else request_id_bytes

    if wire_batch_logger.isEnabledFor(logging.DEBUG):
        wire_batch_logger.debug(
            "Classify batch: zero-row -> %s: %s",
            level_str,
            message_str[:200],
        )

    # EXCEPTION level → raise RpcError (existing behaviour)
    if level_str == Level.EXCEPTION.value:
        error_type = str(raw_extra_data.get("exception_type", level_str))
        traceback_str = str(raw_extra_data.get("traceback", ""))
        raise RpcError(error_type, message_str, traceback_str, request_id=request_id)

    # Non-exception log message → invoke callback
    # Coerce all extra values to str for Message(**extra)
    extra: dict[str, str] = {k: str(v) for k, v in raw_extra_data.items()}
    # Extract server_id from top-level metadata into extra
    server_id_bytes = custom_metadata.get(SERVER_ID_KEY)
    if server_id_bytes is not None:
        extra["server_id"] = server_id_bytes.decode() if isinstance(server_id_bytes, bytes) else server_id_bytes
    if request_id:
        extra["request_id"] = request_id
    msg = Message(Level(level_str), message_str, **extra)
    if on_log is not None:
        on_log(msg)
    return True


def _deserialize_value(value: object, type_hint: Any, ipc_validation: IpcValidation = IpcValidation.FULL) -> object:
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
        reader = ValidatedReader(ipc.open_stream(value), ipc_validation)
        batch, metadata = reader.read_next_batch_with_custom_metadata()
        return base.deserialize_from_batch(batch, metadata, ipc_validation=ipc_validation)
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


def _deserialize_params(
    kwargs: dict[str, Any], param_types: dict[str, Any], ipc_validation: IpcValidation = IpcValidation.FULL
) -> None:
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
        kwargs[name] = _deserialize_value(value, ptype, ipc_validation)


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


def _drain_stream(reader: ValidatedReader) -> None:
    """Consume remaining batches so the IPC EOS marker is read."""
    while True:
        try:
            reader.read_next_batch()
        except StopIteration:
            return


def _write_stream_header(
    dest: IOBase,
    header: ArrowSerializableDataclass | None,
    external_config: ExternalLocationConfig | None = None,
    sink: _ClientLogSink | None = None,
    *,
    method_name: str = "",
) -> None:
    """Write a stream header as a complete IPC stream (schema + 1-row batch + EOS).

    If *sink* is provided, any buffered log messages are flushed into the
    header IPC stream before the header data batch.

    If *external_config* is provided and the serialized header exceeds the
    externalization threshold, the header batch is uploaded to external
    storage and replaced with a zero-row pointer batch.

    Args:
        dest: The destination to write to.
        header: The header dataclass to serialize.  Must not be ``None``.
        external_config: Optional external storage configuration.
        sink: Optional client log sink to flush before writing the header batch.
        method_name: RPC method name, used in error messages.

    Raises:
        TypeError: If *header* is ``None``.

    """
    if header is None:
        raise TypeError(f"Method '{method_name}' declares header type but returned header=None")
    batch = header._serialize()
    _record_output(batch)
    batch, cm = maybe_externalize_batch(batch, None, external_config) if external_config else (batch, None)
    with ipc.new_stream(dest, batch.schema) as writer:
        if sink is not None:
            sink.flush_contents(writer, batch.schema)
        if cm is not None:
            writer.write_batch(batch, custom_metadata=cm)
        else:
            writer.write_batch(batch)
    if sink is not None:
        sink.reset()


def _read_stream_header(
    source: IOBase,
    header_type: type[ArrowSerializableDataclass],
    ipc_validation: IpcValidation,
    on_log: Callable[[Message], None] | None = None,
    external_config: ExternalLocationConfig | None = None,
) -> ArrowSerializableDataclass:
    """Read a stream header IPC stream and deserialize it.

    Also handles the case where the server wrote an error stream instead
    of a header (e.g. if the method raised during init).  If the header
    was externalized, resolves the pointer batch transparently.

    Args:
        source: The IO source to read from.
        header_type: The concrete header dataclass type.
        ipc_validation: Validation level for IPC batches.
        on_log: Optional log callback.
        external_config: Optional external storage configuration for
            resolving externalized headers.

    Returns:
        The deserialized header instance.

    Raises:
        RpcError: If the server wrote an error instead of a header.

    """
    reader = ValidatedReader(ipc.open_stream(source), ipc_validation)
    try:
        while True:
            batch, cm = reader.read_next_batch_with_custom_metadata()
            if not _dispatch_log_or_error(batch, cm, on_log):
                break
    except StopIteration:
        raise RpcError(
            "ProtocolError",
            "Stream header IPC stream ended without a data batch. "
            "The server must write at least one non-log batch in the header stream.",
            "",
        ) from None
    except RpcError:
        _drain_stream(reader)
        raise
    _drain_stream(reader)
    batch, cm = resolve_external_location(batch, cm, external_config, on_log, ipc_validation)
    return header_type.deserialize_from_batch(batch, cm, ipc_validation=ipc_validation)


def _send_request(
    writer: IOBase,
    info: RpcMethodInfo,
    kwargs: dict[str, Any],
    *,
    shm: ShmSegment | None = None,
) -> None:
    """Merge defaults, validate, and write a request IPC stream."""
    merged = {**info.param_defaults, **kwargs}
    if wire_request_logger.isEnabledFor(logging.DEBUG):
        wire_request_logger.debug(
            "Send request: method=%s, type=%s, defaults_applied=%s",
            info.name,
            info.method_type.value,
            sorted(set(info.param_defaults) - set(kwargs)),
        )
    _validate_params(info.name, merged, info.param_types)
    _write_request(writer, info.name, info.params_schema, merged, shm=shm)


def _read_batch_with_log_check(
    reader: ValidatedReader,
    on_log: Callable[[Message], None] | None = None,
    external_config: ExternalLocationConfig | None = None,
    *,
    shm: ShmSegment | None = None,
) -> AnnotatedBatch:
    """Read the next non-log batch, dispatching log batches to *on_log*.

    Loops internally, skipping zero-row log batches (via
    ``_dispatch_log_or_error``).  Returns the first data batch as an
    ``AnnotatedBatch``.  ``StopIteration`` and ``RpcError`` propagate
    to the caller.

    If *external_config* is provided and the batch is an external
    location pointer, resolves it by fetching the data from the URL.

    If *shm* is provided and the batch is a shared memory
    pointer, resolves it via zero-copy read from shared memory.
    """
    while True:
        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        if wire_response_logger.isEnabledFor(logging.DEBUG):
            wire_response_logger.debug(
                "Read batch: %s, metadata=%s",
                fmt_batch(batch),
                fmt_metadata(custom_metadata),
            )
        if not _dispatch_log_or_error(batch, custom_metadata, on_log):
            resolved_batch, resolved_cm = resolve_external_location(
                batch, custom_metadata, external_config, on_log, reader.ipc_validation
            )
            resolved_batch, resolved_cm, release_fn = resolve_shm_batch(resolved_batch, resolved_cm, shm)
            return AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm, _release_fn=release_fn)


def _read_unary_response(
    reader: ValidatedReader,
    info: RpcMethodInfo,
    on_log: Callable[[Message], None] | None,
    external_config: ExternalLocationConfig | None = None,
    *,
    shm: ShmSegment | None = None,
) -> object:
    """Read a unary response: skip logs, extract result, deserialize."""
    try:
        batch = _read_batch_with_log_check(reader, on_log, external_config, shm=shm)
    except RpcError:
        _drain_stream(reader)
        raise
    _drain_stream(reader)
    if not info.has_return:
        if wire_response_logger.isEnabledFor(logging.DEBUG):
            wire_response_logger.debug("Read unary response: method=%s, void return", info.name)
        return None
    value = batch.batch.column("result")[0].as_py()
    _validate_result(info.name, value, info.result_type)
    if wire_response_logger.isEnabledFor(logging.DEBUG):
        wire_response_logger.debug(
            "Read unary response: method=%s, result_type=%s",
            info.name,
            type(value).__name__,
        )
    if value is None:
        return None
    return _deserialize_value(value, info.result_type, reader.ipc_validation)
