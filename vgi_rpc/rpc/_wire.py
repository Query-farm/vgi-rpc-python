"""Wire protocol read/write helpers and serialization."""

from __future__ import annotations

import contextlib
import json
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
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
    SERVER_ID_KEY,
    encode_metadata,
)
from vgi_rpc.rpc._common import _EMPTY_SCHEMA, RpcError, VersionError
from vgi_rpc.rpc._types import (
    AnnotatedBatch,
    RpcMethodInfo,
    _unwrap_annotated,
)
from vgi_rpc.shm import ShmSegment, maybe_write_to_shm, resolve_shm_batch
from vgi_rpc.utils import ArrowSerializableDataclass, IpcValidation, ValidatedReader, _is_optional_type, empty_batch

if TYPE_CHECKING:
    from vgi_rpc.rpc._types import OutputCollector


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
    server_id: str | None = None,
) -> None:
    """Write a zero-row batch with Message metadata on an existing IPC stream writer."""
    md = msg.add_to_metadata()
    if server_id is not None:
        md[SERVER_ID_KEY.decode()] = server_id
    custom_metadata = encode_metadata(md)
    writer.write_batch(empty_batch(schema), custom_metadata=custom_metadata)


def _write_error_batch(
    writer: ipc.RecordBatchStreamWriter,
    schema: pa.Schema,
    exc: BaseException,
    server_id: str | None = None,
) -> None:
    """Write error as zero-row batch (convenience wrapper)."""
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
    if shm is not None:
        batch, cm = maybe_write_to_shm(batch, None, shm)
        if cm is not None:
            writer.write_batch(batch, custom_metadata=cm)
            return
    elif external_config is not None:
        batch, cm = maybe_externalize_batch(batch, None, external_config)
        if cm is not None:
            writer.write_batch(batch, custom_metadata=cm)
            return
    writer.write_batch(batch)


def _read_request(
    reader_stream: IOBase, ipc_validation: IpcValidation = IpcValidation.NONE
) -> tuple[str, dict[str, Any]]:
    """Read a request IPC stream, return (method_name, kwargs).

    Extracts ``vgi_rpc.method`` and validates ``vgi_rpc.request_version``
    from the batch's custom_metadata.

    Raises:
        RpcError: If ``vgi_rpc.method`` is missing.
        VersionError: If ``vgi_rpc.request_version`` is missing or
            does not match ``REQUEST_VERSION``.

    """
    reader = ValidatedReader(ipc.open_stream(reader_stream), ipc_validation)
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
    if len(batch.schema) > 0 and batch.num_rows != 1:
        raise RpcError("ProtocolError", f"Expected 1 row in request batch, got {batch.num_rows}", "")
    kwargs = {f.name: batch.column(i)[0].as_py() for i, f in enumerate(batch.schema)}
    return method_name, kwargs


def _flush_collector(
    writer: ipc.RecordBatchStreamWriter,
    out: OutputCollector,
    external_config: ExternalLocationConfig | None = None,
    *,
    shm: ShmSegment | None = None,
) -> None:
    """Write all accumulated batches from an OutputCollector to an IPC stream writer."""
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
    raw_extra_data: dict[str, object] = {}
    raw_extra = custom_metadata.get(LOG_EXTRA_KEY)
    if raw_extra is not None:
        with contextlib.suppress(json.JSONDecodeError):
            raw_extra_data = json.loads(raw_extra.decode())

    # EXCEPTION level → raise RpcError (existing behaviour)
    if level_str == Level.EXCEPTION.value:
        error_type = str(raw_extra_data.get("exception_type", level_str))
        traceback_str = str(raw_extra_data.get("traceback", ""))
        raise RpcError(error_type, message_str, traceback_str)

    # Non-exception log message → invoke callback
    # Coerce all extra values to str for Message(**extra)
    extra: dict[str, str] = {k: str(v) for k, v in raw_extra_data.items()}
    # Extract server_id from top-level metadata into extra
    server_id_bytes = custom_metadata.get(SERVER_ID_KEY)
    if server_id_bytes is not None:
        extra["server_id"] = server_id_bytes.decode() if isinstance(server_id_bytes, bytes) else server_id_bytes
    msg = Message(Level(level_str), message_str, **extra)
    if on_log is not None:
        on_log(msg)
    return True


def _deserialize_value(value: object, type_hint: Any, ipc_validation: IpcValidation = IpcValidation.NONE) -> object:
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
    kwargs: dict[str, Any], param_types: dict[str, Any], ipc_validation: IpcValidation = IpcValidation.NONE
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


def _send_request(writer: IOBase, info: RpcMethodInfo, kwargs: dict[str, Any]) -> None:
    """Merge defaults, validate, and write a request IPC stream."""
    merged = {**info.param_defaults, **kwargs}
    _validate_params(info.name, merged, info.param_types)
    _write_request(writer, info.name, info.params_schema, merged)


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

    If *shm* is provided and the batch is a shared memory
    pointer, resolves it via zero-copy read from shared memory.
    """
    while True:
        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
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
        return None
    value = batch.batch.column("result")[0].as_py()
    _validate_result(info.name, value, info.result_type)
    if value is None:
        return None
    return _deserialize_value(value, info.result_type, reader.ipc_validation)
