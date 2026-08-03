# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and server.

KEY FUNCTIONS
-------------
serialize_record_batch(destination, batch, custom_metadata) : Serialize to stream
deserialize_record_batch(data, ipc_validation) : Deserialize from bytes
read_single_record_batch(stream, context, ipc_validation) : Read and validate single batch
empty_batch(schema) : Create a zero-row batch from a schema
validate_batch(batch, schema) : Validate a batch against a schema

KEY CLASSES
-----------
ArrowSerializableDataclass : Mixin for dataclasses with automatic Arrow IPC serialization.
ArrowType : Protocol for custom Arrow type annotations.
IPCError : Exception raised on IPC communication errors
IpcValidation : Enum controlling batch validation level (NONE, FULL).
ValidatedReader : Wrapper around RecordBatchStreamReader with configurable validation.

"""

import os
import warnings
from collections.abc import Callable
from dataclasses import MISSING, Field, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum
from functools import lru_cache
from io import BytesIO, IOBase
from types import TracebackType, UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    NamedTuple,
    Protocol,
    Self,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

import pyarrow as pa
from pyarrow import ipc

__all__ = [
    "COMPACT_MARKER",
    "IPC_READ_OPTIONS",
    "IPC_WRITE_OPTIONS",
    "ArrowSerializableDataclass",
    "ArrowType",
    "Transient",
    "IPCError",
    "IpcValidation",
    "ValidatedReader",
    "deserialize_compact",
    "deserialize_record_batch",
    "empty_batch",
    "new_ipc_stream",
    "read_single_record_batch",
    "serialize_compact",
    "serialize_record_batch",
    "validate_batch",
]


@runtime_checkable
class _BytesSerializable(Protocol):
    """Protocol for objects that can serialize themselves to bytes."""

    def serialize_to_bytes(self) -> bytes: ...


class IPCError(Exception):
    """Error during IPC message reading or writing."""


class IpcValidation(Enum):
    """Level of validation applied to incoming IPC record batches.

    Attributes:
        NONE: No validation — batches are used as-is.
        STANDARD: Call ``batch.validate()`` to check schema/column consistency.
        FULL: Call ``batch.validate(full=True)`` to also verify data buffers.

    """

    NONE = "none"
    STANDARD = "standard"
    FULL = "full"

    @classmethod
    def from_env(cls, default: "IpcValidation | None" = None) -> "IpcValidation":
        """Resolve a validation level from ``VGI_RPC_IPC_VALIDATION``.

        Lets an operator trade validation for throughput at deploy time
        without a code change.  ``FULL`` walks every buffer of every
        incoming batch, which is real money on large Arrow payloads
        (measured at ~7% of HTTP server time), but it is also the
        defence against malformed attacker-supplied IPC — so the default
        stays ``FULL`` and lowering it is an explicit, deliberate act.

        Accepts ``none``, ``standard`` or ``full`` (case-insensitive).
        An unrecognised value warns and falls back to *default* rather
        than raising: the failure direction is *more* validation, never
        silently less than the operator believes they configured.

        Args:
            default: Level to use when the variable is unset or invalid.
                ``None`` means :attr:`FULL`.

        Returns:
            The resolved validation level.

        """
        fallback = cls.FULL if default is None else default
        raw = os.environ.get("VGI_RPC_IPC_VALIDATION", "").strip().lower()
        if not raw:
            return fallback
        try:
            return cls(raw)
        except ValueError:
            warnings.warn(
                f"VGI_RPC_IPC_VALIDATION={raw!r} is not one of "
                f"{', '.join(m.value for m in cls)}; using {fallback.value}",
                RuntimeWarning,
                stacklevel=2,
            )
            return fallback


def validate_batch(batch: pa.RecordBatch, ipc_validation: IpcValidation) -> None:
    """Validate a RecordBatch at the specified level.

    Args:
        batch: The batch to validate.
        ipc_validation: Validation level (NONE, STANDARD, or FULL).

    Raises:
        IPCError: If validation fails.

    """
    if ipc_validation is IpcValidation.NONE:
        return
    try:
        batch.validate(full=ipc_validation is IpcValidation.FULL)
    except pa.ArrowInvalid as exc:
        raise IPCError(f"IPC batch validation failed: {exc}") from exc


class ValidatedReader:
    """Wrapper around ``ipc.RecordBatchStreamReader`` that validates every batch on read.

    Proxies the subset of the reader API used by the RPC framework
    (``read_next_batch``, ``read_next_batch_with_custom_metadata``,
    ``schema``, and the context manager protocol).  Downstream code
    needs **zero changes** — just wrap ``ipc.open_stream(...)`` in
    ``ValidatedReader(..., ipc_validation)``.

    When *ipc_validation* is ``IpcValidation.NONE``, each read still
    delegates to the inner reader with minimal extra overhead.
    """

    __slots__ = ("_ipc_validation", "_reader")

    def __init__(self, reader: ipc.RecordBatchStreamReader, ipc_validation: IpcValidation) -> None:
        """Wrap *reader* so every batch is validated at *ipc_validation* level."""
        self._reader = reader
        self._ipc_validation = ipc_validation

    @property
    def ipc_validation(self) -> IpcValidation:
        """The validation level applied to every batch read."""
        return self._ipc_validation

    def read_next_batch(self) -> pa.RecordBatch:
        """Read the next batch, validating it before returning."""
        batch: pa.RecordBatch = self._reader.read_next_batch()
        validate_batch(batch, self._ipc_validation)
        return batch

    def read_next_batch_with_custom_metadata(self) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
        """Read the next batch with custom metadata, validating before returning."""
        batch, cm = self._reader.read_next_batch_with_custom_metadata()
        validate_batch(batch, self._ipc_validation)
        return batch, cm

    @property
    def schema(self) -> pa.Schema:
        """The schema of the underlying IPC stream."""
        return self._reader.schema

    def __enter__(self) -> Self:
        """Enter the context manager."""
        self._reader.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager."""
        self._reader.__exit__(exc_type, exc_val, exc_tb)  # type: ignore[no-untyped-call]


IPC_READ_OPTIONS = ipc.IpcReadOptions()
"""The framework's standard :class:`pyarrow.ipc.IpcReadOptions`.

Built once. With ``options=None`` pyarrow allocates a fresh one per reader
(``_ensure_default_ipc_read_options``), and we open readers per request.
Small on its own; free to avoid.
"""


@lru_cache(maxsize=512)
def _empty_batch_cached(schema: pa.Schema) -> pa.RecordBatch:
    """Memoized :func:`empty_batch`. See there for why sharing is safe."""
    return pa.RecordBatch.from_arrays(
        [_empty_array(field.type) for field in schema],
        schema=schema,
    )


def empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    """Return an empty batch conforming to the schema.

    Cached per schema. A zero-row batch is a pure function of its schema and
    Arrow batches are immutable, so one instance can be shared by every
    caller and every thread — nobody can write to it.

    Worth caching because it is per-request work whose cost scales with
    column count: measured 3.9us for one column but 33.9us for twelve, since
    each column needs its own zero-length array. The streaming path builds
    one per continuation turn as the token-carrying sentinel, so a wide
    output schema was paying that on every turn.

    Bounded rather than unbounded: schemas are not always fixed per class the
    way our other cache keys are — projection pushdown derives new ones per
    query shape — so this caps rather than growing without limit.
    """
    return _empty_batch_cached(schema)


def _empty_array(arrow_type: pa.DataType) -> "pa.Array[Any]":
    """Build a zero-length array of ``arrow_type``.

    ``pa.array([], type=...)`` raises ``ArrowNotImplementedError`` for union
    types and for extension types (including extension types nested inside
    map/list/struct). ``pa.nulls(0, type=...)`` has no such gaps — a
    length-0 array carries no actual nulls, so it is exactly an empty array
    of the type — and it handles arbitrarily nested types uniformly.
    """
    # pyarrow's nulls() stubs declare one overload per concrete DataType
    # subtype (Int8Type, StringType, …) but no overload for the abstract
    # ``DataType`` base — and we accept ``DataType`` because callers pass
    # arbitrary inferred Arrow types. The runtime behaviour is correct
    # for every concrete subtype, so the call is safe; the ignores just
    # silence mypy's overload-matching + no-any-return on the line.
    return pa.nulls(0, type=arrow_type)  # type: ignore[call-overload, no-any-return]  # ty: ignore[no-matching-overload]


def _build_write_options() -> ipc.IpcWriteOptions | None:
    """Resolve this process's standard IPC write options, once.

    Two reasons this exists rather than passing ``options=None`` everywhere:

    First, it is the single place to state what the framework's IPC output
    *is*.  Buffer-level compression stays off deliberately — the HTTP
    transport compresses whole bodies and whole state tokens, where the
    codec sees far more redundancy than it can find inside one column's
    buffers, and doing both would spend CPU twice for nothing.

    Second, it stops pyarrow re-deriving the same answer on every writer.
    When ``options`` is ``None``, ``RecordBatchStreamWriter.__init__`` calls
    ``_get_legacy_format_default``, which re-reads two environment variables
    and allocates a fresh options object every time — ~1.07us, of which
    ~0.78us is the ``os.environ`` lookups.  We build ~4 writers per HTTP
    stream turn, and those variables are process-global legacy escape
    hatches that cannot change under us.

    Returns ``None`` — deferring to pyarrow's own path — when either legacy
    escape hatch is set, rather than trying to mirror semantics we do not
    own.

    Returns:
        The shared options object, or ``None`` to let pyarrow decide.

    """
    if os.environ.get("ARROW_PRE_0_15_IPC_FORMAT", "0") != "0":
        return None
    if os.environ.get("ARROW_PRE_1_0_METADATA_VERSION", "0") != "0":
        return None
    return ipc.IpcWriteOptions(compression=None)


IPC_WRITE_OPTIONS: ipc.IpcWriteOptions | None = _build_write_options()
"""The framework's standard :class:`pyarrow.ipc.IpcWriteOptions`.

Shared by every IPC writer the framework opens.  See
:func:`_build_write_options` for what it settles and why it is resolved once.
"""


def new_ipc_stream(sink: Any, schema: pa.Schema) -> ipc.RecordBatchStreamWriter:
    """Open an Arrow IPC stream writer with the framework's standard options.

    Use this in place of ``pyarrow.ipc.new_stream`` throughout the framework
    so every stream we produce carries the same, stated options
    (:data:`IPC_WRITE_OPTIONS`) instead of whatever pyarrow re-derives per
    writer.  Signature-compatible with ``ipc.new_stream``: returns the writer,
    so it works both as a context manager and as a long-lived writer.

    Args:
        sink: Destination — a ``pa.NativeFile``, Python file object, or any
            other sink ``RecordBatchStreamWriter`` accepts.
        schema: Schema for the stream.

    Returns:
        A writer for the given sink.

    """
    return ipc.RecordBatchStreamWriter(sink, schema, options=IPC_WRITE_OPTIONS)


def serialize_record_batch(
    destination: IOBase,
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None = None,
) -> None:
    """Serialize a RecordBatch to an Arrow IPC stream.

    Uses RecordBatchStreamWriter to produce a complete IPC stream with
    schema, batch, and end-of-stream marker.

    Args:
        destination: The destination to write to (must support binary writes,
            e.g., stdout pipe, BufferedWriter).
        batch: The RecordBatch to serialize.
        custom_metadata: Optional additional metadata to include.

    """
    with ipc.RecordBatchStreamWriter(destination, batch.schema, options=IPC_WRITE_OPTIONS) as writer:
        writer.write_batch(batch, custom_metadata=custom_metadata)


def serialize_record_batch_bytes(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None = None,
) -> bytes:
    """Serialize a RecordBatch to bytes in Arrow IPC stream format.

    Uses RecordBatchStreamWriter to produce a complete IPC stream with
    schema, batch, and end-of-stream marker.

    Args:
        batch: The RecordBatch to serialize.
        custom_metadata: Optional additional metadata to include.

    Returns:
        Complete Arrow IPC stream bytes including EOS marker.

    """
    buffer = BytesIO()
    serialize_record_batch(buffer, batch, custom_metadata)
    return buffer.getvalue()


def deserialize_record_batch(
    data: bytes,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Deserialize bytes back to a RecordBatch with custom metadata.

    Args:
        data: Bytes containing a serialized RecordBatch in Arrow IPC stream format.
        ipc_validation: Validation level for the deserialized batch.

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If no batches are found in a (non-empty) IPC stream.

    """
    # A 0-length buffer means "empty" — a lenient producer (the Rust/Go/Java SDKs,
    # and what the DuckDB C++ client tolerates) may omit the IPC stream entirely for
    # an empty nested value (e.g. a scan function with no arguments) instead of
    # serializing a schema-only empty batch. Return a 0-column batch rather than
    # choking in open_stream ("was null or length 0").
    if not data:
        return pa.record_batch([], schema=pa.schema([])), None
    # Hand pyarrow the bytes directly: wrapping them in a BufferReader first
    # costs an extra object and sends pyarrow down its path-like probe
    # (_stringify_path) before it gets to the buffer. Measured 7.6us -> 5.7us.
    with ValidatedReader(ipc.open_stream(data, options=IPC_READ_OPTIONS), ipc_validation) as reader:
        try:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            # A schema-only stream (no batch message) is how lenient producers
            # (the Rust/Go/Java SDKs, tolerated by the DuckDB C++ client) encode an
            # empty value — e.g. a scan function with no arguments. Return a 0-row
            # batch of the declared schema instead of raising.
            schema = reader.schema
            empty = pa.RecordBatch.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)
            return empty, None

        return batch, custom_metadata


def read_single_record_batch(
    stream: IOBase,
    context: str = "batch",
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Read a single record batch from a stream.

    Args:
        stream: Stream to read from (must support binary reads, e.g., stdin pipe,
            BufferedReader).
        context: Description for error messages (e.g., "invocation", "init_input").
        ipc_validation: Validation level for the deserialized batch.

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If more than a single batch is found, no batches are found,
            or reading fails.

    """
    try:
        with ValidatedReader(ipc.open_stream(stream, options=IPC_READ_OPTIONS), ipc_validation) as reader:
            try:
                batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                raise IPCError(f"No record batch found in {context} stream") from None

            try:
                reader.read_next_batch()
            except StopIteration:
                return batch, custom_metadata

            raise IPCError(f"Expected single record batch in {context} stream, but found multiple batches")
    except IPCError:
        raise
    except Exception as e:
        raise IPCError(f"Error reading record batch from {context} stream: {e}") from e


def _validate_single_row_batch(
    data: pa.RecordBatch,
    class_name: str,
    required_fields: list[str] | None = None,
) -> dict[str, object]:
    """Validate a RecordBatch has exactly one row and return it as a dict.

    Args:
        data: The RecordBatch to validate.
        class_name: Name of the class being deserialized (for error messages).
        required_fields: Optional list of field names that must be present.

    Returns:
        The first (and only) row as a dictionary.

    Raises:
        ValueError: If the batch is empty, has multiple rows, or is missing
            required fields.

    """
    if data.num_columns == 0:
        # All-transient dataclass: no columns to read, return empty row.
        return {}
    if data.num_rows == 0:
        raise ValueError(f"Cannot deserialize {class_name} from empty RecordBatch")
    if data.num_rows > 1:
        raise ValueError(f"Expected single-row RecordBatch for {class_name} deserialization, got {data.num_rows} rows")

    # Column-at-a-time rather than ``to_pylist()[0]``.  ``to_pylist`` builds a
    # list of row dicts for the whole batch through pyarrow's general
    # converter; we want exactly one row, and reading each column's single
    # scalar directly skips that machinery. Measured 1.6-1.7x faster on real
    # payloads, with identical values. Driven off the *batch's* schema, not
    # the class's, so a batch carrying unknown or missing columns still reads
    # the same as before.
    first_row: dict[str, object] = {name: data.column(index)[0].as_py() for index, name in enumerate(data.schema.names)}

    if required_fields:
        found_fields = set(first_row.keys())
        missing = [f for f in required_fields if f not in found_fields]
        if missing:
            raise ValueError(f"Missing fields in {class_name} RecordBatch: {missing}. Found: {sorted(found_fields)}")

    return first_row


# =============================================================================
# ArrowSerializableDataclass - Auto-serialization mixin for dataclasses
# =============================================================================


@dataclass(frozen=True)
class ArrowType:
    """Annotation marker to specify explicit Arrow type for a field.

    Use with Annotated to override the default inferred Arrow type:

        @dataclass(frozen=True)
        class MyData(ArrowSerializableDataclass):
            # Override int64 → int32
            count: Annotated[int, ArrowType(pa.int32())]

            # Override for nested list of int32
            matrix: Annotated[
                list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))
            ]

    """

    arrow_type: pa.DataType


@dataclass(frozen=True)
class Transient:
    """Annotation marker to exclude a field from Arrow serialization.

    Transient fields exist on the Python dataclass but are not included in
    the Arrow schema, serialization, or deserialization. They must have a
    default value (either ``default`` or ``default_factory``).

    Use with Annotated to mark a field as transient:

        @dataclass(frozen=True)
        class MyData(ArrowSerializableDataclass):
            name: str
            _cache: Annotated[dict[str, int], Transient()] = field(
                default_factory=dict,
            )

    """


def _is_transient_field(field_type: object) -> bool:
    """Check if a field type annotation contains a Transient marker.

    Args:
        field_type: The resolved type annotation (must come from
            ``get_type_hints(cls, include_extras=True)``).

    Returns:
        True if the annotation is ``Annotated[T, Transient()]``.

    """
    if get_origin(field_type) is Annotated:
        for arg in get_args(field_type)[1:]:
            if isinstance(arg, Transient):
                return True
    return False


def _has_binary_arrow_type(field_type: object) -> bool:
    """Check if a field type annotation has an explicit ArrowType(pa.binary()).

    When a field is ``Annotated[SomeDataclass, ArrowType(pa.binary())]``, the
    value should be serialized to IPC bytes rather than a struct dict.
    """
    if get_origin(field_type) is Annotated:
        for arg in get_args(field_type)[1:]:
            if isinstance(arg, ArrowType) and arg.arrow_type == pa.binary():
                return True
    return False


class _FieldPlan(NamedTuple):
    """Precomputed per-field metadata used on the serialization hot paths."""

    name: str
    resolved_type: object
    """Annotation as resolved by ``get_type_hints`` (``Annotated`` preserved)."""
    unwrapped_type: object
    """``resolved_type`` with a top-level ``Annotated`` wrapper stripped."""
    transient: bool
    binary_dataclass: bool
    """True for ``Annotated[SomeDataclass, ArrowType(pa.binary())]`` fields."""
    default: object
    """The field default, or ``dataclasses.MISSING``."""
    default_factory: object
    """The field default factory, or ``dataclasses.MISSING``."""


class _SerializationPlan(NamedTuple):
    """Cached per-class serialization metadata (see ``_serialization_plan``)."""

    fields: tuple[_FieldPlan, ...]
    required_fields: list[str]
    """Non-transient fields without a default — must be present in a batch."""


def _serialization_plan(cls: "type[ArrowSerializableDataclass]") -> _SerializationPlan:
    """Return the cached per-class serialization plan, computing it on first use.

    ``_to_row_dict`` and ``deserialize_from_batch`` run once per RPC message,
    and resolving ``get_type_hints(include_extras=True)`` plus re-deriving the
    transient / required / binary-override facts per call dominated their cost.
    The plan is computed once per class and cached on the class itself
    (``cls.__dict__`` check, mirroring ``_ArrowSchemaDescriptor``) so
    subclasses never inherit a parent's cached plan.

    Args:
        cls: The ``ArrowSerializableDataclass`` subclass to plan for.

    Returns:
        The cached ``_SerializationPlan`` for exactly this class.

    """
    cached = cls.__dict__.get("_cached_serialization_plan")
    if cached is not None:
        return cast("_SerializationPlan", cached)

    # Use get_type_hints to resolve string annotations.
    # include_extras=True preserves Annotated[T, ...] for Transient detection.
    try:
        type_hints = get_type_hints(cls, include_extras=True)
    except Exception:
        type_hints = {f.name: f.type for f in dataclass_fields(cls)}

    field_plans: list[_FieldPlan] = []
    required_fields: list[str] = []
    for f in dataclass_fields(cls):
        field_type = type_hints.get(f.name, f.type)
        transient = _is_transient_field(field_type)
        unwrapped = field_type
        if get_origin(field_type) is Annotated:
            args = get_args(field_type)
            unwrapped = args[0] if args else field_type
        has_default = f.default is not MISSING or f.default_factory is not MISSING
        if not transient and not has_default:
            required_fields.append(f.name)
        field_plans.append(
            _FieldPlan(
                name=f.name,
                resolved_type=field_type,
                unwrapped_type=unwrapped,
                transient=transient,
                binary_dataclass=_has_binary_arrow_type(field_type),
                default=f.default,
                default_factory=f.default_factory,
            )
        )

    plan = _SerializationPlan(fields=tuple(field_plans), required_fields=required_fields)
    cls._cached_serialization_plan = plan
    return plan


class _RowEncoder(NamedTuple):
    """Per-class inputs for building a one-row RecordBatch (see ``_row_encoder``)."""

    schema: pa.Schema
    names: tuple[str, ...]
    types: tuple[pa.DataType, ...]
    nulls: tuple["pa.Array[Any]", ...]
    """A length-1 all-null array per column, reused for every unset field."""


def _row_encoder(cls: "type[ArrowSerializableDataclass]") -> _RowEncoder:
    """Return the cached per-class row encoder, computing it on first use.

    The schema, its column names and types, and a length-1 null array per
    column are all fixed once the class exists.  Arrow arrays are immutable,
    so one null array per column can be shared by every batch that class
    ever produces — including concurrently, across threads.

    Kept separate from :func:`_serialization_plan` because this one needs
    ``ARROW_SCHEMA``, and the plan deliberately does not: a class whose
    schema cannot be generated should still be able to resolve its field
    plan.

    Args:
        cls: The ``ArrowSerializableDataclass`` subclass to build for.

    Returns:
        The cached ``_RowEncoder`` for exactly this class.

    """
    cached = cls.__dict__.get("_cached_row_encoder")
    if cached is not None:
        return cast("_RowEncoder", cached)
    schema = cls.ARROW_SCHEMA
    types = tuple(field.type for field in schema)
    encoder = _RowEncoder(
        schema=schema,
        names=tuple(schema.names),
        types=types,
        nulls=tuple(pa.nulls(1, type=arrow_type) for arrow_type in types),
    )
    cls._cached_row_encoder = encoder
    return encoder


# ---------------------------------------------------------------------------
# Compact codec for flat dataclasses
#
# Arrow IPC is a columnar container: every stream carries a schema message, a
# batch message, an end-of-stream marker and 8-byte alignment padding. Those
# are fixed costs, and for a one-row record of scalars they dominate
# completely -- a two-int state measured 416 bytes and 36us to encode against
# 22 bytes and 0.5us for the same two integers via msgpack.
#
# That is the wrong tool for a *record*, and stream cursor states are records:
# a counter, an offset, an opaque blob. So flat dataclasses get msgpack
# instead, and anything Arrow is actually good at (nested dataclasses,
# RecordBatch, Schema) keeps the Arrow path.
#
# Why msgpack and not a hand-rolled positional encoding: the encoding was
# never the cost. Measured on the hot-path state, a bespoke positional codec
# came out at 1.67us per call against msgpack's 1.18us -- both dominated by
# _to_row_dict() rather than by writing the bytes, and the C dict packer beats
# a Python loop over fields anyway.
#
# What the bespoke encoding did cost was correctness: it derived field types
# from *annotations* and then trusted them, so a field annotated ``bytes``
# that _to_row_dict() rendered as a ``str`` (an Enum member is emitted as its
# .name) was written as the wrong type. msgpack encodes what the value
# actually is, which removes that whole failure mode.
#
# Fields are written as a msgpack *map*, not an array. An array is 12 bytes
# smaller here (26 B vs 38 B for the hot state) but positional again, so it
# would need the field-layout fingerprint back to keep a class that gains or
# loses a field from silently misparsing older payloads. Names cost a few
# bytes on a blob that is then zstd-compressed and AEAD-sealed, against
# states measured in the 11-12 KB range; drift-tolerance is worth more.
#
# Deliberately NOT a general replacement for serialize_to_bytes(): that method
# is a published wire contract used for catalog opaque data and nested fields.
# This is opt-in, for callers that own both ends of their bytes -- today the
# HTTP state token, which is server-minted, opaque, and short-lived.
# ---------------------------------------------------------------------------

COMPACT_MARKER = b"\x01"
"""First byte of a compact payload.

Distinguishable from the alternatives by construction: an Arrow IPC stream
starts with 0xFF (continuation indicator) and the state token's union
envelope starts with 0x00, so a reader can dispatch on one byte without
being told which encoding it was handed.
"""

try:
    import msgpack

    _HAVE_MSGPACK = True
except ImportError:  # pragma: no cover - exercised by installs without [http]
    # msgpack ships in the ``http`` extra, since the state token is its only
    # caller. Core stays importable without it; the codec simply reports
    # "not applicable" and every caller falls back to Arrow.
    _HAVE_MSGPACK = False

#: Field types the compact codec will claim, mapped to what a value of that
#: type must actually look like at runtime. Everything else -- nested
#: dataclasses, ``pa.RecordBatch``, ``pa.Schema``, lists, dicts, enums --
#: falls back to Arrow, which is what those are for.
#:
#: ``float`` accepts ``int`` because msgpack and Arrow both widen it; ``int``
#: accepts ``bool`` because ``bool`` is a subclass of ``int``.
_COMPACT_TYPES: dict[object, type | tuple[type, ...]] = {
    bytes: (bytes, bytearray, memoryview),
    str: str,
    int: int,
    float: (float, int),
    bool: bool,
}


class _CompactField(NamedTuple):
    """One non-transient field's compact-codec layout."""

    name: str
    field_type: object
    """The declared (Optional-unwrapped) annotation.

    Decoding passes anything unexpected back through
    :meth:`ArrowSerializableDataclass._convert_value_for_deserialization`,
    the same conversion the Arrow path applies.
    """
    runtime: type | tuple[type, ...]
    """What the value must actually be for this codec to claim it.

    The plan comes from *annotations*, but the values come from
    ``_to_row_dict()``, which applies conversions the annotation does not
    describe -- an ``Enum`` member is emitted as its ``.name`` string
    whatever its mixin type, so a field annotated ``bytes`` holding a
    ``class Ns(bytes, Enum)`` member arrives as ``str``. Such a class is
    mis-annotated and Arrow mangles it too (to ``b"ALPHA"``, not the
    member's real value), but the two codecs mangle it *differently*, and a
    state object must not depend on which transport carried it. So a
    divergence here hands the whole object to Arrow rather than guessing.
    """
    exact: type
    """The base scalar type, for the decode fast path.

    ``serialize_compact`` only emits a value whose type satisfied
    :attr:`runtime`, and msgpack round-trips all five base types faithfully,
    so a decoded value of exactly this type provably needs no conversion --
    which matters, because that conversion measured 1.21us per field against
    0.14us for the whole unpack. An exact ``type() is`` check rather than
    ``isinstance`` keeps the subclass cases (``bool`` under ``int``) on the
    slow, Arrow-identical path instead of guessing at them here.
    """


class _CompactPlan(NamedTuple):
    """Per-class layout for the compact codec, or absent when unsupported."""

    fields: tuple[_CompactField, ...]
    """Layout for each non-transient field."""
    transient_defaults: tuple[tuple[str, object, object], ...]
    """``(name, default, default_factory)`` for transient fields."""


def _compact_plan(cls: "type[ArrowSerializableDataclass]") -> "_CompactPlan | None":
    """Return the compact layout for ``cls``, or ``None`` if it needs Arrow.

    Args:
        cls: The dataclass to plan for.

    Returns:
        The cached plan, or ``None`` when the class is not flat.

    """
    if "_cached_compact_plan" in cls.__dict__:
        return cast("_CompactPlan | None", cls.__dict__["_cached_compact_plan"])

    fields: list[_CompactField] = []
    transient: list[tuple[str, object, object]] = []
    supported = _HAVE_MSGPACK
    if supported:
        for field_plan in _serialization_plan(cls).fields:
            if field_plan.transient:
                transient.append((field_plan.name, field_plan.default, field_plan.default_factory))
                continue
            inner, _nullable = _is_optional_type(field_plan.unwrapped_type)
            runtime = _COMPACT_TYPES.get(inner)
            if runtime is None:
                supported = False
                break
            fields.append(
                _CompactField(
                    name=field_plan.name,
                    field_type=field_plan.unwrapped_type,
                    runtime=runtime,
                    exact=cast("type", inner),
                )
            )

    plan: _CompactPlan | None = None
    if supported:
        plan = _CompactPlan(fields=tuple(fields), transient_defaults=tuple(transient))
    cls._cached_compact_plan = plan
    return plan


def serialize_compact(obj: "ArrowSerializableDataclass") -> bytes | None:
    """Encode ``obj`` with the compact codec, or ``None`` if it is not flat.

    Args:
        obj: The dataclass instance to encode.

    Returns:
        ``COMPACT_MARKER`` + a msgpack map of the row dict, or ``None`` when
        the caller should fall back to :meth:`serialize_to_bytes`.

    """
    plan = _compact_plan(type(obj))
    if plan is None:
        return None
    # Read through _to_row_dict() rather than getattr: subclasses override it
    # to derive serialized fields from transient ones (VGI's stream states
    # pack their user state into a bytes field there), and reading attributes
    # directly would silently skip that and emit stale values -- a cursor that
    # never advances, not an error. For a flat class the conversion inside is
    # the identity fast path, so this costs nothing.
    row = obj._to_row_dict()
    for field in plan.fields:
        value = row.get(field.name)
        if value is not None and not isinstance(value, field.runtime):
            # Mis-annotated field (see _CompactPlan.fields). Defer to Arrow so
            # the object does not depend on which codec encoded it.
            return None
    try:
        packed = msgpack.packb(row, use_bin_type=True)
    except (TypeError, ValueError):
        # Belt and braces: the checks above cover the declared fields, but
        # _to_row_dict() is overridable and may add keys the plan never saw.
        return None
    return COMPACT_MARKER + cast("bytes", packed)


def deserialize_compact(cls: "type[ArrowSerializableDataclass]", data: bytes) -> Any:
    """Decode a payload produced by :func:`serialize_compact`.

    Args:
        cls: The dataclass to rebuild.
        data: The compact payload, marker byte included.

    Returns:
        The reconstructed instance.

    Raises:
        IPCError: The class is not flat, or the payload is malformed.

    """
    plan = _compact_plan(cls)
    if plan is None:
        msg = f"{cls.__name__} has no compact layout"
        raise IPCError(msg)
    if not data or data[:1] != COMPACT_MARKER:
        msg = f"Malformed compact payload for {cls.__name__}"
        raise IPCError(msg)
    try:
        row = msgpack.unpackb(data[1:], raw=False)
    except Exception as exc:
        msg = f"Malformed compact payload for {cls.__name__}: {exc}"
        raise IPCError(msg) from exc
    if not isinstance(row, dict):
        msg = f"Compact payload for {cls.__name__} is not a map"
        raise IPCError(msg)

    kwargs: dict[str, Any] = {}
    for field in plan.fields:
        # A field absent from the map is a class that gained a field since the
        # payload was written; leaving it out lets the dataclass default apply,
        # which is how the Arrow path treats a missing column too.
        if field.name not in row:
            continue
        value = row[field.name]
        kwargs[field.name] = (
            value if type(value) is field.exact else cls._convert_value_for_deserialization(value, field.field_type)
        )

    for name, default, factory in plan.transient_defaults:
        if default is not MISSING:
            kwargs[name] = default
        elif factory is not MISSING:
            kwargs[name] = cast("Callable[[], object]", factory)()
    return cls(**kwargs)


def _is_optional_type(python_type: object) -> tuple[object, bool]:
    """Check if a type is Optional (X | None) and extract the inner type.

    Args:
        python_type: The type annotation to check.

    Returns:
        Tuple of (inner_type, is_nullable). If nullable, inner_type is the
        non-None type. If not nullable, inner_type is the original type.

    """
    origin = get_origin(python_type)
    args = get_args(python_type)

    # Handle X | None (UnionType) or Optional[X] (Union[X, None])
    if origin is UnionType or origin is Union:
        non_none_types = [t for t in args if t is not type(None)]
        if len(non_none_types) == 1 and len(args) == 2:
            return non_none_types[0], True

    return python_type, False


def _infer_arrow_type(python_type: object) -> pa.DataType:
    """Infer Arrow type from Python type annotation.

    Supports:
    - Basic types: str, bytes, int, float, bool
    - Generic types: list[T], dict[K, V], frozenset[T]
    - NewType: auto-unwraps to underlying type
    - Enum: serializes as dictionary-encoded string
    - ArrowSerializableDataclass: serializes as struct

    Not supported:
    - tuple: Arrow has no native heterogeneous-tuple type. Use a nested
      dataclass (``ArrowSerializableDataclass``) for fixed, named fields, or
      ``list[T]`` for homogeneous sequences.

    For other complex types not supported here, use Annotated[T, ArrowType(...)].

    Args:
        python_type: Python type annotation.

    Returns:
        Corresponding PyArrow DataType.

    Raises:
        TypeError: If the type cannot be automatically inferred.

    """
    # Handle Optional types by extracting the inner type
    inner_type, _ = _is_optional_type(python_type)
    if inner_type is not python_type:
        return _infer_arrow_type(inner_type)

    # Handle Annotated[T, ArrowType(...)] — extract explicit type or unwrap
    if get_origin(python_type) is Annotated:
        args = get_args(python_type)
        for arg in args[1:]:
            if isinstance(arg, ArrowType):
                return arg.arrow_type
        # No ArrowType found — recurse with the base type
        return _infer_arrow_type(args[0])

    # Handle NewType - unwrap to underlying type
    # NewType creates a callable with __supertype__ attribute
    if hasattr(python_type, "__supertype__"):
        return _infer_arrow_type(getattr(python_type, "__supertype__"))  # noqa: B009

    # Handle Enum - serialize as dictionary-encoded string
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        return pa.dictionary(pa.int16(), pa.string())

    # Handle ArrowSerializableDataclass - serialize as struct
    if hasattr(python_type, "ARROW_SCHEMA") and isinstance(getattr(python_type, "ARROW_SCHEMA", None), pa.Schema):
        # Convert schema fields to struct type using pa.field tuples
        arrow_schema: pa.Schema = getattr(python_type, "ARROW_SCHEMA")  # noqa: B009
        struct_fields = [pa.field(f.name, f.type, nullable=f.nullable) for f in arrow_schema]
        return pa.struct(struct_fields)

    origin = get_origin(python_type)
    args = get_args(python_type)

    # Handle list[T] -> pa.list_(T)
    if origin is list:
        if args:
            element_type = _infer_arrow_type(args[0])
            return pa.list_(element_type)
        return pa.list_(pa.string())  # Default to list of strings

    # Handle dict[K, V] -> pa.map_(K, V)
    if origin is dict:
        if len(args) >= 2:
            key_type = _infer_arrow_type(args[0])
            value_type = _infer_arrow_type(args[1])
            return pa.map_(key_type, value_type)
        return pa.map_(pa.string(), pa.string())  # Default

    # Handle frozenset[T] -> pa.list_(T) (serialize as list)
    if origin is frozenset:
        if args:
            element_type = _infer_arrow_type(args[0])
            return pa.list_(element_type)
        return pa.list_(pa.string())

    # Types serialized as binary (IPC bytes)
    if python_type is pa.RecordBatch or python_type is pa.Schema:
        return pa.binary()

    # Simple type mappings
    type_map: dict[type, pa.DataType] = {
        str: pa.string(),
        bytes: pa.binary(),
        int: pa.int64(),
        float: pa.float64(),
        bool: pa.bool_(),
    }

    if isinstance(python_type, type) and python_type in type_map:
        return type_map[python_type]

    # Provide a targeted hint for tuple, which is a common attempt
    if python_type is tuple or origin is tuple:
        raise TypeError(
            f"Cannot infer Arrow type for: {python_type}. "
            f"Arrow has no native heterogeneous-tuple type. Use a nested "
            f"ArrowSerializableDataclass for fixed named fields, or list[T] "
            f"for homogeneous sequences."
        )

    raise TypeError(
        f"Cannot infer Arrow type for: {python_type}. "
        f"Use Annotated[T, ArrowType(...)] to specify the Arrow type explicitly."
    )


class _ArrowSchemaDescriptor:
    """Descriptor that lazily generates ARROW_SCHEMA on first access.

    This is needed because the @dataclass decorator runs AFTER __init_subclass__,
    so __dataclass_fields__ isn't available when __init_subclass__ is called.
    This descriptor generates the schema on first access, when @dataclass has
    already processed the class.
    """

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def __get__(self, instance: object | None, owner: type["ArrowSerializableDataclass"]) -> pa.Schema:
        # Check if schema is already cached on the class
        cache_attr = f"_cached_{self._name}"
        if cache_attr in owner.__dict__:
            cached: pa.Schema = getattr(owner, cache_attr)
            return cached

        # Generate schema from dataclass fields
        schema = self._generate_schema(owner)

        # Cache on the class (not the descriptor)
        setattr(owner, cache_attr, schema)
        return schema

    def _generate_schema(self, cls: type["ArrowSerializableDataclass"]) -> pa.Schema:
        """Generate ARROW_SCHEMA from dataclass field annotations."""
        arrow_fields: list[pa.Field[pa.DataType]] = []
        overrides = getattr(cls, "_ARROW_FIELD_OVERRIDES", {})

        # Use get_type_hints to resolve string annotations
        # include_extras=True preserves Annotated[T, ...] wrappers
        try:
            type_hints = get_type_hints(cls, include_extras=True)
        except Exception:
            # Fallback to field.type if get_type_hints fails
            type_hints = {f.name: f.type for f in dataclass_fields(cls)}

        for field in dataclass_fields(cls):
            field_name = field.name
            field_type = type_hints.get(field_name, field.type)

            # Skip transient fields — they don't appear in the Arrow schema
            if _is_transient_field(field_type):
                has_default = field.default is not MISSING or field.default_factory is not MISSING
                if not has_default:
                    raise TypeError(
                        f"Transient field {cls.__name__}.{field_name} must have a default value or default_factory"
                    )
                continue

            # Check for explicit ClassVar override (legacy support)
            if field_name in overrides:
                arrow_type = overrides[field_name]
                _, nullable = _is_optional_type(field_type)
                arrow_fields.append(pa.field(field_name, arrow_type, nullable=nullable))
                continue

            # Infer Arrow type from Python type (handles Annotated[T, ArrowType(...)] internally)
            _, nullable = _is_optional_type(field_type)
            try:
                arrow_type = _infer_arrow_type(field_type)
                arrow_fields.append(pa.field(field_name, arrow_type, nullable=nullable))
            except TypeError as e:
                raise TypeError(f"Cannot generate Arrow schema for {cls.__name__}.{field_name}: {e}") from e

        return pa.schema(arrow_fields)


class ArrowSerializableDataclass:
    """Mixin for dataclasses with automatic Arrow IPC serialization.

    Provides automatic schema generation and serialization/deserialization
    for frozen dataclasses. The ARROW_SCHEMA is auto-generated from field
    type annotations.

    Auto-detected types:
    - Basic types: str, bytes, int, float, bool
    - Generic types: list[T], dict[K, V], frozenset[T]
    - NewType: unwraps to underlying type (e.g., NewType("Id", bytes) -> binary)
    - Enum: serializes as dictionary-encoded string via .name
    - ArrowSerializableDataclass: serializes as struct

    Not supported:
    - tuple: Arrow has no native heterogeneous-tuple type. Use a nested
      dataclass (``ArrowSerializableDataclass``) for fixed, named fields, or
      ``list[T]`` for homogeneous sequences.

    Optional fields (annotated with `| None`) are marked as nullable.
    To override specific field types, use Annotated with ArrowType.

    Attributes:
        ARROW_SCHEMA: Auto-generated Arrow schema from field annotations.

    """

    # Declare dataclass protocol attribute so dataclass_fields() accepts our mixin.
    # Actual value is set by @dataclass on subclasses.
    if TYPE_CHECKING:
        __dataclass_fields__: ClassVar[dict[str, Field[Any]]]

    # Auto-generated from field annotations on first access
    ARROW_SCHEMA: ClassVar[pa.Schema] = _ArrowSchemaDescriptor()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

    # Optional: explicit Arrow type overrides for complex fields
    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {}

    # Per-class serialization plan cache, populated lazily by
    # _serialization_plan(). Each subclass gets its own entry (the lookup
    # checks cls.__dict__, never this inherited default).
    _cached_serialization_plan: ClassVar[_SerializationPlan | None] = None

    # Per-class row-encoder cache, populated lazily by _row_encoder().
    # Same cls.__dict__ discipline as above.
    _cached_row_encoder: ClassVar["_RowEncoder | None"] = None

    # Per-class compact-codec layout, populated lazily by _compact_plan().
    # None means "not flat, use Arrow"; the key's presence in cls.__dict__ is
    # what distinguishes "computed, unsupported" from "not yet computed".
    _cached_compact_plan: ClassVar["_CompactPlan | None"] = None

    def _to_row_dict(self) -> dict[str, object]:
        """Convert instance to a dictionary for Arrow batch construction.

        Handles special type conversions:
        - pa.Schema -> bytes (via serialize())
        - pa.RecordBatch -> bytes (via IPC stream)
        - ArrowSerializableDataclass -> dict (serialize nested dataclass)
        - _BytesSerializable -> bytes (objects with serialize_to_bytes())
        - Enum -> .name (serialize as the enum member's name)
        - frozenset -> list (Arrow doesn't support sets)
        - dict -> list of tuples (for map types)
        - list elements -> recursively converted

        """
        row: dict[str, object] = {}
        for field_plan in _serialization_plan(type(self)).fields:
            if field_plan.transient:
                continue
            value = getattr(self, field_plan.name)
            # If the field has ArrowType(pa.binary()) and the value is an
            # ArrowSerializableDataclass, serialize to IPC bytes instead of
            # converting to a struct dict.
            if field_plan.binary_dataclass and isinstance(value, ArrowSerializableDataclass):
                value = value.serialize_to_bytes()
            else:
                value = self._convert_value_for_serialization(value)
            row[field_plan.name] = value
        return row

    def _convert_value_for_serialization(self, value: object) -> object:
        """Convert a value for Arrow serialization."""
        if value is None:
            return None

        # Fast path: plain scalars pass through untouched. Exact type checks
        # (not isinstance) so subclasses — notably Enum members, which subclass
        # str/int — still take the full dispatch below.
        value_type = type(value)
        if value_type is str or value_type is int or value_type is float or value_type is bool or value_type is bytes:
            return value

        # Handle pa.Schema -> serialize to bytes
        if isinstance(value, pa.Schema):
            return value.serialize().to_pybytes()

        # Handle pa.RecordBatch -> serialize to bytes
        if isinstance(value, pa.RecordBatch):
            sink = pa.BufferOutputStream()
            with new_ipc_stream(sink, value.schema) as writer:
                writer.write_batch(value)
            return sink.getvalue().to_pybytes()

        # Handle nested ArrowSerializableDataclass -> dict (must precede the
        # generic serialize_to_bytes check because every ASDataclass has that method,
        # but struct fields need a dict, not IPC bytes).
        if isinstance(value, ArrowSerializableDataclass):
            return value._to_row_dict()

        # Handle objects with serialize_to_bytes() method
        if isinstance(value, _BytesSerializable):
            return value.serialize_to_bytes()

        # Handle Enum -> .name (string representation of the enum member)
        if isinstance(value, Enum):
            return value.name

        # Handle frozenset -> list
        if isinstance(value, frozenset):
            return [self._convert_value_for_serialization(v) for v in value]

        # Handle dict -> list of tuples for Arrow map type (recursively convert keys and values)
        if isinstance(value, dict):
            return [
                (self._convert_value_for_serialization(k), self._convert_value_for_serialization(v))
                for k, v in value.items()
            ]

        # Handle list - recursively convert elements
        if isinstance(value, list):
            return [self._convert_value_for_serialization(v) for v in value]

        return value

    def _serialize(self) -> pa.RecordBatch:
        """Serialize this instance to a single-row RecordBatch.

        Builds the row column-by-column rather than through
        ``RecordBatch.from_pylist``.  ``from_pylist`` is pyarrow's fully
        general Python-to-Arrow converter — it infers per value, walks
        every column through the same machinery, and for a one-row batch
        that machinery costs far more than the conversion.  Going per
        column lets unset fields reuse a cached all-null array
        (:func:`_row_encoder`) instead of converting ``None`` through the
        general path, which matters because a wire dataclass is mostly
        unset in practice: 15 of ``InitRequest``'s 17 columns on a plain
        table scan.

        Measured on real payloads: 3.8x faster for ``InitRequest``, 2.4x
        for ``BindRequest``, with byte-identical output.

        Returns:
            A pa.RecordBatch containing one row with the instance's field values.

        """
        row_dict = self._to_row_dict()
        encoder = _row_encoder(type(self))
        arrays: list[pa.Array[Any]] = []
        for index, name in enumerate(encoder.names):
            value = row_dict.get(name)
            if value is None:
                arrays.append(encoder.nulls[index])
            else:
                arrays.append(pa.array([value], type=encoder.types[index]))
        return pa.RecordBatch.from_arrays(arrays, schema=encoder.schema)

    def serialize(self, dest: IOBase) -> None:
        """Serialize this instance to an Arrow IPC stream.

        Args:
            dest: The destination to write to (must support binary writes,
                e.g., stdout pipe, BufferedWriter).

        """
        serialize_record_batch(dest, self._serialize())

    def serialize_to_bytes(self) -> bytes:
        """Serialize this instance to Arrow IPC bytes.

        Returns:
            Arrow IPC stream bytes containing a single-row RecordBatch.

        """
        return serialize_record_batch_bytes(self._serialize())

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
        *,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> Self:
        """Deserialize an instance from an Arrow RecordBatch.

        Args:
            batch: Single-row RecordBatch containing the serialized data.
            custom_metadata: Optional metadata from the batch (unused,
                reserved for subclass overrides).
            ipc_validation: Validation level for nested IPC batches.

        Returns:
            Deserialized instance of this class.

        Raises:
            ValueError: If the batch is invalid (wrong row count or missing fields).
            TypeError: If a field value has an unexpected type during conversion.
            KeyError: If an Enum name cannot be resolved.

        """
        # Cached per-class plan: resolved type hints, transient flags, and the
        # required-field list (non-transient fields without defaults; fields
        # with defaults or default_factory are optional for compatibility).
        plan = _serialization_plan(cls)

        # Validate and extract row
        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=plan.required_fields,
        )

        # Convert values back to expected Python types
        kwargs: dict[str, Any] = {}
        for field_plan in plan.fields:
            name = field_plan.name

            # Transient fields are not in the batch — use their default value
            if field_plan.transient:
                if field_plan.default is not MISSING:
                    kwargs[name] = field_plan.default
                elif field_plan.default_factory is not MISSING:
                    factory = cast("Callable[[], object]", field_plan.default_factory)
                    kwargs[name] = factory()
                continue

            # Check if field is present in the row
            if name not in row:
                # Use default if available (for backward compatibility)
                if field_plan.default is not MISSING:
                    kwargs[name] = field_plan.default
                elif field_plan.default_factory is not MISSING:
                    factory = cast("Callable[[], object]", field_plan.default_factory)
                    kwargs[name] = factory()
                # If no default, it would have been caught by validate_single_row_batch
                continue

            value = row.get(name)

            # Convert value based on field type (Annotated already unwrapped in the plan)
            kwargs[name] = cls._convert_value_for_deserialization(value, field_plan.unwrapped_type, ipc_validation)

        return cls(**kwargs)

    @classmethod
    def _convert_value_for_deserialization(
        cls, value: object, field_type: object, ipc_validation: IpcValidation = IpcValidation.FULL
    ) -> object:
        """Convert a deserialized value back to the expected Python type."""
        if value is None:
            return None

        # Unwrap Optional type
        inner_type, _ = _is_optional_type(field_type)

        # Handle pa.Schema reconstruction from bytes
        if inner_type is pa.Schema:
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for pa.Schema deserialization, got {type(value).__name__}")
            # A 0-length buffer means "absent/empty" — a genuinely empty schema still
            # serializes to a full IPC schema message, so an empty buffer only comes
            # from a producer that skipped the field. Tolerate it (matches the DuckDB
            # C++ client) rather than choke in read_schema ("was null or length 0").
            if len(value) == 0:
                return None
            return pa.ipc.read_schema(pa.py_buffer(value))

        # Handle pa.RecordBatch reconstruction from bytes
        if inner_type is pa.RecordBatch:
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for pa.RecordBatch deserialization, got {type(value).__name__}")
            # As above: a 0-length buffer is an absent/empty batch (a real empty batch
            # carries a schema message), so tolerate it as None instead of failing in
            # open_stream ("Tried reading schema message, was null or length 0").
            if len(value) == 0:
                return None
            reader = ValidatedReader(pa.ipc.open_stream(value, options=IPC_READ_OPTIONS), ipc_validation)
            return reader.read_next_batch()

        # Handle types with deserialize_from_bytes class method
        if isinstance(inner_type, type) and hasattr(inner_type, "deserialize_from_bytes") and isinstance(value, bytes):
            deserialize_method: object = getattr(inner_type, "deserialize_from_bytes")  # noqa: B009
            if callable(deserialize_method):
                return deserialize_method(value, ipc_validation)  # ty: ignore[call-top-callable]

        # Handle Enum reconstruction from name (uppercase) or value (legacy lowercase)
        if isinstance(inner_type, type) and issubclass(inner_type, Enum):
            if not isinstance(value, str):
                raise TypeError(f"Expected str for Enum deserialization, got {type(value).__name__}")
            # Try lookup by name first (new format: uppercase)
            try:
                return inner_type[value]
            except KeyError as err:
                # Fallback to lookup by value (legacy format: lowercase)
                for member in inner_type:
                    if member.value == value:
                        return member
                # Re-raise the original error if neither works
                msg = f"'{value}' is not a valid {inner_type.__name__} name or value"
                raise KeyError(msg) from err

        # Handle nested ArrowSerializableDataclass reconstruction
        if (
            isinstance(inner_type, type)
            and hasattr(inner_type, "ARROW_SCHEMA")
            and isinstance(getattr(inner_type, "ARROW_SCHEMA", None), pa.Schema)
            and isinstance(value, dict)
        ):
            # Recursively deserialize nested dataclass.  Consume the nested
            # class's cached plan rather than re-resolving its annotations:
            # this branch runs once per *message* for nested state — a stream
            # state that wraps a whole init request hits it on every batch —
            # so an uncached get_type_hints here sits squarely on the hot path.
            value_dict = cast("dict[str, object]", value)
            nested_kwargs: dict[str, object] = {}
            nested_plan = _serialization_plan(cast("type[ArrowSerializableDataclass]", inner_type))
            for field_plan in nested_plan.fields:
                # Skip transient fields — use their default value
                if field_plan.transient:
                    if field_plan.default is not MISSING:
                        nested_kwargs[field_plan.name] = field_plan.default
                    elif field_plan.default_factory is not MISSING:
                        factory = cast("Callable[[], object]", field_plan.default_factory)
                        nested_kwargs[field_plan.name] = factory()
                    continue
                # Annotated already unwrapped in the plan.
                nested_kwargs[field_plan.name] = cls._convert_value_for_deserialization(
                    value_dict.get(field_plan.name), field_plan.unwrapped_type, ipc_validation
                )
            return inner_type(**nested_kwargs)

        # Handle frozenset reconstruction
        if get_origin(inner_type) is frozenset and isinstance(value, list):
            return frozenset(value)

        # Handle dict reconstruction from list of tuples
        if get_origin(inner_type) is dict and isinstance(value, list):
            return dict(cast("list[tuple[object, object]]", value))

        # Handle list with element type conversion
        origin = get_origin(inner_type)
        if origin is list:
            args = get_args(inner_type)
            if args and isinstance(value, list):
                element_type = args[0]
                return [cls._convert_value_for_deserialization(v, element_type, ipc_validation) for v in value]

        return value

    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation = IpcValidation.FULL) -> Self:
        """Deserialize an instance from Arrow IPC bytes.

        Args:
            data: Arrow IPC stream bytes containing a single-row RecordBatch.
            ipc_validation: Validation level for the deserialized batch.

        Returns:
            Deserialized instance of this class.

        Raises:
            ValueError: If the batch is invalid (wrong row count or missing fields).
            IPCError: If the IPC stream is malformed or truncated.
            TypeError: If a field value has an unexpected type during conversion.
            KeyError: If an Enum name cannot be resolved.

        """
        batch, cm = deserialize_record_batch(data, ipc_validation)
        return cls.deserialize_from_batch(batch, cm, ipc_validation=ipc_validation)
