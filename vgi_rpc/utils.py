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

from dataclasses import MISSING, Field, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum
from io import BytesIO, IOBase
from types import TracebackType, UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
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
    "ArrowSerializableDataclass",
    "ArrowType",
    "Transient",
    "IPCError",
    "IpcValidation",
    "ValidatedReader",
    "deserialize_record_batch",
    "empty_batch",
    "read_single_record_batch",
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


def empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    """Return an empty batch conforming to the schema."""
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )


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
    with ipc.RecordBatchStreamWriter(destination, batch.schema) as writer:
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
        IPCError: If no batches are found in the data.

    """
    with ValidatedReader(ipc.open_stream(pa.BufferReader(data)), ipc_validation) as reader:
        try:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            raise IPCError("No RecordBatch found in provided data") from None

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
        with ValidatedReader(ipc.open_stream(stream), ipc_validation) as reader:
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

    first_row: dict[str, object] = data.to_pylist()[0]

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
    ARROW_SCHEMA: ClassVar[pa.Schema] = _ArrowSchemaDescriptor()  # type: ignore[assignment]

    # Optional: explicit Arrow type overrides for complex fields
    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {}

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
        try:
            type_hints = get_type_hints(type(self), include_extras=True)
        except Exception:
            type_hints = {f.name: f.type for f in dataclass_fields(self)}

        row: dict[str, object] = {}
        for field in dataclass_fields(self):
            field_type = type_hints.get(field.name, field.type)
            if _is_transient_field(field_type):
                continue
            value = getattr(self, field.name)
            # If the field has ArrowType(pa.binary()) and the value is an
            # ArrowSerializableDataclass, serialize to IPC bytes instead of
            # converting to a struct dict.
            if isinstance(value, ArrowSerializableDataclass) and _has_binary_arrow_type(field_type):
                value = value.serialize_to_bytes()
            else:
                value = self._convert_value_for_serialization(value)
            row[field.name] = value
        return row

    def _convert_value_for_serialization(self, value: object) -> object:
        """Convert a value for Arrow serialization."""
        if value is None:
            return None

        # Handle pa.Schema -> serialize to bytes
        if isinstance(value, pa.Schema):
            return value.serialize().to_pybytes()

        # Handle pa.RecordBatch -> serialize to bytes
        if isinstance(value, pa.RecordBatch):
            sink = pa.BufferOutputStream()
            with ipc.RecordBatchStreamWriter(sink, value.schema) as writer:
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

        Returns:
            A pa.RecordBatch containing one row with the instance's field values.

        """
        row_dict = self._to_row_dict()
        batch = pa.RecordBatch.from_pylist([row_dict], schema=self.ARROW_SCHEMA)

        return batch

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
        # Use get_type_hints to resolve string annotations.
        # include_extras=True preserves Annotated[T, ...] for Transient detection.
        try:
            type_hints = get_type_hints(cls, include_extras=True)
        except Exception:
            type_hints = {f.name: f.type for f in dataclass_fields(cls)}

        # Get required fields (those without defaults) from dataclass definition.
        # Fields with defaults or default_factory are optional for compatibility.
        # Transient fields are never required (they are not in the batch).
        required_fields = []
        for f in dataclass_fields(cls):
            field_type = type_hints.get(f.name, f.type)
            if _is_transient_field(field_type):
                continue
            has_default = f.default is not MISSING or f.default_factory is not MISSING
            if not has_default:
                required_fields.append(f.name)

        # Validate and extract row
        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=required_fields,
        )

        # Convert values back to expected Python types
        kwargs: dict[str, Any] = {}
        for field in dataclass_fields(cls):
            field_type = type_hints.get(field.name, field.type)

            # Transient fields are not in the batch — use their default value
            if _is_transient_field(field_type):
                if field.default is not MISSING:
                    kwargs[field.name] = field.default
                elif field.default_factory is not MISSING:
                    kwargs[field.name] = field.default_factory()
                continue

            # Check if field is present in the row
            if field.name not in row:
                # Use default if available (for backward compatibility)
                if field.default is not MISSING:
                    kwargs[field.name] = field.default
                elif field.default_factory is not MISSING:
                    kwargs[field.name] = field.default_factory()
                # If no default, it would have been caught by validate_single_row_batch
                continue

            value = row.get(field.name)

            # Unwrap Annotated to get actual type
            if get_origin(field_type) is Annotated:
                args = get_args(field_type)
                field_type = args[0] if args else field_type

            # Convert value based on field type
            value = cls._convert_value_for_deserialization(value, field_type, ipc_validation)
            kwargs[field.name] = value

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
            return pa.ipc.read_schema(pa.py_buffer(value))

        # Handle pa.RecordBatch reconstruction from bytes
        if inner_type is pa.RecordBatch:
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for pa.RecordBatch deserialization, got {type(value).__name__}")
            reader = ValidatedReader(pa.ipc.open_stream(value), ipc_validation)
            return reader.read_next_batch()

        # Handle types with deserialize_from_bytes class method
        if isinstance(inner_type, type) and hasattr(inner_type, "deserialize_from_bytes") and isinstance(value, bytes):
            deserialize_method: object = getattr(inner_type, "deserialize_from_bytes")  # noqa: B009
            if callable(deserialize_method):
                return deserialize_method(value, ipc_validation)

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
            # Recursively deserialize nested dataclass
            value_dict = cast("dict[str, object]", value)
            nested_kwargs: dict[str, object] = {}
            try:
                nested_hints = get_type_hints(inner_type, include_extras=True)
            except Exception:
                nested_hints = {f.name: f.type for f in dataclass_fields(inner_type)}
            for f in dataclass_fields(inner_type):
                f_type = nested_hints.get(f.name, f.type)
                # Skip transient fields — use their default value
                if _is_transient_field(f_type):
                    if f.default is not MISSING:
                        nested_kwargs[f.name] = f.default
                    elif f.default_factory is not MISSING:
                        nested_kwargs[f.name] = f.default_factory()
                    continue
                if get_origin(f_type) is Annotated:
                    f_type = get_args(f_type)[0]
                nested_kwargs[f.name] = cls._convert_value_for_deserialization(
                    value_dict.get(f.name), f_type, ipc_validation
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
