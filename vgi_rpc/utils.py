"""IPC utility functions for Arrow message reading and writing.

This module provides helper functions for common IPC patterns used in the
VGI protocol, reducing code duplication between client and worker.

KEY FUNCTIONS
-------------
serialize_record_batch(batch, batch_type, metadata) : Serialize with batch type
deserialize_record_batch(data) : Deserialize and validate type
read_single_record_batch(stream, context) : Read and validate
validate_single_row_batch(batch, class_name, required_fields, custom_metadata) : Validate batch and verify batch type
get_batch_type(metadata) : Extract batch type from metadata

KEY CLASSES
-----------
RecordBatchState : Wrapper for RecordBatch implementing Serializable protocol.
    Use this in distributed functions for storing/collecting state across workers.

IPCError : Exception raised on IPC communication errors

"""

import os
from dataclasses import MISSING, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum
from io import BytesIO, IOBase
from types import UnionType
from typing import (
    Annotated,
    Any,
    ClassVar,
    Self,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa
import structlog
from pyarrow import ipc

from vgi_rpc.metadata import decode_metadata

__all__ = [
    "ArrowSerializableDataclass",
    "ArrowType",
]

# IPC debug logging - enable with VGI_IPC_DEBUG=1
_IPC_DEBUG = os.environ.get("VGI_IPC_DEBUG", "").lower() in ("1", "true", "yes")
# IPC stats logging - enable with VGI_IPC_STATS=1 for aggregate stream stats
_IPC_STATS = os.environ.get("VGI_IPC_STATS", "").lower() in ("1", "true", "yes")
_ipc_log: structlog.stdlib.BoundLogger | None = None


def _get_ipc_log() -> structlog.stdlib.BoundLogger:
    """Get or create the IPC debug logger, configured to write to stderr."""
    global _ipc_log
    if _ipc_log is None:
        import sys

        # Configure structlog to write to stderr for IPC debugging
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )
        _ipc_log = structlog.get_logger().bind(component="ipc")
    return _ipc_log


def _schema_to_dict(schema: pa.Schema) -> dict[str, str]:
    """Convert Arrow schema to dict of {name: type} for logging."""
    return {field.name: str(field.type) for field in schema}


class IPCError(Exception):
    """Error during IPC message reading or writing."""


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
    """Serialize a RecordBatch to bytes in Arrow IPC stream format.

    Uses RecordBatchStreamWriter to produce a complete IPC stream with
    schema, batch, and end-of-stream marker. The batch type is automatically
    added to the metadata for validation on deserialization.

    Args:
        destination: The destination to write to (must support binary writes,
        e.g., stdout pipe, BufferedWriter).
        batch: The RecordBatch to serialize.
        custom_metadata: Optional additional metadata to include.

    Returns:
        Complete Arrow IPC stream bytes including EOS marker.

    """
    with ipc.RecordBatchStreamWriter(destination, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=custom_metadata)

    if _IPC_DEBUG:
        _get_ipc_log().debug(
            "ipc_write",
            num_rows=batch.num_rows,
            schema=_schema_to_dict(batch.schema),
            metadata=decode_metadata(custom_metadata),
        )


def serialize_record_batch_bytes(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None = None,
) -> bytes:
    """Serialize a RecordBatch to bytes in Arrow IPC stream format.

    Uses RecordBatchStreamWriter to produce a complete IPC stream with
    schema, batch, and end-of-stream marker. The batch type is automatically
    added to the metadata for validation on deserialization.

    Args:
        batch: The RecordBatch to serialize.
        batch_type: The type of batch being serialized (required for validation).
        custom_metadata: Optional additional metadata to include.

    Returns:
        Complete Arrow IPC stream bytes including EOS marker.

    """
    buffer = BytesIO()
    serialize_record_batch(buffer, batch, custom_metadata)
    return buffer.getvalue()


def deserialize_record_batch(
    data: bytes,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Deserialize bytes back to a RecordBatch with custom metadata.

    Validates that the batch type in metadata matches the expected type.

    Args:
        data: Bytes containing a serialized RecordBatch in Arrow IPC stream format.
        expected_type: The expected batch type (required for validation).

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If more than a single batch is found, or no batches are found.
        ValueError: If the batch type doesn't match expected_type.

    """
    with ipc.open_stream(pa.BufferReader(data)) as reader:
        try:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            raise IPCError("No RecordBatch found in provided data") from None

        if _IPC_DEBUG:
            _get_ipc_log().debug(
                "ipc_read",
                num_rows=batch.num_rows,
                schema=_schema_to_dict(batch.schema),
                metadata=decode_metadata(custom_metadata),
                nbytes=len(data),
            )
        return batch, custom_metadata


def read_single_record_batch(
    stream: Any,
    context: str = "batch",
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Read a single record batch from a stream.

    Args:
        stream: Stream to read from (must support binary reads, e.g., stdin pipe,
            BufferedReader). Type is Any to accommodate runtime reassignment
            of stdin/stdout to binary mode.
        context: Description for error messages (e.g., "invocation", "init_input").
        expected_type: If provided, validate that the batch type matches.

    Returns:
        Tuple of (RecordBatch, custom_metadata). The custom_metadata may be None
        if no custom metadata was attached to the batch.

    Raises:
        IPCError: If more than a single batch is found, or no batches are found.
        ValueError: If expected_type is provided and batch type doesn't match.

    """
    try:
        with ipc.open_stream(stream) as reader:
            try:
                batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                if _IPC_DEBUG:
                    _get_ipc_log().error(
                        "ipc_read: No record batch found in stream",
                        context=context,
                    )
                raise IPCError(f"No record batch found in {context} stream") from None

            try:
                reader.read_next_batch()
            except StopIteration:
                if _IPC_DEBUG:
                    _get_ipc_log().debug(
                        "ipc_read",
                        context=context,
                        num_rows=batch.num_rows,
                        schema=_schema_to_dict(batch.schema),
                        metadata=decode_metadata(custom_metadata),
                    )
                return batch, custom_metadata

            if _IPC_DEBUG:
                _get_ipc_log().error(
                    "ipc_read: Multiple batches found in stream",
                    context=context,
                )
            raise IPCError(f"Expected single record batch in {context} stream, but found multiple batches")
    except Exception as e:
        raise IPCError(f"Error reading record batch from {context} stream: {e}") from e


def _validate_single_row_batch(
    data: pa.RecordBatch,
    class_name: str,
    required_fields: list[str] | None = None,
    custom_metadata: pa.KeyValueMetadata | None = None,
) -> dict[str, Any]:
    """Validate a RecordBatch has exactly one row and return it as a dict.

    Args:
        data: The RecordBatch to validate.
        class_name: Name of the class being deserialized (for error messages).
        required_fields: Optional list of field names that must be present.
        custom_metadata: Optional custom metadata from the batch (for batch
            type validation).
        expected_type: If provided, validate that the batch's type
            matches this value.

    Returns:
        The first (and only) row as a dictionary.

    Raises:
        ValueError: If the batch is empty, has multiple rows, is missing
            required fields, or has wrong batch type.

    """
    if data.num_rows == 0:
        raise ValueError(f"Cannot deserialize {class_name} from empty RecordBatch")
    if data.num_rows > 1:
        raise ValueError(f"Expected single-row RecordBatch for {class_name} deserialization, got {data.num_rows} rows")

    first_row: dict[str, Any] = data.to_pylist()[0]

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


def _is_optional_type(python_type: Any) -> tuple[Any, bool]:
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


def _infer_arrow_type(python_type: Any) -> pa.DataType:
    """Infer Arrow type from Python type annotation.

    Supports:
    - Basic types: str, bytes, int, float, bool
    - Generic types: list[T], dict[K, V], frozenset[T]
    - NewType: auto-unwraps to underlying type
    - Enum: serializes as dictionary-encoded string
    - ArrowSerializableDataclass: serializes as struct

    For complex types not supported here, use Annotated[T, ArrowType(...)].

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
        return _infer_arrow_type(python_type.__supertype__)

    # Handle Enum - serialize as dictionary-encoded string
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        return pa.dictionary(pa.int8(), pa.string())

    # Handle ArrowSerializableDataclass - serialize as struct
    if hasattr(python_type, "ARROW_SCHEMA") and isinstance(getattr(python_type, "ARROW_SCHEMA", None), pa.Schema):
        # Convert schema fields to struct type using pa.field tuples
        struct_fields = [pa.field(f.name, f.type, nullable=f.nullable) for f in python_type.ARROW_SCHEMA]
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

    if python_type in type_map:
        return type_map[python_type]

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
        if hasattr(owner, cache_attr):
            cached: pa.Schema = getattr(owner, cache_attr)
            return cached

        # Generate schema from dataclass fields
        schema = self._generate_schema(owner)

        # Cache on the class (not the descriptor)
        setattr(owner, cache_attr, schema)
        return schema

    def _generate_schema(self, cls: type["ArrowSerializableDataclass"]) -> pa.Schema:
        """Generate ARROW_SCHEMA from dataclass field annotations."""
        # Build schema from dataclass fields
        arrow_fields: list[pa.Field[Any]] = []
        overrides = getattr(cls, "_ARROW_FIELD_OVERRIDES", {})

        # Use get_type_hints to resolve string annotations
        # include_extras=True preserves Annotated[T, ...] wrappers
        try:
            type_hints = get_type_hints(cls, include_extras=True)
        except Exception:
            # Fallback to field.type if get_type_hints fails
            type_hints = {f.name: f.type for f in dataclass_fields(cls)}  # type: ignore[arg-type]

        for field in dataclass_fields(cls):  # type: ignore[arg-type]
            field_name = field.name
            field_type = type_hints.get(field_name, field.type)

            # Check for Annotated[T, ArrowType(...)]
            arrow_type_from_annotation: pa.DataType | None = None
            actual_type = field_type
            if get_origin(field_type) is Annotated:
                args = get_args(field_type)
                actual_type = args[0] if args else field_type
                for arg in args[1:]:
                    if isinstance(arg, ArrowType):
                        arrow_type_from_annotation = arg.arrow_type
                        break

            # Check for explicit ClassVar override (legacy support)
            if field_name in overrides:
                arrow_type = overrides[field_name]
                _, nullable = _is_optional_type(actual_type)
                arrow_fields.append(pa.field(field_name, arrow_type, nullable=nullable))
                continue

            # Use ArrowType from annotation if provided
            if arrow_type_from_annotation is not None:
                _, nullable = _is_optional_type(actual_type)
                arrow_fields.append(pa.field(field_name, arrow_type_from_annotation, nullable=nullable))
                continue

            # Infer Arrow type from Python type
            _, nullable = _is_optional_type(actual_type)
            try:
                arrow_type = _infer_arrow_type(actual_type)
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
    - Enum: serializes as dictionary-encoded string via .value
    - ArrowSerializableDataclass: serializes as struct

    Optional fields (annotated with `| None`) are marked as nullable.
    To override specific field types, use Annotated with ArrowType.

    Attributes:
        ARROW_SCHEMA: Auto-generated Arrow schema from field annotations.

    """

    # Auto-generated from field annotations on first access
    ARROW_SCHEMA: ClassVar[pa.Schema] = _ArrowSchemaDescriptor()  # type: ignore[assignment]

    # Optional: explicit Arrow type overrides for complex fields
    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {}

    def _to_row_dict(self) -> dict[str, Any]:
        """Convert instance to a dictionary for Arrow batch construction.

        Handles special type conversions:
        - frozenset -> list (Arrow doesn't support sets)
        - dict -> list of tuples (for map types)
        - Enum -> .value (serialize as the enum's value)
        - ArrowSerializableDataclass -> dict (serialize nested dataclass)
        - list[Enum] or list[ArrowSerializableDataclass] -> list of values/dicts

        """
        row: dict[str, Any] = {}
        for field in dataclass_fields(self):  # type: ignore[arg-type]
            value = getattr(self, field.name)
            value = self._convert_value_for_serialization(value)
            row[field.name] = value
        return row

    def _convert_value_for_serialization(self, value: Any) -> Any:
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

        # Handle objects with serialize_to_bytes() method (e.g., Arguments)
        if hasattr(value, "serialize_to_bytes") and callable(value.serialize_to_bytes):
            return value.serialize_to_bytes()

        # Handle Enum -> .name (always uppercase, consistent across all enum types)
        if isinstance(value, Enum):
            return value.name

        # Handle nested ArrowSerializableDataclass -> dict
        if isinstance(value, ArrowSerializableDataclass):
            return value._to_row_dict()

        # Handle frozenset -> list
        if isinstance(value, frozenset):
            return [self._convert_value_for_serialization(v) for v in value]

        # Handle dict -> list of tuples for Arrow map type
        if isinstance(value, dict):
            return list(value.items())

        # Handle list - recursively convert elements
        if isinstance(value, list):
            return [self._convert_value_for_serialization(v) for v in value]

        return value

    def _serialize(self) -> pa.RecordBatch:
        """Serialize this instance a batch type and a record Batch.

        Returns:
            A tuple of the pa.RecordBatch and the BatchType

        Raises:
            ValueError: If Meta.batch_type is not defined on the class.

        """
        row_dict = self._to_row_dict()
        batch = pa.RecordBatch.from_pylist([row_dict], schema=self.ARROW_SCHEMA)

        return batch

    def serialize(self, dest: IOBase) -> None:
        """Serialize this instance to an Arrow IPC stream.

        Args:
            dest: The destination to write to (must support binary writes,
                e.g., stdout pipe, BufferedWriter).

        Raises:
            ValueError: If Meta.batch_type is not defined on the class.

        """
        serialize_record_batch(dest, self._serialize())

    def serialize_to_bytes(self) -> bytes:
        """Serialize this instance to Arrow IPC bytes.

        Returns:
            Arrow IPC stream bytes containing a single-row RecordBatch.

        Raises:
            ValueError: If Meta.batch_type is not defined on the class.

        """
        return serialize_record_batch_bytes(self._serialize())

    @classmethod
    def deserialize_from_batch(
        cls,
        batch: pa.RecordBatch,
        custom_metadata: pa.KeyValueMetadata | None = None,
    ) -> Self:
        """Deserialize an instance from an Arrow RecordBatch.

        Args:
            batch: Single-row RecordBatch containing the serialized data.
            custom_metadata: Optional metadata from the batch (for batch type).

        Returns:
            Deserialized instance of this class.

        Raises:
            ValueError: If the batch is invalid (wrong row count, missing fields,
                or wrong batch type).

        """
        # Get required fields (those without defaults) from dataclass definition.
        # Fields with defaults or default_factory are optional for compatibility.
        required_fields = []
        for f in dataclass_fields(cls):  # type: ignore[arg-type]
            has_default = f.default is not MISSING or f.default_factory is not MISSING
            if not has_default:
                required_fields.append(f.name)

        # Validate and extract row
        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=required_fields,
            custom_metadata=custom_metadata,
        )

        # Use get_type_hints to resolve string annotations
        try:
            type_hints = get_type_hints(cls)
        except Exception:
            type_hints = {f.name: f.type for f in dataclass_fields(cls)}  # type: ignore[arg-type]

        # Convert values back to expected Python types
        kwargs: dict[str, Any] = {}
        for field in dataclass_fields(cls):  # type: ignore[arg-type]
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
            field_type = type_hints.get(field.name, field.type)

            # Unwrap Annotated to get actual type
            if get_origin(field_type) is Annotated:
                args = get_args(field_type)
                field_type = args[0] if args else field_type

            # Convert value based on field type
            value = cls._convert_value_for_deserialization(value, field_type)
            kwargs[field.name] = value

        return cls(**kwargs)

    @classmethod
    def _convert_value_for_deserialization(cls, value: Any, field_type: Any) -> Any:
        """Convert a deserialized value back to the expected Python type."""
        if value is None:
            return None

        # Unwrap Optional type
        inner_type, _ = _is_optional_type(field_type)

        # Handle pa.Schema reconstruction from bytes
        if inner_type is pa.Schema:
            return pa.ipc.read_schema(pa.py_buffer(value))

        # Handle pa.RecordBatch reconstruction from bytes
        if inner_type is pa.RecordBatch:
            reader = pa.ipc.open_stream(value)
            return reader.read_next_batch()

        # Handle types with deserialize_from_bytes class method (e.g., Arguments)
        if isinstance(inner_type, type) and hasattr(inner_type, "deserialize_from_bytes") and isinstance(value, bytes):
            deserialize_method = inner_type.deserialize_from_bytes
            if callable(deserialize_method):
                return deserialize_method(value)

        # Handle Enum reconstruction from name (uppercase) or value (legacy lowercase)
        if isinstance(inner_type, type) and issubclass(inner_type, Enum):
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
            nested_kwargs = {}
            try:
                nested_hints = get_type_hints(inner_type)
            except Exception:
                nested_hints = {f.name: f.type for f in dataclass_fields(inner_type)}
            for f in dataclass_fields(inner_type):
                f_type = nested_hints.get(f.name, f.type)
                if get_origin(f_type) is Annotated:
                    f_type = get_args(f_type)[0]
                nested_kwargs[f.name] = cls._convert_value_for_deserialization(value.get(f.name), f_type)
            return inner_type(**nested_kwargs)

        # Handle frozenset reconstruction
        if get_origin(inner_type) is frozenset:
            return frozenset(value)

        # Handle dict reconstruction from list of tuples
        if get_origin(inner_type) is dict:
            return dict(value)

        # Handle list with element type conversion
        origin = get_origin(inner_type)
        if origin is list:
            args = get_args(inner_type)
            if args and isinstance(value, list):
                element_type = args[0]
                return [cls._convert_value_for_deserialization(v, element_type) for v in value]

        return value

    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> Self:
        """Deserialize an instance from Arrow IPC bytes.

        Args:
            data: Arrow IPC stream bytes containing a single-row RecordBatch.

        Returns:
            Deserialized instance of this class.

        Raises:
            ValueError: If the batch is invalid (wrong row count, missing fields,
                wrong batch type, or Meta.batch_type is not defined).

        """
        return cls.deserialize_from_batch(*deserialize_record_batch(data))
