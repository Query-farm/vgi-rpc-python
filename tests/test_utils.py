# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for vgi_rpc.utils — ArrowSerializableDataclass and helpers."""

from __future__ import annotations

import typing
from dataclasses import dataclass, field
from enum import Enum
from io import BytesIO
from typing import Annotated, ClassVar, NewType

import pyarrow as pa
import pytest

from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    ArrowType,
    IPCError,
    _infer_arrow_type,
    _is_optional_type,
    _validate_single_row_batch,
    deserialize_record_batch,
    empty_batch,
    read_single_record_batch,
    serialize_record_batch,
    serialize_record_batch_bytes,
)

# ---------------------------------------------------------------------------
# Test dataclasses (module-level so get_type_hints can resolve them)
# ---------------------------------------------------------------------------


class Color(Enum):
    """Test colour enum."""

    RED = "red"
    GREEN = "green"


@dataclass(frozen=True)
class Inner(ArrowSerializableDataclass):
    """Nested dataclass for struct tests."""

    x: int
    label: str


@dataclass(frozen=True)
class Outer(ArrowSerializableDataclass):
    """Dataclass with a nested Inner field."""

    inner: Inner
    name: str


@dataclass(frozen=True)
class Level2(ArrowSerializableDataclass):
    """Level 2 wrapping Inner."""

    inner: Inner
    tag: str


@dataclass(frozen=True)
class Level3(ArrowSerializableDataclass):
    """Level 3 wrapping Level2."""

    level2: Level2
    score: float


@dataclass(frozen=True)
class Level4(ArrowSerializableDataclass):
    """Level 4 wrapping Level3."""

    level3: Level3
    name: str


@dataclass(frozen=True)
class WithOptional(ArrowSerializableDataclass):
    """Dataclass with an optional field."""

    value: str
    extra: str | None = None


@dataclass(frozen=True)
class WithDefaults(ArrowSerializableDataclass):
    """Dataclass with default values."""

    name: str
    count: int = 0
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class WithEnum(ArrowSerializableDataclass):
    """Dataclass with an Enum field."""

    color: Color


@dataclass(frozen=True)
class WithList(ArrowSerializableDataclass):
    """Dataclass with a list field."""

    items: list[int]


@dataclass(frozen=True)
class WithDict(ArrowSerializableDataclass):
    """Dataclass with a dict field."""

    mapping: dict[str, int]


@dataclass(frozen=True)
class WithFrozenset(ArrowSerializableDataclass):
    """Dataclass with a frozenset field."""

    tags: frozenset[int]


@dataclass(frozen=True)
class WithNestedList(ArrowSerializableDataclass):
    """Dataclass with a list of nested dataclasses."""

    entries: list[Inner]


@dataclass(frozen=True)
class WithEnumList(ArrowSerializableDataclass):
    """Dataclass with a list of enums."""

    colors: list[Color]


@dataclass(frozen=True)
class WithAnnotated(ArrowSerializableDataclass):
    """Dataclass with Annotated ArrowType override."""

    value: Annotated[int, ArrowType(pa.int32())]


@dataclass(frozen=True)
class WithOptionalNested(ArrowSerializableDataclass):
    """Dataclass with an optional nested field."""

    inner: Inner | None = None


@dataclass(frozen=True)
class WithBytes(ArrowSerializableDataclass):
    """Dataclass with a bytes field."""

    data: bytes


@dataclass(frozen=True)
class WithBool(ArrowSerializableDataclass):
    """Dataclass with a bool field."""

    flag: bool


UserId = NewType("UserId", str)


@dataclass(frozen=True)
class WithNewType(ArrowSerializableDataclass):
    """Dataclass with a NewType field."""

    user_id: UserId


# ---------------------------------------------------------------------------
# TestInferArrowType
# ---------------------------------------------------------------------------


class TestInferArrowType:
    """Unit tests for _infer_arrow_type()."""

    def test_optional_recursion(self) -> None:
        """Optional int | None infers to pa.int64()."""
        assert _infer_arrow_type(int | None) == pa.int64()

    def test_annotated_without_arrow_type(self) -> None:
        """Annotated[int, 'metadata'] strips metadata and infers int64."""
        assert _infer_arrow_type(Annotated[int, "metadata"]) == pa.int64()

    def test_annotated_with_arrow_type(self) -> None:
        """Annotated[int, ArrowType(pa.int32())] returns the explicit type."""
        assert _infer_arrow_type(Annotated[int, ArrowType(pa.int32())]) == pa.int32()

    def test_newtype(self) -> None:
        """NewType('UserId', str) infers to pa.string()."""
        assert _infer_arrow_type(UserId) == pa.string()

    def test_nested_dataclass_struct(self) -> None:
        """ArrowSerializableDataclass maps to pa.struct()."""
        result = _infer_arrow_type(Inner)
        assert isinstance(result, pa.StructType)
        assert result.num_fields == 2

    def test_bare_list(self) -> None:
        """Unsubscripted typing.List defaults to pa.list_(pa.string())."""
        # typing.List (no subscript) has get_origin() == list, get_args() == ()
        assert _infer_arrow_type(typing.List) == pa.list_(pa.string())  # noqa: UP006

    def test_bare_dict(self) -> None:
        """Unsubscripted typing.Dict defaults to pa.map_(pa.string(), pa.string())."""
        assert _infer_arrow_type(typing.Dict) == pa.map_(pa.string(), pa.string())  # noqa: UP006

    def test_bare_frozenset(self) -> None:
        """Unsubscripted typing.FrozenSet defaults to pa.list_(pa.string())."""
        assert _infer_arrow_type(typing.FrozenSet) == pa.list_(pa.string())  # noqa: UP006

    def test_bytes_type(self) -> None:
        """Python bytes maps to pa.binary()."""
        assert _infer_arrow_type(bytes) == pa.binary()

    def test_bool_type(self) -> None:
        """Python bool maps to pa.bool_()."""
        assert _infer_arrow_type(bool) == pa.bool_()

    def test_enum_type(self) -> None:
        """Enum maps to dictionary-encoded string."""
        result = _infer_arrow_type(Color)
        assert isinstance(result, pa.DictionaryType)

    def test_unsupported_type_raises(self) -> None:
        """Unsupported type raises TypeError."""
        with pytest.raises(TypeError, match="Cannot infer Arrow type"):
            _infer_arrow_type(object)

    def test_tuple_raises_with_hint(self) -> None:
        """Bare tuple and subscripted tuple both raise with a targeted hint."""
        with pytest.raises(TypeError, match="no native heterogeneous-tuple type"):
            _infer_arrow_type(tuple)
        with pytest.raises(TypeError, match="no native heterogeneous-tuple type"):
            _infer_arrow_type(tuple[str, int])

    def test_subscripted_list(self) -> None:
        """list[str] maps to pa.list_(pa.string())."""
        assert _infer_arrow_type(list[str]) == pa.list_(pa.string())

    def test_subscripted_dict(self) -> None:
        """dict[str, float] maps to pa.map_(pa.string(), pa.float64())."""
        assert _infer_arrow_type(dict[str, float]) == pa.map_(pa.string(), pa.float64())

    def test_subscripted_frozenset(self) -> None:
        """frozenset[int] maps to pa.list_(pa.int64())."""
        assert _infer_arrow_type(frozenset[int]) == pa.list_(pa.int64())

    def test_nested_list(self) -> None:
        """list[list[int]] maps to pa.list_(pa.list_(pa.int64()))."""
        assert _infer_arrow_type(list[list[int]]) == pa.list_(pa.list_(pa.int64()))

    def test_list_of_enum(self) -> None:
        """list[Color] maps to pa.list_ of dictionary-encoded string."""
        result = _infer_arrow_type(list[Color])
        assert isinstance(result, pa.ListType)
        assert isinstance(result.value_type, pa.DictionaryType)

    def test_list_of_dataclass(self) -> None:
        """list[Inner] maps to pa.list_ of struct."""
        result = _infer_arrow_type(list[Inner])
        assert isinstance(result, pa.ListType)
        assert isinstance(result.value_type, pa.StructType)


# ---------------------------------------------------------------------------
# TestIsOptionalType
# ---------------------------------------------------------------------------


class TestIsOptionalType:
    """Unit tests for _is_optional_type()."""

    def test_non_optional(self) -> None:
        """Plain int is not optional."""
        inner, is_nullable = _is_optional_type(int)
        assert inner is int
        assert is_nullable is False

    def test_optional(self) -> None:
        """Union int | None is optional."""
        inner, is_nullable = _is_optional_type(int | None)
        assert inner is int
        assert is_nullable is True


# ---------------------------------------------------------------------------
# TestValidateSingleRowBatch
# ---------------------------------------------------------------------------


class TestValidateSingleRowBatch:
    """Edge cases for _validate_single_row_batch()."""

    def test_empty_batch_raises(self) -> None:
        """Empty batch raises ValueError."""
        schema = pa.schema([pa.field("x", pa.int64())])
        batch = pa.RecordBatch.from_pylist([], schema=schema)
        with pytest.raises(ValueError, match="empty RecordBatch"):
            _validate_single_row_batch(batch, "Test")

    def test_multi_row_batch_raises(self) -> None:
        """Multi-row batch raises ValueError."""
        batch = pa.RecordBatch.from_pylist([{"x": 1}, {"x": 2}])
        with pytest.raises(ValueError, match="got 2 rows"):
            _validate_single_row_batch(batch, "Test")

    def test_missing_required_fields_raises(self) -> None:
        """Missing required fields raises ValueError."""
        batch = pa.RecordBatch.from_pylist([{"x": 1}])
        with pytest.raises(ValueError, match="Missing fields"):
            _validate_single_row_batch(batch, "Test", required_fields=["y", "z"])

    def test_valid_single_row(self) -> None:
        """Single-row batch returns dict."""
        batch = pa.RecordBatch.from_pylist([{"x": 1, "y": "hello"}])
        row = _validate_single_row_batch(batch, "Test")
        assert row == {"x": 1, "y": "hello"}


# ---------------------------------------------------------------------------
# TestSerializationRoundTrip
# ---------------------------------------------------------------------------


class TestSerializationRoundTrip:
    """Serialize -> deserialize round-trip tests."""

    def test_primitives(self) -> None:
        """String and int fields survive round-trip."""
        obj = Inner(x=42, label="hello")
        restored = Inner.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj

    def test_bytes_field(self) -> None:
        """Bytes field round-trips."""
        obj = WithBytes(data=b"\x00\x01\xff")
        restored = WithBytes.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj

    def test_bool_field(self) -> None:
        """Bool field round-trips."""
        for val in (True, False):
            obj = WithBool(flag=val)
            restored = WithBool.deserialize_from_bytes(obj.serialize_to_bytes())
            assert restored.flag is val

    def test_enum_field(self) -> None:
        """Enum round-trips via name."""
        obj = WithEnum(color=Color.RED)
        restored = WithEnum.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert restored.color is Color.RED

    def test_enum_fallback_by_value(self) -> None:
        """Enum deserializes by value when name lookup fails."""
        # Manually build a batch with the *value* string instead of the *name*
        schema = WithEnum.ARROW_SCHEMA
        batch = pa.RecordBatch.from_pydict(
            {"color": pa.array(["red"]).dictionary_encode()},
            schema=schema,
        )
        restored = WithEnum.deserialize_from_batch(batch)
        assert restored.color is Color.RED

    def test_enum_invalid_raises(self) -> None:
        """Bad enum string raises KeyError."""
        schema = WithEnum.ARROW_SCHEMA
        batch = pa.RecordBatch.from_pydict(
            {"color": pa.array(["purple"]).dictionary_encode()},
            schema=schema,
        )
        with pytest.raises(KeyError, match="not a valid"):
            WithEnum.deserialize_from_batch(batch)

    def test_nested_dataclass(self) -> None:
        """Nested ArrowSerializableDataclass round-trips."""
        obj = Outer(inner=Inner(x=42, label="hi"), name="test")
        restored = Outer.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert isinstance(restored.inner, Inner)

    def test_deeply_nested_dataclass(self) -> None:
        """Four-level nested ArrowSerializableDataclass round-trips."""
        obj = Level4(
            level3=Level3(
                level2=Level2(
                    inner=Inner(x=1, label="deep"),
                    tag="t",
                ),
                score=3.14,
            ),
            name="root",
        )
        restored = Level4.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert isinstance(restored.level3, Level3)
        assert isinstance(restored.level3.level2, Level2)
        assert isinstance(restored.level3.level2.inner, Inner)
        assert restored.level3.level2.inner.x == 1
        assert restored.level3.level2.inner.label == "deep"

    def test_optional_none(self) -> None:
        """Optional field set to None round-trips."""
        obj = WithOptional(value="hello", extra=None)
        restored = WithOptional.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.extra is None

    def test_optional_present(self) -> None:
        """Optional field with value round-trips."""
        obj = WithOptional(value="hello", extra="world")
        restored = WithOptional.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.extra == "world"

    def test_list(self) -> None:
        """List[int] round-trips."""
        obj = WithList(items=[1, 2, 3])
        restored = WithList.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.items == [1, 2, 3]

    def test_list_empty(self) -> None:
        """Empty list round-trips."""
        obj = WithList(items=[])
        restored = WithList.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.items == []

    def test_dict(self) -> None:
        """Dict[str, int] round-trips."""
        obj = WithDict(mapping={"a": 1, "b": 2})
        restored = WithDict.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.mapping == {"a": 1, "b": 2}

    def test_dict_empty(self) -> None:
        """Empty dict round-trips."""
        obj = WithDict(mapping={})
        restored = WithDict.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.mapping == {}

    def test_frozenset(self) -> None:
        """Frozenset[int] round-trips."""
        obj = WithFrozenset(tags=frozenset({10, 20}))
        restored = WithFrozenset.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.tags == frozenset({10, 20})

    def test_frozenset_empty(self) -> None:
        """Empty frozenset round-trips."""
        obj = WithFrozenset(tags=frozenset())
        restored = WithFrozenset.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.tags == frozenset()

    def test_list_with_nested_dataclass(self) -> None:
        """List[Inner] round-trips with element conversion."""
        obj = WithNestedList(entries=[Inner(x=1, label="a"), Inner(x=2, label="b")])
        restored = WithNestedList.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert all(isinstance(e, Inner) for e in restored.entries)

    def test_list_of_enums(self) -> None:
        """List[Color] round-trips with enum conversion."""
        obj = WithEnumList(colors=[Color.RED, Color.GREEN])
        restored = WithEnumList.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert all(isinstance(c, Color) for c in restored.colors)

    def test_defaults_applied(self) -> None:
        """Missing fields in batch use dataclass defaults."""
        # Build a batch with only the required field
        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_pylist([{"name": "test"}], schema=schema)
        restored = WithDefaults.deserialize_from_batch(batch)
        assert restored.name == "test"
        assert restored.count == 0
        assert restored.tags == []

    def test_annotated_field(self) -> None:
        """Annotated[int, ArrowType(pa.int32())] round-trips."""
        obj = WithAnnotated(value=42)
        restored = WithAnnotated.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        # Verify the schema actually uses int32
        assert WithAnnotated.ARROW_SCHEMA.field("value").type == pa.int32()

    def test_newtype_field(self) -> None:
        """NewType round-trips as underlying type."""
        obj = WithNewType(user_id=UserId("abc123"))
        restored = WithNewType.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.user_id == "abc123"

    def test_optional_nested_none(self) -> None:
        """Inner | None set to None round-trips."""
        obj = WithOptionalNested(inner=None)
        restored = WithOptionalNested.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.inner is None

    def test_optional_nested_present(self) -> None:
        """Inner | None set to Inner(...) round-trips."""
        obj = WithOptionalNested(inner=Inner(x=7, label="nested"))
        restored = WithOptionalNested.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert isinstance(restored.inner, Inner)

    def test_deserialize_from_bytes_with_invalid_data(self) -> None:
        """Truncated bytes raise an error through deserialize_from_bytes."""
        with pytest.raises((pa.ArrowInvalid, IPCError)):
            Inner.deserialize_from_bytes(b"not valid ipc data")


# ---------------------------------------------------------------------------
# TestIntermediateBatchTypes
# ---------------------------------------------------------------------------


class TestIntermediateBatchTypes:
    """Verify that _serialize() produces batches with correct Arrow types."""

    def test_enum_is_dictionary_encoded(self) -> None:
        """Enum field serializes as a dictionary-encoded string column."""
        batch = WithEnum(color=Color.GREEN)._serialize()
        assert isinstance(batch.schema.field("color").type, pa.DictionaryType)

    def test_frozenset_serializes_as_list(self) -> None:
        """Frozenset field serializes as a list column."""
        batch = WithFrozenset(tags=frozenset({1, 2}))._serialize()
        assert isinstance(batch.schema.field("tags").type, pa.ListType)

    def test_nested_dataclass_serializes_as_struct(self) -> None:
        """Nested ArrowSerializableDataclass serializes as a struct column."""
        batch = Outer(inner=Inner(x=1, label="a"), name="b")._serialize()
        assert isinstance(batch.schema.field("inner").type, pa.StructType)

    def test_annotated_uses_overridden_type(self) -> None:
        """Annotated ArrowType override is reflected in the serialized batch."""
        batch = WithAnnotated(value=99)._serialize()
        assert batch.schema.field("value").type == pa.int32()


# ---------------------------------------------------------------------------
# TestSchemaGeneration
# ---------------------------------------------------------------------------


class TestSchemaGeneration:
    """Tests for _ArrowSchemaDescriptor edge cases."""

    def test_arrow_field_overrides(self) -> None:
        """_ARROW_FIELD_OVERRIDES takes precedence over inference."""

        @dataclass(frozen=True)
        class WithOverride(ArrowSerializableDataclass):
            """Dataclass with a field override."""

            _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"score": pa.float32()}
            score: float = 0.0

        schema = WithOverride.ARROW_SCHEMA
        assert schema.field("score").type == pa.float32()

    def test_unsupported_field_type_raises(self) -> None:
        """Field with unsupported type raises TypeError on schema access."""
        with pytest.raises(TypeError, match="Cannot generate Arrow schema"):

            @dataclass(frozen=True)
            class Bad(ArrowSerializableDataclass):
                """Dataclass with an unsupported field type."""

                value: object

            _ = Bad.ARROW_SCHEMA


# ---------------------------------------------------------------------------
# TestSerializeMethods
# ---------------------------------------------------------------------------


class TestSerializeMethods:
    """Test serialize() and serialize_to_bytes() explicitly."""

    def test_serialize_to_stream(self) -> None:
        """serialize() writes to a BytesIO stream."""
        obj = Inner(x=5, label="stream")
        buf = BytesIO()
        obj.serialize(buf)
        data = buf.getvalue()
        assert len(data) > 0
        restored = Inner.deserialize_from_bytes(data)
        assert restored == obj

    def test_serialize_to_bytes(self) -> None:
        """serialize_to_bytes() returns bytes directly."""
        obj = Inner(x=10, label="bytes")
        data = obj.serialize_to_bytes()
        assert isinstance(data, bytes)
        assert len(data) > 0
        restored = Inner.deserialize_from_bytes(data)
        assert restored == obj


# ---------------------------------------------------------------------------
# TestEmptyBatch
# ---------------------------------------------------------------------------


class TestEmptyBatch:
    """Tests for the empty_batch() helper."""

    def test_empty_batch_has_zero_rows(self) -> None:
        """Empty batch conforms to schema with zero rows."""
        schema = pa.schema({"x": pa.int64(), "y": pa.string()})  # type: ignore[arg-type]
        batch = empty_batch(schema)
        assert batch.num_rows == 0
        assert batch.schema.equals(schema)

    def test_empty_batch_column_types(self) -> None:
        """Empty batch preserves column types."""
        schema = pa.schema({"flag": pa.bool_(), "data": pa.binary()})  # type: ignore[arg-type]
        batch = empty_batch(schema)
        assert batch.column("flag").type == pa.bool_()
        assert batch.column("data").type == pa.binary()


# ---------------------------------------------------------------------------
# TestSerializeDeserializeRecordBatch
# ---------------------------------------------------------------------------


class TestSerializeDeserializeRecordBatch:
    """Tests for serialize_record_batch / deserialize_record_batch."""

    def test_round_trip(self) -> None:
        """Serialize and deserialize a batch preserves data."""
        batch = pa.RecordBatch.from_pylist([{"x": 42, "y": "hello"}])
        data = serialize_record_batch_bytes(batch)
        restored, _metadata = deserialize_record_batch(data)
        assert restored.num_rows == 1
        assert restored.to_pylist() == [{"x": 42, "y": "hello"}]

    def test_round_trip_with_custom_metadata(self) -> None:
        """Custom metadata survives the round-trip."""
        batch = pa.RecordBatch.from_pylist([{"v": 1}])
        meta = pa.KeyValueMetadata({b"key": b"value"})
        data = serialize_record_batch_bytes(batch, custom_metadata=meta)
        _, restored_meta = deserialize_record_batch(data)
        assert restored_meta is not None
        assert restored_meta[b"key"] == b"value"

    def test_serialize_to_stream(self) -> None:
        """serialize_record_batch writes to a stream destination."""
        batch = pa.RecordBatch.from_pylist([{"a": 1}])
        buf = BytesIO()
        serialize_record_batch(buf, batch)
        data = buf.getvalue()
        restored, _ = deserialize_record_batch(data)
        assert restored.to_pylist() == [{"a": 1}]

    def test_deserialize_empty_data_raises(self) -> None:
        """Deserializing truncated data raises."""
        with pytest.raises((pa.ArrowInvalid, IPCError)):
            deserialize_record_batch(b"bad data")


# ---------------------------------------------------------------------------
# TestReadSingleRecordBatch
# ---------------------------------------------------------------------------


class TestReadSingleRecordBatch:
    """Tests for read_single_record_batch()."""

    def test_read_single_batch(self) -> None:
        """Read a single batch from a stream."""
        batch = pa.RecordBatch.from_pylist([{"x": 99}])
        buf = BytesIO()
        serialize_record_batch(buf, batch)
        buf.seek(0)
        restored, _ = read_single_record_batch(buf, context="test")
        assert restored.to_pylist() == [{"x": 99}]

    def test_read_empty_stream_raises(self) -> None:
        """Empty stream raises IPCError."""
        # Write a valid IPC stream with no batches (schema + EOS only)
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with pa.ipc.RecordBatchStreamWriter(buf, schema):
            pass  # No batches written
        buf.seek(0)
        with pytest.raises(IPCError, match="No record batch"):
            read_single_record_batch(buf, context="empty")

    def test_read_multiple_batches_raises(self) -> None:
        """Stream with multiple batches raises IPCError."""
        batch = pa.RecordBatch.from_pylist([{"x": 1}])
        buf = BytesIO()
        with pa.ipc.RecordBatchStreamWriter(buf, batch.schema) as writer:
            writer.write_batch(batch)
            writer.write_batch(batch)
        buf.seek(0)
        with pytest.raises(IPCError, match="multiple batches"):
            read_single_record_batch(buf, context="multi")


# ---------------------------------------------------------------------------
# TestIsOptionalType — additional coverage
# ---------------------------------------------------------------------------


class TestIsOptionalTypeExtra:
    """Additional _is_optional_type edge cases."""

    def test_typing_optional(self) -> None:
        """typing.Optional[str] is recognized as optional."""
        inner, is_nullable = _is_optional_type(typing.Optional[str])  # noqa: UP045
        assert inner is str
        assert is_nullable is True

    def test_union_with_none(self) -> None:
        """typing.Union[int, None] is recognized as optional."""
        inner, is_nullable = _is_optional_type(typing.Union[int, None])  # noqa: UP007
        assert inner is int
        assert is_nullable is True

    def test_multi_type_union_not_optional(self) -> None:
        """typing.Union[int, str, None] is NOT treated as optional (more than 2 args)."""
        _inner, is_nullable = _is_optional_type(typing.Union[int, str, None])  # noqa: UP007
        assert is_nullable is False

    def test_union_without_none(self) -> None:
        """typing.Union[int, str] is not optional."""
        _inner, is_nullable = _is_optional_type(typing.Union[int, str])  # noqa: UP007
        assert is_nullable is False


# ---------------------------------------------------------------------------
# TestSchemaCaching
# ---------------------------------------------------------------------------


class TestSchemaCaching:
    """Tests for ARROW_SCHEMA descriptor caching."""

    def test_subclass_gets_own_schema(self) -> None:
        """Each subclass gets its own cached schema."""
        assert Inner.ARROW_SCHEMA is not Outer.ARROW_SCHEMA
        assert set(Inner.ARROW_SCHEMA.names) == {"x", "label"}
        assert set(Outer.ARROW_SCHEMA.names) == {"inner", "name"}

    def test_schema_nullable_field(self) -> None:
        """Optional fields are marked nullable in the schema."""
        schema = WithOptional.ARROW_SCHEMA
        assert schema.field("extra").nullable is True
        assert schema.field("value").nullable is False


# ---------------------------------------------------------------------------
# TestSerializationEdgeCases
# ---------------------------------------------------------------------------


class TestSerializationEdgeCases:
    """Edge cases and special serialization paths."""

    def test_pa_schema_field_round_trip(self) -> None:
        """pa.Schema field serializes to bytes and reconstructs."""

        @dataclass(frozen=True)
        class WithSchema(ArrowSerializableDataclass):
            """Dataclass with a pa.Schema field."""

            _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"schema": pa.binary()}
            schema: pa.Schema

        original_schema = pa.schema({"a": pa.int64(), "b": pa.string()})  # type: ignore[arg-type]
        obj = WithSchema(schema=original_schema)
        restored = WithSchema.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.schema.equals(original_schema)

    def test_pa_record_batch_field_round_trip(self) -> None:
        """pa.RecordBatch field serializes to bytes and reconstructs."""

        @dataclass(frozen=True)
        class WithBatch(ArrowSerializableDataclass):
            """Dataclass with a pa.RecordBatch field."""

            _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"batch": pa.binary()}
            batch: pa.RecordBatch

        original_batch = pa.RecordBatch.from_pylist([{"x": 1, "y": "hi"}])
        obj = WithBatch(batch=original_batch)
        restored = WithBatch.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.batch.equals(original_batch)

    def test_float_field_round_trip(self) -> None:
        """Float field preserves value through round-trip."""

        @dataclass(frozen=True)
        class WithFloat(ArrowSerializableDataclass):
            """Dataclass with a float field."""

            value: float

        obj = WithFloat(value=3.14159)
        restored = WithFloat.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.value == pytest.approx(3.14159)

    def test_multiple_defaults_deserialization(self) -> None:
        """Batch missing multiple optional fields uses all defaults."""
        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_pylist([{"name": "only-required"}], schema=schema)
        restored = WithDefaults.deserialize_from_batch(batch)
        assert restored.name == "only-required"
        assert restored.count == 0
        assert restored.tags == []

    def test_optional_enum_none(self) -> None:
        """Optional Enum field set to None round-trips."""

        @dataclass(frozen=True)
        class WithOptionalEnum(ArrowSerializableDataclass):
            """Dataclass with an optional enum field."""

            color: Color | None = None

        obj = WithOptionalEnum(color=None)
        restored = WithOptionalEnum.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.color is None

    def test_optional_enum_present(self) -> None:
        """Optional Enum field with value round-trips."""

        @dataclass(frozen=True)
        class WithOptionalEnum(ArrowSerializableDataclass):
            """Dataclass with an optional enum field."""

            color: Color | None = None

        obj = WithOptionalEnum(color=Color.GREEN)
        restored = WithOptionalEnum.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored.color is Color.GREEN
