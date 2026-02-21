# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests targeting surviving mutmut mutants in vgi_rpc.utils.

Each test class documents which mutant(s) it kills.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Annotated, ClassVar

import pyarrow as pa
import pytest

from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    ArrowType,
    IPCError,
    _infer_arrow_type,
    deserialize_record_batch,
    empty_batch,
    read_single_record_batch,
    serialize_record_batch,
    serialize_record_batch_bytes,
)


def _clear_schema_cache(cls: type[ArrowSerializableDataclass]) -> None:
    """Clear the cached ARROW_SCHEMA so _generate_schema runs fresh.

    mutmut uses os.fork() — the schema cache from the stats pass persists
    into the child process. Without clearing it, _generate_schema mutations
    are invisible because the cached (correct) schema is returned instead.
    """
    cache_attr = "_cached_ARROW_SCHEMA"
    if cache_attr in cls.__dict__:
        delattr(cls, cache_attr)


# ---------------------------------------------------------------------------
# Test dataclasses (module-level so get_type_hints can resolve them)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Nested(ArrowSerializableDataclass):
    """Nested dataclass with a nullable field for struct inference tests."""

    name: str
    value: int | None = None


@dataclass(frozen=True)
class WithNested(ArrowSerializableDataclass):
    """Outer dataclass containing a nested struct field."""

    nested: Nested
    tag: str


@dataclass(frozen=True)
class MultiField(ArrowSerializableDataclass):
    """Dataclass with multiple fields for continue-vs-break tests."""

    name: str
    count: int
    score: float


@dataclass(frozen=True)
class AnnotatedOnly(ArrowSerializableDataclass):
    """Dataclass using only Annotated[T, ArrowType(...)] fields."""

    small_int: Annotated[int, ArrowType(pa.int16())]
    label: str


@dataclass(frozen=True)
class AnnotatedOptional(ArrowSerializableDataclass):
    """Dataclass with Annotated + Optional to test nullable handling."""

    value: Annotated[int, ArrowType(pa.int32())]
    maybe: Annotated[int, ArrowType(pa.int32())] | None = None


@dataclass(frozen=True)
class OverrideMultiField(ArrowSerializableDataclass):
    """Dataclass with overrides AND regular fields to test continue-vs-break."""

    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"score": pa.float32()}
    name: str
    score: float
    count: int


@dataclass(frozen=True)
class OverrideOptional(ArrowSerializableDataclass):
    """Dataclass with override on an optional field."""

    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"value": pa.int16()}
    value: int | None = None


if TYPE_CHECKING:

    class _UnresolvableAtRuntime:
        """Sentinel type that only exists at type-check time."""


@dataclass(frozen=True)
class ForwardRefFallback(ArrowSerializableDataclass):
    """Dataclass with a type annotation unresolvable at runtime.

    ``get_type_hints`` raises ``NameError`` because ``_UnresolvableAtRuntime``
    only exists under ``TYPE_CHECKING``.  The fallback path in
    ``_generate_schema`` uses ``field.type`` strings instead, and the
    override covers the Arrow type.
    """

    _ARROW_FIELD_OVERRIDES: ClassVar[dict[str, pa.DataType]] = {"value": pa.int64()}
    value: _UnresolvableAtRuntime


# ---------------------------------------------------------------------------
# TestEmptyBatchTypes — kills empty_batch mutants 6, 8
# ---------------------------------------------------------------------------


class TestEmptyBatchTypes:
    """Verify empty_batch produces arrays with correct per-field types.

    Kills:
        x_empty_batch__mutmut_6: type=field.type → type=None
        x_empty_batch__mutmut_8: type=field.type → removed
    """

    def test_column_types_match_schema(self) -> None:
        """Each column in empty batch must have the exact type from the schema."""
        schema = pa.schema({"i": pa.int32(), "s": pa.string(), "b": pa.bool_()})  # type: ignore[arg-type]
        batch = empty_batch(schema)
        for i, field in enumerate(schema):
            assert batch.column(i).type == field.type, f"Column {field.name} type mismatch"


# ---------------------------------------------------------------------------
# TestDeserializeErrorMessages — kills deserialize_record_batch mutants 4-7
# ---------------------------------------------------------------------------


class TestDeserializeErrorMessages:
    """Verify error messages contain expected text.

    Kills:
        x_deserialize_record_batch__mutmut_4: message → None
        x_deserialize_record_batch__mutmut_5: message → "XX...XX"
        x_deserialize_record_batch__mutmut_6: message → lowercase
        x_deserialize_record_batch__mutmut_7: message → uppercase
    """

    def test_empty_ipc_stream_error_message(self) -> None:
        """IPCError from empty IPC stream contains exact expected message."""
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with pa.ipc.RecordBatchStreamWriter(buf, schema):
            pass
        with pytest.raises(IPCError, match=r"^No RecordBatch found in provided data$"):
            deserialize_record_batch(buf.getvalue())


# ---------------------------------------------------------------------------
# TestReadSingleRecordBatchErrors — kills read_single_record_batch mutants 1, 2, 7
# ---------------------------------------------------------------------------


class TestReadSingleRecordBatchErrors:
    """Verify error messages and default parameter values.

    Kills:
        x_read_single_record_batch__mutmut_1: context default "batch" → "XXbatchXX"
        x_read_single_record_batch__mutmut_2: context default "batch" → "BATCH"
        x_read_single_record_batch__mutmut_7: error message → None
    """

    def test_default_context_in_error_message(self) -> None:
        """Default context 'batch' appears in error when no context is passed."""
        schema = pa.schema([pa.field("x", pa.int64())])
        buf = BytesIO()
        with pa.ipc.RecordBatchStreamWriter(buf, schema):
            pass
        buf.seek(0)
        with pytest.raises(IPCError, match="batch stream"):
            read_single_record_batch(buf)

    def test_generic_read_error_includes_context(self) -> None:
        """Non-IPC error wraps exception with context in message."""
        buf = BytesIO(b"not valid arrow data at all")
        with pytest.raises(IPCError, match="Error reading record batch from myctx stream"):
            read_single_record_batch(buf, context="myctx")


# ---------------------------------------------------------------------------
# TestInferNestedStructFields — kills _infer_arrow_type mutants 28, 38, 39, 41
# ---------------------------------------------------------------------------


class TestInferNestedStructFields:
    """Verify nested ArrowSerializableDataclass struct inference preserves field details.

    Kills:
        x__infer_arrow_type__mutmut_28: and → or in ARROW_SCHEMA check
        x__infer_arrow_type__mutmut_38: nullable=f.nullable → nullable=None
        x__infer_arrow_type__mutmut_39: pa.field(f.name, ...) → pa.field(f.type, ...)
        x__infer_arrow_type__mutmut_41: nullable=f.nullable → removed
    """

    def test_struct_field_names_preserved(self) -> None:
        """Struct fields retain their original names."""
        _clear_schema_cache(Nested)
        result = _infer_arrow_type(Nested)
        assert isinstance(result, pa.StructType)
        field_names = [result.field(i).name for i in range(result.num_fields)]
        assert field_names == ["name", "value"]

    def test_struct_field_nullable_preserved(self) -> None:
        """Struct fields retain correct nullable flags."""
        _clear_schema_cache(Nested)
        result = _infer_arrow_type(Nested)
        assert isinstance(result, pa.StructType)
        name_field = result.field("name")
        value_field = result.field("value")
        assert name_field.nullable is False
        assert value_field.nullable is True

    def test_fake_arrow_schema_rejected(self) -> None:
        """Type with ARROW_SCHEMA that is not pa.Schema raises TypeError.

        Kills:
            x__infer_arrow_type__mutmut_28: and → or (would enter struct branch for fake schema)
        """

        class FakeSchema:
            ARROW_SCHEMA = "not a real schema"

        with pytest.raises(TypeError, match="Cannot infer Arrow type"):
            _infer_arrow_type(FakeSchema)


# ---------------------------------------------------------------------------
# TestDescriptorCaching — kills __set_name__ mutant 1, __get__ mutant 12
# ---------------------------------------------------------------------------


class TestDescriptorCaching:
    """Verify _ArrowSchemaDescriptor caching works correctly.

    Kills:
        xǁ_ArrowSchemaDescriptorǁ__set_name____mutmut_1: self._name = name → None
        xǁ_ArrowSchemaDescriptorǁ__get____mutmut_12: setattr(owner, cache_attr, None)
    """

    def test_schema_cached_on_second_access(self) -> None:
        """Second access returns the same schema object (identity check)."""
        _clear_schema_cache(MultiField)
        schema1 = MultiField.ARROW_SCHEMA
        schema2 = MultiField.ARROW_SCHEMA
        assert schema1 is schema2

    def test_cached_schema_is_valid(self) -> None:
        """Cached schema is actually a valid pa.Schema, not None."""

        # Force a fresh class so we test the caching path
        @dataclass(frozen=True)
        class Fresh(ArrowSerializableDataclass):
            """Fresh class to test cache validity."""

            x: int

        _ = Fresh.ARROW_SCHEMA  # first access generates + caches
        schema = Fresh.ARROW_SCHEMA  # second access uses cache
        assert isinstance(schema, pa.Schema)
        assert schema.field("x").type == pa.int64()


# ---------------------------------------------------------------------------
# TestAnnotatedSchemaGeneration — kills generate_schema mutants 13, 15, 16,
# 28-36, 50-59
# ---------------------------------------------------------------------------


class TestAnnotatedSchemaGeneration:
    """Verify Annotated[T, ArrowType(...)] handling in _generate_schema.

    Kills generate_schema mutants related to the Annotated path:
        mutmut_13: include_extras=True → None
        mutmut_15: include_extras=True → removed
        mutmut_16: include_extras=True → False
        mutmut_28: get_origin(field_type) → get_origin(None)
        mutmut_29: is Annotated → is not Annotated
        mutmut_30: args = get_args(field_type) → None
        mutmut_31: get_args(field_type) → get_args(None)
        mutmut_32: actual_type = args[0] → None
        mutmut_33: args[0] → args[1]
        mutmut_34: args[1:] → args[2:]
        mutmut_35: arrow_type_from_annotation = arg.arrow_type → None
        mutmut_36: break → return
        mutmut_50: _is_optional_type(actual_type) → None
        mutmut_51: _is_optional_type(actual_type) → _is_optional_type(None)
        mutmut_52: arrow_fields.append(...) → arrow_fields.append(None)
        mutmut_53: field_name → None
        mutmut_54: arrow_type_from_annotation → None
        mutmut_55: nullable=nullable → nullable=None
        mutmut_56: pa.field(field_name, ...) → pa.field(arrow_type_from_annotation, ...)
        mutmut_57: pa.field(field_name, type, ...) → pa.field(field_name, ...)
        mutmut_58: nullable=nullable → removed
        mutmut_59: continue → break
    """

    def test_annotated_type_used_in_schema(self) -> None:
        """Annotated ArrowType is reflected in the generated schema."""
        _clear_schema_cache(AnnotatedOnly)
        schema = AnnotatedOnly.ARROW_SCHEMA
        assert schema.field("small_int").type == pa.int16()
        assert schema.field("label").type == pa.string()

    def test_annotated_field_name_correct(self) -> None:
        """Annotated field keeps its correct name in schema."""
        _clear_schema_cache(AnnotatedOnly)
        schema = AnnotatedOnly.ARROW_SCHEMA
        assert schema.names == ["small_int", "label"]

    def test_annotated_nullable_field(self) -> None:
        """Annotated Optional field is nullable in schema."""
        _clear_schema_cache(AnnotatedOptional)
        schema = AnnotatedOptional.ARROW_SCHEMA
        assert schema.field("value").nullable is False
        assert schema.field("value").type == pa.int32()
        assert schema.field("maybe").nullable is True
        assert schema.field("maybe").type == pa.int32()

    def test_annotated_multi_field_all_present(self) -> None:
        """All fields are present when one uses Annotated (continue not break)."""
        _clear_schema_cache(AnnotatedOnly)
        schema = AnnotatedOnly.ARROW_SCHEMA
        assert len(schema) == 2

    def test_annotated_round_trip(self) -> None:
        """Annotated field round-trips correctly through serialize/deserialize."""
        _clear_schema_cache(AnnotatedOnly)
        obj = AnnotatedOnly(small_int=42, label="test")
        restored = AnnotatedOnly.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj

    def test_annotated_optional_round_trip(self) -> None:
        """Annotated Optional field round-trips with both None and value."""
        _clear_schema_cache(AnnotatedOptional)
        obj_none = AnnotatedOptional(value=10, maybe=None)
        restored = AnnotatedOptional.deserialize_from_bytes(obj_none.serialize_to_bytes())
        assert restored == obj_none

        obj_val = AnnotatedOptional(value=10, maybe=99)
        restored = AnnotatedOptional.deserialize_from_bytes(obj_val.serialize_to_bytes())
        assert restored == obj_val


# ---------------------------------------------------------------------------
# TestOverrideSchemaGeneration — kills generate_schema mutants 5, 8, 40, 44, 47, 48
# ---------------------------------------------------------------------------


class TestOverrideSchemaGeneration:
    """Verify _ARROW_FIELD_OVERRIDES handling in _generate_schema.

    Kills:
        mutmut_5: getattr(cls, "_ARROW_FIELD_OVERRIDES", {}) → None
        mutmut_8: getattr(cls, "_ARROW_FIELD_OVERRIDES", {}) → removed default
        mutmut_40: _is_optional_type(actual_type) → _is_optional_type(None)
        mutmut_44: nullable=nullable → nullable=None
        mutmut_47: nullable=nullable → removed
        mutmut_48: continue → break
    """

    def test_override_type_applied(self) -> None:
        """Override changes the Arrow type from default inference."""
        _clear_schema_cache(OverrideMultiField)
        schema = OverrideMultiField.ARROW_SCHEMA
        assert schema.field("score").type == pa.float32()  # overridden from float64
        assert schema.field("name").type == pa.string()  # normal inference
        assert schema.field("count").type == pa.int64()  # normal inference

    def test_override_all_fields_present(self) -> None:
        """All fields present even with override (continue not break)."""
        _clear_schema_cache(OverrideMultiField)
        schema = OverrideMultiField.ARROW_SCHEMA
        assert len(schema) == 3
        assert schema.names == ["name", "score", "count"]

    def test_override_non_optional_not_nullable(self) -> None:
        """Non-optional override field has nullable=False.

        Kills:
            mutmut_47: nullable=nullable → removed (defaults to True)
        """
        _clear_schema_cache(OverrideMultiField)
        schema = OverrideMultiField.ARROW_SCHEMA
        assert schema.field("score").nullable is False
        assert schema.field("name").nullable is False
        assert schema.field("count").nullable is False

    def test_override_with_optional_nullable(self) -> None:
        """Override on optional field preserves nullable flag."""
        _clear_schema_cache(OverrideOptional)
        schema = OverrideOptional.ARROW_SCHEMA
        assert schema.field("value").type == pa.int16()
        assert schema.field("value").nullable is True

    def test_override_round_trip(self) -> None:
        """Override fields round-trip correctly."""
        _clear_schema_cache(OverrideMultiField)
        obj = OverrideMultiField(name="test", score=1.5, count=7)
        restored = OverrideMultiField.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj


# ---------------------------------------------------------------------------
# TestNullableInference — kills generate_schema mutants 23, 25, 61, 67, 70
# ---------------------------------------------------------------------------


class TestNullableInference:
    """Verify nullable inference in the standard (non-override, non-Annotated) path.

    Kills:
        mutmut_23: type_hints.get(field_name, field.type) → get(field_name, None)
        mutmut_25: type_hints.get(field_name, field.type) → get(field_name, ) (removed default)
        mutmut_61: _is_optional_type(actual_type) → _is_optional_type(None)
        mutmut_67: nullable=nullable → nullable=None
        mutmut_70: nullable=nullable → removed
    """

    def test_non_optional_field_not_nullable(self) -> None:
        """Required fields are not nullable in schema."""
        _clear_schema_cache(MultiField)
        schema = MultiField.ARROW_SCHEMA
        for name in ("name", "count", "score"):
            assert schema.field(name).nullable is False, f"{name} should not be nullable"

    def test_optional_field_is_nullable(self) -> None:
        """Optional field is nullable in schema."""
        _clear_schema_cache(Nested)
        schema = Nested.ARROW_SCHEMA
        assert schema.field("value").nullable is True
        assert schema.field("name").nullable is False


# ---------------------------------------------------------------------------
# TestFallbackTypeHints — kills generate_schema mutant 17, 18
# ---------------------------------------------------------------------------


class TestFallbackTypeHints:
    """Verify the get_type_hints fallback path.

    Kills:
        mutmut_17: type_hints = {f.name: f.type ...} → None
        mutmut_18: dataclass_fields(cls) → dataclass_fields(None)

    ``ForwardRefFallback`` has a field typed as ``_UnresolvableAtRuntime``
    which only exists under ``TYPE_CHECKING``.  At runtime ``get_type_hints``
    raises ``NameError``, forcing the fallback path.  The override covers the
    Arrow type so schema generation succeeds — unless the fallback itself is
    mutated.
    """

    def test_all_field_types_resolved(self) -> None:
        """Schema has correct types for all fields (proves type_hints worked)."""
        _clear_schema_cache(MultiField)
        schema = MultiField.ARROW_SCHEMA
        assert schema.field("name").type == pa.string()
        assert schema.field("count").type == pa.int64()
        assert schema.field("score").type == pa.float64()

    def test_unresolvable_forward_ref_uses_fallback(self) -> None:
        """Fallback path generates correct schema when get_type_hints fails.

        Kills:
            mutmut_17: type_hints = {f.name: f.type ...} → None
                       (None.get() → AttributeError)
            mutmut_18: dataclass_fields(cls) → dataclass_fields(None)
                       (TypeError)
        """
        _clear_schema_cache(ForwardRefFallback)
        schema = ForwardRefFallback.ARROW_SCHEMA
        assert schema.field("value").type == pa.int64()
        assert schema.field("value").nullable is False


# ---------------------------------------------------------------------------
# TestNestedStructRoundTrip — validates _infer_arrow_type struct path end-to-end
# ---------------------------------------------------------------------------


class TestNestedStructRoundTrip:
    """End-to-end test for nested dataclass with nullable struct fields."""

    def test_nested_with_nullable_round_trip(self) -> None:
        """Nested dataclass with nullable field round-trips."""
        _clear_schema_cache(WithNested)
        _clear_schema_cache(Nested)
        obj = WithNested(nested=Nested(name="hi", value=42), tag="t")
        restored = WithNested.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert restored.nested.value == 42

    def test_nested_with_null_value_round_trip(self) -> None:
        """Nested dataclass with None in nullable field round-trips."""
        _clear_schema_cache(WithNested)
        _clear_schema_cache(Nested)
        obj = WithNested(nested=Nested(name="hi", value=None), tag="t")
        restored = WithNested.deserialize_from_bytes(obj.serialize_to_bytes())
        assert restored == obj
        assert restored.nested.value is None


# ---------------------------------------------------------------------------
# TestEmptyBatchRoundTrip — additional empty_batch coverage
# ---------------------------------------------------------------------------


class TestEmptyBatchRoundTrip:
    """Verify empty_batch produces valid batches that can be serialized."""

    def test_empty_batch_serializes(self) -> None:
        """Empty batch can be serialized and deserialized."""
        schema = pa.schema({"x": pa.int32(), "y": pa.string()})  # type: ignore[arg-type]
        batch = empty_batch(schema)
        data = serialize_record_batch_bytes(batch)
        restored, _ = deserialize_record_batch(data)
        assert restored.num_rows == 0
        assert restored.schema.equals(schema)


# ---------------------------------------------------------------------------
# TestReadSingleBatchDefaultContext — exercises the default "batch" parameter
# ---------------------------------------------------------------------------


class TestReadSingleBatchDefaultContext:
    """Verify read_single_record_batch uses 'batch' as default context."""

    def test_success_with_default_context(self) -> None:
        """Successful read works with the default context parameter."""
        batch = pa.RecordBatch.from_pylist([{"x": 1}])
        buf = BytesIO()
        serialize_record_batch(buf, batch)
        buf.seek(0)
        restored, _ = read_single_record_batch(buf)
        assert restored.to_pylist() == [{"x": 1}]
