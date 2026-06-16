# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""SPIKE: property-based round-trip testing of every ArrowSerializableDataclass.

Auto-discovers all ``ArrowSerializableDataclass`` subclasses, builds a
Hypothesis strategy for each from its own type annotations plus its resolved
``ARROW_SCHEMA`` (so numeric widths / decimal scale / temporal units are
respected), then asserts ``deserialize(serialize(x)) == x``. Classes whose
fields use a shape the builder doesn't model yet are skipped with a reason,
so the report stays honest about coverage.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import importlib
import types
from dataclasses import fields as dataclass_fields
from decimal import Decimal
from enum import Enum
from typing import Annotated, Union, cast, get_args, get_origin, get_type_hints

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from vgi_rpc.utils import ArrowSerializableDataclass, _is_transient_field

# Import modules that define dataclasses so __subclasses__() can find them.
for _mod in ("vgi_rpc.conformance._types", "tests.test_utils"):
    importlib.import_module(_mod)


class _Unsupported(Exception):
    """Raised when the strategy builder hits a field shape it doesn't model."""


def _unwrap_optional(tp: object) -> tuple[object, bool]:
    origin = get_origin(tp)
    if origin is Union or origin is types.UnionType:
        args = [a for a in get_args(tp) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return tp, False


def _int_bounds(at: pa.DataType) -> tuple[int, int]:
    bits = at.bit_width
    if pa.types.is_unsigned_integer(at):
        return 0, 2**bits - 1
    return -(2 ** (bits - 1)), 2 ** (bits - 1) - 1


_FIELD_TYPES: dict[str, pa.DataType] = {"a": pa.int64(), "b": pa.string(), "c": pa.float64()}


def _mk_schema(names: list[str]) -> pa.Schema:
    return pa.schema([(n, _FIELD_TYPES[n]) for n in names])


def _simple_schema() -> st.SearchStrategy[pa.Schema]:
    return st.lists(st.sampled_from(sorted(_FIELD_TYPES)), min_size=1, max_size=3, unique=True).map(_mk_schema)


def _simple_batch() -> st.SearchStrategy[pa.RecordBatch]:
    return st.lists(st.integers(-(2**31), 2**31 - 1), max_size=5).map(lambda xs: pa.record_batch({"a": xs}))


def _scalar_strategy(py_type: object, at: pa.DataType) -> st.SearchStrategy[object]:
    if pa.types.is_dictionary(at):
        at = at.value_type
    if py_type is pa.Schema:
        return _simple_schema()
    if py_type is pa.RecordBatch:
        return _simple_batch()
    if py_type is bool or pa.types.is_boolean(at):
        return st.booleans()
    if pa.types.is_integer(at):
        lo, hi = _int_bounds(at)
        return st.integers(min_value=lo, max_value=hi)
    if pa.types.is_float32(at):
        return st.floats(width=32, allow_nan=False, allow_infinity=False)
    if pa.types.is_floating(at):
        return st.floats(allow_nan=False, allow_infinity=False)
    if pa.types.is_decimal(at):
        scale = at.scale
        bound = Decimal(10) ** (at.precision - scale) - 1
        return st.decimals(min_value=-bound, max_value=bound, places=scale)
    if pa.types.is_date(at):
        return st.dates()
    if pa.types.is_timestamp(at):
        return st.datetimes(timezones=st.just(dt.UTC)) if at.tz is not None else st.datetimes()
    if pa.types.is_time(at):
        return st.times()
    if pa.types.is_duration(at):
        return st.timedeltas(min_value=dt.timedelta(0), max_value=dt.timedelta(days=100_000))
    if pa.types.is_fixed_size_binary(at):
        n = at.byte_width
        return st.binary(min_size=n, max_size=n)
    if pa.types.is_binary(at) or pa.types.is_large_binary(at):
        return st.binary(max_size=16)
    if pa.types.is_string(at) or pa.types.is_large_string(at):
        return st.text(max_size=16)
    raise _Unsupported(f"arrow type {at} / py {py_type!r}")


def _build_strategy(py_type: object, at: pa.DataType) -> st.SearchStrategy[object]:
    if get_origin(py_type) is Annotated:
        py_type = get_args(py_type)[0]
    if hasattr(py_type, "__supertype__"):  # NewType
        py_type = py_type.__supertype__
    inner, is_opt = _unwrap_optional(py_type)
    if is_opt:
        return st.none() | _build_strategy(inner, at)

    if isinstance(py_type, type) and issubclass(py_type, Enum):
        return st.sampled_from(list(py_type))
    if isinstance(py_type, type) and issubclass(py_type, ArrowSerializableDataclass):
        return strategy_for_dataclass(py_type)

    origin = get_origin(py_type)
    if origin is list:
        (elem_py,) = get_args(py_type)
        elem_at = at.value_type if (pa.types.is_list(at) or pa.types.is_large_list(at)) else at
        return st.lists(_build_strategy(elem_py, elem_at), max_size=4)
    if origin is dict:
        k_py, v_py = get_args(py_type)
        k_at = at.key_type if pa.types.is_map(at) else pa.string()
        v_at = at.item_type if pa.types.is_map(at) else pa.string()
        return st.dictionaries(_build_strategy(k_py, k_at), _build_strategy(v_py, v_at), max_size=4)
    if origin is frozenset:
        (elem_py,) = get_args(py_type)
        elem_at = at.value_type if (pa.types.is_list(at) or pa.types.is_large_list(at)) else at
        return st.frozensets(_build_strategy(elem_py, elem_at), max_size=4)

    return _scalar_strategy(py_type, at)


def strategy_for_dataclass(cls: type[ArrowSerializableDataclass]) -> st.SearchStrategy[ArrowSerializableDataclass]:
    """Build a strategy producing instances of *cls* from its annotations + schema."""
    hints = get_type_hints(cls, include_extras=True)
    schema = cls.ARROW_SCHEMA
    field_strats: dict[str, st.SearchStrategy[object]] = {}
    for f in dataclass_fields(cls):
        hint = hints[f.name]
        if _is_transient_field(hint):
            continue  # not serialized — let the dataclass default fill it in
        field_strats[f.name] = _build_strategy(hint, schema.field(f.name).type)
    return cast("st.SearchStrategy[ArrowSerializableDataclass]", st.builds(cls, **field_strats))


def _discover() -> list[type[ArrowSerializableDataclass]]:
    seen: dict[str, type[ArrowSerializableDataclass]] = {}

    def rec(c: type[ArrowSerializableDataclass]) -> None:
        for sub in c.__subclasses__():
            if dataclasses.is_dataclass(sub):
                try:
                    schema = sub.ARROW_SCHEMA
                except Exception:  # abstract/partial classes can't build a schema
                    schema = None
                if isinstance(schema, pa.Schema):
                    seen.setdefault(sub.__qualname__, sub)
            rec(sub)

    rec(ArrowSerializableDataclass)
    return [seen[k] for k in sorted(seen)]


ALL_SUBCLASSES = _discover()


@pytest.mark.parametrize("cls", ALL_SUBCLASSES, ids=lambda c: c.__qualname__)
@settings(max_examples=60, deadline=None)
@given(data=st.data())
def test_roundtrip(cls: type[ArrowSerializableDataclass], data: st.DataObject) -> None:
    """deserialize(serialize(x)) == x for arbitrary in-contract values."""
    try:
        strat = strategy_for_dataclass(cls)
    except _Unsupported as exc:
        pytest.skip(f"strategy builder does not model: {exc}")
    obj = data.draw(strat)
    restored = type(obj).deserialize_from_bytes(obj.serialize_to_bytes())
    assert restored == obj


def test_discovery_is_nonempty() -> None:
    """Guard: discovery must find the known dataclasses (catches import regressions)."""
    names = {c.__qualname__ for c in ALL_SUBCLASSES}
    assert {"AllTypes", "WideTypes", "ContainerWideTypes", "DeepNested", "EmbeddedArrow"} <= names
