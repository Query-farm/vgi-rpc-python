# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Canonical Arrow type tokens for the protocol-hash preimage.

The point of these tests is that the vocabulary is *total* over the Arrow
types a protocol can reach, and that distinctions Arrow makes survive into the
token.  A token that collapses two distinct Arrow types gives two different
protocols the same hash; a token that varies where Arrow does not gives one
protocol two hashes across ports.
"""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import pytest

from vgi_rpc.rpc._type_tokens import (
    UnsupportedArrowTypeError,
    field_token,
    schema_tokens,
    type_token,
)


class TestPrimitives:
    """Every parameterless type has a fixed lowercase spelling."""

    @pytest.mark.parametrize(
        ("dtype", "expected"),
        [
            (pa.null(), "null"),
            (pa.bool_(), "bool"),
            (pa.int8(), "int8"),
            (pa.int16(), "int16"),
            (pa.int32(), "int32"),
            (pa.int64(), "int64"),
            (pa.uint8(), "uint8"),
            (pa.uint16(), "uint16"),
            (pa.uint32(), "uint32"),
            (pa.uint64(), "uint64"),
            (pa.float16(), "float16"),
            (pa.float32(), "float32"),
            (pa.float64(), "float64"),
            (pa.string(), "utf8"),
            (pa.large_string(), "large_utf8"),
            (pa.binary(), "binary"),
            (pa.large_binary(), "large_binary"),
            (pa.date32(), "date32"),
            (pa.date64(), "date64"),
            (pa.month_day_nano_interval(), "interval_month_day_nano"),
        ],
    )
    def test_spelling(self, dtype: pa.DataType, expected: str) -> None:
        """The spelling is the contract; changing one rotates every hash."""
        assert type_token(dtype) == expected

    def test_utf8_is_not_string(self) -> None:
        """Arrow calls it ``string``; the wire vocabulary calls it ``utf8``.

        Deliberate: ``utf8`` is the name in the Arrow columnar *format* spec,
        which is the cross-language document the ports share, whereas
        ``string`` is the Python binding's name for it.
        """
        assert type_token(pa.string()) == "utf8"
        assert type_token(pa.utf8()) == "utf8"


class TestParameterised:
    """Parameters are folded into the token, never emitted as JSON numbers."""

    def test_decimal_carries_precision_and_scale(self) -> None:
        """Two decimals differing only in scale are different types."""
        assert type_token(pa.decimal128(38, 9)) == "decimal128(38,9)"
        assert type_token(pa.decimal128(38, 2)) != type_token(pa.decimal128(38, 9))

    def test_decimal256_is_distinct_from_decimal128(self) -> None:
        """Same precision and scale, different width, different type."""
        assert type_token(pa.decimal256(38, 9)) == "decimal256(38,9)"
        assert type_token(pa.decimal256(38, 9)) != type_token(pa.decimal128(38, 9))

    def test_fixed_size_binary_carries_width(self) -> None:
        """A 16-byte UUID column is not a 32-byte hash column."""
        assert type_token(pa.binary(16)) == "fixed_size_binary(16)"

    @pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
    def test_timestamp_units_are_distinct(self, unit: Literal["s", "ms", "us", "ns"]) -> None:
        """Unit is part of the type; a micro/nano confusion is a real bug."""
        assert type_token(pa.timestamp(unit)) == f"timestamp({unit})"

    def test_timezone_is_carried_verbatim(self) -> None:
        """The zone string is carried verbatim, never resolved or abbreviated.

        ``UTC`` and ``+00:00`` are distinct Arrow types and must not collapse.
        """
        assert type_token(pa.timestamp("us", tz="UTC")) == "timestamp(us,tz=UTC)"
        assert type_token(pa.timestamp("us", tz="+00:00")) == "timestamp(us,tz=+00:00)"
        assert type_token(pa.timestamp("us", tz="UTC")) != type_token(pa.timestamp("us"))

    def test_time_types_carry_their_width(self) -> None:
        """time32 and time64 are different types with overlapping units."""
        assert type_token(pa.time32("ms")) == "time32(ms)"
        assert type_token(pa.time64("us")) == "time64(us)"

    def test_duration_units(self) -> None:
        """A duration is not a timestamp even at the same unit."""
        assert type_token(pa.duration("ns")) == "duration(ns)"
        assert type_token(pa.duration("us")) != type_token(pa.timestamp("us"))

    def test_no_token_contains_a_json_number(self) -> None:
        """Parameters live inside the string so RFC 8785 number rules never apply.

        Number canonicalisation is JCS's hardest rule and the likeliest place
        for six ports to diverge.  The preimage sidesteps it entirely.
        """
        for dtype in (pa.decimal128(38, 9), pa.binary(16), pa.timestamp("us"), pa.list_(pa.int64())):
            assert isinstance(type_token(dtype), str)


class TestNested:
    """Children carry their field name and their nullability."""

    def test_list_child_name_and_nullability(self) -> None:
        """Arrow's default item field is named ``item`` and is nullable."""
        assert type_token(pa.list_(pa.int64())) == "list<item?:int64>"

    def test_non_nullable_child_has_no_marker(self) -> None:
        """The ``?`` marks nullability, so its absence is meaningful."""
        inner = pa.field("item", pa.int64(), nullable=False)
        assert type_token(pa.list_(inner)) == "list<item:int64>"

    def test_list_child_name_is_normalised_away(self) -> None:
        """Arrow's own equality ignores it, so the token must too.

        pyarrow names it ``item``, some Parquet producers name it ``element``.
        Keeping the name would give two ports different hashes for a protocol
        Arrow itself calls identical -- the exact cross-port divergence the
        canonical preimage exists to prevent.
        """
        a = pa.list_(pa.field("item", pa.int64()))
        b = pa.list_(pa.field("element", pa.int64()))
        assert a == b
        assert type_token(a) == type_token(b) == "list<item?:int64>"

    def test_map_child_names_are_normalised_away(self) -> None:
        """Same rule, same reason: Arrow does not consider them part of the type."""
        a = pa.map_(pa.string(), pa.int64())
        b = pa.map_(pa.field("k", pa.string(), nullable=False), pa.field("v", pa.int64()))  # type: ignore[call-overload]
        assert a == b
        assert type_token(a) == type_token(b)

    def test_struct_field_names_are_kept(self) -> None:
        """Arrow *does* consider these part of the type, so they stay."""
        assert pa.struct([pa.field("a", pa.int64())]) != pa.struct([pa.field("b", pa.int64())])
        assert type_token(pa.struct([pa.field("a", pa.int64())])) != type_token(pa.struct([pa.field("b", pa.int64())]))

    def test_child_nullability_is_kept(self) -> None:
        """Nullability is part of the type even where the name is not."""
        assert pa.list_(pa.field("item", pa.int64())) != pa.list_(pa.field("item", pa.int64(), nullable=False))
        assert type_token(pa.list_(pa.int64())) != type_token(pa.list_(pa.field("item", pa.int64(), nullable=False)))

    def test_large_and_fixed_size_lists_are_distinct(self) -> None:
        """Offset width and fixed size both change the type."""
        assert type_token(pa.large_list(pa.int64())) == "large_list<item?:int64>"
        assert type_token(pa.list_(pa.int64(), 4)) == "fixed_size_list(4)<item?:int64>"

    def test_struct_preserves_declaration_order(self) -> None:
        """Field order is significant in Arrow and in the token."""
        a: pa.Field[Any] = pa.field("a", pa.int32(), nullable=False)
        b: pa.Field[Any] = pa.field("b", pa.string())
        ab = pa.struct([a, b])
        ba = pa.struct([b, a])
        assert type_token(ab) == "struct<a:int32,b?:utf8>"
        assert type_token(ab) != type_token(ba)

    def test_empty_struct(self) -> None:
        """A struct with no fields still has a token."""
        assert type_token(pa.struct([])) == "struct<>"

    def test_map_carries_both_children(self) -> None:
        """A map's key is non-nullable in Arrow; the item is not."""
        assert type_token(pa.map_(pa.string(), pa.int64())) == "map<key:utf8,value?:int64>"

    def test_keys_sorted_is_part_of_the_type(self) -> None:
        """Arrow treats keys_sorted as part of the map type."""
        # Positional: the installed stubs spell the keyword ``key_sorted``
        # while the runtime spells it ``keys_sorted``.
        assert type_token(pa.map_(pa.string(), pa.int64(), True)).endswith(",keys_sorted")

    def test_deep_nesting(self) -> None:
        """Nesting composes; the token recurses without special cases."""
        dtype = pa.list_(pa.struct([pa.field("m", pa.map_(pa.string(), pa.list_(pa.int8())))]))
        assert type_token(dtype) == "list<item?:struct<m?:map<key:utf8,value?:list<item?:int8>>>>"


class TestEncodings:
    """Dictionary and union encodings are types, not hints."""

    def test_dictionary_carries_index_and_value(self) -> None:
        """A dictionary<int8,utf8> is not a dictionary<int32,utf8>."""
        assert type_token(pa.dictionary(pa.int8(), pa.string())) == "dictionary<index:int8,value:utf8>"
        assert type_token(pa.dictionary(pa.int8(), pa.string())) != type_token(pa.dictionary(pa.int32(), pa.string()))

    def test_ordered_dictionary_is_distinct(self) -> None:
        """Ordering is part of the Arrow type."""
        assert type_token(pa.dictionary(pa.int8(), pa.string(), ordered=True)).endswith(",ordered")

    def test_union_spells_its_type_codes(self) -> None:
        """Type codes need not be 0..n-1, so position cannot imply them."""
        fields: list[pa.Field[Any]] = [pa.field("a", pa.int64()), pa.field("b", pa.string())]
        token = type_token(pa.union(fields, mode="dense", type_codes=[7, 9]))
        assert token == "dense_union<7=a?:int64,9=b?:utf8>"

    def test_sparse_and_dense_unions_differ(self) -> None:
        """Same children, different layout, different type."""
        fields: list[pa.Field[Any]] = [pa.field("a", pa.int64())]
        sparse = type_token(pa.union(fields, mode="sparse", type_codes=[0]))
        dense = type_token(pa.union(fields, mode="dense", type_codes=[0]))
        assert sparse.startswith("sparse_union") and dense.startswith("dense_union")
        assert sparse != dense


class TestTotality:
    """The function is total over what a protocol can reach, and loud otherwise."""

    def test_unknown_type_raises_rather_than_guessing(self) -> None:
        """A silent ``str(dtype)`` fallback is a one-sided hash divergence.

        It would not fail here -- it would surface as an unexplained mismatch
        at a client talking to a port that spelled it differently.
        """

        class _Fake:
            def __repr__(self) -> str:
                return "not-an-arrow-type"

        with pytest.raises(UnsupportedArrowTypeError, match="no canonical token"):
            type_token(_Fake())  # type: ignore[arg-type]

    def test_every_public_pyarrow_type_has_a_token(self) -> None:
        """Sweep the type universe rather than trusting a hand-written list.

        A new Arrow release adding a type should fail here, which is the
        signal to add the token on every port at once.
        """
        candidates: list[pa.DataType] = []
        for name in dir(pa):
            if name.startswith("_"):
                continue
            factory = getattr(pa, name)
            if not callable(factory):
                continue
            for args in ((), ("us",), (pa.int64(),)):
                try:
                    produced = factory(*args)
                except Exception:
                    continue
                if isinstance(produced, pa.DataType):
                    candidates.append(produced)
                    break

        assert len(candidates) > 20, "the sweep found suspiciously few types"
        for dtype in candidates:
            assert isinstance(type_token(dtype), str), dtype

    def test_tokens_are_lowercase_ascii(self) -> None:
        """Case and non-ASCII are places six ports can silently differ."""
        for dtype in (pa.int64(), pa.timestamp("us", tz="UTC"), pa.list_(pa.string())):
            token = type_token(dtype)
            assert token.isascii()
            assert token == token.lower() or "tz=UTC" in token


class TestFieldAndSchema:
    """Top-level fields and schemas carry nullability alongside the token."""

    def test_field_token_shape(self) -> None:
        """Strings and booleans only -- no numbers reach the preimage."""
        entry = field_token(pa.field("n", pa.int64(), nullable=False))
        assert entry == {"name": "n", "nullable": False, "type": "int64"}

    def test_schema_preserves_declaration_order(self) -> None:
        """Order is significant, so the list is ordered, not sorted."""
        fields: list[pa.Field[Any]] = [pa.field("b", pa.int64()), pa.field("a", pa.string())]
        schema = pa.schema(fields)
        assert [e["name"] for e in schema_tokens(schema)] == ["b", "a"]

    def test_empty_schema(self) -> None:
        """A method with no parameters still describes a schema."""
        assert schema_tokens(pa.schema([])) == []
