# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Canonical text tokens for Arrow types, for the protocol hash preimage.

The protocol hash is taken over what Arrow *decodes to*, not over what an
encoder emits: each language's Arrow implementation may legitimately produce
different bytes for the same logical schema, so a hash over
``schema.serialize()`` is not a cross-language contract.  The preimage is
canonical JSON (RFC 8785) of the decoded description, and these tokens are how
a type appears inside it.

JSON solves framing, escaping and key ordering.  It does not solve spelling --
two ports can agree on every JSON rule and still disagree on whether a
microsecond timestamp is ``timestamp[us]`` or ``timestamp(us)``, which is a
silent hash divergence.  So the vocabulary is enumerated here exhaustively and
:func:`type_token` is total over Arrow's type universe: an unrecognised type
raises rather than falling back to ``str(dtype)``, whose output is an Arrow
implementation detail that differs between ports and across Arrow releases.

**Grammar.**  A token is lowercase ASCII.  Parameters go in parentheses,
children in angle brackets.  A child is ``name:token`` when the child field is
non-nullable and ``name?:token`` when it is nullable -- child nullability is
part of the type in Arrow, and two schemas differing only there are different
schemas.  Decimal precision and scale are folded into the token
(``decimal128(38,9)``) so the preimage contains no JSON numbers and RFC 8785's
hardest rule, number canonicalisation, never applies.  Keep it that way.

**What is normalised.**  Arrow's own type equality ignores the *name* of a
list's child field and of a map's key/value fields -- pyarrow says ``item``,
some Parquet producers say ``element``, and a hand-built type says whatever it
was given.  Those names are normalised to ``item`` / ``key`` / ``value`` so
two ports that default differently do not hash the same protocol differently.
Everything Arrow treats as part of the type is kept: child *nullability*,
struct field names, union child names and type codes.

Examples::

    int64
    utf8
    timestamp(us,tz=UTC)
    decimal128(38,9)
    list<item?:utf8>
    struct<a:int32,b?:utf8>
    map<key:utf8,value?:int64>
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

__all__ = ["TYPE_TOKEN_VERSION", "UnsupportedArrowTypeError", "field_token", "schema_tokens", "type_token"]

#: Bumped only when the *spelling* changes, which rotates every protocol hash.
#: Adding a token for a type that had none is not a bump: nothing that hashed
#: before hashes differently after.
TYPE_TOKEN_VERSION = 1


class UnsupportedArrowTypeError(TypeError):
    """An Arrow type has no canonical token.

    Raised rather than falling back to ``str(dtype)``: a port that silently
    spelled an unknown type its own way would produce a protocol hash that
    disagrees with every other port, and the disagreement would surface as an
    unexplained mismatch at a client rather than as an error here.
    """


# Unit spellings are Arrow's own ("s", "ms", "us", "ns") and are used verbatim.


def _build_primitives() -> dict[str, str]:
    """Map ``str(dtype)`` to a token for every parameterless Arrow type."""
    table: dict[str, str] = {}
    for dtype, token in (
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
    ):
        table[str(dtype)] = token
    # Arrow added the view types in 15.0; treat them as optional so this module
    # imports against older pyarrow rather than failing at import time.
    for factory, token in (("string_view", "utf8_view"), ("binary_view", "binary_view")):
        maker = getattr(pa, factory, None)
        if maker is not None:
            table[str(maker())] = token
    return table


_PRIMITIVE_TOKENS = _build_primitives()


def _child(field: pa.Field[Any]) -> str:
    """Spell one child field as ``name:token`` or ``name?:token``."""
    return f"{field.name}{'?' if field.nullable else ''}:{type_token(field.type)}"


def _anon(field: pa.Field[Any], name: str) -> str:
    """Spell a child whose name Arrow does not consider part of the type.

    A list's child is named ``item`` by pyarrow, ``element`` by some Parquet
    producers, and whatever the caller passed by anyone constructing the type
    by hand -- and Arrow's own type equality ignores all of it.  Normalising to
    a fixed name is what keeps two ports that default differently from hashing
    the same protocol differently.  Nullability *is* part of the type, so it is
    kept.
    """
    return f"{name}{'?' if field.nullable else ''}:{type_token(field.type)}"


def type_token(dtype: pa.DataType) -> str:
    """Return the canonical token for *dtype*.

    Args:
        dtype: Any Arrow data type.

    Returns:
        The token, as documented in this module's grammar.

    Raises:
        UnsupportedArrowTypeError: The type has no canonical spelling.  A port
            meeting this must add the token on every port at once, since a
            one-sided addition changes that port's protocol hash alone.

    """
    if not isinstance(dtype, pa.DataType):
        raise UnsupportedArrowTypeError(f"{dtype!r} is not an Arrow type, so it has no canonical token.")

    primitive = _PRIMITIVE_TOKENS.get(str(dtype))
    if primitive is not None:
        return primitive

    if pa.types.is_fixed_size_binary(dtype):
        return f"fixed_size_binary({dtype.byte_width})"
    if pa.types.is_decimal128(dtype):
        return f"decimal128({dtype.precision},{dtype.scale})"
    if pa.types.is_decimal256(dtype):
        return f"decimal256({dtype.precision},{dtype.scale})"
    if pa.types.is_time32(dtype) or pa.types.is_time64(dtype):
        return f"time{dtype.bit_width}({dtype.unit})"
    if pa.types.is_timestamp(dtype):
        # The zone is carried verbatim: "UTC" and "+00:00" are distinct Arrow
        # types and must not collapse to one token.
        return f"timestamp({dtype.unit})" if dtype.tz is None else f"timestamp({dtype.unit},tz={dtype.tz})"
    if pa.types.is_duration(dtype):
        return f"duration({dtype.unit})"
    if pa.types.is_interval(dtype):
        # Only month_day_nano is constructible from pyarrow's public API; the
        # other two arrive from other ports' IPC.
        return {"month_interval": "interval_months", "day_time_interval": "interval_day_time"}.get(
            str(dtype), "interval_month_day_nano"
        )

    if pa.types.is_fixed_size_list(dtype):
        return f"fixed_size_list({dtype.list_size})<{_anon(dtype.value_field, 'item')}>"
    if pa.types.is_large_list(dtype):
        return f"large_list<{_anon(dtype.value_field, 'item')}>"
    if pa.types.is_list(dtype):
        return f"list<{_anon(dtype.value_field, 'item')}>"
    if getattr(pa.types, "is_large_list_view", None) is not None and pa.types.is_large_list_view(dtype):
        return f"large_list_view<{_anon(dtype.value_field, 'item')}>"
    if getattr(pa.types, "is_list_view", None) is not None and pa.types.is_list_view(dtype):
        return f"list_view<{_anon(dtype.value_field, 'item')}>"

    if pa.types.is_map(dtype):
        # keys_sorted is part of the type in Arrow, so it is part of the token.
        sorted_flag = ",keys_sorted" if dtype.keys_sorted else ""
        return f"map<{_anon(dtype.key_field, 'key')},{_anon(dtype.item_field, 'value')}>{sorted_flag}"

    if pa.types.is_struct(dtype):
        return f"struct<{','.join(_child(dtype.field(i)) for i in range(dtype.num_fields))}>"

    if pa.types.is_union(dtype):
        kind = "sparse_union" if dtype.mode == "sparse" else "dense_union"
        # Type codes are part of the type and need not be 0..n-1, so they are
        # spelled rather than implied by position.
        parts = [f"{code}={_child(dtype.field(i))}" for i, code in enumerate(dtype.type_codes)]
        return f"{kind}<{','.join(parts)}>"

    if pa.types.is_dictionary(dtype):
        ordered = ",ordered" if dtype.ordered else ""
        return f"dictionary<index:{type_token(dtype.index_type)},value:{type_token(dtype.value_type)}>{ordered}"

    if getattr(pa.types, "is_run_end_encoded", None) is not None and pa.types.is_run_end_encoded(dtype):
        return f"run_end_encoded<run_ends:{type_token(dtype.run_end_type)},values:{type_token(dtype.value_type)}>"

    if isinstance(dtype, pa.BaseExtensionType):
        # The extension name plus its storage: a reader without the extension
        # registered still sees a type it can compare.
        return f"extension({dtype.extension_name})<{type_token(dtype.storage_type)}>"

    raise UnsupportedArrowTypeError(
        f"Arrow type {dtype!r} has no canonical token. Add one to "
        f"vgi_rpc/rpc/_type_tokens.py and to every other port at the same time: "
        f"a one-sided addition changes only that port's protocol hash."
    )


def field_token(field: pa.Field[Any]) -> dict[str, object]:
    """Describe one top-level schema field for the hash preimage.

    Args:
        field: The field to describe.

    Returns:
        ``{"name": ..., "nullable": ..., "type": ...}`` -- strings and booleans
        only, so the preimage carries no JSON numbers.

    """
    return {"name": field.name, "nullable": field.nullable, "type": type_token(field.type)}


def schema_tokens(schema: pa.Schema) -> list[dict[str, object]]:
    """Describe a schema's fields in declaration order, which is significant."""
    return [field_token(schema.field(i)) for i in range(len(schema))]
