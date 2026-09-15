# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The protocol hash: a fingerprint of a protocol's wire surface.

A client and a worker agree on a protocol or they do not, and the hash is how
either side says which one it has without shipping the whole description.  For
that to be worth anything the same protocol must hash the same in every port,
which the previous definition could not promise: it hashed
``schema.serialize()`` bytes, and each language's Arrow implementation may
legitimately emit different bytes for the same logical schema.  The docs said
so, which made the field advisory -- comparable only against itself.

So the preimage is canonical JSON of what Arrow *decodes to*::

    sha256(b"vgi_rpc.protocol_hash.v1|" + canonical_json(description))

**Profile: RFC 8785 (JCS)**, chosen for its published test vectors.  The
structure is deliberately restricted to objects, arrays, strings and booleans;
every number is folded into a type token (``decimal128(38,9)``), so JCS's
hardest rule -- number canonicalisation, and the likeliest place for six ports
to diverge -- never applies.  Keep it that way.

**What is in the preimage.**  The protocol's wire name, and for each method
(sorted by name) its name, method type, the three shape booleans, and its
parameter, result and header schemas as ordered field lists.  Field order
within a schema is declaration order and is significant.

**What is not.**  Server identity, docstrings, parameter defaults, Python type
names, and the framework's own ``REQUEST_VERSION`` / ``DESCRIBE_VERSION``.
Those vary across processes, builds and ports without changing what is on the
wire.  The ``v1`` domain tag is the only version the hash carries, and it moves
only when the hash *definition* moves.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

import pyarrow as pa

from ._type_tokens import schema_tokens

__all__ = ["HASH_DOMAIN", "canonical_json", "compute_protocol_hash", "protocol_description"]

#: Domain separator.  Moves only when the hash definition moves, never when a
#: protocol changes -- that is what the hash itself is for.
HASH_DOMAIN = b"vgi_rpc.protocol_hash.v1|"


def canonical_json(value: Any) -> bytes:
    """Serialize *value* as RFC 8785 canonical JSON.

    Object keys are sorted by their UTF-16 code units, which for the ASCII key
    set used here is the same order Python's ``sort_keys`` produces.  Output is
    UTF-8 with no insignificant whitespace.

    Args:
        value: Objects, arrays, strings and booleans only.

    Returns:
        The canonical UTF-8 encoding.

    Raises:
        TypeError: The value contains a number.  JCS's number rules are the
            hardest part of the spec to implement identically in six
            languages, and the preimage is designed so they never apply -- a
            number reaching here means a type parameter leaked out of its
            token.

    """
    _reject_numbers(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _reject_numbers(value: Any) -> None:
    """Walk *value*, refusing any number so JCS number rules stay unreachable."""
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return
    if isinstance(value, (int, float)):
        raise TypeError(
            f"The protocol-hash preimage carries no numbers, but found {value!r}. "
            f"Fold it into a type token (e.g. 'decimal128(38,9)') instead."
        )
    if isinstance(value, Mapping):
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(f"Object keys must be strings, found {k!r}.")
            _reject_numbers(v)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _reject_numbers(item)
        return
    raise TypeError(f"Unsupported value in the protocol-hash preimage: {value!r}")


def _method_entry(
    name: str,
    method_type: str,
    *,
    has_return: bool,
    has_header: bool,
    is_exchange: bool,
    params_schema: pa.Schema,
    result_schema: pa.Schema | None,
    header_schema: pa.Schema | None,
) -> dict[str, Any]:
    """Describe one method for the preimage."""
    entry: dict[str, Any] = {
        "name": name,
        "type": method_type,
        "has_return": has_return,
        "has_header": has_header,
        "is_exchange": is_exchange,
        "params": schema_tokens(params_schema),
    }
    # Absent and empty are different: a method returning nothing is not a
    # method returning an empty struct, and they must not hash alike.
    if result_schema is not None:
        entry["result"] = schema_tokens(result_schema)
    if header_schema is not None:
        entry["header"] = schema_tokens(header_schema)
    return entry


def protocol_description(protocol_name: str, methods: Mapping[str, Any]) -> dict[str, Any]:
    """Build the hash preimage for one protocol.

    Args:
        protocol_name: The protocol's wire name -- its routing key, which
            carries the major version, so an incompatible major is already a
            different preimage.
        methods: The protocol's method table, as ``RpcMethodInfo`` values.

    Returns:
        A structure of objects, arrays, strings and booleans, ready for
        :func:`canonical_json`.  Methods are sorted by name; field order within
        each schema is declaration order and is preserved.

    """
    return {
        "protocol": protocol_name,
        "methods": [
            _method_entry(
                info.name,
                info.method_type.value,
                has_return=bool(info.has_return),
                has_header=info.header_type is not None,
                is_exchange=bool(info.is_exchange),
                params_schema=info.params_schema,
                result_schema=info.result_schema if info.has_return else None,
                header_schema=info.header_type.ARROW_SCHEMA if info.header_type is not None else None,
            )
            for _, info in sorted(methods.items())
        ],
    }


def compute_protocol_hash(protocol_name: str, methods: Mapping[str, Any]) -> str:
    """Return the SHA-256 hex digest of a protocol's canonical description.

    Args:
        protocol_name: The protocol's wire name.
        methods: The protocol's method table.

    Returns:
        Lowercase 64-character hex digest.  Identical in every port for the
        same protocol -- which is a property conformance can assert, and could
        not before.

    """
    preimage = canonical_json(protocol_description(protocol_name, methods))
    return hashlib.sha256(HASH_DOMAIN + preimage).hexdigest()
