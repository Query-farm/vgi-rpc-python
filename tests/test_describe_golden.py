# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Golden snapshot of the ``__describe__`` payload and ``protocol_hash``.

This exists so a change to the wire-visible description arrives as a **reviewed
diff** rather than a silently regenerated file. Six language ports reimplement
this payload and its digest; the ports have drifted before without anyone
noticing, and a snapshot is the cheapest thing that makes drift visible at the
moment it is introduced rather than months later.

The rendering is deliberately human-readable rather than raw IPC bytes: the
point is that a reviewer can see *what* moved. ``server_id`` is excluded
because it is random per process.

When a change is intentional::

    uv run python tests/test_describe_golden.py --update
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.introspect import build_describe_batch
from vgi_rpc.metadata import PROTOCOL_HASH_KEY
from vgi_rpc.rpc._types import rpc_methods

_GOLDEN = Path(__file__).parent / "golden" / "describe_conformance.txt"
#: Fixed so the digest and rendering are reproducible; the real value is random.
_SERVER_ID = "0" * 12


def _render_schema(schema: pa.Schema) -> str:
    """One line per field: name, nullability, type."""
    if len(schema) == 0:
        return "      (empty)"
    return "\n".join(f"      {f.name}: {f.type}{'' if f.nullable else ' not null'}" for f in schema)


def render() -> str:
    """Render the describe payload in a stable, reviewable form."""
    methods = rpc_methods(ConformanceService)
    batch, md = build_describe_batch(
        ConformanceService.__name__,
        methods,
        _SERVER_ID,
        getattr(ConformanceService, "protocol_version", None),
    )
    out: list[str] = [
        "# Golden describe payload — see tests/test_describe_golden.py",
        f"protocol: {ConformanceService.__name__}",
        f"protocol_hash: {md.get(PROTOCOL_HASH_KEY, b'').decode()}",
        f"method_count: {batch.num_rows}",
        "",
    ]
    cols: dict[str, list[object]] = {f.name: batch.column(i).to_pylist() for i, f in enumerate(batch.schema)}
    for row in range(batch.num_rows):
        method_name = str(cols["name"][row])
        out.append(method_name)
        out.append(f"  method_type: {cols['method_type'][row]}")
        out.append(f"  has_return: {cols['has_return'][row]}")
        out.append(f"  has_header: {cols['has_header'][row]}")
        out.append(f"  is_exchange: {cols['is_exchange'][row]}")
        info = methods[method_name]
        out.append("  params:")
        out.append(_render_schema(info.params_schema))
        out.append("  result:")
        out.append(_render_schema(info.result_schema))
        out.append("")
    return "\n".join(out)


def test_describe_payload_matches_golden() -> None:
    """The describe payload and its hash are unchanged.

    A failure here is not necessarily a bug — it means the wire-visible
    description moved. Confirm the change is intended, then re-baseline with
    ``--update`` so the diff lands in the same commit as the change that caused
    it.
    """
    current = render()
    assert _GOLDEN.exists(), f"golden missing; create it with: uv run python {__file__} --update"
    expected = _GOLDEN.read_text()
    assert current == expected, (
        "The __describe__ payload or protocol_hash changed.\n"
        "If intentional, re-baseline:\n"
        f"  uv run python {__file__} --update\n"
        "and review the diff — six ports reimplement this payload."
    )


if __name__ == "__main__":
    if "--update" in sys.argv:
        _GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        _GOLDEN.write_text(render())
        print(f"wrote {_GOLDEN}")
    else:
        print(render())
