# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Golden snapshot of the reflection description of the conformance service.

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

from vgi_rpc.conformance._impl import ConformanceServiceImpl
from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc import RpcServer
from vgi_rpc.rpc._reflection import Reflection, ReflectionImpl

_GOLDEN = Path(__file__).parent / "golden" / "describe_conformance.txt"
#: Fixed so the digest and rendering are reproducible; the real value is random.
_SERVER_ID = "0" * 12


def _render_schema(schema: pa.Schema) -> str:
    """One line per field: name, nullability, type."""
    if len(schema) == 0:
        return "      (empty)"
    return "\n".join(f"      {f.name}: {f.type}{'' if f.nullable else ' not null'}" for f in schema)


def render() -> str:
    """Render the reflection description in a stable, reviewable form."""
    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        server_id=_SERVER_ID,
        enable_describe=True,
    )
    reflection = server.bindings[Reflection.protocol_name].impl
    assert isinstance(reflection, ReflectionImpl)
    desc = reflection.describe(server.protocol_name)

    out: list[str] = [
        "# Golden reflection description — see tests/test_describe_golden.py",
        f"protocol: {desc.protocol}",
        f"protocol_version: {desc.protocol_version}",
        f"protocol_hash: {desc.protocol_hash}",
        f"method_count: {len(desc.methods)}",
        "",
    ]
    for method in desc.methods:
        out.append(method.name)
        out.append(f"  method_type: {method.method_type}")
        out.append(f"  has_return: {method.has_return}")
        out.append(f"  has_header: {method.has_header}")
        out.append(f"  stream_kind: {method.stream_kind or '(n/a)'}")
        out.append(f"  idempotency: {method.idempotency}")
        out.append("  params:")
        out.append(_render_schema(_read(method.params_schema_ipc)))
        out.append("  result:")
        out.append(_render_schema(_read(method.result_schema_ipc)))
        out.append("")
    return "\n".join(out)


def _read(ipc_bytes: bytes) -> pa.Schema:
    """Read a schema from IPC bytes, treating empty as the empty schema."""
    if not ipc_bytes:
        return pa.schema([])
    return pa.ipc.read_schema(pa.py_buffer(ipc_bytes))


def test_describe_payload_matches_golden() -> None:
    """The reflection description and its hash are unchanged.

    A failure here is not necessarily a bug — it means the wire-visible
    description moved. Confirm the change is intended, then re-baseline with
    ``--update`` so the diff lands in the same commit as the change that caused
    it.
    """
    current = render()
    assert _GOLDEN.exists(), f"golden missing; create it with: uv run python {__file__} --update"
    # Explicit UTF-8, not the platform default: the rendering's header carries an
    # em-dash, and on Windows the locale encoding decodes it to a replacement
    # character, which reads as "the description moved" when nothing moved.
    expected = _GOLDEN.read_text(encoding="utf-8")
    assert current == expected, (
        "The reflection description or protocol_hash changed.\n"
        "If intentional, re-baseline:\n"
        f"  uv run python {__file__} --update\n"
        "and review the diff — six ports reimplement this payload."
    )


if __name__ == "__main__":
    if "--update" in sys.argv:
        _GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        # ``newline`` pins LF so re-baselining from Windows does not rewrite every
        # line ending and present it as the diff.
        _GOLDEN.write_text(render(), encoding="utf-8", newline="\n")
        print(f"wrote {_GOLDEN}")
    else:
        print(render())
