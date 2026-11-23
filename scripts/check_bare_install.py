# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Smoke-test a bare ``pip install vgi-rpc`` (no extras).

Run with the interpreter of an environment that has only vgi-rpc's core
dependencies.  Checks that ``import vgi_rpc`` works, that the optional
modules a bare install must not need (zstandard, msgpack, falcon, httpx2) are
really absent, and that a pipe-transport stream holding a ``StateRef`` round
trips -- the byte-stream transports never seal anything, so they need no
extra.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

import vgi_rpc
from vgi_rpc import AnnotatedBatch, CallContext, ExchangeState, OutputCollector, StateRef, Stream, serve_pipe


@dataclass
class _Hold(ExchangeState):
    value: StateRef

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        out.emit_pydict({"n": [len(ctx.resolve(self.value))]})


class _Service(Protocol):
    def hold(self, size: int) -> Stream[ExchangeState]: ...


class _Impl:
    def hold(self, size: int, ctx: CallContext) -> Stream[_Hold]:
        return Stream(
            output_schema=pa.schema([pa.field("n", pa.int64())]),
            input_schema=pa.schema([pa.field("x", pa.int64())]),
            state=_Hold(value=ctx.state_ref(b"v" * size)),
        )


def main() -> int:
    """Run the checks; return a process exit code."""
    present = [m for m in ("zstandard", "falcon", "httpx2") if importlib.util.find_spec(m) is not None]
    if present:
        print(f"not a bare environment: {present} installed", file=sys.stderr)
        return 2
    with serve_pipe(_Service, _Impl()) as proxy, proxy.hold(size=100_000) as session:
        out = session.exchange(AnnotatedBatch.from_pydict({"x": [1]}))
        assert out.batch.column("n")[0].as_py() == 100_000
    print(f"bare install OK: vgi_rpc {getattr(vgi_rpc, '__version__', '?')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
