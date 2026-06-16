# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""SPIKE: stateful property testing of the shared-memory first-fit allocator.

Drives random ``allocate``/``free`` sequences against a real ``ShmAllocator``
(backed by a plain bytearray — no OS shared memory needed) and a Python model
of the live regions, asserting the core safety invariants after every step:
no overlap, in-bounds, sorted, header count consistent, and "allocate returns
None only when no gap actually fits".
"""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, consumes, invariant, multiple, rule

from vgi_rpc.shm import HEADER_SIZE, ShmAllocator

_DATA_SIZE = 2048  # small data region so gaps fill and first-fit reuse is exercised
_TOTAL = HEADER_SIZE + _DATA_SIZE


class ShmAllocatorMachine(RuleBasedStateMachine):
    """Random alloc/free sequences vs. a Python model of live regions."""

    live = Bundle("live")

    def __init__(self) -> None:
        """Initialize a fresh allocator over a zeroed bytearray segment."""
        super().__init__()
        self._buf = bytearray(_TOTAL)
        mv = memoryview(self._buf)
        ShmAllocator.initialize(mv, _TOTAL)
        self._alloc = ShmAllocator(mv, _TOTAL)
        self._model: dict[int, int] = {}  # offset -> size

    def _max_gap(self) -> int:
        prev = HEADER_SIZE
        biggest = 0
        for off, size in sorted(self._model.items()):
            biggest = max(biggest, off - prev)
            prev = off + size
        return max(biggest, _TOTAL - prev)

    @rule(target=live, size=st.integers(min_value=1, max_value=256))
    def allocate(self, size: int) -> object:
        """Allocate; on success verify in-bounds + non-overlap, else verify no gap fit."""
        off = self._alloc.allocate(size)
        if off is None:
            assert self._max_gap() < size, f"allocate({size}) returned None but a {self._max_gap()}-byte gap exists"
            return multiple()
        assert off >= HEADER_SIZE
        assert off + size <= _TOTAL
        for o2, s2 in self._model.items():
            assert off + size <= o2 or o2 + s2 <= off, f"overlap: [{off},{off + size}) vs [{o2},{o2 + s2})"
        self._model[off] = size
        return off

    @rule(off=consumes(live))
    def free(self, off: int) -> None:
        """Free a live allocation and drop it from the model."""
        self._alloc.free(off)
        del self._model[off]

    @invariant()
    def header_matches_model(self) -> None:
        """Check the on-header allocation list mirrors the model and stays well-formed."""
        allocs = self._alloc._read_allocs()
        assert self._alloc.num_allocs == len(allocs) == len(self._model)
        assert allocs == sorted(allocs), "allocation list not sorted by offset"
        assert dict(allocs) == self._model
        prev = HEADER_SIZE
        for off, length in allocs:
            assert off >= prev, "regions out of order / overlapping"
            assert off + length <= _TOTAL, "region past end of segment"
            prev = off + length


TestShmAllocator = ShmAllocatorMachine.TestCase
TestShmAllocator.settings = settings(max_examples=75, stateful_step_count=40, deadline=None)


def test_free_unknown_offset_raises() -> None:
    """Freeing an offset that was never allocated raises ValueError."""
    buf = bytearray(_TOTAL)
    mv = memoryview(buf)
    ShmAllocator.initialize(mv, _TOTAL)
    alloc = ShmAllocator(mv, _TOTAL)
    off = alloc.allocate(64)
    assert off is not None
    with pytest.raises(ValueError, match="No allocation at offset"):
        alloc.free(off + 1)
