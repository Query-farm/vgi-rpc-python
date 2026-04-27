"""Pin the SHM segment header byte layout against cross-language drift.

The SHM segment header (magic, version, data_size, num_allocs, padding,
followed by an array of (offset, length) entries) is a hand-rolled
binary format shared with the Rust port at
``~/Development/vgi-rpc-rust/vgi-rpc/src/shm.rs`` (and any future Go /
Java port). It is **not** delegated to arrow-ipc — different processes
(potentially in different languages) attach to the same mmap and read
each other's allocator state directly.

Different Arrow IPC writers can legitimately produce different bytes
for the same record batch (alignment padding, buffer ordering, etc.)
and that's fine because each side decodes via arrow-ipc. But the
allocator header must be byte-identical, because we own the format.

This test pins the layout via a fixed hex golden. The mirror Rust test
``shm::tests::header_layout_matches_canonical_golden_bytes`` asserts
the same hex from the Rust side. If both pass, Python and Rust agree
on the layout.

If you intentionally change the layout (e.g. bumping ``_VERSION``),
update both this test and the Rust mirror together.
"""

from __future__ import annotations

import struct

from vgi_rpc.shm import HEADER_SIZE, ShmSegment

# State pinned by this golden. We pick a 16 KiB-aligned request size so
# the post-rounding ``data_size`` field is deterministic across both
# Linux (4 KiB pages) and macOS Apple Silicon (16 KiB pages).
#
#   requested  = HEADER_SIZE + 16384
#   data_size  = 16384  (no platform rounding required)
#   num_allocs = 2
#   entry 0    = (offset=HEADER_SIZE, length=100)
#   entry 1    = (offset=HEADER_SIZE+256, length=50)
#
# Layout (little-endian):
#   magic       4s  = b"VGIS"
#   version     I   = 1
#   data_size   Q   = 16384
#   num_allocs  I   = 2
#   padding     I   = 0
#   entry 0     QQ  = (65536, 100)
#   entry 1     QQ  = (65792, 50)
SHM_HEADER_GOLDEN_HEX = (
    "5647495301000000004000000000000002000000000000000000010000000000640000000000000000010100000000003200000000000000"
)


def test_header_layout_matches_canonical_golden_bytes() -> None:
    """Drive a real segment into the pinned state and compare bytes."""
    seg = ShmSegment.create(HEADER_SIZE + 16384)
    try:
        # Drive the allocator into the golden state. First-fit places
        # entries adjacently, so use a sacrificial middle alloc to
        # leave a gap between entry 0 and entry 1.
        a = seg._allocator.allocate(100)  # at HEADER_SIZE
        gap = seg._allocator.allocate(156)  # at HEADER_SIZE + 100
        b = seg._allocator.allocate(50)  # at HEADER_SIZE + 256
        assert a is not None and gap is not None and b is not None
        seg._allocator.free(gap)
        assert a == HEADER_SIZE
        assert b == HEADER_SIZE + 256

        # Compare the first 56 bytes (24-byte fixed header + two 16-byte
        # entries) against the golden.
        buf = seg._shm.buf
        assert buf is not None
        observed = bytes(buf[:56])
        expected = bytes.fromhex(SHM_HEADER_GOLDEN_HEX)
        assert observed == expected, (
            f"SHM header layout drifted from canonical golden.\n"
            f"  observed: {observed.hex()}\n"
            f"  expected: {expected.hex()}\n"
            f"If this is intentional, update both this test and the "
            f"Rust mirror in vgi-rpc-rust/vgi-rpc/src/shm.rs."
        )
    finally:
        seg.close()
        seg.unlink()


def test_header_struct_format_constants() -> None:
    """Sanity-check that the struct format constants haven't been edited.

    These format strings are part of the cross-language wire contract.
    """
    # Fixed header: 4s + I + Q + I + I = 4+4+8+4+4 = 24 bytes
    assert struct.calcsize("<4sIQII") == 24
    # Allocation entry: Q + Q = 16 bytes
    assert struct.calcsize("<QQ") == 16
    # MAX_ALLOCS = (65536 - 24) // 16 = 4094
    assert (HEADER_SIZE - 24) // 16 == 4094
