"""Cross-language SHM allocator compatibility test.

The SHM segment header is shared mmap state that any conforming peer
(Python, Rust, Go) must agree on. This test creates a segment from
Python with a known allocator state, then runs the Rust port's
``shm_dump`` example binary which attaches to the same segment and
emits its view as JSON. We assert the Rust attach sees byte-identical
allocator state.

**Page-alignment behavior.** macOS rounds shared-memory mmap sizes up
to a page boundary (16 KiB on Apple Silicon). ``ShmSegment.create``
writes the actual post-rounding ``shm.size - HEADER_SIZE`` into the
header (see commit notes in ``vgi_rpc/shm.py``), so cross-language
peers attaching with the actual ``shm.size`` see a consistent
``data_size`` field regardless of whether the original request was
page-aligned. Tests here intentionally include both aligned and
non-aligned sizes to exercise this path.

Skipped when the Rust port is unavailable. To run, build the example::

    cd ~/Development/vgi-rpc-rust
    cargo build --release --example shm_dump --features shm

The binary path can be overridden with ``VGI_RPC_RUST_SHM_DUMP``.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from vgi_rpc.shm import HEADER_SIZE, ShmSegment

_DEFAULT_BIN = Path.home() / "Development" / "vgi-rpc-rust" / "target" / "release" / "examples" / "shm_dump"


def _bin_path() -> Path:
    override = os.environ.get("VGI_RPC_RUST_SHM_DUMP")
    return Path(override) if override else _DEFAULT_BIN


def _require_binary() -> Path:
    bin_path = _bin_path()
    if not bin_path.exists():
        pytest.skip(
            f"Rust shm_dump binary not found at {bin_path}; "
            f"build with `cargo build --release --example shm_dump "
            f"--features shm` in vgi-rpc-rust",
        )
    return bin_path


def _dump(seg: ShmSegment) -> dict[str, object]:
    bin_path = _require_binary()
    result = subprocess.run(
        [str(bin_path), "--name", seg.name, "--size", str(seg.size)],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    parsed: dict[str, object] = json.loads(result.stdout.strip())
    return parsed


def test_rust_sees_pythons_empty_segment() -> None:
    """Rust attaches to an empty Python-created segment and agrees."""
    seg = ShmSegment.create(HEADER_SIZE + 64 * 1024)
    try:
        view = _dump(seg)
        assert view["size"] == seg.size
        assert view["data_size"] == seg.size - HEADER_SIZE
        assert view["num_allocs"] == 0
        assert view["allocs"] == []
    finally:
        seg.close()
        seg.unlink()


def test_rust_sees_pythons_allocated_regions() -> None:
    """Python populates the header; Rust must see the same allocs."""
    seg = ShmSegment.create(HEADER_SIZE + 16 * 1024)
    try:
        a = seg._allocator.allocate(256)
        b = seg._allocator.allocate(512)
        c = seg._allocator.allocate(128)
        assert a is not None and b is not None and c is not None
        # Free the middle entry to make the test more interesting.
        seg._allocator.free(b)
        view = _dump(seg)
        assert view["num_allocs"] == 2
        # Order in the header is preserved in the JSON list.
        assert view["allocs"] == [[a, 256], [c, 128]]
    finally:
        seg.close()
        seg.unlink()


def test_rust_sees_pythons_full_allocation_table() -> None:
    """Many small allocations — Rust must agree on every entry."""
    n_allocs = 64
    bytes_per = 32
    # Deliberately non-page-aligned to exercise the OS-rounding fix in
    # ``ShmSegment.create``: data_size must reflect the actual mmap
    # size, not the requested one, otherwise Rust's attach would error.
    seg = ShmSegment.create(HEADER_SIZE + n_allocs * bytes_per * 2)
    try:
        offsets = []
        for _ in range(n_allocs):
            off = seg._allocator.allocate(bytes_per)
            assert off is not None
            offsets.append(off)
        view = _dump(seg)
        assert view["num_allocs"] == n_allocs
        assert view["allocs"] == [[off, bytes_per] for off in offsets]
    finally:
        seg.close()
        seg.unlink()
