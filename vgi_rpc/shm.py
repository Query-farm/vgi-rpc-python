"""Shared memory transport support for Arrow IPC batches.

Provides a shared-memory side-channel for zero-copy batch transfer between
co-located processes.  The allocator uses a bump-pointer list stored in a
fixed-size header at the start of the shared memory segment.  The lockstep
RPC protocol guarantees only one side is active at a time, so no locking
is needed.

Header Format (language-agnostic, struct-based)
------------------------------------------------
::

    Offset  Size  Field
    0       4     magic: b"VGIS"
    4       4     version: uint32 = 1
    8       8     data_size: uint64 (segment size minus header)
    16      4     num_allocs: uint32 (number of allocated regions)
    20      4     padding
    24      N*16  allocations: array of (offset: uint64, length: uint64),
                  sorted by offset

All integers are little-endian for C++ interop.  Offsets are absolute
(from start of shm segment).  Data region starts at ``HEADER_SIZE``.
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from io import RawIOBase
from multiprocessing.shared_memory import SharedMemory

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.metadata import (
    LOG_LEVEL_KEY,
    SHM_LENGTH_KEY,
    SHM_OFFSET_KEY,
    SHM_SOURCE_KEY,
    merge_metadata,
    strip_keys,
)

__all__ = [
    "ShmAllocator",
    "ShmSegment",
    "is_shm_pointer_batch",
    "make_shm_pointer_batch",
    "maybe_write_to_shm",
    "resolve_shm_batch",
]

HEADER_SIZE = 4096
_MAGIC = b"VGIS"
_VERSION = 1

# struct layout for the fixed header fields
_HEADER_FMT = "<4sIQII"  # magic(4s), version(I), data_size(Q), num_allocs(I), padding(I)
_HEADER_STRUCT = struct.Struct(_HEADER_FMT)
assert _HEADER_STRUCT.size == 24

# Each allocation entry: (offset: uint64, length: uint64)
_ALLOC_FMT = "<QQ"
_ALLOC_STRUCT = struct.Struct(_ALLOC_FMT)
assert _ALLOC_STRUCT.size == 16

MAX_ALLOCS = (HEADER_SIZE - _HEADER_STRUCT.size) // _ALLOC_STRUCT.size  # 254

# IPC stream EOS marker: continuation token (0xFFFFFFFF) + 0-length metadata
_IPC_EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"

# Overhead for IPC stream framing (schema message + EOS) added to
# ipc.get_record_batch_size() when estimating the allocation size.
_STREAM_OVERHEAD = 4096


def _has_dictionary_columns(schema: pa.Schema) -> bool:
    """Check if any top-level field uses dictionary encoding."""
    return any(pa.types.is_dictionary(f.type) for f in schema)


# ---------------------------------------------------------------------------
# _ShmSink — file-like wrapper for direct IPC writes into shared memory
# ---------------------------------------------------------------------------


class _ShmSink(RawIOBase):
    """Writable file-like object targeting a shared memory region.

    Arrow's C++ IPC writer calls ``write()`` with ``pa.Buffer`` objects
    (memoryview format ``'b'``, signed).  We cast to unsigned ``'B'``
    before writing to the SHM memoryview.

    Inherits from ``RawIOBase`` to satisfy ``ipc.new_stream()`` type
    requirements.
    """

    def __init__(self, buf: memoryview, start: int) -> None:
        """Initialize targeting *buf* starting at byte offset *start*."""
        super().__init__()
        self._buf = buf
        self._pos = start
        self._start = start

    def write(self, data: bytes | bytearray | memoryview | pa.Buffer) -> int:  # type: ignore[override]
        """Write *data* into the shared memory region."""
        if isinstance(data, pa.Buffer):
            mv = memoryview(data).cast("B")
        elif isinstance(data, (bytes, bytearray)):
            mv = memoryview(data)
        else:
            mv = memoryview(data).cast("B") if data.format != "B" else data
        n = len(mv)
        self._buf[self._pos : self._pos + n] = mv
        self._pos += n
        return n

    def writable(self) -> bool:
        """Return True (this is a writable stream)."""
        return True

    @property
    def bytes_written(self) -> int:
        """Total bytes written so far."""
        return self._pos - self._start


def _serialize_for_shm(batch: pa.RecordBatch) -> pa.Buffer:
    """Serialize a dictionary-encoded batch for SHM storage.

    Writes a full IPC stream, then strips the schema message and EOS
    marker — keeping only the dictionary messages and the record batch
    message.

    Non-dictionary batches should use the direct-write path in
    ``ShmSegment.allocate_and_write`` instead.
    """
    stream_sink = pa.BufferOutputStream()
    writer = ipc.new_stream(stream_sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    stream_buf = stream_sink.getvalue()

    # Skip first message (schema), keep dictionary + record batch messages
    reader = pa.BufferReader(stream_buf)
    pa.ipc.read_message(reader)  # skip schema

    kept_sink = pa.BufferOutputStream()
    try:
        while True:
            msg = pa.ipc.read_message(reader)
            if msg is None:  # EOS
                break
            msg.serialize_to(kept_sink)
    except EOFError:
        pass  # end of stream
    return kept_sink.getvalue()


def _deserialize_from_shm(buf: pa.Buffer, schema: pa.Schema) -> pa.RecordBatch:
    """Deserialize a batch from SHM storage.

    For non-dictionary schemas, reads a full IPC stream written by
    ``_ShmSink`` + ``ipc.new_stream()`` via ``ipc.open_stream()``.

    For dictionary schemas, reconstructs a valid IPC stream by prepending
    a schema message and appending an EOS marker, then reads via
    ``ipc.open_stream()``.
    """
    if not _has_dictionary_columns(schema):
        reader = ipc.open_stream(buf)
        return reader.read_next_batch()

    # Dictionary path: reconstruct full IPC stream
    # 1. Create schema message bytes (new_stream + close = schema + EOS)
    schema_sink = pa.BufferOutputStream()
    schema_writer = ipc.new_stream(schema_sink, schema)
    schema_writer.close()
    schema_stream = schema_sink.getvalue().to_pybytes()
    schema_msg = schema_stream[: -len(_IPC_EOS)]  # strip trailing EOS

    # 2. Combine: schema_msg + dict/batch messages from SHM + EOS
    combined = schema_msg + buf.to_pybytes() + _IPC_EOS
    reader = ipc.open_stream(pa.py_buffer(combined))
    return reader.read_next_batch()


# ---------------------------------------------------------------------------
# ShmAllocator — bump-pointer list stored in shm header
# ---------------------------------------------------------------------------


class ShmAllocator:
    """First-fit bump-pointer allocator stored in shared memory header.

    The allocation list is kept sorted by offset.  Allocation scans for
    the first gap that fits; freeing removes the entry by offset.
    """

    __slots__ = ("_buf", "_total_size")

    def __init__(self, buf: memoryview, total_size: int) -> None:
        """Attach to an existing header in *buf*."""
        self._buf = buf
        self._total_size = total_size
        magic, version, data_size, _, _ = _HEADER_STRUCT.unpack_from(buf, 0)
        if magic != _MAGIC:
            raise ValueError(f"Bad SHM magic: {magic!r}")
        if version != _VERSION:
            raise ValueError(f"Unsupported SHM version: {version}")
        expected_data = total_size - HEADER_SIZE
        if data_size != expected_data:
            raise ValueError(f"data_size mismatch: header says {data_size}, expected {expected_data}")

    @staticmethod
    def initialize(buf: memoryview, total_size: int) -> None:
        """Write initial header: zero allocations."""
        data_size = total_size - HEADER_SIZE
        _HEADER_STRUCT.pack_into(buf, 0, _MAGIC, _VERSION, data_size, 0, 0)

    def _read_allocs(self) -> list[tuple[int, int]]:
        """Read the sorted allocation list from the header."""
        num = struct.unpack_from("<I", self._buf, 16)[0]
        allocs: list[tuple[int, int]] = []
        base = _HEADER_STRUCT.size
        for i in range(num):
            offset, length = _ALLOC_STRUCT.unpack_from(self._buf, base + i * _ALLOC_STRUCT.size)
            allocs.append((offset, length))
        return allocs

    def _write_allocs(self, allocs: list[tuple[int, int]]) -> None:
        """Write the sorted allocation list to the header."""
        struct.pack_into("<I", self._buf, 16, len(allocs))
        base = _HEADER_STRUCT.size
        for i, (offset, length) in enumerate(allocs):
            _ALLOC_STRUCT.pack_into(self._buf, base + i * _ALLOC_STRUCT.size, offset, length)

    def allocate(self, size: int) -> int | None:
        """Find first gap >= *size*.  Returns absolute offset or ``None``."""
        if size <= 0:
            raise ValueError("Allocation size must be positive")
        allocs = self._read_allocs()
        if len(allocs) >= MAX_ALLOCS:
            return None

        data_end = self._total_size

        # Check gap before first allocation
        prev_end = HEADER_SIZE
        for i, (off, length) in enumerate(allocs):
            gap = off - prev_end
            if gap >= size:
                allocs.insert(i, (prev_end, size))
                self._write_allocs(allocs)
                return prev_end
            prev_end = off + length

        # Check gap after last allocation
        gap = data_end - prev_end
        if gap >= size:
            allocs.append((prev_end, size))
            self._write_allocs(allocs)
            return prev_end

        return None

    def free(self, offset: int) -> None:
        """Remove allocation entry by *offset*.

        Raises:
            ValueError: If no allocation starts at *offset*.

        """
        allocs = self._read_allocs()
        for i, (off, _) in enumerate(allocs):
            if off == offset:
                allocs.pop(i)
                self._write_allocs(allocs)
                return
        raise ValueError(f"No allocation at offset {offset}")

    def reset(self) -> None:
        """Clear all allocations (``num_allocs = 0``)."""
        struct.pack_into("<I", self._buf, 16, 0)


# ---------------------------------------------------------------------------
# ShmSegment — wraps SharedMemory + allocator
# ---------------------------------------------------------------------------


class ShmSegment:
    """Shared memory segment for zero-copy Arrow IPC batch transfer.

    Lifecycle is independent of transports — created once, reused across
    calls, ``reset()`` between calls, caller controls cleanup.
    """

    __slots__ = ("_allocator", "_shm")

    def __init__(self, shm: SharedMemory, allocator: ShmAllocator) -> None:
        """Initialize with an existing SharedMemory and allocator."""
        self._shm = shm
        self._allocator = allocator

    @classmethod
    def create(cls, size: int) -> ShmSegment:
        """Create a new segment and initialize the allocator header.

        Args:
            size: Total segment size in bytes (must be > HEADER_SIZE).

        """
        if size <= HEADER_SIZE:
            raise ValueError(f"Segment size must be > {HEADER_SIZE}, got {size}")
        shm = SharedMemory(create=True, size=size)
        buf = shm.buf
        assert buf is not None  # always valid after create
        ShmAllocator.initialize(buf, size)
        allocator = ShmAllocator(buf, size)
        return cls(shm, allocator)

    @classmethod
    def attach(cls, name: str, size: int) -> ShmSegment:
        """Attach to an existing segment (validates header magic/version).

        Args:
            name: The shared memory segment name.
            size: Expected segment size.

        """
        shm = SharedMemory(name=name, create=False, size=size)
        buf = shm.buf
        assert buf is not None  # always valid after attach
        allocator = ShmAllocator(buf, size)
        return cls(shm, allocator)

    def allocate_and_write(
        self,
        batch: pa.RecordBatch,
    ) -> tuple[int, int] | None:
        """Serialize batch into the shared memory segment.

        For non-dictionary batches, uses ``_ShmSink`` + ``ipc.new_stream()``
        to write an IPC stream directly into SHM — Arrow's C++ writer
        writes through the sink with minimal Python call overhead.

        For dictionary-encoded batches, serializes to an Arrow buffer
        first (stripping schema + EOS), then copies into SHM.

        The counterpart is ``_deserialize_from_shm(buf, schema)``.

        Args:
            batch: The record batch to write.

        Returns:
            ``(offset, length)`` or ``None`` if it doesn't fit.

        """
        shm_buf = self._shm.buf
        assert shm_buf is not None  # segment still open

        if not _has_dictionary_columns(batch.schema):
            # Non-dict: write IPC stream directly into SHM via _ShmSink
            estimated = ipc.get_record_batch_size(batch) + _STREAM_OVERHEAD
            offset = self._allocator.allocate(estimated)
            if offset is None:
                return None
            sink = _ShmSink(shm_buf, offset)
            writer = ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            return offset, sink.bytes_written

        # Dict path: serialize to buffer, then copy
        serialized = _serialize_for_shm(batch)
        size = serialized.size
        offset = self._allocator.allocate(size)
        if offset is None:
            return None
        shm_buf[offset : offset + size] = memoryview(serialized).cast("B")
        return offset, size

    def read_buffer(self, offset: int, length: int) -> pa.Buffer:
        """Zero-copy: return ``pa.py_buffer()`` over shm region.

        Args:
            offset: Absolute offset in the segment.
            length: Number of bytes to read.

        Returns:
            An Arrow buffer backed by the shared memory region.

        """
        buf = self._shm.buf
        assert buf is not None  # segment still open
        return pa.py_buffer(buf[offset : offset + length])

    def free(self, offset: int) -> None:
        """Free a previously allocated region by offset.

        Args:
            offset: The offset returned by ``allocate_and_write``.

        """
        self._allocator.free(offset)

    def reset(self) -> None:
        """Clear all allocations.

        Call between RPC calls to reuse the segment.  Caller must ensure
        no Arrow batches still reference the shm data.
        """
        self._allocator.reset()

    def close(self) -> None:
        """Close the shared memory handle.

        Releases internal memoryview references first.  If external
        Arrow buffers still reference the shared memory (from
        ``read_buffer`` / ``pa.py_buffer``), the close may raise
        ``BufferError`` — callers should ensure all Arrow batches
        backed by this segment are deleted before calling close.
        """
        # Release allocator's memoryview reference first
        self._allocator._buf = memoryview(b"")
        self._shm.close()

    def unlink(self) -> None:
        """Unlink (destroy) the shared memory segment."""
        self._shm.unlink()

    @property
    def name(self) -> str:
        """The OS name of the shared memory segment."""
        return self._shm.name

    @property
    def size(self) -> int:
        """Total size of the shared memory segment in bytes."""
        return self._shm.size

    @property
    def buf(self) -> memoryview:
        """Raw memoryview of the shared memory segment."""
        buf = self._shm.buf
        assert buf is not None  # segment still open
        return buf


# ---------------------------------------------------------------------------
# Pointer batch helpers
# ---------------------------------------------------------------------------


def is_shm_pointer_batch(batch: pa.RecordBatch, custom_metadata: pa.KeyValueMetadata | None) -> bool:
    """Check whether a batch is a shared memory pointer.

    A pointer batch is a zero-row batch whose custom metadata contains
    ``SHM_OFFSET_KEY`` and does NOT contain ``LOG_LEVEL_KEY`` (which would
    make it a log batch).

    Args:
        batch: The record batch to check.
        custom_metadata: Custom metadata from the batch.

    Returns:
        ``True`` if the batch is a shared memory pointer.

    """
    if batch.num_rows != 0:
        return False
    if custom_metadata is None:
        return False
    if custom_metadata.get(SHM_OFFSET_KEY) is None:
        return False
    return custom_metadata.get(LOG_LEVEL_KEY) is None


def make_shm_pointer_batch(
    schema: pa.Schema,
    offset: int,
    length: int,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata]:
    """Create a zero-row pointer batch for a shared memory region.

    Args:
        schema: The schema the pointer batch should conform to.
        offset: Absolute offset in the shared memory segment.
        length: Number of bytes written.

    Returns:
        A ``(batch, custom_metadata)`` tuple.

    """
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=f.type) for f in schema],
        schema=schema,
    )
    custom_metadata = pa.KeyValueMetadata({SHM_OFFSET_KEY: str(offset).encode(), SHM_LENGTH_KEY: str(length).encode()})
    return batch, custom_metadata


def resolve_shm_batch(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    shm: ShmSegment | None,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None, Callable[[], None] | None]:
    """Resolve a shared memory pointer batch, or return it unchanged.

    Safe to call on any batch — non-pointer batches and ``shm=None``
    are returned as-is with ``release_fn=None``.

    Args:
        batch: A record batch (pointer or regular data).
        custom_metadata: Custom metadata from the batch.
        shm: The shared memory segment, or ``None``.

    Returns:
        ``(resolved_batch, resolved_metadata, release_fn)`` where
        *release_fn* is a callable that frees the shm region (or
        ``None`` if the batch did not come from shm).

    """
    if shm is None or not is_shm_pointer_batch(batch, custom_metadata):
        return batch, custom_metadata, None

    assert custom_metadata is not None  # guaranteed by is_shm_pointer_batch
    offset_bytes = custom_metadata.get(SHM_OFFSET_KEY)
    length_bytes = custom_metadata.get(SHM_LENGTH_KEY)
    assert offset_bytes is not None and length_bytes is not None  # guaranteed by is_shm_pointer_batch
    offset = int(offset_bytes)
    length = int(length_bytes)

    buf = shm.read_buffer(offset, length)
    resolved_batch = _deserialize_from_shm(buf, batch.schema)

    # Strip pointer keys, add provenance
    resolved_cm = strip_keys(custom_metadata, SHM_OFFSET_KEY, SHM_LENGTH_KEY)
    resolved_cm = merge_metadata(resolved_cm, {SHM_SOURCE_KEY: shm.name.encode()})

    def release_fn() -> None:
        shm.free(offset)

    return resolved_batch, resolved_cm, release_fn


def maybe_write_to_shm(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    shm: ShmSegment | None,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Try to write a batch to shared memory; fall back to original if it doesn't fit.

    Args:
        batch: The record batch to write.
        custom_metadata: Custom metadata for the batch.
        shm: The shared memory segment, or ``None``.

    Returns:
        ``(pointer_batch, pointer_metadata)`` if written to shm,
        otherwise ``(batch, custom_metadata)`` unchanged.

    """
    if shm is None or batch.num_rows == 0:
        return batch, custom_metadata

    result = shm.allocate_and_write(batch)
    if result is None:
        return batch, custom_metadata

    offset, actual_length = result
    pointer_batch, pointer_cm = make_shm_pointer_batch(batch.schema, offset, actual_length)
    # Merge any existing custom metadata (e.g. stream state) with pointer metadata
    final_cm = merge_metadata(custom_metadata, pointer_cm)
    return pointer_batch, final_cm
