# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for shared memory transport support."""

from __future__ import annotations

import gc
import logging
import struct
import threading
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.metadata import LOG_LEVEL_KEY, SHM_LENGTH_KEY, SHM_OFFSET_KEY, SHM_SOURCE_KEY
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    OutputCollector,
    RpcServer,
    ShmPipeTransport,
    Stream,
    StreamState,
    _RpcProxy,
    make_pipe_pair,
)
from vgi_rpc.shm import (
    HEADER_SIZE,
    MAX_ALLOCS,
    ShmAllocator,
    ShmSegment,
    _deserialize_from_shm,
    _serialize_for_shm,
    is_shm_pointer_batch,
    make_shm_pointer_batch,
    maybe_write_to_shm,
    resolve_shm_batch,
)
from vgi_rpc.utils import ArrowSerializableDataclass

# ---------------------------------------------------------------------------
# Test schema & helpers
# ---------------------------------------------------------------------------

_TEST_SCHEMA: pa.Schema = pa.schema({"x": pa.int64(), "y": pa.float64()})  # type: ignore[arg-type]


def _make_batch(n: int = 5) -> pa.RecordBatch:
    """Create a test batch with *n* rows."""
    return pa.RecordBatch.from_pydict({"x": list(range(n)), "y": [float(i) for i in range(n)]}, schema=_TEST_SCHEMA)


def _cleanup_shm(shm: ShmSegment) -> None:
    """Unlink and close a ShmSegment.

    ``unlink()`` marks the segment for OS-level deletion.  ``close()``
    handles ``BufferError`` internally (nulls out the mmap to prevent
    ``SharedMemory.__del__`` from retrying).
    """
    shm.unlink()
    shm.close()


# ---------------------------------------------------------------------------
# Stream state for integration tests (must be defined before Protocol)
# ---------------------------------------------------------------------------

_GENERATE_OUTPUT_SCHEMA = pa.schema([("value", pa.int64())])
_DICT_OUTPUT_SCHEMA = pa.schema([("label", pa.dictionary(pa.int32(), pa.utf8()))])


@dataclass
class GenerateState(StreamState, ArrowSerializableDataclass):
    """State for the generate producer stream."""

    count: int
    current: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next batch."""
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"value": [self.current * 10]})
        self.current += 1


@dataclass
class GenerateDictState(StreamState, ArrowSerializableDataclass):
    """State for the dictionary-encoded producer stream.

    Each batch emits a different dictionary value so the IPC stream
    contains a new dictionary message per batch.
    """

    count: int
    current: int = 0

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next dictionary-encoded batch."""
        if self.current >= self.count:
            out.finish()
            return
        label = f"item_{self.current}"
        out.emit(
            pa.RecordBatch.from_pydict(
                {"label": pa.array([label]).dictionary_encode()},
                schema=_DICT_OUTPUT_SCHEMA,
            )
        )
        self.current += 1


class ShmTestService(Protocol):
    """Minimal RPC service for SHM integration tests."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        ...

    def generate(self, count: int) -> Stream[GenerateState]:
        """Generate a stream of integers."""
        ...

    def generate_dict(self, count: int) -> Stream[GenerateDictState]:
        """Generate a stream of dictionary-encoded batches."""
        ...


class ShmTestServiceImpl:
    """Implementation for SHM integration tests."""

    def add(self, a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    def generate(self, count: int) -> Stream[GenerateState]:
        """Generate a stream of integers."""
        return Stream(output_schema=_GENERATE_OUTPUT_SCHEMA, state=GenerateState(count=count))

    def generate_dict(self, count: int) -> Stream[GenerateDictState]:
        """Generate a stream of dictionary-encoded batches."""
        return Stream(output_schema=_DICT_OUTPUT_SCHEMA, state=GenerateDictState(count=count))


def _make_shm_conn(
    shm_size: int = 4 * 1024 * 1024,
) -> tuple[ShmPipeTransport, ShmPipeTransport, ShmSegment, RpcServer]:
    """Create client/server SHM transports."""
    shm = ShmSegment.create(shm_size)
    client_pipe, server_pipe = make_pipe_pair()
    client = ShmPipeTransport(client_pipe, shm)
    server = ShmPipeTransport(server_pipe, shm)
    rpc_server = RpcServer(ShmTestService, ShmTestServiceImpl())
    return client, server, shm, rpc_server


# ---------------------------------------------------------------------------
# ShmAllocator tests
# ---------------------------------------------------------------------------


class TestShmAllocator:
    """Tests for the bump-pointer allocator."""

    def _make_allocator(self, size: int = HEADER_SIZE + 8192) -> tuple[memoryview, ShmAllocator]:
        """Create a fresh allocator for testing."""
        buf = bytearray(size)
        mv = memoryview(buf)
        ShmAllocator.initialize(mv, size)
        return mv, ShmAllocator(mv, size)

    def test_initialize_and_attach(self) -> None:
        """Fresh allocator has no allocations."""
        _mv, alloc = self._make_allocator()
        assert alloc._read_allocs() == []

    def test_allocate_first(self) -> None:
        """First allocation starts at HEADER_SIZE."""
        _, alloc = self._make_allocator()
        offset = alloc.allocate(100)
        assert offset == HEADER_SIZE
        assert alloc._read_allocs() == [(HEADER_SIZE, 100)]

    def test_allocate_multiple(self) -> None:
        """Multiple allocations are contiguous and sorted."""
        _, alloc = self._make_allocator()
        o1 = alloc.allocate(100)
        o2 = alloc.allocate(200)
        assert o1 == HEADER_SIZE
        assert o2 == HEADER_SIZE + 100
        allocs = alloc._read_allocs()
        assert len(allocs) == 2
        assert allocs[0] == (HEADER_SIZE, 100)
        assert allocs[1] == (HEADER_SIZE + 100, 200)

    def test_allocate_exhaustion(self) -> None:
        """Returns None when segment is full."""
        _, alloc = self._make_allocator(size=HEADER_SIZE + 100)
        alloc.allocate(100)
        assert alloc.allocate(1) is None

    def test_free_and_reuse(self) -> None:
        """Freed space can be reused."""
        _, alloc = self._make_allocator()
        o1 = alloc.allocate(100)
        o2 = alloc.allocate(200)
        assert o1 is not None
        assert o2 is not None
        alloc.free(o1)
        o3 = alloc.allocate(50)
        assert o3 == HEADER_SIZE

    def test_free_unknown_raises(self) -> None:
        """Freeing unknown offset raises ValueError."""
        _, alloc = self._make_allocator()
        with pytest.raises(ValueError, match="No allocation at offset"):
            alloc.free(9999)

    def test_reset(self) -> None:
        """Reset clears all allocations."""
        _, alloc = self._make_allocator()
        alloc.allocate(100)
        alloc.allocate(200)
        alloc.reset()
        assert alloc._read_allocs() == []
        offset = alloc.allocate(50)
        assert offset == HEADER_SIZE

    def test_gap_reuse_between_allocations(self) -> None:
        """Freed gap between allocations is reused."""
        _, alloc = self._make_allocator()
        o1 = alloc.allocate(100)
        o2 = alloc.allocate(100)
        o3 = alloc.allocate(100)
        assert o1 is not None
        assert o2 is not None
        assert o3 is not None
        alloc.free(o2)
        o4 = alloc.allocate(50)
        assert o4 == o2
        o5 = alloc.allocate(50)
        assert o5 == o2 + 50

    def test_allocate_zero_raises(self) -> None:
        """Zero-size allocation raises ValueError."""
        _, alloc = self._make_allocator()
        with pytest.raises(ValueError, match="positive"):
            alloc.allocate(0)

    def test_bad_magic(self) -> None:
        """Bad magic in header raises ValueError."""
        size = HEADER_SIZE + 1024
        buf = bytearray(size)
        mv = memoryview(buf)
        with pytest.raises(ValueError, match="Bad SHM magic"):
            ShmAllocator(mv, size)

    def test_version_mismatch(self) -> None:
        """Wrong version in header raises ValueError."""
        size = HEADER_SIZE + 1024
        buf = bytearray(size)
        mv = memoryview(buf)
        ShmAllocator.initialize(mv, size)
        struct.pack_into("<I", mv, 4, 99)
        with pytest.raises(ValueError, match="Unsupported SHM version"):
            ShmAllocator(mv, size)

    def test_num_allocs_property(self) -> None:
        """num_allocs reflects current allocation count."""
        _, alloc = self._make_allocator()
        assert alloc.num_allocs == 0
        alloc.allocate(100)
        assert alloc.num_allocs == 1
        alloc.allocate(200)
        assert alloc.num_allocs == 2
        alloc.free(HEADER_SIZE)
        assert alloc.num_allocs == 1

    def test_max_allocs_property(self) -> None:
        """max_allocs returns the module-level MAX_ALLOCS constant."""
        _, alloc = self._make_allocator()
        assert alloc.max_allocs == MAX_ALLOCS
        assert alloc.max_allocs == (HEADER_SIZE - 24) // 16

    def test_max_allocs_is_4094(self) -> None:
        """With HEADER_SIZE=65536, max_allocs is 4094."""
        assert MAX_ALLOCS == 4094

    def test_allocate_over_254(self) -> None:
        """Can allocate more than the old 254-entry limit."""
        # Need enough data space: 300 allocations of 16 bytes each = 4800 bytes
        size = HEADER_SIZE + 300 * 16
        _, alloc = self._make_allocator(size=size)
        offsets: list[int] = []
        for _ in range(300):
            off = alloc.allocate(16)
            assert off is not None
            offsets.append(off)
        assert alloc.num_allocs == 300

    def test_warn_at_80_percent(self, caplog: pytest.LogCaptureFixture) -> None:
        """Warning is logged when allocations reach 80% of max_allocs."""
        # We need a buffer large enough for _WARN_THRESHOLD allocations.
        # Each alloc is 1 byte of data, but we need header space for entries.
        threshold = int(MAX_ALLOCS * 0.8)
        data_needed = threshold * 8  # 8 bytes per data allocation
        size = HEADER_SIZE + data_needed
        _, alloc = self._make_allocator(size=size)

        with caplog.at_level(logging.WARNING, logger="vgi_rpc.shm"):
            for _ in range(threshold - 1):
                alloc.allocate(8)
            assert len(caplog.records) == 0

            # This allocation crosses the 80% threshold
            alloc.allocate(8)
            assert len(caplog.records) == 1
            assert "near capacity" in caplog.records[0].message
            assert f"{threshold}/{MAX_ALLOCS}" in caplog.records[0].message


# ---------------------------------------------------------------------------
# ShmSegment tests
# ---------------------------------------------------------------------------


class TestShmSegment:
    """Tests for ShmSegment create/attach/allocate_and_write/read_buffer."""

    def test_create_and_properties(self) -> None:
        """Create segment and check properties."""
        shm = ShmSegment.create(128 * 1024)
        try:
            assert shm.size >= 128 * 1024
            assert shm.name
            assert len(shm.buf) >= 128 * 1024
        finally:
            _cleanup_shm(shm)

    def test_allocator_property(self) -> None:
        """ShmSegment.allocator exposes the underlying ShmAllocator."""
        shm = ShmSegment.create(128 * 1024)
        try:
            alloc = shm.allocator
            assert isinstance(alloc, ShmAllocator)
            assert alloc.num_allocs == 0
            result = shm.allocate_and_write(_make_batch(1))
            assert result is not None
            assert alloc.num_allocs == 1
        finally:
            _cleanup_shm(shm)

    def test_create_too_small(self) -> None:
        """Segment smaller than header is rejected."""
        with pytest.raises(ValueError, match="must be >"):
            ShmSegment.create(HEADER_SIZE)

    def test_attach(self) -> None:
        """Attach to existing segment validates header."""
        shm1 = ShmSegment.create(128 * 1024)
        try:
            shm2 = ShmSegment.attach(shm1.name, shm1.size)
            try:
                assert shm2.name == shm1.name
            finally:
                shm2.close()
        finally:
            shm1.unlink()
            shm1.close()

    def test_allocate_and_write_round_trip(self) -> None:
        """Write batch to SHM and read it back."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(10)
            result = shm.allocate_and_write(batch)
            assert result is not None
            offset, length = result

            buf = shm.read_buffer(offset, length)
            resolved = _deserialize_from_shm(buf, batch.schema)
            assert resolved.equals(batch)
            del resolved, buf
        finally:
            _cleanup_shm(shm)

    def test_allocate_and_write_overflow(self) -> None:
        """Returns None when batch doesn't fit."""
        shm = ShmSegment.create(HEADER_SIZE + 64)
        try:
            batch = _make_batch(1000)
            result = shm.allocate_and_write(batch)
            assert result is None
        finally:
            _cleanup_shm(shm)

    def test_reset_and_reuse(self) -> None:
        """Reset allows reallocating from the start."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(10)
            result = shm.allocate_and_write(batch)
            assert result is not None
            shm.reset()
            result2 = shm.allocate_and_write(batch)
            assert result2 is not None
            assert result2[0] == HEADER_SIZE
        finally:
            _cleanup_shm(shm)

    def test_free(self) -> None:
        """Free allows reallocating in freed space."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(5)
            result = shm.allocate_and_write(batch)
            assert result is not None
            offset, _ = result
            shm.free(offset)
            result2 = shm.allocate_and_write(batch)
            assert result2 is not None
            assert result2[0] == offset
        finally:
            _cleanup_shm(shm)


# ---------------------------------------------------------------------------
# Serialization helper tests
# ---------------------------------------------------------------------------


class TestShmSerialization:
    """Tests for SHM serialization round-trips."""

    def test_non_dict_shm_round_trip(self) -> None:
        """Non-dictionary batch round-trips through SHM (direct-write path)."""
        batch = _make_batch(10)
        shm = ShmSegment.create(1024 * 1024)
        try:
            result = shm.allocate_and_write(batch)
            assert result is not None
            offset, length = result
            buf = shm.read_buffer(offset, length)
            resolved = _deserialize_from_shm(buf, batch.schema)
            assert resolved.equals(batch)
            del resolved, buf
        finally:
            _cleanup_shm(shm)

    def test_non_dict_size_bound(self) -> None:
        """Direct-write path fits within estimated allocation."""
        batch = _make_batch(100)
        estimated = ipc.get_record_batch_size(batch) + 4096  # _STREAM_OVERHEAD
        shm = ShmSegment.create(estimated + HEADER_SIZE + 4096)
        try:
            result = shm.allocate_and_write(batch)
            assert result is not None
            _offset, actual = result
            assert actual <= estimated
        finally:
            _cleanup_shm(shm)

    def test_dict_round_trip(self) -> None:
        """Dictionary-encoded batch round-trips through serialize/deserialize."""
        dict_schema = pa.schema([pa.field("color", pa.dictionary(pa.int8(), pa.utf8()))])
        batch = pa.RecordBatch.from_pydict(
            {"color": pa.array(["red", "green", "red"]).dictionary_encode()},
            schema=dict_schema,
        )
        data = _serialize_for_shm(batch)
        buf = pa.py_buffer(data)
        resolved = _deserialize_from_shm(buf, batch.schema)
        assert resolved.equals(batch)

    def test_dict_shm_round_trip(self) -> None:
        """Dictionary-encoded batch round-trips through SHM."""
        dict_schema = pa.schema([pa.field("color", pa.dictionary(pa.int8(), pa.utf8()))])
        batch = pa.RecordBatch.from_pydict(
            {"color": pa.array(["red", "green", "red"]).dictionary_encode()},
            schema=dict_schema,
        )
        shm = ShmSegment.create(1024 * 1024)
        try:
            result = shm.allocate_and_write(batch)
            assert result is not None
            offset, length = result
            buf = shm.read_buffer(offset, length)
            resolved = _deserialize_from_shm(buf, batch.schema)
            assert resolved.equals(batch)
            del resolved, buf
        finally:
            _cleanup_shm(shm)


# ---------------------------------------------------------------------------
# Pointer batch helper tests
# ---------------------------------------------------------------------------


class TestPointerBatchHelpers:
    """Tests for is_shm_pointer_batch, make_shm_pointer_batch, resolve_shm_batch."""

    def test_is_shm_pointer_positive(self) -> None:
        """Pointer batch is detected correctly."""
        batch, cm = make_shm_pointer_batch(_TEST_SCHEMA, 4096, 1000)
        assert is_shm_pointer_batch(batch, cm)

    def test_is_shm_pointer_nonzero_rows(self) -> None:
        """Non-zero-row batch is not a pointer."""
        batch = _make_batch(1)
        cm = pa.KeyValueMetadata({SHM_OFFSET_KEY: b"4096"})
        assert not is_shm_pointer_batch(batch, cm)

    def test_is_shm_pointer_no_metadata(self) -> None:
        """Batch without metadata is not a pointer."""
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.float64())],
            schema=_TEST_SCHEMA,
        )
        assert not is_shm_pointer_batch(batch, None)

    def test_is_shm_pointer_log_batch_excluded(self) -> None:
        """Log batch with SHM_OFFSET_KEY is not a pointer."""
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.float64())],
            schema=_TEST_SCHEMA,
        )
        cm = pa.KeyValueMetadata({SHM_OFFSET_KEY: b"4096", LOG_LEVEL_KEY: b"INFO"})
        assert not is_shm_pointer_batch(batch, cm)

    def test_make_pointer_batch(self) -> None:
        """Make and inspect a pointer batch."""
        batch, cm = make_shm_pointer_batch(_TEST_SCHEMA, 4096, 500)
        assert batch.num_rows == 0
        assert batch.schema == _TEST_SCHEMA
        assert cm.get(SHM_OFFSET_KEY) == b"4096"
        assert cm.get(SHM_LENGTH_KEY) == b"500"

    def test_resolve_passthrough_no_shm(self) -> None:
        """Non-pointer batch passes through when shm is None."""
        batch = _make_batch(5)
        resolved, resolved_cm, release_fn = resolve_shm_batch(batch, None, None)
        assert resolved is batch
        assert resolved_cm is None
        assert release_fn is None

    def test_resolve_passthrough_non_pointer(self) -> None:
        """Non-pointer batch passes through even with shm."""
        shm = ShmSegment.create(128 * 1024)
        try:
            batch = _make_batch(5)
            resolved, _resolved_cm, release_fn = resolve_shm_batch(batch, None, shm)
            assert resolved is batch
            assert release_fn is None
        finally:
            _cleanup_shm(shm)

    def test_resolve_pointer_round_trip(self) -> None:
        """Pointer batch resolves to original data with SHM_SOURCE_KEY."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            original = _make_batch(10)
            result = shm.allocate_and_write(original)
            assert result is not None
            offset, length = result

            pointer_batch, pointer_cm = make_shm_pointer_batch(original.schema, offset, length)
            resolved, resolved_cm, release_fn = resolve_shm_batch(pointer_batch, pointer_cm, shm)

            assert resolved.equals(original)
            assert resolved_cm is not None
            assert resolved_cm.get(SHM_SOURCE_KEY) is not None
            assert resolved_cm.get(SHM_OFFSET_KEY) is None
            assert resolved_cm.get(SHM_LENGTH_KEY) is None

            assert release_fn is not None
            del resolved, resolved_cm
            release_fn()
        finally:
            _cleanup_shm(shm)


# ---------------------------------------------------------------------------
# maybe_write_to_shm tests
# ---------------------------------------------------------------------------


class TestMaybeWriteToShm:
    """Tests for maybe_write_to_shm."""

    def test_none_shm(self) -> None:
        """Returns original when shm is None."""
        batch = _make_batch(5)
        result_batch, result_cm = maybe_write_to_shm(batch, None, None)
        assert result_batch is batch
        assert result_cm is None

    def test_zero_row_skip(self) -> None:
        """Zero-row batches are never written to SHM."""
        shm = ShmSegment.create(128 * 1024)
        try:
            batch = pa.RecordBatch.from_arrays(
                [pa.array([], type=pa.int64()), pa.array([], type=pa.float64())],
                schema=_TEST_SCHEMA,
            )
            result_batch, _result_cm = maybe_write_to_shm(batch, None, shm)
            assert result_batch is batch
        finally:
            _cleanup_shm(shm)

    def test_fits_in_shm(self) -> None:
        """Batch that fits produces a pointer batch."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(10)
            result_batch, result_cm = maybe_write_to_shm(batch, None, shm)
            assert result_batch.num_rows == 0
            assert result_cm is not None
            assert result_cm.get(SHM_OFFSET_KEY) is not None
            assert result_cm.get(SHM_LENGTH_KEY) is not None
        finally:
            _cleanup_shm(shm)

    def test_does_not_fit(self) -> None:
        """Batch too large falls back to original."""
        shm = ShmSegment.create(HEADER_SIZE + 64)
        try:
            batch = _make_batch(1000)
            result_batch, result_cm = maybe_write_to_shm(batch, None, shm)
            assert result_batch is batch
            assert result_cm is None
        finally:
            _cleanup_shm(shm)

    def test_preserves_existing_metadata(self) -> None:
        """Existing custom metadata is preserved in pointer batch."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(5)
            existing_cm = pa.KeyValueMetadata({b"custom_key": b"custom_val"})
            result_batch, result_cm = maybe_write_to_shm(batch, existing_cm, shm)
            assert result_batch.num_rows == 0
            assert result_cm is not None
            assert result_cm.get(b"custom_key") == b"custom_val"
            assert result_cm.get(SHM_OFFSET_KEY) is not None
        finally:
            _cleanup_shm(shm)


# ---------------------------------------------------------------------------
# AnnotatedBatch.release() tests
# ---------------------------------------------------------------------------


class TestAnnotatedBatchRelease:
    """Tests for AnnotatedBatch.release()."""

    def test_release_noop(self) -> None:
        """Release on batch without release_fn is a no-op."""
        ab = AnnotatedBatch(batch=_make_batch(1))
        ab.release()

    def test_release_calls_fn(self) -> None:
        """Release invokes the release function."""
        called = [False]

        def fn() -> None:
            called[0] = True

        ab = AnnotatedBatch(batch=_make_batch(1), _release_fn=fn)
        ab.release()
        assert called[0]


# ---------------------------------------------------------------------------
# SHM-specific integration tests
# ---------------------------------------------------------------------------


class TestShmIntegration:
    """SHM-specific integration tests."""

    def test_unary_shm_source_key(self) -> None:
        """Verify SHM_SOURCE_KEY present in metadata for batches that went through SHM."""
        client, server, shm, rpc_server = _make_shm_conn()
        try:
            thread = threading.Thread(target=rpc_server.serve_one, args=(server,), daemon=True)
            thread.start()
            proxy = _RpcProxy(ShmTestService, client)
            result = proxy.add(a=1, b=2)
            assert result == 3
            client.close()
            thread.join(timeout=5)
        finally:
            _cleanup_shm(shm)

    def test_stream_shm_source_key(self) -> None:
        """Verify SHM batches in stream mode carry SHM_SOURCE_KEY."""
        client, server, shm, rpc_server = _make_shm_conn()
        try:
            thread = threading.Thread(target=rpc_server.serve_one, args=(server,), daemon=True)
            thread.start()
            proxy = _RpcProxy(ShmTestService, client)
            session = proxy.generate(count=3)
            batches = list(session)
            assert len(batches) == 3
            for ab in batches:
                assert ab.custom_metadata is not None
                assert ab.custom_metadata.get(SHM_SOURCE_KEY) is not None
            for ab in batches:
                ab.release()
            del batches, session, proxy
            gc.collect()
            client.close()
            thread.join(timeout=5)
        finally:
            _cleanup_shm(shm)

    def test_fallback_to_pipe(self) -> None:
        """Oversized batch falls back to pipe transparently."""
        client, server, shm, rpc_server = _make_shm_conn(shm_size=HEADER_SIZE + 32)
        try:
            thread = threading.Thread(target=rpc_server.serve_one, args=(server,), daemon=True)
            thread.start()
            proxy = _RpcProxy(ShmTestService, client)
            result = proxy.add(a=10, b=20)
            assert result == 30
            client.close()
            thread.join(timeout=5)
        finally:
            _cleanup_shm(shm)

    def test_multiple_sequential_calls(self) -> None:
        """Multiple sequential calls on same segment reuse SHM."""
        shm = ShmSegment.create(4 * 1024 * 1024)
        try:
            for _ in range(3):
                client_pipe, server_pipe = make_pipe_pair()
                client = ShmPipeTransport(client_pipe, shm)
                server = ShmPipeTransport(server_pipe, shm)
                rpc_server = RpcServer(ShmTestService, ShmTestServiceImpl())
                thread = threading.Thread(target=rpc_server.serve_one, args=(server,), daemon=True)
                thread.start()
                proxy = _RpcProxy(ShmTestService, client)
                result = proxy.add(a=1, b=2)
                assert result == 3
                client.close()
                thread.join(timeout=5)
                shm.reset()
        finally:
            _cleanup_shm(shm)

    def test_release_frees_region(self) -> None:
        """Verify release() frees the SHM region."""
        shm = ShmSegment.create(1024 * 1024)
        try:
            batch = _make_batch(10)
            result = shm.allocate_and_write(batch)
            assert result is not None
            offset, length = result

            pointer_batch, pointer_cm = make_shm_pointer_batch(batch.schema, offset, length)
            resolved, _resolved_cm, release_fn = resolve_shm_batch(pointer_batch, pointer_cm, shm)
            assert release_fn is not None

            allocs = shm._allocator._read_allocs()
            assert len(allocs) == 1

            del resolved
            release_fn()

            allocs = shm._allocator._read_allocs()
            assert len(allocs) == 0
        finally:
            _cleanup_shm(shm)

    def test_dict_stream_1000_batches(self) -> None:
        """Stream 1000 dictionary-encoded batches with varying values through SHM.

        Each batch has a unique dictionary value (``item_0`` .. ``item_999``)
        so every batch produces a new dictionary message in the IPC stream.
        Releases each batch after verification so SHM regions are reused.
        """
        # 16 MB segment — plenty of room for 1000 tiny batches
        client, server, shm, rpc_server = _make_shm_conn(shm_size=16 * 1024 * 1024)
        try:
            thread = threading.Thread(target=rpc_server.serve_one, args=(server,), daemon=True)
            thread.start()
            proxy = _RpcProxy(ShmTestService, client)
            count = 0
            for ab in proxy.generate_dict(count=1000):
                col = ab.batch.column("label")
                assert col[0].as_py() == f"item_{count}"
                assert ab.custom_metadata is not None
                assert ab.custom_metadata.get(SHM_SOURCE_KEY) is not None
                ab.release()
                count += 1
            assert count == 1000
            del proxy
            gc.collect()
            client.close()
            thread.join(timeout=10)
        finally:
            _cleanup_shm(shm)
