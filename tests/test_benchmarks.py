"""Performance benchmarks for vgi-rpc.

Run with:
    uv run pytest tests/test_benchmarks.py --benchmark-enable --benchmark-only -o "addopts=" --timeout=300
"""

from __future__ import annotations

import tracemalloc
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from pytest_benchmark.fixture import BenchmarkFixture

from vgi_rpc.rpc import AnnotatedBatch
from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    deserialize_record_batch,
    serialize_record_batch_bytes,
)

from .conftest import ConnFactory
from .test_rpc import Color

# ---------------------------------------------------------------------------
# Helper dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimpleBenchData(ArrowSerializableDataclass):
    """Dataclass with primitive fields for benchmarking."""

    name: str
    count: int
    score: float
    active: bool


@dataclass(frozen=True)
class ComplexBenchData(ArrowSerializableDataclass):
    """Dataclass with complex fields for benchmarking."""

    color: Color
    mapping: dict[str, int]
    tags: frozenset[int]
    items: list[float]


# ---------------------------------------------------------------------------
# Group 1: Serialization / Deserialization
# ---------------------------------------------------------------------------


class TestSerializationBenchmarks:
    """Benchmarks for Arrow IPC serialization and deserialization."""

    def test_serialize_primitive_dataclass(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark serializing a dataclass with primitive fields."""
        obj = SimpleBenchData(name="benchmark", count=42, score=3.14, active=True)
        benchmark(obj.serialize_to_bytes)

    def test_deserialize_primitive_dataclass(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark deserializing a dataclass with primitive fields."""
        obj = SimpleBenchData(name="benchmark", count=42, score=3.14, active=True)
        data = obj.serialize_to_bytes()
        benchmark(SimpleBenchData.deserialize_from_bytes, data)

    def test_serialize_complex_dataclass(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark serializing a dataclass with enum/dict/frozenset fields."""
        obj = ComplexBenchData(
            color=Color.RED,
            mapping={"a": 1, "b": 2, "c": 3},
            tags=frozenset({10, 20, 30}),
            items=[1.0, 2.0, 3.0, 4.0, 5.0],
        )
        benchmark(obj.serialize_to_bytes)

    def test_deserialize_complex_dataclass(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark deserializing a dataclass with enum/dict/frozenset fields."""
        obj = ComplexBenchData(
            color=Color.RED,
            mapping={"a": 1, "b": 2, "c": 3},
            tags=frozenset({10, 20, 30}),
            items=[1.0, 2.0, 3.0, 4.0, 5.0],
        )
        data = obj.serialize_to_bytes()
        benchmark(ComplexBenchData.deserialize_from_bytes, data)

    def test_serialize_large_batch(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark serializing a 10K-row RecordBatch."""
        n = 10_000
        batch = pa.RecordBatch.from_pydict(
            {
                "id": list(range(n)),
                "value": [float(i) * 1.1 for i in range(n)],
                "label": [f"item_{i}" for i in range(n)],
            }
        )
        result: bytes = benchmark(serialize_record_batch_bytes, batch)
        benchmark.extra_info["batch_rows"] = n
        benchmark.extra_info["bytes"] = len(result)

    def test_deserialize_large_batch(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark deserializing a 10K-row RecordBatch."""
        n = 10_000
        batch = pa.RecordBatch.from_pydict(
            {
                "id": list(range(n)),
                "value": [float(i) * 1.1 for i in range(n)],
                "label": [f"item_{i}" for i in range(n)],
            }
        )
        data = serialize_record_batch_bytes(batch)
        benchmark.extra_info["batch_rows"] = n
        benchmark.extra_info["bytes"] = len(data)
        benchmark(deserialize_record_batch, data)


# ---------------------------------------------------------------------------
# Group 2: End-to-End RPC Calls
# ---------------------------------------------------------------------------


class TestRpcBenchmarks:
    """Benchmarks for end-to-end RPC calls across all transports."""

    def test_unary_noop(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark minimum framework overhead — noop call."""
        with make_conn() as proxy:
            benchmark(proxy.noop)

    def test_unary_add(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark primitive params + return — add(a, b)."""
        with make_conn() as proxy:
            benchmark(proxy.add, a=1.0, b=2.0)

    def test_unary_greet(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark string params — greet(name)."""
        with make_conn() as proxy:
            benchmark(proxy.greet, name="benchmark")

    def test_unary_roundtrip_types(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark complex types — roundtrip_types(color, mapping, tags)."""
        with make_conn() as proxy:
            benchmark(proxy.roundtrip_types, color=Color.GREEN, mapping={"x": 1}, tags=frozenset({7}))

    def test_stream_producer(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark producer stream throughput — generate(count=50)."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                return list(proxy.generate(count=50))

        benchmark(run)

    def test_stream_exchange(self, benchmark: BenchmarkFixture, make_conn: ConnFactory) -> None:
        """Benchmark exchange throughput — 20 exchanges via transform."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                session = proxy.transform(factor=2.0)
                results = []
                for i in range(20):
                    batch = AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"value": [float(i)]}))
                    results.append(session.exchange(batch))
                session.close()
                return results

        benchmark(run)


# ---------------------------------------------------------------------------
# Group 3: Schema Generation
# ---------------------------------------------------------------------------

_CACHE_ATTR = "_cached_ARROW_SCHEMA"


def _clear_schema_cache(cls: type[ArrowSerializableDataclass]) -> None:
    """Clear the cached ARROW_SCHEMA so _generate_schema runs fresh."""
    if _CACHE_ATTR in cls.__dict__:
        delattr(cls, _CACHE_ATTR)


class TestSchemaGenerationBenchmarks:
    """Benchmarks for Arrow schema generation."""

    def test_schema_generation_uncached(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark schema generation without caching."""

        def generate_uncached() -> pa.Schema:
            _clear_schema_cache(SimpleBenchData)
            return SimpleBenchData.ARROW_SCHEMA

        benchmark(generate_uncached)

    def test_schema_generation_cached(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark cached schema access (baseline)."""
        # Ensure schema is cached
        _ = SimpleBenchData.ARROW_SCHEMA
        benchmark(lambda: SimpleBenchData.ARROW_SCHEMA)


# ---------------------------------------------------------------------------
# Group 4: Memory Bounds
# ---------------------------------------------------------------------------


class TestMemoryBenchmarks:
    """Benchmarks for memory usage during serialization."""

    def test_large_batch_serialize_memory(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark peak memory when serializing a 100K-row batch."""
        n = 100_000
        batch = pa.RecordBatch.from_pydict(
            {
                "id": list(range(n)),
                "value": [float(i) * 1.1 for i in range(n)],
                "label": [f"item_{i}" for i in range(n)],
            }
        )

        def serialize_and_measure() -> bytes:
            tracemalloc.start()
            try:
                result = serialize_record_batch_bytes(batch)
                _, peak = tracemalloc.get_traced_memory()
                benchmark.extra_info["peak_memory_mb"] = round(peak / (1024 * 1024), 2)
                return result
            finally:
                tracemalloc.stop()

        result = benchmark(serialize_and_measure)
        peak_mb = benchmark.extra_info.get("peak_memory_mb", 0)
        assert isinstance(peak_mb, (int, float))
        assert peak_mb < 50, f"Peak memory {peak_mb} MB exceeds 50 MB limit"
        assert len(result) > 0

    def test_large_batch_deserialize_memory(self, benchmark: BenchmarkFixture) -> None:
        """Benchmark peak memory when deserializing a 100K-row batch."""
        n = 100_000
        batch = pa.RecordBatch.from_pydict(
            {
                "id": list(range(n)),
                "value": [float(i) * 1.1 for i in range(n)],
                "label": [f"item_{i}" for i in range(n)],
            }
        )
        data = serialize_record_batch_bytes(batch)

        def deserialize_and_measure() -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
            tracemalloc.start()
            try:
                result = deserialize_record_batch(data)
                _, peak = tracemalloc.get_traced_memory()
                benchmark.extra_info["peak_memory_mb"] = round(peak / (1024 * 1024), 2)
                return result
            finally:
                tracemalloc.stop()

        result = benchmark(deserialize_and_measure)
        peak_mb = benchmark.extra_info.get("peak_memory_mb", 0)
        assert isinstance(peak_mb, (int, float))
        assert peak_mb < 50, f"Peak memory {peak_mb} MB exceeds 50 MB limit"
        assert result[0].num_rows == n
