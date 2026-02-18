---
hide:
  - navigation
---

# Benchmarks

Performance benchmarks measured with [pytest-benchmark](https://pytest-benchmark.readthedocs.io/) on a single machine. All times are **median** values across multiple iterations.

!!! note "Run benchmarks yourself"
    ```bash
    uv run pytest tests/test_benchmarks.py --benchmark-enable --benchmark-only \
        -o "addopts=" --timeout=300
    ```

## Serialization

Round-trip Arrow IPC serialization and deserialization for dataclasses and raw `RecordBatch` objects.

| Benchmark | Median | Description |
|-----------|--------|-------------|
| Serialize primitive dataclass | 65 us | 4 fields: `str`, `int`, `float`, `bool` |
| Deserialize primitive dataclass | 194 us | Same 4-field dataclass |
| Serialize complex dataclass | 237 us | `Enum`, `dict`, `frozenset`, `list` fields |
| Deserialize complex dataclass | 206 us | Same complex dataclass |
| Serialize 10K-row batch | 33 us | `int64` + `float64` + `utf8` columns |
| Deserialize 10K-row batch | 60 us | Same 10K-row batch |

Raw `RecordBatch` serialization is significantly faster than dataclass serialization because dataclasses require Python-level field packing/unpacking on top of the Arrow IPC encoding.

## End-to-End RPC Calls

Full round-trip latency for unary and streaming calls across all four transports. Includes serialization, transport overhead, dispatch, and deserialization.

### Unary Methods

| Method | Pipe | Subprocess | Shared Memory | HTTP |
|--------|------|------------|---------------|------|
| `noop()` | 0.42 ms | 0.20 ms | 0.17 ms | 3.1 ms |
| `add(a, b)` | 0.25 ms | 0.34 ms | 0.60 ms | 3.0 ms |
| `greet(name)` | 0.29 ms | 0.20 ms | 1.0 ms | 3.2 ms |
| `roundtrip_types(...)` | 0.51 ms | 0.90 ms | 0.96 ms | 3.8 ms |

- **Pipe** and **subprocess** transports have the lowest latency (~0.2-0.9 ms)
- **Shared memory** is comparable for simple calls but carries more setup overhead
- **HTTP** adds ~3 ms baseline from the Falcon/httpx stack (in-process WSGI, no network)

### Streaming

| Method | Pipe | Subprocess | Shared Memory | HTTP |
|--------|------|------------|---------------|------|
| Producer (50 batches) | 15 ms | 5.7 ms | 42 ms | 40 ms |
| Exchange (20 rounds) | 6.8 ms | 4.1 ms | 15 ms | 131 ms |

- Producer streams generate 50 batches; exchange streams perform 20 bidirectional exchanges
- HTTP exchange is the slowest because each round-trip goes through the full WSGI stack with stream state token serialization
- Subprocess transport benefits from OS-level pipe buffering for streaming workloads

## Schema Generation

| Benchmark | Median | Description |
|-----------|--------|-------------|
| Schema generation (uncached) | 70 us | Full `_generate_schema` from type hints |
| Schema generation (cached) | 0.4 us | Cached descriptor access |

Schema generation is a one-time cost per dataclass. After the first access, `ARROW_SCHEMA` is cached on the class via a descriptor — subsequent accesses are ~175x faster.

## Memory Bounds

| Benchmark | Median | Peak Memory | Description |
|-----------|--------|-------------|-------------|
| Serialize 100K-row batch | 0.67 ms | < 50 MB | `int64` + `float64` + `utf8` columns |
| Deserialize 100K-row batch | 0.59 ms | < 50 MB | Same 100K-row batch |

Memory usage stays well within bounds for large batches. The 50 MB assertion in the benchmark suite ensures regressions are caught automatically.

## Methodology

- **Timer**: `time.perf_counter` (wall clock)
- **Values reported**: Median (robust against outliers from GC pauses, OS scheduling)
- **Calibration**: pytest-benchmark auto-calibrates iteration count for statistical significance
- **Environment**: Results vary by hardware; run locally for your own baseline
- **Test fixture**: RPC benchmarks use the `make_conn` fixture parametrized over pipe, subprocess, shared memory, and HTTP transports
