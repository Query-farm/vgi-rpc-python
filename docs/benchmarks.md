---
hide:
  - navigation
---

# Benchmarks

Performance benchmarks measured with [pytest-benchmark](https://pytest-benchmark.readthedocs.io/) on a single machine (Apple Silicon). All times are **median** values across multiple iterations.

!!! note "Run benchmarks yourself"
    ```bash
    uv run pytest tests/test_benchmarks.py --benchmark-enable --benchmark-only \
        -o "addopts=" --timeout=300
    ```

## Serialization

Round-trip Arrow IPC serialization and deserialization for dataclasses and raw `RecordBatch` objects.

| Benchmark | Median | Description |
|-----------|--------|-------------|
| Serialize primitive dataclass | 39 us | 4 fields: `str`, `int`, `float`, `bool` |
| Deserialize primitive dataclass | 69 us | Same 4-field dataclass |
| Serialize complex dataclass | 101 us | `Enum`, `dict`, `frozenset`, `list` fields |
| Deserialize complex dataclass | 113 us | Same complex dataclass |
| Serialize 10K-row batch | 18 us | `int64` + `float64` + `utf8` columns |
| Deserialize 10K-row batch | 43 us | Same 10K-row batch |

Raw `RecordBatch` serialization is significantly faster than dataclass serialization because dataclasses require Python-level field packing/unpacking on top of the Arrow IPC encoding.

## End-to-End RPC Calls

Full round-trip latency for unary and streaming calls across all transports. Includes serialization, transport overhead, dispatch, and deserialization.

### Unary Methods

| Method | Pipe | Subprocess | Unix | Unix (threaded) | Shared Memory | Pool | HTTP |
|--------|------|------------|------|-----------------|---------------|------|------|
| `noop()` | 0.11 ms | 0.07 ms | 0.07 ms | 0.07 ms | 0.11 ms | 0.07 ms | 0.50 ms |
| `add(a, b)` | 0.17 ms | 0.09 ms | 0.10 ms | 0.10 ms | 0.44 ms | 0.09 ms | 0.57 ms |
| `greet(name)` | 0.15 ms | 0.08 ms | 0.10 ms | 0.09 ms | 0.40 ms | 0.08 ms | 0.52 ms |
| `roundtrip_types(...)` | 0.25 ms | 0.14 ms | 0.15 ms | 0.16 ms | 0.51 ms | 0.15 ms | 0.61 ms |

- **Subprocess**, **Unix**, and **pool** transports have the lowest latency (~0.07-0.16 ms)
- **Pipe** is slightly higher due to thread coordination overhead
- **Shared memory** carries more setup overhead for simple calls but shines with large batches
- **HTTP** adds ~0.5 ms baseline from the Falcon/httpx stack (in-process WSGI, no network)

### Streaming

| Method | Pipe | Subprocess | Unix | Unix (threaded) | Shared Memory | Pool | HTTP |
|--------|------|------------|------|-----------------|---------------|------|------|
| Producer (50 batches) | 3.5 ms | 2.3 ms | 3.7 ms | 4.1 ms | 7.3 ms | 2.3 ms | 9.2 ms |
| Exchange (20 rounds) | 3.3 ms | 1.3 ms | 1.7 ms | 2.0 ms | 6.9 ms | 1.4 ms | 24 ms |

- Producer streams generate 50 batches; exchange streams perform 20 bidirectional exchanges
- HTTP exchange is the slowest because each round-trip goes through the full WSGI stack with stream state token serialization
- Subprocess and pool transports benefit from OS-level pipe buffering for streaming workloads

## Schema Generation

| Benchmark | Median | Description |
|-----------|--------|-------------|
| Schema generation (uncached) | 51 us | Full `_generate_schema` from type hints |
| Schema generation (cached) | 0.3 us | Cached descriptor access |

Schema generation is a one-time cost per dataclass. After the first access, `ARROW_SCHEMA` is cached on the class via a descriptor — subsequent accesses are ~170x faster.

## Memory Bounds

| Benchmark | Median | Peak Memory | Description |
|-----------|--------|-------------|-------------|
| Serialize 100K-row batch | 0.33 ms | < 50 MB | `int64` + `float64` + `utf8` columns |
| Deserialize 100K-row batch | 0.40 ms | < 50 MB | Same 100K-row batch |

Memory usage stays well within bounds for large batches. The 50 MB assertion in the benchmark suite ensures regressions are caught automatically.

## Methodology

- **Timer**: `time.perf_counter` (wall clock)
- **Values reported**: Median (robust against outliers from GC pauses, OS scheduling)
- **Calibration**: pytest-benchmark auto-calibrates iteration count for statistical significance
- **Environment**: Apple Silicon (M-series), Python 3.13. Results vary by hardware; run locally for your own baseline
- **Test fixture**: RPC benchmarks use the `make_conn` fixture parametrized over pipe, subprocess, Unix, Unix threaded, shared memory, pool, and HTTP transports
