"""Benchmark: find the crossover point for parallel vs single GET.

Uses Range requests to fetch the first N bytes of the test file,
comparing a single GET against parallel chunked fetch at each size.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp

from vgi_rpc.external_fetch import FetchConfig, _ensure_pool, _fetch_chunks_with_hedging

URL = "https://electrified.cloud/random.data"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB — optimal from round 2
MAX_PARALLEL = 8
RUNS = 3  # best-of-N


@dataclass
class SizeResult:
    """Comparison result for one file size."""

    size_mb: float
    single_best_s: float
    parallel_best_s: float
    single_mbps: float
    parallel_mbps: float
    speedup: float  # parallel / single (>1 means parallel is faster)


async def _single_get(client: aiohttp.ClientSession, size: int) -> bytes:
    """Fetch first *size* bytes with a single Range GET."""
    headers = {"Range": f"bytes=0-{size - 1}"}
    async with client.get(URL, headers=headers) as resp:
        resp.raise_for_status()
        return await resp.read()


async def _parallel_get(client: aiohttp.ClientSession, size: int, config: FetchConfig) -> bytes:
    """Fetch first *size* bytes using parallel chunked Range GETs."""
    return await _fetch_chunks_with_hedging(client, URL, size, config)


def _time_coro(
    loop: asyncio.AbstractEventLoop,
    coro_factory: object,
    runs: int,
) -> list[float]:
    """Run a coroutine factory N times and return wall-clock times."""
    times: list[float] = []
    for _ in range(runs):
        t0 = time.monotonic()
        asyncio.run_coroutine_threadsafe(coro_factory(), loop).result()  # type: ignore[union-attr]
        times.append(time.monotonic() - t0)
    return times


def bench_crossover() -> None:
    """Find the single-vs-parallel crossover point."""
    # Test sizes from 1 MB to 128 MB
    sizes_mb = [1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128]

    config = FetchConfig(
        chunk_size_bytes=CHUNK_SIZE,
        max_parallel_requests=MAX_PARALLEL,
        parallel_threshold_bytes=1024,  # force parallel path
        speculative_retry_multiplier=2.0,
        timeout_seconds=120.0,
        max_fetch_bytes=256 * 1024 * 1024,
    )

    pool = _ensure_pool(config)
    assert pool.loop is not None
    assert pool.session is not None
    loop = pool.loop
    client = pool.session

    print(f"Chunk size: {CHUNK_SIZE / 1024 / 1024:.0f} MB, Parallel: {MAX_PARALLEL}")
    print(f"Runs per config: {RUNS} (best-of)\n")

    results: list[SizeResult] = []

    for size_mb in sizes_mb:
        size = size_mb * 1024 * 1024
        num_chunks = max(1, size // CHUNK_SIZE + (1 if size % CHUNK_SIZE else 0))

        # --- Single GET ---
        single_times = _time_coro(
            loop,
            lambda s=size: _single_get(client, s),
            RUNS,
        )
        single_best = min(single_times)

        # --- Parallel GET ---
        parallel_times = _time_coro(
            loop,
            lambda s=size: _parallel_get(client, s, config),
            RUNS,
        )
        parallel_best = min(parallel_times)

        single_mbps = (size_mb * 8) / single_best if single_best > 0 else 0
        parallel_mbps = (size_mb * 8) / parallel_best if parallel_best > 0 else 0
        speedup = single_best / parallel_best if parallel_best > 0 else 0

        single_str = ", ".join(f"{t:.3f}" for t in single_times)
        parallel_str = ", ".join(f"{t:.3f}" for t in parallel_times)
        winner = "PAR" if speedup > 1.0 else "GET"

        print(
            f"  {size_mb:>4} MB  ({num_chunks:>2} chunks)  "
            f"single: {single_best:.3f}s ({single_mbps:>6.0f} Mbps) [{single_str}]  "
            f"parallel: {parallel_best:.3f}s ({parallel_mbps:>6.0f} Mbps) [{parallel_str}]  "
            f"speedup: {speedup:.2f}x  {winner}"
        )

        results.append(SizeResult(
            size_mb=size_mb,
            single_best_s=single_best,
            parallel_best_s=parallel_best,
            single_mbps=single_mbps,
            parallel_mbps=parallel_mbps,
            speedup=speedup,
        ))

    config.close()

    # Summary
    print(f"\n{'='*72}")
    print("CROSSOVER ANALYSIS")
    print(f"{'='*72}")
    print(f"{'Size':>8} {'Single':>10} {'Parallel':>10} {'Speedup':>8} {'Winner':>7}")
    print(f"{'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*7}")
    crossover_found = False
    for r in results:
        winner = "PAR" if r.speedup > 1.0 else "GET"
        marker = " <-- crossover" if not crossover_found and r.speedup > 1.0 else ""
        if r.speedup > 1.0:
            crossover_found = True
        print(
            f"{r.size_mb:>6} MB {r.single_best_s:>9.3f}s {r.parallel_best_s:>9.3f}s "
            f"{r.speedup:>7.2f}x {winner:>7}{marker}"
        )

    # Recommend threshold
    for r in results:
        if r.speedup > 1.05:  # >5% improvement to justify overhead
            print(f"\nRecommended parallel_threshold_bytes: {int(r.size_mb)} MB "
                  f"({int(r.size_mb * 1024 * 1024)} bytes)")
            break


if __name__ == "__main__":
    bench_crossover()
