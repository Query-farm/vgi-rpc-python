# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests that prove concurrent connection handling on threaded Unix sockets."""

from __future__ import annotations

import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.conftest import _short_unix_path, _wait_for_unix

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets not available on Windows")

_SERVE_FIXTURE_UNIX_THREADED = str(Path(__file__).parent / "serve_fixture_unix_threaded.py")


@pytest.fixture()
def threaded_server_path() -> Iterator[str]:
    """Spawn a threaded Unix socket server for the duration of one test."""
    path = _short_unix_path("conc")
    proc = subprocess.Popen(
        [sys.executable, _SERVE_FIXTURE_UNIX_THREADED, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_threaded_two_concurrent_streams(threaded_server_path: str) -> None:
    """Two clients stream simultaneously on the same socket — proves parallelism.

    Each client opens a ``generate`` stream that yields 5 batches.  A
    ``threading.Barrier`` ensures both clients are actively mid-stream
    at the same time.  If the server were sequential, the second client
    would block until the first finishes, causing the barrier to time out.
    """
    from tests.test_rpc import RpcFixtureService
    from vgi_rpc.rpc import unix_connect

    barrier = threading.Barrier(2, timeout=10)
    results: list[list[object]] = [[], []]
    errors: list[BaseException | None] = [None, None]

    def _stream_client(idx: int) -> None:
        try:
            with unix_connect(RpcFixtureService, threaded_server_path) as proxy:
                stream = proxy.generate(count=5)
                first = True
                for ab in stream:
                    results[idx].append(ab.batch.column("i")[0].as_py())
                    if first:
                        # Both clients synchronise after reading their first batch.
                        # If the server is sequential this barrier will time out.
                        barrier.wait()
                        first = False
        except BaseException as exc:
            errors[idx] = exc

    t1 = threading.Thread(target=_stream_client, args=(0,))
    t2 = threading.Thread(target=_stream_client, args=(1,))
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)

    for i, err in enumerate(errors):
        assert err is None, f"Client {i} raised: {err}"

    # Both clients should have read all 5 batches
    assert results[0] == [0, 1, 2, 3, 4]
    assert results[1] == [0, 1, 2, 3, 4]


def test_threaded_concurrent_unary(threaded_server_path: str) -> None:
    """Multiple unary calls execute concurrently on the same threaded socket."""
    from tests.test_rpc import RpcFixtureService
    from vgi_rpc.rpc import unix_connect

    results: list[float | None] = [None] * 4
    errors: list[BaseException | None] = [None] * 4

    def _unary_client(idx: int) -> None:
        try:
            with unix_connect(RpcFixtureService, threaded_server_path) as proxy:
                results[idx] = proxy.add(a=float(idx), b=10.0)
        except BaseException as exc:
            errors[idx] = exc

    threads = [threading.Thread(target=_unary_client, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    for i, err in enumerate(errors):
        assert err is None, f"Client {i} raised: {err}"

    for i in range(4):
        assert results[i] == float(i) + 10.0
