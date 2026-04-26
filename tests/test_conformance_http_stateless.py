# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Dedicated assertions for HTTP streaming statelessness.

The generic conformance suite is run against a round-robin client (see
``conformance_conn`` in conftest), which already forces exchanges to
land on different backends.  These tests add targeted assertions that
*both* backends actually handled exchanges — guarding against a
regression where an httpx-level sticky pool or protocol quirk silently
pins a stream to one server, which would make round-robin coverage a
false positive.
"""

from __future__ import annotations

from typing import cast

import httpx

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import AnnotatedBatch


def test_health_reports_distinct_server_ids(conformance_http_two_servers: tuple[int, int]) -> None:
    """Sanity check: the two shared-key workers report different server_ids."""
    port_a, port_b = conformance_http_two_servers
    ids = set()
    for port in (port_a, port_b):
        resp = httpx.get(f"http://127.0.0.1:{port}/health", timeout=5.0)
        resp.raise_for_status()
        ids.add(resp.json()["server_id"])
    assert len(ids) == 2, f"expected distinct server_ids across the two backends, got {ids}"


def test_exchange_accumulate_survives_backend_swap(conformance_http_two_servers: tuple[int, int]) -> None:
    """Running-sum state must round-trip through the signed token, not live in one process.

    ``exchange_accumulate`` maintains ``running_sum`` and ``exchange_count``
    in its ``StreamState``.  With round-robin routing every exchange lands
    on a different backend; if the server stashed state in a process-local
    dict instead of the signed token, the counters would reset (or the
    second exchange would error).
    """
    from tests.conftest import _make_roundrobin_client

    client = _make_roundrobin_client(conformance_http_two_servers)
    try:
        with http_connect(ConformanceService, client=client) as proxy, proxy.exchange_accumulate() as session:
            out1 = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert abs(cast(float, out1.batch.column("running_sum")[0].as_py()) - 3.0) < 1e-6
            assert cast(int, out1.batch.column("exchange_count")[0].as_py()) == 1

            out2 = session.exchange(AnnotatedBatch.from_pydict({"value": [10.0]}))
            assert abs(cast(float, out2.batch.column("running_sum")[0].as_py()) - 13.0) < 1e-6
            assert cast(int, out2.batch.column("exchange_count")[0].as_py()) == 2

            out3 = session.exchange(AnnotatedBatch.from_pydict({"value": [100.0, 200.0]}))
            assert abs(cast(float, out3.batch.column("running_sum")[0].as_py()) - 313.0) < 1e-6
            assert cast(int, out3.batch.column("exchange_count")[0].as_py()) == 3
    finally:
        client.close()


def test_producer_stream_survives_backend_swap(conformance_http_two_servers: tuple[int, int]) -> None:
    """A producer stream (client sends ticks) must survive backend swaps per tick."""
    from tests.conftest import _make_roundrobin_client

    client = _make_roundrobin_client(conformance_http_two_servers)
    try:
        with http_connect(ConformanceService, client=client) as proxy:
            total_rows = sum(ab.batch.num_rows for ab in proxy.produce_n(count=10))
            assert total_rows == 10
    finally:
        client.close()


def test_cancel_notification_survives_backend_swap(conformance_http_two_servers: tuple[int, int]) -> None:
    """``cancel()`` must reach whichever backend the round-robin client routes it to.

    Because stream state lives in the signed token rather than in any one
    process, a cancel POSTed against either backend must verify the token
    and run ``on_cancel`` exactly once. We reset the per-process probe
    counters on both backends, drive a cancellable exchange through the
    round-robin client, then sum the counters across backends and assert
    a single ``on_cancel`` fired.
    """
    from tests.conftest import _make_roundrobin_client

    port_a, port_b = conformance_http_two_servers

    for port in (port_a, port_b):
        with (
            httpx.Client(base_url=f"http://127.0.0.1:{port}") as direct,
            http_connect(ConformanceService, client=direct) as proxy,
        ):
            proxy.reset_cancel_probe()

    rr_client = _make_roundrobin_client(conformance_http_two_servers)
    try:
        with http_connect(ConformanceService, client=rr_client) as proxy:
            session = proxy.cancellable_exchange()
            session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
            session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
            session.cancel()
    finally:
        rr_client.close()

    with (
        httpx.Client(base_url=f"http://127.0.0.1:{port_a}") as direct,
        http_connect(ConformanceService, client=direct) as proxy,
    ):
        _produce, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert exchange_calls == 2, f"expected 2 exchanges across backends, got {exchange_calls}"
    assert on_cancel_calls == 1, f"expected on_cancel to fire exactly once across backends, got {on_cancel_calls}"
