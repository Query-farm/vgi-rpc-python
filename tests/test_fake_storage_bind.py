# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The fake storage fixture must start without a reverse DNS lookup."""

from __future__ import annotations

import socket
import urllib.request

import pytest

from vgi_rpc.conformance.fake_storage import serve_in_thread


def test_serve_in_thread_does_not_resolve_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """Starting the server never calls ``socket.getfqdn``, which can take 35 seconds."""

    def fail(name: str = "") -> str:
        raise AssertionError(f"socket.getfqdn({name!r}) called while starting the server")

    monkeypatch.setattr(socket, "getfqdn", fail)
    base_url, shutdown = serve_in_thread()
    try:
        with urllib.request.urlopen(f"{base_url}/_stats", timeout=10) as response:
            assert response.status == 200
    finally:
        shutdown()
