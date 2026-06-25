# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the TCP transport (TcpTransport / make_tcp_pair / serve_tcp)."""

from __future__ import annotations

import threading
from typing import Protocol

import pytest

from vgi_rpc.rpc import (
    RpcServer,
    TcpTransport,
    make_tcp_pair,
    serve_tcp,
    tcp_connect,
)


class _EchoService(Protocol):
    """Minimal unary service for transport round-trip tests."""

    def echo(self, value: str) -> str:
        """Return *value* with a ``!`` suffix."""
        ...


class _EchoImpl:
    """Echo implementation."""

    def echo(self, value: str) -> str:
        """Return *value* with a ``!`` suffix."""
        return value + "!"


def _start_server(host: str = "127.0.0.1", *, threaded: bool) -> tuple[str, int, threading.Thread]:
    """Start ``serve_tcp`` on a daemon thread and return the bound ``(host, port, thread)``."""
    bound_host = ""
    bound_port = 0
    ready = threading.Event()

    def _on_bound(h: str, p: int) -> None:
        nonlocal bound_host, bound_port
        bound_host = h
        bound_port = p
        ready.set()

    server = RpcServer(_EchoService, _EchoImpl())
    thread = threading.Thread(
        target=lambda: serve_tcp(server, host, 0, threaded=threaded, on_bound=_on_bound),
        daemon=True,
    )
    thread.start()
    assert ready.wait(5), "serve_tcp did not bind within 5s"
    return bound_host, bound_port, thread


def test_make_tcp_pair_roundtrip() -> None:
    """A direct TcpTransport pair carries a unary round trip."""
    client_transport, server_transport = make_tcp_pair()
    assert isinstance(client_transport, TcpTransport)
    server = RpcServer(_EchoService, _EchoImpl())
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        from vgi_rpc.rpc._client import _RpcProxy

        proxy = _RpcProxy(_EchoService, client_transport, None)
        assert proxy.echo(value="hi") == "hi!"
    finally:
        client_transport.close()
        thread.join(timeout=5)
        server_transport.close()


def test_serve_tcp_sequential_roundtrip() -> None:
    """serve_tcp in sequential mode binds loopback and serves a call."""
    host, port, _ = _start_server(threaded=False)
    assert host == "127.0.0.1"
    assert port > 0
    with tcp_connect(_EchoService, host, port) as svc:
        assert svc.echo(value="seq") == "seq!"


def test_serve_tcp_threaded_roundtrip() -> None:
    """serve_tcp in threaded mode serves concurrent clients."""
    host, port, _ = _start_server(threaded=True)
    with tcp_connect(_EchoService, host, port) as svc1, tcp_connect(_EchoService, host, port) as svc2:
        assert svc1.echo(value="a") == "a!"
        assert svc2.echo(value="b") == "b!"


def test_serve_tcp_defaults_to_loopback() -> None:
    """The default host is loopback-only (127.0.0.1)."""
    host, port, _ = _start_server(threaded=True)
    assert host == "127.0.0.1"
    assert port > 0


def test_idle_timeout_requires_threaded() -> None:
    """serve_tcp rejects idle_timeout without threaded=True."""
    server = RpcServer(_EchoService, _EchoImpl())
    with pytest.raises(ValueError, match="idle_timeout requires threaded=True"):
        serve_tcp(server, "127.0.0.1", 0, idle_timeout=1.0)


def test_tcp_connect_host_port_parsing() -> None:
    """tcp_connect accepts an explicit host and port."""
    host, port, _ = _start_server(threaded=True)
    # Connect via the loopback hostname as well as the dotted address.
    with tcp_connect(_EchoService, host, port) as svc:
        assert svc.echo(value="x") == "x!"
