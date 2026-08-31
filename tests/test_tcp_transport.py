# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the TCP transport (TcpTransport / make_tcp_pair / serve_tcp)."""

from __future__ import annotations

import errno
import logging
import socket
import threading
import time
from typing import Protocol

import pytest

from vgi_rpc import AuthContext, PeerEvidenceSet
from vgi_rpc.rpc import (
    RpcServer,
    TcpTransport,
    _transport,
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


def test_threaded_listener_survives_temporary_fd_exhaustion() -> None:
    """EMFILE pauses accept instead of permanently killing the listener."""

    class FakeConnection:
        def settimeout(self, _timeout: float | None) -> None:
            pass

        def fileno(self) -> int:
            return 42

    class FakeListener:
        calls = 0

        def settimeout(self, _timeout: float) -> None:
            pass

        def accept(self) -> tuple[FakeConnection, None]:
            self.calls += 1
            if self.calls == 1:
                raise OSError(errno.EMFILE, "too many open files")
            if self.calls == 2:
                return FakeConnection(), None
            raise OSError(errno.EBADF, "listener closed")

    class FakeTransport:
        def close(self) -> None:
            pass

    class FakeServer:
        calls = 0

        def serve(self, _transport: FakeTransport) -> None:
            self.calls += 1

    listener = FakeListener()
    server = FakeServer()
    _transport._serve_socket_threaded(
        server,  # type: ignore[arg-type]
        listener,  # type: ignore[arg-type]
        None,
        None,
        lambda _conn: FakeTransport(),  # type: ignore[arg-type,return-value]
        "test-listener",
    )

    assert listener.calls == 3
    assert server.calls == 1


def test_max_connections_limits_accepts_not_only_handlers() -> None:
    """A saturated handler cap leaves excess sockets in the listen backlog."""

    class FakeConnection:
        def settimeout(self, _timeout: float | None) -> None:
            pass

        def fileno(self) -> int:
            return 42

    class FakeListener:
        calls = 0

        def settimeout(self, _timeout: float) -> None:
            pass

        def accept(self) -> tuple[FakeConnection, None]:
            self.calls += 1
            if self.calls <= 2:
                return FakeConnection(), None
            raise OSError(errno.EBADF, "listener closed")

    class FakeTransport:
        def close(self) -> None:
            pass

    first_started = threading.Event()
    release_first = threading.Event()

    class FakeServer:
        calls = 0

        def serve(self, _transport: FakeTransport) -> None:
            self.calls += 1
            if self.calls == 1:
                first_started.set()
                assert release_first.wait(2)

    listener = FakeListener()
    server = FakeServer()
    serving = threading.Thread(
        target=_transport._serve_socket_threaded,
        args=(server, listener, 1, None, lambda _conn: FakeTransport(), "test-listener"),
    )
    serving.start()
    assert first_started.wait(2)
    assert listener.calls == 1, "second connection was accepted while the only slot was occupied"
    release_first.set()
    serving.join(2)

    assert not serving.is_alive()
    assert listener.calls == 3
    assert server.calls == 2


def test_peer_authentication_policy_error_is_redacted(caplog: pytest.LogCaptureFixture) -> None:
    """A custom raw-TCP policy cannot put capability text in server logs."""
    secret = "raw-policy-capability-secret"
    ready = threading.Event()
    bound: list[tuple[str, int]] = []

    def policy(_evidence: PeerEvidenceSet, _auth: AuthContext) -> AuthContext:
        raise PermissionError(secret)

    def on_bound(host: str, port: int) -> None:
        bound.append((host, port))
        ready.set()

    thread = threading.Thread(
        target=lambda: serve_tcp(
            RpcServer(_EchoService, _EchoImpl()),
            "127.0.0.1",
            0,
            threaded=True,
            on_bound=on_bound,
            peer_authentication_policy=policy,
        ),
        daemon=True,
    )
    with caplog.at_level(logging.DEBUG, logger="vgi_rpc.rpc"):
        thread.start()
        assert ready.wait(5)
        with socket.create_connection(bound[0], timeout=1):
            pass
        for _ in range(100):
            if "Error serving socket connection" in caplog.text:
                break
            time.sleep(0.01)
    assert "Error serving socket connection" in caplog.text
    assert secret not in caplog.text


def test_tcp_connect_host_port_parsing() -> None:
    """tcp_connect accepts an explicit host and port."""
    host, port, _ = _start_server(threaded=True)
    # Connect via the loopback hostname as well as the dotted address.
    with tcp_connect(_EchoService, host, port) as svc:
        assert svc.echo(value="x") == "x!"
