# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for redaction-safe VGI mesh diagnostics."""

from __future__ import annotations

import json
import socket
import threading
from typing import Protocol

from typer.testing import CliRunner

from vgi_rpc.cli import app
from vgi_rpc.mesh_doctor import diagnose_tcp
from vgi_rpc.rpc import RpcServer, serve_tcp


class _DoctorService(Protocol):
    def ping(self) -> str:
        """Return a diagnostic value."""
        ...


class _DoctorImplementation:
    def ping(self) -> str:
        """Return a diagnostic value."""
        return "pong"


def test_mesh_doctor_rejects_ambiguous_endpoint_without_network() -> None:
    """Reject endpoint userinfo before performing any network operation."""
    result = diagnose_tcp("tcp://user@worker.example:9400")
    assert len(result) == 1
    assert result[0].check == "endpoint"
    assert result[0].status == "fail"


def test_mesh_doctor_reports_connectivity_without_leaking_proxy_uri() -> None:
    """Keep explicit proxy configuration out of failure reports."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]

    def reject() -> None:
        connection, _ = listener.accept()
        with connection:
            greeting = bytearray()
            while len(greeting) < 3:
                greeting.extend(connection.recv(3 - len(greeting)))
            connection.sendall(b"\x05\xff")
        listener.close()

    threading.Thread(target=reject, daemon=True).start()
    proxy = f"socks5h://127.0.0.1:{port}"
    results = diagnose_tcp("tcp://must-not-resolve.invalid:9400", proxy=proxy, timeout=1)
    rendered = json.dumps([item.json_value() for item in results])
    assert proxy not in rendered
    assert [(item.check, item.status) for item in results[:3]] == [
        ("endpoint", "pass"),
        ("dns", "not_applicable"),
        ("socks5h", "fail"),
    ]


def test_mesh_doctor_cli_json_and_failure_exit() -> None:
    """Expose stable machine-readable results and a failing exit code."""
    result = CliRunner().invoke(app, ["mesh", "doctor", "not-an-endpoint", "--json"])
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload[0]["check"] == "endpoint"
    assert payload[0]["status"] == "fail"


def test_mesh_doctor_performs_real_vgi_describe_handshake() -> None:
    """Distinguish a listening TCP port from a valid introspectable worker."""
    ready = threading.Event()
    bound_port = 0

    def bound(_host: str, port: int) -> None:
        nonlocal bound_port
        bound_port = port
        ready.set()

    thread = threading.Thread(
        target=serve_tcp,
        args=(RpcServer(_DoctorService, _DoctorImplementation(), enable_describe=True), "127.0.0.1", 0),
        kwargs={
            "threaded": True,
            "max_connections": 1,
            "idle_timeout": 0.05,
            "on_bound": bound,
        },
        daemon=True,
    )
    thread.start()
    assert ready.wait(2)
    results = diagnose_tcp(f"tcp://127.0.0.1:{bound_port}", timeout=1)
    assert {result.check: result.status for result in results} == {
        "endpoint": "pass",
        "dns": "pass",
        "connectivity": "pass",
        "vgi_handshake": "pass",
        "tailscale_whois": "not_applicable",
    }
    thread.join(timeout=2)
    assert not thread.is_alive()
