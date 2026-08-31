# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Adversarial tests for the explicit SOCKS5h TCP client path."""

from __future__ import annotations

import socket
import threading
import time
from typing import Any

import pytest

from vgi_rpc.rpc._socks import connect_socks5h


def _read_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise AssertionError("fake proxy received a truncated request")
        result.extend(chunk)
    return bytes(result)


def _send_fragmented(sock: socket.socket, data: bytes) -> None:
    for byte in data:
        sock.sendall(bytes((byte,)))
        time.sleep(0.001)


def _fake_proxy(
    listener: socket.socket,
    observed: dict[str, object],
) -> None:
    try:
        conn, _ = listener.accept()
        with conn:
            assert _read_exact(conn, 3) == b"\x05\x01\x00"
            _send_fragmented(conn, b"\x05\x00")
            header = _read_exact(conn, 4)
            assert header[:3] == b"\x05\x01\x00"
            atyp = header[3]
            if atyp == 1:
                raw_target = _read_exact(conn, 4)
                host = str(socket.inet_ntop(socket.AF_INET, raw_target))
            elif atyp == 4:
                raw_target = _read_exact(conn, 16)
                host = str(socket.inet_ntop(socket.AF_INET6, raw_target))
            elif atyp == 3:
                host = _read_exact(conn, _read_exact(conn, 1)[0]).decode("ascii")
            else:
                raise AssertionError(f"unexpected target address type {atyp}")
            port = int.from_bytes(_read_exact(conn, 2), "big")
            observed.update(atyp=atyp, host=host, port=port)
            _send_fragmented(conn, b"\x05\x00\x00\x01\x7f\x00\x00\x01\x24\xb8")
            while conn.recv(1):
                pass
    except BaseException as exc:
        observed["error"] = exc


@pytest.mark.parametrize(
    ("target", "expected_atyp", "expected_host"),
    [
        ("must-not-resolve.invalid", 3, "must-not-resolve.invalid"),
        ("café.example", 3, "xn--caf-dma.example"),
        ("192.0.2.10", 1, "192.0.2.10"),
        ("2001:db8::10", 4, "2001:db8::10"),
    ],
)
def test_connect_socks5h_supports_domain_ipv4_and_ipv6_targets(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    expected_atyp: int,
    expected_host: str,
) -> None:
    """Encode domain, IPv4, and IPv6 targets without target-side DNS."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    proxy_host, proxy_port = listener.getsockname()
    observed: dict[str, object] = {}
    thread = threading.Thread(target=_fake_proxy, args=(listener, observed), daemon=True)
    thread.start()

    original_getaddrinfo: Any = socket.getaddrinfo

    def guarded_getaddrinfo(host: str, *args: Any, **kwargs: Any) -> Any:
        assert host != target, "SOCKS5h target was resolved locally"
        return original_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
    sock = connect_socks5h(target, 19400, f"socks5h://{proxy_host}:{proxy_port}", timeout=2.0)
    sock.close()
    thread.join(timeout=2)
    listener.close()

    assert not thread.is_alive()
    assert "error" not in observed
    assert observed == {"atyp": expected_atyp, "host": expected_host, "port": 19400}


def test_connect_socks5h_rejects_userinfo() -> None:
    """Milestone one permits only the SOCKS NO AUTH method."""
    with pytest.raises(ValueError, match="credentials"):
        connect_socks5h(
            "must-not-resolve.invalid",
            9400,
            "socks5h://user:password@127.0.0.1:1080",
            timeout=1.0,
        )


def test_connect_socks5h_rejects_control_characters() -> None:
    """Embedded NUL/control text cannot be truncated or sent as a DNS name."""
    with pytest.raises(ValueError, match="control"):
        connect_socks5h(
            "safe.example\0hidden",
            9400,
            "socks5h://127.0.0.1:1080",
            timeout=1.0,
        )


def test_connect_socks5h_proxy_failure_never_falls_back_to_target() -> None:
    """A failed proxy cannot silently turn into a direct connection."""
    target = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target.bind(("127.0.0.1", 0))
    target.listen(1)
    target.settimeout(0.1)
    target_host, target_port = target.getsockname()

    unused_proxy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    unused_proxy.bind(("127.0.0.1", 0))
    proxy_port = unused_proxy.getsockname()[1]
    unused_proxy.close()

    with pytest.raises(OSError):
        connect_socks5h(
            target_host,
            target_port,
            f"socks5h://127.0.0.1:{proxy_port}",
            timeout=0.5,
        )
    with pytest.raises(TimeoutError):
        target.accept()
    target.close()
