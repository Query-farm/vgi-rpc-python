# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Small, dependency-free SOCKS5h client dialer for raw RPC transports."""

from __future__ import annotations

import ipaddress
import socket
import time
import unicodedata
from urllib.parse import urlsplit


def _parse_proxy_uri(uri: str) -> tuple[str, int]:
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"invalid SOCKS5h proxy URI: {exc}") from exc
    if parsed.scheme != "socks5h":
        raise ValueError("TCP proxy must use socks5h://")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("SOCKS5h proxy credentials are not supported")
    if parsed.path or parsed.query or parsed.fragment:
        raise ValueError("SOCKS5h proxy URI must contain only host and port")
    if parsed.hostname is None or port is None:
        raise ValueError("SOCKS5h proxy URI must be socks5h://host:port")
    if not 1 <= port <= 65535:
        raise ValueError("SOCKS5h proxy port is out of range")
    return parsed.hostname, port


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("SOCKS5h connection timed out")
    return remaining


def _send_all(sock: socket.socket, data: bytes, deadline: float | None) -> None:
    view = memoryview(data)
    while view:
        sock.settimeout(_remaining(deadline))
        sent = sock.send(view)
        if sent == 0:
            raise ConnectionError("SOCKS5h proxy closed while reading the request")
        view = view[sent:]


def _read_exact(sock: socket.socket, size: int, deadline: float | None) -> bytes:
    result = bytearray()
    while len(result) < size:
        sock.settimeout(_remaining(deadline))
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise ConnectionError("SOCKS5h proxy returned a truncated reply")
        result.extend(chunk)
    return bytes(result)


def connect_socks5h(
    target_host: str,
    target_port: int,
    proxy: str,
    *,
    timeout: float | None,
) -> socket.socket:
    """Connect through a NO-AUTH SOCKS5 proxy without resolving the target."""
    if not target_host:
        raise ValueError("TCP host must not be empty")
    if any(unicodedata.category(char).startswith("C") for char in target_host):
        raise ValueError("SOCKS5h target contains a control or format character")
    if not 1 <= target_port <= 65535:
        raise ValueError("TCP port must be in [1, 65535]")
    if timeout is not None and timeout <= 0:
        raise ValueError("connect_timeout must be positive or None")
    proxy_host, proxy_port = _parse_proxy_uri(proxy)
    deadline = None if timeout is None else time.monotonic() + timeout

    # Only the proxy endpoint is resolved locally. Resolution time is charged
    # to the same monotonic budget as connect and SOCKS negotiation.
    addresses = socket.getaddrinfo(proxy_host, proxy_port, type=socket.SOCK_STREAM)
    last_error: OSError | None = None
    sock: socket.socket | None = None
    for family, socktype, proto, _canonname, sockaddr in addresses:
        candidate = socket.socket(family, socktype, proto)
        try:
            candidate.settimeout(_remaining(deadline))
            candidate.connect(sockaddr)
            sock = candidate
            break
        except OSError as exc:
            last_error = exc
            candidate.close()
    if sock is None:
        if last_error is not None:
            raise last_error
        raise OSError(f"SOCKS5h proxy {proxy_host!r} resolved without usable addresses")

    try:
        _send_all(sock, b"\x05\x01\x00", deadline)
        if _read_exact(sock, 2, deadline) != b"\x05\x00":
            raise ConnectionError("SOCKS5h proxy did not accept NO AUTH")

        try:
            target_ip = ipaddress.ip_address(target_host)
        except ValueError:
            encoded_host = target_host.encode("idna")
            labels = encoded_host.split(b".")
            allowed = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"
            if (
                len(encoded_host) > 253
                or any(not label or len(label) > 63 for label in labels)
                or any(label[:1] == b"-" or label[-1:] == b"-" for label in labels)
                or any(byte not in allowed for label in labels for byte in label)
            ):
                raise ValueError("SOCKS5h target is not a valid DNS IDNA A-label") from None
            encoded_target = b"\x03" + bytes((len(encoded_host),)) + encoded_host
        else:
            encoded_target = (b"\x01" if target_ip.version == 4 else b"\x04") + target_ip.packed
        request = b"\x05\x01\x00" + encoded_target + target_port.to_bytes(2, "big")
        _send_all(sock, request, deadline)

        reply = _read_exact(sock, 4, deadline)
        if reply[0] != 5 or reply[2] != 0:
            raise ConnectionError("malformed SOCKS5h connect reply")
        if reply[1] != 0:
            raise ConnectionError(f"SOCKS5h proxy rejected target (reply {reply[1]})")
        if reply[3] == 1:
            address_size = 4
        elif reply[3] == 4:
            address_size = 16
        elif reply[3] == 3:
            address_size = _read_exact(sock, 1, deadline)[0]
        else:
            raise ConnectionError("SOCKS5h reply used an unknown address type")
        _read_exact(sock, address_size + 2, deadline)
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock
    except BaseException:
        sock.close()
        raise
