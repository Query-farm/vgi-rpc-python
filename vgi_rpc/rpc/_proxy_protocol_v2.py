# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bounded HAProxy PROXY protocol v2 parsing for trusted TCP ingress."""

from __future__ import annotations

import ipaddress
import socket
import time
from dataclasses import dataclass

_SIGNATURE = b"\r\n\r\n\0\r\nQUIT\n"
_FIXED_BYTES = 16
_VERSION_2 = 0x20
_PROXY_COMMAND = 0x01
_TCP_IPV4 = 0x11
_TCP_IPV6 = 0x21


class ProxyProtocolV2Error(ValueError):
    """The trusted proxy sent a malformed or unsupported v2 preamble."""


@dataclass(frozen=True)
class ProxyProtocolV2Address:
    """Source and destination asserted by one validated TCP preamble."""

    source_address: str
    source_port: int
    destination_address: str
    destination_port: int


def proxy_protocol_v2_size(prefix: bytes, *, maximum_bytes: int = 4096) -> int:
    """Return the exact preamble size after validating its fixed prefix."""
    if len(prefix) != _FIXED_BYTES:
        raise ProxyProtocolV2Error("PROXY v2 fixed preamble must be exactly 16 bytes")
    if prefix[:12] != _SIGNATURE:
        raise ProxyProtocolV2Error("missing PROXY v2 signature")
    if prefix[12] & 0xF0 != _VERSION_2:
        raise ProxyProtocolV2Error("unsupported PROXY protocol version")
    total = _FIXED_BYTES + int.from_bytes(prefix[14:16], "big")
    if maximum_bytes < _FIXED_BYTES or total > maximum_bytes:
        raise ProxyProtocolV2Error("PROXY v2 preamble exceeds configured limit")
    return total


def parse_proxy_protocol_v2(preamble: bytes, *, maximum_bytes: int = 4096) -> ProxyProtocolV2Address:
    """Parse one exact v2 TCP/IPv4 or TCP/IPv6 preamble."""
    if len(preamble) < _FIXED_BYTES:
        raise ProxyProtocolV2Error("truncated PROXY v2 fixed preamble")
    expected = proxy_protocol_v2_size(preamble[:_FIXED_BYTES], maximum_bytes=maximum_bytes)
    if len(preamble) != expected:
        raise ProxyProtocolV2Error("truncated or overlong PROXY v2 preamble")
    if preamble[12] & 0x0F != _PROXY_COMMAND:
        raise ProxyProtocolV2Error("PROXY v2 LOCAL command is not accepted")

    body = memoryview(preamble)[_FIXED_BYTES:]
    family_protocol = preamble[13]
    if family_protocol == _TCP_IPV4:
        address_bytes = 12
        if len(body) < address_bytes:
            raise ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv4 address block")
        source = str(ipaddress.IPv4Address(bytes(body[:4])))
        destination = str(ipaddress.IPv4Address(bytes(body[4:8])))
        source_port = int.from_bytes(body[8:10], "big")
        destination_port = int.from_bytes(body[10:12], "big")
    elif family_protocol == _TCP_IPV6:
        address_bytes = 36
        if len(body) < address_bytes:
            raise ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv6 address block")
        source_ip = ipaddress.IPv6Address(bytes(body[:16]))
        destination_ip = ipaddress.IPv6Address(bytes(body[16:32]))
        source = str(source_ip.ipv4_mapped or source_ip)
        destination = str(destination_ip.ipv4_mapped or destination_ip)
        source_port = int.from_bytes(body[32:34], "big")
        destination_port = int.from_bytes(body[34:36], "big")
    else:
        raise ProxyProtocolV2Error("PROXY v2 requires TCP over IPv4 or IPv6")

    offset = address_bytes
    while offset < len(body):
        if len(body) - offset < 3:
            raise ProxyProtocolV2Error("PROXY v2 contains a truncated TLV header")
        length = int.from_bytes(body[offset + 1 : offset + 3], "big")
        offset += 3
        if length > len(body) - offset:
            raise ProxyProtocolV2Error("PROXY v2 contains a truncated TLV value")
        offset += length
    return ProxyProtocolV2Address(source, source_port, destination, destination_port)


def read_proxy_protocol_v2(
    sock: socket.socket,
    *,
    timeout: float = 1.0,
    maximum_bytes: int = 4096,
) -> ProxyProtocolV2Address:
    """Read exactly one preamble under an independent monotonic deadline."""
    if timeout <= 0:
        raise ValueError("PROXY v2 preamble timeout must be positive")
    deadline = time.monotonic() + timeout
    prefix = _recv_exact(sock, _FIXED_BYTES, deadline)
    total = proxy_protocol_v2_size(prefix, maximum_bytes=maximum_bytes)
    remainder = _recv_exact(sock, total - _FIXED_BYTES, deadline)
    return parse_proxy_protocol_v2(prefix + remainder, maximum_bytes=maximum_bytes)


def _recv_exact(sock: socket.socket, size: int, deadline: float) -> bytes:
    output = bytearray()
    try:
        while len(output) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProxyProtocolV2Error("PROXY v2 preamble timed out")
            sock.settimeout(remaining)
            try:
                chunk = sock.recv(size - len(output))
            except TimeoutError as exc:
                raise ProxyProtocolV2Error("PROXY v2 preamble timed out") from exc
            if not chunk:
                raise ProxyProtocolV2Error("truncated PROXY v2 preamble")
            output.extend(chunk)
    finally:
        sock.settimeout(None)
    return bytes(output)
