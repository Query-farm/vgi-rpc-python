# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Redaction-safe connectivity and identity diagnostics for VGI mesh links."""

from __future__ import annotations

import queue
import socket
import threading
import time
from dataclasses import asdict, dataclass
from typing import Literal
from urllib.parse import urlsplit

from vgi_rpc.http import tailscale_localapi_provider
from vgi_rpc.introspect import introspect
from vgi_rpc.rpc import PeerIdentityStatus, PeerResolutionContext, TcpTransport
from vgi_rpc.rpc._socks import connect_socks5h


@dataclass(frozen=True)
class MeshDiagnostic:
    """One bounded diagnostic result with no credential-bearing fields."""

    check: str
    status: Literal["pass", "fail", "not_applicable"]
    detail: str
    elapsed_ms: int

    def json_value(self) -> dict[str, str | int]:
        """Return a JSON-serializable result."""
        return asdict(self)


def _result(
    check: str,
    status: Literal["pass", "fail", "not_applicable"],
    detail: str,
    started: float,
) -> MeshDiagnostic:
    return MeshDiagnostic(check, status, detail, round((time.monotonic() - started) * 1000))


def _parse_tcp_endpoint(endpoint: str) -> tuple[str, int]:
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "tcp"
        or parsed.hostname is None
        or parsed.port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("endpoint must be tcp://HOST:PORT without userinfo, path, query, or fragment")
    return parsed.hostname, parsed.port


def _bounded_addresses(host: str, port: int, timeout: float) -> tuple[str, ...]:
    """Resolve in a daemon thread because the system resolver is not cancellable."""
    outcomes: queue.Queue[object] = queue.Queue(maxsize=1)

    def resolve() -> None:
        try:
            values = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            outcomes.put(tuple(dict.fromkeys(item[4][0] for item in values)))
        except Exception as exc:
            outcomes.put(exc)

    threading.Thread(target=resolve, name="vgi-mesh-doctor-dns", daemon=True).start()
    try:
        outcome = outcomes.get(timeout=timeout)
    except queue.Empty as exc:
        raise TimeoutError("DNS lookup exceeded the diagnostic deadline") from exc
    if isinstance(outcome, Exception):
        raise outcome
    assert isinstance(outcome, tuple)
    return outcome


def diagnose_tcp(
    endpoint: str,
    *,
    proxy: str | None = None,
    timeout: float = 5.0,
    tailscale_issuer: str | None = None,
    tailscale_source: str | None = None,
    tailscale_localapi: str | None = None,
    tailscale_password: str | None = None,
    service_name: str | None = None,
) -> tuple[MeshDiagnostic, ...]:
    """Diagnose endpoint parsing, DNS/SOCKS, VGI framing, and optional WhoIs.

    Sensitive evidence is intentionally reduced to outcome, assurance, and
    subject kind. Capability values, profile fields, tokens, certificates, and
    stable subject keys are never returned.
    """
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    results: list[MeshDiagnostic] = []
    started = time.monotonic()
    try:
        host, port = _parse_tcp_endpoint(endpoint)
    except (TypeError, ValueError) as exc:
        return (_result("endpoint", "fail", str(exc), started),)
    results.append(_result("endpoint", "pass", f"tcp endpoint on port {port}", started))

    started = time.monotonic()
    if proxy is None:
        try:
            addresses = _bounded_addresses(host, port, timeout)
            detail = f"resolved {len(addresses)} address family candidate(s)"
            results.append(_result("dns", "pass", detail, started))
        except (OSError, TimeoutError) as exc:
            results.append(_result("dns", "fail", str(exc), started))
            return tuple(results)
    else:
        results.append(_result("dns", "not_applicable", "target resolution is delegated to SOCKS5h", started))

    started = time.monotonic()
    sock: socket.socket | None = None
    transport: TcpTransport | None = None
    try:
        if proxy is None:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.settimeout(None)
        else:
            sock = connect_socks5h(host, port, proxy, timeout=timeout)
            results.append(
                _result("socks5h", "pass", "NO AUTH negotiation and proxy target connect succeeded", started)
            )
        results.append(_result("connectivity", "pass", "TCP connection established", started))
        transport = TcpTransport(sock)
        sock = None
        handshake_started = time.monotonic()
        description = introspect(transport)
        results.append(
            _result(
                "vgi_handshake",
                "pass",
                f"VGI describe version {description.describe_version}; {len(description.methods)} method(s)",
                handshake_started,
            )
        )
    except Exception as exc:
        check = ("socks5h" if proxy is not None else "connectivity") if transport is None else "vgi_handshake"
        results.append(_result(check, "fail", f"{type(exc).__name__}: {exc}", started))
    finally:
        if transport is not None:
            transport.close()
        elif sock is not None:
            sock.close()

    started = time.monotonic()
    if tailscale_issuer is None and tailscale_source is None:
        results.append(_result("tailscale_whois", "not_applicable", "no WhoIs source and issuer supplied", started))
    elif not tailscale_issuer or not tailscale_source:
        results.append(_result("tailscale_whois", "fail", "WhoIs requires both issuer and source address", started))
    else:
        try:
            if tailscale_localapi and tailscale_localapi.startswith("http://"):
                provider = tailscale_localapi_provider(
                    issuer=tailscale_issuer,
                    endpoint=tailscale_localapi,
                    password=tailscale_password,
                    timeout=timeout,
                )
            else:
                provider = tailscale_localapi_provider(
                    issuer=tailscale_issuer,
                    unix_socket=tailscale_localapi,
                    timeout=timeout,
                )
            destination = None
            try:
                socket.inet_pton(socket.AF_INET, host)
                destination = host
            except OSError:
                try:
                    socket.inet_pton(socket.AF_INET6, host)
                    destination = host
                except OSError:
                    pass
            evidence = provider(
                PeerResolutionContext(
                    transport="tcp",
                    immediate_peer=tailscale_source,
                    destination_address=destination,
                    service_name=service_name,
                    deadline=time.monotonic() + timeout,
                )
            )
            if evidence.status is PeerIdentityStatus.AVAILABLE:
                identity = evidence.identities[0]
                detail = f"available {identity.assurance.value} {identity.subject_kind.value} evidence"
                results.append(_result("tailscale_whois", "pass", detail, started))
            else:
                results.append(_result("tailscale_whois", "fail", evidence.status.value, started))
        except Exception as exc:
            results.append(_result("tailscale_whois", "fail", f"{type(exc).__name__}: {exc}", started))
    return tuple(results)
