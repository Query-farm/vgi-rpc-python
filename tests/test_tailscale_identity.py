# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Adversarial Tailscale Serve and LocalAPI identity-provider tests."""

from __future__ import annotations

import contextlib
import json
import socketserver
import threading
import time
from collections.abc import Callable, Iterator
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, urlsplit

import pytest

from vgi_rpc import (
    AuthContext,
    PeerIdentityStatus,
    PeerResolutionContext,
    SubjectKind,
    SubjectStability,
)
from vgi_rpc import (
    tailscale_localapi_provider as top_level_localapi_provider,
)
from vgi_rpc import (
    tailscale_serve_header_provider as top_level_serve_provider,
)
from vgi_rpc.http import tailscale_localapi_provider, tailscale_serve_header_provider
from vgi_rpc.rpc import PeerEvidenceSet, peer_identity_primary

_Response = tuple[int, dict[str, str], bytes]
_Callback = Callable[[str, Message], _Response]


class _CallbackServer(Protocol):
    callback: _Callback


class _FakeHTTPServer(ThreadingHTTPServer):
    callback: _Callback
    daemon_threads = True


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        status, headers, body = cast("_CallbackServer", self.server).callback(self.path, self.headers)
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        if "Content-Length" not in headers and "Transfer-Encoding" not in headers:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


@contextlib.contextmanager
def _http_server(callback: _Callback) -> Iterator[str]:
    server = _FakeHTTPServer(("127.0.0.1", 0), _Handler)
    server.callback = callback
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@contextlib.contextmanager
def _unix_server(path: Path, callback: _Callback) -> Iterator[str]:
    unix_stream_server = getattr(socketserver, "UnixStreamServer", None)
    if unix_stream_server is None:
        pytest.skip("Unix-domain LocalAPI transport is not available on this platform")
    fake_server_type = type(
        "_FakeUnixServer",
        (socketserver.ThreadingMixIn, unix_stream_server),
        {"daemon_threads": True},
    )
    server: Any = fake_server_type(str(path), _Handler)
    server.callback = callback
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield str(path)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        path.unlink(missing_ok=True)


def _whois(*, tagged: bool = False, capabilities: object | None = None) -> bytes:
    return json.dumps(
        {
            "Node": {
                "ID": 44,
                "StableID": "n123CNTRL",
                "Name": "client.example.ts.net.",
                "User": 123,
                "Addresses": ["100.64.0.10/32"],
                "Tags": ["tag:batch-worker"] if tagged else [],
            },
            "UserProfile": {
                "ID": 123,
                "LoginName": "alice@example.com",
                "DisplayName": "Alice Architect",
            },
            "CapMap": capabilities if capabilities is not None else {"example.com/cap/run": [{"queue": "blue"}]},
        },
        separators=(",", ":"),
    ).encode()


def _json_response(body: bytes, status: int = 200) -> _Response:
    return status, {"Content-Type": "application/json"}, body


def test_tailscale_providers_are_publicly_exported() -> None:
    """Both canonical adapters are available from the HTTP and top-level APIs."""
    assert top_level_localapi_provider is tailscale_localapi_provider
    assert top_level_serve_provider is tailscale_serve_header_provider


def test_tailscale_serve_user_is_verified_but_login_stability() -> None:
    """Serve login evidence is proxy-verified but never mislabeled stable."""
    provider = tailscale_serve_header_provider(issuer="tailnet:example", trusted_proxy_addresses={"127.0.0.1"})
    result = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="127.0.0.1",
            headers={
                "Tailscale-User-Login": ("alice@example.com",),
                "Tailscale-User-Name": ("=?utf-8?q?Ferris_B=C3=BCller?=",),
            },
        )
    )
    identity = result.identities[0]
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert identity.subject_kind is SubjectKind.USER
    assert identity.subject_key == "login:alice@example.com"
    assert identity.subject_stability is SubjectStability.LOGIN
    assert identity.subject_verified
    assert identity.attributes["user_display_name"] == "Ferris Büller"
    evidence = PeerEvidenceSet.from_results((result,))
    with pytest.raises(PermissionError, match="stable subject"):
        peer_identity_primary("tailscale")(evidence, AuthContext.anonymous())


def test_tailscale_serve_capability_only_request_is_subjectless() -> None:
    """Tagged clients can carry verified capabilities without a user principal."""
    provider = tailscale_serve_header_provider(issuer="tailnet:example", trusted_proxy_addresses={"proxy"})
    encoded = '=?utf-8?q?{"example.com/cap/monitoring":[{"role":"=F0=9F=90=BF=EF=B8=8F"}]}?='
    result = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={"Tailscale-App-Capabilities": (encoded,)},
        )
    )
    identity = result.identities[0]
    assert identity.subject_kind is SubjectKind.UNKNOWN
    assert identity.subject_key is None
    assert identity.capabilities_verified
    assert identity.capabilities["example.com/cap/monitoring"][0]["role"] == "🐿️"


def test_tailscale_serve_funnel_and_proxy_boundary_fail_closed() -> None:
    """Funnel never produces identity, and untrusted peers cannot assert headers."""
    provider = tailscale_serve_header_provider(issuer="tailnet:example", trusted_proxy_addresses={"proxy"})
    headers = {"Tailscale-User-Login": ("alice@example.com",)}
    assert (
        provider(PeerResolutionContext(transport="http", immediate_peer="attacker", headers=headers)).status
        is PeerIdentityStatus.UNTRUSTED_PROXY
    )
    funnel = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={**headers, "Tailscale-Funnel-Request": ("?1",)},
        )
    )
    assert funnel.status is PeerIdentityStatus.NOT_APPLICABLE


@pytest.mark.parametrize(
    "headers",
    [
        {"Tailscale-User-Login": ("one@example.com", "two@example.com")},
        {"Tailscale-User-Login": ("=?utf-8?b?YWxpY2U=?=",)},
        {"Tailscale-User-Login": ("alice\x1f@example.com",)},
        {"Tailscale-User-Name": ("=?utf-8?q?Alice=7FAdmin?=",), "Tailscale-User-Login": ("alice@example.com",)},
        {"Tailscale-User-Name": ("Alice",)},
        {"Tailscale-App-Capabilities": ('{"example.com/cap/run":[],"example.com/cap/run":[]}',)},
        {"Tailscale-App-Capabilities": ('{"example.com/cap/run":["admin"]}',)},
        {"Tailscale-Funnel-Request": ("true",)},
    ],
)
def test_tailscale_serve_rejects_ambiguous_or_malformed_headers(headers: dict[str, tuple[str, ...]]) -> None:
    """Duplicate, non-Q, orphaned, malformed JSON, and fake Funnel values are invalid."""
    provider = tailscale_serve_header_provider(issuer="tailnet:example", trusted_proxy_addresses={"proxy"})
    result = provider(PeerResolutionContext(transport="http", immediate_peer="proxy", headers=headers))
    assert result.status is PeerIdentityStatus.INVALID


def test_tailscale_localapi_user_service_scope_and_basic_auth() -> None:
    """WhoIs uses official Host/auth and service-scoped capability lookup."""
    requests: list[str] = []

    def callback(path: str, headers: Message) -> _Response:
        requests.append(path)
        assert headers["Host"] == "local-tailscaled.sock"
        assert headers["Authorization"] == "Basic OnNlY3JldA=="
        query = parse_qs(urlsplit(path).query)
        assert query == {"addr": ["100.64.0.10:4242"], "proto": ["tcp"], "svc_name": ["svc:analytics"]}
        return _json_response(_whois())

    with _http_server(callback) as endpoint:
        provider = tailscale_localapi_provider(
            issuer="tailnet:example",
            endpoint=endpoint,
            password="secret",
        )
        context = PeerResolutionContext(
            transport="tcp",
            immediate_peer="100.64.0.10:4242",
            destination_address="192.0.2.20:9400",
            service_name="svc:analytics",
        )
        first = provider(context)
        second = provider(context)
    identity = first.identities[0]
    assert len(requests) == 2  # no cache: every connection gets a fresh snapshot
    assert second.status is PeerIdentityStatus.AVAILABLE
    assert identity.subject_key == "user:123"
    assert identity.subject_stability is SubjectStability.STABLE
    assert identity.attributes["capability_target"] == {"kind": "service", "value": "svc:analytics"}
    assert identity.capabilities_verified


def test_tailscale_localapi_accepts_chunked_official_http_transport() -> None:
    """HTTP endpoint framing remains bounded when a daemon uses chunked JSON."""
    body = _whois()
    chunked = f"{len(body):X}\r\n".encode() + body + b"\r\n0\r\n\r\n"
    with _http_server(
        lambda path, headers: (200, {"Content-Type": "application/json", "Transfer-Encoding": "chunked"}, chunked)
    ) as endpoint:
        provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint=endpoint)
        result = provider(PeerResolutionContext(transport="tcp", immediate_peer="100.64.0.10:1"))
    assert result.status is PeerIdentityStatus.AVAILABLE


def test_tailscale_localapi_uses_http_source_endpoint_with_port() -> None:
    """WhoIs uses the socket endpoint while proxy trust can retain only the IP."""

    def callback(path: str, headers: Message) -> _Response:
        del headers
        query = parse_qs(urlsplit(path).query)
        assert query["addr"] == ["100.64.0.10:4242"]
        assert query["proto"] == ["tcp"]
        return _json_response(_whois())

    with _http_server(callback) as endpoint:
        provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint=endpoint)
        result = provider(
            PeerResolutionContext(
                transport="http",
                immediate_peer="100.64.0.10",
                source_endpoint="100.64.0.10:4242",
            )
        )
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert result.identities[0].source_address == "100.64.0.10:4242"


def test_tailscale_localapi_tagged_node_ignores_user_profile_over_unix_socket(tmp_path: Path) -> None:
    """Tags switch the stable principal from UserProfile to StableNodeID."""

    def callback(path: str, headers: Message) -> _Response:
        assert headers["Host"] == "local-tailscaled.sock"
        query = parse_qs(urlsplit(path).query)
        assert query["dst_ip"] == ["2001:db8::8"]
        return _json_response(_whois(tagged=True))

    socket_path = Path("/tmp") / f"vgi-ts-{time.time_ns()}-{tmp_path.name}.sock"
    with _unix_server(socket_path, callback) as unix_socket:
        provider = tailscale_localapi_provider(issuer="tailnet:example", unix_socket=unix_socket)
        result = provider(
            PeerResolutionContext(
                transport="http",
                asserted_peer="100.64.0.10:4242",
                immediate_peer="127.0.0.1",
                destination_address="[2001:db8::8]:443",
            )
        )
    identity = result.identities[0]
    assert identity.subject_kind is SubjectKind.TAGGED_NODE
    assert identity.subject_key == "node:n123CNTRL"
    assert "user_id" not in identity.attributes


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (403, PeerIdentityStatus.PERMISSION_DENIED),
        (404, PeerIdentityStatus.NO_MATCH),
        (503, PeerIdentityStatus.UNAVAILABLE),
        (400, PeerIdentityStatus.INVALID),
    ],
)
def test_tailscale_localapi_preserves_distinct_http_outcomes(status: int, expected: PeerIdentityStatus) -> None:
    """Permission, no-match, daemon outage, and invalid request remain distinct."""
    with _http_server(lambda path, headers: _json_response(b"{}", status)) as endpoint:
        provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint=endpoint)
        result = provider(PeerResolutionContext(transport="tcp", immediate_peer="100.64.0.10:1"))
    assert result.status is expected


@pytest.mark.parametrize(
    "body",
    [
        b'{"Node":{},"Node":{},"UserProfile":{"ID":1}}',
        b'{"Node":{},"UserProfile":{"ID":1},"CapMap":[]}',
        b'{"Node":{"StableID":"bad\\ud800","Tags":["tag:worker"]},"CapMap":{}}',
        b'{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":{"example.com/cap/run":[NaN]}}',
        b'{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":{"bad\xff":[]}}',
        b"not-json",
    ],
)
def test_tailscale_localapi_rejects_malformed_json(body: bytes) -> None:
    """Duplicate keys, invalid shapes, and invalid JSON cannot become evidence."""
    with _http_server(lambda path, headers: _json_response(body)) as endpoint:
        provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint=endpoint)
        result = provider(PeerResolutionContext(transport="tcp", immediate_peer="100.64.0.10:1"))
    assert result.status is PeerIdentityStatus.INVALID


def test_tailscale_localapi_bounds_response_and_total_deadline() -> None:
    """Oversized and slow daemon responses fail without escaping the request budget."""
    with _http_server(lambda path, headers: _json_response(b"x" * 128)) as endpoint:
        provider = tailscale_localapi_provider(
            issuer="tailnet:example",
            endpoint=endpoint,
            max_response_bytes=64,
        )
        assert (
            provider(PeerResolutionContext(transport="tcp", immediate_peer="100.64.0.10:1")).status
            is PeerIdentityStatus.INVALID
        )

    def slow(path: str, headers: Message) -> _Response:
        del path, headers
        time.sleep(0.2)
        return _json_response(_whois())

    with _http_server(slow) as endpoint:
        provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint=endpoint, timeout=0.03)
        started = time.monotonic()
        result = provider(PeerResolutionContext(transport="tcp", immediate_peer="100.64.0.10:1"))
        elapsed = time.monotonic() - started
    assert result.status is PeerIdentityStatus.UNAVAILABLE
    assert elapsed < 0.15


def test_tailscale_localapi_rejects_invalid_scope_without_dialing() -> None:
    """Invalid service targets and absent peers produce inert status outcomes."""
    provider = tailscale_localapi_provider(issuer="tailnet:example", endpoint="http://127.0.0.1:1")
    absent = provider(PeerResolutionContext(transport="tcp"))
    invalid = provider(
        PeerResolutionContext(
            transport="tcp",
            immediate_peer="100.64.0.10:1",
            service_name="svc:not.a-dns-label",
        )
    )
    assert absent.status is PeerIdentityStatus.NOT_APPLICABLE
    assert invalid.status is PeerIdentityStatus.INVALID
