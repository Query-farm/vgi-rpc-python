# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Cross-implementation conformance test for the OAuth token-proxy.

Verifies that the Python, Go, and TypeScript HTTP transports all expose the
same wire-level behavior for ``{prefix}/_oauth/token`` and the
``token_endpoint`` advertisement in the protected-resource metadata.

For each implementation, the test:

1. Spawns a mock IdP that serves an OIDC discovery document and a /token
   endpoint that captures POST form data.
2. Boots the implementation's HTTP server with PKCE + ``client_secret``
   configured against the mock IdP.
3. Asserts identical responses for: metadata advertisement, OPTIONS
   preflight, authorization_code/refresh_token forwarding, mismatched
   client_id rejection, unsupported grant_type rejection, IdP error
   passthrough, and wrong content-type rejection.

The Python implementation runs in-process. The Go and TS implementations
run as subprocesses; if their workers can't be located they're skipped
(the test still runs against Python alone, which keeps CI useful for
developers who only have one toolchain).
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs

import httpx
import pytest
from werkzeug.serving import make_server

from vgi_rpc import RpcServer
from vgi_rpc.http import OAuthResourceMetadata, make_wsgi_app


class _EchoService(Protocol):
    def echo(self, message: str) -> str: ...


class _EchoImpl:
    def echo(self, message: str) -> str:
        return message

CLIENT_ID = "my-client-id"
CLIENT_SECRET = "my-client-secret"
PREFIX = "/vgi"


# ---------------------------------------------------------------------------
# Mock IdP
# ---------------------------------------------------------------------------


class _MockIdP:
    """Minimal OIDC IdP for token-proxy conformance tests.

    Serves /.well-known/openid-configuration and a /token endpoint that
    records the most recent POST form so tests can inspect what the proxy
    forwarded.
    """

    def __init__(self) -> None:
        self.captured: dict[str, str] = {}
        self.resp_status = 200
        self.resp_body = b'{"access_token":"new","token_type":"Bearer","expires_in":3600}'
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.url = ""

    def start(self) -> None:
        idp = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: object) -> None:
                return

            def do_GET(self) -> None:
                if self.path == "/.well-known/openid-configuration":
                    body = json.dumps({
                        "issuer": idp.url,
                        "authorization_endpoint": idp.url + "/authorize",
                        "token_endpoint": idp.url + "/token",
                    }).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.path != "/token":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length).decode("utf-8")
                idp.captured = {k: v[0] for k, v in parse_qs(raw).items() if v}
                self.send_response(idp.resp_status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(idp.resp_body)))
                self.end_headers()
                self.wfile.write(idp.resp_body)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        port = self._server.server_address[1]
        self.url = f"http://127.0.0.1:{port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)


@pytest.fixture
def mock_idp() -> Iterator[_MockIdP]:
    """Spawn a mock OIDC IdP for the duration of one test."""
    idp = _MockIdP()
    idp.start()
    try:
        yield idp
    finally:
        idp.stop()


# ---------------------------------------------------------------------------
# Server fixtures (one per implementation)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_http(base_url: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(base_url + "/health", timeout=0.5)
            return
        except Exception:
            time.sleep(0.05)
    raise TimeoutError(f"server at {base_url} did not start within {timeout}s")


def _reject_auth(_req: Any) -> Any:
    raise ValueError("auth required")


@contextmanager
def _python_server(idp_url: str) -> Iterator[str]:
    """Boot the Python reference server in-process via Werkzeug."""
    server = RpcServer(_EchoService, _EchoImpl())
    port = _free_port()
    resource = f"http://127.0.0.1:{port}{PREFIX}"

    app = make_wsgi_app(
        server,
        prefix=PREFIX,
        signing_key=b"test-signing-key-32-bytes-long!!",
        authenticate=_reject_auth,
        oauth_resource_metadata=OAuthResourceMetadata(
            resource=resource,
            authorization_servers=(idp_url,),
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        ),
    )

    httpd = make_server("127.0.0.1", port, app)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        _wait_for_http(f"http://127.0.0.1:{port}{PREFIX}")
        yield f"http://127.0.0.1:{port}{PREFIX}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def _read_port_line(proc: subprocess.Popen[bytes], timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode("utf-8", errors="replace").strip()
        if line.startswith("PORT:"):
            return int(line.split(":", 1)[1])
        if not line and proc.poll() is not None:
            err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
            raise RuntimeError(f"worker exited before printing PORT: {err}")
    raise TimeoutError("timed out waiting for PORT: line from worker")


@contextmanager
def _go_server(idp_url: str) -> Iterator[str]:
    binary = os.environ.get("GO_CONFORMANCE_WORKER")
    if not binary:
        candidate = Path.home() / "Development" / "vgi-rpc-go" / "vgi-rpc-conformance-go"
        binary = str(candidate) if candidate.exists() else ""
    if not binary or not Path(binary).exists():
        pytest.skip("Go conformance worker not available")

    port = _free_port()
    resource = f"http://127.0.0.1:{port}{PREFIX}"
    proc = subprocess.Popen(
        [binary, "--http-pkce", "--port", str(port), "--idp-url", idp_url, "--resource", resource],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        actual_port = _read_port_line(proc)
        assert actual_port == port, f"Go worker bound port {actual_port}, expected {port}"
        base = f"http://127.0.0.1:{actual_port}{PREFIX}"
        _wait_for_http(f"http://127.0.0.1:{actual_port}")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@contextmanager
def _ts_server(idp_url: str) -> Iterator[str]:
    bun = shutil.which("bun")
    if bun is None:
        pytest.skip("bun not available")
    ts_repo = Path.home() / "Development" / "vgi-rpc-typescript"
    script = ts_repo / "examples" / "conformance-http-pkce.ts"
    if not script.exists():
        pytest.skip("TypeScript conformance-http-pkce.ts not available")

    port = _free_port()
    resource = f"http://127.0.0.1:{port}{PREFIX}"
    proc = subprocess.Popen(
        [bun, "run", str(script), "--port", str(port), "--idp-url", idp_url, "--resource", resource],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(ts_repo),
    )
    try:
        actual_port = _read_port_line(proc)
        base = f"http://127.0.0.1:{actual_port}{PREFIX}"
        _wait_for_http(f"http://127.0.0.1:{actual_port}")
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


_IMPLS: list[tuple[str, object]] = [
    ("python", _python_server),
    ("go", _go_server),
    ("ts", _ts_server),
]


# ---------------------------------------------------------------------------
# Conformance assertions
# ---------------------------------------------------------------------------


def _well_known_url(base: str) -> str:
    # base ends with /vgi → metadata path is /.well-known/oauth-protected-resource/vgi
    origin = base.rsplit(PREFIX, 1)[0]
    return f"{origin}/.well-known/oauth-protected-resource{PREFIX}"


@pytest.mark.parametrize("impl_name,server_factory", _IMPLS)
class TestOAuthTokenProxyConformance:
    """Run the same proxy assertions against each implementation."""

    def test_well_known_advertises_token_endpoint(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """Resource metadata advertises {prefix}/_oauth/token as the token_endpoint."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.get(_well_known_url(base))
            assert resp.status_code == 200
            body = resp.json()
            assert body["token_endpoint"] == f"{base}/_oauth/token", (impl_name, body)

    def test_options_preflight(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """OPTIONS returns 204 with CORS headers when Origin is in the allowlist."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.request(
                "OPTIONS",
                f"{base}/_oauth/token",
                headers={"Origin": "https://cupola.query-farm.services"},
            )
            assert resp.status_code == 204, (impl_name, resp.status_code)
            assert resp.headers.get("access-control-allow-origin") == "https://cupola.query-farm.services", impl_name
            assert "POST" in (resp.headers.get("access-control-allow-methods") or ""), impl_name

    def test_authorization_code_forwarded_with_injected_secret(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """authorization_code grant is forwarded with the proxy-injected client_secret."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content="grant_type=authorization_code&code=abc&code_verifier=v&redirect_uri=https://x/cb&client_id=my-client-id",
            )
            assert resp.status_code == 200, (impl_name, resp.status_code, resp.text)
            assert mock_idp.captured.get("client_secret") == CLIENT_SECRET, impl_name
            assert mock_idp.captured.get("client_id") == CLIENT_ID, impl_name
            assert mock_idp.captured.get("code") == "abc", impl_name
            assert mock_idp.captured.get("code_verifier") == "v", impl_name
            assert mock_idp.captured.get("redirect_uri") == "https://x/cb", impl_name

    def test_refresh_token_forwarded_with_injected_secret(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """refresh_token grant is forwarded with the proxy-injected client_secret."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content="grant_type=refresh_token&refresh_token=rtok&client_id=my-client-id&scope=openid",
            )
            assert resp.status_code == 200, (impl_name, resp.status_code)
            assert mock_idp.captured.get("grant_type") == "refresh_token", impl_name
            assert mock_idp.captured.get("refresh_token") == "rtok", impl_name
            assert mock_idp.captured.get("scope") == "openid", impl_name
            assert mock_idp.captured.get("client_secret") == CLIENT_SECRET, impl_name

    def test_mismatched_client_id_rejected(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """Submitting a client_id different from the configured one returns 400 invalid_client."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content="grant_type=authorization_code&client_id=evil&code=x&code_verifier=v&redirect_uri=https://x",
            )
            assert resp.status_code == 400, (impl_name, resp.status_code)
            assert resp.json().get("error") == "invalid_client", impl_name

    def test_unsupported_grant_type_rejected(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """Grant types other than authorization_code/refresh_token are rejected."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content="grant_type=client_credentials",
            )
            assert resp.status_code == 400, (impl_name, resp.status_code)
            assert resp.json().get("error") == "unsupported_grant_type", impl_name

    def test_idp_error_passthrough(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """IdP error response is forwarded verbatim with its status code."""
        mock_idp.resp_status = 400
        mock_idp.resp_body = b'{"error":"invalid_grant","error_description":"bad code"}'
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                content="grant_type=authorization_code&code=bad&code_verifier=v&redirect_uri=https://x",
            )
            assert resp.status_code == 400, (impl_name, resp.status_code)
            body = resp.json()
            assert body.get("error") == "invalid_grant", impl_name

    def test_wrong_content_type_rejected(
        self,
        impl_name: str,
        server_factory: object,
        mock_idp: _MockIdP,
    ) -> None:
        """Non-form Content-Type is rejected with 415."""
        with server_factory(mock_idp.url) as base:  # type: ignore[operator]
            resp = httpx.post(
                f"{base}/_oauth/token",
                headers={"Content-Type": "application/json"},
                content='{"grant_type":"authorization_code"}',
            )
            assert resp.status_code == 415, (impl_name, resp.status_code)
