#!/usr/bin/env python3
"""Cross-implementation HTML page tests for vgi-rpc.

Compares the HTML landing page and describe page between Python and Go
implementations. TypeScript has no HTML pages (documented as a gap).

Run: uv run pytest test_html_pages.py -v
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

import httpx
import pytest

REPOS_DIR = Path.home() / "Development"
PYTHON_REPO = REPOS_DIR / "vgi-rpc"
GO_REPO = REPOS_DIR / "vgi-rpc-go"
TS_REPO = REPOS_DIR / "vgi-rpc-typescript"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_method_names(html: str) -> set[str]:
    """Extract RPC method names from a describe page HTML."""
    # Both Python and Go render method names in elements with identifiable patterns.
    # Look for method names that match the conformance service pattern.
    # Common patterns: id="method-{name}", or the method name appears in headings/cards.
    names: set[str] = set()
    # Pattern: method names appear as text in h3/h2 tags or method-card divs
    for match in re.finditer(r'["\s>]([a-z_]+(?:_[a-z]+)*)[<"\s]', html):
        candidate = match.group(1)
        # Filter to known conformance method patterns
        if candidate.startswith(("echo_", "void_", "add_", "concatenate", "with_defaults",
                                  "raise_", "produce_", "exchange_", "inspect_")):
            names.add(candidate)
    return names


def extract_badges(html: str) -> set[str]:
    """Extract method type badges from a describe page HTML (case-insensitive)."""
    badges: set[str] = set()
    for match in re.finditer(r'badge[^>]*>([a-zA-Z]+)<', html):
        badge = match.group(1).lower()
        if badge in ("unary", "stream", "producer", "exchange", "header"):
            badges.add(badge)
    return badges


# ---------------------------------------------------------------------------
# Python HTML pages (in-process via Falcon test client)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def python_client():
    """Create a Falcon test client for the Python conformance server."""
    try:
        from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
        from vgi_rpc.rpc import RpcServer
        from vgi_rpc.http._server import make_wsgi_app
        import falcon.testing
    except ImportError:
        pytest.skip("vgi-rpc[http] not installed")

    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    app = make_wsgi_app(server)
    return falcon.testing.TestClient(app)


class TestPythonLandingPage:
    def test_status_200(self, python_client) -> None:
        result = python_client.simulate_get("/")
        assert result.status_code == 200

    def test_content_type_html(self, python_client) -> None:
        result = python_client.simulate_get("/")
        assert "text/html" in result.headers.get("content-type", "")

    def test_contains_vgi_rpc(self, python_client) -> None:
        result = python_client.simulate_get("/")
        assert "vgi-rpc" in result.text.lower() or "vgi_rpc" in result.text.lower()

    def test_contains_logo(self, python_client) -> None:
        result = python_client.simulate_get("/")
        assert "logo" in result.text.lower() or ".png" in result.text or ".svg" in result.text


class TestPythonDescribePage:
    def test_status_200(self, python_client) -> None:
        result = python_client.simulate_get("/describe")
        assert result.status_code == 200

    def test_content_type_html(self, python_client) -> None:
        result = python_client.simulate_get("/describe")
        assert "text/html" in result.headers.get("content-type", "")

    def test_contains_method_names(self, python_client) -> None:
        result = python_client.simulate_get("/describe")
        methods = extract_method_names(result.text)
        assert "echo_string" in methods
        assert "echo_int" in methods

    def test_contains_badges(self, python_client) -> None:
        result = python_client.simulate_get("/describe")
        badges = extract_badges(result.text)
        assert "unary" in badges


class TestPython404Page:
    def test_status_not_found(self, python_client) -> None:
        result = python_client.simulate_get("/nonexistent")
        # Falcon returns 404 (with custom page) or 405 (default for unmatched methods)
        assert result.status_code in (404, 405)


# ---------------------------------------------------------------------------
# Go HTML pages (subprocess HTTP server)
# ---------------------------------------------------------------------------


def _start_go_http() -> tuple[subprocess.Popen, int] | None:
    """Start the Go conformance worker in HTTP mode. Returns (process, port) or None."""
    worker = GO_REPO / "conformance-worker"
    if not worker.exists():
        # Try building
        result = subprocess.run(
            ["go", "build", "-o", "conformance-worker", "./conformance/cmd/vgi-rpc-conformance-go"],
            cwd=GO_REPO,
            capture_output=True,
        )
        if result.returncode != 0:
            return None

    proc = subprocess.Popen(
        [str(worker), "--http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=GO_REPO,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    if not line.startswith("PORT:"):
        proc.terminate()
        return None
    port = int(line.split(":", 1)[1])

    # Wait for HTTP readiness
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)

    return proc, port


@pytest.fixture(scope="module")
def go_http():
    """Start Go HTTP server, yield (base_url), stop on teardown."""
    result = _start_go_http()
    if result is None:
        pytest.skip("Go conformance worker not available")
    proc, port = result
    yield f"http://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(timeout=5)


class TestGoLandingPage:
    def test_status_200(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/", timeout=5)
        assert r.status_code == 200

    def test_content_type_html(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/", timeout=5)
        assert "text/html" in r.headers.get("content-type", "")

    def test_contains_vgi_rpc(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/", timeout=5)
        assert "vgi-rpc" in r.text.lower() or "vgi_rpc" in r.text.lower()


class TestGoDescribePage:
    def test_status_200(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/describe", timeout=5)
        assert r.status_code == 200

    def test_content_type_html(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/describe", timeout=5)
        assert "text/html" in r.headers.get("content-type", "")

    def test_contains_method_names(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/describe", timeout=5)
        methods = extract_method_names(r.text)
        assert "echo_string" in methods
        assert "echo_int" in methods


class TestGo404Page:
    def test_status_404(self, go_http: str) -> None:
        r = httpx.get(f"{go_http}/nonexistent", timeout=5)
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Cross-implementation comparison
# ---------------------------------------------------------------------------


class TestCrossImplDescribe:
    def test_same_methods(self, python_client, go_http: str) -> None:
        """Python and Go describe pages list the same methods."""
        py_html = python_client.simulate_get("/describe").text
        go_html = httpx.get(f"{go_http}/describe", timeout=5).text

        py_methods = extract_method_names(py_html)
        go_methods = extract_method_names(go_html)

        # Both should find a substantial number of methods
        assert len(py_methods) > 10, f"Too few Python methods found: {py_methods}"
        assert len(go_methods) > 10, f"Too few Go methods found: {go_methods}"

        # Methods should match
        missing_in_go = py_methods - go_methods
        missing_in_python = go_methods - py_methods
        assert not missing_in_go, f"Methods in Python but not Go: {missing_in_go}"
        assert not missing_in_python, f"Methods in Go but not Python: {missing_in_python}"

    def test_common_badges(self, python_client, go_http: str) -> None:
        """Python and Go both use unary and stream badges."""
        py_html = python_client.simulate_get("/describe").text
        go_html = httpx.get(f"{go_http}/describe", timeout=5).text

        py_badges = extract_badges(py_html)
        go_badges = extract_badges(go_html)

        # Both should have at least unary and stream
        assert "unary" in py_badges, f"Python missing 'unary' badge: {py_badges}"
        assert "unary" in go_badges, f"Go missing 'unary' badge: {go_badges}"
        assert "stream" in py_badges, f"Python missing 'stream' badge: {py_badges}"
        assert "stream" in go_badges, f"Go missing 'stream' badge: {go_badges}"
        # Note: Go also uses producer/exchange badges — more specific than Python's generic 'stream'


# ---------------------------------------------------------------------------
# TypeScript gap
# ---------------------------------------------------------------------------


class TestTypeScriptHtmlGap:
    def test_no_html_pages(self) -> None:
        """TypeScript implementation has no HTML pages — document this gap."""
        pytest.skip("TypeScript has no HTML pages (feature gap)")
