#!/usr/bin/env python3
"""Cross-implementation HTML page tests for vgi-rpc.

Compares the HTML landing page and describe page between Python and Go
implementations. TypeScript has no HTML pages (documented as a gap).

Run: uv run pytest test_html_pages.py -v
"""

from __future__ import annotations

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
        if candidate.startswith(
            ("echo_", "void_", "add_", "concatenate", "with_defaults", "raise_", "produce_", "exchange_", "inspect_")
        ):
            names.add(candidate)
    return names


def extract_badges(html: str) -> set[str]:
    """Extract method type badges from a describe page HTML (case-insensitive)."""
    badges: set[str] = set()
    for match in re.finditer(r"badge[^>]*>([a-zA-Z]+)<", html):
        badge = match.group(1).lower()
        if badge in ("unary", "stream", "producer", "exchange", "header"):
            badges.add(badge)
    return badges


def extract_method_badges(html: str) -> dict[str, list[str]]:
    """Extract per-method badge lists from a describe page HTML."""
    cards: dict[str, list[str]] = {}
    for m in re.finditer(r'class="method-name">([^<]+)</span>(.*?)</div>', html, re.DOTALL):
        method = m.group(1).strip()
        badges = sorted(b.lower() for b in re.findall(r"badge[^>]*>([^<]+)<", m.group(2)))
        cards[method] = badges
    return cards


def extract_table_columns(html: str) -> list[str]:
    """Extract unique table column headers from HTML."""
    headers = re.findall(r"<th>([^<]+)</th>", html)
    return list(dict.fromkeys(headers))


def extract_params_for_method(html: str, method_name: str) -> list[str] | None:
    """Extract parameter table cells for a specific method."""
    idx = html.find(f">{method_name}<")
    if idx < 0:
        return None
    table_start = html.find("<table>", idx)
    table_end = html.find("</table>", table_start)
    if table_start < 0 or table_end < 0:
        return None
    table = html[table_start:table_end]
    rows = re.findall(r"<td[^>]*>(.*?)</td>", table, re.DOTALL)
    return [r.strip() for r in rows]


# ---------------------------------------------------------------------------
# Python HTML pages (in-process via Falcon test client)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def python_client():
    """Create a Falcon test client for the Python conformance server."""
    try:
        import falcon.testing
        from vgi_rpc.http._server import make_wsgi_app

        from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
        from vgi_rpc.rpc import RpcServer
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
# TypeScript HTML pages (subprocess HTTP server)
# ---------------------------------------------------------------------------


def _start_ts_http() -> tuple[subprocess.Popen, int] | None:
    """Start the TypeScript conformance worker in HTTP mode. Returns (process, port) or None."""
    proc = subprocess.Popen(
        ["bun", "run", "examples/conformance-http.ts"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=TS_REPO,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    if not line.startswith("PORT:"):
        proc.terminate()
        return None
    port = int(line.split(":", 1)[1])

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)

    return proc, port


@pytest.fixture(scope="module")
def ts_http():
    """Start TypeScript HTTP server, yield base_url, stop on teardown."""
    result = _start_ts_http()
    if result is None:
        pytest.skip("TypeScript conformance worker not available")
    proc, port = result
    yield f"http://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(timeout=5)


class TestTsLandingPage:
    def test_status_200(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/", timeout=5)
        assert r.status_code == 200

    def test_content_type_html(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/", timeout=5)
        assert "text/html" in r.headers.get("content-type", "")

    def test_contains_vgi_rpc(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/", timeout=5)
        assert "vgi-rpc" in r.text.lower() or "vgi_rpc" in r.text.lower()

    def test_contains_logo(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/", timeout=5)
        assert "logo" in r.text.lower() or ".png" in r.text


class TestTsDescribePage:
    def test_status_200(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/describe", timeout=5)
        assert r.status_code == 200

    def test_content_type_html(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/describe", timeout=5)
        assert "text/html" in r.headers.get("content-type", "")

    def test_contains_method_names(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/describe", timeout=5)
        methods = extract_method_names(r.text)
        assert "echo_string" in methods
        assert "echo_int" in methods

    def test_contains_badges(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/describe", timeout=5)
        badges = extract_badges(r.text)
        assert "unary" in badges


class TestTs404Page:
    def test_status_404(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/nonexistent", timeout=5)
        assert r.status_code == 404

    def test_content_type_html(self, ts_http: str) -> None:
        r = httpx.get(f"{ts_http}/nonexistent", timeout=5)
        assert "text/html" in r.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# Cross-implementation comparison (all three)
# ---------------------------------------------------------------------------


class TestCrossImplAllDescribe:
    def test_all_three_same_methods(self, python_client, go_http: str, ts_http: str) -> None:
        """Python, Go, and TypeScript describe pages list the same methods."""
        py_methods = extract_method_names(python_client.simulate_get("/describe").text)
        go_methods = extract_method_names(httpx.get(f"{go_http}/describe", timeout=5).text)
        ts_methods = extract_method_names(httpx.get(f"{ts_http}/describe", timeout=5).text)

        assert len(py_methods) > 10
        assert len(go_methods) > 10
        assert len(ts_methods) > 10

        all_methods = py_methods | go_methods | ts_methods
        for lang, methods in [("python", py_methods), ("go", go_methods), ("typescript", ts_methods)]:
            missing = all_methods - methods
            assert not missing, f"{lang} missing methods: {missing}"

    def test_all_three_have_badges(self, python_client, go_http: str, ts_http: str) -> None:
        """All three implementations use unary and stream badges."""
        for label, html in [
            ("python", python_client.simulate_get("/describe").text),
            ("go", httpx.get(f"{go_http}/describe", timeout=5).text),
            ("typescript", httpx.get(f"{ts_http}/describe", timeout=5).text),
        ]:
            badges = extract_badges(html)
            assert "unary" in badges, f"{label} missing 'unary' badge"
            assert "stream" in badges, f"{label} missing 'stream' badge"


class TestContentParity:
    """Deep content comparison against the Python reference implementation."""

    def test_ts_badges_match_python(self, python_client, ts_http: str) -> None:
        """TypeScript per-method badges should match Python exactly."""
        py_badges = extract_method_badges(python_client.simulate_get("/describe").text)
        ts_badges = extract_method_badges(httpx.get(f"{ts_http}/describe", timeout=5).text)

        common = set(py_badges) & set(ts_badges)
        assert len(common) > 40, f"Only {len(common)} methods in common"

        diffs = []
        for m in sorted(common):
            if py_badges[m] != ts_badges[m]:
                diffs.append(f"  {m}: python={py_badges[m]} ts={ts_badges[m]}")
        assert not diffs, "Badge differences:\n" + "\n".join(diffs)

    def test_ts_columns_match_python(self, python_client, ts_http: str) -> None:
        """TypeScript table columns should match Python (Name, Type, Default, Description)."""
        py_cols = extract_table_columns(python_client.simulate_get("/describe").text)
        ts_cols = extract_table_columns(httpx.get(f"{ts_http}/describe", timeout=5).text)
        assert py_cols == ts_cols, f"Column mismatch: python={py_cols} ts={ts_cols}"

    def test_ts_params_echo_string(self, python_client, ts_http: str) -> None:
        """TypeScript echo_string params should match Python."""
        py_p = extract_params_for_method(python_client.simulate_get("/describe").text, "echo_string")
        ts_p = extract_params_for_method(httpx.get(f"{ts_http}/describe", timeout=5).text, "echo_string")
        assert py_p == ts_p, f"echo_string params differ:\n  python={py_p}\n  ts={ts_p}"

    def test_ts_params_add_floats(self, python_client, ts_http: str) -> None:
        """TypeScript add_floats params should match Python (float types)."""
        py_p = extract_params_for_method(python_client.simulate_get("/describe").text, "add_floats")
        ts_p = extract_params_for_method(httpx.get(f"{ts_http}/describe", timeout=5).text, "add_floats")
        assert py_p == ts_p, f"add_floats params differ:\n  python={py_p}\n  ts={ts_p}"

    def test_go_badges_match_python(self, python_client, go_http: str) -> None:
        """Go per-method badges should match Python exactly."""
        py_badges = extract_method_badges(python_client.simulate_get("/describe").text)
        go_badges = extract_method_badges(httpx.get(f"{go_http}/describe", timeout=5).text)

        common = set(py_badges) & set(go_badges)
        diffs = []
        for m in sorted(common):
            if py_badges[m] != go_badges[m]:
                diffs.append(f"  {m}: python={py_badges[m]} go={go_badges[m]}")
        assert not diffs, "Badge differences:\n" + "\n".join(diffs)

    def test_go_columns_match_python(self, python_client, go_http: str) -> None:
        """Go table columns should match Python (Name, Type, Default, Description)."""
        py_cols = extract_table_columns(python_client.simulate_get("/describe").text)
        go_cols = extract_table_columns(httpx.get(f"{go_http}/describe", timeout=5).text)
        assert py_cols == go_cols, f"Column mismatch: python={py_cols} go={go_cols}"
