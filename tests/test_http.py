"""Tests for HTTP-specific behavior in vgi_rpc.http.

Functional correctness (unary, stream, bidi, type fidelity, logs, errors)
is already covered by the ``[http]`` parametrization in ``test_rpc.py``.
This file tests only things unique to the HTTP transport layer.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from vgi_rpc.http import (
    _SyncASGIClient,
    http_connect,
    make_asgi_app,
    make_sync_client,
)
from vgi_rpc.rpc import RpcServer

from .test_rpc import (
    RpcFixtureService,
    RpcFixtureServiceImpl,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_URL = "http://test"


@pytest.fixture
def client() -> Iterator[_SyncASGIClient]:
    """Create a sync ASGI test client with proper cleanup."""
    c = make_sync_client(RpcServer(RpcFixtureService, RpcFixtureServiceImpl()), base_url=_BASE_URL)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Tests: make_asgi_app
# ---------------------------------------------------------------------------


class TestMakeAsgiApp:
    """Tests for make_asgi_app factory."""

    def test_returns_starlette_app(self) -> None:
        """make_asgi_app returns a Starlette application."""
        from starlette.applications import Starlette

        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_asgi_app(server)
        assert isinstance(app, Starlette)


# ---------------------------------------------------------------------------
# Tests: HTTP-specific error cases
# ---------------------------------------------------------------------------


class TestHttpErrorCases:
    """Tests for HTTP-specific error cases (raw status codes)."""

    def test_unknown_method_404(self, client: _SyncASGIClient) -> None:
        """Unknown method returns 404."""
        resp = client.post(
            f"{_BASE_URL}/vgi/nonexistent",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 404

    def test_non_bidi_on_bidi_endpoint_400(self, client: _SyncASGIClient) -> None:
        """Non-bidi method on the /bidi endpoint returns 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/bidi",
            content=b"",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 400

    def test_malformed_body_bidi_init_400(self, client: _SyncASGIClient) -> None:
        """Garbage bytes on a bidi init endpoint return 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/bidi",
            content=b"garbage bytes",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 400

    def test_malformed_body_bidi_exchange_400(self, client: _SyncASGIClient) -> None:
        """Garbage bytes on a bidi exchange endpoint return 400 (after valid init)."""
        # Initialize a valid bidi session so the exchange path reaches ipc.open_stream
        with http_connect(RpcFixtureService, _BASE_URL, client=client) as svc:
            svc.transform(factor=1.0)

        # Now send garbage to the exchange endpoint
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/exchange",
            content=b"garbage bytes",
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert resp.status_code == 400
