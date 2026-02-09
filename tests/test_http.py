"""Tests for HTTP-specific behavior in vgi_rpc.http.

Functional correctness (unary, stream, bidi, type fidelity, logs, errors)
is already covered by the ``[http]`` parametrization in ``test_rpc.py``.
This file tests only things unique to the HTTP transport layer.
"""

from __future__ import annotations

from collections.abc import Iterator
from io import BytesIO

import falcon
import pytest
from pyarrow import ipc

from vgi_rpc.http import (
    _ARROW_CONTENT_TYPE,
    _SyncTestClient,
    _SyncTestResponse,
    make_sync_client,
    make_wsgi_app,
)
from vgi_rpc.rpc import RpcError, RpcServer, _dispatch_log_or_error, _drain_stream

from .test_rpc import (
    RpcFixtureService,
    RpcFixtureServiceImpl,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_URL = "http://test"


@pytest.fixture
def client() -> Iterator[_SyncTestClient]:
    """Create a sync Falcon test client with proper cleanup."""
    c = make_sync_client(RpcServer(RpcFixtureService, RpcFixtureServiceImpl()))
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Tests: make_wsgi_app
# ---------------------------------------------------------------------------


class TestMakeWsgiApp:
    """Tests for make_wsgi_app factory."""

    def test_returns_falcon_app(self) -> None:
        """make_wsgi_app returns a Falcon application."""
        server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
        app = make_wsgi_app(server)
        assert isinstance(app, falcon.App)


# ---------------------------------------------------------------------------
# Tests: HTTP-specific error cases
# ---------------------------------------------------------------------------


def _extract_rpc_error(resp: _SyncTestResponse) -> RpcError:
    """Parse an Arrow IPC error stream from a response body, return the RpcError."""
    reader = ipc.open_stream(BytesIO(resp.content))
    try:
        while True:
            batch, cm = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, cm)
    except RpcError as exc:
        _drain_stream(reader)
        return exc
    except StopIteration:
        pass
    raise AssertionError("Response body did not contain an RpcError")


class TestHttpErrorCases:
    """Tests for HTTP-specific error cases (status codes + Arrow IPC error bodies)."""

    def test_unknown_method_404(self, client: _SyncTestClient) -> None:
        """Unknown method returns 404 with a parseable Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/nonexistent",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 404
        err = _extract_rpc_error(resp)
        assert err.error_type == "AttributeError"
        assert "nonexistent" in err.error_message

    def test_non_bidi_on_bidi_endpoint_400(self, client: _SyncTestClient) -> None:
        """Non-bidi method on the /bidi endpoint returns 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add/bidi",
            content=b"",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "not a bidi stream" in err.error_message

    def test_malformed_body_bidi_init_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a bidi init endpoint return 400 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/bidi",
            content=b"garbage bytes",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "ArrowInvalid" in err.error_type

    def test_malformed_body_bidi_exchange_400(self, client: _SyncTestClient) -> None:
        """Garbage bytes on a bidi exchange endpoint return 400."""
        resp = client.post(
            f"{_BASE_URL}/vgi/transform/exchange",
            content=b"garbage bytes",
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        assert resp.status_code == 400
        err = _extract_rpc_error(resp)
        assert "ArrowInvalid" in err.error_type

    def test_wrong_content_type_415(self, client: _SyncTestClient) -> None:
        """Wrong Content-Type returns 415 with Arrow IPC error."""
        resp = client.post(
            f"{_BASE_URL}/vgi/add",
            content=b"hello",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 415
        err = _extract_rpc_error(resp)
        assert err.error_type == "TypeError"
        assert "Content-Type" in err.error_message
