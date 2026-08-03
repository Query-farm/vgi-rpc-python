# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Python-only conformance tests for HTTP sticky sessions.

Tests Python-implementation-specific guarantees that have no business
running cross-language: client-side leak prevention, multi-session
multiplexing on one connection, ``with_session_token()`` exit semantics
(best-effort DELETE), Python ``state.close()`` invocation on eviction,
and the pipe-transport guard. The wire-protocol contract is covered by
the canonical :class:`vgi_rpc.conformance.TestSticky` group.
"""

from __future__ import annotations

import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol, cast

import falcon
import pytest

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance._types import _StickyCounter
from vgi_rpc.http import http_capabilities, http_connect, make_wsgi_app
from vgi_rpc.http._common import SESSION_ACCEPT_HEADER, SESSION_HEADER
from vgi_rpc.http._testing import _SyncTestClient, make_sync_client
from vgi_rpc.http.server._sticky import (
    _ReaperThread,
    _SessionRegistry,
    _StickyMiddleware,
)
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    RpcError,
    RpcServer,
    SessionLostError,
    serve_pipe,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

# Explicit key so the principal-binding tests can share one app across two
# clients (and two apps in the wrong-worker test) without the framework
# minting a fresh per-process key underneath them.
_TOKEN_KEY = b"sticky-test-key-32-bytes-long!!!"

# Silence the per-process random token_key warning the framework emits when
# a test omits an explicit key — tests don't run multi-process so the warning
# is noise.
warnings.filterwarnings("ignore", message="No token_key provided")


@pytest.fixture
def sticky_client() -> Iterator[_SyncTestClient]:
    """Yield a sticky-enabled in-process sync test client.

    Fresh per test (function-scoped) so registry state never leaks across
    tests. Short TTL keeps the reaper test fast.
    """
    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    client = make_sync_client(server, enable_sticky=True, sticky_default_ttl=1.0)
    try:
        yield client
    finally:
        client.close()


def _connect(client: _SyncTestClient) -> Any:
    """Open a typed proxy as ``Any`` so mypy doesn't complain about ``proxy.with_session_token``.

    The Protocol class doesn't declare ``with_session_token``; that's a
    method on the concrete ``_HttpProxy`` returned by ``http_connect``.
    Tests need attribute access through the Protocol typing, which is
    structurally a hole — narrowing to ``Any`` here keeps the type
    system honest about the mismatch.
    """
    return http_connect(ConformanceService, client=client)


class TestPythonClientGuards:
    """Client-side leak-prevention and opt-in behaviour."""

    def test_no_session_headers_outside_with_block(self, sticky_client: _SyncTestClient) -> None:
        """A bare proxy call must never send ``VGI-Session-Accept`` or ``VGI-Session``."""
        # Probe via the test client directly so we can read the request headers
        # Falcon saw. Sticky middleware records nothing — we just verify that
        # ctx.open_session raises server-side because the opt-in header is missing.
        with _connect(sticky_client) as proxy:
            # Calling open_counter outside any with_session_token() block
            # MUST raise — proves the server rejects open_session when the
            # client didn't opt in.
            with pytest.raises(RpcError) as excinfo:
                proxy.open_counter(initial=1)
            assert "VGI-Session-Accept" in str(excinfo.value)

    def test_with_block_sends_accept_header(self, sticky_client: _SyncTestClient) -> None:
        """Inside ``with_session_token()`` every request carries the opt-in header."""
        # Open succeeds → header was sent.
        with _connect(sticky_client) as proxy, proxy.with_session_token() as sess:
            assert sess.open_counter(initial=5) == 5

    def test_multiple_concurrent_sessions_on_same_connection(self, sticky_client: _SyncTestClient) -> None:
        """Two ``with_session_token()`` blocks on one connection multiplex without conflict.

        The header-only transport ensures each block carries its own token
        per-call — no shared cookie jar, no cross-contamination. This is the
        reason for choosing header transport over cookies.
        """
        with (
            _connect(sticky_client) as proxy,
            proxy.with_session_token() as sess_a,
            proxy.with_session_token() as sess_b,
        ):
            a_init = sess_a.open_counter(initial=100)
            b_init = sess_b.open_counter(initial=200)
            assert a_init == 100
            assert b_init == 200
            # Tokens must be distinct.
            assert sess_a.current_session_token() != sess_b.current_session_token()
            # Mutations must not bleed across sessions.
            assert sess_a.increment_counter(by=1) == 101
            assert sess_b.increment_counter(by=2) == 202
            assert sess_a.increment_counter(by=10) == 111
            assert sess_b.increment_counter(by=20) == 222

    def test_token_persistence_across_with_blocks(self, sticky_client: _SyncTestClient) -> None:
        """A detached token resumes the same session in a fresh ``with_session_token()`` block.

        The default ``with`` exit fires a best-effort DELETE — which
        would defeat stashing. :meth:`_SessionView.detach` suppresses
        the exit-time DELETE so the registry entry survives the block.
        """
        with _connect(sticky_client) as proxy:
            stash: str | None = None
            with proxy.with_session_token() as sess1:
                sess1.open_counter(initial=42)
                sess1.increment_counter(by=8)  # → 50
                # Detach: hand the token off, suppress exit DELETE.
                stash = sess1.detach()
            assert stash is not None
            # Re-enter with the stashed token — server still has the entry.
            with proxy.with_session_token(token=stash) as sess2:
                # The stashed session counter is at 50; increment by 7 → 57.
                assert sess2.increment_counter(by=7) == 57


class TestWithBlockExit:
    """Best-effort ``DELETE /vgi/__session__`` semantics on ``with`` exit."""

    def test_exit_fires_delete_when_live(self, sticky_client: _SyncTestClient) -> None:
        """Leaving a block with a live session evicts via DELETE; resumption fails."""
        with _connect(sticky_client) as proxy:
            stash: str | None = None
            with proxy.with_session_token() as sess:
                sess.open_counter(initial=1)
                stash = sess.current_session_token()
            assert stash is not None
            # Re-presenting the token must fail — the DELETE on exit
            # evicted the registry entry.
            with (
                proxy.with_session_token(token=stash) as sess2,
                pytest.raises(RpcError) as excinfo,
            ):
                sess2.increment_counter(by=1)
            assert excinfo.value.error_type == "SessionLostError"

    def test_exit_skips_delete_when_already_closed(self, sticky_client: _SyncTestClient) -> None:
        """When the method already called ``close_session``, exit does NOT re-fire DELETE."""
        # Verifies the view honours VGI-Session-Close: true by clearing its
        # token; with no token the exit-time DELETE shortcut takes the
        # "nothing to do" branch.
        with _connect(sticky_client) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            sess.close_counter()
            assert sess.current_session_token() is None  # cleared by VGI-Session-Close
        # No assertion failure means the exit path did not error out
        # trying to delete a non-existent session.


class TestEvictionAndShutdown:
    """Python ``state.close()`` invocation: TTL reaper + WSGI app teardown."""

    def test_close_called_on_ttl_eviction(self) -> None:
        """Reaper invokes ``state.close()`` when a session ages out, even with no client involvement.

        Builds the server in-process so we have a handle on the state
        object and can assert ``closed`` flips after TTL + reaper tick.
        """
        from vgi_rpc.http.server._sticky import _SessionRegistry

        registry = _SessionRegistry(default_ttl=0.05)
        counter = _StickyCounter(value=1)
        registry.open(counter, ttl=0.05, principal_key="anon")
        assert counter.closed is False
        # Sleep past TTL, then trigger the reaper's expiry sweep directly
        # (no need to wait for the daemon thread's 1s tick — drain_expired
        # is the unit under test here).
        time.sleep(0.1)
        evicted = registry.drain_expired()
        assert evicted == 1, "exactly one entry should have expired"
        assert counter.closed is True, "TTL reaper must invoke state.close()"

    def test_close_called_on_registry_shutdown(self) -> None:
        """Registry shutdown invokes ``state.close()`` on every live session.

        Exercises the WSGI-app-teardown contract: when the app is being
        retired (drain → shutdown), all live sessions get their handles
        released even if their TTLs would otherwise extend further.
        """
        from vgi_rpc.http.server._sticky import _SessionRegistry

        registry = _SessionRegistry(default_ttl=60.0)
        counters = [_StickyCounter(value=i) for i in range(3)]
        for c in counters:
            registry.open(c, ttl=None, principal_key="anon")
        registry.shutdown()
        assert all(c.closed for c in counters), "shutdown must close every live session"

    def test_close_called_on_explicit_close(self, sticky_client: _SyncTestClient) -> None:
        """``ctx.close_session`` invokes ``state.close()`` on the bound counter.

        Verified end-to-end through the RPC stack: client opens via
        ``open_counter``, calls ``close_counter`` which internally calls
        ``ctx.close_session``; the registry's eviction path invokes
        ``_StickyCounter.close()``. We assert by opening a second time
        with the same token, which must fail because the entry is gone.
        """
        with _connect(sticky_client) as proxy:
            stash: str | None = None
            with proxy.with_session_token() as sess:
                sess.open_counter(initial=1)
                stash = sess.current_session_token()
                sess.close_counter()
            assert stash is not None
            with (
                proxy.with_session_token(token=stash) as sess2,
                pytest.raises(RpcError) as excinfo,
            ):
                sess2.increment_counter(by=1)
            assert excinfo.value.error_type == "SessionLostError"


class TestTransportGuards:
    """Sticky session API surface guards on non-HTTP transports."""

    def test_open_session_on_pipe_transport_raises(self) -> None:
        """``ctx.open_session`` raises ``RuntimeError`` on the pipe transport.

        Sticky machinery is HTTP-only — the contextvar that brokers
        ``CallContext.open_session`` to the registry is set by
        ``_StickyMiddleware``, which is only installed on the HTTP WSGI
        app. Any other transport sees an absent sentinel and raises
        cleanly so callers know to detect-and-fall-back.
        """

        class _PipeStickyService:
            """In-process service exposing one method that tries to open a session."""

            def try_open(self, value: int, ctx: CallContext) -> int:
                # Should raise — no sticky machinery on pipe transport.
                ctx.open_session(_StickyCounter(value=value))
                return value

        class _PipeStickyProtocol(Protocol):
            def try_open(self, value: int) -> int:
                """Try to open a sticky session over a pipe transport — expected to raise."""
                ...

        with serve_pipe(_PipeStickyProtocol, _PipeStickyService()) as proxy:
            with pytest.raises(RpcError) as excinfo:
                proxy.try_open(value=1)
            # Error type is RuntimeError (from CallContext.open_session) —
            # the exact message names the transport guard.
            assert excinfo.value.error_type == "RuntimeError"
            assert "sticky sessions not available" in str(excinfo.value)


class TestConcurrentSessions:
    """Per-session RLock contract: same-session calls serialize, different-session calls parallel."""

    def test_concurrent_same_session_no_state_corruption(self, sticky_client: _SyncTestClient) -> None:
        """Two parallel ``increment_counter(by=1)`` calls on one session produce a final value of N.

        With the per-session RLock, the framework serializes them; both
        increments observe the latest state. We don't assert lock-hold
        *ordering* (which would be flaky); only that the final state is
        consistent.
        """
        with _connect(sticky_client) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=0)
            # 10 parallel increments by 1 each → final must be 10
            n = 10
            with ThreadPoolExecutor(max_workers=n) as pool:
                futures = [pool.submit(sess.increment_counter, by=1) for _ in range(n)]
                results = [f.result(timeout=10) for f in futures]
            # Every per-call return value must be unique (we hold the RLock
            # so consecutive increments produce strictly increasing values).
            assert sorted(results) == list(range(1, n + 1)), (
                f"per-call values must be 1..{n} with no collisions, got {sorted(results)}"
            )
            # Final state is consistent.
            assert sess.close_counter() == n


class TestSessionLostErrorClass:
    """Direct unit-test coverage of the new typed errors."""

    def test_session_lost_error_kind(self) -> None:
        """``SessionLostError.error_kind`` is the wire-stable string."""
        assert SessionLostError.error_kind == "session_lost"
        # Instance-level access works too — important for the wire serializer.
        exc = SessionLostError("boom")
        assert exc.error_kind == "session_lost"

    def test_capabilities_sticky_fields_populated(self, sticky_client: _SyncTestClient) -> None:
        """``http_capabilities()`` surfaces ``sticky_enabled`` and ``sticky_default_ttl``."""
        caps = http_capabilities(client=sticky_client)
        assert caps.sticky_enabled is True
        assert caps.sticky_default_ttl == 1, (
            "sticky_default_ttl in the fixture is 1.0 — must round-trip via the int-typed header"
        )


class TestNonStickyServerUnaffected:
    """Regression guard: sticky-disabled server stays byte-identical."""

    def test_sticky_disabled_does_not_advertise_capabilities(self) -> None:
        """A server constructed without ``enable_sticky=True`` does not advertise sticky headers."""
        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        client = make_sync_client(server)  # default: enable_sticky=False
        try:
            caps = http_capabilities(client=client)
            assert caps.sticky_enabled is False
            assert caps.sticky_default_ttl is None
        finally:
            client.close()

    def test_sticky_disabled_does_not_expose_delete_endpoint(self) -> None:
        """``DELETE /vgi/__session__`` does NOT route to the sticky resource when sticky is off.

        The non-sticky server doesn't register the ``_SessionResource``
        route, so the unary RPC route ``POST /{method}`` matches
        ``/__session__`` as ``method=__session__`` for any method that
        existed — but since the method ``__session__`` doesn't exist
        either, the request lands on the unary handler with method-not-
        implemented, OR Falcon returns 405 because the route only allows
        POST. Either way, the 200-with-idempotent-no-op the sticky
        resource would produce is not what's returned.
        """
        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        client = make_sync_client(server)  # default: enable_sticky=False
        try:
            resp = client.delete("/__session__")
            assert resp.status_code != 200, (
                "without enable_sticky the DELETE endpoint must not idempotently succeed — "
                "that would falsely advertise sticky support to clients"
            )
            # 404 (no route), 405 (route exists but DELETE not allowed) and
            # 415 (Content-Type) are all acceptable "not the sticky resource"
            # responses; the test cares about the negative — not 200.
            assert resp.status_code in (404, 405, 415), (
                f"unexpected non-200 status for non-sticky DELETE: {resp.status_code}"
            )
        finally:
            client.close()


# Make sure the constants we import here actually exist (regression: a refactor
# could rename them and only show up in deep tests).
def test_session_header_constants_exist() -> None:
    """Sanity: the session-header constants exported from ``vgi_rpc.http._common`` are stable strings."""
    assert SESSION_HEADER == "VGI-Session"
    assert SESSION_ACCEPT_HEADER == "VGI-Session-Accept"


# ---------------------------------------------------------------------------
# Echo headers (PR2)
# ---------------------------------------------------------------------------


@pytest.fixture
def echo_sticky_client() -> Iterator[_SyncTestClient]:
    """Sticky-enabled client configured with a marker echo header.

    Used for tests that exercise the server emission + client capture/
    replay round-trip end-to-end. The marker name is intentionally
    multi-token so case-insensitivity bugs surface.
    """
    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    client = make_sync_client(
        server,
        enable_sticky=True,
        sticky_default_ttl=10.0,
        sticky_echo_headers={"x-test-echo-marker": "captured-by-client"},
    )
    try:
        yield client
    finally:
        client.close()


class TestEchoHeadersServer:
    """Server-side echo-header emission contract."""

    def test_emitted_on_session_open(self, echo_sticky_client: _SyncTestClient) -> None:
        """Server emits ``VGI-Echo-<name>: <value>`` on the session-opening response."""
        from vgi_rpc.http._common import ECHO_HEADER_PREFIX

        seen_echo_headers: list[dict[str, str]] = []
        # Patch the capture callback so we can inspect what the inner test
        # client returned BEFORE the view consumes the response.
        from vgi_rpc.http import _client as _hc

        orig_capture = _hc._SessionTrackingClient._capture

        def patched(self: _hc._SessionTrackingClient, resp: Any) -> None:
            seen_echo_headers.append(
                {
                    k[len(ECHO_HEADER_PREFIX) :]: v
                    for k, v in resp.headers.items()
                    if k.lower().startswith(ECHO_HEADER_PREFIX.lower())
                }
            )
            orig_capture(self, resp)

        _hc._SessionTrackingClient._capture = patched  # type: ignore[method-assign]
        try:
            with _connect(echo_sticky_client) as proxy, proxy.with_session_token() as sess:
                sess.open_counter(initial=1)
                sess.increment_counter(by=1)  # follow-up response should NOT carry echo
        finally:
            _hc._SessionTrackingClient._capture = orig_capture  # type: ignore[method-assign]
        # First captured response is the open: must have the echo header.
        assert seen_echo_headers[0] == {"x-test-echo-marker": "captured-by-client"}, (
            f"open response must carry VGI-Echo-x-test-echo-marker; saw {seen_echo_headers[0]!r}"
        )
        # Subsequent responses do NOT carry the echo header (once-only emission).
        assert seen_echo_headers[1] == {}, (
            f"subsequent responses must NOT carry VGI-Echo-* (echo is once-only); saw {seen_echo_headers[1]!r}"
        )

    def test_absent_when_unconfigured(self, sticky_client: _SyncTestClient) -> None:
        """Server with no ``sticky_echo_headers`` emits no ``VGI-Echo-*`` headers and no capability advert."""
        caps = http_capabilities(client=sticky_client)
        got = caps.sticky_echo_headers
        assert got == (), f"sticky-enabled-but-no-echo server must advertise empty echo-headers tuple; got {got!r}"

    def test_capability_lists_configured_names(self, echo_sticky_client: _SyncTestClient) -> None:
        """``VGI-Sticky-Echo-Headers`` lists the configured header names; surfaces in capabilities."""
        caps = http_capabilities(client=echo_sticky_client)
        assert caps.sticky_echo_headers == ("x-test-echo-marker",)


class TestEchoHeadersClient:
    """Client-side capture + replay contract."""

    def test_current_echo_headers_populated_after_open(self, echo_sticky_client: _SyncTestClient) -> None:
        """After ``open_counter``, ``view.current_echo_headers()`` returns the marker dict."""
        with _connect(echo_sticky_client) as proxy, proxy.with_session_token() as sess:
            assert dict(sess.current_echo_headers()) == {}, (
                "no echo headers should be captured before the first session-opening call"
            )
            sess.open_counter(initial=1)
            assert dict(sess.current_echo_headers()) == {"x-test-echo-marker": "captured-by-client"}

    def test_replay_on_subsequent_requests(self, echo_sticky_client: _SyncTestClient) -> None:
        """Captured echo headers ride on every subsequent request inside the same block."""
        captured_request_headers: list[dict[str, str]] = []

        from vgi_rpc.http import _client as _hc

        orig_merge = _hc._SessionTrackingClient._merge_headers

        def patched(self: _hc._SessionTrackingClient, headers: dict[str, str] | None) -> dict[str, str]:
            merged = orig_merge(self, headers)
            captured_request_headers.append(dict(merged))
            return merged

        _hc._SessionTrackingClient._merge_headers = patched  # type: ignore[method-assign]
        try:
            with _connect(echo_sticky_client) as proxy, proxy.with_session_token() as sess:
                sess.open_counter(initial=1)
                sess.increment_counter(by=1)
                sess.increment_counter(by=1)
        finally:
            _hc._SessionTrackingClient._merge_headers = orig_merge  # type: ignore[method-assign]
        # Open call has no echo header on the way out (it's the FIRST call;
        # the server's echo header lands on the *response*).
        assert "x-test-echo-marker" not in captured_request_headers[0]
        # Every subsequent request carries the captured echo header.
        for i, hdrs in enumerate(captured_request_headers[1:], start=1):
            assert hdrs.get("x-test-echo-marker") == "captured-by-client", (
                f"request #{i} must carry the captured echo header; got {hdrs!r}"
            )

    def test_close_clears_echo_headers(self, echo_sticky_client: _SyncTestClient) -> None:
        """``VGI-Session-Close: true`` from the server clears the captured echo headers too."""
        with _connect(echo_sticky_client) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            assert dict(sess.current_echo_headers()) == {"x-test-echo-marker": "captured-by-client"}
            sess.close_counter()
            assert dict(sess.current_echo_headers()) == {}, (
                "close_session must clear captured echo headers alongside the token"
            )

    def test_current_echo_headers_returns_readonly_snapshot(self, echo_sticky_client: _SyncTestClient) -> None:
        """``current_echo_headers()`` returns a read-only mapping; caller can't mutate view state."""
        with _connect(echo_sticky_client) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=1)
            snapshot = sess.current_echo_headers()
            # MappingProxyType is read-only — writing raises TypeError.
            with pytest.raises(TypeError):
                # Cast to ignore the type-system disagreement; the runtime
                # behaviour is what's being verified.
                cast("dict[str, str]", snapshot)["x-test-echo-marker"] = "modified"
            # Inner state unaffected.
            assert dict(sess.current_echo_headers()) == {"x-test-echo-marker": "captured-by-client"}


class TestFlyHelper:
    """``vgi_rpc.http.fly`` quickstart helpers."""

    def test_auto_server_id_off_fly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``auto_server_id()`` returns ``None`` when ``FLY_MACHINE_ID`` is unset."""
        monkeypatch.delenv("FLY_MACHINE_ID", raising=False)
        # Force reimport so module-level FLY_MACHINE_ID is recomputed.
        import importlib

        from vgi_rpc.http import fly

        importlib.reload(fly)
        assert fly.auto_server_id() is None
        assert fly.fly_sticky_echo_headers() is None

    def test_auto_server_id_on_fly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``auto_server_id()`` returns the machine ID when ``FLY_MACHINE_ID`` is set."""
        monkeypatch.setenv("FLY_MACHINE_ID", "machine-test-abc123")
        import importlib

        from vgi_rpc.http import fly

        importlib.reload(fly)
        assert fly.auto_server_id() == "machine-test-abc123"
        assert fly.fly_sticky_echo_headers() == {"fly-force-instance-id": "machine-test-abc123"}

    def test_module_exports(self) -> None:
        """``vgi_rpc.http.fly.__all__`` lists the documented surface."""
        from vgi_rpc.http import fly

        assert set(fly.__all__) == {"FLY_MACHINE_ID", "auto_server_id", "fly_sticky_echo_headers"}


# ---------------------------------------------------------------------------
# Drain (PR3)
# ---------------------------------------------------------------------------


class TestDrainHandle:
    """``vgi_rpc.http.drain_handle`` API + drain semantics."""

    def test_drain_handle_returns_none_for_non_sticky(self) -> None:
        """``drain_handle(app)`` returns ``None`` when sticky isn't enabled."""
        from vgi_rpc.http import drain_handle

        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        client = make_sync_client(server)  # no enable_sticky
        try:
            assert drain_handle(client._client.app) is None
        finally:
            client.close()

    def test_drain_handle_methods(self, sticky_client: _SyncTestClient) -> None:
        """``drain_handle(app).drain()`` / ``is_draining()`` / ``shutdown()`` behave as documented."""
        from vgi_rpc.http import drain_handle

        handle = drain_handle(sticky_client._client.app)
        assert handle is not None
        assert handle.is_draining() is False
        handle.drain()
        assert handle.is_draining() is True
        # drain() is idempotent.
        handle.drain()
        assert handle.is_draining() is True

    def test_drain_blocks_new_opens_existing_serves(self, sticky_client: _SyncTestClient) -> None:
        """During drain, new opens raise ``ServerDrainingError``; existing sessions keep serving."""
        from vgi_rpc.http import drain_handle

        handle = drain_handle(sticky_client._client.app)
        assert handle is not None

        with _connect(sticky_client) as proxy, proxy.with_session_token() as existing:
            existing.open_counter(initial=100)
            # Flip drain — existing session still works.
            handle.drain()
            assert existing.increment_counter(by=1) == 101, "drain must not disturb existing sessions"

            # New session opens are rejected with ServerDrainingError.
            with proxy.with_session_token() as new_sess, pytest.raises(RpcError) as excinfo:
                new_sess.open_counter(initial=1)
            assert excinfo.value.error_type == "ServerDrainingError"

            existing.close_counter()

    def test_shutdown_invokes_state_close_on_live_sessions(self, sticky_client: _SyncTestClient) -> None:
        """``handle.shutdown()`` calls ``state.close()`` on every live session — the WSGI-teardown contract."""
        from vgi_rpc.http import drain_handle

        handle = drain_handle(sticky_client._client.app)
        assert handle is not None

        # Directly populate the registry with sentinel counters whose
        # ``close()`` flips a flag we can assert on. Going through the
        # public open_counter path would also work but adds two HTTP
        # round-trips; the direct registry interaction is simpler.
        registry: _SessionRegistry = handle.shutdown.__self__  # type: ignore[attr-defined]
        counters = [_StickyCounter(value=i) for i in range(3)]
        for c in counters:
            registry.open(c, ttl=None, principal_key="anon")
        handle.shutdown()
        assert all(c.closed for c in counters), "shutdown must close every live session"
        assert len(registry) == 0, "shutdown must clear the registry"


class TestAccessLogSessionFields:
    """``session_id`` + ``session_action`` fields on the ``vgi_rpc.access`` log."""

    @pytest.fixture
    def captured_access_records(self) -> Iterator[list[dict[str, object]]]:
        """Capture access-log records during the test."""
        import logging

        records: list[dict[str, object]] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record.__dict__.copy())

        handler = _Capture()
        handler.setLevel(logging.INFO)
        logger = logging.getLogger("vgi_rpc.access")
        prior_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            yield records
        finally:
            logger.removeHandler(handler)
            logger.setLevel(prior_level)

    def test_non_sticky_call_no_session_fields(
        self,
        sticky_client: _SyncTestClient,
        captured_access_records: list[dict[str, object]],
    ) -> None:
        """Calls that don't touch sticky machinery have no session fields."""
        with _connect(sticky_client) as proxy:
            proxy.echo_int(value=42)
        # echo_int went through sticky middleware (sticky enabled on the
        # fixture client) but didn't open a session — action should be "none".
        assert captured_access_records, "test must produce at least one access record"
        last = captured_access_records[-1]
        assert last.get("method") == "echo_int"
        assert last.get("session_action") == "none", (
            f"non-sticky call should have session_action='none'; got {last.get('session_action')!r}"
        )
        assert last.get("session_id") is None, (
            f"non-sticky call should have no session_id; got {last.get('session_id')!r}"
        )

    def test_full_lifecycle_actions(
        self,
        sticky_client: _SyncTestClient,
        captured_access_records: list[dict[str, object]],
    ) -> None:
        """Open → resume → close emit the expected access-log actions with consistent session_id."""
        with _connect(sticky_client) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=7)
            sess.increment_counter(by=1)
            sess.close_counter()

        # Find the records for our methods in order.
        ours = [
            r
            for r in captured_access_records
            if r.get("method") in ("open_counter", "increment_counter", "close_counter")
        ]
        assert len(ours) >= 3, f"expected at least 3 records, got {len(ours)}"
        open_rec, resume_rec, close_rec = ours[-3], ours[-2], ours[-1]
        assert open_rec["session_action"] == "open"
        assert resume_rec["session_action"] == "resume"
        assert close_rec["session_action"] == "close"
        # All three records carry the SAME session_id — proves the contract
        # that close records still surface the id of the just-closed session.
        ids = {open_rec["session_id"], resume_rec["session_id"], close_rec["session_id"]}
        assert len(ids) == 1, f"all three lifecycle records must share one session_id; got {ids!r}"
        assert isinstance(open_rec["session_id"], str) and len(open_rec["session_id"]) == 24, (
            f"session_id must be a 24-char hex string; got {open_rec['session_id']!r}"
        )

    def test_non_sticky_server_omits_fields(
        self,
        captured_access_records: list[dict[str, object]],
    ) -> None:
        """A non-sticky server emits no session_id / session_action fields at all."""
        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        client = make_sync_client(server)  # no enable_sticky
        try:
            with _connect(client) as proxy:
                proxy.echo_int(value=1)
        finally:
            client.close()
        last = captured_access_records[-1]
        assert "session_id" not in last
        assert "session_action" not in last


# ---------------------------------------------------------------------------
# Runtime API guards (CallContext.open_session / close_session)
# ---------------------------------------------------------------------------
#
# These are contracts on the Python runtime API rather than the wire, so
# they live here: expressing them cross-language would mean adding methods
# to the conformance protocol that every port must then implement.


class _GuardProtocol(Protocol):
    """Service whose methods deliberately misuse the sticky runtime API."""

    def double_open(self, value: int) -> int:
        """Call ``ctx.open_session`` twice in one request."""
        ...

    def close_without_open(self, value: int) -> int:
        """Call ``ctx.close_session`` with no session bound."""
        ...

    def open_with_ttl(self, value: int, ttl_ms: int) -> int:
        """Open a session with a per-call TTL override."""
        ...

    def peek(self) -> int:
        """Return the bound counter's value, or ``-1`` when nothing is bound."""
        ...


class _GuardImpl:
    """Implementation backing :class:`_GuardProtocol`."""

    def double_open(self, value: int, ctx: CallContext) -> int:
        """Open twice — the second call must raise."""
        ctx.open_session(_StickyCounter(value=value))
        ctx.open_session(_StickyCounter(value=value))
        return value

    def close_without_open(self, value: int, ctx: CallContext) -> int:
        """Close with nothing bound — documented as an idempotent no-op."""
        ctx.close_session()
        return value

    def open_with_ttl(self, value: int, ttl_ms: int, ctx: CallContext) -> int:
        """Open a session that expires after *ttl_ms*, ignoring the server default."""
        ctx.open_session(_StickyCounter(value=value), ttl=ttl_ms / 1000.0)
        return value

    def peek(self, ctx: CallContext) -> int:
        """Report the bound counter's value."""
        counter = ctx.session
        return counter.value if isinstance(counter, _StickyCounter) else -1


@pytest.fixture
def guard_client() -> Iterator[_SyncTestClient]:
    """Sticky client serving :class:`_GuardProtocol` with a long default TTL.

    The long default is what makes the per-call ``ttl=`` override
    observable — an entry that ages out did so because the override took
    effect, not because the server default elapsed.
    """
    server = RpcServer(_GuardProtocol, _GuardImpl())
    client = make_sync_client(server, token_key=_TOKEN_KEY, enable_sticky=True, sticky_default_ttl=300.0)
    try:
        yield client
    finally:
        client.close()


class TestRuntimeApiGuards:
    """``open_session`` / ``close_session`` misuse and per-call TTL (spec §4)."""

    def test_double_open_raises(self, guard_client: _SyncTestClient) -> None:
        """A second ``open_session`` in one request raises rather than orphaning the first."""
        with (
            http_connect(_GuardProtocol, client=guard_client) as proxy,
            cast("Any", proxy).with_session_token() as sess,
            pytest.raises(RpcError) as excinfo,
        ):
            sess.double_open(value=1)
        assert excinfo.value.error_type == "RuntimeError"
        assert "already active" in str(excinfo.value)

    def test_close_without_open_is_noop(self, guard_client: _SyncTestClient) -> None:
        """``close_session`` with nothing bound is an idempotent no-op, not an error."""
        with (
            http_connect(_GuardProtocol, client=guard_client) as proxy,
            cast("Any", proxy).with_session_token() as sess,
        ):
            assert sess.close_without_open(value=7) == 7
            # The no-op must not poison the view: a real session still opens.
            assert sess.open_with_ttl(value=3, ttl_ms=60_000) == 3
            assert sess.peek() == 3

    def test_per_call_ttl_override_expires_early(self, guard_client: _SyncTestClient) -> None:
        """``open_session(ttl=...)`` overrides the server default (300s here)."""
        with (
            http_connect(_GuardProtocol, client=guard_client) as proxy,
            cast("Any", proxy).with_session_token() as sess,
        ):
            sess.open_with_ttl(value=11, ttl_ms=200)
            assert sess.peek() == 11
            time.sleep(0.5)
            with pytest.raises(RpcError) as excinfo:
                sess.peek()
            assert excinfo.value.error_type == "SessionLostError", (
                "a per-call ttl shorter than the server default must still expire the session"
            )


# ---------------------------------------------------------------------------
# Registry internals
# ---------------------------------------------------------------------------


class _ExplodingState:
    """Session state whose ``close()`` raises — eviction must survive it."""

    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        """Record the call, then raise."""
        self.close_calls += 1
        msg = "close() blew up"
        raise RuntimeError(msg)


class TestRegistryInternals:
    """Direct coverage of ``_SessionRegistry`` branches the HTTP path reaches rarely."""

    def test_get_evicts_expired_entry_inline(self) -> None:
        """A lookup past the TTL evicts the entry and closes its state, returning ``None``."""
        registry = _SessionRegistry(default_ttl=0.01)
        counter = _StickyCounter(value=1)
        session_id, _expires_at = registry.open(counter, ttl=0.01, principal_key="anon")
        time.sleep(0.05)
        assert registry.get(session_id, "anon") is None, "an aged-out entry must not resolve"
        assert counter.closed is True, "inline eviction must invoke state.close()"
        assert len(registry) == 0, "the expired entry must be removed, not just hidden"

    def test_get_rejects_principal_mismatch(self) -> None:
        """A live entry does not resolve for a different principal key.

        Defense-in-depth behind the token's AAD binding: even a validly
        sealed token must not bind to another principal's entry.
        """
        registry = _SessionRegistry(default_ttl=60.0)
        counter = _StickyCounter(value=1)
        session_id, _expires_at = registry.open(counter, ttl=None, principal_key="domain\x00alice")
        assert registry.get(session_id, "domain\x00bob") is None
        # Not evicted — the owner can still resolve it.
        assert registry.get(session_id, "domain\x00alice") is not None
        assert counter.closed is False

    def test_close_returns_false_on_miss(self) -> None:
        """Closing an unknown session id reports a miss instead of raising."""
        registry = _SessionRegistry(default_ttl=60.0)
        assert registry.close(b"\x00" * 12) is False

    def test_state_close_exception_is_suppressed(self) -> None:
        """A raising ``state.close()`` never escapes eviction, on any eviction path."""
        registry = _SessionRegistry(default_ttl=60.0)
        explicit = _ExplodingState()
        sid, _ = registry.open(explicit, ttl=None, principal_key="anon")
        assert registry.close(sid) is True
        assert explicit.close_calls == 1

        expiring = _ExplodingState()
        registry.open(expiring, ttl=-1.0, principal_key="anon")  # already expired
        assert registry.drain_expired() == 1
        assert expiring.close_calls == 1

        remaining = _ExplodingState()
        registry.open(remaining, ttl=None, principal_key="anon")
        registry.shutdown()
        assert remaining.close_calls == 1
        assert len(registry) == 0

    def test_iteration_snapshots_live_ids(self) -> None:
        """Iterating the registry yields the live session ids."""
        registry = _SessionRegistry(default_ttl=60.0)
        ids = {registry.open(_StickyCounter(value=i), ttl=None, principal_key="anon")[0] for i in range(3)}
        assert set(registry) == ids

    def test_open_rejected_while_draining(self) -> None:
        """``open`` raises ``ServerDrainingError`` once the drain flag is set."""
        from vgi_rpc.rpc import ServerDrainingError

        registry = _SessionRegistry(default_ttl=60.0)
        registry.set_draining(True)
        with pytest.raises(ServerDrainingError):
            registry.open(_StickyCounter(value=1), ttl=None, principal_key="anon")


class TestReaperThread:
    """The daemon sweep thread: it must evict, survive errors, and stop."""

    def test_reaper_evicts_without_any_request(self) -> None:
        """The reaper closes an aged-out session with no client traffic at all."""
        registry = _SessionRegistry(default_ttl=0.05)
        counter = _StickyCounter(value=1)
        registry.open(counter, ttl=0.05, principal_key="anon")
        reaper = _ReaperThread(registry, tick_seconds=0.02)
        reaper.start()
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not counter.closed:
                time.sleep(0.01)
            assert counter.closed is True, "the reaper thread must evict expired sessions on its own"
        finally:
            reaper.stop()
            reaper.join(timeout=5)
        assert not reaper.is_alive(), "stop() must end the reaper at the next tick boundary"

    def test_reaper_survives_a_failing_tick(self) -> None:
        """An exception from one sweep must not kill the thread."""
        failures: list[int] = []

        class _FlakyRegistry(_SessionRegistry):
            def drain_expired(self, now: float | None = None) -> int:
                """Fail the first sweep, then behave."""
                if not failures:
                    failures.append(1)
                    msg = "sweep exploded"
                    raise RuntimeError(msg)
                return super().drain_expired(now)

        registry = _FlakyRegistry(default_ttl=0.05)
        reaper = _ReaperThread(registry, tick_seconds=0.02)
        reaper.start()
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not failures:
                time.sleep(0.01)
            assert failures, "the flaky sweep never ran"
            # Give the thread a few more ticks; it must still be alive.
            time.sleep(0.1)
            counter = _StickyCounter(value=1)
            registry.open(counter, ttl=0.01, principal_key="anon")
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not counter.closed:
                time.sleep(0.01)
            assert counter.closed is True, "the reaper must keep sweeping after a failed tick"
        finally:
            reaper.stop()
            reaper.join(timeout=5)

    def test_middleware_reaper_lifecycle_is_idempotent(self) -> None:
        """``_ensure_reaper`` starts one thread; ``stop_reaper`` is safe to call twice."""
        registry = _SessionRegistry(default_ttl=60.0)
        middleware = _StickyMiddleware(registry, _TOKEN_KEY)
        middleware._ensure_reaper()
        first = middleware._reaper
        assert first is not None and first.is_alive()
        middleware._ensure_reaper()
        assert middleware._reaper is first, "_ensure_reaper must not spawn a second thread"
        middleware.stop_reaper()
        middleware.stop_reaper()  # idempotent
        first.join(timeout=5)
        assert not first.is_alive()


# ---------------------------------------------------------------------------
# Concurrency: distinct sessions must not serialize against each other
# ---------------------------------------------------------------------------


class _RendezvousProtocol(Protocol):
    """Service used to prove two sessions execute concurrently."""

    def open_it(self, value: int) -> int:
        """Open a session holding *value*."""
        ...

    def rendezvous(self, tag: int) -> int:
        """Block until another in-flight call reaches the same barrier."""
        ...


class _RendezvousImpl:
    """Implementation whose ``rendezvous`` only returns if two calls run at once."""

    def __init__(self, barrier: threading.Barrier) -> None:
        self._barrier = barrier

    def open_it(self, value: int, ctx: CallContext) -> int:
        """Open a sticky session."""
        ctx.open_session(_StickyCounter(value=value))
        return value

    def rendezvous(self, tag: int, ctx: CallContext) -> int:
        """Wait for the peer call; raises ``BrokenBarrierError`` if it never arrives."""
        self._barrier.wait(timeout=10)
        return tag


class TestDistinctSessionParallelism:
    """Spec §5: different-session calls run in parallel; the registry lock is not a chokepoint."""

    def test_two_sessions_execute_concurrently(self) -> None:
        """Two calls on distinct sessions meet at a barrier — impossible if they serialized.

        A timing assertion would be flaky; the barrier makes the contract
        deterministic. If the framework serialized across sessions, the
        first call would wait for a peer that cannot start, and both would
        surface ``BrokenBarrierError``.
        """
        barrier = threading.Barrier(2)
        server = RpcServer(_RendezvousProtocol, _RendezvousImpl(barrier))
        client = make_sync_client(server, token_key=_TOKEN_KEY, enable_sticky=True, sticky_default_ttl=60.0)
        try:
            with (
                http_connect(_RendezvousProtocol, client=client) as proxy,
                cast("Any", proxy).with_session_token() as sess_a,
                cast("Any", proxy).with_session_token() as sess_b,
            ):
                sess_a.open_it(value=1)
                sess_b.open_it(value=2)
                with ThreadPoolExecutor(max_workers=2) as pool:
                    future_a = pool.submit(sess_a.rendezvous, tag=1)
                    future_b = pool.submit(sess_b.rendezvous, tag=2)
                    assert future_a.result(timeout=30) == 1
                    assert future_b.result(timeout=30) == 2
        finally:
            client.close()


# ---------------------------------------------------------------------------
# Principal binding on the DELETE endpoint
# ---------------------------------------------------------------------------
#
# The cross-principal *replay* contract is canonical and lives in the
# TestSticky conformance group; what stays here is the Python-side check
# that DELETE honours the same binding while remaining unprobeable.


_PRINCIPAL_HEADER = "X-Test-Principal"


def _principal_auth(req: falcon.Request) -> AuthContext:
    """Map ``X-Test-Principal`` to an authenticated principal; anonymous when absent."""
    principal = req.get_header(_PRINCIPAL_HEADER)
    if not principal:
        return AuthContext(domain=None, authenticated=False, principal=None)
    return AuthContext(domain="test", authenticated=True, principal=principal)


class TestSessionDeleteEndpointPrincipalBinding:
    """``DELETE /vgi/__session__`` is principal-bound and idempotent (spec §2.5)."""

    def test_delete_with_foreign_token_is_a_no_op(self) -> None:
        """Another principal's DELETE returns 200 and leaves the session untouched.

        200 rather than 404 is deliberate: a stolen token must not reveal
        whether the session exists.
        """
        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        app = make_wsgi_app(
            server,
            token_key=_TOKEN_KEY,
            enable_sticky=True,
            sticky_default_ttl=60.0,
            authenticate=_principal_auth,
        )
        alice = _SyncTestClient(app, default_headers={_PRINCIPAL_HEADER: "alice"})
        bob = _SyncTestClient(app, default_headers={_PRINCIPAL_HEADER: "bob"})

        with _connect(alice) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=5)
            token = sess.detach()
        assert token is not None

        resp = bob.delete("/__session__", headers={SESSION_HEADER: token})
        assert resp.status_code == 200, "a foreign DELETE must look like an idempotent no-op"

        with _connect(alice) as proxy, proxy.with_session_token(token=token) as sess:
            assert sess.increment_counter(by=1) == 6, "a foreign DELETE must not evict the session"
            sess.close_counter()

    def test_delete_by_owner_evicts(self) -> None:
        """The owning principal's DELETE returns 204 and evicts the session."""
        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        app = make_wsgi_app(
            server,
            token_key=_TOKEN_KEY,
            enable_sticky=True,
            sticky_default_ttl=60.0,
            authenticate=_principal_auth,
        )
        alice = _SyncTestClient(app, default_headers={_PRINCIPAL_HEADER: "alice"})

        with _connect(alice) as proxy, proxy.with_session_token() as sess:
            sess.open_counter(initial=5)
            token = sess.detach()
        assert token is not None

        resp = alice.delete("/__session__", headers={SESSION_HEADER: token})
        assert resp.status_code == 204

        with (
            _connect(alice) as proxy,
            proxy.with_session_token(token=token) as sess,
            pytest.raises(RpcError) as excinfo,
        ):
            sess.increment_counter(by=1)
        assert excinfo.value.error_type == "SessionLostError"


# ---------------------------------------------------------------------------
# serve_http's SIGTERM / SIGINT drain wiring
# ---------------------------------------------------------------------------


class _Exited(Exception):
    """Stand-in for ``os._exit`` so the handler aborts like the real thing."""

    def __init__(self, code: int) -> None:
        super().__init__(f"exit {code}")
        self.code = code


class _CapturedTimer:
    """``threading.Timer`` stand-in that records the callback instead of scheduling it."""

    calls: list[tuple[float, Any]] = []  # noqa: RUF012 — test-local recorder, not shared state

    def __init__(self, interval: float, function: Any) -> None:
        self.daemon = False
        _CapturedTimer.calls.append((interval, function))

    def start(self) -> None:
        """Record-only: the test invokes the callback itself."""


class TestDrainSignalHandlers:
    """``serve_http``'s graceful-shutdown wiring (spec §7)."""

    @staticmethod
    def _sticky_app_and_registry() -> tuple[Any, Any]:
        """Build a sticky WSGI app and return it with its session registry."""
        from vgi_rpc.http import drain_handle

        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        app = make_wsgi_app(server, token_key=_TOKEN_KEY, enable_sticky=True, sticky_default_ttl=60.0)
        handle = drain_handle(app)
        assert handle is not None
        registry = handle.shutdown.__self__  # type: ignore[attr-defined]
        return app, registry

    @pytest.fixture
    def signal_harness(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[int, Any]]:
        """Capture installed signal handlers, timers, and exits instead of running them."""
        import os
        import signal

        installed: dict[int, Any] = {}
        _CapturedTimer.calls = []

        # pytest-timeout arms its own SIGALRM handler around the test body, and
        # because the suite sets `timeout_func_only` that happens *after* this
        # fixture runs. Pass SIGALRM through to the real implementation rather
        # than capturing it: swallowing it would both disable the timeout for
        # these tests and pollute the drain-handler assertions below, which are
        # only ever about SIGTERM/SIGINT. SIGALRM does not exist on Windows.
        _real_signal = signal.signal
        _sigalrm = getattr(signal, "SIGALRM", None)

        def _fake_signal(sig: int, handler: Any) -> Any:
            if _sigalrm is not None and sig == _sigalrm:
                return _real_signal(sig, handler)
            installed[sig] = handler
            return None

        def _fake_exit(code: int) -> None:
            raise _Exited(code)

        monkeypatch.setattr(signal, "signal", _fake_signal)
        monkeypatch.setattr(threading, "Timer", _CapturedTimer)
        monkeypatch.setattr(os, "_exit", _fake_exit)
        yield installed
        _CapturedTimer.calls = []

    def test_no_handlers_installed_for_non_sticky_app(self, signal_harness: dict[int, Any]) -> None:
        """A non-sticky app installs nothing — ``serve_http`` must not touch process signals."""
        from vgi_rpc.http.server._serve import _install_drain_signal_handlers

        server = RpcServer(ConformanceService, ConformanceServiceImpl())
        app = make_wsgi_app(server, token_key=_TOKEN_KEY)  # no enable_sticky
        _install_drain_signal_handlers(app, 30.0)
        assert signal_harness == {}, "no drain handlers should be installed without sticky sessions"

    def test_sigterm_drains_then_closes_sessions_after_grace(self, signal_harness: dict[int, Any]) -> None:
        """SIGTERM flips the drain flag; the grace timer closes live sessions and exits 0."""
        import signal as signal_mod

        from vgi_rpc.http import drain_handle
        from vgi_rpc.http.server._serve import _install_drain_signal_handlers

        app, registry = self._sticky_app_and_registry()
        counter = _StickyCounter(value=1)
        registry.open(counter, ttl=None, principal_key="anon")

        _install_drain_signal_handlers(app, 30.0)
        assert signal_mod.SIGTERM in signal_harness, "SIGTERM handler must be installed"

        handle = drain_handle(app)
        assert handle is not None
        assert handle.is_draining() is False

        signal_harness[signal_mod.SIGTERM](int(signal_mod.SIGTERM), None)
        assert handle.is_draining() is True, "the signal must flip the drain flag immediately"
        assert counter.closed is False, "live sessions must survive until the grace period elapses"

        assert len(_CapturedTimer.calls) == 1, "a single grace timer must be scheduled"
        interval, on_grace_expired = _CapturedTimer.calls[0]
        assert interval == 30.0

        with pytest.raises(_Exited) as excinfo:
            on_grace_expired()
        assert excinfo.value.code == 0
        assert counter.closed is True, "grace expiry must invoke state.close() on live sessions"
        assert len(registry) == 0

    def test_second_signal_exits_immediately(self, signal_harness: dict[int, Any]) -> None:
        """A second signal during grace skips the wait and exits non-zero."""
        import signal as signal_mod

        from vgi_rpc.http.server._serve import _install_drain_signal_handlers

        app, registry = self._sticky_app_and_registry()
        counter = _StickyCounter(value=1)
        registry.open(counter, ttl=None, principal_key="anon")

        _install_drain_signal_handlers(app, 30.0)
        handler = signal_harness[signal_mod.SIGINT]
        handler(int(signal_mod.SIGINT), None)
        with pytest.raises(_Exited) as excinfo:
            handler(int(signal_mod.SIGINT), None)
        assert excinfo.value.code == 1
