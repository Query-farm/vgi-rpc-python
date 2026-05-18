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

import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance._impl import _StickyCounter
from vgi_rpc.http import http_capabilities, http_connect
from vgi_rpc.http._common import SESSION_ACCEPT_HEADER, SESSION_HEADER
from vgi_rpc.http._testing import _SyncTestClient, make_sync_client
from vgi_rpc.rpc import (
    CallContext,
    RpcError,
    RpcServer,
    SessionLostError,
    serve_pipe,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

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
        from vgi_rpc.http.server._sticky import _SessionRegistry

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
