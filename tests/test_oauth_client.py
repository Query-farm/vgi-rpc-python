# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for vgi_rpc.http._oauth_client -- the client-driven OAuth login flow.

Unlike tests/test_oauth.py and tests/test_oauth_pkce.py (which exercise the
server side of OAuth against a real vgi-rpc WSGI app), everything here plays
the *other* role: a mock IdP + mock protected resource, driven entirely by
httpx2.MockTransport (no sockets, no real server), with VgiOAuthAuth as the
client under test.
"""

from __future__ import annotations

import threading
import time

import httpx2
import pytest

from vgi_rpc.http._client import OAuthResourceMetadataResponse, OAuthServerMetadata
from vgi_rpc.http._oauth_client import (
    OAuthRefreshContext,
    OAuthTokenError,
    OAuthTokenSet,
    VgiOAuthAuth,
    _redact_secret,
    attempt_token_refresh,
    is_colab_environment,
    is_headless_environment,
    parse_oauth_challenge,
    perform_device_code_flow,
    select_device_code_client,
)

RESOURCE_METADATA_URL = "https://api.example.com/.well-known/oauth-protected-resource"
DEVICE_AUTH_URL = "https://auth.example.com/device/code"
TOKEN_URL = "https://auth.example.com/token"
PROXY_TOKEN_URL = "https://api.example.com/_oauth/token"
AUTHORIZATION_ENDPOINT = "https://auth.example.com/authorize"
CHALLENGE_HEADER = f'Bearer resource_metadata="{RESOURCE_METADATA_URL}"'


class MockIdp:
    """A scriptable mock IdP + protected resource, driven by httpx2.MockTransport.

    Routes by exact URL. ``token_responses`` is a queue consumed in order by
    every POST to *any* token-shaped endpoint (the real ``/token`` and the
    proxy ``/_oauth/token`` alike) -- tests push ``(status_code, json_body)``
    tuples to script a sequence (e.g. authorization_pending, then success).
    """

    def __init__(
        self,
        *,
        resource_metadata: dict[str, object] | None = None,
        server_metadata: dict[str, object] | None = None,
        device_auth_response: dict[str, object] | None = None,
    ) -> None:
        """Build a mock IdP with sensible defaults, all overridable per test."""
        self.resource_metadata = resource_metadata or {
            "resource": "https://api.example.com",
            "authorization_servers": ["https://auth.example.com"],
            "client_id": "ordinary-client",
            "client_secret": "ordinary-secret",
            "device_code_client_id": "device-client",
            "device_code_client_secret": "device-secret",
        }
        self.server_metadata = server_metadata or {
            "authorization_endpoint": AUTHORIZATION_ENDPOINT,
            "token_endpoint": TOKEN_URL,
            "device_authorization_endpoint": DEVICE_AUTH_URL,
            "grant_types_supported": [
                "urn:ietf:params:oauth:grant-type:device_code",
                "authorization_code",
                "refresh_token",
            ],
        }
        self.device_auth_response = device_auth_response or {
            "device_code": "devcode-123",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://auth.example.com/activate",
            "expires_in": 600,
            "interval": 0,  # tests don't want to actually wait
        }
        self.token_responses: list[tuple[int, dict[str, object]]] = []
        self.device_auth_calls = 0
        self.token_calls = 0
        self.token_calls_by_url: dict[str, int] = {}
        self.resource_calls = 0
        self.granted_token = "access-token-xyz"

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        """Route a request to the matching mock endpoint by exact URL."""
        url = str(request.url)
        if url == RESOURCE_METADATA_URL:
            return httpx2.Response(200, json=self.resource_metadata)
        if url == "https://auth.example.com/.well-known/openid-configuration":
            return httpx2.Response(200, json=self.server_metadata)
        if url == DEVICE_AUTH_URL:
            self.device_auth_calls += 1
            return httpx2.Response(200, json=self.device_auth_response)
        if url in (TOKEN_URL, PROXY_TOKEN_URL):
            self.token_calls += 1
            self.token_calls_by_url[url] = self.token_calls_by_url.get(url, 0) + 1
            if self.token_responses:
                status, body = self.token_responses.pop(0)
                return httpx2.Response(status, json=body)
            return httpx2.Response(200, json={"access_token": self.granted_token, "token_type": "Bearer"})
        if url.endswith("/protected"):
            self.resource_calls += 1
            token = request.headers.get("authorization", "")
            if token:
                return httpx2.Response(200, json={"ok": True, "authorization": token})
            return httpx2.Response(401, headers={"www-authenticate": CHALLENGE_HEADER})
        return httpx2.Response(404, json={"error": "not_found", "url": url})  # pragma: no cover

    def transport(self) -> httpx2.MockTransport:
        """Return a fresh MockTransport bound to this IdP's handler."""
        return httpx2.MockTransport(self.handler)


def _resource_meta(**overrides: object) -> OAuthResourceMetadataResponse:
    """Build an OAuthResourceMetadataResponse matching MockIdp's defaults."""
    defaults: dict[str, object] = {
        "resource": "https://api.example.com",
        "authorization_servers": ("https://auth.example.com",),
        "client_id": "ordinary-client",
        "client_secret": "ordinary-secret",
        "device_code_client_id": "device-client",
        "device_code_client_secret": "device-secret",
    }
    defaults.update(overrides)
    return OAuthResourceMetadataResponse(**defaults)  # type: ignore[arg-type]


def _server_meta(**overrides: object) -> OAuthServerMetadata:
    """Build an OAuthServerMetadata matching MockIdp's defaults."""
    defaults: dict[str, object] = {
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_URL,
        "device_authorization_endpoint": DEVICE_AUTH_URL,
        "grant_types_supported": ("urn:ietf:params:oauth:grant-type:device_code", "refresh_token"),
    }
    defaults.update(overrides)
    return OAuthServerMetadata(**defaults)  # type: ignore[arg-type]


class TestRedactSecret:
    """Tests for _redact_secret, the DebugSecret-equivalent redaction helper."""

    def test_empty(self) -> None:
        """An empty secret redacts to a fixed "(empty)" marker."""
        assert _redact_secret("") == "(empty)"

    def test_short_is_fully_redacted(self) -> None:
        """A secret of 8 chars or fewer never appears, only its length."""
        result = _redact_secret("abc12345")  # exactly 8 chars
        assert "abc12345" not in result
        assert "8 chars" in result

    def test_long_shows_first_and_last_four(self) -> None:
        """A longer secret shows only its first/last 4 chars plus a length prefix."""
        result = _redact_secret("supersecretvalue1234")
        assert result.startswith("(20 chars) supe")
        assert result.endswith("1234")
        assert "supersecretvalue" not in result


class TestOAuthTokenSetValidity:
    """Tests for OAuthTokenSet.is_valid() and .bearer_token()."""

    def test_no_expiry_is_always_valid(self) -> None:
        """A token with no advertised expiry is always considered valid."""
        assert OAuthTokenSet(access_token="tok").is_valid()

    def test_empty_access_token_is_never_valid(self) -> None:
        """An empty access_token is never valid, regardless of expiry."""
        assert not OAuthTokenSet(access_token="").is_valid()

    def test_fresh_token_is_valid(self) -> None:
        """A token well ahead of its expiry is valid."""
        now = 1000.0
        tok = OAuthTokenSet(access_token="tok", expires_at=now + 100)
        assert tok.is_valid(now=now)

    def test_token_within_skew_is_stale(self) -> None:
        """A token inside the refresh skew window is treated as already stale."""
        now = 1000.0
        tok = OAuthTokenSet(access_token="tok", expires_at=now + 10)  # < default 45s skew
        assert not tok.is_valid(now=now)

    def test_bearer_token_prefers_id_token_when_use_id_token_set(self) -> None:
        """With use_id_token=True and an id_token present, bearer_token() returns the id_token."""
        tok = OAuthTokenSet(access_token="access", id_token="id", use_id_token=True)
        assert tok.bearer_token() == "id"

    def test_bearer_token_falls_back_to_access_token_without_id_token(self) -> None:
        """With use_id_token=True but no id_token, bearer_token() falls back to access_token."""
        tok = OAuthTokenSet(access_token="access", use_id_token=True)
        assert tok.bearer_token() == "access"

    def test_bearer_token_ignores_id_token_when_flag_unset(self) -> None:
        """With use_id_token=False, bearer_token() always returns access_token."""
        tok = OAuthTokenSet(access_token="access", id_token="id", use_id_token=False)
        assert tok.bearer_token() == "access"


class TestSelectDeviceCodeClient:
    """Tests for select_device_code_client's three-tier precedence."""

    def test_device_client_wins_when_present(self) -> None:
        """A device_code_client_id, when present, always wins."""
        client_id, secret = select_device_code_client(
            device_client_id="dev-id",
            device_client_secret="dev-secret",
            client_id="ord-id",
            client_secret="ord-secret",
            challenge_client_id="chal-id",
        )
        assert (client_id, secret) == ("dev-id", "dev-secret")

    def test_ordinary_client_wins_over_challenge(self) -> None:
        """With no device client, the ordinary resource-metadata client wins over the challenge's."""
        client_id, secret = select_device_code_client(
            device_client_id="",
            device_client_secret="",
            client_id="ord-id",
            client_secret="ord-secret",
            challenge_client_id="chal-id",
        )
        assert (client_id, secret) == ("ord-id", "ord-secret")

    def test_challenge_client_id_is_last_resort(self) -> None:
        """With neither device nor ordinary client_id set, the challenge's client_id is used."""
        client_id, secret = select_device_code_client(
            device_client_id="",
            device_client_secret="",
            client_id="",
            client_secret="ord-secret",  # secret still comes from resource metadata, never the challenge
            challenge_client_id="chal-id",
        )
        assert (client_id, secret) == ("chal-id", "ord-secret")


class TestEnvironmentDetection:
    """Tests for is_headless_environment / is_colab_environment."""

    def test_ssh_connection_is_headless(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SSH_CONNECTION alone marks the environment as headless."""
        monkeypatch.setenv("SSH_CONNECTION", "1.2.3.4 1 5.6.7.8 22")
        assert is_headless_environment()

    def test_ci_true_is_headless(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CI=true alone marks the environment as headless."""
        monkeypatch.setenv("CI", "true")
        assert is_headless_environment()

    def test_ci_other_value_not_headless_by_itself(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CI set to anything other than "true" doesn't trip headless detection by itself."""
        monkeypatch.delenv("SSH_CONNECTION", raising=False)
        monkeypatch.delenv("SSH_CLIENT", raising=False)
        monkeypatch.setenv("CI", "false")
        monkeypatch.delenv("DOCKER_CONTAINER", raising=False)
        monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
        monkeypatch.setenv("DISPLAY", ":0")
        assert not is_headless_environment()

    def test_colab_release_tag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """COLAB_RELEASE_TAG alone marks the environment as Colab."""
        monkeypatch.setenv("COLAB_RELEASE_TAG", "release-1")
        assert is_colab_environment()

    def test_no_colab_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With none of the three Colab env vars set, is_colab_environment() is False."""
        monkeypatch.delenv("COLAB_RELEASE_TAG", raising=False)
        monkeypatch.delenv("COLAB_GPU", raising=False)
        monkeypatch.delenv("COLAB_JUPYTER_IP", raising=False)
        assert not is_colab_environment()


class TestParseOAuthChallenge:
    """Tests for parse_oauth_challenge."""

    def test_valid_challenge(self) -> None:
        """A well-formed Bearer challenge parses its resource_metadata and client_id."""
        header = f'Bearer resource_metadata="{RESOURCE_METADATA_URL}", client_id="my-app"'
        challenge = parse_oauth_challenge(header)
        assert challenge is not None
        assert challenge.resource_metadata_url == RESOURCE_METADATA_URL
        assert challenge.client_id == "my-app"

    def test_missing_resource_metadata_returns_none(self) -> None:
        """A Bearer challenge with no resource_metadata param is not a challenge this module acts on."""
        assert parse_oauth_challenge('Bearer client_id="my-app"') is None

    def test_non_bearer_scheme_returns_none(self) -> None:
        """A non-Bearer auth scheme is never treated as an OAuth challenge."""
        assert parse_oauth_challenge(f'Basic resource_metadata="{RESOURCE_METADATA_URL}"') is None

    def test_empty_header_returns_none(self) -> None:
        """An empty header returns None rather than raising."""
        assert parse_oauth_challenge("") is None


class TestPerformDeviceCodeFlow:
    """Tests for the RFC 8628 device-code polling loop."""

    def test_success(self) -> None:
        """A clean success returns the granted token and the device-selected refresh context."""
        idp = MockIdp()
        idp.granted_token = "granted-access-token"
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, refresh_ctx = perform_device_code_flow(
            challenge,
            _resource_meta(),
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=5.0,
        )
        assert tokens.access_token == "granted-access-token"
        assert idp.device_auth_calls == 1
        # Device polling goes straight to the real token endpoint, never a proxy.
        assert idp.token_calls_by_url == {TOKEN_URL: 1}
        # The device client (not the ordinary one) was used, and its secret
        # rides the poll -- unconditionally, since there's no proxy here.
        assert refresh_ctx.client_id == "device-client"
        assert refresh_ctx.client_secret == "device-secret"

    def test_authorization_pending_then_success(self) -> None:
        """authorization_pending is retried silently until the IdP grants the token."""
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "authorization_pending"})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, _ = perform_device_code_flow(
            challenge,
            _resource_meta(),
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=5.0,
        )
        assert tokens.access_token == idp.granted_token
        assert idp.token_calls == 2

    def test_slow_down_via_body_increases_interval_and_eventually_succeeds(self) -> None:
        """A slow_down error body widens the poll interval, then the flow still succeeds."""
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "slow_down"})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, _ = perform_device_code_flow(
            challenge,
            _resource_meta(),
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=15.0,  # slow_down adds 5s to the poll interval -- needs headroom
        )
        assert tokens.access_token == idp.granted_token
        assert idp.token_calls == 2

    def test_slow_down_via_429_status(self) -> None:
        """A plain HTTP 429 (no error body needed) is treated the same as slow_down."""
        idp = MockIdp()
        idp.token_responses = [(429, {})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, _ = perform_device_code_flow(
            challenge,
            _resource_meta(),
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=15.0,
        )
        assert tokens.access_token == idp.granted_token

    def test_expired_token_raises(self) -> None:
        """expired_token raises OAuthTokenError with that structured error_code."""
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "expired_token"})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        with pytest.raises(OAuthTokenError, match="expired") as excinfo:
            perform_device_code_flow(
                challenge,
                _resource_meta(),
                _server_meta(),
                http_client=httpx2.Client(transport=idp.transport()),
                timeout_seconds=5.0,
            )
        assert excinfo.value.error_code == "expired_token"

    def test_access_denied_raises(self) -> None:
        """access_denied raises OAuthTokenError with that structured error_code."""
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "access_denied"})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        with pytest.raises(OAuthTokenError, match="denied") as excinfo:
            perform_device_code_flow(
                challenge,
                _resource_meta(),
                _server_meta(),
                http_client=httpx2.Client(transport=idp.transport()),
                timeout_seconds=5.0,
            )
        assert excinfo.value.error_code == "access_denied"

    def test_5xx_then_recovery_stays_under_retry_cap(self) -> None:
        """A couple of 5xx responses are retried and don't trip the network-retry cap."""
        idp = MockIdp()
        idp.token_responses = [(500, {}), (502, {})]
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, _ = perform_device_code_flow(
            challenge,
            _resource_meta(),
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=5.0,
        )
        assert tokens.access_token == idp.granted_token

    def test_exceeding_retry_cap_raises(self) -> None:
        """More 5xx responses than the retry cap allows raises a clear OAuthTokenError."""
        idp = MockIdp()
        idp.token_responses = [(500, {})] * 5  # more than _MAX_NETWORK_RETRIES
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        with pytest.raises(OAuthTokenError, match="retries"):
            perform_device_code_flow(
                challenge,
                _resource_meta(),
                _server_meta(),
                http_client=httpx2.Client(transport=idp.transport()),
                timeout_seconds=5.0,
            )

    def test_secret_less_proxy_never_used_for_device_polling(self) -> None:
        """Device-code polling always targets the real token endpoint, never a proxy.

        The vgi-rpc token-exchange proxy only forwards authorization_code/
        refresh_token grants (see its own docstring) -- routing a
        device_code grant through it would be rejected outright.
        """
        idp = MockIdp()
        resource_meta = _resource_meta(token_endpoint=PROXY_TOKEN_URL)
        challenge = parse_oauth_challenge(CHALLENGE_HEADER)
        assert challenge is not None
        tokens, refresh_ctx = perform_device_code_flow(
            challenge,
            resource_meta,
            _server_meta(),
            http_client=httpx2.Client(transport=idp.transport()),
            timeout_seconds=5.0,
        )
        assert tokens.access_token == idp.granted_token
        assert idp.token_calls_by_url == {TOKEN_URL: 1}  # never PROXY_TOKEN_URL
        # But the refresh context records the proxy, for *future* silent refresh.
        assert refresh_ctx.token_endpoint == PROXY_TOKEN_URL
        assert refresh_ctx.client_secret == ""  # never carried locally once a proxy exists


class TestAttemptTokenRefresh:
    """Tests for attempt_token_refresh."""

    def test_success(self) -> None:
        """A successful refresh returns the newly granted access token."""
        idp = MockIdp()
        idp.granted_token = "refreshed-token"
        ctx = OAuthRefreshContext(token_endpoint=TOKEN_URL, client_id="cid", client_secret="csecret")
        tokens = attempt_token_refresh(ctx, "old-refresh-token", http_client=httpx2.Client(transport=idp.transport()))
        assert tokens.access_token == "refreshed-token"

    def test_omitted_refresh_token_preserves_old_one(self) -> None:
        """If the refresh response omits refresh_token, the caller's old one is preserved."""
        idp = MockIdp()
        idp.token_responses = [(200, {"access_token": "new-access"})]  # no refresh_token in the body
        ctx = OAuthRefreshContext(token_endpoint=TOKEN_URL, client_id="cid")
        tokens = attempt_token_refresh(ctx, "old-refresh-token", http_client=httpx2.Client(transport=idp.transport()))
        assert tokens.refresh_token == "old-refresh-token"

    def test_invalid_grant_is_structured_not_string_matched(self) -> None:
        """invalid_grant surfaces as a structured .error_code, not text buried in the message."""
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "invalid_grant", "error_description": "Token expired"})]
        ctx = OAuthRefreshContext(token_endpoint=TOKEN_URL, client_id="cid")
        with pytest.raises(OAuthTokenError) as excinfo:
            attempt_token_refresh(ctx, "dead-refresh-token", http_client=httpx2.Client(transport=idp.transport()))
        assert excinfo.value.error_code == "invalid_grant"

    def test_secret_never_leaked_into_error_message(self) -> None:
        """A refresh failure's exception text never contains the raw client_secret."""
        idp = MockIdp()
        idp.token_responses = [(500, {})] * 10  # force a raw (non-JSON-error) failure path
        ctx = OAuthRefreshContext(token_endpoint=TOKEN_URL, client_id="cid", client_secret="super-secret-value")
        with pytest.raises(OAuthTokenError) as excinfo:
            attempt_token_refresh(ctx, "refresh-tok", http_client=httpx2.Client(transport=idp.transport()))
        assert "super-secret-value" not in str(excinfo.value)


class TestVgiOAuthAuthEndToEnd:
    """End-to-end tests: VgiOAuthAuth attached to a real httpx2.Client, driving sync_auth_flow."""

    def _protected_client(self, idp: MockIdp, auth: VgiOAuthAuth) -> httpx2.Client:
        """Build the caller-facing client under test, sharing the same mock IdP."""
        return httpx2.Client(transport=idp.transport(), auth=auth, base_url="https://api.example.com")

    def test_device_code_login_then_cached_reuse(self) -> None:
        """A 401 triggers a device-code login; a second request reuses the cached token."""
        idp = MockIdp()
        idp.granted_token = "device-flow-token"
        auth = VgiOAuthAuth(
            base_url="https://api.example.com",
            flow="device_code",
            timeout_seconds=5.0,
            transport=idp.transport(),
        )
        client = self._protected_client(idp, auth)
        try:
            resp1 = client.get("/protected")
            assert resp1.status_code == 200
            assert resp1.json()["authorization"] == "Bearer device-flow-token"
            calls_after_first = idp.device_auth_calls

            resp2 = client.get("/protected")
            assert resp2.status_code == 200
            # No second device-code flow -- the cached token was reused.
            assert idp.device_auth_calls == calls_after_first
        finally:
            client.close()
            auth.close()

    def test_pkce_flow_raises_not_implemented(self) -> None:
        """Explicitly requesting flow="pkce" raises a clear NotImplementedError (slice 2, unshipped)."""
        idp = MockIdp()
        auth = VgiOAuthAuth(
            base_url="https://api.example.com", flow="pkce", timeout_seconds=5.0, transport=idp.transport()
        )
        client = self._protected_client(idp, auth)
        try:
            with pytest.raises(NotImplementedError, match="PKCE"):
                client.get("/protected")
        finally:
            client.close()
            auth.close()

    def test_auto_mode_with_only_pkce_endpoint_raises_actionable_error(self) -> None:
        """Auto mode against a PKCE-only server raises a specific, actionable NotImplementedError."""
        idp = MockIdp(server_metadata={"authorization_endpoint": AUTHORIZATION_ENDPOINT, "token_endpoint": TOKEN_URL})
        auth = VgiOAuthAuth(
            base_url="https://api.example.com", flow="auto", timeout_seconds=5.0, transport=idp.transport()
        )
        client = self._protected_client(idp, auth)
        try:
            with pytest.raises(NotImplementedError, match="oauth_refresh_token"):
                client.get("/protected")
        finally:
            client.close()
            auth.close()

    def test_auto_mode_picks_device_code_when_available(self) -> None:
        """Auto mode against a server offering both flows picks device-code (the implemented one)."""
        idp = MockIdp()
        auth = VgiOAuthAuth(
            base_url="https://api.example.com", flow="auto", timeout_seconds=5.0, transport=idp.transport()
        )
        client = self._protected_client(idp, auth)
        try:
            resp = client.get("/protected")
            assert resp.status_code == 200
        finally:
            client.close()
            auth.close()

    def test_seeded_refresh_token_skips_interactive_login(self) -> None:
        """A pre-seeded refresh_token silently refreshes instead of running an interactive flow."""
        idp = MockIdp()
        idp.granted_token = "refreshed-not-interactive"
        auth = VgiOAuthAuth(
            base_url="https://api.example.com",
            refresh_token="pre-seeded-refresh-token",
            timeout_seconds=5.0,
            transport=idp.transport(),
        )
        client = self._protected_client(idp, auth)
        try:
            resp = client.get("/protected")
            assert resp.status_code == 200
            assert resp.json()["authorization"] == "Bearer refreshed-not-interactive"
            assert idp.device_auth_calls == 0  # never ran an interactive flow
            assert not auth.was_interactive()
        finally:
            client.close()
            auth.close()

    def test_invalid_grant_on_refresh_fails_closed_then_next_request_logs_in_fresh(self) -> None:
        """A dead refresh_token fails the request outright (never silently falls back mid-request).

        Matches the C++ reference's own documented behavior: a refresh
        failure is rethrown immediately, not masked behind a fresh login in
        the same call. The *next* 401 (a separate request) then finds the
        refresh_token cleared and runs a real interactive flow instead.
        """
        idp = MockIdp()
        idp.token_responses = [(400, {"error": "invalid_grant"})]
        idp.granted_token = "fresh-interactive-token"
        auth = VgiOAuthAuth(
            base_url="https://api.example.com",
            refresh_token="dead-refresh-token",
            flow="device_code",
            timeout_seconds=5.0,
            transport=idp.transport(),
        )
        client = self._protected_client(idp, auth)
        try:
            with pytest.raises(OAuthTokenError, match="invalid_grant"):
                client.get("/protected")
            assert idp.device_auth_calls == 0  # first request never reached an interactive flow

            resp = client.get("/protected")
            assert resp.status_code == 200
            assert resp.json()["authorization"] == "Bearer fresh-interactive-token"
            assert auth.was_interactive()
            assert idp.device_auth_calls == 1
        finally:
            client.close()
            auth.close()

    def test_concurrent_401s_run_exactly_one_flow(self) -> None:
        """Three threads racing the same 401 at once trigger exactly one device-code flow."""
        idp = MockIdp()
        call_lock = threading.Lock()
        original_handler = idp.handler
        release = threading.Event()
        entered_device_auth = threading.Event()

        def slow_handler(request: httpx2.Request) -> httpx2.Response:
            if str(request.url) == DEVICE_AUTH_URL:
                entered_device_auth.set()
                release.wait(timeout=5.0)
            return original_handler(request)

        idp.transport = lambda: httpx2.MockTransport(slow_handler)  # type: ignore[method-assign]
        auth = VgiOAuthAuth(
            base_url="https://api.example.com", flow="device_code", timeout_seconds=5.0, transport=idp.transport()
        )
        client = self._protected_client(idp, auth)
        results: list[int] = []

        def worker() -> None:
            resp = client.get("/protected")
            with call_lock:
                results.append(resp.status_code)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        assert entered_device_auth.wait(timeout=5.0)
        time.sleep(0.05)  # let the other threads pile up on the condition variable
        release.set()
        for t in threads:
            t.join(timeout=5.0)

        assert results == [200, 200, 200]
        assert idp.device_auth_calls == 1  # exactly one flow ran, not three
        client.close()
        auth.close()

    def test_clear_tokens_resets_to_idle(self) -> None:
        """clear_tokens() forces the next request to log in again from scratch."""
        idp = MockIdp()
        auth = VgiOAuthAuth(
            base_url="https://api.example.com", flow="device_code", timeout_seconds=5.0, transport=idp.transport()
        )
        client = self._protected_client(idp, auth)
        try:
            client.get("/protected")
            auth.clear_tokens()
            resp = client.get("/protected")
            assert resp.status_code == 200
            assert idp.device_auth_calls == 2  # logged in again from scratch
        finally:
            client.close()
            auth.close()
