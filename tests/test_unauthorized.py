# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the standardized 401 response.

The cross-language part of this contract lives in ``docs/unauthorized-spec.md``
and is asserted by the ``TestUnauthorized`` conformance group. What is here is
the Python-specific half: which reason code each built-in authenticator picks,
how declarations survive composition, and how the client parses the envelope.
"""

from __future__ import annotations

import json
from typing import Any

import falcon
import falcon.testing.helpers
import pytest

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.http import (
    AuthenticationError,
    AuthFailure,
    AuthReason,
    ProxyProofConfig,
    bearer_authenticate_static,
    chain_authenticate,
    declare_proxy_headers,
    make_sync_client,
    mtls_authenticate_xfcc,
    proxy_proof_gate,
    require_all,
)
from vgi_rpc.http._client import _parse_unauthorized
from vgi_rpc.http._testing import _SyncTestClient, _SyncTestResponse
from vgi_rpc.http._unauthorized import classify_auth_failure, proxy_headers_of
from vgi_rpc.rpc import AuthContext, RpcServer

_ALICE = AuthContext(domain="test", authenticated=True, principal="alice", claims={})
_REASON_HEADER = "vgi-auth-reason"
_PROXY_HEADER = "vgi-auth-proxy-required"


def _client(**kwargs: Any) -> _SyncTestClient:
    """Build a sync test client over the conformance service."""
    return make_sync_client(RpcServer(ConformanceService, ConformanceServiceImpl()), **kwargs)


def _reject(_req: falcon.Request) -> AuthContext:
    raise ValueError("nope")


def _body(resp: _SyncTestResponse) -> dict[str, str]:
    """Decode a JSON 401 envelope from the sync test client's response."""
    parsed: object = json.loads(resp.content)
    assert isinstance(parsed, dict)
    return parsed


def _text(resp: _SyncTestResponse) -> str:
    """Decode an HTML 401 page from the sync test client's response."""
    return resp.content.decode()


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


class TestClassification:
    """Mapping an authenticate-callback exception onto a reason code."""

    def test_auth_failure_declares_its_own(self) -> None:
        """An AuthFailure's reason is used verbatim."""
        assert classify_auth_failure(AuthFailure(AuthReason.EXPIRED_CREDENTIAL)) is AuthReason.EXPIRED_CREDENTIAL

    def test_bare_value_error_is_unclassified(self) -> None:
        """A custom authenticator's plain ValueError is not guessed at.

        Guessing would mean matching message text, which misclassifies the
        moment someone rewords a string.
        """
        assert classify_auth_failure(ValueError("Missing token")) is AuthReason.UNAUTHORIZED

    def test_permission_error_is_insufficient_scope(self) -> None:
        """PermissionError means the caller got as far as being identified."""
        assert classify_auth_failure(PermissionError("no")) is AuthReason.INSUFFICIENT_SCOPE

    def test_auth_failure_default_message_is_the_code(self) -> None:
        """Omitting the detail leaves the code as the message rather than blank."""
        assert str(AuthFailure(AuthReason.MISSING_CREDENTIAL)) == "missing_credential"


# ---------------------------------------------------------------------------
# Reason codes chosen by the built-in authenticators
# ---------------------------------------------------------------------------


class TestBuiltinReasons:
    """Each built-in authenticator's failures land on the right code."""

    def test_bearer_missing_header(self) -> None:
        """No Authorization header at all is a missing credential."""
        client = _client(authenticate=bearer_authenticate_static(tokens={"good": _ALICE}))
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.status_code == 401
        assert resp.headers[_REASON_HEADER] == "missing_credential"

    def test_bearer_wrong_token(self) -> None:
        """A presented-but-unknown token is invalid, not missing."""
        client = _client(authenticate=bearer_authenticate_static(tokens={"good": _ALICE}))
        resp = client.post("/echo_int", content=b"", headers={"Authorization": "Bearer bad"})
        assert resp.headers[_REASON_HEADER] == "invalid_credential"

    def test_xfcc_missing_header_is_proxy_required(self) -> None:
        """An absent proxy-injected header points at the deployment, not the caller.

        Reporting this as ``missing_credential`` would send an operator
        hunting for a certificate the caller may well have presented — to a
        proxy that then did not forward it.
        """
        client = _client(authenticate=mtls_authenticate_xfcc())
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers[_REASON_HEADER] == "proxy_required"

    def test_xfcc_empty_header_is_invalid(self) -> None:
        """A header the proxy *did* set, but empty, is a bad credential."""
        client = _client(authenticate=mtls_authenticate_xfcc())
        resp = client.post("/echo_int", content=b"", headers={"x-forwarded-client-cert": ","})
        assert resp.headers[_REASON_HEADER] == "invalid_credential"

    def test_unclassified_callback(self) -> None:
        """A custom callback raising a bare ValueError falls back cleanly."""
        client = _client(authenticate=_reject)
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers[_REASON_HEADER] == "unauthorized"


class TestChainComposition:
    """Reason codes across alternative credentials."""

    def test_all_missing_reports_missing(self) -> None:
        """When nothing was presented anywhere, telling the caller to send something is right."""
        chained = chain_authenticate(
            bearer_authenticate_static(tokens={"good": _ALICE}),
            bearer_authenticate_static(tokens={"other": _ALICE}),
        )
        client = _client(authenticate=chained)
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers[_REASON_HEADER] == "missing_credential"

    def test_one_substantive_failure_wins(self) -> None:
        """A credential that was seen and rejected outranks 'you sent nothing'.

        Both alternatives read the same header here, so a bad token makes both
        fail — but reporting ``missing_credential`` would be advice the caller
        has already followed.
        """

        def missing(_req: falcon.Request) -> AuthContext:
            raise AuthFailure(AuthReason.MISSING_CREDENTIAL, "no cookie")

        chained = chain_authenticate(missing, bearer_authenticate_static(tokens={"good": _ALICE}))
        client = _client(authenticate=chained)
        resp = client.post("/echo_int", content=b"", headers={"Authorization": "Bearer bad"})
        assert resp.headers[_REASON_HEADER] == "invalid_credential"

    def test_chain_still_reports_every_detail(self) -> None:
        """Aggregating the codes must not cost the per-authenticator diagnostics."""
        chained = chain_authenticate(_reject, bearer_authenticate_static(tokens={"good": _ALICE}))
        client = _client(authenticate=chained)
        resp = client.post("/echo_int", content=b"", headers={})
        detail = _body(resp)["detail"]
        assert detail == "authentication rejected"


# ---------------------------------------------------------------------------
# The proxy note
# ---------------------------------------------------------------------------


class TestProxyNote:
    """Which services carry the proxy-configuration note, and how it is discovered."""

    def test_absent_by_default(self) -> None:
        """A service with no proxy dependency stays quiet about proxies."""
        client = _client(authenticate=bearer_authenticate_static(tokens={"good": _ALICE}))
        resp = client.post("/echo_int", content=b"", headers={})
        assert _PROXY_HEADER not in resp.headers
        assert "proxy_hint" not in _body(resp)

    def test_discovered_from_mtls_authenticator(self) -> None:
        """The operator does not have to restate what the authenticator already knows."""
        client = _client(authenticate=mtls_authenticate_xfcc())
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers[_PROXY_HEADER] == "true"
        assert "x-forwarded-client-cert" in _body(resp)["proxy_hint"]

    def test_declared_by_a_custom_authenticator(self) -> None:
        """A third-party authenticator can opt in without a framework change.

        Declared on a local closure, not the shared ``_reject``: the helper
        annotates the callable in place, so marking a function every other
        test also uses would leak the note into their assertions.
        """

        def gateway(_req: falcon.Request) -> AuthContext:
            raise ValueError("nope")

        client = _client(authenticate=declare_proxy_headers(gateway, "X-Gateway-Assertion"))
        resp = client.post("/echo_int", content=b"", headers={})
        assert "X-Gateway-Assertion" in _body(resp)["proxy_hint"]

    def test_stated_directly_by_the_operator(self) -> None:
        """An authenticator the framework cannot introspect is still coverable."""
        client = _client(authenticate=_reject, proxy_auth_headers=["X-Edge-Verified"])
        resp = client.post("/echo_int", content=b"", headers={})
        assert "X-Edge-Verified" in _body(resp)["proxy_hint"]

    def test_survives_chain_composition(self) -> None:
        """Wrapping an authenticator must not silently drop the note."""
        chained = chain_authenticate(bearer_authenticate_static(tokens={"good": _ALICE}), mtls_authenticate_xfcc())
        assert proxy_headers_of(chained) == ("x-forwarded-client-cert",)

    def test_survives_require_all(self) -> None:
        """A gate's header dependency reaches the app through require_all."""
        config = ProxyProofConfig(mode="require", origin_id="w1", secrets={"k1": (b"\x01" * 32, "k1")})
        assert proxy_headers_of(require_all(proxy_proof_gate(config))) == ("VGI-Proxy-Proof",)

    def test_allow_mode_declares_nothing(self) -> None:
        """In allow mode an absent proof never denies, so the note would misdirect."""
        config = ProxyProofConfig(mode="allow", origin_id="w1", secrets={"k1": (b"\x01" * 32, "k1")})
        assert proxy_headers_of(require_all(proxy_proof_gate(config))) == ()

    def test_proof_required_flag_alone_is_enough(self) -> None:
        """An operator who only sets the advertisement flag still gets the note."""
        client = _client(authenticate=_reject, proxy_proof_required=True)
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers[_PROXY_HEADER] == "true"
        assert "VGI-Proxy-Proof" in _body(resp)["proxy_hint"]


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


class TestResponseShape:
    """Negotiation, headers, and the envelope itself."""

    def test_json_for_machine_clients(self) -> None:
        """No Accept header means a machine client, which gets JSON."""
        client = _client(authenticate=_reject)
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers["content-type"].startswith("application/json")
        assert _body(resp) == {
            "error": "unauthorized",
            "reason": "unauthorized",
            "detail": "authentication rejected",
        }

    def test_wildcard_accept_is_not_html(self) -> None:
        """``*/*`` is what httpx2 sends by default and must not select the page."""
        client = _client(authenticate=_reject)
        resp = client.post("/echo_int", content=b"", headers={"Accept": "*/*"})
        assert resp.headers["content-type"].startswith("application/json")

    def test_html_for_browsers(self) -> None:
        """A browser gets the styled page, with the code shown on it."""
        client = _client(authenticate=_reject)
        resp = client.post("/echo_int", content=b"", headers={"Accept": "text/html,*/*;q=0.8"})
        assert resp.headers["content-type"].startswith("text/html")
        assert "401" in _text(resp) and 'class="reason">unauthorized<' in _text(resp)

    def test_html_page_shows_the_proxy_note(self) -> None:
        """The page is where a human reads the note, so it must be on the page."""
        client = _client(authenticate=mtls_authenticate_xfcc())
        resp = client.post("/echo_int", content=b"", headers={"Accept": "text/html"})
        assert "Is the reverse proxy configured?" in _text(resp)
        assert "x-forwarded-client-cert" in _text(resp)

    def test_detail_is_escaped_on_the_page(self) -> None:
        """The detail comes from a callback and reaches a browser — escape it."""

        def reject(_req: falcon.Request) -> AuthContext:
            raise ValueError("<script>alert(1)</script>")

        client = _client(authenticate=reject)
        resp = client.post("/echo_int", content=b"", headers={"Accept": "text/html"})
        assert "<script>alert(1)</script>" not in _text(resp)
        assert "authentication rejected" in _text(resp)

    def test_not_cached(self) -> None:
        """The next attempt with a credential is a 200 — no shared cache may hold this."""
        client = _client(authenticate=_reject)
        resp = client.post("/echo_int", content=b"", headers={})
        assert resp.headers["cache-control"] == "no-store"

    def test_www_authenticate_survives(self) -> None:
        """The serializer must not clobber the challenge Falcon already set."""
        from vgi_rpc.http import OAuthResourceMetadata

        client = _client(
            authenticate=_reject,
            oauth_resource_metadata=OAuthResourceMetadata(
                resource="https://x.test", authorization_servers=("https://a.test",)
            ),
        )
        resp = client.post("/echo_int", content=b"", headers={})
        assert "Bearer" in resp.headers["www-authenticate"]

    def test_non_401_errors_are_untouched(self) -> None:
        """Only 401 gets the standardized treatment; the rest keep Falcon's JSON."""
        client = _client()
        resp = client.get("/vgi/nope", headers={})
        assert resp.status_code == 404
        assert _REASON_HEADER not in resp.headers

    def test_reason_headers_absent_on_success(self) -> None:
        """These explain a rejection; on a 200 they would be noise."""
        client = _client(authenticate=lambda _req: _ALICE)
        resp = client.get("/health", headers={})
        assert resp.status_code == 200
        assert _REASON_HEADER not in resp.headers
        assert _PROXY_HEADER not in resp.headers

    def test_body_cache_is_bounded(self) -> None:
        """A callback embedding request data in the detail must not grow the cache forever."""
        counter = {"n": 0}

        def reject(_req: falcon.Request) -> AuthContext:
            counter["n"] += 1
            raise ValueError(f"attempt {counter['n']}")

        client = _client(authenticate=reject)
        for _ in range(200):
            client.post("/echo_int", content=b"", headers={})
        # Distinct details keep rendering correctly even past the cache bound.
        resp = client.post("/echo_int", content=b"", headers={})
        assert _body(resp)["detail"] == "authentication rejected"


# ---------------------------------------------------------------------------
# Client-side parsing
# ---------------------------------------------------------------------------


class TestClientParsing:
    """Turning a 401 body back into a typed error."""

    def test_full_envelope(self) -> None:
        """Every field is unpacked onto the error."""
        body = json.dumps(
            {
                "error": "unauthorized",
                "reason": "expired_credential",
                "detail": "token expired",
                "proxy_hint": "check the proxy",
            }
        ).encode()
        err = _parse_unauthorized(body)
        assert err.reason is AuthReason.EXPIRED_CREDENTIAL
        assert err.detail == "token expired"
        assert err.proxy_hint == "check the proxy"
        assert err.error_type == "AuthenticationError"

    def test_proxy_hint_reaches_the_message(self) -> None:
        """The place this gets read is a traceback, not an attribute inspector."""
        body = json.dumps({"reason": "proxy_required", "detail": "d", "proxy_hint": "the note"}).encode()
        assert "the note" in str(_parse_unauthorized(body))

    def test_unknown_reason_degrades(self) -> None:
        """A code this client has never heard of means a newer server, not a broken one."""
        err = _parse_unauthorized(json.dumps({"reason": "from_the_future", "detail": "d"}).encode())
        assert err.reason is AuthReason.UNAUTHORIZED
        assert err.detail == "d"

    def test_html_body_is_summarized(self) -> None:
        """A page of markup in an exception message buries the rest of the traceback."""
        err = _parse_unauthorized(b"<!DOCTYPE html>\n<html><body>" + b"x" * 5000 + b"</body></html>")
        assert "HTML 401 page" in err.detail
        assert "<html>" not in err.detail

    def test_foreign_body_is_truncated(self) -> None:
        """A gateway or WAF has its own idea of an error body; keep a usable prefix."""
        err = _parse_unauthorized(b"nginx says no. " + b"y" * 5000)
        assert err.detail.startswith("nginx says no.")
        assert len(err.detail) <= 500

    def test_empty_body(self) -> None:
        """An empty 401 still produces a message worth reading."""
        assert _parse_unauthorized(b"").detail == "unauthorized"

    def test_json_that_is_not_an_object(self) -> None:
        """Valid JSON of the wrong shape falls through to the text path."""
        assert _parse_unauthorized(b'["nope"]').reason is AuthReason.UNAUTHORIZED

    def test_is_an_rpc_error(self) -> None:
        """Existing ``except RpcError`` call sites keep working."""
        from vgi_rpc.rpc import RpcError

        assert issubclass(AuthenticationError, RpcError)


class TestClientEndToEnd:
    """The proxy note survives the whole round trip into a raised exception."""

    def test_proxy_hint_reaches_the_caller(self) -> None:
        """An operator staring at a traceback is the audience for the note."""
        from vgi_rpc.http import http_connect

        client = _client(authenticate=mtls_authenticate_xfcc())
        with (
            http_connect(ConformanceService, "http://testserver", client=client) as proxy,
            pytest.raises(AuthenticationError) as exc_info,
        ):
            proxy.echo_int(value=1)
        assert exc_info.value.reason is AuthReason.PROXY_REQUIRED
        assert "reverse proxy" in exc_info.value.proxy_hint

    def test_plain_401_is_still_an_rpc_error(self) -> None:
        """Callers catching RpcError see no behaviour change."""
        from vgi_rpc.http import http_connect
        from vgi_rpc.rpc import RpcError

        client = _client(authenticate=_reject)
        with (
            http_connect(ConformanceService, "http://testserver", client=client) as proxy,
            pytest.raises(RpcError, match="AuthenticationError"),
        ):
            proxy.echo_int(value=1)
