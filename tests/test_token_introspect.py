# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the token-introspection endpoint.

Distinct from ``test_introspect.py``, which covers ``__describe__``.  This
endpoint resolves an opaque bearer credential to a principal for a reverse
proxy that must know the caller's identity before it can authorize.  Its output
steers privileges the worker does not itself hold, so most of what is asserted
here is what it refuses to do.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

import falcon
import falcon.testing
import pytest

from vgi_rpc.http import AuthUnavailableError, chain_authenticate
from vgi_rpc.http._testing import make_sync_client
from vgi_rpc.http.server._introspect import (
    INTROSPECT_ENABLED_HEADER,
    TokenIdentity,
    _RateLimiter,
    token_digest,
)
from vgi_rpc.rpc import AuthContext, RpcServer

_SUBJECT = "opaque-subject-credential-value"
_PROXY = "proxy@example.com"


class _Svc(Protocol):
    """Minimal protocol; introspection is orthogonal to the RPC surface."""

    def ping(self) -> str:
        """Return pong."""
        ...


class _Impl:
    """Reference impl."""

    def ping(self) -> str:
        """Return pong."""
        return "pong"


_IDENTITIES = {_SUBJECT: TokenIdentity(principal="alice@example.com", token_name="laptop", ttl_seconds=300)}


def _resolver(token: str) -> TokenIdentity | None:
    """Resolve only the one known subject credential."""
    return _IDENTITIES.get(token)


def _authenticate(req: falcon.Request) -> AuthContext:
    """Treat the bearer value as the principal — enough to exercise the allowlist."""
    who = (req.get_header("Authorization") or "").removeprefix("Bearer ").strip()
    if not who:
        raise ValueError("no credential")
    return AuthContext(domain="test", authenticated=True, principal=who)


def _client(**kwargs: Any) -> Any:
    """Build a sync test client with introspection enabled unless overridden."""
    kwargs.setdefault("introspect_resolver", _resolver)
    kwargs.setdefault("introspect_principals", [_PROXY])
    return make_sync_client(
        RpcServer(_Svc, _Impl()),
        token_key=b"test-key",
        authenticate=_authenticate,
        **kwargs,
    )


def _post(client: Any, caller: str | None, body: object) -> Any:
    """POST *body* to the introspection endpoint as *caller*."""
    headers = {"Content-Type": "application/json"}
    if caller is not None:
        headers["Authorization"] = f"Bearer {caller}"
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return client.post("http://x/__introspect_token__", content=raw, headers=headers)


class TestResolution:
    """The happy path, and what the response is allowed to contain."""

    def test_valid_credential_resolves(self) -> None:
        """A known credential yields its principal."""
        c = _client()
        try:
            resp = _post(c, _PROXY, {"token": _SUBJECT})
            assert resp.status_code == 200
            body = json.loads(resp.content)
            assert body["principal"] == "alice@example.com"
            assert body["token_name"] == "laptop"
            assert body["ttl_seconds"] == 300
        finally:
            c.close()

    def test_response_carries_no_claims(self) -> None:
        """The single most dangerous field this feature could grow.

        A pass-through claims field would let a worker choose its caller's
        tenant routing, row scope, and policy branch. The response is a closed
        set of three keys, asserted as a set so a future addition has to come
        through this test.
        """
        c = _client()
        try:
            body = json.loads(_post(c, _PROXY, {"token": _SUBJECT}).content)
            assert set(body) == {"principal", "token_name", "ttl_seconds"}
        finally:
            c.close()

    def test_ttl_is_usable_by_a_caching_caller(self) -> None:
        """``ttl_seconds`` exists so the caller caches; it must be a live number."""
        c = _client()
        try:
            ttl = json.loads(_post(c, _PROXY, {"token": _SUBJECT}).content)["ttl_seconds"]
            assert isinstance(ttl, int)
            assert ttl > 0
        finally:
            c.close()

    def test_response_is_not_cacheable_by_an_intermediary(self) -> None:
        """The caller caches on its own terms; a shared cache must not."""
        c = _client()
        try:
            resp = _post(c, _PROXY, {"token": _SUBJECT})
            assert "no-store" in resp.headers.get("cache-control", "")
        finally:
            c.close()


class TestOffByDefault:
    """No worker grows a credential-to-identity oracle by upgrading."""

    def test_absent_without_a_resolver(self) -> None:
        """A worker that configured nothing resolves nothing."""
        c = make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", authenticate=_authenticate)
        try:
            resp = _post(c, _PROXY, {"token": _SUBJECT})
            assert resp.status_code == 404
            assert json.loads(resp.content)["error"] == "not_enabled"
        finally:
            c.close()

    def test_disabled_is_definitive_not_transient(self) -> None:
        """A caller must stop, not spin.

        Left to the generic ``{method}`` route this path answers 415 for a
        JSON body, which a caller classifying 401/403/404 as definitive reads
        as "retry later" — so pointing a proxy at a worker without the feature
        would retry forever instead of failing at preflight.
        """
        c = make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", authenticate=_authenticate)
        try:
            assert _post(c, _PROXY, {"token": _SUBJECT}).status_code in (401, 403, 404)
        finally:
            c.close()

    def test_disabled_worker_does_not_advertise(self) -> None:
        """The preflight signal: absent header means do not configure this."""
        c = make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k")
        try:
            resp = c._client.simulate_options("/health")
            assert INTROSPECT_ENABLED_HEADER.lower() not in resp.headers
        finally:
            c.close()

    def test_enabled_worker_advertises(self) -> None:
        """A proxy can preflight at boot rather than at first login."""
        c = _client()
        try:
            resp = c._client.simulate_options("/health")
            assert resp.headers[INTROSPECT_ENABLED_HEADER.lower()] == "true"
        finally:
            c.close()

    def test_allowlist_is_required(self) -> None:
        """There is no permissive default; omitting it cannot start a server."""
        with pytest.raises(ValueError, match="at least one principal"):
            make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", introspect_resolver=_resolver)

    def test_empty_allowlist_is_rejected(self) -> None:
        """An empty list is the same mistake spelled differently."""
        with pytest.raises(ValueError, match="at least one principal"):
            make_sync_client(
                RpcServer(_Svc, _Impl()),
                token_key=b"k",
                introspect_resolver=_resolver,
                introspect_principals=[],
            )

    def test_allowlist_without_resolver_is_rejected(self) -> None:
        """Config that looks enabled but is not must not pass silently."""
        with pytest.raises(ValueError, match="without introspect_resolver"):
            make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", introspect_principals=[_PROXY])


class TestCallerAuthorization:
    """Introspection is a distinct capability from authentication."""

    def test_unauthenticated_caller_refused(self) -> None:
        """No credential, no answer."""
        c = _client()
        try:
            assert _post(c, None, {"token": _SUBJECT}).status_code in (401, 403)
        finally:
            c.close()

    def test_authenticated_non_introspector_refused(self) -> None:
        """The oracle test.

        A deployment where any valid credential may introspect lets any user
        test guesses of any other user's credential at unlimited rate, and
        resolve a stolen one to its owner. Authentication alone is not the
        capability.
        """
        c = _client()
        try:
            resp = _post(c, "mallory@example.com", {"token": _SUBJECT})
            assert resp.status_code == 403
            assert "alice@example.com" not in resp.content.decode()
        finally:
            c.close()

    def test_refusal_precedes_resolution(self) -> None:
        """A refused caller must not reach the resolver at all."""
        seen: list[str] = []

        def spy(token: str) -> TokenIdentity | None:
            seen.append(token)
            return _IDENTITIES.get(token)

        c = _client(introspect_resolver=spy)
        try:
            _post(c, "mallory@example.com", {"token": _SUBJECT})
            assert seen == [], "resolver was consulted for an unauthorized caller"
        finally:
            c.close()


class TestUniformRejection:
    """Rejections must not confirm which credentials exist."""

    @pytest.mark.parametrize(
        "body",
        [
            {"token": "no-such-credential"},
            {"token": "expired-credential"},
            {"token": "!!not-a-token!!"},
            {"token": ""},
            {"nottoken": "x"},
            {},
        ],
        ids=["unknown", "expired", "malformed", "empty", "wrong-key", "no-key"],
    )
    def test_all_failures_look_identical(self, body: dict[str, str]) -> None:
        """Unknown, expired and malformed are one answer."""
        c = _client()
        try:
            resp = _post(c, _PROXY, body)
            assert resp.status_code == 404
            assert json.loads(resp.content) == {"error": "unresolved"}
        finally:
            c.close()

    def test_jws_shaped_subject_is_never_resolved(self) -> None:
        """A JWS validated locally must not be vouched for here.

        Routing one through means handing a third party a bearer token the
        asker may itself have rejected — an expired access token is still live
        elsewhere.
        """
        seen: list[str] = []

        def spy(token: str) -> TokenIdentity | None:
            seen.append(token)
            return TokenIdentity(principal="should-never-happen")

        c = _client(introspect_resolver=spy)
        try:
            resp = _post(c, _PROXY, {"token": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"})
            assert resp.status_code == 404
            assert seen == [], "a JWS-shaped subject reached the resolver"
        finally:
            c.close()

    def test_oversized_body_is_refused(self) -> None:
        """The generic request cap would admit megabytes into a JSON parse."""
        c = _client()
        try:
            assert _post(c, _PROXY, {"token": "x" * 100_000}).status_code == 404
        finally:
            c.close()


class TestCredentialNeverLeaks:
    """The credential must not survive anywhere a human or shipper can read."""

    @pytest.mark.parametrize("token", [_SUBJECT, "no-such-credential"])
    def test_absent_from_response_bodies(self, token: str) -> None:
        """Neither a success nor a rejection may echo the subject."""
        c = _client()
        try:
            assert token not in _post(c, _PROXY, {"token": token}).content.decode()
        finally:
            c.close()

    @pytest.mark.parametrize("token", [_SUBJECT, "no-such-credential"])
    def test_absent_from_log_records(self, token: str, caplog: pytest.LogCaptureFixture) -> None:
        """Asserted on captured output, not on review.

        Log lines are where a credential most plausibly escapes: a diagnostic
        added in a hurry outlives the hurry. The digest is what correlates a
        credential's failures across records without being the credential.
        """
        c = _client()
        try:
            with caplog.at_level(logging.DEBUG):
                _post(c, _PROXY, {"token": token})
            rendered = "\n".join(
                [r.getMessage() for r in caplog.records]
                + [str(v) for r in caplog.records for v in vars(r).values() if isinstance(v, str)]
            )
            assert token not in rendered, "the credential reached a log record"
            assert token_digest(token) in rendered, "no digest was logged to correlate on"
        finally:
            c.close()


class TestRateLimiter:
    """Bounds, rather than closes, the oracle a compromised caller still has."""

    def test_allows_up_to_the_limit(self) -> None:
        """The limit is a ceiling, not an off switch."""
        limiter = _RateLimiter(3)
        assert [limiter.allow("a", now=100.0) for _ in range(3)] == [True, True, True]

    def test_refuses_beyond_the_limit(self) -> None:
        """The fourth request in a window is refused."""
        limiter = _RateLimiter(3)
        for _ in range(3):
            limiter.allow("a", now=100.0)
        assert limiter.allow("a", now=100.0) is False

    def test_window_resets(self) -> None:
        """A refusal is not permanent."""
        limiter = _RateLimiter(1)
        assert limiter.allow("a", now=100.0) is True
        assert limiter.allow("a", now=100.0) is False
        assert limiter.allow("a", now=101.5) is True

    def test_callers_are_counted_separately(self) -> None:
        """One noisy caller must not lock out another."""
        limiter = _RateLimiter(1)
        assert limiter.allow("a", now=100.0) is True
        assert limiter.allow("b", now=100.0) is True

    def test_endpoint_enforces_the_limit(self) -> None:
        """Wired up, not merely present."""
        c = _client(introspect_rate_limit=2)
        try:
            codes = [_post(c, _PROXY, {"token": _SUBJECT}).status_code for _ in range(4)]
            assert 429 in codes, f"rate limit never triggered: {codes}"
        finally:
            c.close()


class TestTransientVsDefinitive:
    """Worker-says-no and worker-is-broken are different answers."""

    def test_unavailable_survives_chain_authenticate(self) -> None:
        """The contract the caller's cache depends on.

        ``chain_authenticate`` advances on ``ValueError``, so an outage raised
        as one is read as "not my credential, try the next" and emerges as a
        401 from the end of the chain — turning a sidecar restart into a
        fleet-wide re-login storm.
        """

        def broken(_req: falcon.Request) -> AuthContext:
            raise AuthUnavailableError("backing store unreachable")

        def never_reached(_req: falcon.Request) -> AuthContext:
            raise AssertionError("chain advanced past a transient failure")

        chained = chain_authenticate(broken, never_reached)
        with pytest.raises(AuthUnavailableError):
            chained(falcon.testing.create_req(path="/"))

    def test_unavailable_surfaces_as_503_with_retry_after(self) -> None:
        """A caller must retry, not re-authenticate and not cache."""

        def broken(_req: falcon.Request) -> AuthContext:
            raise AuthUnavailableError("backing store unreachable", retry_after=7)

        c = make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", authenticate=broken)
        try:
            resp = c.post("http://x/ping", content=b"", headers={"Content-Type": "application/json"})
            assert resp.status_code == 503
            assert resp.headers.get("retry-after") == "7"
        finally:
            c.close()

    def test_rejection_still_surfaces_as_401(self) -> None:
        """The distinction only pays if the ordinary path is unchanged."""

        def rejects(_req: falcon.Request) -> AuthContext:
            raise ValueError("bad credential")

        c = make_sync_client(RpcServer(_Svc, _Impl()), token_key=b"k", authenticate=rejects)
        try:
            resp = c.post("http://x/ping", content=b"", headers={"Content-Type": "application/json"})
            assert resp.status_code == 401
        finally:
            c.close()
