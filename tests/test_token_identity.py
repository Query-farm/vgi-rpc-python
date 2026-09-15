# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``vgi_rpc.Identity.v1`` — resolving a credential, and minting a grant.

The two methods are guarded very differently and the difference is the point,
so most of what is tested here is the *asymmetry*: introspection answers a
question about somebody else's credential and is therefore an oracle that has
to be locked down; issuance is always about the caller and therefore is not.
"""

from __future__ import annotations

import time

import pytest

from vgi_rpc.rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.rpc._token_identity import (
    MAX_TOKEN_CHARS,
    GrantRefusedError,
    Identity,
    IdentityImpl,
    IdentityUnavailableError,
    IntrospectionRefusedError,
    IssuedGrant,
    RateLimiter,
    StaleAuthError,
    TokenIdentity,
    TokenUnresolvedError,
    token_digest,
)


def _auth(
    principal: str | None = "alice", *, authenticated: bool = True, auth_time: float | None = None
) -> AuthContext:
    claims: dict[str, object] = {}
    if auth_time is not None:
        claims["auth_time"] = auth_time
    return AuthContext(authenticated=authenticated, principal=principal, domain="test", claims=claims)


def _ctx(auth: AuthContext) -> CallContext:
    return CallContext(auth=auth, implementation=None, transport_metadata={}, emit_client_log=lambda *a, **k: None)


def _resolver(token: str) -> TokenIdentity | None:
    return TokenIdentity(principal="bob", token_name="ci-key") if token == "good" else None


def _minter(principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
    return IssuedGrant(token=f"grant-for-{principal}", expires_at=time.time() + ttl_seconds, grant_id="g1")


class TestRegistration:
    """Absent beats routed-and-refusing."""

    def test_absent_by_default(self) -> None:
        """A dependency upgrade must not grow an oracle on every worker."""
        from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl

        assert "vgi_rpc.Identity.v1" not in RpcServer(RpcFixtureService, RpcFixtureServiceImpl()).bindings

    def test_only_methods_with_hooks_are_hosted(self) -> None:
        """What the server hosts describes what it actually does.

        A worker that resolves credentials but does not mint grants offers one
        method, and a client learns that from reflection rather than by calling
        and reading an error.
        """
        impl = IdentityImpl(resolve_token=_resolver, introspect_principals=["proxy"])
        assert impl.offered_methods() == {"introspect_token"}

        impl = IdentityImpl(mint_grant=_minter)
        assert impl.offered_methods() == {"issue_grant"}

        impl = IdentityImpl(resolve_token=_resolver, mint_grant=_minter, introspect_principals=["proxy"])
        assert impl.offered_methods() == {"introspect_token", "issue_grant"}

    def test_hosted_binding_carries_only_the_offered_methods(self) -> None:
        """Narrowing the method set narrows the protocol hash with it."""
        from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl

        server = RpcServer(
            RpcFixtureService,
            RpcFixtureServiceImpl(),
            identity=IdentityImpl(mint_grant=_minter),
        )
        assert sorted(server.bindings[Identity.protocol_name].methods) == ["issue_grant"]

    def test_claims_the_reserved_prefix(self) -> None:
        """Framework-owned, so an application cannot impersonate it."""
        assert Identity.protocol_name.startswith("vgi_rpc.")


class TestIntrospectionIsLockedDown:
    """The answer is an identity assertion the asker acts on with its own credentials.

    "Trust it as much as you trust the worker" is the wrong frame: the asker
    trusts it *more*, because it authorizes with credentials the worker does
    not hold.
    """

    def _impl(self, **kw: object) -> IdentityImpl:
        return IdentityImpl(resolve_token=_resolver, introspect_principals=["proxy"], **kw)  # type: ignore[arg-type]

    def test_resolves_for_an_allowlisted_caller(self) -> None:
        """The happy path, for the reverse proxy the method exists for."""
        got = self._impl().introspect_token("good", _ctx(_auth("proxy")))
        assert got.principal == "bob"
        assert got.token_name == "ci-key"

    @pytest.mark.parametrize("caller", ["alice", "", None])
    def test_a_caller_off_the_allowlist_is_refused(self, caller: str | None) -> None:
        """Authentication is not the same capability as introspection.

        A deployment where any valid credential may introspect lets any user
        test guesses of any other user's credential at unlimited rate, and
        resolve a stolen one to its owner.
        """
        with pytest.raises(IntrospectionRefusedError):
            self._impl().introspect_token("good", _ctx(_auth(caller)))

    def test_an_unauthenticated_caller_is_refused(self) -> None:
        """Subprocess and unix transports carry no authenticated principal."""
        with pytest.raises(IntrospectionRefusedError):
            self._impl().introspect_token("good", _ctx(_auth("proxy", authenticated=False)))

    def test_refusal_precedes_the_resolver(self) -> None:
        """An unauthorized caller learns nothing, including how long it took."""
        seen: list[str] = []

        def spy(token: str) -> TokenIdentity | None:
            seen.append(token)
            return None

        impl = IdentityImpl(resolve_token=spy, introspect_principals=["proxy"])
        with pytest.raises(IntrospectionRefusedError):
            impl.introspect_token("secret", _ctx(_auth("mallory")))
        assert seen == [], "the resolver must not see a credential from an unauthorized caller"

    @pytest.mark.parametrize("token", ["", "unknown", "x" * (MAX_TOKEN_CHARS + 1)])
    def test_rejections_are_uniform(self, token: str) -> None:
        """Unknown, malformed and over-long are one answer.

        Distinguishing them would confirm that a guessed credential exists.
        """
        with pytest.raises(TokenUnresolvedError):
            self._impl().introspect_token(token, _ctx(_auth("proxy")))

    def test_a_jws_never_reaches_the_resolver(self) -> None:
        """Routing one onward hands a third party a token the asker may have rejected.

        A JWS is validated locally against a key set.  Forwarding one the asker
        already refused -- expired, wrong audience -- to something that might
        accept it turns this method into a laundering step.
        """
        seen: list[str] = []

        def spy(token: str) -> TokenIdentity | None:
            seen.append(token)
            return TokenIdentity(principal="bob")

        impl = IdentityImpl(resolve_token=spy, introspect_principals=["proxy"])
        with pytest.raises(TokenUnresolvedError):
            impl.introspect_token("aaa.bbb.ccc", _ctx(_auth("proxy")))
        assert seen == []

    def test_unavailable_is_transient_not_definitive(self) -> None:
        """A caller that negative-caches "unknown" must not cache this.

        Cache an outage and a worker restart takes the fleet down for the
        cache's lifetime; retry a rejection and the worker is hammered.
        """

        def down(token: str) -> TokenIdentity | None:
            raise IdentityUnavailableError("store is down")

        impl = IdentityImpl(resolve_token=down, introspect_principals=["proxy"])
        with pytest.raises(IdentityUnavailableError) as exc:
            impl.introspect_token("good", _ctx(_auth("proxy")))
        assert exc.value.retry_after > 0
        assert not isinstance(exc.value, ValueError), (
            "must not be a ValueError: chain_authenticate advances on ValueError, so a "
            "sidecar outage raised as one is read as 'not my credential, try the next'"
        )

    def test_rate_limited(self) -> None:
        """Bounds, rather than closes, the oracle an allowlisted caller still has."""
        impl = self._impl(introspect_rate_limit=2)
        ctx = _ctx(_auth("proxy"))
        assert impl.introspect_token("good", ctx).principal == "bob"
        assert impl.introspect_token("good", ctx).principal == "bob"
        with pytest.raises(IntrospectionRefusedError, match="rate limit"):
            impl.introspect_token("good", ctx)

    def test_an_allowlist_is_mandatory(self) -> None:
        """There is no permissive default, so it cannot be reached by omission."""
        with pytest.raises(ValueError, match="at least one principal"):
            IdentityImpl(resolve_token=_resolver)

        with pytest.raises(ValueError, match="at least one principal"):
            IdentityImpl(resolve_token=_resolver, introspect_principals=[])


class TestIssuanceIsNotAnOracle:
    """Issuance is always about the caller, so it needs neither allowlist nor limit."""

    def test_mints_for_the_caller(self) -> None:
        """The happy path: a present user minting their own standing grant."""
        impl = IdentityImpl(mint_grant=_minter)
        grant = impl.issue_grant("reports", ["read"], 3600, _ctx(_auth("alice", auth_time=time.time())))
        assert grant.token == "grant-for-alice"
        assert grant.expires_at > time.time()

    def test_the_subject_is_the_caller_and_is_not_a_parameter(self) -> None:
        """Cross-subject minting is closed by construction, not by a check.

        A check is something one of six ports can forget; a missing parameter
        is not.
        """
        import inspect

        params = set(inspect.signature(Identity.issue_grant).parameters)
        assert "subject" not in params
        assert "principal" not in params

    def test_needs_no_allowlist(self) -> None:
        """Unlike introspection — and the asymmetry is the whole design."""
        assert IdentityImpl(mint_grant=_minter).offered_methods() == {"issue_grant"}


class TestFreshness:
    """A credential with no verifiable auth_time cannot mint."""

    def _impl(self) -> IdentityImpl:
        return IdentityImpl(mint_grant=_minter, max_auth_age=900.0)

    def test_absent_auth_time_is_refused(self) -> None:
        """A static bearer proves a machine holds a secret, never that a human just logged in."""
        with pytest.raises(StaleAuthError, match="no auth_time"):
            self._impl().issue_grant("p", [], 60, _ctx(_auth("alice")))

    def test_stale_auth_time_is_refused_actionably(self) -> None:
        """Naming the reason leaks nothing here: it is always about the caller.

        A console that cannot tell "your login is too old" from "no" cannot
        know to re-prompt.
        """
        with pytest.raises(StaleAuthError, match="re-authenticate"):
            self._impl().issue_grant("p", [], 60, _ctx(_auth("alice", auth_time=time.time() - 5000)))

    def test_fresh_auth_time_is_accepted(self) -> None:
        """The ceiling is a ceiling, not an equality."""
        grant = self._impl().issue_grant("p", [], 60, _ctx(_auth("alice", auth_time=time.time() - 10)))
        assert grant.token == "grant-for-alice"

    def test_a_grant_cannot_mint_another_grant(self) -> None:
        """The lineage cannot escape the identity provider.

        A grant is not an IdP-issued token, so it carries no ``auth_time``, so
        presenting one here fails the freshness check.  That single rule is
        what stops indefinite self-renewal.
        """
        grant_bearer = _auth("alice")  # no auth_time: this is what a grant looks like
        with pytest.raises(StaleAuthError):
            self._impl().issue_grant("p", [], 60, _ctx(grant_bearer))

    def test_unauthenticated_transport_fails_closed(self) -> None:
        """Subprocess and unix have no authenticated principal at all."""
        with pytest.raises(StaleAuthError, match="not authenticated"):
            self._impl().issue_grant("p", [], 60, _ctx(_auth(None, authenticated=False)))


class TestAbsentHooks:
    """Calling a method the deployment did not configure."""

    def test_introspection_without_a_resolver(self) -> None:
        """Refused rather than crashing, for a caller that reached it anyway."""
        with pytest.raises(IntrospectionRefusedError, match="does not resolve"):
            IdentityImpl(mint_grant=_minter).introspect_token("good", _ctx(_auth("proxy")))

    def test_issuance_without_a_minter(self) -> None:
        """Same, on the other side."""
        impl = IdentityImpl(resolve_token=_resolver, introspect_principals=["proxy"])
        with pytest.raises(GrantRefusedError, match="does not mint"):
            impl.issue_grant("p", [], 60, _ctx(_auth("alice", auth_time=time.time())))


class TestDiagnostics:
    """The credential must never reach a log, a span, or an error message."""

    def test_the_digest_is_not_the_token(self) -> None:
        """Stable enough to correlate one credential's failures; not the credential."""
        assert token_digest("secret") != "secret"
        assert token_digest("secret") == token_digest("secret")
        assert token_digest("secret") != token_digest("other")
        assert len(token_digest("secret")) == 64

    def test_error_kinds_are_stable(self) -> None:
        """They are the only definitive/transient signal a caller has.

        These were an HTTP route whose callers classified on the status code
        (404 vs 503).  As protocol methods every handler exception surfaces the
        same way, so ``error_kind`` carries the whole distinction.
        """
        assert IntrospectionRefusedError.error_kind == "introspection_refused"
        assert TokenUnresolvedError.error_kind == "token_unresolved"
        assert StaleAuthError.error_kind == "stale_auth"
        assert GrantRefusedError.error_kind == "grant_refused"
        assert IdentityUnavailableError.error_kind == "identity_unavailable"


class TestRateLimiter:
    """Fixed-window, because the state is two integers rather than an aged float."""

    def test_admits_up_to_the_limit(self) -> None:
        """Within a window."""
        limiter = RateLimiter(3)
        assert [limiter.allow("a", now=100.0) for _ in range(4)] == [True, True, True, False]

    def test_window_rolls(self) -> None:
        """A new window resets the count."""
        limiter = RateLimiter(1)
        assert limiter.allow("a", now=100.0)
        assert not limiter.allow("a", now=100.5)
        assert limiter.allow("a", now=101.5)

    def test_callers_are_independent(self) -> None:
        """One caller exhausting its budget must not refuse another."""
        limiter = RateLimiter(1)
        assert limiter.allow("a", now=100.0)
        assert limiter.allow("b", now=100.0)
        assert not limiter.allow("a", now=100.0)

    def test_cycling_keys_cannot_grow_the_map(self) -> None:
        """Whole-map reset rather than per-key ageing, so an attacker cannot.

        Per-key ageing would let a caller cycling keys grow the map without
        bound between sweeps.
        """
        limiter = RateLimiter(1)
        for i in range(1000):
            limiter.allow(f"k{i}", now=100.0)
        limiter.allow("fresh", now=200.0)
        assert len(limiter._counts) == 1
