# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``vgi_rpc.Identity.v1`` -- resolving a credential, and minting a grant.

Identity lives here, at the RPC layer, rather than in any application
protocol: a bearer token is not a VGI concept, the auth primitives it builds on
(``AuthContext``, ``chain_authenticate``, ``AuthUnavailableError``) are already
here, and implementing it once is the whole point.  It was previously an HTTP
JSON route, ``POST {prefix}/__introspect_token__``, which meant it existed only
on one transport and had to be hand-written in every port.

Two methods share this module's guards, and they are guarded *differently* on
purpose.

``introspect_token`` answers "which principal is this credential" for a reverse
proxy that terminates the only public listener.  The answer is an identity
assertion made by the thing being protected, which the asker then acts on using
credentials the worker does not hold -- storage credentials, entitlement
lookups, policy-tier selection.  "Trust it as much as you trust the worker" is
the wrong frame: it must be trusted *more*.  So every rejection is uniform, the
caller must be on an allowlist with no permissive default, a JWS-shaped subject
never reaches the resolver, and the whole thing is rate limited.

``issue_grant`` mints a credential for the *calling* user, so it is not an
oracle about anybody else.  It therefore needs no allowlist and no rate limit,
and its rejections are deliberately *actionable*: a console that cannot tell
"your login is too old" from "no" cannot know to re-prompt.

Errors carry a stable ``error_kind``.  That is load-bearing rather than
decorative: these used to be a bespoke HTTP route whose callers classified
definitive-vs-transient on the HTTP status (404 vs 503).  As protocol methods
every handler exception surfaces as HTTP 500, so ``error_kind`` is now the
*only* signal a caller has.  A caller that negative-caches a transient failure
locks out valid users; one that retries a definitive rejection hammers the
worker.
"""

from __future__ import annotations

import dataclasses
import hashlib
import re
import threading
import time
from typing import TYPE_CHECKING, ClassVar, Protocol

from vgi_rpc.utils import ArrowSerializableDataclass

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ._common import AuthContext, CallContext

__all__ = [
    "IdentityImpl",
    "TokenIdentity",
    "IssuedGrant",
    "Identity",
    "GrantRefusedError",
    "IdentityUnavailableError",
    "IntrospectionRefusedError",
    "MAX_TOKEN_CHARS",
    "RateLimiter",
    "StaleAuthError",
    "TokenUnresolvedError",
    "check_freshness",
    "check_introspector",
    "normalise_principals",
    "reject_jws_shaped",
    "token_digest",
]

#: Three dot-separated base64url segments -- a JWS.  Such a credential is
#: validated locally against a key set and MUST NOT be routed to a resolver:
#: doing so sends a bearer token the asker may itself have rejected (expired,
#: wrong audience) to a third party that might accept it.
_JWS_SHAPED = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$")

#: Cap on a credential we will even attempt to resolve.  Anything longer is not
#: a bearer token; refusing early keeps a resolver from being handed megabytes.
MAX_TOKEN_CHARS = 4096


def token_digest(token: str) -> str:
    """Return a SHA-256 hex digest of *token*, for diagnostics.

    The credential itself must never reach a log, a span, or an error message.
    A digest is stable enough to correlate one credential's failures across
    records without being the credential.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class IntrospectionRefusedError(PermissionError):
    """The caller may not introspect.

    Definitive: a caller may cache this.  Authentication is not the same
    capability as introspection -- a deployment where any valid credential may
    introspect lets any user test guesses of any other user's credential at
    unlimited rate, and resolve a stolen one to its owner.
    """

    error_kind: ClassVar[str] = "introspection_refused"


class TokenUnresolvedError(ValueError):
    """The subject credential did not resolve.

    Definitive, and deliberately uniform: unknown, expired and malformed are
    one answer, because reporting which would confirm that a guessed
    credential exists.
    """

    error_kind: ClassVar[str] = "token_unresolved"


class StaleAuthError(PermissionError):
    """The caller has not authenticated recently enough to mint a grant.

    Definitive but *actionable*, unlike the introspection rejections: this is
    always about the caller themselves, so naming the reason leaks nothing and
    is the only way a console learns to re-prompt.
    """

    error_kind: ClassVar[str] = "stale_auth"


class GrantRefusedError(PermissionError):
    """The worker declined to mint this grant.

    Definitive.  The worker holds the policy; the framework only asked.
    """

    error_kind: ClassVar[str] = "grant_refused"


class IdentityUnavailableError(Exception):
    """The answer is not *knowable* -- a backing store is down, a 5xx upstream.

    Transient, and distinct from a definitive rejection: a caller that
    negative-caches "unknown" must not cache this.  Deliberately not a
    ``ValueError`` so an authenticate chain does not read it as "not my
    credential, try the next" and turn a thirty-second outage into a fleet-wide
    re-login.
    """

    error_kind: ClassVar[str] = "identity_unavailable"

    def __init__(self, detail: str = "", *, retry_after: int = 5) -> None:
        """Build the error, carrying how long the caller should wait."""
        super().__init__(detail or "identity lookup unavailable")
        self.detail = detail
        self.retry_after = retry_after


class RateLimiter:
    """Fixed-window request limiter, keyed by caller.

    Present because introspection is a credential-to-identity oracle even when
    correctly restricted: an allowlisted caller whose own credential leaks can
    still test guesses.  Rate limiting does not close that, it bounds it.

    Fixed-window rather than a token bucket: a window admits at most twice the
    rate across a boundary, which is a rounding error here, and the state is
    two integers per caller rather than a float that has to be aged.
    """

    __slots__ = ("_counts", "_lock", "_per_window", "_window", "_window_start")

    def __init__(self, per_window: int, window_seconds: float = 1.0) -> None:
        """Build a limiter admitting *per_window* requests per window."""
        self._per_window = per_window
        self._window = window_seconds
        self._counts: dict[str, int] = {}
        self._window_start = 0.0
        self._lock = threading.Lock()

    def allow(self, key: str, now: float | None = None) -> bool:
        """Return ``True`` if *key* may make a request in the current window."""
        current = time.monotonic() if now is None else now
        with self._lock:
            if current - self._window_start >= self._window:
                # Whole-map reset rather than per-key ageing: an attacker
                # cycling keys cannot grow the map beyond one window's worth.
                self._counts.clear()
                self._window_start = current
            count = self._counts.get(key, 0)
            if count >= self._per_window:
                return False
            self._counts[key] = count + 1
            return True


def normalise_principals(principals: Iterable[str] | None) -> frozenset[str]:
    """Validate the introspector allowlist.

    Args:
        principals: Principals permitted to introspect.

    Returns:
        The allowlist as a frozenset.

    Raises:
        ValueError: If the allowlist is missing or empty.  There is no
            permissive default: "any authenticated caller" is precisely the
            configuration that turns introspection into an open oracle, so it
            cannot be reached by omission.

    """
    allowed = frozenset(p for p in (principals or ()) if p)
    if not allowed:
        raise ValueError(
            "introspect_principals must name at least one principal. "
            "Introspection is a distinct capability from authentication: "
            "allowing any authenticated caller lets any user resolve any "
            "other user's credential to its owner."
        )
    return allowed


def check_introspector(auth: AuthContext, principals: frozenset[str]) -> str:
    """Return the caller principal, or refuse.

    Checked before anything touches the subject credential: an unauthorized
    caller must not learn anything about it, including how long it took.
    """
    caller = auth.principal or ""
    if not auth.authenticated or caller not in principals:
        raise IntrospectionRefusedError("caller is not an introspector")
    return caller


def reject_jws_shaped(token: str) -> None:
    """Refuse a JWS-shaped subject before it reaches a resolver."""
    if not token or len(token) > MAX_TOKEN_CHARS or _JWS_SHAPED.match(token):
        raise TokenUnresolvedError("unresolved")


def check_freshness(auth: AuthContext, max_auth_age: float, *, now: float | None = None) -> float:
    """Return the caller's ``auth_time``, or refuse if it is missing or stale.

    A credential with no verifiable ``auth_time`` cannot mint.  That single
    rule is what stops a grant being used to mint another grant: a grant is not
    an IdP-issued token, so it carries no ``auth_time``, so the lineage cannot
    escape the identity provider.  It also makes subprocess and unix transports
    fail closed for free -- there is no authenticated principal there at all.

    A static bearer proves a machine holds a secret, never that a human just
    authenticated, so it is refused here too.

    .. warning::

       ``auth_time`` is an OIDC claim meaning *when this session began*, which
       can be arbitrarily old while still present and cryptographically valid.
       Requiring it is not the same as requiring a recent login: the deployment
       must send ``max_age`` (or an appropriate ``acr``) at the authorize
       endpoint for this guard to mean what it says.
    """
    if not auth.authenticated or not auth.principal:
        raise StaleAuthError("caller is not authenticated")
    raw = auth.claims.get("auth_time")
    if raw is None:
        raise StaleAuthError("credential carries no auth_time; only a recently authenticated user may mint a grant")
    try:
        auth_time = float(raw)
    except (TypeError, ValueError) as exc:
        raise StaleAuthError("credential carries an unusable auth_time") from exc
    age = (time.time() if now is None else now) - auth_time
    if age > max_auth_age:
        raise StaleAuthError(
            f"last authentication was {age:.0f}s ago, which exceeds the "
            f"{max_auth_age:.0f}s ceiling for minting a grant; re-authenticate"
        )
    return auth_time


# ---------------------------------------------------------------------------
# The protocol
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class TokenIdentity(ArrowSerializableDataclass):
    """The identity an opaque credential authenticates as.

    **Never carries claims.**  A pass-through claims field would let a worker
    choose its caller's tenant routing, its row scope, and its policy branch,
    and the asker derives everything it needs from the principal alone.

    Attributes:
        principal: The canonical principal, in the exact form the worker itself
            would derive -- so an asker that normalises differently does not
            authorize as one identity while the worker serves another.
        token_name: Human-readable name for the credential, for audit trails.
            Never the credential.
        ttl_seconds: How long the answer may be cached.  The caller does the
            caching.  Treat it as an authorization window: for any path the
            asker serves without re-presenting the credential it is exactly
            that, and therefore also the revocation lag.

    """

    principal: str
    token_name: str = ""
    ttl_seconds: int = 300


@dataclasses.dataclass(frozen=True)
class IssuedGrant(ArrowSerializableDataclass):
    """A standing delegation credential.

    Attributes:
        token: The credential.  **Opaque to the framework** -- the worker owns
            the format entirely (a sealed envelope, a database row, or a
            credential brokered from the IdP are all equally valid and equally
            invisible here).  Never parsed, never logged.
        expires_at: Unix timestamp after which the worker will stop honouring
            the grant.  Required *because* the framework cannot enforce it: the
            real lifetime lives inside the opaque token, so this is a
            declaration rather than an enforcement.  A worker that must state a
            lifetime has thought about one.
        grant_id: Correlation handle for the audit trail.  Not a credential and
            not secret -- it is what ties a mint record to later use.

    """

    token: str
    expires_at: float
    grant_id: str = ""


class Identity(Protocol):
    """Resolving a credential, and minting a standing grant.

    Two methods, guarded very differently, because they are different kinds of
    thing: one answers a question about *somebody else's* credential, the other
    mints one for the caller.
    """

    protocol_name: ClassVar[str] = "vgi_rpc.Identity.v1"

    def introspect_token(self, token: str) -> TokenIdentity:
        """Resolve an opaque bearer credential to the identity it authenticates as.

        For a reverse proxy that terminates the only public listener and must
        know *which principal* a credential is before it can authorize
        anything.  When the credential is opaque the proxy holds no local copy
        and has to ask the worker.

        The answer is an identity assertion made by the thing being protected,
        which the asker then acts on using credentials the worker does not hold
        -- storage credentials, entitlement lookups, policy-tier selection.
        "Trust it as much as you trust the worker" is the wrong frame: it must
        be trusted *more*.  Hence the guards: an introspector allowlist with no
        permissive default, uniform rejections, a JWS-shaped subject refused
        before the resolver runs, and rate limiting.

        Deliberately *not* "replay the credential through the worker's own
        authenticate chain": that would run an independently-configured
        audience and issuer set, cannot replay cookie- or mTLS-derived
        identity, and would silently elevate any address-allowlist member.

        Args:
            token: The opaque credential.  Never a JWS -- three-segment
                credentials are refused before reaching the resolver, because
                routing one onward would hand a third party a token the asker
                may itself have rejected.

        Returns:
            The resolved identity.

        """
        ...

    def issue_grant(self, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
        """Mint a standing delegation credential for the *calling* user.

        OAuth cannot express durable delegation: it fuses the grant, the
        credential and the session into one refresh token, so an IdP shortening
        session lifetime shortens the grant.  This is the durable record --
        minted while the user is present, presented later by unattended
        automation as an ordinary bearer.

        **There is no subject parameter.**  The subject is always the caller's
        authenticated principal, so cross-subject minting is closed by
        construction rather than by a check.  That is also why this method
        needs no allowlist while ``introspect_token`` has one: introspection
        resolves *other people's* credentials, so "any authenticated caller" is
        an open oracle there; issuance is always about the caller themselves.

        The caller must have authenticated recently.  A credential with no
        verifiable ``auth_time`` cannot mint, which is what stops a grant being
        used to mint another grant and escaping the identity provider
        permanently -- and makes subprocess and unix transports fail closed for
        free, since there is no authenticated principal there at all.

        Args:
            purpose: Why the grant is being minted, for the audit trail.
            scopes: What the grant may do.  The worker decides what these mean;
                the framework neither interprets nor validates them.
            ttl_seconds: Requested lifetime.  A request, not an instruction --
                the worker may return a shorter one, and the returned
                ``expires_at`` is authoritative.

        Returns:
            The minted grant.

        """
        ...


class IdentityImpl:
    """Applies this module's guards, then delegates to worker-supplied hooks.

    The framework owns the guards and owns none of the policy.  It decides who
    may ask, how often, and what shape of credential is refused outright; the
    worker decides what a credential resolves to and whether a grant is minted.
    That split is deliberate -- the guards are the part that is identical in
    every deployment and catastrophic to get wrong, and the policy is the part
    that is different in every deployment and cannot be guessed.

    **A method whose hook is absent is not registered at all**, so the protocol
    a server hosts describes what it actually does.  A worker that resolves
    credentials but does not mint grants hosts ``introspect_token`` and not
    ``issue_grant``, and a client discovers that through ordinary reflection
    rather than by calling and reading an error.  Absent beats
    routed-and-refusing: it is what keeps a dependency upgrade from growing a
    credential-to-identity oracle on every existing worker.
    """

    __slots__ = ("_limiter", "_max_auth_age", "_mint_grant", "_principals", "_resolve_token")

    _resolve_token: Callable[[str], TokenIdentity | None] | None
    _mint_grant: Callable[[str, str, list[str], int], IssuedGrant] | None

    def __init__(
        self,
        *,
        resolve_token: Callable[[str], TokenIdentity | None] | None = None,
        mint_grant: Callable[[str, str, list[str], int], IssuedGrant] | None = None,
        introspect_principals: Iterable[str] | None = None,
        introspect_rate_limit: int = 20,
        max_auth_age: float = 900.0,
    ) -> None:
        """Build the implementation.

        Args:
            resolve_token: ``(token) -> TokenIdentity | None``.  ``None`` means
                the store answered and the credential is unknown; raise
                :class:`IdentityUnavailableError` for "not knowable".
            mint_grant: ``(principal, purpose, scopes, ttl_seconds) -> IssuedGrant``.
            introspect_principals: Who may call ``introspect_token``.  Required
                whenever *resolve_token* is supplied; there is no permissive
                default.
            introspect_rate_limit: Introspections allowed per caller per second.
            max_auth_age: How recently a caller must have authenticated to mint
                a grant.

        Raises:
            ValueError: ``resolve_token`` was supplied without an allowlist.

        """
        self._resolve_token = resolve_token
        self._mint_grant = mint_grant
        self._max_auth_age = max_auth_age
        # Validated at construction, not at first call: a worker that would
        # refuse every introspection should fail to start rather than serve
        # traffic until someone tries.
        self._principals = normalise_principals(introspect_principals) if resolve_token is not None else frozenset()
        self._limiter = RateLimiter(introspect_rate_limit)

    def offered_methods(self) -> frozenset[str]:
        """Return the methods this deployment can actually answer.

        A method whose hook is absent is not registered, so the protocol a
        server hosts describes what it does.  A worker that resolves
        credentials but does not mint grants offers ``introspect_token`` and
        not ``issue_grant``, and a client learns that from reflection rather
        than by calling and reading an error.
        """
        offered: set[str] = set()
        if self._resolve_token is not None:
            offered.add("introspect_token")
        if self._mint_grant is not None:
            offered.add("issue_grant")
        return frozenset(offered)

    def introspect_token(self, token: str, ctx: CallContext) -> TokenIdentity:
        """Resolve *token*, after checking the caller may ask."""
        if self._resolve_token is None:
            raise IntrospectionRefusedError("this worker does not resolve credentials")

        # Authorization first: an unauthorized caller must learn nothing about
        # the subject credential, including how long looking at it took.
        caller = check_introspector(ctx.auth, self._principals)
        if not self._limiter.allow(caller):
            raise IntrospectionRefusedError("introspection rate limit exceeded")
        reject_jws_shaped(token)

        identity = self._resolve_token(token)
        if identity is None:
            # Uniform with malformed and expired: reporting which would confirm
            # that a guessed credential exists.
            raise TokenUnresolvedError("unresolved")
        return identity

    def issue_grant(self, purpose: str, scopes: list[str], ttl_seconds: int, ctx: CallContext) -> IssuedGrant:
        """Mint a grant for the caller, after checking they authenticated recently."""
        if self._mint_grant is None:
            raise GrantRefusedError("this worker does not mint grants")
        check_freshness(ctx.auth, self._max_auth_age)
        # The subject is the caller, never a parameter: cross-subject minting
        # is closed by construction rather than by a check that could be
        # forgotten in one of six ports.
        return self._mint_grant(ctx.auth.principal or "", purpose, scopes, ttl_seconds)
