# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Fixed deployment policy for the ``vgi_rpc.Identity.v1`` conformance group.

``vgi_rpc.Identity.v1`` is nearly all *guards*, and every guard reads
deployment policy: who may introspect, what a credential resolves to, whether a
grant is minted, how recently the caller authenticated.  A cross-port
assertion is impossible unless every port's conformance worker configures the
same policy, so it is pinned here and restated normatively in
``IDENTITY_CONFORMANCE_FIXTURE.md``.

Two design rules shape the values below, and both come from the spec's §5b.

**The resolver resolves almost everything.**  Rejections are deliberately
uniform -- unknown, expired, malformed and over-long are one answer -- which
makes the obvious guard test prove nothing: an over-long credential is *also*
an unknown one, so probing with a credential the resolver does not know cannot
distinguish "the cap refused it" from "the cap let it through and the resolver
refused it".  Delete the cap and such a test stays green.  With a resolver that
answers for anything it is handed, a rejection can only have come from a guard,
and the success that a broken guard produces is loud.

**The policy hooks are pure functions of their arguments.**  No clock, no
counter, no shared state: a conformance worker must answer identically on the
first call and the thousandth, and on a port whose runtime dispatches the two
methods on different threads.  The one value that would otherwise need a clock
-- a grant's ``expires_at`` -- is a fixed constant so the wire value can be
asserted exactly rather than within a tolerance.

.. warning::

   The authentication this fixture relies on is header-driven and therefore
   trivially spoofable by anyone who can reach the port.  It exists to give six
   language ports a deterministic authenticated caller without an identity
   provider.  It is a **test fixture** and must never be deployed.

"""

from __future__ import annotations

import json
from typing import ClassVar, Protocol

from vgi_rpc.errors import AuthUnavailableError
from vgi_rpc.grants import GrantKeys
from vgi_rpc.rpc import CallContext
from vgi_rpc.rpc._token_identity import (
    GrantRefusedError,
    IdentityUnavailableError,
    IssuedGrant,
    TokenIdentity,
)

__all__ = [
    "GRANT_AUDIENCE",
    "GRANT_KEY_CURRENT",
    "GRANT_KEY_PREVIOUS",
    "GRANT_KEY_STRANGER",
    "GRANT_MAX_TTL",
    "WHOAMI_PROTOCOL_HASH",
    "WHOAMI_PROTOCOL_NAME",
    "Whoami",
    "WhoamiImpl",
    "conformance_grant_keys",
    "IDENTITY_BOTH_METHODS_HASH",
    "IDENTITY_GRANT_ONLY_HASH",
    "IDENTITY_INTROSPECT_ONLY_HASH",
    "IDENTITY_PROTOCOL_NAME",
    "AUTH_TIME_HEADER",
    "AUTH_UNAVAILABLE_PURPOSE",
    "AUTH_UNAVAILABLE_RETRY_AFTER",
    "GRANT_EXPIRES_AT",
    "GRANT_ID",
    "GRANT_TOKEN_PREFIX",
    "INTROSPECTOR_PRINCIPAL",
    "MAX_AUTH_AGE",
    "MINIMAL_PURPOSE",
    "MINTER_PRINCIPAL",
    "OTHER_MINTER_PRINCIPAL",
    "OUTSIDER_PRINCIPAL",
    "PRINCIPAL_HEADER",
    "REFUSED_PURPOSE",
    "SCOPE_SEPARATOR",
    "SUBJECT_PRINCIPAL",
    "SUBJECT_TOKEN",
    "SUBJECT_TOKEN_NAME",
    "SUBJECT_TTL",
    "TOKEN_JWS_TRAP",
    "TOKEN_MINIMAL",
    "TOKEN_PADDED_PROBE",
    "TOKEN_PADDED_PROBE_NAME",
    "TOKEN_AUTH_UNAVAILABLE",
    "TOKEN_UNAVAILABLE",
    "UNAVAILABLE_RETRY_AFTER",
    "TOKEN_UNKNOWN",
    "TOKEN_ZERO_TTL",
    "conformance_mint_grant",
    "conformance_resolve_token",
]

#: Routing key of the protocol under test.
IDENTITY_PROTOCOL_NAME = "vgi_rpc.Identity.v1"

#: The three pinned digests from ``IDENTITY_V1_SPEC.md`` §1.  Restated here so
#: the shared suite can assert them against what a worker *reports over the
#: wire* through reflection, rather than each port asserting a constant against
#: a constant of its own.  A digest computed locally and compared to a copied
#: literal proves the copy was made; only reading it back off a running server
#: proves the wire shape.
IDENTITY_BOTH_METHODS_HASH = "8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5"
IDENTITY_INTROSPECT_ONLY_HASH = "27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385"
IDENTITY_GRANT_ONLY_HASH = "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8"

# ---------------------------------------------------------------------------
# Authentication: two request headers, no identity provider
# ---------------------------------------------------------------------------

#: Header naming the authenticated principal.  Absent means *unauthenticated*
#: -- not "anonymous but present" -- so an unauthenticated probe needs no
#: special casing anywhere.  Already the convention for the sticky fixture
#: (``sticky-sessions-spec.md`` §9); reused rather than invented.
PRINCIPAL_HEADER = "X-Conformance-Principal"

#: Header carrying the ``auth_time`` claim, placed in the claim map **verbatim
#: as a string and unparsed**.
#:
#: Verbatim matters.  A fixture that parses the header and drops it when
#: parsing fails converts "carries an unusable auth_time" into "carries no
#: auth_time" -- the same ``stale_auth`` answer for a different reason, which
#: makes the unparseable case untestable rather than failing.  The guard is
#: what parses; the fixture only transports.
AUTH_TIME_HEADER = "X-Conformance-Auth-Time"

#: The single principal on the introspector allowlist.
INTROSPECTOR_PRINCIPAL = "conformance-introspector"

#: An authenticated principal that is *not* on the allowlist.  Used to show
#: that authentication alone does not confer introspection.
OUTSIDER_PRINCIPAL = "conformance-outsider"

#: Callers used for ``issue_grant``.  Two, because "the subject is the caller"
#: is only observable by minting as two different callers and seeing two
#: different grants.
MINTER_PRINCIPAL = "minter@conformance.example"
OTHER_MINTER_PRINCIPAL = "other-minter@conformance.example"

#: ``max_auth_age`` the worker must configure -- the documented default.
MAX_AUTH_AGE = 900.0

# There is no ``introspect_rate_limit`` to configure: introspection is not rate
# limited (``TestIntrospectionIsNotThrottled`` pins that). The fixture used to
# set it to 100,000 so a production-tuned limiter could not fire mid-group.

# ---------------------------------------------------------------------------
# What the resolver answers
# ---------------------------------------------------------------------------

#: The identity every resolvable credential maps to.
SUBJECT_PRINCIPAL = "subject@conformance.example"
SUBJECT_TOKEN_NAME = "conformance-subject"
SUBJECT_TTL = 300

#: An ordinary opaque credential.  Nothing distinguishes it from any other
#: string the resolver has no special rule for; it exists so the happy-path
#: case reads as one.
SUBJECT_TOKEN = "conformance-opaque-subject-token"

#: The one credential the resolver reports as **unknown**.  Everything else it
#: has no rule for resolves, so a rejection of anything else can only have come
#: from a guard.
TOKEN_UNKNOWN = "conformance-unknown-token"

#: The credential the resolver reports as *unknowable* rather than unknown.  A
#: caller may negative-cache "unknown"; caching an outage locks out a valid
#: user for as long as the cache holds, so the two must not share an answer.
TOKEN_UNAVAILABLE = "conformance-unavailable-token"

#: Retry hint :data:`TOKEN_UNAVAILABLE` carries.  ``identity_unavailable``
#: MUST carry ``RetryInfo`` (WIRE_PROTOCOL.md §16), so the value is pinned
#: rather than left to each port's default.
UNAVAILABLE_RETRY_AFTER = 5

#: The credential the resolver answers by raising the port's **transport-auth**
#: "could not find out" error -- ``AuthUnavailableError`` here -- rather than
#: the identity one.  A hook calling the same store an authenticator calls
#: raises what the authenticator raises, and vgi-python's docs tell workers to.
#: The framework MUST translate it to ``identity_unavailable`` and keep its
#: retry hint; before the rule was written down five of seven ports sent it
#: unclassified, with no kind and no hint.
TOKEN_AUTH_UNAVAILABLE = "conformance-auth-unavailable-token"

#: Retry hint the transport-auth error carries.  Deliberately not the identity
#: error's default, so a port that translates but substitutes its own default
#: hint is caught: 7 can only have come from the hook.
AUTH_UNAVAILABLE_RETRY_AFTER = 7

#: A resolver that names a TTL of zero is saying *do not cache this*.  Several
#: ports have a zero value where Python has an absent column, and the tempting
#: fix -- normalise ``<= 0`` up to the 300 default -- silently converts that
#: into five minutes of continued access after revocation.  Spec §5a: honour
#: what the hook returned, including zero.
TOKEN_ZERO_TTL = "conformance-zero-ttl-token"

#: Resolves to an identity built with *only* ``principal`` supplied, so the
#: other two fields must land on their documented defaults (``""`` and 300).
#: The other half of §5a: a default is for an omitted field, never a coercion
#: applied to a value a hook actually set.
TOKEN_MINIMAL = "conformance-minimal-token"

#: A JWS-shaped credential the resolver **would** resolve.  Deliberately
#: resolvable: offered as an unknown credential instead, a port with no shape
#: guard would reject it as unknown and pass for the wrong reason.  Resolvable,
#: the guard becomes observable -- a port that routes a JWS to its resolver
#: answers with an identity and fails loudly.
TOKEN_JWS_TRAP = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"

#: A credential carrying two leading and two trailing ASCII spaces, resolved to
#: a *distinguishable* ``token_name``.  The shape test runs on the trimmed
#: credential while the resolver receives the untrimmed original; a port that
#: trims before resolving hands the resolver a different string, falls through
#: to the generic rule, and reports the wrong name.  Nothing else can see that.
TOKEN_PADDED_PROBE = "  conformance-padded-probe  "
TOKEN_PADDED_PROBE_NAME = "conformance-padded"


def conformance_resolve_token(token: str) -> TokenIdentity | None:
    """Resolve a credential under the fixed conformance policy.

    Args:
        token: The credential, exactly as the caller sent it.

    Returns:
        The identity, or ``None`` for the one credential this policy calls
        unknown.

    Raises:
        IdentityUnavailableError: For :data:`TOKEN_UNAVAILABLE`, standing in
            for a backing store that cannot be reached.
        AuthUnavailableError: For :data:`TOKEN_AUTH_UNAVAILABLE` -- the
            transport-auth spelling of the same outage, which the framework
            must translate.

    """
    if token == TOKEN_UNAVAILABLE:
        raise IdentityUnavailableError("conformance: mapping store unreachable", retry_after=UNAVAILABLE_RETRY_AFTER)
    if token == TOKEN_AUTH_UNAVAILABLE:
        raise AuthUnavailableError("conformance: authority unreachable", retry_after=AUTH_UNAVAILABLE_RETRY_AFTER)
    if token == TOKEN_UNKNOWN:
        return None
    if token == TOKEN_ZERO_TTL:
        return TokenIdentity(principal=SUBJECT_PRINCIPAL, token_name=SUBJECT_TOKEN_NAME, ttl_seconds=0)
    if token == TOKEN_MINIMAL:
        return TokenIdentity(principal=SUBJECT_PRINCIPAL)
    if token == TOKEN_PADDED_PROBE:
        return TokenIdentity(
            principal=SUBJECT_PRINCIPAL,
            token_name=TOKEN_PADDED_PROBE_NAME,
            ttl_seconds=SUBJECT_TTL,
        )
    # Everything else resolves.  That is the point: see the module docstring.
    return TokenIdentity(principal=SUBJECT_PRINCIPAL, token_name=SUBJECT_TOKEN_NAME, ttl_seconds=SUBJECT_TTL)


# ---------------------------------------------------------------------------
# What the minter answers
# ---------------------------------------------------------------------------

#: Prefix of a minted grant's token.  The caller's principal is appended, which
#: is how "the subject is the caller, never a parameter" becomes observable:
#: two callers get two different tokens from identical requests.
GRANT_TOKEN_PREFIX = "conformance-grant-for:"

#: Separator between the principal and the echoed scopes in a grant token, so
#: the scope list's round trip is visible in the response.
SCOPE_SEPARATOR = "|"

#: Fixed rather than ``now + ttl``: a constant can be asserted exactly, which
#: also pins the float64 round trip.  ``expires_at`` is a declaration rather
#: than an enforcement (the real lifetime lives inside the opaque token), so
#: nothing is lost by making it a constant.  2030-01-01T00:00:00Z.
GRANT_EXPIRES_AT = 1893456000.0

#: Correlation handle a full grant carries.
GRANT_ID = "conformance-grant-id"

#: The purpose this policy refuses, so ``grant_refused`` reaches the wire.
REFUSED_PURPOSE = "conformance-refused"

#: The purpose that mints a grant built without ``grant_id``, so the field's
#: documented default (``""``) is observable.
MINIMAL_PURPOSE = "conformance-minimal"

#: The purpose the minter answers by raising the transport-auth unavailable
#: error, so the translation rule is observable on ``issue_grant`` too -- the
#: rule covers both hooks, and a port that wraps only one passes a token-only
#: test.
AUTH_UNAVAILABLE_PURPOSE = "conformance-auth-unavailable"


def conformance_mint_grant(principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
    """Mint a grant under the fixed conformance policy.

    ``ttl_seconds`` is deliberately ignored.  It is a *request*, the returned
    ``expires_at`` is authoritative, and a fixture that honoured it would need
    a clock and become unassertable.

    Args:
        principal: The caller's authenticated principal, supplied by the
            framework.  There is no subject parameter and must not be one.
        purpose: Why the grant is wanted.  Two values carry policy meaning.
        scopes: What the grant may do; echoed into the token so the list's
            round trip is observable.
        ttl_seconds: Ignored, see above.

    Returns:
        The minted grant.

    Raises:
        GrantRefusedError: For :data:`REFUSED_PURPOSE`.
        AuthUnavailableError: For :data:`AUTH_UNAVAILABLE_PURPOSE`.

    """
    del ttl_seconds
    if purpose == AUTH_UNAVAILABLE_PURPOSE:
        raise AuthUnavailableError("conformance: grant store unreachable", retry_after=AUTH_UNAVAILABLE_RETRY_AFTER)
    if purpose == REFUSED_PURPOSE:
        raise GrantRefusedError("conformance: this purpose is refused")
    token = GRANT_TOKEN_PREFIX + principal + SCOPE_SEPARATOR + ",".join(scopes)
    if purpose == MINIMAL_PURPOSE:
        return IssuedGrant(token=token, expires_at=GRANT_EXPIRES_AT)
    return IssuedGrant(token=token, expires_at=GRANT_EXPIRES_AT, grant_id=GRANT_ID)


# ---------------------------------------------------------------------------
# Sealed grants and bearer acceptance (IDENTITY_CONFORMANCE_FIXTURE.md §10)
# ---------------------------------------------------------------------------

#: The minting key of the grant worker: bytes 0x10..0x2f.  Published on
#: purpose -- the shared suite mints with it to prove a port's *verifier*
#: accepts reference-minted grants, and decodes the port's grants to prove its
#: *minter* matches the format.  A fixture key, never a deployment one.
GRANT_KEY_CURRENT = bytes(range(0x10, 0x30))

#: The previous key, still configured to verify (rotation): bytes 0x30..0x4f.
GRANT_KEY_PREVIOUS = bytes(range(0x30, 0x50))

#: A key the grant worker does not hold: 0x77 * 32.
GRANT_KEY_STRANGER = bytes([0x77]) * 32

#: Audience bound into the grant worker's tokens.
GRANT_AUDIENCE = "conformance"

#: The grant worker's lifetime ceiling, in seconds.
GRANT_MAX_TTL = 3600


def conformance_grant_keys() -> GrantKeys:
    """Return the grant worker's configuration: current key mints, both verify."""
    return GrantKeys(
        keys=(GRANT_KEY_CURRENT, GRANT_KEY_PREVIOUS),
        audience=GRANT_AUDIENCE,
        max_ttl_seconds=GRANT_MAX_TTL,
    )


#: Routing key of the probe that reports how a request was authenticated.
WHOAMI_PROTOCOL_NAME = "conformance.Whoami.v1"

#: Pinned digest of :class:`Whoami` (IDENTITY_CONFORMANCE_FIXTURE.md §10).
WHOAMI_PROTOCOL_HASH = "a280333ba72432020e162cab388a78355969a30aa74f0665ad9d2932d7a10b8f"


class Whoami(Protocol):
    """Hosted only by the grant worker: reports what authentication decided."""

    protocol_name: ClassVar[str] = WHOAMI_PROTOCOL_NAME

    def whoami(self) -> str:
        """Return the caller's ``AuthContext`` as canonical JSON.

        ``{"authenticated": bool, "claims": {...}, "domain": str, "principal": str}``
        with sorted keys; ``domain`` and ``principal`` are ``""`` when absent.
        """
        ...


class WhoamiImpl:
    """Reference implementation of :class:`Whoami`."""

    def whoami(self, ctx: CallContext) -> str:
        """Report the request's authentication outcome."""
        auth = ctx.auth
        return json.dumps(
            {
                "authenticated": auth.authenticated,
                "claims": dict(auth.claims),
                "domain": auth.domain or "",
                "principal": auth.principal or "",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
