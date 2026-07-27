# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bearer token and chain authentication factories for vgi-rpc HTTP transport.

Provides ``bearer_authenticate()`` for opaque bearer token validation,
``bearer_authenticate_static()`` for static token-to-context mapping,
``chain_authenticate()`` for composing alternative credentials (OR), and
``require_all()`` for composing a precondition with a credential (AND).

No extra dependencies beyond ``falcon`` and ``vgi_rpc.rpc.AuthContext``.
"""

from __future__ import annotations

import dataclasses
import hmac
from collections.abc import Callable, Mapping

import falcon

from vgi_rpc.rpc import AuthContext


def bearer_authenticate(
    *,
    validate: Callable[[str], AuthContext],
) -> Callable[[falcon.Request], AuthContext]:
    """Create a bearer-token authenticate callback.

    Returns a callback compatible with ``make_wsgi_app(authenticate=...)``.
    Extracts the ``Authorization: Bearer <token>`` header and delegates
    validation to the user-supplied ``validate`` callback.

    Args:
        validate: Callable that receives the raw bearer token string and
            returns an ``AuthContext`` on success.  Must raise ``ValueError``
            when the token is invalid or unknown.

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    """

    def authenticate(req: falcon.Request) -> AuthContext:
        auth_header = req.get_header("Authorization") or ""
        if not auth_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")
        token = auth_header[7:]
        return validate(token)

    return authenticate


def bearer_authenticate_static(
    *,
    tokens: Mapping[str, AuthContext],
) -> Callable[[falcon.Request], AuthContext]:
    """Create a bearer-token authenticate callback from a static token map.

    Convenience wrapper around ``bearer_authenticate`` that looks up the
    token in a pre-built mapping.

    The lookup uses :func:`hmac.compare_digest` against each known token
    rather than a hash-table lookup so the comparison runs in constant
    time relative to a single token.  ``dict.get`` would short-circuit
    string comparison on the first mismatching byte and let a remote
    attacker brute-force a valid token byte-by-byte through response
    timing; this avoids that side channel at the cost of an O(n) scan
    over the typically tiny token set.

    Args:
        tokens: Mapping from bearer token strings to ``AuthContext`` values.

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    """
    # Pre-encode known tokens once so the hot path only does encode + compare.
    encoded_tokens: list[tuple[bytes, AuthContext]] = [(k.encode("utf-8"), v) for k, v in tokens.items()]

    def validate(token: str) -> AuthContext:
        token_b = token.encode("utf-8")
        match: AuthContext | None = None
        for known, ctx in encoded_tokens:
            # Always run compare_digest for every entry — short-circuiting
            # on the first hit would re-introduce the timing side channel.
            if hmac.compare_digest(token_b, known) and match is None:
                match = ctx
        if match is None:
            raise ValueError("Unknown bearer token")
        return match

    return bearer_authenticate(validate=validate)


def chain_authenticate(
    *authenticators: Callable[[falcon.Request], AuthContext],
) -> Callable[[falcon.Request], AuthContext]:
    """Chain multiple authenticate callbacks, trying each in order.

    Each authenticator is called in sequence.  ``ValueError`` (bad or missing
    credentials) causes the next authenticator to be tried.  Any other
    exception (e.g. ``PermissionError``) propagates immediately.

    Args:
        *authenticators: One or more authenticate callbacks to try in order.

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    Raises:
        ValueError: At construction time if no authenticators are provided.

    """
    if not authenticators:
        raise ValueError("chain_authenticate requires at least one authenticator")

    for auth_fn in authenticators:
        if isinstance(auth_fn, PreconditionGate):
            raise TypeError(
                f"chain_authenticate received a precondition gate ({auth_fn.name!r}). "
                "Chaining is OR semantics — the first authenticator that succeeds wins — "
                "so a gate placed in a chain can be bypassed by any later member. "
                "A gate is a requirement, not an alternative: wrap the credential with "
                "require_all(gate, inner) instead, and chain the results if you need "
                "several alternative credentials behind the same gate.",
            )

    def authenticate(req: falcon.Request) -> AuthContext:
        last_error: ValueError | None = None
        reasons: list[str] = []
        for auth_fn in authenticators:
            try:
                return auth_fn(req)
            except ValueError as exc:
                last_error = exc
                reasons.append(str(exc) or type(exc).__name__)
        # Surface every authenticator's reason. The middleware logs and returns
        # ``str(exc)``, so collapsing to a bare "nothing accepted it" would throw
        # away the only diagnostic an operator gets for a 401.
        detail = "; ".join(reasons)
        raise ValueError(f"No authenticator accepted the request ({detail})") from last_error

    return authenticate


class PreconditionGate:
    """An authenticate callback that is a *requirement*, not an alternative.

    A gate answers "may this request be served at all?" — for example, did it
    arrive through a trusted proxy. It is not a credential and carries no
    caller identity, so it must be ANDed with whatever authenticates the user
    rather than offered alongside it.

    Wrapping it in a distinct type is what lets :func:`chain_authenticate`
    reject it at construction time. Chaining a gate would silently make it
    bypassable, and that mistake is invisible in review and in testing until
    someone omits the gate's header and is served anyway.

    Attributes:
        name: Short identifier used in error messages and claims.

    """

    __slots__ = ("_fn", "claims_key", "name")

    def __init__(
        self,
        fn: Callable[[falcon.Request], Mapping[str, str]],
        *,
        name: str,
        claims_key: str,
    ) -> None:
        """Wrap a gate callable.

        Args:
            fn: Callable that verifies the precondition and returns the claims
                to merge on success. It MUST raise ``PermissionError`` on
                failure — ``ValueError`` is swallowed by chain composition.
            name: Short identifier for error messages.
            claims_key: Key under which the gate's claims are merged.

        """
        self._fn = fn
        self.name = name
        self.claims_key = claims_key

    def __call__(self, req: falcon.Request) -> Mapping[str, str]:
        """Verify the precondition, returning claims to merge.

        Raises:
            PermissionError: If the precondition does not hold.

        """
        return self._fn(req)


def require_all(
    gate: PreconditionGate,
    inner: Callable[[falcon.Request], AuthContext] | None = None,
) -> Callable[[falcon.Request], AuthContext]:
    """Compose a precondition gate with a credential (AND semantics).

    The gate runs first. If it fails, ``inner`` is never invoked and the
    request is refused. Otherwise ``inner`` determines the caller's identity
    and the gate's claims are merged into the resulting context under the
    gate's ``claims_key``.

    This is deliberately not a general N-way AND. ``AuthContext`` is frozen
    with a single ``domain`` and ``principal``, so an N-way AND would have no
    defensible answer for whose identity wins — and whatever it picked would
    become a cross-language contract. Here exactly one authenticator produces
    identity, so the question does not arise.

    Args:
        gate: The precondition to enforce. Fails with ``PermissionError``,
            which chain composition propagates rather than swallowing.
        inner: The credential that establishes caller identity. When ``None``
            the gate alone authenticates — "only my proxy may call this
            worker", with user identity handled upstream.

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    """
    if not isinstance(gate, PreconditionGate):
        raise TypeError(f"require_all expects a PreconditionGate, got {type(gate).__name__}")

    def authenticate(req: falcon.Request) -> AuthContext:
        claims = gate(req)
        if inner is None:
            return AuthContext(
                domain=gate.name,
                authenticated=True,
                principal=claims.get("proxy"),
                claims={gate.claims_key: claims},
            )
        ctx = inner(req)
        merged = dict(ctx.claims)
        merged[gate.claims_key] = claims
        return dataclasses.replace(ctx, claims=merged)

    return authenticate
