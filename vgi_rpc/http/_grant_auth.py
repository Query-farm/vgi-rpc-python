# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bearer authenticators that close the ``vgi_rpc.Identity.v1`` loop.

``issue_grant`` mints a credential "presented later by unattended automation
as an ordinary bearer", and ``resolve_token`` answers which principal an opaque
credential is -- but until this module neither fed back into authentication.
Two authenticators do (WIRE_PROTOCOL.md §16, "Accepting identity credentials"):

- :func:`grant_authenticate` accepts the framework's own sealed grants.
- :func:`resolve_token_authenticate` asks the worker's ``resolve_token``.

:func:`compose_identity_authenticate` puts them after the deployment's own
authenticator, in the normative order -- JWT / static first, then sealed grants
(a cheap prefix check), then ``resolve_token`` -- and ``make_wsgi_app`` calls it
automatically for a server hosting ``vgi_rpc.Identity.v1``.

Routing is by prefix and is strict in both directions.  A token without the
``vgig1.`` prefix never reaches the grant verifier.  A token *with* it that does
not verify is refused outright (401) and never reaches ``resolve_token``: a
forged or stale grant must not get a second chance from a resolver that might
answer for it, and handing a third party a credential this deployment minted
is the same leak the JWS trap closes for introspection.
"""

from __future__ import annotations

from collections.abc import Callable

import falcon

from vgi_rpc.errors import AuthUnavailableError
from vgi_rpc.grants import GRANT_TOKEN_PREFIX, GrantInvalidError, GrantKeys, verify_grant_token
from vgi_rpc.http._bearer import chain_authenticate
from vgi_rpc.http._unauthorized import REASON_ATTR, AuthFailure, AuthReason, proxy_headers_of
from vgi_rpc.rpc import AuthContext
from vgi_rpc.rpc._token_identity import (
    MAX_TOKEN_BYTES,
    IdentityUnavailableError,
    TokenIdentity,
    reject_jws_shaped,
)

__all__ = [
    "GRANT_AUTH_DOMAIN",
    "TOKEN_AUTH_DOMAIN",
    "compose_identity_authenticate",
    "grant_authenticate",
    "resolve_token_authenticate",
]

#: ``AuthContext.domain`` of a grant-authenticated request.
GRANT_AUTH_DOMAIN = "grant"

#: ``AuthContext.domain`` of a request authenticated through ``resolve_token``.
TOKEN_AUTH_DOMAIN = "token"

_BEARER = "Bearer "


class GrantRejectedError(PermissionError):
    """A ``vgig1.`` credential that did not verify.

    A :class:`PermissionError`, not a :class:`ValueError`, on purpose:
    ``chain_authenticate`` advances to the next authenticator on
    ``ValueError``, and a grant-shaped token must stop here rather than fall
    through to ``resolve_token``.  The middleware still answers 401.
    """

    def __init__(self, detail: str, reason: AuthReason) -> None:
        """Build the refusal.

        Args:
            detail: Operator-facing text; never contains the token.
            reason: The 401 reason code.

        """
        super().__init__(detail)
        setattr(self, REASON_ATTR, reason)


def _bearer(req: falcon.Request) -> str:
    """Return the bearer credential, or raise ``AuthFailure`` (fall through)."""
    header = req.get_header("Authorization") or ""
    if not header:
        raise AuthFailure(AuthReason.MISSING_CREDENTIAL, "Missing Authorization header")
    if not header.startswith(_BEARER):
        raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "Authorization header is not a Bearer credential")
    return header[len(_BEARER) :]


def grant_authenticate(keys: GrantKeys) -> Callable[[falcon.Request], AuthContext]:
    """Accept the framework's own sealed grants as bearer credentials.

    The resulting ``AuthContext`` has ``domain="grant"``, the grant's
    principal, and claims ``{grant_id, scopes, purpose}`` -- and **no
    ``auth_time``**, so a grant-authenticated caller cannot ``issue_grant``:
    grants never mint grants.

    Args:
        keys: The deployment's grant configuration.

    Returns:
        An authenticate callback.  A bearer without the ``vgig1.`` prefix
        raises ``ValueError`` (the chain moves on); one with it that does not
        verify raises :class:`GrantRejectedError` (401, the chain stops).

    """

    def authenticate(req: falcon.Request) -> AuthContext:
        token = _bearer(req)
        if not token.startswith(GRANT_TOKEN_PREFIX):
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "not a sealed grant")
        try:
            claims = verify_grant_token(keys, token)
        except GrantInvalidError as exc:
            reason = AuthReason.EXPIRED_CREDENTIAL if exc.expired else AuthReason.INVALID_CREDENTIAL
            raise GrantRejectedError(f"sealed grant rejected: {exc}", reason) from exc
        return AuthContext(
            domain=GRANT_AUTH_DOMAIN,
            authenticated=True,
            principal=claims.principal,
            claims={"grant_id": claims.grant_id, "scopes": list(claims.scopes), "purpose": claims.purpose},
        )

    return authenticate


def resolve_token_authenticate(
    resolve_token: Callable[[str], TokenIdentity | None],
) -> Callable[[falcon.Request], AuthContext]:
    """Accept bearer credentials the worker's ``resolve_token`` resolves.

    ``None`` from the hook means "unknown" and raises ``ValueError`` -- the
    chain falls through, and ends in 401 if nothing else accepts.  An outage
    (``AuthUnavailableError``, or ``IdentityUnavailableError`` from the same
    hook) propagates as ``AuthUnavailableError``: 503 with ``Retry-After``,
    never 401, so a blip does not log a fleet out.

    The hook never sees a ``vgig1.`` token, a JWS-shaped token (validated
    locally against a key set, never routed onward), or one over 4096 UTF-8
    bytes -- the same shape guards ``introspect_token`` applies.

    Args:
        resolve_token: The worker's hook.

    Returns:
        An authenticate callback producing ``domain="token"``, the resolved
        principal, and ``{"token_name": ...}`` as claims.

    """

    def authenticate(req: falcon.Request) -> AuthContext:
        token = _bearer(req)
        if token.startswith(GRANT_TOKEN_PREFIX):
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "sealed grants are not resolved by resolve_token")
        if len(token.encode("utf-8")) > MAX_TOKEN_BYTES or not token.strip():
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "bearer credential rejected")
        try:
            reject_jws_shaped(token)
        except Exception as exc:
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "a JWS is not resolved by resolve_token") from exc
        try:
            identity = resolve_token(token)
        except IdentityUnavailableError as exc:
            raise AuthUnavailableError(
                exc.detail or "identity lookup unavailable", retry_after=int(exc.retry_after)
            ) from exc
        if identity is None:
            raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "bearer credential did not resolve")
        return AuthContext(
            domain=TOKEN_AUTH_DOMAIN,
            authenticated=True,
            principal=identity.principal,
            claims={"token_name": identity.token_name},
        )

    return authenticate


def _anonymous_without_credentials(req: falcon.Request) -> AuthContext:
    """Keep an unauthenticated server unauthenticated for requests with no credential."""
    if req.get_header("Authorization"):
        raise AuthFailure(AuthReason.INVALID_CREDENTIAL, "bearer credential not accepted")
    return AuthContext.anonymous()


def compose_identity_authenticate(
    authenticate: Callable[[falcon.Request], AuthContext] | None,
    *,
    grant_keys: GrantKeys | None,
    resolve_token: Callable[[str], TokenIdentity | None] | None,
) -> Callable[[falcon.Request], AuthContext] | None:
    """Append the identity bearer authenticators after the deployment's own.

    Order: *authenticate* (JWT, static, ...), then sealed grants, then
    ``resolve_token``.  With neither identity source, *authenticate* is
    returned unchanged.  With no *authenticate*, a request carrying no
    ``Authorization`` header stays anonymous, exactly as before; a request
    carrying one that nothing accepts is 401.

    Args:
        authenticate: The deployment's authenticator, or ``None``.  It must
            raise ``ValueError`` for a credential it does not recognise --
            one that answers anonymous for everything ends the chain first.
        grant_keys: Sealed-grant configuration, or ``None``.
        resolve_token: The worker's ``resolve_token``, or ``None``.

    Returns:
        The composed authenticator.

    Raises:
        ValueError: *authenticate* depends on proxy-injected evidence (a
            proxy-proof gate, mTLS headers).  Appending alternatives with OR
            semantics would let a grant or resolved token bypass it, so the
            server refuses to start; compose explicitly with
            ``require_all(gate, chain_authenticate(...))`` and pass
            ``identity_bearer=False``.

    """
    members: list[Callable[[falcon.Request], AuthContext]] = []
    if grant_keys is not None:
        members.append(grant_authenticate(grant_keys))
    if resolve_token is not None:
        members.append(resolve_token_authenticate(resolve_token))
    if not members:
        return authenticate
    if authenticate is None:
        return chain_authenticate(*members, _anonymous_without_credentials)
    if proxy_headers_of(authenticate):
        raise ValueError(
            "this server's authenticate depends on proxy-injected evidence, and accepting sealed grants or "
            "resolve_token bearers would be an OR beside it that bypasses that requirement. Compose it "
            "yourself -- require_all(gate, chain_authenticate(inner, grant_authenticate(keys), "
            "resolve_token_authenticate(hook))) -- and pass make_wsgi_app(identity_bearer=False)."
        )
    return chain_authenticate(authenticate, *members)
