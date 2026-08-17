# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Token introspection — resolving an opaque bearer credential to a principal.

A reverse proxy that terminates the only public listener has to know *which
principal a credential authenticates as* before it can authorize anything: that
principal becomes the policy principal, the row-rule literal, and the bind
parameter of every entitlement query.  When the credential is opaque the proxy
may hold no local copy of it, so it has to ask the worker.

**The response is an identity assertion made by the thing being protected, and
the asker acts on it with credentials the worker does not hold** — storage
credentials on the data-plane host, service-credential attachments in an
entitlement resolver, policy-tier selection.  "Trust it as much as you trust the
worker" is therefore the wrong frame: it must be trusted *more*, because it
steers privileges the worker never has.  Every guard below follows from that.

What the endpoint returns is deliberately tiny: a principal, a display name for
the credential, and how long the answer may be cached.  **It never returns
claims.**  A pass-through claims field would let a worker choose its caller's
tenant routing, its row scope, and its policy branch — the single most dangerous
thing this feature could grow.

It is also **not** "replay the credential through the worker's own authenticate
chain", which is the attractive design and breaks four ways: a precondition gate
wrapping the chain makes the replay unimplementable; it would run the worker's
independently-configured audience/issuer set, so a credential the *asker*
rejected could be accepted here; cookie- and mTLS/IP-derived identity cannot be
replayed at all, and a synthesized request carries the proxy's own address,
silently elevating any address-allowlist member; and it invents a fake-request
contract every future authenticator would have to honour with no type to enforce
it.  The resolver is a narrow callable instead.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from http import HTTPStatus

import falcon

from vgi_rpc.http._unauthorized import AuthUnavailableError
from vgi_rpc.rpc import _get_auth_and_metadata

_logger = logging.getLogger("vgi_rpc.http.introspect")

#: Endpoint path, appended to the app's prefix.  Matches the de-facto contract
#: the existing proxy client already speaks; changing it would cost a lockstep
#: release for no benefit.
INTROSPECT_ENDPOINT = "/__introspect_token__"

#: Advertised on every response (including ``OPTIONS /health``) when the route
#: is enabled, so a proxy can preflight at boot rather than discovering at first
#: login that the worker it depends on cannot answer.
INTROSPECT_ENABLED_HEADER = "VGI-Token-Introspection"

#: Three dot-separated base64url segments — a JWS.  Such a credential is
#: validated locally against a key set and MUST NOT be routed here: doing so
#: sends a bearer token the asker may itself have rejected (expired, wrong
#: audience) to a third party that might accept it.
_JWS_SHAPED = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$")

#: Hard cap on the request body.  The generic request-size middleware would
#: otherwise admit megabytes into a JSON parse for a body whose only legitimate
#: content is one credential.
_MAX_BODY_BYTES = 8192

#: Cap on a credential we will even attempt to resolve.  Anything longer is not
#: a bearer token; refusing early keeps a resolver from being handed megabytes.
_MAX_TOKEN_CHARS = 4096


def token_digest(token: str) -> str:
    """Return a SHA-256 hex digest of *token*, for diagnostics.

    The credential itself must never reach a log, a span, or an error message.
    A digest is stable enough to correlate one credential's failures across
    records without being the credential.

    Args:
        token: The opaque credential.

    Returns:
        Lowercase hex digest.

    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TokenIdentity:
    """The identity an opaque credential authenticates as.

    Attributes:
        principal: The canonical principal.  Return it in the exact form the
            worker itself would derive, so an asker that normalises differently
            does not authorize as one identity while the worker serves another.
        token_name: Human-readable name for the credential, for audit trails.
            Never the credential.
        ttl_seconds: How long the answer may be cached.  The caller does the
            caching; this endpoint holds no cache of its own.  Treat it as an
            authorization window, because for any path the asker serves without
            re-presenting the credential it is exactly that.

    """

    principal: str
    token_name: str = ""
    ttl_seconds: int = 300


#: Resolves an opaque credential, returning ``None`` when it does not resolve.
#:
#: Raise :class:`~vgi_rpc.http.AuthUnavailableError` when the answer is not
#: knowable — a backing store that is down is not the same as a credential that
#: is unknown, and a caller that negative-caches the second must not cache the
#: first.
TokenResolver = Callable[[str], "TokenIdentity | None"]


class _RateLimiter:
    """Fixed-window request limiter, keyed by caller.

    Present because the endpoint is a credential→identity oracle even when
    correctly restricted: an allowlisted caller whose own credential leaks can
    still test guesses. Rate limiting does not close that, it bounds it — a
    lower ceiling on how fast an attacker converts guesses to answers.

    Fixed-window rather than a token bucket: a window admits at most twice the
    rate across a boundary, which is a rounding error here, and the state is
    two integers per caller rather than a float that has to be aged.
    """

    __slots__ = ("_counts", "_lock", "_per_window", "_window", "_window_start")

    def __init__(self, per_window: int, window_seconds: float = 1.0) -> None:
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


class _TokenIntrospectionResource:
    """``POST {prefix}/__introspect_token__`` — credential to principal.

    Registered only when a resolver is supplied, so a worker cannot grow this
    oracle by upgrading a dependency.

    Two rejection axes, deliberately distinguishable from each other and
    deliberately uniform within themselves:

    * **403** — the caller may not introspect. Authentication is not the same
      capability as introspection: a deployment where any valid credential can
      introspect lets any user test guesses of any other user's credential at
      unlimited rate, and resolve a stolen one to its owner.
    * **404** — the *subject* credential did not resolve. Unknown, expired and
      malformed are one answer, because reporting which would confirm that a
      guessed credential exists.

    Both are definitive: a caller may cache them. Anything transient must reach
    the caller as 5xx so it is retried rather than cached.
    """

    __slots__ = ("_limiter", "_principals", "_resolver")

    def __init__(
        self,
        resolver: TokenResolver,
        principals: frozenset[str],
        rate_limit_per_second: int,
    ) -> None:
        self._resolver = resolver
        self._principals = principals
        self._limiter = _RateLimiter(rate_limit_per_second)

    @staticmethod
    def _refuse(resp: falcon.Response, status: HTTPStatus, error: str) -> None:
        """Write a rejection carrying no detail about why."""
        resp.status = status
        resp.content_type = falcon.MEDIA_JSON
        resp.data = json.dumps({"error": error}, separators=(",", ":")).encode()
        # A credential's resolution can change; nothing here may sit in a
        # shared cache.
        resp.set_header("Cache-Control", "no-store")

    def _read_token(self, req: falcon.Request) -> str | None:
        """Return the subject credential, or ``None`` if the body is unusable."""
        length = req.content_length
        if length is not None and length > _MAX_BODY_BYTES:
            return None
        raw = req.bounded_stream.read(_MAX_BODY_BYTES + 1)
        if len(raw) > _MAX_BODY_BYTES:
            return None
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            return None
        if not isinstance(body, dict):
            return None
        token = body.get("token")
        if not isinstance(token, str) or not token or len(token) > _MAX_TOKEN_CHARS:
            return None
        return token

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Resolve the posted credential to a principal."""
        auth, _metadata = _get_auth_and_metadata()
        caller = auth.principal or ""

        # Caller authorization first: an unauthorized caller must not learn
        # anything about a subject credential, including how long it took.
        if not auth.authenticated or caller not in self._principals:
            _logger.warning(
                "introspection refused: caller is not an introspector",
                extra={
                    "remote_addr": req.remote_addr or "",
                    "principal": caller,
                    "authenticated": auth.authenticated,
                },
            )
            self._refuse(resp, HTTPStatus.FORBIDDEN, "not_an_introspector")
            return

        if not self._limiter.allow(caller):
            _logger.warning(
                "introspection rate limit exceeded",
                extra={"remote_addr": req.remote_addr or "", "principal": caller},
            )
            resp.set_header("Retry-After", "1")
            self._refuse(resp, HTTPStatus.TOO_MANY_REQUESTS, "rate_limited")
            return

        token = self._read_token(req)
        if token is None:
            # Indistinguishable from an unresolvable credential: a malformed
            # body is not worth a separate signal, and giving one lets a caller
            # probe the parser.
            self._refuse(resp, HTTPStatus.NOT_FOUND, "unresolved")
            return

        digest = token_digest(token)

        if _JWS_SHAPED.match(token):
            # Refused without ever reaching the resolver. A JWS is validated
            # locally against a key set; one arriving here is either a caller
            # bug or an attempt to have this worker vouch for a token its
            # asker already rejected.
            _logger.warning(
                "introspection refused: JWS-shaped subject",
                extra={"principal": caller, "token_digest": digest},
            )
            self._refuse(resp, HTTPStatus.NOT_FOUND, "unresolved")
            return

        try:
            identity = self._resolver(token)
        except AuthUnavailableError as exc:
            # "I could not find out" is not "it did not resolve". Refusing with
            # 404 here would hand the caller a *definitive* answer for an outage,
            # and a caller that negative-caches definitive answers — which is the
            # correct thing to do — would cache it. Deliberately not routed
            # through `_refuse`: that shape is for definitive rejections, and a
            # transient needs `Retry-After` instead of `Cache-Control: no-store`.
            _logger.warning(
                "introspection unavailable: %s",
                exc,
                extra={
                    "principal": caller,
                    "token_digest": digest,
                    "error_type": type(exc).__name__,
                },
            )
            raise falcon.HTTPServiceUnavailable(
                description=str(exc),
                retry_after=exc.retry_after,
            ) from exc
        if identity is None:
            _logger.info(
                "introspection: credential did not resolve",
                extra={"principal": caller, "token_digest": digest},
            )
            self._refuse(resp, HTTPStatus.NOT_FOUND, "unresolved")
            return

        _logger.info(
            "introspection: resolved",
            extra={
                "principal": caller,
                "token_digest": digest,
                "resolved_principal": identity.principal,
            },
        )
        resp.content_type = falcon.MEDIA_JSON
        resp.set_header("Cache-Control", "no-store")
        resp.data = json.dumps(
            {
                "principal": identity.principal,
                "token_name": identity.token_name,
                "ttl_seconds": identity.ttl_seconds,
            },
            separators=(",", ":"),
        ).encode()


class _IntrospectionDisabledResource:
    """Answers ``404 not_enabled`` when introspection is off.

    The oracle is still absent in every sense that matters: no resolver is
    held, nothing is looked up, and the answer does not depend on the request.
    What this adds is a *definitive* answer for a caller that asks anyway.

    Without it the path falls through to the generic ``{method}`` RPC route,
    which rejects a JSON body with ``415``. A caller that classifies
    ``401/403/404`` as definitive and everything else as transient — which is
    the sensible classification, and the one the existing proxy client uses —
    reads ``415`` as "try again later" and retries forever against a worker
    that will never support the feature. A misconfiguration should stop, not
    spin.

    Deliberately no authentication requirement of its own: "this worker does
    not do introspection" is not a secret, and a caller needs to learn it at
    preflight rather than after arranging credentials.
    """

    __slots__ = ()

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Report that this worker does not implement introspection."""
        resp.status = HTTPStatus.NOT_FOUND
        resp.content_type = falcon.MEDIA_JSON
        resp.data = json.dumps({"error": "not_enabled"}, separators=(",", ":")).encode()
        resp.set_header("Cache-Control", "no-store")


def _normalise_principals(principals: Iterable[str] | None) -> frozenset[str]:
    """Validate the introspector allowlist.

    Args:
        principals: Principals permitted to introspect.

    Returns:
        The allowlist as a frozenset.

    Raises:
        ValueError: If the allowlist is missing or empty.  There is no
            permissive default: "any authenticated caller" is precisely the
            configuration that turns this endpoint into an open oracle, so it
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
