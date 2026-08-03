# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The standardized shape of an HTTP 401 from a vgi-rpc service.

Every 401 a vgi-rpc server emits carries the same three things: a reason code
from a closed set, a human-readable detail, and — when the service's own
configuration says requests must arrive through a reverse proxy — a note
saying so. The normative cross-language contract is
``docs/unauthorized-spec.md``; this module is its reference implementation.

The reason code exists because the detail string is not something a client can
branch on. It is deliberately coarse: it names the *stage* that refused the
request, never the verifier's internal diagnosis. Whether a signature was
malformed or simply wrong is an operator's business, not a caller's, and
echoing that back is how a rejection turns into an oracle.

The proxy note is derived from **server configuration**, not from what failed
on this particular request. A worker that requires proxy-injected evidence
emits the same note on every 401 it produces, so the note reveals nothing a
caller could not already read off the service's advertised capabilities — and
it stays useful in the case operators actually hit, where the proxy is simply
not forwarding the header and the credential was never the problem.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from enum import StrEnum
from typing import Any

from vgi_rpc.rpc import RpcError

__all__ = [
    "AuthFailure",
    "AuthReason",
    "AuthenticationError",
    "build_proxy_hint",
    "classify_auth_failure",
    "declare_proxy_headers",
    "merge_proxy_headers",
    "proxy_headers_of",
]


class AuthReason(StrEnum):
    """The closed set of reason codes a 401 may carry.

    Coarse by design — see the module docstring. A port that cannot map one
    of its failures onto a specific member uses :attr:`UNAUTHORIZED` rather
    than inventing a code, so the set stays closed across languages.
    """

    MISSING_CREDENTIAL = "missing_credential"
    """No credential was presented at all."""

    INVALID_CREDENTIAL = "invalid_credential"
    """A credential was presented and rejected."""

    EXPIRED_CREDENTIAL = "expired_credential"
    """A well-formed credential that is outside its validity window."""

    INSUFFICIENT_SCOPE = "insufficient_scope"
    """The caller was identified but is not permitted."""

    PROXY_REQUIRED = "proxy_required"
    """The request did not carry evidence that it came through the trusted proxy."""

    UNAUTHORIZED = "unauthorized"
    """Refused, unclassified. The fallback for a custom authenticator."""


# Every exception that wants to steer the reason code sets this attribute.
# Duck-typed rather than isinstance-checked so that an authenticator defined
# outside this package — or in a module this one must not import, such as
# ``_proof`` — can participate without a dependency edge. The name is
# deliberately not ``reason``: ``ProofError.reason`` already means the
# verifier's internal code, which must never reach a caller.
REASON_ATTR = "vgi_auth_reason"


class AuthFailure(ValueError):
    """A rejected credential, carrying its reason code.

    Subclasses :class:`ValueError` so that
    :func:`vgi_rpc.http.chain_authenticate` still treats it as "try the next
    credential". A precondition that must *not* be swallowed that way raises
    :class:`PermissionError` instead — see :class:`vgi_rpc.http.ProofError`.
    """

    def __init__(self, reason: AuthReason, detail: str = "") -> None:
        """Create a failure with a reason code and an optional detail message.

        Args:
            reason: The code to surface to the caller.
            detail: Human-readable text. Defaults to the reason code itself.

        """
        super().__init__(detail or reason.value)
        self.reason = reason
        setattr(self, REASON_ATTR, reason)


class AuthenticationError(RpcError):
    """Client-side view of a 401, with the server's reason code unpacked.

    Subclasses :class:`~vgi_rpc.rpc.RpcError` with ``error_type`` fixed at
    ``"AuthenticationError"``, so code that catches ``RpcError`` or matches on
    that type keeps working; the added attributes are for callers that want to
    branch — retry after refreshing a token on
    :attr:`AuthReason.EXPIRED_CREDENTIAL`, give up on
    :attr:`AuthReason.INSUFFICIENT_SCOPE`.
    """

    def __init__(
        self,
        reason: AuthReason,
        detail: str,
        proxy_hint: str = "",
        *,
        request_id: str = "",
    ) -> None:
        """Build the error from a parsed 401 envelope.

        Args:
            reason: The reason code the server reported.
            detail: The server's human-readable text.
            proxy_hint: The server's proxy-configuration note, if any. It is
                appended to the exception message rather than left on an
                attribute alone, because the place this actually gets read is
                a traceback in a deployment log.
            request_id: Correlation id, when one is known.

        """
        message = detail or reason.value
        if proxy_hint:
            message = f"{message}\n\n{proxy_hint}"
        super().__init__("AuthenticationError", message, "", request_id=request_id)
        self.reason = reason
        self.detail = detail
        self.proxy_hint = proxy_hint


def classify_auth_failure(exc: BaseException) -> AuthReason:
    """Map an authenticate-callback exception onto a reason code.

    Args:
        exc: The exception raised by the ``authenticate`` callback.

    Returns:
        The declared reason when the exception carries one, otherwise a
        conservative guess: :attr:`AuthReason.INSUFFICIENT_SCOPE` for a
        :class:`PermissionError` (the caller got as far as being identified)
        and :attr:`AuthReason.UNAUTHORIZED` for anything else. Guessing more
        finely would mean matching on message text, which silently
        misclassifies the moment someone rewords a string.

    """
    declared = getattr(exc, REASON_ATTR, None)
    if isinstance(declared, AuthReason):
        return declared
    if isinstance(exc, PermissionError):
        return AuthReason.INSUFFICIENT_SCOPE
    return AuthReason.UNAUTHORIZED


# ---------------------------------------------------------------------------
# Proxy-dependence declaration
# ---------------------------------------------------------------------------

_PROXY_HEADERS_ATTR = "vgi_proxy_headers"


def declare_proxy_headers[F: Callable[..., Any]](fn: F, *headers: str) -> F:
    """Record that *fn* can only succeed on requests a reverse proxy has stamped.

    The built-in mTLS and proxy-proof authenticators call this on themselves,
    so ``make_wsgi_app`` discovers the dependency without the operator
    restating it. Third-party authenticators can call it too; anything not
    declared can still be stated directly via ``make_wsgi_app``'s
    ``proxy_auth_headers``.

    Args:
        fn: The authenticate callback (or gate) to annotate, mutated in place.
        *headers: Header names the proxy must inject. Duplicates and
            already-declared names are collapsed.

    Returns:
        *fn*, so this can wrap a callable at its return site.

    """
    merged = tuple(dict.fromkeys([*proxy_headers_of(fn), *headers]))
    setattr(fn, _PROXY_HEADERS_ATTR, merged)
    return fn


def proxy_headers_of(fn: object) -> tuple[str, ...]:
    """Return the proxy-injected headers *fn* was declared to depend on.

    Args:
        fn: Any object, typically an authenticate callback. ``None`` and
            objects with no declaration are accepted.

    Returns:
        The declared header names, or an empty tuple.

    """
    declared = getattr(fn, _PROXY_HEADERS_ATTR, ())
    if isinstance(declared, tuple | list):
        return tuple(str(name) for name in declared)
    return ()


def merge_proxy_headers(*sources: object) -> tuple[str, ...]:
    """Collect the proxy-header declarations of several callables in order.

    Args:
        *sources: Callables (or anything) to read declarations from.

    Returns:
        The union, first-seen order preserved.

    """
    names: list[str] = []
    for source in sources:
        names.extend(proxy_headers_of(source))
    return tuple(dict.fromkeys(names))


def build_proxy_hint(headers: Iterable[str]) -> str:
    """Compose the operator-facing note about proxy-dependent authentication.

    Args:
        headers: The header names a trusted proxy must inject.

    Returns:
        The note text, or ``""`` when *headers* is empty.

    """
    names = tuple(dict.fromkeys(headers))
    if not names:
        return ""
    listed = ", ".join(names)
    noun = "header" if len(names) == 1 else "headers"
    return (
        f"This service only accepts requests that arrive through its configured reverse proxy, "
        f"which must set the {listed} {noun}. A rejection here is as likely to be a proxy that is "
        f"not forwarding {'that header' if len(names) == 1 else 'those headers'} — or a request "
        f"that reached the service without passing through the proxy at all — as it is a bad "
        f"credential. Check the proxy configuration before rotating credentials."
    )
