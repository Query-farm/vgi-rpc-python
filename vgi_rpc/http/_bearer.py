# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bearer token and chain authentication factories for vgi-rpc HTTP transport.

Provides ``bearer_authenticate()`` for opaque bearer token validation,
``bearer_authenticate_static()`` for static token-to-context mapping, and
``chain_authenticate()`` for composing multiple authenticate callbacks.

No extra dependencies beyond ``falcon`` and ``vgi_rpc.rpc.AuthContext``.
"""

from __future__ import annotations

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

    Args:
        tokens: Mapping from bearer token strings to ``AuthContext`` values.

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    """

    def validate(token: str) -> AuthContext:
        ctx = tokens.get(token)
        if ctx is None:
            raise ValueError("Unknown bearer token")
        return ctx

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

    def authenticate(req: falcon.Request) -> AuthContext:
        last_error: ValueError | None = None
        for auth_fn in authenticators:
            try:
                return auth_fn(req)
            except ValueError as exc:
                last_error = exc
        raise ValueError("No authenticator accepted the request") from last_error

    return authenticate
