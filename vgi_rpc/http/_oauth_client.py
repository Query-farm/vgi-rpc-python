# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Client-driven OAuth login (device-code + PKCE) for the vgi-rpc HTTP client.

Everything in ``_oauth.py`` / ``_oauth_pkce.py`` is server-side: it gates a
vgi-rpc worker's own endpoints, or drives a *browser*-redirect login for a
human whose browser already hit the worker. This module is the missing other
half -- code that runs *inside a vgi-rpc HTTP client process* and obtains its
own bearer token, with no server involved on this side of the exchange.

It is a port of the DuckDB VGI extension's C++ OAuth client
(``vgi_oauth.cpp`` / ``vgi_oauth.hpp`` in the sibling ``vgi`` repository),
which has been running this exact flow logic in production. See that file's
comments for the reasoning behind each of the less-obvious choices ported
here (the secret-less-proxy ``token_endpoint`` override, the device-vs-PKCE
client selection precedence, the 45s refresh skew, etc.) -- they are not
repeated at length in every docstring below.

Slice 1 (current): RFC 8414 authorization-server discovery, the RFC 8628
device-code request+poll loop, token refresh, and the ``VgiOAuthAuth``
state machine that ties it to an ``httpx2.Client`` via the standard
``httpx2.Auth`` hook. PKCE (browser-driven authorization-code flow with a
local loopback callback server) is slice 2 and is not yet implemented --
``flow="pkce"`` (or "auto" mode when a server offers only PKCE) raises a
clear ``NotImplementedError`` rather than silently doing nothing.

Two deliberate deviations from the C++ reference, both explained in the
implementation plan this module was built from:

- The internal HTTP calls this module makes on its own behalf (resource
  metadata discovery, RFC 8414 discovery, device-authorization/token
  endpoint POSTs) use a plain, unauthenticated ``httpx2.Client`` -- never
  one with this module's own :class:`VgiOAuthAuth` attached as ``auth=``.
  Only the caller-facing client (the one actually issuing VGI RPCs) has
  that. This means nothing this module does internally can recurse back
  into :meth:`VgiOAuthAuth.sync_auth_flow`, so the C++ reference's
  same-thread re-entrancy guard has a narrower job here than there (see
  :class:`VgiOAuthAuth`'s docstring).
- ``invalid_grant`` detection is a structured check on
  :class:`OAuthTokenError`'s ``.error_code`` attribute, not a substring
  search over a formatted exception message -- the C++ reference does the
  latter (``e.what().find("invalid_grant")``), which is exactly the
  "keying on message text" pattern that same file's own
  ``PkceCallbackBindFailure`` comment warns against.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, Literal, NoReturn

import httpx2

from ._client import (
    OAuthResourceMetadataResponse,
    OAuthServerMetadata,
    fetch_auth_server_metadata,
    fetch_oauth_metadata,
    parse_www_authenticate_params,
)
from ._pkce_math import decode_jwt_payload_unverified

logger = logging.getLogger("vgi_rpc.oauth")

__all__ = [
    "OAuthChallenge",
    "OAuthFlow",
    "OAuthIdentity",
    "OAuthPrompt",
    "OAuthRefreshContext",
    "OAuthTokenError",
    "OAuthTokenSet",
    "VgiOAuthAuth",
    "attempt_token_refresh",
    "is_colab_environment",
    "is_headless_environment",
    "parse_oauth_challenge",
    "perform_device_code_flow",
    "select_device_code_client",
]

OAuthFlow = Literal["auto", "device_code", "pkce"]
OAuthPrompt = Literal["none", "login", "select_account", "consent"]

# How far ahead of expiry a token is treated as already stale. Matches the
# C++ reference's kTokenRefreshSkew: an expired token isn't merely refreshed
# lazily -- the request goes out with no Authorization header at all, the
# server 401s, and the whole request (Arrow IPC payload included) is
# re-sent. Refreshing slightly early trades that for one cheap token
# exchange.
_TOKEN_REFRESH_SKEW_SECONDS = 45.0

# Default RFC 8628 fallbacks when the server's device-authorization response
# omits them.
_DEFAULT_DEVICE_CODE_EXPIRES_IN = 300
_DEFAULT_DEVICE_CODE_INTERVAL = 5
_DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
_MAX_NETWORK_RETRIES = 3


def _redact_secret(value: str) -> str:
    """Redact a secret for inclusion in a diagnostic message.

    Matches the C++ reference's ``DebugSecret`` shape exactly, because the
    same reasoning applies here (arguably more so): exception text travels
    further than the caller's terminal -- loggers, test snapshots,
    error-tracking services, notebook cell output -- so a diagnostic message
    that would otherwise embed a client_secret/refresh_token/code/
    code_verifier verbatim must never do so.
    """
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return f"({len(value)} chars) <redacted>"
    return f"({len(value)} chars) {value[:4]}...{value[-4:]}"


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------


def is_headless_environment() -> bool:
    """Best-effort detection of a headless/non-interactive environment.

    Ported from the C++ reference's ``IsHeadlessEnvironment``. Any one match
    is enough; order doesn't matter since these are independent signals.
    """
    if os.environ.get("SSH_CONNECTION") or os.environ.get("SSH_CLIENT"):
        return True
    if os.environ.get("CI") == "true":
        return True
    if os.environ.get("DOCKER_CONTAINER"):
        return True
    if os.path.exists("/.dockerenv"):
        return True
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return True
    # Assigned to a local rather than compared as `sys.platform == ...` directly:
    # mypy special-cases the latter form as a platform-narrowing literal check, which
    # would make every branch but the one matching *this* dev/CI machine's own OS
    # "unreachable" under `warn_unreachable = true` -- this function's whole point is
    # to run correctly on whichever platform it's actually imported on.
    platform = sys.platform
    if platform == "darwin":
        return not (os.environ.get("TERM_PROGRAM") or os.environ.get("DISPLAY"))
    if platform.startswith(("linux", "freebsd", "openbsd", "netbsd")):
        return not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
    return False  # Windows: always considered to have a GUI, matching the C++ reference.


def is_colab_environment() -> bool:
    """Whether this process is running inside a Google Colab kernel.

    Colab captures the kernel's C-level stderr into the notebook cell, so
    the device-code prompt (URL + user code) written to stderr is visible
    there -- but only if it's actually flushed (see ``_print_prompt_if_colab``).
    """
    return bool(
        os.environ.get("COLAB_RELEASE_TAG") or os.environ.get("COLAB_GPU") or os.environ.get("COLAB_JUPYTER_IP")
    )


def _print_prompt_if_colab(message: str, on_prompt: Callable[[str], None] | None) -> None:
    """Surface a login prompt somewhere a Colab notebook cell will show it.

    ``logger.warning()`` alone isn't enough under Colab -- the Python
    client's log handlers don't reach the notebook cell, so without this a
    device-code login silently appears to hang. Writing to stderr with an
    explicit flush does reach it. When *on_prompt* is given, it always
    fires too (Colab or not), letting a caller intercept the message
    instead of relying on stderr.
    """
    if on_prompt is not None:
        on_prompt(message)
    if is_colab_environment():
        print(f"[vgi_rpc] {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Token / identity types
# ---------------------------------------------------------------------------


@dataclass
class OAuthIdentity:
    """Parsed OIDC id_token claims. Not cryptographically verified.

    The id_token arrived over TLS as part of this client's own OAuth
    exchange -- not attacker-reachable input -- so it's trusted as a
    display/SQL-accessible identity hint, never as an authorization
    decision. Only the four universal OIDC claims are lifted into dedicated
    fields; everything else (provider-specific claims) stays in ``claims``
    verbatim so a caller can reach any of them without this module needing
    per-provider knowledge.

    Attributes:
        present: True iff the id_token parsed as a well-formed JWT with a
            JSON object payload.
        sub: The "sub" claim (subject / user id), or "".
        email: The "email" claim, or "".
        name: The "name" claim, or "".
        issuer: The "iss" claim, or "".
        claims: The full decoded JWT payload.

    """

    present: bool = False
    sub: str = ""
    email: str = ""
    name: str = ""
    issuer: str = ""
    claims: dict[str, object] = field(default_factory=dict)


def _parse_id_token_claims(id_token: str) -> OAuthIdentity:
    """Parse an id_token JWT into an :class:`OAuthIdentity`. Unverified -- see its docstring."""
    payload = decode_jwt_payload_unverified(id_token)
    if payload is None:
        return OAuthIdentity()

    def _str_claim(name: str) -> str:
        value = payload.get(name)
        return value if isinstance(value, str) else ""

    return OAuthIdentity(
        present=True,
        sub=_str_claim("sub"),
        email=_str_claim("email"),
        name=_str_claim("name"),
        issuer=_str_claim("iss"),
        claims=payload,
    )


@dataclass
class OAuthTokenSet:
    """A set of tokens obtained from an authorization server.

    Attributes:
        access_token: The OAuth access token.
        refresh_token: The refresh token, if the server issued one.
        id_token: The OIDC id_token, if the server issued one.
        scope: The granted scope string, as returned by the server.
        expires_at: ``time.monotonic()``-based expiry, or ``None`` if the
            server didn't advertise one (never treated as stale).
        use_id_token: When True, :meth:`bearer_token` returns ``id_token``
            instead of ``access_token`` -- set from the resource metadata's
            ``use_id_token_as_bearer`` flag, not decided per-token.
        identity: Parsed claims from ``id_token`` (``present=False`` if
            there was no id_token or it didn't parse).

    """

    access_token: str = ""
    refresh_token: str = ""
    id_token: str = ""
    scope: str = ""
    expires_at: float | None = None
    use_id_token: bool = False
    identity: OAuthIdentity = field(default_factory=OAuthIdentity)

    def is_valid(self, *, skew: float = _TOKEN_REFRESH_SKEW_SECONDS, now: float | None = None) -> bool:
        """Whether this token set is still usable *skew* seconds from now."""
        if not self.access_token:
            return False
        if self.expires_at is None:
            return True  # No expiry advertised.
        return (now if now is not None else time.monotonic()) + skew < self.expires_at

    def bearer_token(self) -> str:
        """Return the token to send as ``Authorization: Bearer <...>``."""
        if self.use_id_token and self.id_token:
            return self.id_token
        return self.access_token


class OAuthTokenError(Exception):
    """A token/device/refresh endpoint returned a structured OAuth error.

    Attributes:
        error_code: The endpoint's ``error`` field (e.g. ``"invalid_grant"``,
            ``"authorization_pending"``, ``"access_denied"``), or "" if the
            response wasn't a recognizable OAuth error body.
        error_description: The endpoint's ``error_description`` field, if any.

    """

    def __init__(self, message: str, *, error_code: str = "", error_description: str = "") -> None:
        super().__init__(message)
        self.error_code = error_code
        self.error_description = error_description


@dataclass(frozen=True)
class OAuthChallenge:
    """A parsed ``WWW-Authenticate: Bearer ...`` OAuth challenge."""

    resource_metadata_url: str
    client_id: str = ""


@dataclass
class OAuthRefreshContext:
    """Everything needed to silently refresh a token later, without re-discovery.

    Populated once, by whichever flow actually ran (device-code or PKCE) --
    see :func:`select_device_code_client`'s docstring for why the device
    flow's client selection can differ from what's recorded here initially.
    """

    token_endpoint: str = ""
    client_id: str = ""
    client_secret: str = ""
    scope: str = ""
    use_id_token: bool = False
    resource_metadata_url: str = ""


def parse_oauth_challenge(www_authenticate: str) -> OAuthChallenge | None:
    """Parse a ``WWW-Authenticate`` header into an :class:`OAuthChallenge`.

    Returns ``None`` if the header isn't a ``Bearer`` challenge carrying a
    ``resource_metadata`` parameter -- i.e. not an OAuth-protected-resource
    challenge this module knows how to act on.
    """
    stripped = www_authenticate.strip()
    if not stripped:
        return None
    scheme_end = 0
    while scheme_end < len(stripped) and not stripped[scheme_end].isspace():
        scheme_end += 1
    if stripped[:scheme_end].lower() != "bearer":
        return None
    params = parse_www_authenticate_params(www_authenticate)
    resource_metadata_url = params.get("resource_metadata")
    if not resource_metadata_url:
        return None
    return OAuthChallenge(resource_metadata_url=resource_metadata_url, client_id=params.get("client_id", ""))


def select_device_code_client(
    *,
    device_client_id: str,
    device_client_secret: str,
    client_id: str,
    client_secret: str,
    challenge_client_id: str,
) -> tuple[str, str]:
    """Choose which client credentials the device-code flow presents.

    Exact port of the C++ reference's ``SelectDeviceCodeClient``. Precedence:

    1. ``device_client_id`` (+ its own paired ``device_client_secret``), if set --
       pairing a device client id with the "ordinary" client's secret would
       be rejected just as surely as using the wrong id.
    2. Else the ordinary ``client_id`` (+ ``client_secret``).
    3. Else the challenge's ``client_id`` (+ ``client_secret`` -- note the
       secret still comes from resource metadata, never from the challenge,
       since the challenge never carries one).

    Returns:
        ``(client_id, client_secret)``.

    """
    if device_client_id:
        return device_client_id, device_client_secret
    if client_id:
        return client_id, client_secret
    return challenge_client_id, client_secret


def _build_scope_string(resource_meta: OAuthResourceMetadataResponse) -> str:
    return " ".join(resource_meta.scopes_supported) if resource_meta.scopes_supported else "openid"


def _resolve_token_endpoint(resource_meta: OAuthResourceMetadataResponse, server_meta: OAuthServerMetadata) -> str:
    """Resource-metadata's ``token_endpoint`` (the secret-less-proxy override) always wins.

    Used for the PKCE code exchange and for silent refresh -- **not** for
    device-code requests, which always go straight to the authorization
    server's own endpoints (see :func:`perform_device_code_flow`'s
    docstring for why).
    """
    return resource_meta.token_endpoint or server_meta.token_endpoint


def _resource_display_name(resource_meta: OAuthResourceMetadataResponse) -> str:
    if resource_meta.resource_name:
        return resource_meta.resource_name
    resource = resource_meta.resource
    if "://" in resource:
        after_scheme = resource.split("://", 1)[1]
        return after_scheme.split("/", 1)[0]
    return "VGI Service"


def _fetch_resource_and_server_metadata(
    challenge: OAuthChallenge,
    *,
    http_client: httpx2.Client,
) -> tuple[OAuthResourceMetadataResponse, OAuthServerMetadata]:
    resource_meta = fetch_oauth_metadata(challenge.resource_metadata_url, client=http_client)
    if not resource_meta.authorization_servers:
        raise ValueError(f"resource metadata at {challenge.resource_metadata_url} has no authorization_servers")
    server_meta = fetch_auth_server_metadata(resource_meta.authorization_servers[0], client=http_client)
    return resource_meta, server_meta


def _post_form(http_client: httpx2.Client, url: str, data: dict[str, str]) -> httpx2.Response:
    return http_client.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})


def _raise_token_error(context: str, resp: httpx2.Response) -> NoReturn:
    body: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        body = json.loads(resp.content)
    error_code = body.get("error", "") if isinstance(body, dict) else ""
    error_description = body.get("error_description", "") if isinstance(body, dict) else ""
    if error_code:
        raise OAuthTokenError(
            f"{context} failed: {error_code} - {error_description}",
            error_code=error_code,
            error_description=error_description,
        )
    raise OAuthTokenError(f"{context} failed (HTTP {resp.status_code}): {resp.text[:500]}")


def _parse_token_response(resp: httpx2.Response, *, use_id_token: bool) -> OAuthTokenSet:
    body: dict[str, Any] = json.loads(resp.content)
    id_token = body.get("id_token", "") or ""
    tokens = OAuthTokenSet(
        access_token=body.get("access_token", "") or "",
        refresh_token=body.get("refresh_token", "") or "",
        id_token=id_token,
        scope=body.get("scope", "") or "",
        use_id_token=use_id_token,
    )
    expires_in = body.get("expires_in")
    if isinstance(expires_in, (int, float)):
        tokens.expires_at = time.monotonic() + float(expires_in)
    if id_token:
        tokens.identity = _parse_id_token_claims(id_token)
    return tokens


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


def attempt_token_refresh(
    ctx: OAuthRefreshContext,
    refresh_token: str,
    *,
    http_client: httpx2.Client,
) -> OAuthTokenSet:
    """Exchange a refresh_token for a fresh :class:`OAuthTokenSet`.

    Raises:
        OAuthTokenError: On any non-200 response, with ``.error_code`` set
            when the endpoint returned a structured OAuth error body.

    """
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": ctx.client_id,
    }
    if ctx.client_secret:
        data["client_secret"] = ctx.client_secret
    if ctx.scope:
        data["scope"] = ctx.scope
    logger.debug(
        "vgi_rpc.oauth: refreshing token endpoint=%s client_id=%s refresh_token=%s",
        ctx.token_endpoint,
        ctx.client_id,
        _redact_secret(refresh_token),
    )
    resp = _post_form(http_client, ctx.token_endpoint, data)
    if resp.status_code != HTTPStatus.OK:
        _raise_token_error("refresh token request", resp)
    tokens = _parse_token_response(resp, use_id_token=ctx.use_id_token)
    if not tokens.refresh_token:
        # Google (among others) frequently omits refresh_token on a refresh
        # response -- preserve the one we already had rather than losing it.
        tokens.refresh_token = refresh_token
    return tokens


# ---------------------------------------------------------------------------
# Device-code flow (RFC 8628)
# ---------------------------------------------------------------------------


def perform_device_code_flow(
    challenge: OAuthChallenge,
    resource_meta: OAuthResourceMetadataResponse,
    server_meta: OAuthServerMetadata,
    *,
    http_client: httpx2.Client,
    timeout_seconds: float,
    on_prompt: Callable[[str], None] | None = None,
) -> tuple[OAuthTokenSet, OAuthRefreshContext]:
    """Run the full RFC 8628 device-code flow and return the resulting tokens.

    Blocks for as long as it takes a human to visit the verification URL and
    enter the user code, up to *timeout_seconds* (further capped by the
    server's own ``expires_in``).

    Raises:
        ValueError: If the server doesn't advertise a usable device flow.
        OAuthTokenError: If the flow fails (denied, expired, or a
            structured error from the device/token endpoint).

    """
    if not server_meta.device_authorization_endpoint:
        raise ValueError("server has no device_authorization_endpoint")
    if not server_meta.supports_grant_type(_DEVICE_CODE_GRANT_TYPE):
        raise ValueError("server does not advertise support for the device_code grant type")

    client_id, client_secret = select_device_code_client(
        device_client_id=resource_meta.device_code_client_id or "",
        device_client_secret=resource_meta.device_code_client_secret or "",
        client_id=resource_meta.client_id or "",
        client_secret=resource_meta.client_secret or "",
        challenge_client_id=challenge.client_id,
    )
    if not client_id:
        raise ValueError("no client_id available for the device code flow")

    refresh_ctx = OAuthRefreshContext(
        token_endpoint=_resolve_token_endpoint(resource_meta, server_meta),
        client_id=client_id,
        # Never carry a local secret once a secret-less proxy token_endpoint is in play.
        client_secret=client_secret if not resource_meta.token_endpoint else "",
        use_id_token=resource_meta.use_id_token_as_bearer,
        resource_metadata_url=challenge.resource_metadata_url,
        scope=_build_scope_string(resource_meta),
    )

    # Step 1: request a device code.
    resp = _post_form(
        http_client,
        server_meta.device_authorization_endpoint,
        {"client_id": client_id, "scope": refresh_ctx.scope},
    )
    if resp.status_code != HTTPStatus.OK:
        _raise_token_error("device authorization request", resp)
    device_resp: dict[str, Any] = json.loads(resp.content)
    device_code = device_resp.get("device_code", "")
    user_code = device_resp.get("user_code", "")
    verification_uri = device_resp.get("verification_uri") or device_resp.get("verification_url") or ""
    verification_uri_complete = device_resp.get("verification_uri_complete", "")
    if not device_code or not user_code or not verification_uri:
        raise OAuthTokenError("device authorization response missing device_code/user_code/verification_uri")
    expires_in = device_resp.get("expires_in", _DEFAULT_DEVICE_CODE_EXPIRES_IN)
    interval = float(device_resp.get("interval", _DEFAULT_DEVICE_CODE_INTERVAL))

    # Step 2: prompt the human.
    prompt = (
        f"Authentication required for {_resource_display_name(resource_meta)}.\n"
        f"Visit: {verification_uri}\n"
        f"Enter code: {user_code}"
    )
    if verification_uri_complete:
        prompt += f"\nOr visit: {verification_uri_complete}"
    logger.warning(prompt)
    _print_prompt_if_colab(prompt, on_prompt)

    # Step 3: poll.
    effective_timeout = min(timeout_seconds, float(expires_in))
    deadline = time.monotonic() + effective_timeout
    poll_data = {
        "grant_type": _DEVICE_CODE_GRANT_TYPE,
        "device_code": device_code,
        "client_id": client_id,
    }
    if client_secret:
        # Unconditionally the raw device-selected secret -- unlike refresh_ctx.client_secret
        # (which is nulled when a proxy token_endpoint exists), device-code polling never
        # goes through that proxy (see below), so there is no secret to withhold here.
        poll_data["client_secret"] = client_secret

    network_retries = 0
    last_status_print = time.monotonic()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OAuthTokenError(f"device code authentication timed out after {effective_timeout:.0f} seconds")
        time.sleep(min(interval, max(remaining, 0.0)))
        if time.monotonic() >= deadline:
            raise OAuthTokenError(f"device code authentication timed out after {effective_timeout:.0f} seconds")
        now = time.monotonic()
        if now - last_status_print >= 30:
            logger.warning("Still waiting for authentication...")
            _print_prompt_if_colab("Still waiting for authentication...", on_prompt)
            last_status_print = now

        try:
            # Always the authorization server's own token endpoint, never the resource
            # metadata's proxy override -- that proxy (vgi-rpc's own /_oauth/token) only
            # forwards authorization_code/refresh_token grants, not device_code (see the
            # module docstring). refresh_ctx.token_endpoint (possibly the proxy) is still
            # what gets used for *future* silent refresh of the resulting token.
            poll_resp = _post_form(http_client, server_meta.token_endpoint, poll_data)
        except httpx2.HTTPError as exc:
            network_retries += 1
            if network_retries > _MAX_NETWORK_RETRIES:
                raise OAuthTokenError(
                    f"device code polling failed after {_MAX_NETWORK_RETRIES} retries: {exc}"
                ) from exc
            continue

        if poll_resp.status_code >= 500:
            network_retries += 1
            if network_retries > _MAX_NETWORK_RETRIES:
                raise OAuthTokenError(
                    f"device code polling failed after {_MAX_NETWORK_RETRIES} retries (HTTP {poll_resp.status_code})"
                )
            continue

        if poll_resp.status_code == HTTPStatus.OK:
            tokens = _parse_token_response(poll_resp, use_id_token=resource_meta.use_id_token_as_bearer)
            logger.warning("Authentication successful.")
            _print_prompt_if_colab("Authentication successful.", on_prompt)
            return tokens, refresh_ctx

        network_retries = 0
        error_code = ""
        error_description = ""
        try:
            poll_body: dict[str, Any] = json.loads(poll_resp.content)
            error_code = poll_body.get("error", "") or ""
            error_description = poll_body.get("error_description", "") or ""
        except Exception:
            pass

        if error_code == "authorization_pending":
            continue
        if error_code == "slow_down" or poll_resp.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            interval += 5
            continue
        if error_code == "expired_token":
            raise OAuthTokenError("device code expired. Please try again.", error_code=error_code)
        if error_code == "access_denied":
            raise OAuthTokenError("authentication was denied by the user.", error_code=error_code)
        if error_code:
            raise OAuthTokenError(
                f"device code authentication failed: {error_code} - {error_description}",
                error_code=error_code,
                error_description=error_description,
            )
        raise OAuthTokenError(
            f"unexpected response during device code polling (HTTP {poll_resp.status_code}): {poll_resp.text[:500]}"
        )


# ---------------------------------------------------------------------------
# The stateful httpx2.Auth integration
# ---------------------------------------------------------------------------


class _AuthStatus:
    """The four states an OAuth login can be in for one :class:`VgiOAuthAuth`.

    A plain class of string constants rather than ``enum.Enum`` purely so
    ``repr()`` in an error message reads as ``'in_progress'`` and not
    ``<_AuthStatus.IN_PROGRESS: 'in_progress'>``.
    """

    IDLE = "idle"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


class VgiOAuthAuth(httpx2.Auth):
    """An ``httpx2.Auth`` that transparently completes an OAuth login on a 401.

    Attach one instance per ``httpx2.Client`` (equivalently: per
    ``vgi.client.Client``, since that's a 1:1 wrapping) — this is a stateful
    object, not a stateless credential. On every outgoing request it attaches
    a cached, still-valid bearer token if it has one; on a 401 carrying a
    parseable OAuth challenge (``WWW-Authenticate: Bearer
    resource_metadata=...``), it blocks to obtain one (refreshing silently if
    possible, else running an interactive device-code or PKCE login), then
    retries the request exactly once. A 401 with no such challenge is left
    alone.

    State machine (``IDLE``/``IN_PROGRESS``/``COMPLETE``/``FAILED``) and
    locking discipline are a direct port of the C++ VGI extension's
    per-catalog ``OAuthCatalogAuth`` (``vgi_oauth.cpp``): the lock is held
    only for brief state reads/transitions, never across the flow's own
    network I/O, so a second thread hitting a 401 while a flow is already
    running blocks on a condition variable (bounded by
    ``timeout_seconds + 30``) rather than starting a redundant flow of its
    own.

    Re-entrancy: unlike the C++ reference, the internal HTTP calls this
    flow makes on its own behalf (discovery, device/token endpoint POSTs)
    use a plain, unauthenticated ``httpx2.Client`` — never this
    ``VgiOAuthAuth`` itself. So nothing the flow does internally can
    recurse back into :meth:`sync_auth_flow` on the same instance; the
    owner-thread check below exists only for the narrower case of a caller
    reusing this same instance as ``auth=`` on a second, independent
    ``httpx2.Client`` whose request happens to land on the same thread as
    an in-flight flow.
    """

    def __init__(
        self,
        *,
        base_url: str,
        flow: OAuthFlow = "auto",
        refresh_token: str | None = None,
        timeout_seconds: float = 120.0,
        prompt: OAuthPrompt = "none",
        on_prompt: Callable[[str], None] | None = None,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        if flow not in ("auto", "device_code", "pkce"):
            raise ValueError(f"flow must be 'auto', 'device_code', or 'pkce', got {flow!r}")
        if prompt not in ("none", "login", "select_account", "consent"):
            raise ValueError(f"prompt must be 'none', 'login', 'select_account', or 'consent', got {prompt!r}")
        self._base_url = base_url
        self._flow: OAuthFlow = flow
        self._timeout_seconds = timeout_seconds
        self._prompt: OAuthPrompt = prompt
        self._on_prompt = on_prompt

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._status = _AuthStatus.IDLE
        self._token = OAuthTokenSet(refresh_token=refresh_token or "")
        self._refresh_ctx: OAuthRefreshContext | None = None
        self._error_message = ""
        self._owner: int | None = None
        self._interactive = False

        # Deliberately separate from any client this Auth is attached to --
        # see the class docstring's re-entrancy note. *transport* is exposed
        # mainly so tests can inject an httpx2.MockTransport rather than
        # touching a real network or IdP.
        self._flow_http_client = httpx2.Client(follow_redirects=True, timeout=30.0, transport=transport)

    def close(self) -> None:
        """Close the internal HTTP client used for discovery/flow requests."""
        self._flow_http_client.close()

    def sync_auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response]:
        """Attach a cached token; on a 401 OAuth challenge, log in and retry once."""
        token = self._get_cached_token()
        if token:
            request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code != HTTPStatus.UNAUTHORIZED:
            return
        challenge = parse_oauth_challenge(response.headers.get("www-authenticate", ""))
        if challenge is None:
            return  # Not an OAuth-protected-resource challenge -- let the 401 propagate as-is.
        new_token = self._handle_unauthorized(challenge)
        request.headers["Authorization"] = f"Bearer {new_token}"
        yield request

    def _get_cached_token(self) -> str:
        with self._lock:
            if self._status != _AuthStatus.COMPLETE or not self._token.is_valid():
                return ""
            return self._token.bearer_token()

    def _handle_unauthorized(self, challenge: OAuthChallenge) -> str:
        """Obtain a valid bearer token, refreshing or logging in as needed. Blocks. Thread-safe."""
        with self._lock:
            if self._status in (_AuthStatus.IDLE, _AuthStatus.FAILED, _AuthStatus.COMPLETE):
                self._status = _AuthStatus.IN_PROGRESS
                self._owner = threading.get_ident()
                self._error_message = ""
                refresh_token = self._token.refresh_token
                refresh_ctx = self._refresh_ctx
            elif self._status == _AuthStatus.IN_PROGRESS:
                if self._owner == threading.get_ident():
                    raise OAuthTokenError(
                        "nested 401 during an in-progress OAuth flow on the same thread; aborting to avoid "
                        "self-deadlock. This module's own flow requests use an unauthenticated client, so "
                        "this should not normally happen -- if it does, the resource server itself is "
                        "likely misconfigured to require this same auth on its discovery/token endpoints."
                    )
                deadline = time.monotonic() + self._timeout_seconds + 30
                while self._status == _AuthStatus.IN_PROGRESS:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise OAuthTokenError(
                            "timed out waiting for a concurrent OAuth flow on the same client to complete"
                        )
                    self._cv.wait(timeout=remaining)
                if self._status == _AuthStatus.COMPLETE and self._token.is_valid():
                    return self._token.bearer_token()
                if self._status == _AuthStatus.IDLE:
                    raise OAuthTokenError("tokens were cleared during authentication, please retry")
                raise OAuthTokenError(self._error_message or "OAuth authentication failed")
            else:  # pragma: no cover - exhaustive over _AuthStatus's four values
                raise AssertionError(f"unreachable auth status {self._status!r}")

        # Lock released -- the actual (possibly slow, possibly interactive) work happens here,
        # exactly like the C++ reference's AuthState mutex being released before RefreshOrFullAuth.
        try:
            tokens, refresh_ctx, interactive = self._refresh_or_full_auth(challenge, refresh_token, refresh_ctx)
        except OAuthTokenError as exc:
            with self._lock:
                if exc.error_code == "invalid_grant":
                    # Don't retry a known-dead refresh token next time.
                    self._token = OAuthTokenSet()
                self._status = _AuthStatus.FAILED
                self._error_message = str(exc)
                self._owner = None
                self._cv.notify_all()
            raise
        except BaseException as exc:
            # Catch-all, not just OAuthTokenError/Exception: if anything escapes here without
            # storing FAILED, every concurrent waiter hangs until its own bounded timeout instead
            # of failing promptly. Matches the C++ reference's own catch(...) for the same reason.
            with self._lock:
                self._status = _AuthStatus.FAILED
                self._error_message = f"unexpected error during OAuth authentication: {exc}"
                self._owner = None
                self._cv.notify_all()
            raise

        with self._lock:
            self._token = tokens
            self._refresh_ctx = refresh_ctx
            self._interactive = self._interactive or interactive
            self._status = _AuthStatus.COMPLETE
            self._owner = None
            self._cv.notify_all()
            return tokens.bearer_token()

    def _refresh_or_full_auth(
        self,
        challenge: OAuthChallenge,
        refresh_token: str,
        refresh_ctx: OAuthRefreshContext | None,
    ) -> tuple[OAuthTokenSet, OAuthRefreshContext, bool]:
        """Obtain fresh tokens, returning ``(tokens, refresh_ctx, was_interactive)``. Called with no lock held."""
        if refresh_token:
            ctx = refresh_ctx
            if ctx is None or not ctx.token_endpoint:
                try:
                    resource_meta, server_meta = _fetch_resource_and_server_metadata(
                        challenge, http_client=self._flow_http_client
                    )
                except Exception as exc:
                    logger.warning(
                        "vgi_rpc.oauth: could not discover a refresh context (%s); falling back to a fresh login",
                        exc,
                    )
                    ctx = None
                else:
                    ctx = OAuthRefreshContext(
                        token_endpoint=_resolve_token_endpoint(resource_meta, server_meta),
                        client_id=resource_meta.client_id or challenge.client_id,
                        client_secret=((resource_meta.client_secret or "") if not resource_meta.token_endpoint else ""),
                        use_id_token=resource_meta.use_id_token_as_bearer,
                        resource_metadata_url=challenge.resource_metadata_url,
                        scope=_build_scope_string(resource_meta),
                    )
            if ctx is not None and ctx.token_endpoint:
                tokens = attempt_token_refresh(ctx, refresh_token, http_client=self._flow_http_client)
                return tokens, ctx, False

        # No usable refresh path -- a fresh interactive login is required.
        resource_meta, server_meta = _fetch_resource_and_server_metadata(challenge, http_client=self._flow_http_client)
        tokens, new_ctx = self._perform_flow(challenge, resource_meta, server_meta)
        return tokens, new_ctx, True

    def _perform_flow(
        self,
        challenge: OAuthChallenge,
        resource_meta: OAuthResourceMetadataResponse,
        server_meta: OAuthServerMetadata,
    ) -> tuple[OAuthTokenSet, OAuthRefreshContext]:
        has_device_ep = bool(server_meta.device_authorization_endpoint) and server_meta.supports_grant_type(
            _DEVICE_CODE_GRANT_TYPE
        )
        has_auth_ep = bool(server_meta.authorization_endpoint)

        if self._flow == "device_code":
            return perform_device_code_flow(
                challenge,
                resource_meta,
                server_meta,
                http_client=self._flow_http_client,
                timeout_seconds=self._timeout_seconds,
                on_prompt=self._on_prompt,
            )
        if self._flow == "pkce":
            raise NotImplementedError(
                "the PKCE (browser) OAuth flow is not yet implemented in vgi_rpc's Python client "
                "(only device_code is, so far); pass flow='device_code' if the server offers it, "
                "or oauth_refresh_token=... to skip interactive login entirely."
            )
        # flow == "auto". Slice 1: PKCE isn't implemented yet, so auto mode can only ever
        # pick device-code -- but it picks it the same way the C++ reference's auto mode
        # would (server-capability driven), and fails with a specific, actionable message
        # rather than a confusing NotImplementedError from deep inside a flow that was
        # never actually selected.
        if has_device_ep:
            return perform_device_code_flow(
                challenge,
                resource_meta,
                server_meta,
                http_client=self._flow_http_client,
                timeout_seconds=self._timeout_seconds,
                on_prompt=self._on_prompt,
            )
        if has_auth_ep:
            raise NotImplementedError(
                "this server only offers the PKCE authorization-code flow, which vgi_rpc's "
                "Python client doesn't implement yet; pass oauth_refresh_token=... to skip "
                "interactive login, or ask the server operator to enable device-code auth."
            )
        raise ValueError("server has no supported authorization endpoints (neither device-code nor PKCE)")

    def clear_tokens(self) -> None:
        """Reset to a fresh, unauthenticated state."""
        with self._lock:
            self._token = OAuthTokenSet()
            self._refresh_ctx = None
            self._status = _AuthStatus.IDLE
            self._owner = None
            self._error_message = ""
            self._cv.notify_all()

    def identity(self) -> OAuthIdentity | None:
        """Return the signed-in identity's parsed id_token claims, or ``None`` if not (yet) authenticated."""
        with self._lock:
            if self._status != _AuthStatus.COMPLETE or not self._token.identity.present:
                return None
            return self._token.identity

    def was_interactive(self) -> bool:
        """Return True iff an interactive login flow actually ran (as opposed to a silent refresh)."""
        with self._lock:
            return self._interactive
