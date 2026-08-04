# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Falcon error serializer and 404 sink for unmatched routes.

The 401 serializer is the server half of ``docs/unauthorized-spec.md``: one
reason code, one detail, and — on a service whose auth depends on a reverse
proxy — one note saying so, rendered as a page for a browser and as JSON for
everything else.
"""

from __future__ import annotations

import html as _html
import json
from collections.abc import Callable
from typing import Any

import falcon

from .._common import (
    _ERROR_PAGE_STYLE,
    _FONT_IMPORTS,
    _VGI_LOGO_HTML,
    AUTH_PROXY_REQUIRED_HEADER,
    AUTH_REASON_HEADER,
)
from .._unauthorized import AuthReason

_NOT_FOUND_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>404 &mdash; vgi-rpc</title>
"""
    + _FONT_IMPORTS
    + _ERROR_PAGE_STYLE
    + """
</head>
<body>
"""
    + _VGI_LOGO_HTML
    + """
<h1>404 &mdash; Not Found</h1>
<p>This is a <code>vgi-rpc</code> service endpoint{protocol_fragment}.</p>
<p>RPC methods are available under <code>{prefix}/&lt;method&gt;</code>.</p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>"""
)

_UNAUTHORIZED_HTML_TEMPLATE = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>401 &mdash; Unauthorized</title>
"""
    + _FONT_IMPORTS
    + _ERROR_PAGE_STYLE
    + """
</head>
<body>
"""
    + _VGI_LOGO_HTML
    + """
<h1>401 &mdash; Unauthorized</h1>
<div class="reason">{reason}</div>
<p>Authentication is required to access this <code>vgi-rpc</code> service.</p>
{detail}
{note}
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>"""
)

# Keyed by (reason, detail); the proxy note is fixed per app, so it is baked
# into the closure rather than the key. Bounded because `detail` is chosen by
# the authenticate callback and a callback is free to embed request data in it.
_UNAUTHORIZED_CACHE_LIMIT = 64


def _render_unauthorized_html(reason: AuthReason, detail: str, proxy_hint: str) -> bytes:
    """Render the 401 page for a browser.

    Args:
        reason: The reason code, shown as a chip under the heading.
        detail: Human-readable rejection text, or ``""`` to omit the block.
        proxy_hint: The proxy-configuration note, or ``""`` to omit it.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    detail_html = f'<div class="detail">{_html.escape(detail)}</div>' if detail else ""
    note_html = (
        f'<div class="note"><strong>Is the reverse proxy configured?</strong>{_html.escape(proxy_hint)}</div>'
        if proxy_hint
        else ""
    )
    return _UNAUTHORIZED_HTML_TEMPLATE.format(
        reason=_html.escape(reason.value),
        detail=detail_html,
        note=note_html,
    ).encode("utf-8")


def _render_unauthorized_json(reason: AuthReason, detail: str, proxy_hint: str) -> bytes:
    """Render the 401 envelope for a non-browser client.

    Args:
        reason: The reason code.
        detail: Human-readable rejection text.
        proxy_hint: The proxy-configuration note, omitted from the payload
            when empty so that its presence alone is a usable signal.

    Returns:
        UTF-8 encoded JSON bytes.

    """
    payload: dict[str, str] = {"error": "unauthorized", "reason": reason.value, "detail": detail}
    if proxy_hint:
        payload["proxy_hint"] = proxy_hint
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _wants_html(req: falcon.Request) -> bool:
    """Report whether the caller is a browser that should get the styled page.

    Substring-matching ``Accept`` rather than doing full media-type
    negotiation is deliberate and matches the OAuth PKCE redirect middleware:
    the only clients that ask for ``text/html`` are browsers, and the Arrow
    client sends ``*/*``, which must resolve to JSON.

    Args:
        req: The Falcon request.

    Returns:
        ``True`` when the request explicitly accepts HTML.

    """
    return "text/html" in (req.get_header("Accept") or "")


def _make_error_serializer(proxy_hint: str = "") -> Callable[[falcon.Request, falcon.Response, falcon.HTTPError], None]:
    """Build the Falcon error serializer for one app.

    Only ``HTTPUnauthorized`` (401) is given the standardized treatment; every
    other error falls back to Falcon's default JSON serialization.

    Args:
        proxy_hint: The app's static proxy-configuration note, or ``""`` when
            its authentication does not depend on a reverse proxy. Fixed for
            the life of the app — see :mod:`vgi_rpc.http._unauthorized` for
            why it is not derived per request.

    Returns:
        A serializer suitable for ``falcon.App.set_error_serializer``.

    """
    html_cache: dict[tuple[AuthReason, str], bytes] = {}
    json_cache: dict[tuple[AuthReason, str], bytes] = {}

    def _serialize(req: falcon.Request, resp: falcon.Response, exc: falcon.HTTPError) -> None:
        """Serialize one Falcon error onto the response."""
        if not isinstance(exc, falcon.HTTPUnauthorized):
            resp.content_type = falcon.MEDIA_JSON
            resp.data = exc.to_json()
            return

        # _AuthMiddleware stashes the classified reason here. Anything else
        # that raises HTTPUnauthorized — a resource, a third-party middleware
        # — lands on the unclassified fallback rather than being guessed at.
        reason = getattr(req.context, "vgi_auth_reason", AuthReason.UNAUTHORIZED)
        if not isinstance(reason, AuthReason):
            reason = AuthReason.UNAUTHORIZED
        detail = exc.description or ""

        resp.set_header(AUTH_REASON_HEADER, reason.value)
        if proxy_hint:
            resp.set_header(AUTH_PROXY_REQUIRED_HEADER, "true")
        # A 401 is request-specific and may flip to 200 on the next attempt
        # with a credential; a shared cache holding onto it would be a bug.
        resp.set_header("Cache-Control", "no-store")

        key = (reason, detail)
        if _wants_html(req):
            body = html_cache.get(key)
            if body is None:
                body = _render_unauthorized_html(reason, detail, proxy_hint)
                if len(html_cache) < _UNAUTHORIZED_CACHE_LIMIT:
                    html_cache[key] = body
            resp.content_type = "text/html; charset=utf-8"
        else:
            body = json_cache.get(key)
            if body is None:
                body = _render_unauthorized_json(reason, detail, proxy_hint)
                if len(json_cache) < _UNAUTHORIZED_CACHE_LIMIT:
                    json_cache[key] = body
            resp.content_type = falcon.MEDIA_JSON
        resp.data = body

    return _serialize


def _make_not_found_sink(
    prefix: str,
    protocol_name: str,
) -> Callable[..., None]:
    """Create a Falcon sink that returns a 404 HTML page for unmatched routes.

    The HTML is pre-rendered at creation time so there is zero per-request
    template overhead.

    Args:
        prefix: The URL prefix for RPC endpoints (e.g. ``/vgi``).
        protocol_name: The protocol name from the ``RpcServer``.

    Returns:
        A Falcon sink callable.

    """
    protocol_fragment = f" serving <strong>{_html.escape(protocol_name)}</strong>" if protocol_name else ""
    body_bytes = _NOT_FOUND_HTML_TEMPLATE.format(
        prefix=_html.escape(prefix),
        protocol_fragment=protocol_fragment,
    ).encode("utf-8")

    def _not_found_sink(req: falcon.Request, resp: falcon.Response, **kwargs: Any) -> None:
        """Return an HTML 404 page for requests that do not match any RPC route."""
        resp.status = "404"
        resp.content_type = "text/html; charset=utf-8"
        resp.data = body_bytes

    return _not_found_sink
