# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Falcon error serializer and 404 sink for unmatched routes."""

from __future__ import annotations

import html as _html
from collections.abc import Callable
from typing import Any

import falcon

from .._common import _ERROR_PAGE_STYLE, _FONT_IMPORTS

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
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>
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
<div class="logo">
  <img src="https://vgi-rpc-python.query.farm/assets/logo-hero.png" alt="vgi-rpc logo">
</div>
<h1>401 &mdash; Unauthorized</h1>
<p>Authentication is required to access this <code>vgi-rpc</code> service.</p>
{detail}
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>"""
)

_UNAUTHORIZED_BODY_CACHE: dict[str, bytes] = {}


def _get_unauthorized_body(detail: str) -> bytes:
    """Return cached UTF-8 bytes for a 401 HTML page with the given detail message.

    Args:
        detail: The error description to display on the page.

    Returns:
        UTF-8 encoded HTML bytes.

    """
    body = _UNAUTHORIZED_BODY_CACHE.get(detail)
    if body is not None:
        return body
    detail_html = f'<div class="detail">{_html.escape(detail)}</div>' if detail else ""
    body = _UNAUTHORIZED_HTML_TEMPLATE.format(detail=detail_html).encode("utf-8")
    # Bound cache size to prevent memory issues with unique error messages.
    if len(_UNAUTHORIZED_BODY_CACHE) < 64:
        _UNAUTHORIZED_BODY_CACHE[detail] = body
    return body


def _error_serializer(req: falcon.Request, resp: falcon.Response, exc: falcon.HTTPError) -> None:
    """Serialize Falcon HTTP errors as styled HTML pages.

    Only ``HTTPUnauthorized`` (401) gets a styled HTML page; all other errors
    fall back to Falcon's default JSON serialization.

    Args:
        req: The Falcon request.
        resp: The Falcon response.
        exc: The Falcon HTTP error being serialized.

    """
    if isinstance(exc, falcon.HTTPUnauthorized):
        resp.content_type = "text/html; charset=utf-8"
        resp.data = _get_unauthorized_body(exc.description or "")
    else:
        resp.content_type = falcon.MEDIA_JSON
        resp.data = exc.to_json()


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
