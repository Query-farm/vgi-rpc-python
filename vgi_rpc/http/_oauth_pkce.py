# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Server-side OAuth PKCE authorization code flow for vgi-rpc HTTP browse pages.

When both ``authenticate`` and ``oauth_resource_metadata`` (with a ``client_id``)
are configured, this module enables browser-based authentication:

1. A browser GET that would return 401 is instead redirected to the
   authorization server's login page (with PKCE code challenge).
2. After the user authenticates, the authorization server redirects back
   to ``{prefix}/_oauth/callback`` with an authorization code.
3. The callback exchanges the code for a token, stores it in a JS-readable
   cookie, and redirects back to the original page.

Requires ``pip install vgi-rpc[oauth]`` (httpx).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import struct
import threading
import time
from collections.abc import Callable
from urllib.parse import urlencode, urlparse

import falcon
import httpx

from vgi_rpc.rpc import AuthContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SESSION_COOKIE_NAME = "_vgi_oauth_session"
_AUTH_COOKIE_NAME = "_vgi_auth"
_SESSION_COOKIE_VERSION = 4  # v4 adds return_to field for external frontends
_SESSION_MAX_AGE = 600  # 10 minutes
_AUTH_COOKIE_DEFAULT_MAX_AGE = 3600  # 1 hour fallback
_MAX_ORIGINAL_URL_LEN = 2048
_HMAC_LEN = 32

# ---------------------------------------------------------------------------
# PKCE helpers (RFC 7636)
# ---------------------------------------------------------------------------


def _generate_code_verifier() -> str:
    """Generate a 43-character URL-safe random code verifier (RFC 7636 S4.1)."""
    return secrets.token_urlsafe(32)


def _generate_code_challenge(code_verifier: str) -> str:
    """Compute S256 code challenge from a code verifier (RFC 7636 S4.2)."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _generate_state_nonce() -> str:
    """Generate a random state nonce for CSRF protection."""
    return secrets.token_urlsafe(24)


# ---------------------------------------------------------------------------
# Derived HMAC key — prevents cross-protocol forgery with stream state tokens
# ---------------------------------------------------------------------------


def _derive_session_key(signing_key: bytes) -> bytes:
    """Derive a separate HMAC key for OAuth session cookies."""
    return hmac.new(signing_key, b"oauth-pkce-session", hashlib.sha256).digest()


# ---------------------------------------------------------------------------
# Signed session cookie (stores code_verifier + state + original URL)
# ---------------------------------------------------------------------------


def _pack_oauth_cookie(
    code_verifier: str,
    state_nonce: str,
    original_url: str,
    session_key: bytes,
    created_at: int | None = None,
    return_to: str = "",
) -> str:
    """Pack PKCE session data into a signed, base64-encoded cookie value.

    Wire format::

        [1 byte:  version=4           (uint8)]
        [8 bytes: created_at          (uint64 LE, seconds since epoch)]
        [2 bytes: cv_len              (uint16 LE)]
        [cv_len bytes: code_verifier  (UTF-8)]
        [2 bytes: state_len           (uint16 LE)]
        [state_len bytes: state_nonce (UTF-8)]
        [2 bytes: url_len             (uint16 LE)]
        [url_len bytes: original_url  (UTF-8)]
        [2 bytes: rt_len              (uint16 LE)]
        [rt_len bytes: return_to      (UTF-8)]
        [32 bytes: HMAC-SHA256(session_key, all above)]

    """
    if created_at is None:
        created_at = int(time.time())
    cv_bytes = code_verifier.encode("utf-8")
    state_bytes = state_nonce.encode("utf-8")
    url_bytes = original_url.encode("utf-8")
    rt_bytes = return_to.encode("utf-8")
    payload = (
        struct.pack("B", _SESSION_COOKIE_VERSION)
        + struct.pack("<Q", created_at)
        + struct.pack("<H", len(cv_bytes))
        + cv_bytes
        + struct.pack("<H", len(state_bytes))
        + state_bytes
        + struct.pack("<H", len(url_bytes))
        + url_bytes
        + struct.pack("<H", len(rt_bytes))
        + rt_bytes
    )
    mac = hmac.new(session_key, payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(payload + mac).decode("ascii")


def _unpack_oauth_cookie(
    cookie_value: str,
    session_key: bytes,
    max_age: int = _SESSION_MAX_AGE,
) -> tuple[str, str, str, str]:
    """Unpack and verify a signed OAuth session cookie.

    Returns:
        ``(code_verifier, state_nonce, original_url, return_to)``

    Raises:
        ValueError: On tampered, expired, or malformed cookies.

    """
    try:
        raw = base64.urlsafe_b64decode(cookie_value)
    except Exception as exc:
        raise ValueError("Malformed session cookie") from exc

    # Minimum: version(1) + timestamp(8) + 4 x length(2) + HMAC(32) = 49
    if len(raw) < 49:
        raise ValueError("Session cookie too short")

    # Verify HMAC before inspecting payload
    payload = raw[:-_HMAC_LEN]
    received_mac = raw[-_HMAC_LEN:]
    expected_mac = hmac.new(session_key, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(received_mac, expected_mac):
        raise ValueError("Session cookie signature mismatch")

    # Parse payload
    version = struct.unpack_from("B", payload, 0)[0]
    if version != _SESSION_COOKIE_VERSION:
        raise ValueError(f"Unexpected session cookie version: {version}")

    created_at = struct.unpack_from("<Q", payload, 1)[0]
    if max_age > 0:
        age = int(time.time()) - created_at
        if age < 0 or age > max_age:
            raise ValueError(f"Session cookie expired (age={age}s, max={max_age}s)")

    pos = 9
    cv_len = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    code_verifier = payload[pos : pos + cv_len].decode("utf-8")
    pos += cv_len

    state_len = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    state_nonce = payload[pos : pos + state_len].decode("utf-8")
    pos += state_len

    url_len = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    original_url = payload[pos : pos + url_len].decode("utf-8")
    pos += url_len

    rt_len = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    return_to = payload[pos : pos + rt_len].decode("utf-8")

    return code_verifier, state_nonce, original_url, return_to


# ---------------------------------------------------------------------------
# OIDC discovery cache
# ---------------------------------------------------------------------------


def _create_oidc_discovery(issuer: str) -> Callable[[], tuple[str, str] | None]:
    """Create a thread-safe callable that lazily caches OIDC discovery.

    Returns a callable that returns ``(authorization_endpoint, token_endpoint)``
    or ``None`` on failure.
    """
    lock = threading.Lock()
    cached: tuple[str, str] | None = None
    fetched = False

    def discover() -> tuple[str, str] | None:
        nonlocal cached, fetched
        if fetched:
            return cached
        with lock:
            if fetched:
                return cached
            try:
                url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
                with httpx.Client() as client:
                    resp = client.get(url, timeout=10.0)
                    resp.raise_for_status()
                    data = resp.json()
                cached = (data["authorization_endpoint"], data["token_endpoint"])
                logger.debug("OIDC discovery from %s: auth=%s token=%s", issuer, cached[0], cached[1])
            except Exception:
                logger.warning("OIDC discovery failed for %s", issuer, exc_info=True)
                cached = None
            fetched = True
            return cached

    return discover


# ---------------------------------------------------------------------------
# Token exchange
# ---------------------------------------------------------------------------


def _exchange_code_for_token(
    token_endpoint: str,
    code: str,
    redirect_uri: str,
    code_verifier: str,
    client_id: str,
    client_secret: str | None,
    use_id_token: bool,
) -> tuple[str, int]:
    """Exchange an authorization code for a token.

    Returns:
        ``(token, max_age_seconds)``

    Raises:
        ValueError: On token exchange failure.

    """
    form_data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
        "client_id": client_id,
    }
    if client_secret is not None:
        form_data["client_secret"] = client_secret

    try:
        with httpx.Client() as client:
            resp = client.post(token_endpoint, data=form_data, timeout=15.0)
            resp.raise_for_status()
            body = resp.json()
    except Exception as exc:
        raise ValueError(f"Token exchange failed: {exc}") from exc

    if use_id_token:
        token = body.get("id_token")
        if not token:
            raise ValueError("Token response missing id_token")
        # Derive max_age from the id_token's exp claim
        try:
            # Decode JWT payload without verification (just for exp)
            parts = token.split(".")
            if len(parts) >= 2:
                padding = 4 - len(parts[1]) % 4
                payload_json = base64.urlsafe_b64decode(parts[1] + "=" * padding)
                claims = json.loads(payload_json)
                exp = claims.get("exp")
                if exp is not None:
                    max_age = max(int(exp) - int(time.time()), 60)
                    return token, max_age
        except Exception:
            pass
        return token, _AUTH_COOKIE_DEFAULT_MAX_AGE
    else:
        token = body.get("access_token")
        if not token:
            raise ValueError("Token response missing access_token")
        expires_in = body.get("expires_in", _AUTH_COOKIE_DEFAULT_MAX_AGE)
        return token, int(expires_in)


# ---------------------------------------------------------------------------
# Original URL validation
# ---------------------------------------------------------------------------


def _validate_original_url(url: str, prefix: str) -> str:
    """Validate the original URL is relative and within the expected prefix."""
    if len(url) > _MAX_ORIGINAL_URL_LEN:
        url = url[:_MAX_ORIGINAL_URL_LEN]
    parsed = urlparse(url)
    if parsed.scheme or parsed.netloc:
        # Not a relative URL — fall back to the prefix root
        return prefix or "/"
    if prefix and not url.startswith(prefix):
        return prefix or "/"
    return url


def _validate_return_to(url: str) -> str:
    """Validate an external return-to URL. Returns empty string if invalid."""
    if not url or len(url) > 2048:
        return ""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return ""
    if not parsed.netloc:
        return ""
    return url


# ---------------------------------------------------------------------------
# Error HTML page
# ---------------------------------------------------------------------------

_OAUTH_ERROR_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Authentication Error</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 600px;
         margin: 0 auto; padding: 60px 20px; color: #2c2c1e; text-align: center;
         background: #faf8f0; }}
  h1 {{ color: #8b0000; }}
  .detail {{ background: #f0ece0; padding: 12px 20px; border-radius: 6px;
             font-family: monospace; margin: 20px 0; text-align: left; }}
  a {{ color: #2d5016; }}
</style>
</head>
<body>
<h1>Authentication Error</h1>
<p>{message}</p>
{detail}
<p><a href="{retry_url}">Try again</a></p>
</body>
</html>"""


def _oauth_error_page(message: str, detail: str | None, retry_url: str) -> bytes:
    """Render a user-friendly OAuth error page."""
    import html as _html

    detail_html = f'<div class="detail">{_html.escape(detail)}</div>' if detail else ""
    return _OAUTH_ERROR_HTML.format(
        message=_html.escape(message),
        detail=detail_html,
        retry_url=_html.escape(retry_url),
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Falcon resource: OAuth callback
# ---------------------------------------------------------------------------


class _OAuthCallbackResource:
    """Falcon resource for ``GET {prefix}/_oauth/callback``.

    Handles the redirect from the authorization server after user login.
    Validates state, exchanges code for token, sets auth cookie, redirects.
    """

    __slots__ = (
        "_client_id",
        "_client_secret",
        "_cookie_path",
        "_oidc_discovery",
        "_prefix",
        "_redirect_uri",
        "_secure_cookie",
        "_session_key",
        "_use_id_token",
    )

    def __init__(
        self,
        session_key: bytes,
        oidc_discovery: Callable[[], tuple[str, str] | None],
        client_id: str,
        client_secret: str | None,
        use_id_token: bool,
        prefix: str,
        secure_cookie: bool,
        redirect_uri: str,
    ) -> None:
        self._session_key = session_key
        self._oidc_discovery = oidc_discovery
        self._client_id = client_id
        self._client_secret = client_secret
        self._use_id_token = use_id_token
        self._prefix = prefix
        self._secure_cookie = secure_cookie
        self._cookie_path = prefix or "/"
        self._redirect_uri = redirect_uri

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Handle the OAuth callback."""
        retry_url = self._prefix or "/"

        # Check for authorization server error
        error = req.get_param("error")
        if error:
            error_desc = req.get_param("error_description") or error
            logger.warning("OAuth callback error: %s (%s)", error, error_desc)
            resp.status = "400 Bad Request"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "The authorization server returned an error.",
                error_desc,
                retry_url,
            )
            return

        # Extract code and state from query
        code = req.get_param("code")
        state = req.get_param("state")
        if not code or not state:
            resp.status = "400 Bad Request"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "Missing authorization code or state parameter.",
                None,
                retry_url,
            )
            return

        # Read and validate session cookie
        session_cookie = req.cookies.get(_SESSION_COOKIE_NAME)
        if not session_cookie:
            resp.status = "400 Bad Request"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "Session cookie missing or expired. Please try again.",
                None,
                retry_url,
            )
            return

        try:
            code_verifier, expected_state, original_url, return_to = _unpack_oauth_cookie(
                session_cookie, self._session_key
            )
        except ValueError as exc:
            logger.warning("OAuth session cookie invalid: %s", exc)
            resp.status = "400 Bad Request"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "Session expired or invalid. Please try again.",
                None,
                retry_url,
            )
            return

        # CSRF: validate state matches
        if not hmac.compare_digest(state, expected_state):
            logger.warning("OAuth state mismatch")
            resp.status = "400 Bad Request"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "State mismatch — possible CSRF. Please try again.",
                None,
                retry_url,
            )
            return

        # Discover token endpoint
        endpoints = self._oidc_discovery()
        if endpoints is None:
            resp.status = "502 Bad Gateway"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "Could not reach the authorization server.",
                "OIDC discovery failed.",
                retry_url,
            )
            return

        _, token_endpoint = endpoints

        # Exchange code for token
        try:
            token, max_age = _exchange_code_for_token(
                token_endpoint=token_endpoint,
                code=code,
                redirect_uri=self._redirect_uri,
                code_verifier=code_verifier,
                client_id=self._client_id,
                client_secret=self._client_secret,
                use_id_token=self._use_id_token,
            )
        except ValueError as exc:
            logger.warning("OAuth token exchange failed: %s", exc)
            resp.status = "502 Bad Gateway"
            resp.content_type = "text/html; charset=utf-8"
            resp.data = _oauth_error_page(
                "Token exchange with the authorization server failed.",
                str(exc),
                retry_url,
            )
            return

        logger.info("OAuth PKCE authentication successful")

        # External frontend: redirect with token in URL fragment (no cookie needed)
        if return_to:
            separator = "#" if "#" not in return_to else "&"
            redirect_url = f"{return_to}{separator}token={token}"
            logger.info("OAuth redirecting to external frontend: %s", return_to.split("?")[0])
            resp.status = "302 Found"
            resp.set_header("Location", redirect_url)
            resp.set_header("Cache-Control", "no-cache, no-store, must-revalidate")
            resp.content_type = "text/html; charset=utf-8"
            resp.data = None
            resp.text = None
            # Clear session cookie (path must match where it was set)
            resp.set_cookie(
                _SESSION_COOKIE_NAME,
                "",
                max_age=0,
                path=f"{self._prefix}/_oauth/",
                secure=self._secure_cookie,
                http_only=True,
                same_site="Lax",
            )
            return

        # Same-origin: redirect to original page with cookies
        original_url = _validate_original_url(original_url, self._prefix)
        resp.status = "302 Found"
        resp.set_header("Location", original_url)
        resp.set_header("Cache-Control", "no-cache, no-store, must-revalidate")
        resp.content_type = "text/html; charset=utf-8"
        resp.data = None
        resp.text = None

        # Auth cookie (JS-readable for WASM — no HttpOnly)
        resp.set_cookie(
            _AUTH_COOKIE_NAME,
            token,
            max_age=max_age,
            path=self._cookie_path,
            secure=self._secure_cookie,
            http_only=False,
            same_site="Lax",
        )
        # Clear session cookie
        resp.set_cookie(
            _SESSION_COOKIE_NAME,
            "",
            max_age=0,
            path=f"{self._prefix}/_oauth/",
            secure=self._secure_cookie,
            http_only=True,
            same_site="Lax",
        )


# ---------------------------------------------------------------------------
# Falcon resource: OAuth logout
# ---------------------------------------------------------------------------


class _OAuthLogoutResource:
    """Falcon resource for ``GET {prefix}/_oauth/logout``.

    Clears the auth cookie and redirects to the landing page.
    """

    __slots__ = ("_cookie_path", "_prefix", "_secure_cookie")

    def __init__(self, prefix: str, secure_cookie: bool) -> None:
        self._prefix = prefix
        self._secure_cookie = secure_cookie
        self._cookie_path = prefix or "/"

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Clear auth cookie and redirect to landing page."""
        resp.set_cookie(
            _AUTH_COOKIE_NAME,
            "",
            max_age=0,
            path=self._cookie_path,
            secure=self._secure_cookie,
            http_only=False,
        )
        raise falcon.HTTPFound(self._prefix or "/")


# ---------------------------------------------------------------------------
# PKCE redirect middleware
# ---------------------------------------------------------------------------


class _OAuthPkceMiddleware:
    """Falcon middleware that intercepts 401s on browser GETs and redirects
    to the OAuth authorization endpoint with PKCE.

    Only activates when the response is 401, the request is GET, and the
    Accept header contains ``text/html``.
    """

    __slots__ = (
        "_client_id",
        "_oidc_discovery",
        "_prefix",
        "_redirect_uri",
        "_scope",
        "_secure_cookie",
        "_session_key",
    )

    def __init__(
        self,
        session_key: bytes,
        oidc_discovery: Callable[[], tuple[str, str] | None],
        client_id: str,
        prefix: str,
        secure_cookie: bool,
        redirect_uri: str,
        scope: str = "openid email",
    ) -> None:
        self._session_key = session_key
        self._oidc_discovery = oidc_discovery
        self._client_id = client_id
        self._prefix = prefix
        self._secure_cookie = secure_cookie
        self._redirect_uri = redirect_uri
        self._scope = scope

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """If 401 on a browser GET, redirect to OAuth authorization."""
        # Only intercept 401s on browser GET requests
        if req.method != "GET":
            return
        status = resp.status
        if isinstance(status, str):
            if not status.startswith("401"):
                return
        elif isinstance(status, int):
            if status != 401:
                return
        else:
            return

        # Only redirect browsers (Accept: text/html)
        accept = req.get_header("Accept") or ""
        if "text/html" not in accept:
            return

        # Discover authorization endpoint
        endpoints = self._oidc_discovery()
        if endpoints is None:
            logger.warning("PKCE redirect skipped: OIDC discovery failed")
            return  # Fall through to normal 401

        authorization_endpoint, _ = endpoints

        # Generate PKCE parameters
        code_verifier = _generate_code_verifier()
        code_challenge = _generate_code_challenge(code_verifier)
        state_nonce = _generate_state_nonce()

        # Capture original URL
        original_url = req.path
        if req.query_string:
            original_url = f"{original_url}?{req.query_string}"
        original_url = _validate_original_url(original_url, self._prefix)

        # Check for external frontend return URL
        return_to = _validate_return_to(req.get_param("_vgi_return_to") or "")

        # Pack session cookie
        cookie_value = _pack_oauth_cookie(
            code_verifier, state_nonce, original_url, self._session_key, return_to=return_to
        )

        # Build authorization URL
        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "state": state_nonce,
            "scope": self._scope,
        }
        auth_url = f"{authorization_endpoint}?{urlencode(params)}"

        logger.debug("OAuth PKCE redirect to %s (original: %s)", authorization_endpoint, original_url)

        # Overwrite the 401 response with a 302 redirect
        resp.status = "302 Found"
        resp.set_header("Location", auth_url)
        resp.set_header("Cache-Control", "no-cache, no-store, must-revalidate")
        resp.content_type = "text/html; charset=utf-8"
        resp.data = None
        resp.text = None

        # Set session cookie
        resp.set_cookie(
            _SESSION_COOKIE_NAME,
            cookie_value,
            max_age=_SESSION_MAX_AGE,
            path=f"{self._prefix}/_oauth/",
            secure=self._secure_cookie,
            http_only=True,
            same_site="Lax",
        )


# ---------------------------------------------------------------------------
# Cookie authenticate function
# ---------------------------------------------------------------------------


def make_cookie_authenticate(
    inner: Callable[[falcon.Request], AuthContext],
    cookie_name: str = _AUTH_COOKIE_NAME,
) -> Callable[[falcon.Request], AuthContext]:
    """Create an authenticate callback that reads a bearer token from a cookie.

    Extracts the token from the named cookie and delegates validation to the
    ``inner`` authenticator by temporarily injecting an ``Authorization``
    header into the WSGI environ.  The original header value is always
    restored in a ``finally`` block.

    Intended for use with ``chain_authenticate``::

        authenticate = chain_authenticate(
            original_authenticate,                      # tries Authorization header
            make_cookie_authenticate(original_authenticate),  # falls back to cookie
        )

    Args:
        inner: The original authenticate callback (e.g. ``jwt_authenticate``).
        cookie_name: Name of the cookie containing the bearer token.

    Returns:
        A callback ``(falcon.Request) -> AuthContext``.

    """

    def authenticate(req: falcon.Request) -> AuthContext:
        token = req.cookies.get(cookie_name)
        if not token:
            raise ValueError("No auth cookie")
        # Temporarily inject the cookie token as an Authorization header
        # so the inner authenticator (JWT, bearer, etc.) can validate it.
        # Safe in synchronous WSGI — the environ is per-request and mutable.
        saved = req.env.get("HTTP_AUTHORIZATION")
        req.env["HTTP_AUTHORIZATION"] = f"Bearer {token}"
        try:
            return inner(req)
        finally:
            if saved is None:
                req.env.pop("HTTP_AUTHORIZATION", None)
            else:
                req.env["HTTP_AUTHORIZATION"] = saved

    return authenticate


# ---------------------------------------------------------------------------
# User-info JS snippet for landing/describe pages
# ---------------------------------------------------------------------------

_USER_INFO_STYLE = """\
#vgi-user-info {
  position: fixed; top: 12px; right: 16px; z-index: 1000;
  font-family: 'Inter', system-ui, sans-serif; font-size: 0.85em;
  display: flex; align-items: center; gap: 8px;
  background: #fff; border: 1px solid #e0ddd0; border-radius: 20px;
  padding: 4px 14px 4px 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
#vgi-user-info img {
  width: 26px; height: 26px; border-radius: 50%;
}
#vgi-user-info .email { color: #2c2c1e; font-weight: 500; }
#vgi-user-info a {
  color: #6b6b5a; text-decoration: none; margin-left: 4px;
  font-size: 0.9em;
}
#vgi-user-info a:hover { color: #8b0000; }
"""

_USER_INFO_SCRIPT = """\
(function() {{
  var c = document.cookie.match('(^|;)\\\\s*{cookie_name}=([^;]+)');
  if (!c) return;
  try {{
    var parts = c[2].split('.');
    var payload = JSON.parse(atob(parts[1].replace(/-/g,'+').replace(/_/g,'/')));
    var el = document.getElementById('vgi-user-info');
    if (!el) return;
    var html = '';
    if (payload.picture) html += '<img src="' + payload.picture + '" alt="">';
    html += '<span class="email">' + (payload.email || payload.sub || '') + '</span>';
    html += '<a href="{logout_url}">Sign out</a>';
    el.innerHTML = html;
  }} catch(e) {{}}
}})();"""


def build_user_info_html(prefix: str) -> str:
    """Return HTML snippet (style + div + script) for user info display.

    Injected into landing/describe page templates when PKCE is active.
    """
    logout_url = f"{prefix}/_oauth/logout"
    return (
        f"<style>{_USER_INFO_STYLE}</style>\n"
        f'<div id="vgi-user-info"></div>\n'
        f"<script>{_USER_INFO_SCRIPT.format(cookie_name=_AUTH_COOKIE_NAME, logout_url=logout_url)}</script>"
    )
