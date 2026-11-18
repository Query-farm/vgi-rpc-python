"""Location delegation: sign in at a Front Desk, then talk to the Field Address it names.

Client side of ``vgi/docs/location-delegation.md`` (the "Pass Holder"). A
server is *delegating* when its resource metadata's authorization server
advertises ``resource_indicators_supported: true`` and the metadata's
``resource`` equals the URL the client was given. Then:

* the client sends RFC 8707 ``resource`` on every token request (and no
  ``scope``), with the built-in public client id ``vgi-duckdb``;
* the token response's ``vgi_location`` is the Field Address every request
  goes to instead of the Desk Address, checked against the allowed delegation
  hosts for that issuer;
* a 401 from the Field Address is answered by one refresh, never by
  rediscovery, so a Field Worker can't steer the client to another
  authorization server.
"""

from __future__ import annotations

import ipaddress
import os
from urllib.parse import urlsplit

__all__ = [
    "DEFAULT_ALLOWED_DELEGATION_HOSTS",
    "DELEGATED_CLIENT_ID",
    "DelegationError",
    "canonical_resource",
    "check_field_address",
    "delegation_allowed_for_issuer",
    "rewrite_to_field",
]

DELEGATED_CLIENT_ID = "vgi-duckdb"

#: Issuer → host patterns its ``vgi_location`` may name. The built-in default;
#: any other issuer may not delegate. ``*.`` matches exactly one DNS label,
#: ``**.`` one or more. A Fly.io app host is one label under ``fly.dev``.
_ORCHARD_FIELD_HOSTS: tuple[str, ...] = ("**.field.queryfarm.cloud", "*.fly.dev")
DEFAULT_ALLOWED_DELEGATION_HOSTS: dict[str, tuple[str, ...]] = {
    "https://auth.queryfarm.cloud": _ORCHARD_FIELD_HOSTS,
    "https://auth.staging.queryfarm.cloud": _ORCHARD_FIELD_HOSTS,
}

_TEST_LOOPBACK_ENV = "VGI_TEST_ALLOW_LOOPBACK_DELEGATION"


class DelegationError(ValueError):
    """A delegating server or its token response broke the delegation rules."""


def canonical_resource(url: str) -> str | None:
    """Return the spec's canonical form of *url* (§1), or ``None`` if it has none."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme.lower() != "https" or parts.username or parts.password or parts.query or parts.fragment:
        return None
    if "\\" in url or any(seg in (".", "..") for seg in parts.path.split("/")):
        return None
    host = (parts.hostname or "").lower()
    if not host:
        return None
    port = parts.port
    netloc = host if port in (None, 443) else f"{host}:{port}"
    return f"https://{netloc}{parts.path.rstrip('/')}"


def _host_matches(host: str, pattern: str) -> bool:
    """Match *host* against ``*.suffix`` (one label), ``**.suffix`` (one or more) or an exact host."""
    for prefix, single in (("**.", False), ("*.", True)):
        if pattern.startswith(prefix):
            suffix = pattern[len(prefix) - 1 :]  # keeps the leading dot
            if not host.endswith(suffix):
                return False
            labels = host[: -len(suffix)].split(".")
            return all(labels) and (not single or len(labels) == 1)
    return host == pattern


def _loopback_allowed() -> bool:
    return os.environ.get(_TEST_LOOPBACK_ENV, "") == "1"


def delegation_allowed_for_issuer(issuer: str, allowed: dict[str, tuple[str, ...]] | None = None) -> bool:
    """Whether *issuer* may delegate at all (it has an allowed-hosts entry)."""
    table = DEFAULT_ALLOWED_DELEGATION_HOSTS if allowed is None else allowed
    if issuer.rstrip("/") in table:
        return True
    if _loopback_allowed():
        host = (urlsplit(issuer).hostname or "").lower()
        return host in ("127.0.0.1", "localhost")
    return False


def check_field_address(issuer: str, location: str, allowed: dict[str, tuple[str, ...]] | None = None) -> str:
    """Validate a ``vgi_location`` for *issuer*; return it without a trailing slash.

    Args:
        issuer: The checked delegating issuer the token response came from.
        location: The token response's ``vgi_location``.
        allowed: Issuer → host patterns; ``None`` uses the built-in default.

    Returns:
        The Field Address, without a trailing slash.

    Raises:
        DelegationError: If it isn't ``https``, carries a path, query or
            userinfo, is an IP literal, or isn't within the issuer's allowed hosts.

    """
    table = DEFAULT_ALLOWED_DELEGATION_HOSTS if allowed is None else allowed
    parts = urlsplit(location)
    host = (parts.hostname or "").lower()
    loopback_ok = _loopback_allowed() and parts.scheme == "http" and host in ("127.0.0.1", "localhost")
    if parts.scheme != "https" and not loopback_ok:
        raise DelegationError(f"vgi_location must be https: {location!r}")
    if parts.username or parts.password or parts.query or parts.fragment or parts.path not in ("", "/"):
        raise DelegationError(f"vgi_location must be a bare origin: {location!r}")
    if loopback_ok:
        return location.rstrip("/")
    try:
        ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        pass
    else:
        raise DelegationError(f"vgi_location may not be an IP address: {location!r}")
    patterns = table.get(issuer.rstrip("/"), ())
    if not any(_host_matches(host, p) for p in patterns):
        raise DelegationError(f"vgi_location {host!r} is not an allowed delegation host for {issuer}")
    return location.rstrip("/")


def rewrite_to_field(url: str, desk_base: str, field: str) -> str | None:
    """Map a request URL under *desk_base* onto the Field Address, or ``None`` if it isn't under it."""
    base = desk_base.rstrip("/")
    if url == base:
        return field
    if url.startswith((base + "/", base + "?")):
        return field + url[len(base) :]
    return None
