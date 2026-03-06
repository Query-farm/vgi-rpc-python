# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""RFC 9728 OAuth 2.0 Protected Resource Metadata for vgi-rpc HTTP transport.

Provides ``OAuthResourceMetadata`` configuration and a Falcon resource that
serves ``/.well-known/oauth-protected-resource`` JSON, plus a helper to
build the ``WWW-Authenticate`` header for 401 responses.

No external dependencies beyond Falcon (already required by ``[http]``).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import falcon

_URL_SAFE_RE = re.compile(r"[A-Za-z0-9\-._~]+")


@dataclass(frozen=True)
class OAuthResourceMetadata:
    """RFC 9728 Protected Resource Metadata configuration (server-side).

    Pass an instance to ``make_wsgi_app(oauth_resource_metadata=...)`` to
    enable OAuth discovery.  For the client-side parsed response, see
    ``OAuthResourceMetadataResponse`` in ``vgi_rpc.http._client``.

    Attributes:
        resource: Canonical URL of the protected resource (REQUIRED).
        authorization_servers: Authorization server issuer URLs (REQUIRED,
            must contain at least one entry).
        scopes_supported: OAuth scopes the resource understands.
        bearer_methods_supported: Token delivery methods supported.
        resource_signing_alg_values_supported: JWS algorithms for
            resource-signed responses.
        resource_name: Human-readable name of the resource.
        resource_documentation: URL to developer documentation.
        resource_policy_uri: URL to the resource's privacy policy.
        resource_tos_uri: URL to the resource's terms of service.
        client_id: OAuth client_id that clients should use when
            authenticating with the authorization server.  Custom
            extension (not defined in RFC 9728).
        client_secret: OAuth client_secret that clients should use when
            authenticating with the authorization server.  Custom
            extension (not defined in RFC 9728).
        use_id_token_as_bearer: When ``True``, tells clients to use the
            OIDC ``id_token`` as the Bearer token instead of the
            ``access_token``.  Custom extension (not defined in RFC 9728).
        device_code_client_id: OAuth client_id that clients should use
            specifically for the device code grant flow.  Custom extension
            (not defined in RFC 9728).
        device_code_client_secret: OAuth client_secret that clients should
            use specifically for the device code grant flow.  Custom
            extension (not defined in RFC 9728).

    Raises:
        ValueError: If *resource* is empty or *authorization_servers* is empty.

    """

    resource: str
    authorization_servers: tuple[str, ...]
    scopes_supported: tuple[str, ...] = ()
    bearer_methods_supported: tuple[str, ...] = ("header",)
    resource_signing_alg_values_supported: tuple[str, ...] = ()
    resource_name: str | None = None
    resource_documentation: str | None = None
    resource_policy_uri: str | None = None
    resource_tos_uri: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    use_id_token_as_bearer: bool = False
    device_code_client_id: str | None = None
    device_code_client_secret: str | None = None

    def __post_init__(self) -> None:
        """Validate required fields."""
        if not self.resource:
            raise ValueError("OAuthResourceMetadata.resource must not be empty")
        if not self.authorization_servers:
            raise ValueError("OAuthResourceMetadata.authorization_servers must contain at least one entry")
        if self.client_id is not None and not _URL_SAFE_RE.fullmatch(self.client_id):
            raise ValueError(
                f"OAuthResourceMetadata.client_id must contain only URL-safe characters "
                f"(alphanumeric, hyphen, underscore, period, tilde), got: {self.client_id!r}"
            )
        if self.client_secret is not None and not _URL_SAFE_RE.fullmatch(self.client_secret):
            raise ValueError(
                f"OAuthResourceMetadata.client_secret must contain only URL-safe characters "
                f"(alphanumeric, hyphen, underscore, period, tilde), got: {self.client_secret!r}"
            )
        if self.device_code_client_id is not None and not _URL_SAFE_RE.fullmatch(self.device_code_client_id):
            raise ValueError(
                f"OAuthResourceMetadata.device_code_client_id must contain only URL-safe characters "
                f"(alphanumeric, hyphen, underscore, period, tilde), got: {self.device_code_client_id!r}"
            )
        if self.device_code_client_secret is not None and not _URL_SAFE_RE.fullmatch(self.device_code_client_secret):
            raise ValueError(
                f"OAuthResourceMetadata.device_code_client_secret must contain only URL-safe characters "
                f"(alphanumeric, hyphen, underscore, period, tilde), got: {self.device_code_client_secret!r}"
            )

    def to_json_dict(self) -> dict[str, object]:
        """Serialize to a JSON-compatible dict per RFC 9728.

        Only includes fields with non-default values.

        Returns:
            A dictionary suitable for ``json.dumps()``.

        """
        d: dict[str, object] = {
            "resource": self.resource,
            "authorization_servers": list(self.authorization_servers),
        }
        if self.scopes_supported:
            d["scopes_supported"] = list(self.scopes_supported)
        if self.bearer_methods_supported != ("header",):
            d["bearer_methods_supported"] = list(self.bearer_methods_supported)
        if self.resource_signing_alg_values_supported:
            d["resource_signing_alg_values_supported"] = list(self.resource_signing_alg_values_supported)
        if self.resource_name is not None:
            d["resource_name"] = self.resource_name
        if self.resource_documentation is not None:
            d["resource_documentation"] = self.resource_documentation
        if self.resource_policy_uri is not None:
            d["resource_policy_uri"] = self.resource_policy_uri
        if self.resource_tos_uri is not None:
            d["resource_tos_uri"] = self.resource_tos_uri
        if self.client_id is not None:
            d["client_id"] = self.client_id
        if self.client_secret is not None:
            d["client_secret"] = self.client_secret
        if self.use_id_token_as_bearer:
            d["use_id_token_as_bearer"] = True
        if self.device_code_client_id is not None:
            d["device_code_client_id"] = self.device_code_client_id
        if self.device_code_client_secret is not None:
            d["device_code_client_secret"] = self.device_code_client_secret
        return d


class _OAuthResourceMetadataResource:
    """Falcon resource for ``GET /.well-known/oauth-protected-resource``."""

    __slots__ = ("_body",)

    def __init__(self, metadata: OAuthResourceMetadata) -> None:
        self._body = json.dumps(metadata.to_json_dict(), separators=(",", ":")).encode()

    def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Serve the cached JSON metadata document.

        Args:
            req: Falcon request.
            resp: Falcon response.

        """
        resp.content_type = falcon.MEDIA_JSON
        resp.data = self._body
        resp.set_header("Cache-Control", "public, max-age=60")


def _build_www_authenticate(metadata: OAuthResourceMetadata, prefix: str = "/vgi") -> str:
    """Build a ``WWW-Authenticate`` header value per RFC 9728 Section 5.1.

    Uses the origin from ``metadata.resource`` combined with *prefix* to
    construct the well-known URL that matches the route registered by
    ``make_wsgi_app``.

    Args:
        metadata: The OAuth resource metadata configuration.
        prefix: The URL prefix used by the server (must match
            ``make_wsgi_app(prefix=...)``).

    Returns:
        A ``Bearer`` challenge string with ``resource_metadata`` parameter.

    """
    parsed = urlparse(metadata.resource)
    path_suffix = prefix if prefix != "/" else ""
    well_known_url = f"{parsed.scheme}://{parsed.netloc}/.well-known/oauth-protected-resource{path_suffix}"
    challenge = f'Bearer resource_metadata="{well_known_url}"'
    if metadata.client_id is not None:
        challenge += f', client_id="{metadata.client_id}"'
    # client_secret in the header is intentional: Google's PKCE flow treats it
    # as a "public" secret for native/SPA apps, not a truly confidential value.
    if metadata.client_secret is not None:
        challenge += f', client_secret="{metadata.client_secret}"'
    if metadata.use_id_token_as_bearer:
        challenge += ', use_id_token_as_bearer="true"'
    if metadata.device_code_client_id is not None:
        challenge += f', device_code_client_id="{metadata.device_code_client_id}"'
    if metadata.device_code_client_secret is not None:
        challenge += f', device_code_client_secret="{metadata.device_code_client_secret}"'
    return challenge
