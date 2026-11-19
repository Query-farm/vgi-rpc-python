# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Blob-vendor client: region-pinned, single-object pre-signed URLs.

The *blob vendor* is a small HTTP service that holds the object-storage
credentials so workers never do.  A worker asks it for a pre-signed URL for
one object of a declared size and SHA-256, uploads straight to storage with
that URL, and hands a download URL (or an object id) to whoever needs the
bytes.  The storage enforces the signed ``Content-Length`` and
``x-amz-checksum-sha256``, so whatever sits at the object's key is exactly the
declared bytes.

This module provides:

* :class:`VendorStorage` — implements both
  :class:`~vgi_rpc.external.ExternalStorage` (server-side externalization of
  large results) and :class:`~vgi_rpc.external.UploadUrlProvider` (vending
  upload URLs to clients for large requests).
* :class:`VendorBlobClient` — raw put/get/delete of byte blobs, for callers
  that keep their own references (e.g. value-level externalization in a
  key-value store).
* :func:`pinned_hosts_validator` and :func:`vendor_url_validator` — URL
  validators that accept only ``https`` URLs on the vendor's storage hosts,
  for use as ``url_validator`` on the external-location configs.
* :func:`external_config_from_env` / :func:`client_external_config_from_env`
  — environment-driven configs for hosted deployments.

Environment variables
---------------------

``VGI_BLOB_VENDOR_URL``
    Base URL of the vendor, e.g. ``https://blobs.queryfarm.cloud``.  When
    unset, the ``*_from_env`` config helpers return ``None``.
``VGI_BLOB_VENDOR_TOKEN``
    Bearer token for the vendor.  Falls back to ``VGI_CF_DO_TOKEN`` (the
    vendor shares the storage service's per-deployment keys).
``VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES``
    Result-batch size above which the server externalizes (default 1 MiB).
``VGI_RPC_EXTERNAL_COMPRESSION``
    ``none`` (default), ``zstd``, ``gzip``, optionally with a level, e.g.
    ``zstd:3``.
``VGI_RPC_MAX_UPLOAD_BYTES``
    Optional size advertised to clients for vendor upload URLs.

Every vendor request carries ``Authorization: Bearer <token>``.  The token and
signed URLs are never logged: diagnostics go through
:func:`~vgi_rpc.external_fetch.redact_url`, which keeps only scheme, host,
port and path.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import ipaddress
import json
import logging
import os
import random
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self
from urllib.parse import urlsplit

from vgi_rpc.external import ClientExternalConfig, Compression, ServerExternalConfig, UploadUrl
from vgi_rpc.external_fetch import FetchConfig, fetch_url, redact_httpx_logging, redact_url

if TYPE_CHECKING:
    import httpx2
    import pyarrow as pa

__all__ = [
    "ENV_COMPRESSION",
    "ENV_MAX_UPLOAD_BYTES",
    "ENV_THRESHOLD",
    "ENV_TOKEN",
    "ENV_TOKEN_FALLBACK",
    "ENV_URL",
    "BlobLocation",
    "BlobRef",
    "BlobScope",
    "PutGrant",
    "VendorBlobClient",
    "VendorError",
    "VendorInfo",
    "VendorStorage",
    "client_external_config_from_env",
    "external_config_from_env",
    "max_upload_bytes_from_env",
    "pinned_hosts_validator",
    "vendor_url_validator",
]

_logger = logging.getLogger(__name__)

ENV_URL = "VGI_BLOB_VENDOR_URL"
ENV_TOKEN = "VGI_BLOB_VENDOR_TOKEN"
ENV_TOKEN_FALLBACK = "VGI_CF_DO_TOKEN"
ENV_THRESHOLD = "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES"
ENV_COMPRESSION = "VGI_RPC_EXTERNAL_COMPRESSION"
ENV_MAX_UPLOAD_BYTES = "VGI_RPC_MAX_UPLOAD_BYTES"

_DEFAULT_THRESHOLD = 1_048_576
_DEFAULT_TIMEOUT = 30.0
_DEFAULT_INFO_TTL = 600.0
_MAX_BACKOFF_SECONDS = 5.0

Purpose = Literal["rpc", "fs"]


class VendorError(RuntimeError):
    """A blob-vendor or signed-URL request failed.

    Messages never contain the bearer token or a signed query string.

    Attributes:
        status: The HTTP status of the failing response, or ``None`` for a
            transport failure or a malformed response.
        code: The vendor's machine-readable ``error`` code (e.g.
            ``"not_found"``, ``"not_uploaded"``, ``"quota_exceeded"``,
            ``"idempotency_conflict"``), or the storage's XML error code
            (e.g. ``"BadDigest"``), when there was one.  Checks made by
            :meth:`VendorBlobClient.get_bytes` itself use ``"size_mismatch"``
            and ``"sha256_mismatch"``.

    """

    status: int | None
    code: str | None

    def __init__(self, message: str, status: int | None = None, code: str | None = None) -> None:
        """Create the error.

        Args:
            message: Diagnostic text (already redacted).
            status: HTTP status, when there was a response.
            code: Machine-readable error code, when there was one.

        """
        super().__init__(message)
        self.status = status
        self.code = code


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BlobScope:
    """The execution a blob belongs to, for scoped clean-up.

    The vendor accepts 1-128 characters of ``[A-Za-z0-9._:-]`` for each
    field, so encode binary ids as hex (base64's ``+/=`` are refused).

    Attributes:
        shard: The storage shard key.
        execution_id: The execution identifier (e.g. hex of the raw id bytes).

    """

    shard: str
    execution_id: str


@dataclass(frozen=True)
class BlobRef:
    """A reference to one vendored blob, small enough to store inline.

    Attributes:
        object_id: The vendor's object id.
        size: Exact byte length of the blob.
        sha256: Lowercase hex SHA-256 of the blob bytes.

    """

    object_id: str
    size: int
    sha256: str


@dataclass(frozen=True)
class PutGrant:
    """The vendor's answer to ``put``: where and how to upload one object.

    Attributes:
        object_id: The server-chosen object id (32 lowercase hex).
        upload_url: Pre-signed PUT URL.
        required_headers: Headers the PUT must carry exactly as given (the
            signature covers them).
        download_url: Pre-signed GET URL, when the vendor returns one (only
            for ``rpc`` objects).
        expires_at: When ``upload_url`` stops working (UTC).
        region: The region the object lives in.
        download_expires_at: When ``download_url`` stops working, if given.

    """

    object_id: str
    upload_url: str
    required_headers: Mapping[str, str]
    download_url: str | None
    expires_at: datetime
    region: str | None = None
    download_expires_at: datetime | None = None


@dataclass(frozen=True)
class BlobLocation:
    """The vendor's answer to ``get``: where one object can be downloaded.

    Attributes:
        download_url: Pre-signed GET URL.
        size: Declared byte length.
        sha256_b64: Declared SHA-256, standard base64.
        content_encoding: ``Content-Encoding`` stored with the object.
        expires_at: When ``download_url`` stops working (UTC).

    """

    download_url: str
    size: int
    sha256_b64: str
    content_encoding: str | None
    expires_at: datetime


@dataclass(frozen=True)
class VendorInfo:
    """The vendor's ``/v1/info``: storage hosts for this tenant's region.

    Attributes:
        region: The tenant's region name.
        upload_hosts: Hosts upload URLs may point at.
        download_hosts: Hosts download URLs may point at.
        tenant: The tenant the token resolves to.
        jurisdiction: The R2 jurisdiction (``"eu"``, ``"us"``), or ``None``.
        url_prefixes: URL prefixes this tenant's objects live under.
        limits: Numeric limits (``max_object_bytes``, ``quota_bytes``,
            ``used_bytes``, ``max_upload_urls``, ``max_delete_ids``).
        ttl_seconds: URL lifetimes (``put``, ``rpc_download``, ``get``).

    """

    region: str
    upload_hosts: tuple[str, ...]
    download_hosts: tuple[str, ...]
    tenant: str = ""
    jurisdiction: str | None = None
    url_prefixes: tuple[str, ...] = ()
    limits: Mapping[str, int] = field(default_factory=dict)
    ttl_seconds: Mapping[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _b64_sha256(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")


def _parse_expires(value: object) -> datetime:
    """Parse ``expires_at`` as ISO 8601 text or epoch seconds/milliseconds."""
    if isinstance(value, bool):
        raise VendorError("Blob vendor returned a malformed expires_at")
    if isinstance(value, int | float):
        seconds = float(value) / 1000.0 if value > 1e11 else float(value)
        return datetime.fromtimestamp(seconds, UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise VendorError("Blob vendor returned a malformed expires_at") from None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    raise VendorError("Blob vendor returned a malformed expires_at")


def _field(obj: Mapping[str, object], key: str, op: str) -> object:
    if key not in obj:
        raise VendorError(f"Blob vendor {op} response is missing {key!r}")
    return obj[key]


def _str_field(obj: Mapping[str, object], key: str, op: str) -> str:
    value = _field(obj, key, op)
    if not isinstance(value, str) or not value:
        raise VendorError(f"Blob vendor {op} response has a malformed {key!r}")
    return value


def _opt_str_field(obj: Mapping[str, object], key: str, op: str) -> str | None:
    value = obj.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise VendorError(f"Blob vendor {op} response has a malformed {key!r}")
    return value


def _int_field(obj: Mapping[str, object], key: str, op: str) -> int:
    value = _field(obj, key, op)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise VendorError(f"Blob vendor {op} response has a malformed {key!r}")
    return value


def _int_map(obj: Mapping[str, object], key: str) -> dict[str, int]:
    value = obj.get(key)
    if not isinstance(value, dict):
        return {}
    return {str(k): v for k, v in value.items() if isinstance(v, int) and not isinstance(v, bool)}


def _hosts_field(obj: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = obj.get(key, [])
    if not isinstance(value, list) or not all(isinstance(h, str) and h for h in value):
        raise VendorError(f"Blob vendor info response has a malformed {key!r}")
    return tuple(str(h) for h in value)


def _is_loopback_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _default_port(scheme: str) -> int:
    return 443 if scheme == "https" else 80


def _host_key(host: str, port: int | None, scheme: str) -> tuple[str, int]:
    return host.lower().strip("[]"), port if port is not None else _default_port(scheme)


# ---------------------------------------------------------------------------
# URL pinning
# ---------------------------------------------------------------------------


def pinned_hosts_validator(
    hosts: Iterable[str], *, allow_http: bool = False, prefixes: Iterable[str] = ()
) -> Callable[[str], None]:
    """Build a ``url_validator`` that accepts only URLs on *hosts*.

    A URL passes when its scheme is ``https`` (or ``http`` with
    *allow_http*), it carries no userinfo, and its host and port are exactly
    one of *hosts*.  An entry is ``host`` or ``host:port`` (``[v6]:port`` for
    IPv6); an entry without a port matches the scheme's default port only.
    Subdomains do not match.  With *prefixes*, the URL (without its query)
    must also start with one of them, e.g. a region's bucket path.

    Args:
        hosts: Allowed hosts.
        allow_http: Also accept ``http`` URLs.  Only for loopback test
            vendors; never enable it for a real deployment.
        prefixes: Optional URL prefixes the URL must start with.

    Returns:
        A callable that raises ``ValueError`` for a disallowed URL.

    Raises:
        ValueError: If *hosts* is empty or an entry will not parse.

    """
    allowed: set[tuple[str, int]] = set()
    allowed_schemes = ("https", "http") if allow_http else ("https",)
    for entry in hosts:
        parts = urlsplit(f"//{entry.strip()}")
        if not parts.hostname or parts.path or parts.query or parts.username is not None:
            raise ValueError(f"Invalid pinned host entry: {entry!r}")
        if parts.port is None:
            for scheme in allowed_schemes:
                allowed.add(_host_key(parts.hostname, None, scheme))
        else:
            allowed.add(_host_key(parts.hostname, parts.port, "https"))
    if not allowed:
        raise ValueError("pinned_hosts_validator needs at least one host")
    frozen = frozenset(allowed)
    url_prefixes = tuple(p for p in prefixes if p)

    def validate(url: str) -> None:
        try:
            parts = urlsplit(url)
            port = parts.port
        except ValueError:
            raise ValueError("URL is malformed") from None
        if parts.scheme not in allowed_schemes:
            raise ValueError(f"URL scheme '{parts.scheme}' not allowed (only {' or '.join(allowed_schemes)})")
        if parts.username is not None or parts.password is not None:
            raise ValueError("URL userinfo is not allowed")
        if not parts.hostname or _host_key(parts.hostname, port, parts.scheme) not in frozen:
            raise ValueError(f"URL host is not a pinned storage host: {redact_url(url)}")
        if url_prefixes:
            bare = f"{parts.scheme}://{parts.netloc}{parts.path}"
            if ".." in parts.path.split("/") or not any(bare.startswith(p) for p in url_prefixes):
                raise ValueError(f"URL is outside the pinned storage prefixes: {redact_url(url)}")

    return validate


class _VendorHostsValidator:
    """``url_validator`` backed by the vendor's ``/v1/info``, cached."""

    def __init__(self, api: _VendorApi, ttl_seconds: float) -> None:
        self._api = api
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._validator: Callable[[str], None] | None = None
        self._fetched_at = 0.0

    def _current(self) -> Callable[[str], None]:
        with self._lock:
            now = time.monotonic()
            if self._validator is not None and now - self._fetched_at < self._ttl:
                return self._validator
            try:
                info = self._api.info()
                validator = pinned_hosts_validator(
                    (*info.upload_hosts, *info.download_hosts),
                    allow_http=self._api.loopback_http,
                    prefixes=info.url_prefixes,
                )
            except (VendorError, ValueError) as exc:
                if self._validator is not None:
                    # Keep the last known-good pin through a vendor blip.
                    _logger.warning("Blob vendor info refresh failed; keeping cached hosts: %s", exc)
                    self._fetched_at = now
                    return self._validator
                raise ValueError(f"Cannot pin storage hosts: {exc}") from None
            self._validator = validator
            self._fetched_at = now
            return validator

    def __call__(self, url: str) -> None:
        self._current()(url)


# ---------------------------------------------------------------------------
# HTTP + wire mapping (the one place that knows the vendor's JSON shapes)
# ---------------------------------------------------------------------------


class _VendorApi:
    """HTTP transport, retries, and the vendor's request/response shapes.

    Every JSON key the vendor defines appears only in this class, so a change
    to the vendor's wire format is a change here and nowhere else.
    """

    # Endpoint paths.
    PUT_PATH = "/v1/blobs/put"
    GET_PATH = "/v1/blobs/get"
    DELETE_PATH = "/v1/blobs/delete"
    DELETE_SCOPE_PATH = "/v1/blobs/delete_scope"
    UPLOAD_URLS_PATH = "/v1/upload_urls"
    INFO_PATH = "/v1/info"
    MAX_UPLOAD_URLS = 100
    MAX_DELETE_IDS = 1000

    def __init__(
        self,
        url: str,
        token: str,
        *,
        timeout: float,
        session: httpx2.Client | None,
        max_attempts: int,
        backoff_seconds: float,
    ) -> None:
        parts = urlsplit(url)
        if parts.scheme not in ("https", "http") or not parts.hostname:
            raise ValueError(f"Blob vendor URL must be an absolute https URL: {redact_url(url)}")
        self.loopback_http = parts.scheme == "http"
        if self.loopback_http and not _is_loopback_host(parts.hostname):
            # The bearer token would cross the network in plain text.
            raise ValueError(f"Blob vendor URL must use https (http is allowed only for loopback): {redact_url(url)}")
        if not token:
            raise ValueError("Blob vendor token is empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.base_url = url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._own_session = session is None
        self._session = session
        self._storage: httpx2.Client | None = None
        redact_httpx_logging()

    # -- transport --------------------------------------------------------

    def _client(self) -> httpx2.Client:
        if self._session is None:
            import httpx2

            # No redirects: a redirect could carry the bearer token or a
            # signed URL to a host nobody vetted.
            self._session = httpx2.Client(timeout=self._timeout, follow_redirects=False)
        return self._session

    def _storage_client(self) -> httpx2.Client:
        # Storage hosts get their own bare client, never the caller's
        # session: that may carry default headers, cookies, an auth flow or
        # event hooks, all of which would reach the storage host.
        if self._storage is None:
            self._storage = _new_storage_client(self._timeout)
        return self._storage

    def close(self) -> None:
        if self._own_session and self._session is not None:
            self._session.close()
            self._session = None
        if self._storage is not None:
            self._storage.close()
            self._storage = None

    def _send(
        self,
        method: str,
        url: str,
        op: str,
        *,
        headers: Mapping[str, str],
        content: bytes | None = None,
        storage: bool = False,
    ) -> httpx2.Response:
        """Send one request, retrying transport errors, 429 and 5xx with backoff.

        The request is built standalone and sent with ``auth=None``, so it
        carries exactly *headers*: no client default headers, cookies or auth
        flow.  ``storage=True`` sends it on the bare storage client.
        """
        import httpx2

        client = self._storage_client() if storage else self._client()
        last_error = ""
        status: int | None = None
        for attempt in range(self._max_attempts):
            if attempt:
                delay = min(self._backoff * (2 ** (attempt - 1)), _MAX_BACKOFF_SECONDS)
                time.sleep(delay * (0.5 + random.random() / 2))
            try:
                request = httpx2.Request(method, url, headers=dict(headers), content=content)
                resp = client.send(request, auth=None, follow_redirects=False)
            except httpx2.TransportError as exc:
                # Never str(exc): httpx messages can embed the full URL.
                status = None
                last_error = type(exc).__name__
                _logger.debug(
                    "Blob vendor %s transport error (attempt %d/%d): %s %s",
                    op,
                    attempt + 1,
                    self._max_attempts,
                    last_error,
                    redact_url(url),
                )
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                status = resp.status_code
                last_error = f"HTTP {resp.status_code}"
                _logger.debug(
                    "Blob vendor %s got HTTP %d (attempt %d/%d): %s",
                    op,
                    resp.status_code,
                    attempt + 1,
                    self._max_attempts,
                    redact_url(url),
                )
                resp.close()
                continue
            return resp
        raise VendorError(
            f"Blob vendor {op} failed after {self._max_attempts} attempts ({last_error}): {redact_url(url)}",
            status,
        )

    def _post_json(self, path: str, op: str, body: Mapping[str, object]) -> dict[str, object]:
        return self._call("POST", path, op, json.dumps(body).encode())

    def _call(self, method: str, path: str, op: str, content: bytes | None) -> dict[str, object]:
        url = f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if content is not None:
            headers["Content-Type"] = "application/json"
        resp = self._send(method, url, op, headers=headers, content=content)
        if not 200 <= resp.status_code < 300:
            code, detail = _error_detail(resp)
            raise VendorError(f"Blob vendor {op} failed: HTTP {resp.status_code}{detail}", resp.status_code, code)
        if not resp.content:
            return {}
        try:
            parsed = resp.json()
        except ValueError:
            raise VendorError(f"Blob vendor {op} returned non-JSON", resp.status_code) from None
        if not isinstance(parsed, dict):
            raise VendorError(f"Blob vendor {op} returned a non-object JSON body", resp.status_code)
        return {str(k): v for k, v in parsed.items()}

    # -- wire mapping -----------------------------------------------------

    def put(
        self,
        *,
        size: int,
        sha256_b64: str,
        purpose: str,
        scope: BlobScope | None = None,
        content_encoding: str | None = None,
        idempotency_key: str | None = None,
    ) -> PutGrant:
        body: dict[str, object] = {"size": size, "sha256_b64": sha256_b64, "purpose": purpose}
        if scope is not None:
            body["scope"] = {"shard": scope.shard, "execution_id": scope.execution_id}
        if content_encoding is not None:
            body["content_encoding"] = content_encoding
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        resp = self._post_json(self.PUT_PATH, "put", body)
        method = resp.get("upload_method", "PUT")
        if method != "PUT":
            raise VendorError(f"Blob vendor put asked for unsupported upload_method {method!r}")
        raw_headers = _field(resp, "required_headers", "put")
        if not isinstance(raw_headers, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in raw_headers.items()
        ):
            raise VendorError("Blob vendor put response has malformed 'required_headers'")
        return PutGrant(
            object_id=_str_field(resp, "object_id", "put"),
            upload_url=_str_field(resp, "upload_url", "put"),
            required_headers={str(k): str(v) for k, v in raw_headers.items()},
            download_url=_opt_str_field(resp, "download_url", "put"),
            expires_at=_parse_expires(_field(resp, "expires_at", "put")),
            region=_opt_str_field(resp, "region", "put"),
            download_expires_at=(
                _parse_expires(resp["download_expires_at"]) if resp.get("download_expires_at") is not None else None
            ),
        )

    def get(self, object_id: str) -> BlobLocation:
        resp = self._post_json(self.GET_PATH, "get", {"object_id": object_id})
        return BlobLocation(
            download_url=_str_field(resp, "download_url", "get"),
            size=_int_field(resp, "size", "get"),
            sha256_b64=_str_field(resp, "sha256_b64", "get"),
            content_encoding=_opt_str_field(resp, "content_encoding", "get"),
            expires_at=_parse_expires(_field(resp, "expires_at", "get")),
        )

    def delete(self, object_ids: list[str]) -> int:
        resp = self._post_json(self.DELETE_PATH, "delete", {"object_ids": object_ids})
        deleted = resp.get("deleted", 0)
        return deleted if isinstance(deleted, int) else 0

    def delete_scope(self, scope: BlobScope) -> int:
        resp = self._post_json(
            self.DELETE_SCOPE_PATH, "delete_scope", {"shard": scope.shard, "execution_id": scope.execution_id}
        )
        deleted = resp.get("deleted", 0)
        return deleted if isinstance(deleted, int) else 0

    def upload_urls(self, count: int) -> list[UploadUrl]:
        if not 1 <= count <= self.MAX_UPLOAD_URLS:
            raise ValueError(f"count must be between 1 and {self.MAX_UPLOAD_URLS}")
        resp = self._post_json(self.UPLOAD_URLS_PATH, "upload_urls", {"count": count})
        urls = _field(resp, "urls", "upload_urls")
        if not isinstance(urls, list) or not urls:
            raise VendorError("Blob vendor upload_urls response has malformed 'urls'")
        out: list[UploadUrl] = []
        for item in urls:
            if not isinstance(item, dict):
                raise VendorError("Blob vendor upload_urls response has a malformed entry")
            entry = {str(k): v for k, v in item.items()}
            out.append(
                UploadUrl(
                    upload_url=_str_field(entry, "upload_url", "upload_urls"),
                    download_url=_str_field(entry, "download_url", "upload_urls"),
                    expires_at=_parse_expires(_field(entry, "expires_at", "upload_urls")),
                )
            )
        return out

    def info(self) -> VendorInfo:
        resp = self._call("GET", self.INFO_PATH, "info", None)
        region = resp.get("region", "")
        tenant = resp.get("tenant", "")
        return VendorInfo(
            region=region if isinstance(region, str) else "",
            upload_hosts=_hosts_field(resp, "upload_hosts"),
            download_hosts=_hosts_field(resp, "download_hosts"),
            tenant=tenant if isinstance(tenant, str) else "",
            jurisdiction=_opt_str_field(resp, "jurisdiction", "info"),
            url_prefixes=_hosts_field(resp, "url_prefixes"),
            limits=_int_map(resp, "limits"),
            ttl_seconds=_int_map(resp, "ttl_seconds"),
        )

    # -- signed-URL upload ------------------------------------------------

    def put_object(self, grant: PutGrant, data: bytes, *, content_encoding: str | None = None) -> None:
        """PUT *data* to a granted URL with exactly its required headers."""
        headers = dict(grant.required_headers)
        lower = {k.lower(): v for k, v in headers.items()}
        declared = lower.get("content-length")
        if declared is not None and declared != str(len(data)):
            raise VendorError(f"Blob vendor granted Content-Length {declared} for a {len(data)}-byte object")
        if content_encoding is not None and "content-encoding" not in lower:
            headers["Content-Encoding"] = content_encoding
        resp = self._send("PUT", grant.upload_url, "upload", headers=headers, content=data, storage=True)
        if not 200 <= resp.status_code < 300:
            code = _s3_error_code(resp.content)
            raise VendorError(
                f"Signed upload failed: HTTP {resp.status_code}{f' {code}' if code else ''}: "
                f"{redact_url(grant.upload_url)}",
                resp.status_code,
                code,
            )


def _new_storage_client(timeout: float) -> httpx2.Client:
    """Build the bare client used for storage-host (signed URL) requests."""
    import httpx2

    return httpx2.Client(timeout=timeout, follow_redirects=False)


def _error_detail(resp: httpx2.Response) -> tuple[str | None, str]:
    """Pull ``(code, ": code: message")`` out of a vendor ``{error, message}`` body."""
    with contextlib.suppress(ValueError):
        parsed = resp.json()
        if isinstance(parsed, dict):
            code = parsed.get("error")
            message = parsed.get("message")
            code_text = code[:64] if isinstance(code, str) and code else None
            parts = [p for p in (code_text, message[:200] if isinstance(message, str) else None) if p]
            return code_text, (f": {': '.join(parts)}" if parts else "")
    return None, ""


def _s3_error_code(body: bytes) -> str | None:
    """Return the ``<Code>`` of an S3/R2 XML error body, if any."""
    start = body.find(b"<Code>")
    end = body.find(b"</Code>", start)
    if start < 0 or end < 0:
        return None
    code = body[start + 6 : end].decode("ascii", "replace")
    return code if code.isalnum() and len(code) <= 64 else None


def _resolve_env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _env_url_and_token(env: Mapping[str, str]) -> tuple[str, str]:
    url = env.get(ENV_URL, "").strip()
    if not url:
        raise ValueError(f"{ENV_URL} is not set")
    token = env.get(ENV_TOKEN, "").strip() or env.get(ENV_TOKEN_FALLBACK, "").strip()
    if not token:
        raise ValueError(f"{ENV_URL} is set but neither {ENV_TOKEN} nor {ENV_TOKEN_FALLBACK} is")
    return url, token


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------


class _VendorClientBase:
    """Lifecycle and pinning shared by the two public clients."""

    def __init__(
        self,
        url: str,
        token: str,
        *,
        timeout: float,
        session: httpx2.Client | None,
        max_attempts: int,
        backoff_seconds: float,
        info_ttl_seconds: float,
    ) -> None:
        self._api = _VendorApi(
            url,
            token,
            timeout=timeout,
            session=session,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
        self._validator = _VendorHostsValidator(self._api, info_ttl_seconds)

    @property
    def url_validator(self) -> Callable[[str], None]:
        """A ``url_validator`` pinned to the vendor's storage hosts (``/v1/info``, cached)."""
        return self._validator

    def info(self) -> VendorInfo:
        """Fetch the vendor's region and storage hosts (uncached).

        Returns:
            The vendor's :class:`VendorInfo`.

        """
        return self._api.info()

    def close(self) -> None:
        """Close the HTTP session this client created (a passed-in session is left open)."""
        self._api.close()

    def __enter__(self) -> Self:
        """Enter the context manager.

        Returns:
            This client.

        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the client."""
        self.close()


# ---------------------------------------------------------------------------
# VendorStorage
# ---------------------------------------------------------------------------


class VendorStorage(_VendorClientBase):
    """``ExternalStorage`` and ``UploadUrlProvider`` backed by the blob vendor.

    ``upload()`` (server-side externalization) asks the vendor for a sized,
    checksummed grant, PUTs the bytes with exactly the grant's required
    headers, and returns the grant's download URL.  ``generate_upload_url()``
    (client uploads) asks ``/v1/upload_urls`` for one unsized PUT/GET pair.

    Thread-safe.  Objects are cleaned up by the vendor (registry alarm plus
    storage lifecycle rules); vgi-rpc never deletes them.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        purpose: Purpose = "rpc",
        timeout: float = _DEFAULT_TIMEOUT,
        session: httpx2.Client | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        info_ttl_seconds: float = _DEFAULT_INFO_TTL,
        pin_hosts: bool = True,
    ) -> None:
        """Create a vendor-backed storage.

        Args:
            url: Vendor base URL (``https``; ``http`` only for loopback).
            token: Bearer token.
            purpose: Object purpose; ``"rpc"`` (default) gets a download URL
                and the short lifecycle.
            timeout: Per-request timeout in seconds.
            session: Optional ``httpx2.Client`` for vendor API requests
                (its transport only: requests carry exactly the vendor
                headers, never its default headers, cookies or auth).
                Storage-host requests always use a separate bare client.
            max_attempts: Attempts per request (transport errors, 429 and
                5xx are retried with exponential backoff).
            backoff_seconds: Base backoff between attempts.
            info_ttl_seconds: How long the ``/v1/info`` host pin is cached.
            pin_hosts: Check vended upload/download URLs against the
                vendor's hosts before using them.

        """
        super().__init__(
            url,
            token,
            timeout=timeout,
            session=session,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            info_ttl_seconds=info_ttl_seconds,
        )
        self._purpose: Purpose = purpose
        self._pin = pin_hosts

    @classmethod
    def from_env(cls, *, purpose: Purpose = "rpc", env: Mapping[str, str] | None = None) -> VendorStorage:
        """Build from ``VGI_BLOB_VENDOR_URL`` and the vendor token variables.

        Args:
            purpose: Object purpose.
            env: Environment mapping (defaults to ``os.environ``).

        Returns:
            A configured :class:`VendorStorage`.

        Raises:
            ValueError: If the URL or token is missing.

        """
        url, token = _env_url_and_token(_resolve_env(env))
        return cls(url, token, purpose=purpose)

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Upload *data* via a vendor grant and return its download URL.

        Args:
            data: Bytes to store (already compressed when *content_encoding*
                is set).
            schema: Arrow schema of the stream (unused).
            content_encoding: Encoding applied to *data*, stored with the
                object.

        Returns:
            The pre-signed download URL.

        Raises:
            VendorError: If the vendor or the signed upload fails.

        """
        grant = self._api.put(
            size=len(data),
            sha256_b64=_b64_sha256(data),
            purpose=self._purpose,
            content_encoding=content_encoding,
        )
        if self._pin:
            self._check(grant.upload_url)
        self._api.put_object(grant, data, content_encoding=content_encoding)
        download_url = grant.download_url
        if download_url is None:
            download_url = self._api.get(grant.object_id).download_url
        if self._pin:
            self._check(download_url)
        _logger.debug("Vendored %d bytes as %s", len(data), redact_url(download_url))
        return download_url

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Ask the vendor for one unsized PUT/GET pair.

        Args:
            schema: Arrow schema of the data to be uploaded (unused).

        Returns:
            The vendored :class:`~vgi_rpc.external.UploadUrl`.

        Raises:
            VendorError: If the vendor fails.

        """
        upload = self._api.upload_urls(1)[0]
        if self._pin:
            self._check(upload.upload_url)
            self._check(upload.download_url)
        return upload

    def _check(self, url: str) -> None:
        try:
            self._validator(url)
        except ValueError as exc:
            raise VendorError(f"Blob vendor returned a URL off its pinned hosts: {exc}") from None


# ---------------------------------------------------------------------------
# VendorBlobClient
# ---------------------------------------------------------------------------


class VendorBlobClient(_VendorClientBase):
    """Raw byte blobs through the vendor, referenced by :class:`BlobRef`.

    For callers that store references themselves (e.g. a key-value store
    that externalizes large values).  Objects use purpose ``"fs"`` by
    default: no download URL at ``put``; ``get_bytes`` asks the vendor for a
    fresh one.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        purpose: Purpose = "fs",
        timeout: float = _DEFAULT_TIMEOUT,
        session: httpx2.Client | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        info_ttl_seconds: float = _DEFAULT_INFO_TTL,
        fetch_config: FetchConfig | None = None,
    ) -> None:
        """Create a blob client.

        Args:
            url: Vendor base URL (``https``; ``http`` only for loopback).
            token: Bearer token.
            purpose: Object purpose (default ``"fs"``).
            timeout: Per-request timeout in seconds.
            session: Optional ``httpx2.Client`` for vendor API requests
                (transport only; storage requests use a separate bare client).
            max_attempts: Attempts per vendor request.
            backoff_seconds: Base backoff between attempts.
            info_ttl_seconds: How long the ``/v1/info`` host pin is cached.
            fetch_config: Download settings; ``max_fetch_bytes`` caps the
                size of a blob ``get_bytes`` will fetch.  A config created
                here is closed by :meth:`close`.

        """
        super().__init__(
            url,
            token,
            timeout=timeout,
            session=session,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            info_ttl_seconds=info_ttl_seconds,
        )
        self._purpose: Purpose = purpose
        self._own_fetch = fetch_config is None
        self._fetch = fetch_config if fetch_config is not None else FetchConfig()

    @classmethod
    def from_env(cls, *, purpose: Purpose = "fs", env: Mapping[str, str] | None = None) -> VendorBlobClient:
        """Build from ``VGI_BLOB_VENDOR_URL`` and the vendor token variables.

        Args:
            purpose: Object purpose.
            env: Environment mapping (defaults to ``os.environ``).

        Returns:
            A configured :class:`VendorBlobClient`.

        Raises:
            ValueError: If the URL or token is missing.

        """
        url, token = _env_url_and_token(_resolve_env(env))
        return cls(url, token, purpose=purpose)

    def put_bytes(self, data: bytes, *, scope: BlobScope | None = None, idempotency_key: str | None = None) -> BlobRef:
        """Store *data* and return its reference.

        Args:
            data: The bytes to store.
            scope: The execution the blob belongs to, for ``delete_scope``.
            idempotency_key: Makes a retried logical write reuse the same
                object: the same key with the same size and SHA-256 returns
                the same ``object_id``; with different bytes the vendor
                refuses (``VendorError.code == "idempotency_conflict"``).

        Returns:
            A :class:`BlobRef` naming the object, its size and SHA-256.

        Raises:
            VendorError: If the vendor or the signed upload fails.

        """
        digest = hashlib.sha256(data).digest()
        grant = self._api.put(
            size=len(data),
            sha256_b64=base64.b64encode(digest).decode("ascii"),
            purpose=self._purpose,
            scope=scope,
            idempotency_key=idempotency_key,
        )
        try:
            self._validator(grant.upload_url)
        except ValueError as exc:
            raise VendorError(f"Blob vendor returned a URL off its pinned hosts: {exc}") from None
        self._api.put_object(grant, data)
        return BlobRef(object_id=grant.object_id, size=len(data), sha256=digest.hex())

    def get_bytes(self, ref: BlobRef) -> bytes:
        """Fetch and verify the blob *ref* names.

        The download is pinned to the vendor's storage hosts, capped at the
        fetch config's ``max_fetch_bytes``, and checked against the size and
        SHA-256 in *ref* (not the vendor's own report).

        Args:
            ref: The reference from :meth:`put_bytes`.

        Returns:
            The blob bytes.

        Raises:
            VendorError: If the vendor fails, the object is missing, the
                download fails, or the bytes do not match *ref*.

        """
        if ref.size > self._fetch.max_fetch_bytes:
            raise VendorError(
                f"Blob {ref.object_id} is {ref.size} bytes, over max_fetch_bytes ({self._fetch.max_fetch_bytes})"
            )
        location = self._api.get(ref.object_id)
        if location.content_encoding not in (None, "identity"):
            raise VendorError(f"Blob {ref.object_id} has unsupported content_encoding {location.content_encoding!r}")
        if location.size != ref.size:
            raise VendorError(
                f"Blob {ref.object_id} size mismatch: vendor reports {location.size}, ref has {ref.size}",
                code="size_mismatch",
            )
        try:
            data = fetch_url(location.download_url, self._fetch, url_validator=self._validator)
        except ValueError as exc:
            raise VendorError(f"Blob {ref.object_id} download rejected: {exc}") from None
        except RuntimeError as exc:
            raise VendorError(f"Blob {ref.object_id} download failed: {exc}") from None
        except Exception as exc:
            # aiohttp errors can embed the signed URL; report the type only.
            status = getattr(exc, "status", None)
            raise VendorError(
                f"Blob {ref.object_id} download failed ({type(exc).__name__}"
                f"{f' HTTP {status}' if isinstance(status, int) else ''}): {redact_url(location.download_url)}",
                status if isinstance(status, int) else None,
            ) from None
        if len(data) != ref.size:
            raise VendorError(
                f"Blob {ref.object_id} size mismatch: expected {ref.size}, got {len(data)}", code="size_mismatch"
            )
        actual = hashlib.sha256(data).hexdigest()
        if actual != ref.sha256.lower():
            raise VendorError(
                f"Blob {ref.object_id} SHA-256 mismatch: expected {ref.sha256}, got {actual}", code="sha256_mismatch"
            )
        return data

    def delete(self, refs: Iterable[BlobRef | str]) -> int:
        """Delete objects by reference or object id (missing objects are ignored).

        Args:
            refs: References or object ids.

        Returns:
            How many objects the vendor deleted.

        """
        ids = [r.object_id if isinstance(r, BlobRef) else r for r in refs]
        step = self._api.MAX_DELETE_IDS
        return sum(self._api.delete(ids[start : start + step]) for start in range(0, len(ids), step))

    def delete_scope(self, shard: str, execution_id: str) -> int:
        """Delete every object put under ``(shard, execution_id)``.

        Args:
            shard: The storage shard key.
            execution_id: The execution id, encoded as in :class:`BlobScope`.

        Returns:
            How many objects the vendor deleted.

        """
        return self._api.delete_scope(BlobScope(shard=shard, execution_id=execution_id))

    def close(self) -> None:
        """Close the HTTP session and any fetch config this client created."""
        super().close()
        if self._own_fetch:
            self._fetch.close()


# ---------------------------------------------------------------------------
# Env-driven configs
# ---------------------------------------------------------------------------


def vendor_url_validator(
    url: str,
    token: str,
    *,
    ttl_seconds: float = _DEFAULT_INFO_TTL,
    session: httpx2.Client | None = None,
) -> Callable[[str], None]:
    """Build a ``url_validator`` pinned to the vendor's storage hosts.

    The hosts come from the vendor's ``GET /v1/info``, fetched on first use
    and cached for *ttl_seconds*; a failed refresh keeps the last good list.
    Only ``https`` URLs pass (``http`` only when the vendor itself is a
    loopback ``http`` URL, i.e. a local test vendor).

    Args:
        url: Vendor base URL.
        token: Bearer token.
        ttl_seconds: Cache lifetime of the host list.
        session: Optional ``httpx2.Client``.

    Returns:
        A callable raising ``ValueError`` for a URL off the vendor's hosts.

    """
    api = _VendorApi(url, token, timeout=_DEFAULT_TIMEOUT, session=session, max_attempts=3, backoff_seconds=0.25)
    return _VendorHostsValidator(api, ttl_seconds)


def _parse_positive_int(env: Mapping[str, str], name: str, default: int | None) -> int | None:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _parse_compression(raw: str) -> Compression | None:
    text = raw.strip().lower()
    if text in ("", "none", "off", "identity"):
        return None
    name, _, level_text = text.partition(":")
    algorithm: Literal["zstd", "gzip"]
    if name == "zstd":
        algorithm = "zstd"
    elif name == "gzip":
        algorithm = "gzip"
    else:
        raise ValueError(f"{ENV_COMPRESSION} must be none, zstd or gzip (optionally ':<level>'), got {raw!r}")
    if not level_text:
        return Compression(algorithm=algorithm)
    try:
        level = int(level_text)
    except ValueError:
        raise ValueError(f"{ENV_COMPRESSION} level must be an integer, got {raw!r}") from None
    return Compression(algorithm=algorithm, level=level)


def external_config_from_env(env: Mapping[str, str] | None = None) -> ServerExternalConfig | None:
    """Server external-location config from the blob-vendor environment.

    Uses :class:`VendorStorage` for uploads, pins inbound pointer URLs to the
    vendor's hosts, and reads ``VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES``
    (default 1 MiB) and ``VGI_RPC_EXTERNAL_COMPRESSION`` (default none).

    Pass ``config.storage`` as ``upload_url_provider`` to ``serve_http`` /
    ``make_wsgi_app`` to also vend client upload URLs.

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The config, or ``None`` when ``VGI_BLOB_VENDOR_URL`` is unset.

    Raises:
        ValueError: If the vendor URL is set but the token is missing, or a
            variable will not parse.

    """
    environ = _resolve_env(env)
    if not environ.get(ENV_URL, "").strip():
        return None
    storage = VendorStorage.from_env(env=environ)
    threshold = _parse_positive_int(environ, ENV_THRESHOLD, _DEFAULT_THRESHOLD)
    assert threshold is not None
    return ServerExternalConfig(
        storage=storage,
        externalize_threshold_bytes=threshold,
        compression=_parse_compression(environ.get(ENV_COMPRESSION, "")),
        url_validator=storage.url_validator,
    )


def client_external_config_from_env(env: Mapping[str, str] | None = None) -> ClientExternalConfig | None:
    """Client external-location config pinned to the blob vendor's hosts.

    For clients that hold a vendor token (e.g. a worker calling another
    worker): pointer and upload URLs are accepted only on the vendor's
    storage hosts.

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The config, or ``None`` when ``VGI_BLOB_VENDOR_URL`` is unset.

    Raises:
        ValueError: If the vendor URL is set but the token is missing.

    """
    environ = _resolve_env(env)
    if not environ.get(ENV_URL, "").strip():
        return None
    url, token = _env_url_and_token(environ)
    return ClientExternalConfig(url_validator=vendor_url_validator(url, token))


def max_upload_bytes_from_env(env: Mapping[str, str] | None = None) -> int | None:
    """Read ``VGI_RPC_MAX_UPLOAD_BYTES`` (advertised client-upload size).

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The size, or ``None`` when unset.

    """
    return _parse_positive_int(_resolve_env(env), ENV_MAX_UPLOAD_BYTES, None)
