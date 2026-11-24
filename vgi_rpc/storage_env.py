# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Direct object storage configured from the environment (direct-storage contract v1).

A self-hosted worker can keep large values (externalized results, upload
grants / argument references, StateRef objects, function-storage blobs) in
the operator's own bucket -- AWS S3, any S3-compatible service, or GCS --
instead of the blob vendor.  The worker signs URLs itself.  Every SDK reads
the same variables:

``VGI_STORAGE_BACKEND``
    ``vendor`` | ``s3`` | ``gcs`` | ``none``.  Unset: ``vendor`` if
    ``VGI_BLOB_VENDOR_URL`` is set, else ``s3`` if ``VGI_STORAGE_S3_BUCKET``
    is, else ``gcs`` if ``VGI_STORAGE_GCS_BUCKET`` is, else ``none``.  More
    than one of those three set without an explicit backend is an error.

S3: ``VGI_STORAGE_S3_BUCKET``, ``_PREFIX``, ``_ENDPOINT``, ``_REGION``,
``_FORCE_PATH_STYLE``, ``_PUBLIC_ENDPOINT``, ``_CHECKSUM`` (``sha256`` |
``none``), ``_ACCESS_KEY_ID`` / ``_SECRET_ACCESS_KEY`` / ``_SESSION_TOKEN``,
``_VERIFY``.  GCS: ``VGI_STORAGE_GCS_BUCKET``, ``_PREFIX``, ``_ENDPOINT``,
``_CREDENTIALS_FILE``.  Both: ``VGI_STORAGE_URL_TTL_SECONDS`` (default 3600)
and ``VGI_STORAGE_ALLOW_PLAIN_HTTP`` (accept ``http://`` pre-signed URLs; off
by default, mirroring the extension's ``vgi_storage_allow_plain_http``).

Objects are never deleted by the worker: give the bucket a lifecycle rule
expiring the prefix after one to two days.
"""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlsplit

from vgi_rpc.external import ObjectStore, ServerExternalConfig, UploadUrlProvider
from vgi_rpc.external_fetch import DEFAULT_MAX_FETCH_BYTES, FetchConfig, redact_url

if TYPE_CHECKING:
    from types import TracebackType

    from vgi_rpc.gcs import GCSStorage
    from vgi_rpc.s3 import S3Storage
    from vgi_rpc.vendor import BlobRef

__all__ = [
    "ENV_ALLOW_PLAIN_HTTP",
    "ENV_BACKEND",
    "ENV_GCS_BUCKET",
    "ENV_GCS_CREDENTIALS_FILE",
    "ENV_GCS_ENDPOINT",
    "ENV_GCS_PREFIX",
    "ENV_S3_ACCESS_KEY_ID",
    "ENV_S3_BUCKET",
    "ENV_S3_CHECKSUM",
    "ENV_S3_ENDPOINT",
    "ENV_S3_FORCE_PATH_STYLE",
    "ENV_S3_PREFIX",
    "ENV_S3_PUBLIC_ENDPOINT",
    "ENV_S3_REGION",
    "ENV_S3_SECRET_ACCESS_KEY",
    "ENV_S3_SESSION_TOKEN",
    "ENV_S3_VERIFY",
    "ENV_URL_TTL",
    "DirectStorageConfig",
    "ObjectStoreBlobClient",
    "StorageBackend",
    "direct_storage_from_env",
    "gcs_storage_from_env",
    "plain_http_allowed",
    "s3_storage_from_env",
    "storage_backend_from_env",
    "storage_url_validator",
]

_logger = logging.getLogger(__name__)

StorageBackend = Literal["vendor", "s3", "gcs", "none"]

ENV_BACKEND = "VGI_STORAGE_BACKEND"
ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL"
ENV_S3_BUCKET = "VGI_STORAGE_S3_BUCKET"
ENV_S3_PREFIX = "VGI_STORAGE_S3_PREFIX"
ENV_S3_ENDPOINT = "VGI_STORAGE_S3_ENDPOINT"
ENV_S3_REGION = "VGI_STORAGE_S3_REGION"
ENV_S3_FORCE_PATH_STYLE = "VGI_STORAGE_S3_FORCE_PATH_STYLE"
ENV_S3_PUBLIC_ENDPOINT = "VGI_STORAGE_S3_PUBLIC_ENDPOINT"
ENV_S3_CHECKSUM = "VGI_STORAGE_S3_CHECKSUM"
ENV_S3_ACCESS_KEY_ID = "VGI_STORAGE_S3_ACCESS_KEY_ID"
ENV_S3_SECRET_ACCESS_KEY = "VGI_STORAGE_S3_SECRET_ACCESS_KEY"
ENV_S3_SESSION_TOKEN = "VGI_STORAGE_S3_SESSION_TOKEN"
ENV_S3_VERIFY = "VGI_STORAGE_S3_VERIFY"
ENV_GCS_BUCKET = "VGI_STORAGE_GCS_BUCKET"
ENV_GCS_PREFIX = "VGI_STORAGE_GCS_PREFIX"
ENV_GCS_ENDPOINT = "VGI_STORAGE_GCS_ENDPOINT"
ENV_GCS_CREDENTIALS_FILE = "VGI_STORAGE_GCS_CREDENTIALS_FILE"
ENV_URL_TTL = "VGI_STORAGE_URL_TTL_SECONDS"
ENV_ALLOW_PLAIN_HTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP"

#: botocore reads this itself; when it is set the operator's choice wins.
_ENV_AWS_REQUEST_CHECKSUM = "AWS_REQUEST_CHECKSUM_CALCULATION"

_DEFAULT_URL_TTL = 3600
#: V4 signed URLs (S3 and GCS) live at most seven days.
_MAX_URL_TTL = 7 * 24 * 3600
_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def _get(env: Mapping[str, str], name: str) -> str:
    return env.get(name, "").strip()


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _get(env, name).lower()
    if not raw:
        return default
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    raise ValueError(f"{name} must be true or false, got {raw!r}")


def _url(env: Mapping[str, str], name: str) -> str | None:
    raw = _get(env, name)
    if not raw:
        return None
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"{name} must be an absolute http:// or https:// URL, got {redact_url(raw)!r}")
    return raw.rstrip("/")


def _url_ttl(env: Mapping[str, str]) -> int:
    raw = _get(env, ENV_URL_TTL)
    if not raw:
        return _DEFAULT_URL_TTL
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{ENV_URL_TTL} must be an integer number of seconds, got {raw!r}") from None
    if not 0 < value <= _MAX_URL_TTL:
        raise ValueError(f"{ENV_URL_TTL} must be between 1 and {_MAX_URL_TTL} (7 days), got {value}")
    return value


def _resolve(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def storage_backend_from_env(env: Mapping[str, str] | None = None) -> StorageBackend:
    """Return the configured storage backend (contract §1).

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        ``"vendor"``, ``"s3"``, ``"gcs"`` or ``"none"``.

    Raises:
        ValueError: ``VGI_STORAGE_BACKEND`` is not one of those, or it is
            unset while more than one backend's variable is set.

    """
    environ = _resolve(env)
    explicit = _get(environ, ENV_BACKEND).lower()
    if explicit:
        if explicit not in ("vendor", "s3", "gcs", "none"):
            raise ValueError(f"{ENV_BACKEND} must be vendor, s3, gcs or none, got {explicit!r}")
        return cast("StorageBackend", explicit)
    present = [name for name in (ENV_VENDOR_URL, ENV_S3_BUCKET, ENV_GCS_BUCKET) if _get(environ, name)]
    if len(present) > 1:
        raise ValueError(
            f"Conflicting storage configuration: {', '.join(present)} are all set; "
            f"set {ENV_BACKEND} to vendor, s3, gcs or none to choose one"
        )
    if not present:
        return "none"
    return {ENV_VENDOR_URL: "vendor", ENV_S3_BUCKET: "s3", ENV_GCS_BUCKET: "gcs"}[present[0]]  # type: ignore[return-value]


def plain_http_allowed(env: Mapping[str, str] | None = None) -> bool:
    """Whether ``VGI_STORAGE_ALLOW_PLAIN_HTTP`` opts in to ``http://`` storage URLs.

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        ``True`` when the variable is a true value.

    """
    return _bool(_resolve(env), ENV_ALLOW_PLAIN_HTTP, False)


def s3_storage_from_env(env: Mapping[str, str] | None = None) -> S3Storage:
    """Build an :class:`~vgi_rpc.s3.S3Storage` from the ``VGI_STORAGE_S3_*`` variables (contract §2-§4).

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The storage; it HEADs the bucket before first use.

    Raises:
        ValueError: The bucket is unset or a variable is malformed.

    """
    from vgi_rpc.s3 import S3Storage

    environ = _resolve(env)
    bucket = _get(environ, ENV_S3_BUCKET)
    if not bucket:
        raise ValueError(f"{ENV_BACKEND}=s3 needs {ENV_S3_BUCKET}")
    endpoint = _url(environ, ENV_S3_ENDPOINT)
    public = _url(environ, ENV_S3_PUBLIC_ENDPOINT)
    region = (
        _get(environ, ENV_S3_REGION)
        or _get(environ, "AWS_REGION")
        or _get(environ, "AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    path_style = _bool(environ, ENV_S3_FORCE_PATH_STYLE, endpoint is not None)
    checksum = _get(environ, ENV_S3_CHECKSUM).lower() or "sha256"
    if checksum not in ("sha256", "none"):
        raise ValueError(f"{ENV_S3_CHECKSUM} must be sha256 or none, got {checksum!r}")
    key_id = _get(environ, ENV_S3_ACCESS_KEY_ID) or None
    secret = _get(environ, ENV_S3_SECRET_ACCESS_KEY) or None
    token = _get(environ, ENV_S3_SESSION_TOKEN) or None
    if (key_id is None) != (secret is None):
        raise ValueError(f"{ENV_S3_ACCESS_KEY_ID} and {ENV_S3_SECRET_ACCESS_KEY} must be set together")
    if token is not None and key_id is None:
        raise ValueError(f"{ENV_S3_SESSION_TOKEN} needs {ENV_S3_ACCESS_KEY_ID} and {ENV_S3_SECRET_ACCESS_KEY}")
    # botocore >= 1.36 adds a CRC32 to every PutObject ("when_supported"),
    # which several S3-compatible stores reject.  Off for custom endpoints
    # unless the operator chose via AWS_REQUEST_CHECKSUM_CALCULATION.
    request_checksum = None
    if endpoint is not None and not _get(environ, _ENV_AWS_REQUEST_CHECKSUM):
        request_checksum = "when_required"
    return S3Storage(
        bucket=bucket,
        prefix=_get(environ, ENV_S3_PREFIX),
        presign_expiry_seconds=_url_ttl(environ),
        region_name=region,
        endpoint_url=endpoint,
        public_endpoint_url=public,
        addressing_style="path" if path_style else "virtual",
        access_key_id=key_id,
        secret_access_key=secret,
        session_token=token,
        checksum=cast("Literal['sha256', 'none']", checksum),
        request_checksum_calculation=cast("Literal['when_required'] | None", request_checksum),
        verify_bucket=True,
    )


def gcs_storage_from_env(env: Mapping[str, str] | None = None) -> GCSStorage:
    """Build a :class:`~vgi_rpc.gcs.GCSStorage` from the ``VGI_STORAGE_GCS_*`` variables (contract §5).

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The storage; it checks the bucket before first use.

    Raises:
        ValueError: The bucket is unset or a variable is malformed.

    """
    from vgi_rpc.gcs import GCSStorage

    environ = _resolve(env)
    bucket = _get(environ, ENV_GCS_BUCKET)
    if not bucket:
        raise ValueError(f"{ENV_BACKEND}=gcs needs {ENV_GCS_BUCKET}")
    return GCSStorage(
        bucket=bucket,
        prefix=_get(environ, ENV_GCS_PREFIX),
        presign_expiry_seconds=_url_ttl(environ),
        endpoint_url=_url(environ, ENV_GCS_ENDPOINT),
        credentials_file=_get(environ, ENV_GCS_CREDENTIALS_FILE) or None,
        verify_bucket=True,
    )


def storage_url_validator(storage: S3Storage | GCSStorage, *, allow_plain_http: bool = False) -> Callable[[str], None]:
    """Build a ``url_validator`` pinned to the hosts *storage* signs URLs for.

    ``https`` only, unless *allow_plain_http* (``VGI_STORAGE_ALLOW_PLAIN_HTTP``).
    The host list is taken from a URL the storage signs, on first use.

    Args:
        storage: The direct storage backend.
        allow_plain_http: Also accept ``http://`` URLs on those hosts.

    Returns:
        A callable raising ``ValueError`` for any other URL.

    """
    hosts: list[frozenset[str]] = []

    def validate(url: str) -> None:
        parts = urlsplit(url)
        if parts.scheme != "https" and not (allow_plain_http and parts.scheme == "http"):
            hint = f" (set {ENV_ALLOW_PLAIN_HTTP}=1 to allow http)" if parts.scheme == "http" else ""
            raise ValueError(f"URL scheme {parts.scheme!r} not allowed for storage URLs{hint}")
        if not hosts:
            hosts.append(storage.presign_hosts())
        if parts.netloc.lower() not in hosts[0]:
            raise ValueError(f"URL host {parts.hostname!r} is not this worker's storage host")

    return validate


@dataclass(frozen=True)
class DirectStorageConfig:
    """Server storage settings for the ``s3`` / ``gcs`` backends.

    Attributes:
        backend: ``"s3"`` or ``"gcs"``.
        storage: The storage object (``ExternalStorage`` + ``ObjectStore``).
        external_location: Pass to the RPC server (result externalization,
            inbound pointer fetches, StateRef / argument objects).
        upload_url_provider: Pass to ``make_wsgi_app`` to issue upload
            grants; ``None`` with ``VGI_STORAGE_S3_CHECKSUM=none``.
        verify_at_startup: ``VGI_STORAGE_S3_VERIFY=1``: call
            ``storage.verify()`` before serving (otherwise it runs on first use).

    """

    backend: Literal["s3", "gcs"]
    storage: S3Storage | GCSStorage
    external_location: ServerExternalConfig
    upload_url_provider: UploadUrlProvider | None
    verify_at_startup: bool

    def describe(self) -> str:
        """Return a one-line, secret-free description for startup logs."""
        s = self.storage
        endpoint = getattr(s, "presign_endpoint_url", None) or s.endpoint_url
        grants = "on" if self.upload_url_provider is not None else "off (checksum=none)"
        return (
            f"{self.backend} bucket={s.bucket} prefix={s.prefix!r} endpoint={endpoint or 'default'} "
            f"url_ttl={s.presign_expiry_seconds}s upload_grants={grants}"
        )


def direct_storage_from_env(env: Mapping[str, str] | None = None) -> DirectStorageConfig | None:
    """Server storage for ``VGI_STORAGE_BACKEND`` ``s3`` / ``gcs``; ``None`` for ``vendor`` / ``none``.

    Reads ``VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES``,
    ``VGI_RPC_EXTERNAL_COMPRESSION`` and the inbound fetch cap the same way
    as the vendor path.

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The settings, or ``None`` when the backend is not ``s3`` / ``gcs``.

    Raises:
        ValueError: A variable is missing or malformed, the backends
            conflict, or pre-signed URLs would be ``http://`` without
            ``VGI_STORAGE_ALLOW_PLAIN_HTTP=1``.

    """
    from vgi_rpc.vendor import (
        _DEFAULT_THRESHOLD,
        ENV_COMPRESSION,
        ENV_THRESHOLD,
        _parse_compression,
        _parse_positive_int,
        max_fetch_bytes_from_env,
    )

    environ = _resolve(env)
    backend = storage_backend_from_env(environ)
    if backend not in ("s3", "gcs"):
        return None
    threshold = _parse_positive_int(environ, ENV_THRESHOLD, _DEFAULT_THRESHOLD)
    assert threshold is not None
    compression = _parse_compression(environ.get(ENV_COMPRESSION, ""))
    max_fetch = max_fetch_bytes_from_env(environ)
    allow_http = plain_http_allowed(environ)
    storage: S3Storage | GCSStorage
    if backend == "s3":
        storage = s3_storage_from_env(environ)
        signed_endpoint = storage.presign_endpoint_url
        provider: UploadUrlProvider | None = storage if storage.checksum == "sha256" else None
        verify = _bool(environ, ENV_S3_VERIFY, False)
    else:
        storage = gcs_storage_from_env(environ)
        signed_endpoint = storage.endpoint_url
        provider = storage
        verify = False
    if signed_endpoint and urlsplit(signed_endpoint).scheme == "http" and not allow_http:
        raise ValueError(
            f"Storage URLs would use plain http ({redact_url(signed_endpoint)}); set {ENV_ALLOW_PLAIN_HTTP}=1 "
            "to allow it (the extension needs its vgi_storage_allow_plain_http setting too), or use https"
        )
    external = ServerExternalConfig(
        storage=storage,
        externalize_threshold_bytes=threshold,
        compression=compression,
        fetch_config=FetchConfig(max_fetch_bytes=max_fetch),
        url_validator=storage_url_validator(storage, allow_plain_http=allow_http),
    )
    return DirectStorageConfig(
        backend=backend,
        storage=storage,
        external_location=external,
        upload_url_provider=provider,
        verify_at_startup=verify,
    )


class ObjectStoreBlobClient:
    """A :class:`~vgi_rpc.vendor.VendorBlobClient`-compatible client over an ``ObjectStore``.

    Lets code written for the vendor's ``put_bytes`` / ``get_bytes`` (e.g.
    function-storage externalization) use a direct S3 / GCS bucket.  Reads
    verify the size and SHA-256 recorded in the :class:`BlobRef`.  Deletes
    are no-ops: the worker never deletes from the operator's bucket (its
    lifecycle rule does).
    """

    def __init__(self, store: ObjectStore, *, max_fetch_bytes: int = DEFAULT_MAX_FETCH_BYTES) -> None:
        """Wrap *store*.

        Args:
            store: The object store (``S3Storage`` / ``GCSStorage``).
            max_fetch_bytes: Largest blob :meth:`get_bytes` accepts.

        """
        self._store = store
        self._max = max_fetch_bytes

    def put_bytes(self, data: bytes, *, scope: object = None, idempotency_key: str | None = None) -> BlobRef:
        """Store *data*; return its reference (``scope`` / ``idempotency_key`` are vendor-only, ignored).

        Args:
            data: The bytes.
            scope: Ignored.
            idempotency_key: Ignored (every put is a fresh object).

        Returns:
            The object's id, size and SHA-256.

        """
        from vgi_rpc.vendor import BlobRef

        del scope, idempotency_key
        object_id = self._store.put_object(data)
        return BlobRef(object_id=object_id, size=len(data), sha256=hashlib.sha256(data).hexdigest())

    def get_bytes(self, ref: BlobRef) -> bytes:
        """Fetch *ref* and verify its size and SHA-256.

        Args:
            ref: The reference :meth:`put_bytes` returned.

        Returns:
            The bytes.

        Raises:
            vgi_rpc.vendor.VendorError: Size or digest mismatch, or the
                object is over the fetch cap.

        """
        from vgi_rpc.vendor import VendorError

        if ref.size > self._max:
            raise VendorError(f"Blob {ref.object_id} is {ref.size} bytes, over max_fetch_bytes ({self._max})")
        data = self._store.get(ref.object_id, max_bytes=self._max)
        if len(data) != ref.size:
            raise VendorError(
                f"Blob {ref.object_id} size mismatch: expected {ref.size}, got {len(data)}", code="size_mismatch"
            )
        if hashlib.sha256(data).hexdigest() != ref.sha256.lower():
            raise VendorError(f"Blob {ref.object_id} SHA-256 mismatch", code="sha256_mismatch")
        return data

    def delete(self, refs: Iterable[object]) -> int:
        """Do nothing: the bucket's lifecycle rule removes objects.

        Args:
            refs: Ignored.

        Returns:
            ``0``.

        """
        del refs
        return 0

    def delete_scope(self, shard: str, execution_id: str) -> int:
        """Do nothing: the bucket's lifecycle rule removes objects.

        Args:
            shard: Ignored.
            execution_id: Ignored.

        Returns:
            ``0``.

        """
        del shard, execution_id
        return 0

    def close(self) -> None:
        """Release nothing (the store owns its clients)."""

    def __enter__(self) -> ObjectStoreBlobClient:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager."""
        self.close()
