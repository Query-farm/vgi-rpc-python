# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""S3 storage backend for ExternalLocation batches.

Optional dependency: ``pip install vgi-rpc[s3]``

Provides ``S3Storage``, an ``ExternalStorage`` implementation that
uploads IPC data to S3 (or any S3-compatible service) and returns
pre-signed URLs for retrieval.  It also serves as an ``ObjectStore``
(StateRef objects, argument references) and an ``UploadUrlProvider``
(sized, SHA-256-bound client upload grants).

Credentials come from explicit keys when given, otherwise from the standard
AWS chain (environment, shared config / profile incl. ``credential_process``
and ``role_arn``, web identity, container credentials, EC2 instance
metadata).  Temporary credentials are refreshed by botocore before they
expire, and a pre-signed URL is never given a lifetime longer than the
credentials that sign it (minus 60 s).
"""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit

import pyarrow as pa

from vgi_rpc.external import (
    ExternalStorage,
    ObjectMissingError,
    ObjectStore,
    UploadUrl,
    UploadUrlProvider,
    sha256_b64_from_hex,
)
from vgi_rpc.external_fetch import (
    DEFAULT_MAX_FETCH_BYTES,
    DEFAULT_STORAGE_CONNECT_TIMEOUT_SECONDS,
    storage_stall_timeout,
)

_logger = logging.getLogger(__name__)

__all__ = ["UPLOAD_GRANT_MIN_SECONDS", "S3Storage", "StorageConfigError"]

#: Shortest lifetime an upload grant may be signed with (contract §4): when
#: the signing credentials expire sooner, they are refreshed first.
UPLOAD_GRANT_MIN_SECONDS = 15 * 60

#: Margin kept between a URL's expiry and its signing credentials' expiry.
_CREDENTIAL_EXPIRY_MARGIN_SECONDS = 60


class StorageConfigError(RuntimeError):
    """The storage backend is misconfigured or unreachable (bucket, credentials, region, endpoint).

    The message says what to change; it never contains a secret.
    """


@dataclass
class _S3Pool:
    """Mutable client state owned by a ``S3Storage``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    session: Any = None
    client: Any = None
    presign_client: Any = None
    verified: bool = False


@dataclass(frozen=True)
class S3Storage:
    """S3-backed ``ExternalStorage`` / ``ObjectStore`` / ``UploadUrlProvider`` using boto3.

    .. important:: **Object lifecycle** — uploaded objects persist
       indefinitely; the worker never deletes them.  Configure an
       `S3 Lifecycle Policy`_ on the bucket to expire objects under
       ``prefix`` (default ``vgi-rpc/``) after one to two days.
       See :mod:`vgi_rpc.external` for full details and examples.

    Attributes:
        bucket: S3 bucket name.
        prefix: Key prefix for uploaded objects.
        presign_expiry_seconds: Lifetime of pre-signed URLs, capped by the
            remaining lifetime of temporary signing credentials minus 60 s.
        region_name: Signing region (``auto`` for Cloudflare R2; ``None``
            uses boto3's default resolution).
        endpoint_url: Custom endpoint for S3-compatible services
            (e.g. MinIO, Ceph, LocalStack, Cloudflare R2's S3 API, Wasabi,
            Backblaze B2); ``http://`` endpoints work.  ``None`` = AWS.
        public_endpoint_url: Endpoint used *only* for the pre-signed URLs
            handed to clients, when they reach storage at a different
            address than this process does (e.g. Docker-internal
            ``http://minio:9000`` vs public ``https://s3.example.com``).
            ``None`` = ``endpoint_url``.
        addressing_style: ``"path"`` (``endpoint/bucket/key``, needed by
            MinIO and Ceph without wildcard DNS), ``"virtual"``
            (``bucket.endpoint/key``) or ``"auto"``; ``None`` keeps
            botocore's default (path-style for a custom endpoint).
        access_key_id: Explicit access key; ``None`` uses the AWS chain.
        secret_access_key: Secret for ``access_key_id``.
        session_token: Session token for temporary explicit keys.
        checksum: ``"sha256"`` (default): upload grants sign
            ``x-amz-checksum-sha256`` and stored objects carry one.
            ``"none"``: for services that do not enforce that header -- no
            checksum headers are sent and :meth:`generate_upload_url`
            refuses (results are still externalized; readers verify).
        request_checksum_calculation: botocore's ``"when_supported"``
            (adds a CRC32 to every ``PutObject``, which some S3-compatible
            stores reject) or ``"when_required"``.  ``None`` keeps
            botocore's default / ``AWS_REQUEST_CHECKSUM_CALCULATION``.
        verify_bucket: HEAD the bucket before first use and fail with an
            actionable :class:`StorageConfigError` (missing bucket, access
            denied, wrong region).  :meth:`verify` runs it explicitly.
        session_expires: When explicit temporary keys (``session_token``)
            expire.  Pre-signed URLs are capped at it minus 60 s; ``None``
            means unknown, so URLs signed with explicit temporary keys cannot
            be capped.
        stall_timeout_seconds: Fail an S3 request when no bytes move for this
            long (botocore ``read_timeout``, applied to each socket read and
            write).  ``None`` reads ``VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS``
            (default 30).  Connect is bounded by 30 s.  Requests have no
            total deadline; transient failures (connect errors, stalls, 429,
            5xx) get 3 attempts (botocore "standard" retry mode).

    .. _S3 Lifecycle Policy:
       https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    region_name: str | None = None
    endpoint_url: str | None = field(default=None, repr=False)
    public_endpoint_url: str | None = field(default=None, repr=False)
    addressing_style: Literal["auto", "path", "virtual"] | None = None
    access_key_id: str | None = field(default=None, repr=False)
    secret_access_key: str | None = field(default=None, repr=False)
    session_token: str | None = field(default=None, repr=False)
    checksum: Literal["sha256", "none"] = "sha256"
    request_checksum_calculation: Literal["when_supported", "when_required"] | None = None
    verify_bucket: bool = False
    session_expires: datetime | None = None
    stall_timeout_seconds: float | None = None
    _pool: _S3Pool = field(default_factory=_S3Pool, init=False, repr=False, compare=False, hash=False)

    def __post_init__(self) -> None:
        """Check field combinations that boto3 would only reject later."""
        if (self.access_key_id is None) != (self.secret_access_key is None):
            raise ValueError("S3Storage: access_key_id and secret_access_key must be given together")
        if self.session_token is not None and self.access_key_id is None:
            raise ValueError("S3Storage: session_token needs access_key_id and secret_access_key")
        if self.checksum not in ("sha256", "none"):
            raise ValueError(f"S3Storage: checksum must be 'sha256' or 'none', got {self.checksum!r}")
        if self.presign_expiry_seconds <= 0:
            raise ValueError("S3Storage: presign_expiry_seconds must be positive")

    # -- clients -------------------------------------------------------------

    def _config(self) -> Any:
        from botocore.config import Config

        stall = self.stall_timeout_seconds if self.stall_timeout_seconds is not None else storage_stall_timeout()
        kwargs: dict[str, Any] = {}
        if self.request_checksum_calculation is not None:
            kwargs["request_checksum_calculation"] = self.request_checksum_calculation
        # SigV4 explicitly: botocore may presign with legacy SigV2 (e.g. in
        # us-east-1), which signs neither Content-Length nor
        # x-amz-checksum-sha256 as headers, so a sized upload URL would not
        # bind its body.
        return Config(
            signature_version="s3v4",
            connect_timeout=DEFAULT_STORAGE_CONNECT_TIMEOUT_SECONDS,
            read_timeout=stall,
            retries={"total_max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": self.addressing_style} if self.addressing_style else None,
            **kwargs,
        )

    def _ensure_clients(self) -> _S3Pool:
        pool = self._pool
        with pool.lock:
            if pool.client is None:
                import boto3

                # One session per storage: both clients share its credential
                # provider, so a refresh (temporary credentials) reaches both.
                session = boto3.session.Session(
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    aws_session_token=self.session_token,
                    region_name=self.region_name,
                )
                config = self._config()
                pool.session = session
                pool.client = session.client("s3", endpoint_url=self.endpoint_url, config=config)
                pool.presign_client = (
                    session.client("s3", endpoint_url=self.public_endpoint_url, config=config)
                    if self.public_endpoint_url
                    else pool.client
                )
            return pool

    def _get_client(self) -> Any:
        """Lazy-create and cache the boto3 S3 client used for storage requests."""
        return self._ensure_clients().client

    def _get_presign_client(self) -> Any:
        """Return the client whose endpoint appears in pre-signed URLs (the public endpoint)."""
        pool = self._ensure_clients()
        return pool.presign_client if pool.presign_client is not None else pool.client

    @property
    def presign_endpoint_url(self) -> str | None:
        """The endpoint pre-signed URLs point at (``public_endpoint_url`` or ``endpoint_url``)."""
        return self.public_endpoint_url or self.endpoint_url

    def presign_hosts(self) -> frozenset[str]:
        """Return the ``host[:port]`` values this storage's pre-signed URLs use (for URL pinning)."""
        url = self._get_presign_client().generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": f"{self.prefix}probe"}, ExpiresIn=60
        )
        return frozenset({urlsplit(url).netloc.lower()})

    # -- URL lifetime (temporary credentials) ----------------------------------

    def _presign_expiry(self, min_seconds: int = 0) -> int:
        """Seconds a URL signed now may live: ``min(TTL, credential expiry - now - 60 s)``.

        Temporary credentials whose remaining lifetime is below *min_seconds*
        (plus the margin) are refreshed before signing.
        """
        ttl = self.presign_expiry_seconds
        session = self._ensure_clients().session
        if session is None:
            return ttl
        credentials = session.get_credentials()
        if credentials is None:
            return ttl
        # Gives botocore the chance to do its own (advisory) refresh.
        credentials.get_frozen_credentials()
        remaining = _remaining_seconds(credentials)
        if remaining is None and self.session_expires is not None and self.access_key_id is not None:
            expires = self.session_expires if self.session_expires.tzinfo else self.session_expires.replace(tzinfo=UTC)
            remaining = (expires - datetime.now(UTC)).total_seconds()
        if remaining is None:
            return ttl
        needed = max(min_seconds, 1) + _CREDENTIAL_EXPIRY_MARGIN_SECONDS
        if remaining < needed:
            refresh = getattr(credentials, "_protected_refresh", None)
            if callable(refresh):
                try:
                    refresh(is_mandatory=True)
                except Exception as exc:
                    raise StorageConfigError(
                        f"S3 credentials expire in {int(remaining)} s and could not be refreshed ({type(exc).__name__})"
                    ) from None
                remaining = _remaining_seconds(credentials) or remaining
            if remaining < needed:
                _logger.warning(
                    "S3 signing credentials expire in %d s, less than the %d s a URL here needs; "
                    "signing for the remaining lifetime",
                    int(remaining),
                    needed,
                )
        return max(1, min(ttl, int(remaining) - _CREDENTIAL_EXPIRY_MARGIN_SECONDS))

    # -- startup check -----------------------------------------------------------

    def verify(self) -> None:
        """HEAD the bucket and raise an actionable :class:`StorageConfigError` on failure.

        Raises:
            StorageConfigError: The bucket is missing, access is denied, the
                region is wrong (the message names the bucket's region), the
                endpoint is unreachable, or no credentials were found.

        """
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

        client = self._get_client()
        where = f"endpoint {self.endpoint_url}" if self.endpoint_url else "AWS"
        try:
            client.head_bucket(Bucket=self.bucket)
        except NoCredentialsError:
            raise StorageConfigError(
                "No S3 credentials found: set VGI_STORAGE_S3_ACCESS_KEY_ID/VGI_STORAGE_S3_SECRET_ACCESS_KEY, "
                "or provide AWS credentials (AWS_* env, profile, web identity, container or instance role)"
            ) from None
        except ClientError as exc:
            meta = exc.response.get("ResponseMetadata", {})
            status = meta.get("HTTPStatusCode")
            headers = {str(k).lower(): v for k, v in (meta.get("HTTPHeaders") or {}).items()}
            bucket_region = headers.get("x-amz-bucket-region")
            signing_region = client.meta.region_name
            if bucket_region and bucket_region != signing_region and status in (301, 307, 400, 403):
                raise StorageConfigError(
                    f"S3 bucket {self.bucket!r} is in region {bucket_region!r}, but requests are signed for "
                    f"{signing_region!r}: set VGI_STORAGE_S3_REGION={bucket_region}"
                ) from None
            if status == 404:
                raise StorageConfigError(f"S3 bucket {self.bucket!r} does not exist at {where}") from None
            if status == 403:
                method = self._credential_method()
                raise StorageConfigError(
                    f"Access denied to S3 bucket {self.bucket!r} at {where} with credentials from {method}: "
                    "grant s3:GetObject, s3:PutObject and s3:ListBucket on the bucket and prefix"
                ) from None
            code = exc.response.get("Error", {}).get("Code", "")
            raise StorageConfigError(
                f"S3 bucket {self.bucket!r} check failed at {where}: HTTP {status} {code}"
            ) from None
        except BotoCoreError as exc:
            raise StorageConfigError(
                f"Cannot reach S3 {where} for bucket {self.bucket!r}: {type(exc).__name__}"
            ) from None

    def _credential_method(self) -> str:
        """Where the credentials came from (``explicit``, ``env``, ``iam-role``, ...); never the secret."""
        if self.access_key_id is not None:
            return "explicit keys"
        session = self._pool.session
        creds = session.get_credentials() if session is not None else None
        return str(getattr(creds, "method", None) or "unknown")

    def _ensure_verified(self) -> None:
        if not self.verify_bucket or self._pool.verified:
            return
        self.verify()
        self._pool.verified = True

    # -- operations --------------------------------------------------------------

    def _put(self, key: str, data: bytes, **extra: Any) -> None:
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            # A file object, not bytes: botocore then writes the body in small
            # blocks, so the stall timeout bounds each write.  A bytes body
            # over TLS is one write under one timeout for the whole object.
            "Body": io.BytesIO(data),
            "ContentLength": len(data),
            "ContentType": "application/octet-stream",
            **extra,
        }
        if self.checksum == "sha256":
            kwargs["ChecksumSHA256"] = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        self._get_client().put_object(**kwargs)

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Upload IPC data to S3 and return a pre-signed GET URL.

        Args:
            data: Serialized Arrow IPC stream bytes.
            schema: Schema of the data (unused but required by protocol).
            content_encoding: Optional encoding applied to *data*
                (e.g. ``"zstd"``).

        Returns:
            A pre-signed URL that can be used to download the data.

        """
        self._ensure_verified()
        ext = ".arrow.zst" if content_encoding == "zstd" else ".arrow"
        key = f"{self.prefix}{uuid.uuid4().hex}{ext}"
        extra = {"ContentEncoding": content_encoding} if content_encoding is not None else {}

        t0 = time.monotonic()
        try:
            self._put(key, data, **extra)
        except Exception as exc:
            _logger.error(
                "S3 upload failed: bucket=%s key=%s",
                self.bucket,
                key,
                exc_info=True,
                extra={"bucket": self.bucket, "key": key, "error_type": type(exc).__name__},
            )
            raise
        duration_ms = (time.monotonic() - t0) * 1000

        url: str = self._get_presign_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self._presign_expiry(),
        )
        _logger.debug(
            "S3 upload completed: bucket=%s key=%s (%d bytes, %.1fms)",
            self.bucket,
            key,
            len(data),
            duration_ms,
            extra={"bucket": self.bucket, "key": key, "size_bytes": len(data), "duration_ms": round(duration_ms, 2)},
        )
        return url

    def generate_upload_url(self, size: int, sha256_hex: str) -> UploadUrl:
        """Generate pre-signed PUT and GET URLs for one sized client upload.

        The PUT URL signs ``Content-Length`` and ``x-amz-checksum-sha256``
        (SigV4), so S3 rejects any body that is not exactly *size* bytes
        hashing to *sha256_hex* (``BadDigest``).  Both URLs live at least
        15 minutes (credentials are refreshed first if needed) and at most
        ``presign_expiry_seconds``.  The created S3 object is not
        automatically deleted.  Configure S3 Lifecycle Policies on the
        bucket to expire objects after a suitable retention period.

        Args:
            size: Exact byte length the client will PUT.
            sha256_hex: SHA-256 of those bytes, 64 lowercase hex.

        Returns:
            An ``UploadUrl`` with PUT and GET pre-signed URLs for the
            same S3 object and the ``x-amz-checksum-sha256`` header the
            PUT must carry.

        Raises:
            StorageConfigError: ``checksum="none"``: this storage cannot
                bind an upload to its digest, so it issues no grants.

        """
        if self.checksum != "sha256":
            raise StorageConfigError(
                "Upload grants are disabled: S3 checksum is 'none' (VGI_STORAGE_S3_CHECKSUM), so the storage "
                "cannot enforce the upload's SHA-256"
            )
        checksum = sha256_b64_from_hex(sha256_hex)
        self._ensure_verified()
        client = self._get_presign_client()
        key = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        params = {"Bucket": self.bucket, "Key": key}
        expiry = self._presign_expiry(min(UPLOAD_GRANT_MIN_SECONDS, self.presign_expiry_seconds))
        expires_at = datetime.now(UTC) + timedelta(seconds=expiry)

        put_url: str = client.generate_presigned_url(
            "put_object",
            Params={**params, "ContentLength": size, "ChecksumSHA256": checksum},
            ExpiresIn=expiry,
        )
        get_url: str = client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expiry,
        )

        _logger.debug(
            "S3 upload URL generated: bucket=%s key=%s size=%d",
            self.bucket,
            key,
            size,
            extra={"bucket": self.bucket, "key": key, "size_bytes": size},
        )
        return UploadUrl(
            upload_url=put_url,
            download_url=get_url,
            expires_at=expires_at,
            headers={"x-amz-checksum-sha256": checksum},
            object_id=key,
        )

    def put_object(self, data: bytes) -> str:
        """Store *data* under a fresh key and return the key (``ObjectStore``).

        A SigV4 ``PutObject`` with ``ContentLength`` and (unless
        ``checksum="none"``) ``ChecksumSHA256``, so S3/R2 verify the body.
        Objects carry no TTL: set a lifecycle rule of at least one day on
        ``prefix``.

        Args:
            data: The bytes to store.

        Returns:
            The object key (the ``object_id``).

        """
        self._ensure_verified()
        key = f"{self.prefix}{uuid.uuid4().hex}.bin"
        self._put(key, data)
        return key

    def delete_object(self, object_id: str) -> None:
        """Delete the object *object_id* (a key); best effort, failures are logged and ignored.

        Args:
            object_id: The object key.

        """
        try:
            self._get_client().delete_object(Bucket=self.bucket, Key=object_id)
        except Exception as exc:
            _logger.debug("S3 delete failed (ignored): key=%s %s", object_id, type(exc).__name__)

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch the object *object_id* (a key) names (``ObjectStore``).

        Args:
            object_id: The object key.
            max_bytes: Largest object accepted; ``None`` means 256 MiB.

        Returns:
            The object bytes.

        Raises:
            ObjectMissingError: S3 reports ``NoSuchKey`` / 404.
            ValueError: The object is larger than *max_bytes*.

        """
        self._ensure_verified()
        limit = max_bytes if max_bytes is not None else DEFAULT_MAX_FETCH_BYTES
        client = self._get_client()
        try:
            resp = client.get_object(Bucket=self.bucket, Key=object_id)
        except Exception as exc:
            error = getattr(exc, "response", None)
            code = error.get("Error", {}).get("Code") if isinstance(error, dict) else None
            if code in ("NoSuchKey", "404", "NotFound"):
                raise ObjectMissingError("S3 object is gone") from None
            raise
        body = resp["Body"]
        try:
            length = resp.get("ContentLength")
            if isinstance(length, int) and length > limit:
                raise ValueError(f"S3 object is {length} bytes, over max_fetch_bytes ({limit})")
            data: bytes = body.read(limit + 1)
        finally:
            body.close()
        if len(data) > limit:
            raise ValueError(f"S3 object exceeds max_fetch_bytes ({limit})")
        return data


def _remaining_seconds(credentials: Any) -> float | None:
    """Seconds until temporary *credentials* expire, or ``None`` for static ones."""
    expiry = getattr(credentials, "_expiry_time", None)
    if not isinstance(expiry, datetime):
        return None
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=UTC)
    return (expiry - datetime.now(UTC)).total_seconds()


# Runtime check that S3Storage satisfies ExternalStorage and UploadUrlProvider protocols
def _check_protocol() -> None:
    """Verify S3Storage satisfies ExternalStorage and UploadUrlProvider at import time."""
    _inst = S3Storage(bucket="test")
    _storage: ExternalStorage = _inst
    _objects: ObjectStore = _inst
    _provider: UploadUrlProvider = _inst


_check_protocol()
