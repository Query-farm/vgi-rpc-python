# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""S3 storage backend for ExternalLocation batches.

Optional dependency: ``pip install vgi-rpc[s3]``

Provides ``S3Storage``, an ``ExternalStorage`` implementation that
uploads IPC data to S3 and returns pre-signed URLs for retrieval.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa

from vgi_rpc.external import ExternalStorage, UploadUrl, UploadUrlProvider

_logger = logging.getLogger(__name__)

__all__ = ["S3Storage"]


@dataclass
class _S3Pool:
    """Mutable connection-pool state owned by a ``S3Storage``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    client: Any = None


@dataclass(frozen=True)
class S3Storage:
    """S3-backed ``ExternalStorage`` using boto3.

    .. important:: **Object lifecycle** — uploaded objects persist
       indefinitely.  Configure an `S3 Lifecycle Policy`_ on the
       bucket to expire objects under ``prefix`` (default
       ``vgi-rpc/``) after a suitable retention period.
       See :mod:`vgi_rpc.external` for full details and examples.

    Attributes:
        bucket: S3 bucket name.
        prefix: Key prefix for uploaded objects.
        presign_expiry_seconds: Lifetime of pre-signed GET URLs.
        region_name: AWS region (``None`` uses boto3 default).
        endpoint_url: Custom endpoint for S3-compatible services
            (e.g. MinIO, LocalStack).

    .. _S3 Lifecycle Policy:
       https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    region_name: str | None = None
    endpoint_url: str | None = field(default=None, repr=False)
    _pool: _S3Pool = field(default_factory=_S3Pool, init=False, repr=False, compare=False, hash=False)

    def _get_client(self) -> Any:
        """Lazy-create and cache the boto3 S3 client."""
        pool = self._pool
        with pool.lock:
            if pool.client is None:
                import boto3

                pool.client = boto3.client(
                    "s3",
                    region_name=self.region_name,
                    endpoint_url=self.endpoint_url,
                )
            return pool.client

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
        client = self._get_client()

        ext = ".arrow.zst" if content_encoding == "zstd" else ".arrow"
        key = f"{self.prefix}{uuid.uuid4().hex}{ext}"

        put_kwargs: dict[str, str | bytes] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": "application/octet-stream",
        }
        if content_encoding is not None:
            put_kwargs["ContentEncoding"] = content_encoding

        t0 = time.monotonic()
        try:
            client.put_object(**put_kwargs)
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

        url: str = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.presign_expiry_seconds,
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

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Generate pre-signed PUT and GET URLs for client-side upload.

        The created S3 object is not automatically deleted.  Configure
        S3 Lifecycle Policies on the bucket to expire objects after
        a suitable retention period.

        Args:
            schema: The Arrow schema of the data to be uploaded
                (unused but available for metadata hints).

        Returns:
            An ``UploadUrl`` with PUT and GET pre-signed URLs for the
            same S3 object.

        """
        client = self._get_client()
        key = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        params = {"Bucket": self.bucket, "Key": key}
        expires_at = datetime.now(UTC) + timedelta(seconds=self.presign_expiry_seconds)

        put_url: str = client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )
        get_url: str = client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )

        _logger.debug(
            "S3 upload URL generated: bucket=%s key=%s",
            self.bucket,
            key,
            extra={"bucket": self.bucket, "key": key},
        )
        return UploadUrl(upload_url=put_url, download_url=get_url, expires_at=expires_at)


# Runtime check that S3Storage satisfies ExternalStorage and UploadUrlProvider protocols
def _check_protocol() -> None:
    """Verify S3Storage satisfies ExternalStorage and UploadUrlProvider at import time."""
    _inst = S3Storage(bucket="test")
    _storage: ExternalStorage = _inst
    _provider: UploadUrlProvider = _inst


_check_protocol()
