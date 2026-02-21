# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Google Cloud Storage backend for ExternalLocation batches.

Optional dependency: ``pip install vgi-rpc[gcs]``

Provides ``GCSStorage``, an ``ExternalStorage`` implementation that
uploads IPC data to GCS and returns signed URLs for retrieval.
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

__all__ = ["GCSStorage"]


@dataclass
class _GCSPool:
    """Mutable connection-pool state owned by a ``GCSStorage``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    client: Any = None


@dataclass(frozen=True)
class GCSStorage:
    """GCS-backed ``ExternalStorage`` using google-cloud-storage.

    .. important:: **Object lifecycle** — uploaded objects persist
       indefinitely.  Configure `Object Lifecycle Management`_ on the
       bucket to delete objects under ``prefix`` (default
       ``vgi-rpc/``) after a suitable retention period.
       See :mod:`vgi_rpc.external` for full details and examples.

    Attributes:
        bucket: GCS bucket name.
        prefix: Key prefix for uploaded objects.
        presign_expiry_seconds: Lifetime of signed GET URLs.
        project: GCS project ID (``None`` uses Application Default
            Credentials default project).

    .. _Object Lifecycle Management:
       https://cloud.google.com/storage/docs/lifecycle

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    project: str | None = None
    _pool: _GCSPool = field(default_factory=_GCSPool, init=False, repr=False, compare=False, hash=False)

    def _get_client(self) -> Any:
        """Lazy-create and cache the google-cloud-storage client."""
        pool = self._pool
        with pool.lock:
            if pool.client is None:
                from google.cloud import storage

                pool.client = storage.Client(project=self.project)
            return pool.client

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Upload IPC data to GCS and return a signed GET URL.

        Args:
            data: Serialized Arrow IPC stream bytes.
            schema: Schema of the data (unused but required by protocol).
            content_encoding: Optional encoding applied to *data*
                (e.g. ``"zstd"``).

        Returns:
            A signed URL that can be used to download the data.

        """
        client = self._get_client()
        bucket = client.bucket(self.bucket)
        ext = ".arrow.zst" if content_encoding == "zstd" else ".arrow"
        blob_name = f"{self.prefix}{uuid.uuid4().hex}{ext}"
        blob = bucket.blob(blob_name)
        if content_encoding is not None:
            blob.content_encoding = content_encoding
        t0 = time.monotonic()
        try:
            blob.upload_from_string(data, content_type="application/octet-stream")
        except Exception as exc:
            _logger.error(
                "GCS upload failed: bucket=%s key=%s",
                self.bucket,
                blob_name,
                exc_info=True,
                extra={"bucket": self.bucket, "key": blob_name, "error_type": type(exc).__name__},
            )
            raise
        duration_ms = (time.monotonic() - t0) * 1000

        url: str = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=self.presign_expiry_seconds),
            method="GET",
        )
        _logger.debug(
            "GCS upload completed: bucket=%s key=%s (%d bytes, %.1fms)",
            self.bucket,
            blob_name,
            len(data),
            duration_ms,
            extra={
                "bucket": self.bucket,
                "key": blob_name,
                "size_bytes": len(data),
                "duration_ms": round(duration_ms, 2),
            },
        )
        return url

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Generate signed PUT and GET URLs for client-side upload.

        The created GCS object is not automatically deleted.  Configure
        GCS Object Lifecycle Management on the bucket to expire objects
        after a suitable retention period.

        Args:
            schema: The Arrow schema of the data to be uploaded
                (unused but available for metadata hints).

        Returns:
            An ``UploadUrl`` with PUT and GET signed URLs for the
            same GCS object.

        """
        client = self._get_client()
        bucket = client.bucket(self.bucket)
        blob_name = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        blob = bucket.blob(blob_name)
        expiration = timedelta(seconds=self.presign_expiry_seconds)
        expires_at = datetime.now(UTC) + expiration

        put_url: str = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="PUT",
            content_type="application/octet-stream",
        )
        get_url: str = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET",
        )

        _logger.debug(
            "GCS upload URL generated: bucket=%s key=%s",
            self.bucket,
            blob_name,
            extra={"bucket": self.bucket, "key": blob_name},
        )
        return UploadUrl(upload_url=put_url, download_url=get_url, expires_at=expires_at)


# Runtime check that GCSStorage satisfies ExternalStorage and UploadUrlProvider protocols
def _check_protocol() -> None:
    """Verify GCSStorage satisfies ExternalStorage and UploadUrlProvider at import time."""
    _inst = GCSStorage(bucket="test")
    _storage: ExternalStorage = _inst
    _provider: UploadUrlProvider = _inst


_check_protocol()
