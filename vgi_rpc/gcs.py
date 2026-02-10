"""Google Cloud Storage backend for ExternalLocation batches.

Optional dependency: ``pip install vgi-rpc[gcs]``

Provides ``GCSStorage``, an ``ExternalStorage`` implementation that
uploads IPC data to GCS and returns signed URLs for retrieval.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pyarrow as pa

from vgi_rpc.external import ExternalStorage

__all__ = ["GCSStorage"]


@dataclass
class _GCSPool:
    """Mutable connection-pool state owned by a ``GCSStorage``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    client: Any = None


@dataclass(frozen=True)
class GCSStorage:
    """GCS-backed ``ExternalStorage`` using google-cloud-storage.

    Attributes:
        bucket: GCS bucket name.
        prefix: Key prefix for uploaded objects.
        presign_expiry_seconds: Lifetime of signed GET URLs.
        project: GCS project ID (``None`` uses Application Default
            Credentials default project).
        content_encoding: Optional HTTP ``Content-Encoding`` header
            to set on stored objects (e.g. ``"zstd"``).

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    project: str | None = None
    content_encoding: str | None = None
    _pool: _GCSPool = field(default_factory=_GCSPool, init=False, repr=False, compare=False, hash=False)

    def _get_client(self) -> Any:
        """Lazy-create and cache the google-cloud-storage client."""
        pool = self._pool
        with pool.lock:
            if pool.client is None:
                from google.cloud import storage

                pool.client = storage.Client(project=self.project)
            return pool.client

    def upload(self, data: bytes, schema: pa.Schema) -> str:
        """Upload IPC data to GCS and return a signed GET URL.

        Args:
            data: Serialized Arrow IPC stream bytes.
            schema: Schema of the data (unused but required by protocol).

        Returns:
            A signed URL that can be used to download the data.

        """
        client = self._get_client()
        bucket = client.bucket(self.bucket)
        ext = ".arrow.zst" if self.content_encoding == "zstd" else ".arrow"
        blob_name = f"{self.prefix}{uuid.uuid4().hex}{ext}"
        blob = bucket.blob(blob_name)
        if self.content_encoding is not None:
            blob.content_encoding = self.content_encoding
        blob.upload_from_string(data, content_type="application/octet-stream")

        url: str = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=self.presign_expiry_seconds),
            method="GET",
        )
        return url


# Runtime check that GCSStorage satisfies ExternalStorage protocol
def _check_protocol() -> None:
    """Verify GCSStorage satisfies ExternalStorage at import time."""
    storage: ExternalStorage = GCSStorage(bucket="test")  # noqa: F841


_check_protocol()
