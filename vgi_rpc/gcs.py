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

from vgi_rpc.external import (
    ExternalStorage,
    ObjectMissingError,
    ObjectStore,
    UploadUrl,
    UploadUrlProvider,
    is_sha256_hex,
)
from vgi_rpc.external_fetch import (
    DEFAULT_MAX_FETCH_BYTES,
    DEFAULT_STORAGE_CONNECT_TIMEOUT_SECONDS,
    STORAGE_FLOOR_BYTES_PER_SECOND,
    storage_stall_timeout,
)

_logger = logging.getLogger(__name__)

__all__ = ["GCSStorage"]

#: Upload request size: uploads over 8 MiB go resumable in 8 MiB chunks (a
#: multiple of 256 KiB, as GCS requires), so no single request is larger.
_UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024


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
        stall_timeout_seconds: Read timeout (no bytes arriving) for GCS
            requests.  ``None`` reads ``VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS``
            (default 30).  Connect is bounded by 30 s.
        upload_floor_bytes_per_second: google-cloud-storage writes each
            upload request (at most 8 MiB) as one timed socket write, so an
            upload's read timeout is raised to cover a full request at this
            rate (default 64 KiB/s: 128 s for an 8 MiB chunk).

    Uploads are retried on transient errors with the library's default retry
    (429, 5xx, connection errors); every object name is fresh, so a retried
    upload cannot overwrite another object.

    .. _Object Lifecycle Management:
       https://cloud.google.com/storage/docs/lifecycle

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    project: str | None = None
    stall_timeout_seconds: float | None = None
    upload_floor_bytes_per_second: int = STORAGE_FLOOR_BYTES_PER_SECOND
    _pool: _GCSPool = field(default_factory=_GCSPool, init=False, repr=False, compare=False, hash=False)

    def _get_client(self) -> Any:
        """Lazy-create and cache the google-cloud-storage client."""
        pool = self._pool
        with pool.lock:
            if pool.client is None:
                from google.cloud import storage

                pool.client = storage.Client(project=self.project)
            return pool.client

    def _stall(self) -> float:
        return self.stall_timeout_seconds if self.stall_timeout_seconds is not None else storage_stall_timeout()

    def _read_timeout(self) -> tuple[float, float]:
        """``(connect, read)`` for downloads and metadata calls."""
        return (DEFAULT_STORAGE_CONNECT_TIMEOUT_SECONDS, self._stall())

    def _upload_timeout(self, size: int) -> tuple[float, float]:
        """``(connect, read)`` for an upload: the read timeout covers one request at the floor rate."""
        per_request = min(size, _UPLOAD_CHUNK_BYTES) / max(1, self.upload_floor_bytes_per_second)
        return (DEFAULT_STORAGE_CONNECT_TIMEOUT_SECONDS, max(self._stall(), per_request))

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
        blob = bucket.blob(blob_name, chunk_size=_UPLOAD_CHUNK_BYTES)
        if content_encoding is not None:
            blob.content_encoding = content_encoding
        t0 = time.monotonic()
        try:
            from google.cloud.storage.retry import DEFAULT_RETRY

            blob.upload_from_string(
                data,
                content_type="application/octet-stream",
                timeout=self._upload_timeout(len(data)),
                retry=DEFAULT_RETRY,
            )
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

    def generate_upload_url(self, size: int, sha256_hex: str) -> UploadUrl:
        """Generate signed PUT and GET URLs for one sized client upload.

        GCS cannot verify a SHA-256 at storage, so the PUT URL signs the
        length (``x-goog-content-length-range: size,size``) and the content
        type; the server's ``vgi_rpc.location.sha256`` check on the request
        pointer catches a wrong body.  The created GCS object is not
        automatically deleted.  Configure GCS Object Lifecycle Management on
        the bucket to expire objects after a suitable retention period.

        Args:
            size: Exact byte length the client will PUT.
            sha256_hex: SHA-256 of those bytes, 64 lowercase hex (validated;
                GCS cannot enforce it).

        Returns:
            An ``UploadUrl`` with PUT and GET signed URLs for the same GCS
            object and the signed headers the PUT must carry.

        Raises:
            ValueError: If *sha256_hex* is not 64 lowercase hex characters.

        """
        if not is_sha256_hex(sha256_hex):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        client = self._get_client()
        bucket = client.bucket(self.bucket)
        blob_name = f"{self.prefix}{uuid.uuid4().hex}.arrow"
        blob = bucket.blob(blob_name)
        expiration = timedelta(seconds=self.presign_expiry_seconds)
        expires_at = datetime.now(UTC) + expiration
        put_headers = {"x-goog-content-length-range": f"{size},{size}"}

        put_url: str = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="PUT",
            content_type="application/octet-stream",
            headers=put_headers,
        )
        get_url: str = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET",
        )

        _logger.debug(
            "GCS upload URL generated: bucket=%s key=%s size=%d",
            self.bucket,
            blob_name,
            size,
            extra={"bucket": self.bucket, "key": blob_name, "size_bytes": size},
        )
        return UploadUrl(
            upload_url=put_url,
            download_url=get_url,
            expires_at=expires_at,
            headers={"Content-Type": "application/octet-stream", **put_headers},
            object_id=blob_name,
        )

    def put_object(self, data: bytes) -> str:
        """Store *data* under a fresh name and return the name (``ObjectStore``).

        GCS cannot verify a SHA-256 at storage; the upload is length-exact and
        callers verify the digest on read.  Objects carry no TTL: set Object
        Lifecycle Management of at least one day on ``prefix``.

        Args:
            data: The bytes to store.

        Returns:
            The blob name (the ``object_id``).

        """
        client = self._get_client()
        blob_name = f"{self.prefix}{uuid.uuid4().hex}.bin"
        blob = client.bucket(self.bucket).blob(blob_name, chunk_size=_UPLOAD_CHUNK_BYTES)
        blob.upload_from_string(
            data,
            content_type="application/octet-stream",
            if_generation_match=0,
            timeout=self._upload_timeout(len(data)),
        )
        return blob_name

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch the object *object_id* (a blob name) names (``ObjectStore``).

        Args:
            object_id: The blob name.
            max_bytes: Largest object accepted; ``None`` means 256 MiB.

        Returns:
            The object bytes.

        Raises:
            ObjectMissingError: GCS reports the blob absent.
            ValueError: The object is larger than *max_bytes*.

        """
        limit = max_bytes if max_bytes is not None else DEFAULT_MAX_FETCH_BYTES
        blob = self._get_client().bucket(self.bucket).get_blob(object_id, timeout=self._read_timeout())
        if blob is None:
            raise ObjectMissingError("GCS object is gone")
        if blob.size is not None and blob.size > limit:
            raise ValueError(f"GCS object is {blob.size} bytes, over max_fetch_bytes ({limit})")
        data: bytes = blob.download_as_bytes(raw_download=True, timeout=self._read_timeout())
        if len(data) > limit:
            raise ValueError(f"GCS object exceeds max_fetch_bytes ({limit})")
        return data


# Runtime check that GCSStorage satisfies ExternalStorage and UploadUrlProvider protocols
def _check_protocol() -> None:
    """Verify GCSStorage satisfies ExternalStorage and UploadUrlProvider at import time."""
    _inst = GCSStorage(bucket="test")
    _storage: ExternalStorage = _inst
    _objects: ObjectStore = _inst
    _provider: UploadUrlProvider = _inst


_check_protocol()
