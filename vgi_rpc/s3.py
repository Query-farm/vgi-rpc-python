"""S3 storage backend for ExternalLocation batches.

Optional dependency: ``pip install vgi-rpc[s3]``

Provides ``S3Storage``, an ``ExternalStorage`` implementation that
uploads IPC data to S3 and returns pre-signed URLs for retrieval.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from vgi_rpc.external import ExternalStorage

__all__ = ["S3Storage"]


@dataclass
class _S3Pool:
    """Mutable connection-pool state owned by a ``S3Storage``."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    client: Any = None


@dataclass(frozen=True)
class S3Storage:
    """S3-backed ``ExternalStorage`` using boto3.

    Attributes:
        bucket: S3 bucket name.
        prefix: Key prefix for uploaded objects.
        presign_expiry_seconds: Lifetime of pre-signed GET URLs.
        region_name: AWS region (``None`` uses boto3 default).
        endpoint_url: Custom endpoint for S3-compatible services
            (e.g. MinIO, LocalStack).
        content_encoding: Optional HTTP ``Content-Encoding`` header
            to set on stored objects (e.g. ``"zstd"``).

    """

    bucket: str
    prefix: str = "vgi-rpc/"
    presign_expiry_seconds: int = 3600
    region_name: str | None = None
    endpoint_url: str | None = field(default=None, repr=False)
    content_encoding: str | None = None
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

    def upload(self, data: bytes, schema: pa.Schema) -> str:
        """Upload IPC data to S3 and return a pre-signed GET URL.

        Args:
            data: Serialized Arrow IPC stream bytes.
            schema: Schema of the data (unused but required by protocol).

        Returns:
            A pre-signed URL that can be used to download the data.

        """
        client = self._get_client()

        ext = ".arrow.zst" if self.content_encoding == "zstd" else ".arrow"
        key = f"{self.prefix}{uuid.uuid4().hex}{ext}"

        put_kwargs: dict[str, str | bytes] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": "application/octet-stream",
        }
        if self.content_encoding is not None:
            put_kwargs["ContentEncoding"] = self.content_encoding

        client.put_object(**put_kwargs)

        url: str = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.presign_expiry_seconds,
        )
        return url


# Runtime check that S3Storage satisfies ExternalStorage protocol
def _check_protocol() -> None:
    """Verify S3Storage satisfies ExternalStorage at import time."""
    _storage: ExternalStorage = S3Storage(bucket="test")


_check_protocol()
