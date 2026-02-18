# External Storage

When Arrow IPC batches exceed a configurable size threshold, they can be transparently uploaded to cloud storage (S3, GCS) and replaced with lightweight pointer batches. The client resolves pointers automatically via parallel range-request fetching.

## When to Use This

External storage is for services that transfer **large datasets** (multi-MB batches). If your batches are small (typical RPC parameters), you don't need this.

```python
from vgi_rpc import Compression, ExternalLocationConfig, FetchConfig, RpcServer, S3Storage

storage = S3Storage(bucket="my-bucket", prefix="rpc-data/")
config = ExternalLocationConfig(
    storage=storage,
    externalize_threshold_bytes=1_048_576,  # 1 MiB — batches above this go to S3
    compression=Compression(),              # optional zstd compression
    fetch_config=FetchConfig(
        chunk_size_bytes=8 * 1024 * 1024,   # 8 MiB parallel chunks
        max_parallel_requests=8,
    ),
)

server = RpcServer(MyService, MyServiceImpl(), external_location=config)
```

!!! note "Object lifecycle"
    Uploaded objects persist indefinitely. Configure S3 lifecycle policies or GCS object lifecycle management to auto-expire old data.

## API Reference

### Configuration

::: vgi_rpc.external.ExternalLocationConfig

::: vgi_rpc.external.Compression

::: vgi_rpc.external_fetch.FetchConfig

### Storage Protocol

::: vgi_rpc.external.ExternalStorage

::: vgi_rpc.external.UploadUrlProvider

::: vgi_rpc.external.UploadUrl

### Validation

::: vgi_rpc.external.https_only_validator

### S3 Backend

Requires `pip install vgi-rpc[s3]`.

::: vgi_rpc.s3.S3Storage

### GCS Backend

Requires `pip install vgi-rpc[gcs]`.

::: vgi_rpc.gcs.GCSStorage
