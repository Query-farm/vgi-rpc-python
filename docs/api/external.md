# External Storage

When RPC payloads grow large — multi-megabyte query results, bulk data imports, large model artifacts — they can exceed the capacity of the underlying transport. HTTP servers and reverse proxies typically enforce request and response body size limits (e.g., 10 MB). Even pipe-based transports become inefficient when serializing very large batches inline.

External storage solves this by transparently offloading oversized Arrow IPC batches to cloud storage ([S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage), or any compatible backend). The batch is replaced with a lightweight **pointer batch** — a zero-row batch carrying a download URL in its metadata. The receiving side resolves the pointer automatically, fetching the actual data in parallel chunks.

This works in **both directions**:

- **Large outputs** — the server externalizes result batches that exceed a size threshold
- **Large inputs** — the client uploads input data to a pre-signed URL and sends a pointer in the RPC request

From the caller's perspective, nothing changes. The proxy returns the same typed results; the externalization is invisible.

## How It Works

### Large Outputs (Server to Client)

When a server method returns a batch that exceeds `externalize_threshold_bytes`, the framework intercepts the response before writing it to the wire:

1. The batch is serialized to Arrow IPC format
2. Optionally compressed with [zstd](https://facebook.github.io/zstd/)
3. Uploaded to cloud storage via the configured [`ExternalStorage`](#vgi_rpc.external.ExternalStorage) backend
4. The storage backend returns a download URL (typically a pre-signed URL with an expiry)
5. A **pointer batch** replaces the original — a zero-row batch with `vgi_rpc.location` metadata containing the URL
6. The pointer batch is written to the wire instead of the full data

On the client side, pointer batches are detected and resolved transparently:

1. The client reads the pointer batch and extracts the URL from `vgi_rpc.location` metadata
2. A HEAD probe determines the content size and whether the server supports range requests
3. For large payloads (>64 MB by default), parallel range-request fetching splits the download into chunks with speculative hedging for slow chunks
4. For smaller payloads, a single GET request fetches the data
5. If the data was compressed, it is decompressed automatically
6. The Arrow IPC stream is parsed back into a `RecordBatch` and returned to the caller

This works for both unary results and streaming outputs. For streaming, all batches in a single output cycle (including any log batches) are serialized as one IPC stream, uploaded together, and resolved as a unit.

### Large Inputs (Client to Server)

For the reverse direction — when a client needs to send large data to the server — the framework provides **upload URLs**. This is particularly important for HTTP transport where request body size limits are common.

1. The client calls `request_upload_urls()` to get one or more pre-signed URL pairs from the server
2. Each [`UploadUrl`](#vgi_rpc.external.UploadUrl) contains an `upload_url` (for PUT) and a `download_url` (for GET) — these may be the same or different depending on the storage backend
3. The client serializes the large batch to Arrow IPC and uploads it directly to the `upload_url` via HTTP PUT, bypassing the RPC server entirely
4. The client sends the RPC request with a pointer batch referencing the `download_url`
5. The server resolves the pointer batch transparently, fetching the data from storage

This pattern lets clients send arbitrarily large payloads even when the HTTP server enforces strict request size limits — the large data goes directly to cloud storage, and only a small pointer crosses the RPC boundary.

The upload URL endpoint is enabled by passing `upload_url_provider` to [`make_wsgi_app()`](http.md). The server advertises this capability via the `VGI-Upload-URL-Support: true` response header, and clients can discover it with `http_capabilities()`.

## Server Configuration

```python
from vgi_rpc import Compression, ExternalLocationConfig, FetchConfig, RpcServer, S3Storage

storage = S3Storage(bucket="my-bucket", prefix="rpc-data/")
config = ExternalLocationConfig(
    storage=storage,
    externalize_threshold_bytes=1_048_576,  # 1 MiB — batches above this go to S3
    compression=Compression(),              # zstd level 3 by default
    fetch_config=FetchConfig(
        chunk_size_bytes=8 * 1024 * 1024,   # 8 MiB parallel chunks
        max_parallel_requests=8,
    ),
)

server = RpcServer(MyService, MyServiceImpl(), external_location=config)
```

The same `ExternalLocationConfig` is passed to the client so it can resolve pointer batches in responses:

```python
from vgi_rpc import connect

with connect(MyService, ["python", "worker.py"], external_location=config) as proxy:
    result = proxy.large_query()  # pointer batches resolved transparently
```

For HTTP transport with upload URL support:

```python
from vgi_rpc import make_wsgi_app

app = make_wsgi_app(
    server,
    upload_url_provider=storage,       # enables __upload_url__ endpoint
    max_upload_bytes=100 * 1024 * 1024,  # advertise 100 MiB max upload
)
```

## Client Upload URLs

```python
from vgi_rpc import UploadUrl, request_upload_urls

# Get pre-signed upload URLs from the server
urls: list[UploadUrl] = request_upload_urls(
    base_url="http://localhost:8080",
    count=1,
)

# Upload large data directly to storage (bypasses RPC server)
import httpx
ipc_data = serialize_large_batch(my_batch)
httpx.put(urls[0].upload_url, content=ipc_data)

# Send RPC request with pointer to uploaded data
# The server resolves the pointer transparently
```

Clients can discover whether the server supports upload URLs:

```python
from vgi_rpc import http_capabilities

caps = http_capabilities("http://localhost:8080")
if caps.upload_url_support:
    urls = request_upload_urls("http://localhost:8080", count=5)
```

## Compression

When compression is enabled, batches are compressed with [zstd](https://facebook.github.io/zstd/) before upload and decompressed transparently on fetch:

```python
from vgi_rpc import Compression

# Default: zstd level 3
config = ExternalLocationConfig(
    storage=storage,
    compression=Compression(),  # algorithm="zstd", level=3
)

# Custom compression level (higher = smaller but slower)
config = ExternalLocationConfig(
    storage=storage,
    compression=Compression(level=9),
)

# Disable compression
config = ExternalLocationConfig(
    storage=storage,
    compression=None,
)
```

The storage backend stores the `Content-Encoding` header alongside the object. When the client fetches the data, the `Content-Encoding: zstd` header triggers automatic decompression.

Requires `pip install vgi-rpc[external]` (installs `zstandard`).

## Parallel Fetching

For large externalized batches, the client fetches data in parallel chunks using range requests. This significantly reduces download time for multi-megabyte payloads:

- A HEAD probe determines the total size and whether the server supports `Accept-Ranges: bytes`
- If the payload exceeds `parallel_threshold_bytes` (default 64 MB) and ranges are supported, it is split into `chunk_size_bytes` chunks (default 8 MB) fetched concurrently
- **Speculative hedging**: if a chunk takes longer than `speculative_retry_multiplier` times the median chunk time, a hedge request is launched in parallel — the first response wins
- For smaller payloads or servers without range support, a single GET request is used

```python
from vgi_rpc import FetchConfig

fetch_config = FetchConfig(
    parallel_threshold_bytes=64 * 1024 * 1024,  # 64 MiB
    chunk_size_bytes=8 * 1024 * 1024,            # 8 MiB chunks
    max_parallel_requests=8,                      # concurrent fetches
    timeout_seconds=60.0,                         # overall deadline
    max_fetch_bytes=256 * 1024 * 1024,            # 256 MiB hard cap
    speculative_retry_multiplier=2.0,             # hedge at 2x median
    max_speculative_hedges=4,                     # max hedge requests
)
```

## Object Lifecycle

Uploaded objects persist indefinitely — vgi-rpc does not delete them. Configure storage-level cleanup policies to auto-expire old data.

**S3 lifecycle rule:**

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket MY_BUCKET \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "expire-vgi-rpc",
      "Filter": {"Prefix": "rpc-data/"},
      "Status": "Enabled",
      "Expiration": {"Days": 1}
    }]
  }'
```

**GCS lifecycle rule:**

```bash
gsutil lifecycle set <(cat <<EOF
{"rule": [{"action": {"type": "Delete"},
           "condition": {"age": 1, "matchesPrefix": ["rpc-data/"]}}]}
EOF
) gs://MY_BUCKET
```

## URL Validation

By default, external location URLs are validated with `https_only_validator`, which rejects non-HTTPS URLs. This prevents pointer batches from being used to probe internal networks. You can provide a custom validator via `ExternalLocationConfig.url_validator`.

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
