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

1. The client serializes the request to Arrow IPC, hashes it, and calls `request_upload_urls()` with one `(size, sha256)` entry per body
2. Each [`UploadUrl`](#vgi_rpc.external.UploadUrl) contains an `upload_url` (for PUT) signed for exactly that size and digest, a `download_url` (for GET), and the `headers` the PUT must carry
3. The client PUTs exactly those bytes to the `upload_url` with those headers, bypassing the RPC server entirely; storage refuses any other body where the backend can verify it (S3, R2, the blob vendor)
4. The client sends the RPC request with a pointer batch referencing the `download_url` and carrying `vgi_rpc.location.sha256`
5. The server verifies the digest while resolving the pointer, and refuses a request pointer without one

This pattern lets clients send arbitrarily large payloads even when the HTTP server enforces strict request size limits — the large data goes directly to cloud storage, and only a small pointer crosses the RPC boundary.

The upload URL endpoint is enabled by passing `upload_url_provider` to [`make_wsgi_app()`](http.md). The server advertises this capability via the `VGI-Upload-URL-Support: 2` response header (upload-URL protocol v2; the retired `true` was the unsized `count` protocol, which a v2 client refuses), and clients can discover it with `http_capabilities()` (`upload_url_version == 2`). The HTTP client does all of this automatically when a request exceeds the server's `max_request_bytes`.

## Server Configuration

`S3Storage` requires `pip install vgi-rpc[s3]` (and `GCSStorage` requires `[gcs]`); the symbols are only exported from `vgi_rpc` when their backend dependency is installed.

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
    max_upload_bytes=100 * 1024 * 1024,  # refuse (400) uploads over 100 MiB
)
```

`max_upload_bytes` defaults to the server's inbound fetch cap
(`ServerExternalConfig.fetch_config.max_fetch_bytes`, 256 MiB by default) and
may not exceed it — an upload the server would refuse to fetch would fail only
after the client sent it. To accept larger uploads, raise `max_fetch_bytes` on
the server's external config as well.

## Client Upload URLs

```python
import hashlib

import httpx2

from vgi_rpc import UploadUrl, request_upload_urls
from vgi_rpc.http import upload_put_headers

ipc_data = serialize_large_batch(my_batch)
digest = hashlib.sha256(ipc_data).hexdigest()

# One pre-signed URL pair, signed for exactly these bytes
urls: list[UploadUrl] = request_upload_urls(
    base_url="http://localhost:8080",
    uploads=[(len(ipc_data), digest)],
)

# Upload directly to storage (bypasses the RPC server), with the vended headers
httpx2.put(urls[0].upload_url, content=ipc_data, headers=upload_put_headers(urls[0]))

# Send the RPC request with a pointer naming urls[0].download_url and
# carrying vgi_rpc.location.sha256 = digest; the server verifies it
```

Clients can discover whether the server supports upload URLs:

```python
from vgi_rpc import http_capabilities

caps = http_capabilities("http://localhost:8080")
if caps.upload_url_version == 2:
    urls = request_upload_urls("http://localhost:8080", uploads=[(len(body), digest)])
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

# gzip — stdlib zlib, the right choice when consumers can't do zstd
config = ExternalLocationConfig(
    storage=storage,
    compression=Compression(algorithm="gzip"),  # level defaults to 6
)

# Disable compression
config = ExternalLocationConfig(
    storage=storage,
    compression=None,
)
```

`Compression(algorithm=...)` accepts `"zstd"` (default; needs `zstandard` on both writer and reader) or `"gzip"` (stdlib `zlib`, no extra dependency). The storage backend stores the `Content-Encoding` header alongside the object; on fetch the `Content-Encoding: zstd` / `gzip` header triggers automatic decompression.

zstd requires `pip install vgi-rpc[external]` (installs `zstandard`); gzip works with no extra dependency.

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

## Pre-published References

Per-call externalization re-serializes, re-compresses and re-uploads a result on
every call. When a large result rarely changes — a worker's whole catalog, say —
publish it **once** with [`publish_external()`](#vgi_rpc.external.publish_external),
keep the returned [`ExternalRef`](#vgi_rpc.external.ExternalRef), and return that
ref from the unary method on later calls. The server writes the pointer batch
directly: no serialization, compression or upload happens during the call.

```python
import threading
from typing import Protocol

import pyarrow as pa

from vgi_rpc import Compression, ExternalRef, ExternalStorage, publish_external, rpc_methods


class CatalogService(Protocol):
    def catalog(self) -> str: ...


class CatalogServiceImpl:
    def __init__(self, storage: ExternalStorage, compression: Compression | None) -> None:
        self._storage, self._compression = storage, compression
        self._ref: ExternalRef | None = None
        self._lock = threading.Lock()

    def catalog(self) -> str | ExternalRef:
        with self._lock:
            if self._ref is None:  # publish once per process / catalog version
                schema = rpc_methods(CatalogService)["catalog"].result_schema
                batch = pa.RecordBatch.from_pydict({"result": [build_catalog()]}, schema=schema)
                self._ref = publish_external(batch, self._storage, self._compression)
            return self._ref
```

A unary method may return an `ExternalRef` instead of its declared value. The
return annotation may say so (`-> str | ExternalRef`); `ExternalRef` is stripped
when the result schema is derived, so the wire contract (and protocol hash) is
that of `str`. Returning a ref:

- always answers with a pointer — whether or not the server has external storage
  configured, and regardless of `externalize_threshold_bytes` (a ref is never
  inlined, nor routed through shared memory);
- skips building and validating the result value;
- is not counted toward `max_externalized_response_bytes` (nothing is uploaded
  during the call); the tiny pointer still goes through the wire-body budget;
- works on every transport (pipe, subprocess, Unix, TCP, HTTP). Stream methods
  are not supported.

`publish_external(batch, storage, compression=None, *, include_sha256=True)`
serializes the 1-row result batch exactly as the per-call externalizer would,
hashes the raw IPC bytes, compresses if asked, and calls `storage.upload()` once.
With `include_sha256=False` the ref carries no digest, so clients skip the
content check — for an object rewritten in place, or one too large to hash.
Clients need no change: a ref's pointer is indistinguishable from any other.

The caller owns the ref and the object's lifecycle:

- A long-lived ref must **not** point at an object under the short-TTL lifecycle
  rule used for per-call uploads (see [Object Lifecycle](#object-lifecycle)) —
  publish under a different prefix.
- A pre-signed URL expires; re-sign or rebuild the ref before it does.
- Only return a ref to callers who are all entitled to the same content.

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

## Blob Vendor

The *blob vendor* is a small HTTP service that holds the object-storage
credentials so workers never do. A worker asks it for a pre-signed URL for one
object of a declared size and SHA-256, uploads straight to storage, and hands on
a download URL. The storage (Cloudflare R2) enforces the signed
`Content-Length` and `x-amz-checksum-sha256`, so whatever sits at the object's
key is exactly the declared bytes. Buckets are per region (EU / US
jurisdictions, or a location hint), chosen by the vendor from the caller's key,
never from the request.

`VendorStorage` implements both `ExternalStorage` (server-side externalization)
and `UploadUrlProvider` (client upload URLs):

```python
from vgi_rpc.vendor import VendorStorage, external_config_from_env

storage = VendorStorage("https://blobs.example.com", token)

# Or, from the environment (returns None when VGI_BLOB_VENDOR_URL is unset):
config = external_config_from_env()
server = RpcServer(MyService, MyServiceImpl(), external_location=config)
app = make_wsgi_app(
    server,
    max_request_bytes=8 * 1024 * 1024,
    upload_url_provider=config.storage if config else None,
)
```

`serve_http()` takes the same `upload_url_provider` / `max_upload_bytes`
arguments, and `run_server()` applies the environment automatically under
`--http`.

For raw bytes that the caller references itself (e.g. large values in a
key-value store), `VendorBlobClient` stores and verifies blobs by `BlobRef`:

```python
from vgi_rpc.vendor import BlobScope, VendorBlobClient

with VendorBlobClient.from_env() as blobs:
    # Scope fields: 1-128 chars of [A-Za-z0-9._:-], so hex-encode binary ids.
    ref = blobs.put_bytes(value, scope=BlobScope(shard="s1", execution_id=exec_id_hex), idempotency_key="attempt-7:0")
    assert blobs.get_bytes(ref) == value  # size + SHA-256 checked against ref
    blobs.delete_scope("s1", exec_id_hex)
```

### Environment variables

| Variable | Meaning |
|---|---|
| `VGI_BLOB_VENDOR_URL` | Vendor base URL. Unset: the `*_from_env` configs return `None`. |
| `VGI_BLOB_VENDOR_TOKEN` | Bearer token; falls back to `VGI_CF_DO_TOKEN`. |
| `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` | Result size above which the server externalizes (default 1 MiB). |
| `VGI_RPC_EXTERNAL_COMPRESSION` | `none` (default), `zstd` or `gzip`, optionally `:<level>`. |
| `VGI_RPC_MAX_UPLOAD_BYTES` | Largest client upload, advertised as `VGI-Max-Upload-Bytes`. Unset: the inbound fetch cap below, so a server that vends upload URLs always advertises a limit. |
| `VGI_RPC_MAX_FETCH_BYTES` | Largest request the server fetches from an inbound pointer (`max_fetch_bytes`). Default 256 MiB, raised to `VGI_RPC_MAX_UPLOAD_BYTES` when that is larger; setting both with the upload cap above the fetch cap is a startup error. |
| `VGI_RPC_MAX_REQUEST_BYTES` | Request-body cap for `run_server --http` (`--max-request-bytes`); clients send larger requests through vendor upload URLs. Without a cap, clients never upload. |

A server never advertises an upload size it would then refuse to fetch:
`make_wsgi_app` / `serve_http` default `max_upload_bytes` to the server's
inbound fetch cap (`external_location.fetch_config.max_fetch_bytes`) and raise
`ValueError` for a larger value. A client over the limit is refused at
`__upload_url__/init`, before it uploads anything. Fetched requests are held
in memory, so size the worker for the cap you set.

### Host pinning

Configs built from the environment do not use `https_only_validator`, which
accepts any host. They pin pointer and upload URLs to the vendor's storage
hosts and URL prefixes (the region's bucket), read from its `GET /v1/info` and
cached for ten minutes; a failed
refresh keeps the last good list, and with no list at all every URL is
rejected. `VendorStorage` also checks the URLs the vendor itself returns before
using them. Build a pin by hand with `pinned_hosts_validator`:

```python
from vgi_rpc.vendor import pinned_hosts_validator

validate = pinned_hosts_validator(["acct.eu.r2.cloudflarestorage.com"])
```

An entry matches only that exact host (no subdomains) on the default `https`
port, or the port it names. Only `https` passes; plain `http` is accepted only
when the vendor URL itself is a loopback `http` URL (a local test vendor).

### Retries and secrets

Vendor and signed-URL requests retry transport errors, 429 and 5xx with
exponential backoff (3 attempts by default); 4xx responses are not retried.
Failures raise `VendorError` with the HTTP `status` and the vendor's `code`
(`not_found`, `not_uploaded`, `quota_exceeded`, `object_too_large`,
`idempotency_conflict`, ...) or the storage's (`BadDigest`). Pass
`idempotency_key` to `put_bytes` so a retried logical write reuses one object.
The bearer token goes only to the vendor, never to a signed URL. Every request
to a storage host (the vendor clients' PUTs, and the HTTP client's upload of a
large request) goes on a bare, single-purpose client: never the caller's
`httpx2.Client`, so its default headers, cookies, auth flow and event hooks
cannot reach the storage host. A `session=` passed to the vendor clients
supplies only the transport for vendor API calls.

vgi-rpc's own logs and errors show URLs through `redact_url` (scheme, host, port
and path only). httpx2 logs every request with its full URL at `INFO` on the
global `httpx2` logger, and for a signed URL the query string is the
credential, so the vendor clients and the upload path install a filter on that
logger (`vgi_rpc.external_fetch.redact_httpx_logging()`, idempotent) that
redacts the URLs in those lines. The lines are still logged at the same level;
call the helper yourself to cover other httpx2 use in the process.

### Testing

`vgi_rpc.conformance.fake_vendor` is an in-process fake of the vendor API and
of R2's signed-URL enforcement (declared size and checksum, method-bound
signatures, range GETs), with fault injection:

```python
from vgi_rpc.conformance.fake_vendor import serve_fake_vendor

with serve_fake_vendor({"tok": "tenant-a"}) as fake:
    storage = VendorStorage(fake.url, "tok")
    fake.vendor.fail_next("/v1/blobs/put", status=503)
```

The same fake runs standalone for test lanes in any language. It prints
`PORT:<n>` and then `URL:<base>`, and needs only vgi-rpc's base dependencies:

```bash
uvx --from vgi-rpc vgi-rpc-fake-vendor --test-routes --token test-token=tenant-a
```

`--test-routes` adds unauthenticated routes. Each answers `GET` with a JSON
body padded to 1,024 bytes, and a `HEAD` has no side effects:

- `GET /__stats__`: call and byte counters (`puts`, `gets`, `deletes`,
  `delete_scopes`, `upload_urls`, `storage_puts`, `storage_gets`, `objects`,
  `bytes`, `bytes_uploaded`, `bytes_downloaded`), signed-PUT refusals
  (`upload_refusals`, `upload_bad_digest`, `upload_incomplete_body`,
  `upload_signature_mismatch`, `uploads_corrupted`), and counters by purpose:
  `puts_rpc`, `puts_fs` and `puts_upload` split the `/v1/blobs/put` grants
  issued into server rpc writes (results), function storage, and grants
  minted for a client upload; `client_uploads`, `client_upload_bytes` and
  `client_upload_refusals` count the signed PUTs to client-upload objects.
  A server marks a client-upload grant with the advisory
  `origin: "client_upload"` field, which `VendorStorage.generate_upload_url`
  sends and the real vendor ignores.
- `GET /__admin__/fault?purpose=fs|rpc&action=corrupt|missing&count=N`: the
  next `N` objects of that purpose that are downloaded are damaged for every
  later `GET`. `corrupt` keeps the length and changes every byte; `missing`
  answers 404.
- `GET /__admin__/fault?purpose=upload&action=corrupt&count=N`: the next `N`
  signed PUTs of client uploads are damaged in transit, so the store refuses
  them with `400 BadDigest`. Server writes are never touched.
- `GET /__admin__/reset`: clears all faults.

The standalone fake logs to stderr which stop signal it received, the sending
pid and its command line (Linux), and its final counters.

In-process, the same faults are available as
`fake.vendor.set_download_fault(...)` and `reset_faults()`, and the counters as
`test_stats()`.

## URL Validation

By default, external location URLs are validated with `https_only_validator`, which rejects non-HTTPS URLs. This prevents pointer batches from being used to probe internal networks. You can provide a custom validator via `ExternalLocationConfig.url_validator`; for the blob vendor use the host-pinned validators described in [Host pinning](#host-pinning).

Client uploads to a vended URL are sent with `Content-Type: application/octet-stream`, the type pre-signed URLs that sign the content type (such as `GCSStorage.generate_upload_url`) expect.

## API Reference

### Configuration

::: vgi_rpc.external.ExternalLocationConfig

::: vgi_rpc.external.Compression

::: vgi_rpc.external_fetch.FetchConfig

### Pre-published References

::: vgi_rpc.external.ExternalRef

::: vgi_rpc.external.publish_external

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

### Blob Vendor

Requires `pip install vgi-rpc[http,external]`.

::: vgi_rpc.vendor.VendorStorage

::: vgi_rpc.vendor.VendorBlobClient

::: vgi_rpc.vendor.BlobRef

::: vgi_rpc.vendor.BlobScope

::: vgi_rpc.vendor.VendorError

::: vgi_rpc.vendor.pinned_hosts_validator

::: vgi_rpc.vendor.vendor_url_validator

::: vgi_rpc.vendor.external_config_from_env

::: vgi_rpc.vendor.client_external_config_from_env

::: vgi_rpc.vendor.max_upload_bytes_from_env

::: vgi_rpc.vendor.max_fetch_bytes_from_env
