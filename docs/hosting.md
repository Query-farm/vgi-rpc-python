---
description: "Deploy vgi-rpc services on AWS Lambda, Google Cloud Run, Cloudflare Workers, Azure Functions, Fly.io, and Railway — with external storage for large payloads."
hide:
  - navigation
---

# Hosting & Deployment

vgi-rpc's [HTTP transport](api/http.md) produces a standard **WSGI application** — the same interface used by Flask, Django, and Falcon. Any platform that can run a WSGI app (or adapt one) can host a vgi-rpc service.

This page covers deployment patterns for popular cloud platforms, how to work around request size limits using external storage, and production configuration for multi-worker environments.

## The WSGI App

Every deployment starts the same way. Install the HTTP extra first:

```bash
pip install vgi-rpc[http]
```

```python
import os
from vgi_rpc import RpcServer
from vgi_rpc.http import make_wsgi_app

from my_service import MyProtocol, MyServiceImpl

server = RpcServer(MyProtocol, MyServiceImpl())

app = make_wsgi_app(
    server,
    signing_key=os.environ["VGI_SIGNING_KEY"].encode(),
)
```

`make_wsgi_app()` returns a [Falcon](https://falcon.readthedocs.io/) WSGI application that you can serve with [gunicorn](https://gunicorn.org/), [waitress](https://docs.pylonsproject.org/projects/waitress/), [uWSGI](https://uwsgi-docs.readthedocs.io/), or any WSGI-compatible runtime. The key configuration options for production:

| Parameter | Purpose | Default |
|-----------|---------|---------|
| `signing_key` | HMAC-SHA256 key for stream state tokens | Random per-process (breaks multi-worker!) |
| `prefix` | URL path prefix for RPC endpoints | `/vgi` |
| `max_request_bytes` | Advertised request size limit | None (unlimited) |
| `max_stream_response_bytes` | Split large producer stream responses | None (single response) |
| `max_upload_bytes` | Advertised upload size limit | None (unlimited) |
| `authenticate` | Auth callback `(Request) → AuthContext` | None (anonymous) |
| `cors_origins` | Enable CORS for browser clients | None (disabled) |
| `upload_url_provider` | Enable pre-signed upload URL vending | None (disabled) |
| `otel_config` | [OpenTelemetry](api/otel.md) instrumentation | None (disabled) |

!!! warning "Signing key in multi-worker deployments"
    Stream state tokens are signed with HMAC-SHA256. If each worker generates its own random key, a token signed by worker A is rejected by worker B. **Always provide a shared `signing_key`** from environment variables or a secrets manager.

    If using gunicorn with `--preload`, the app (and its random key) is shared across workers via fork. Without `--preload`, each worker creates its own app — you **must** provide an explicit key.

All platforms handle TLS termination at the load balancer or edge — your WSGI process runs plain HTTP internally. Pre-signed storage URLs are always HTTPS.

## Platform Limits

Cloud platforms impose request/response size limits that matter when you're sending [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) batches. Here's the landscape:

| Platform | Request Limit | Response Limit | Max Timeout |
|----------|--------------|----------------|-------------|
| [**AWS Lambda**](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html) (API Gateway) | 10 MB | 10 MB | 29 s |
| [**AWS Lambda**](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html) (Function URL) | 6 MB | 6 MB | 15 min |
| [**Google Cloud Run**](https://cloud.google.com/run/quotas) | 32 MB (HTTP/1), unlimited (HTTP/2) | 32 MB (HTTP/1), unlimited (streaming) | 60 min |
| [**Google Cloud Functions**](https://cloud.google.com/functions/quotas) (2nd gen) | 32 MB | 32 MB | 60 min |
| [**Cloudflare Workers**](https://developers.cloudflare.com/workers/platform/limits/) | 100–500 MB (plan-dependent) | No enforced limit | 5 min CPU |
| [**Azure Functions**](https://learn.microsoft.com/en-us/azure/azure-functions/functions-scale) | 210 MB | No explicit limit | 230 s (HTTP) |
| [**Fly.io**](https://fly.io/docs/) | No platform limit | No platform limit | 60 s idle |
| [**Railway**](https://docs.railway.com/) | No platform limit | No platform limit | 15 min |

For small payloads (< 5 MB), every platform works fine. For large Arrow batches — think geospatial data, ML feature vectors, or analytics results — you need **external storage**.

## External Storage: Working Around Size Limits

vgi-rpc has built-in support for [externalizing large batches](api/external.md) to object storage. When a batch exceeds a configurable threshold, the server uploads it to S3/GCS/R2 and sends the client a lightweight pointer batch containing a pre-signed download URL. The client fetches the data directly from storage — the HTTP service never carries the large payload.

```d2
shape: sequence_diagram
Client -> Server: "RPC call (small params)"
Server -> Server: "compute large result"
Server -> S3/GCS/R2: "upload batch (pre-signed PUT)"
Server -> Client: "pointer batch (download URL)"
Client -> S3/GCS/R2: "fetch data (pre-signed GET)"
```

This pattern means your vgi-rpc service can return **gigabyte-scale results** through a Lambda function with a 6 MB limit.

### Configuring External Storage

=== "Amazon S3"

    ```python
    from vgi_rpc import RpcServer, S3Storage, ExternalLocationConfig, Compression

    storage = S3Storage(
        bucket="my-vgi-rpc-data",
        prefix="results/",
        presign_expiry_seconds=3600,  # 1 hour
    )

    server = RpcServer(
        MyProtocol,
        MyServiceImpl(),
        external_location=ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=1_000_000,  # 1 MB
            compression=Compression(level=3),       # zstd compression
        ),
    )
    ```

    Install: `pip install vgi-rpc[s3,external]`

=== "Google Cloud Storage"

    ```python
    from vgi_rpc import RpcServer, GCSStorage, ExternalLocationConfig, Compression

    storage = GCSStorage(
        bucket="my-vgi-rpc-data",
        prefix="results/",
        presign_expiry_seconds=3600,
        project="my-gcp-project",  # optional if using ADC
    )

    server = RpcServer(
        MyProtocol,
        MyServiceImpl(),
        external_location=ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=1_000_000,
            compression=Compression(level=3),
        ),
    )
    ```

    Install: `pip install vgi-rpc[gcs,external]`

=== "Cloudflare R2"

    [R2](https://developers.cloudflare.com/r2/) is S3-compatible — use `S3Storage` with a custom endpoint:

    ```python
    from vgi_rpc import S3Storage

    storage = S3Storage(
        bucket="my-vgi-rpc-data",
        prefix="results/",
        endpoint_url="https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
        presign_expiry_seconds=3600,
    )
    ```

    Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your R2 API token credentials. Install: `pip install vgi-rpc[s3,external]`

### Object Lifecycle

vgi-rpc does **not** manage object cleanup — uploaded objects persist until you delete them. Configure storage-level lifecycle rules:

=== "S3 / R2"

    ```bash
    aws s3api put-bucket-lifecycle-configuration \
      --bucket my-vgi-rpc-data \
      --lifecycle-configuration '{
        "Rules": [{
          "ID": "expire-vgi-rpc",
          "Filter": {"Prefix": "results/"},
          "Status": "Enabled",
          "Expiration": {"Days": 1}
        }]
      }'
    ```

=== "GCS"

    ```bash
    gsutil lifecycle set - gs://my-vgi-rpc-data <<'EOF'
    {"rule": [{"action": {"type": "Delete"},
               "condition": {"age": 1, "matchesPrefix": ["results/"]}}]}
    EOF
    ```

### Client-Side Upload (Large Inputs)

For large **inputs** (client → server), enable upload URL vending so clients can upload directly to storage:

```python
app = make_wsgi_app(
    server,
    signing_key=signing_key,
    upload_url_provider=storage,      # S3Storage/GCSStorage implement this
    max_upload_bytes=500_000_000,     # 500 MB
)
```

The client workflow:

1. Call `http_capabilities()` — discovers `max_request_bytes` and upload URL support
2. If payload exceeds the limit, call `request_upload_urls()` — gets pre-signed PUT/GET URL pairs
3. Upload data directly to storage via the PUT URL
4. Send a pointer batch (with the GET URL) to the server
5. Server resolves the pointer transparently

## Platform Guides

### Google Cloud Run

[Cloud Run](https://cloud.google.com/run) is the easiest path to production. It runs containers, supports HTTP/2, has generous limits (32 MB+ payloads, 60-minute timeouts), and scales to zero.

```python title="app.py"
# pip install vgi-rpc[http,gcs,external]
import os
from vgi_rpc import RpcServer, GCSStorage, ExternalLocationConfig
from vgi_rpc.http import make_wsgi_app
from my_service import MyProtocol, MyServiceImpl

storage = GCSStorage(
    bucket=os.environ["GCS_BUCKET"],
    prefix="vgi-rpc/",
)

server = RpcServer(
    MyProtocol,
    MyServiceImpl(),
    external_location=ExternalLocationConfig(storage=storage),
    enable_describe=True,
)

app = make_wsgi_app(
    server,
    signing_key=os.environ["VGI_SIGNING_KEY"].encode(),
)
```

```dockerfile title="Dockerfile"
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "-w", "4", "--timeout", "600"]
```

```bash title="Deploy"
# Store signing key in Secret Manager (recommended over env vars)
echo -n "$(openssl rand -hex 32)" | \
  gcloud secrets create vgi-signing-key --data-file=-

gcloud run deploy my-vgi-service \
  --source . \
  --set-env-vars "GCS_BUCKET=my-bucket" \
  --set-secrets "VGI_SIGNING_KEY=vgi-signing-key:latest" \
  --allow-unauthenticated \
  --timeout 600 \
  --memory 1Gi
```

**Why Cloud Run works well:**

- GCS is the natural storage backend — same network, low latency, no egress cost
- IAM-based auth via Application Default Credentials (no key management)
- HTTP/2 support removes payload size limits for streaming workloads
- Scales to zero when idle — you pay only for requests

### AWS Lambda

[Lambda's](https://aws.amazon.com/lambda/) 6 MB payload limit makes external storage essential for non-trivial workloads. The pattern: Lambda handles the RPC logic, S3 handles the data.

```python title="handler.py"
# pip install vgi-rpc[http,s3,external] apig-wsgi
import os
from vgi_rpc import RpcServer, S3Storage, ExternalLocationConfig, Compression
from vgi_rpc.http import make_wsgi_app
from apig_wsgi import make_lambda_handler
from my_service import MyProtocol, MyServiceImpl

storage = S3Storage(
    bucket=os.environ["S3_BUCKET"],
    prefix="vgi-rpc/",
)

server = RpcServer(
    MyProtocol,
    MyServiceImpl(),
    external_location=ExternalLocationConfig(
        storage=storage,
        externalize_threshold_bytes=512_000,  # 512 KB — stay well under 6 MB
        compression=Compression(level=3),
    ),
)

app = make_wsgi_app(
    server,
    signing_key=os.environ["VGI_SIGNING_KEY"].encode(),
    max_request_bytes=5_000_000,  # 5 MB (leave room for headers)
    upload_url_provider=storage,
)

# apig-wsgi adapts the WSGI app for API Gateway / Function URL
handler = make_lambda_handler(app)
```

!!! tip "Cold starts"
    PyArrow and boto3 are heavy imports. Lambda cold starts may reach 5–10 seconds. Use **provisioned concurrency** for latency-sensitive workloads, and package pyarrow in a Lambda layer to share it across functions.

**Lambda tips:**

- Set `externalize_threshold_bytes` well below the payload limit (512 KB is a good starting point) to leave headroom for log batches and metadata
- Use zstd compression — it reduces S3 storage and fetch time
- Store the signing key in AWS Secrets Manager and cache it in the Lambda init phase
- For producer streams, enable `max_stream_response_bytes` to split large streaming responses across multiple exchanges

### Cloudflare Workers

[Cloudflare Workers](https://developers.cloudflare.com/workers/) run on V8 isolates and don't natively support Python WSGI. The recommended pattern is to use Workers as an **edge proxy** in front of a Cloud Run or Lambda backend:

```d2
Client -> Cloudflare Worker: "request"
Cloudflare Worker -> Cloud Run (vgi-rpc): "forward"
Cloud Run (vgi-rpc) -> Cloudflare Worker: "response"
Cloudflare Worker -> Client: "response"
Cloud Run (vgi-rpc) -> Cloudflare R2: "upload large data"
Client -> Cloudflare R2: "fetch large data"
Cloudflare Worker: {
  label: "Cloudflare Worker\n(auth, rate limiting)"
}
```

This gives you:

- **Edge auth and rate limiting** — validate JWTs or API keys at the edge before hitting your backend
- **R2 for external storage** — same Cloudflare network, no egress fees
- **Global routing** — Workers run in 300+ locations, reducing latency to the nearest edge
- **Request/response passthrough** — Workers' generous limits (100–500 MB) don't constrain your payloads

The vgi-rpc backend uses `S3Storage` pointed at R2 (S3-compatible), and the Worker forwards the `application/vnd.apache.arrow.stream` content type transparently.

### Azure Functions

[Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/) support up to 210 MB request bodies — often enough to skip external storage for moderate workloads:

```python title="function_app.py"
# pip install vgi-rpc[http]
import os
import azure.functions as func
from vgi_rpc import RpcServer
from vgi_rpc.http import make_wsgi_app
from my_service import MyProtocol, MyServiceImpl

server = RpcServer(MyProtocol, MyServiceImpl(), enable_describe=True)

wsgi_app = make_wsgi_app(
    server,
    signing_key=os.environ["VGI_SIGNING_KEY"].encode(),
    max_request_bytes=200_000_000,
)

# Azure Functions WSGI adapter — create once, reuse across invocations
main = func.WsgiMiddleware(wsgi_app).main
```

!!! note "HTTP trigger timeout"
    Azure's HTTP trigger has a **hard 230-second timeout** regardless of your function timeout setting. For long-running producer streams, consider Cloud Run or Railway instead.

### Fly.io and Railway

Both [Fly.io](https://fly.io/) and [Railway](https://railway.com/) run containers with no platform-imposed payload limits — ideal for workloads with large Arrow batches where external storage adds unnecessary complexity.

```dockerfile title="Dockerfile"
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "-w", "4", "--timeout", "300"]
```

```bash title="Deploy to Fly.io"
fly launch
fly secrets set VGI_SIGNING_KEY=$(openssl rand -hex 32)
```

```bash title="Deploy to Railway"
railway up
railway variables set VGI_SIGNING_KEY=$(openssl rand -hex 32)
```

These platforms are a good fit when:

- Payloads are large but you want to keep the architecture simple (no object storage)
- You need long-lived streaming connections (Fly.io has no wall-clock timeout)
- You want predictable per-second pricing instead of per-invocation

## Choosing a Platform

| Need | Best Fit | Why |
|------|----------|-----|
| Lowest latency, scale to zero | **Google Cloud Run** | HTTP/2, 60 min timeout, GCS integration |
| Serverless, AWS ecosystem | **AWS Lambda** + S3 | Pay-per-invocation, S3 external storage |
| Large payloads, simple setup | **Fly.io** or **Railway** | No payload limits, container-based |
| Edge auth + global routing | **Cloudflare Workers** + R2 | Proxy pattern with R2 storage |
| .NET/Azure ecosystem | **Azure Functions** | 210 MB request limit, Azure Blob Storage |
| Maximum payload flexibility | **Google Cloud Run** (HTTP/2) | No documented size cap |

## Production Checklist

Before going live, verify these settings:

- **Signing key** — shared across all workers, loaded from secrets manager
- **Authentication** — `authenticate` callback validates tokens/API keys ([Auth & Context](api/auth.md))
- **CORS** — `cors_origins` set if serving browser clients
- **External storage** — lifecycle rules configured (auto-delete after retention period)
- **Request limits** — `max_request_bytes` set to match platform limits
- **Compression** — zstd enabled for bandwidth-sensitive deployments (`pip install vgi-rpc[external]`)
- **Introspection** — `enable_describe=True` for [service discovery](api/introspection.md) (disable in production if sensitive)
- **Logging** — structured JSON logging with [`VgiJsonFormatter`](api/logging.md)
- **OpenTelemetry** — [`otel_config`](api/otel.md) for distributed tracing (`pip install vgi-rpc[otel]`)
- **Health check** — platform health probe configured (e.g., Cloud Run startup probe)
