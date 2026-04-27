# Access Log

VGI servers emit a structured access-log entry for every RPC request via the `vgi_rpc.access` Python logger. Each entry is a single JSON line containing transport-level metrics, authentication context, and (for unary calls and stream init) the serialized request batch.

!!! note "HTTP-only"
    Access logging is an HTTP-transport concern. Pipe, subprocess, shared-memory pipe, and Unix-socket transports do **not** emit access logs — those transports run trusted, co-located workers where per-call audit logging adds no value. The flags and helpers described below are only meaningful when serving over HTTP.

The wire format is pinned by the [Access Log Specification](access-log-spec.md) and the [`access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json) JSON Schema. This page is a practical operator's guide; the spec is authoritative.

## Enabling

Pass `--access-log` to any HTTP worker:

```bash
vgi-rpc-test --http --access-log /var/log/vgi-rpc/access.jsonl ...
```

Or set the env var `VGI_RPC_ACCESS_LOG`. Without one of these, the `vgi_rpc.access` logger is unconfigured and records are dropped.

## Path placeholders

The path may contain `{pid}` and `{server_id}` placeholders, expanded once at handler construction. Use them to give every worker process its own file — multiple HTTP workers writing to the same file is unsupported (concurrent rotation will race, and POSIX append atomicity is not guaranteed for large records).

```bash
--access-log /var/log/vgi-rpc/access-{pid}-{server_id}.jsonl
```

## Rotation

| Flag | Env | Default | Effect |
|---|---|---|---|
| `--access-log-max-bytes N` | `VGI_RPC_ACCESS_LOG_MAX_BYTES` | `0` (off) | Size-based rotation when > 0. Uses `RotatingFileHandler`. |
| `--access-log-when STR` | `VGI_RPC_ACCESS_LOG_WHEN` | unset | Time-based rotation (`H`, `D`, `midnight`, …). Mutually exclusive with `--access-log-max-bytes`. Uses `TimedRotatingFileHandler`. |
| `--access-log-backup-count N` | `VGI_RPC_ACCESS_LOG_BACKUP_COUNT` | `5` | Number of rotated files retained. |

Without a rotation flag, a plain `FileHandler` is used (the file grows forever — fine for short-lived workers, dangerous for long-lived ones).

Both rotation handlers use the standard rename-then-reopen pattern, which Vector and Fluent Bit follow correctly via their `tail`/`file` sources.

## Per-record size cap

Downstream log shippers impose per-line ceilings (Vector `file` source default 100 KiB, Fluent Bit `Buffer_Max_Size` default 256 KiB). Lines exceeding the ceiling are silently dropped, which is the worst possible failure mode for an audit log.

The Python emitter caps each record at 1 MiB by default — well above realistic record sizes but bounded so a runaway `request_data` blob can't produce a 50 MB log line that the shipper drops.

| Flag | Env | Default |
|---|---|---|
| `--access-log-max-record-bytes N` | `VGI_RPC_ACCESS_LOG_MAX_RECORD_BYTES` | `1048576` (1 MiB) |

When a record would exceed the cap, the formatter sheds fields in this order:

1. Drop `request_data`. Add `original_request_bytes` (character length of the dropped string) and set `truncated: true`.
2. Empty `claims`. Keep `truncated: true`.
3. Fall back to a sentinel form: keep all always-required envelope fields plus `error_message` (when `status == "error"`) and set `truncated: "record_too_large"`. Everything else is dropped.

`error_message` is **never** truncated — operators rely on the full server-side message for debugging.

When pairing with shippers, raise their per-line limits to match the emitter's cap. The reference configs in [`docs/log-shipping/`](https://github.com/Query-farm/vgi-rpc-python/tree/main/docs/log-shipping) set Vector's `max_line_bytes` and Fluent Bit's `Buffer_Max_Size` to 1 MiB.

## Multi-worker deployments

If you run N HTTP workers in one container or VM:

1. Use `{pid}` (and optionally `{server_id}`) in `--access-log` so each worker writes its own file.
2. Glob the directory in your shipper config: `/var/log/vgi-rpc/access-*.jsonl*`.
3. Keep rotation per-file, not shared.

Sharing one access-log file across processes is unsupported and the spec explicitly warns against it.

## Shipping to cloud storage

VGI deliberately does **not** include an in-process uploader to S3/GCS/Azure Blob. Vector and Fluent Bit already solve rotation following, batching, retries, backpressure, and multi-cloud auth — there's no reason to reimplement them inside the framework.

Ready-to-adapt shipper configs live under [`docs/log-shipping/`](https://github.com/Query-farm/vgi-rpc-python/tree/main/docs/log-shipping):

| File | Shipper | Destination |
|---|---|---|
| `vector-s3.toml` | Vector | AWS S3 |
| `vector-gcs.toml` | Vector | Google Cloud Storage |
| `vector-azure.toml` | Vector | Azure Blob Storage |
| `fluent-bit-s3.conf` | Fluent Bit | AWS S3 |
| `fluent-bit-gcs.conf` | Fluent Bit | GCS via Cloud Logging sink |
| `fluent-bit-azure.conf` | Fluent Bit | Azure Blob Storage |

Cloud authentication is picked up from the environment (instance profile, workload identity, managed identity) — the configs do not embed static credentials.

## Decoding `request_data`

The `request_data` field on unary records and stream `init` records contains base64-encoded Arrow IPC stream bytes. Each column in the batch is a method parameter:

```python
import base64
import pyarrow as pa

batch_bytes = base64.b64decode(record["request_data"])
reader = pa.ipc.open_stream(batch_bytes)
batch = reader.read_all().to_batches()[0]
print(batch.to_pydict())
```

A truncated record (`truncated: true`) will have `request_data` absent and `original_request_bytes` set to the original character length of the dropped field.

If your Protocol carries sensitive parameters (passwords, API keys, PII), strip them in a downstream pipeline step before ingestion into a log aggregation system — the framework does not redact `request_data`.

## Correlating stream requests

A single client query may produce multiple HTTP exchanges: one stream `init` plus zero or more `exchange`/`produce` continuations. Every record from the same logical stream carries the same `stream_id` (32-char UUID hex). Continuation records do not carry `request_data` — the call parameters were logged on `init`.

```sql
-- Reconstruct one stream's full execution
SELECT * FROM access_log
WHERE stream_id = '85b7099a1e9b4a6b9d1fc917f27212ee'
ORDER BY timestamp;
```

For cross-call correlation (e.g. a downstream service called from inside a method), populate the optional `request_id` field — VGI sets it automatically on HTTP transport, and shippers forward it as a structured field.

## Programmatic access

The access log is a standard Python logger; attach any handler:

```python
import logging
from logging.handlers import HTTPHandler

logging.getLogger("vgi_rpc.access").addHandler(
    HTTPHandler("analytics.example.com", "/ingest", method="POST")
)
```

The recommended path for production deployments is still: file with rotation + Vector/Fluent Bit sidecar. Programmatic handlers are useful for tests, in-process aggregation, or integrating with a non-file destination already wired into your stack.

## Conformance validation

The cross-language conformance runner validates a worker's access log against the JSON Schema:

```bash
vgi-rpc-test --cmd "./my-go-worker" --access-log /tmp/go-worker.log
```

Exit code is `0` if every record passes, `1` if any record fails, `2` if the runner itself errored. See the [Access Log Specification](access-log-spec.md) for the full contract and [Conformance Testing](cross-language-conformance.md) for cross-language status.

## Example record

```json
{
  "timestamp": "2026-04-26T15:30:45.123Z",
  "level": "INFO",
  "logger": "vgi_rpc.access",
  "message": "ConformanceService.greet ok",
  "server_id": "a85cd92650b7",
  "protocol": "ConformanceService",
  "protocol_hash": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "method": "greet",
  "method_type": "unary",
  "principal": "alice@example.com",
  "auth_domain": "jwt",
  "authenticated": true,
  "remote_addr": "10.0.0.1:54321",
  "duration_ms": 1.23,
  "status": "ok",
  "error_type": "",
  "request_id": "f41f090e23b84789",
  "http_status": 200,
  "request_data": "QVJST1cxAAA..."
}
```

## Example truncated record

```json
{
  "timestamp": "2026-04-26T15:30:45.456Z",
  "level": "INFO",
  "logger": "vgi_rpc.access",
  "message": "ConformanceService.upload ok",
  "server_id": "a85cd92650b7",
  "protocol": "ConformanceService",
  "protocol_hash": "9f86d081...",
  "method": "upload",
  "method_type": "unary",
  "principal": "alice@example.com",
  "auth_domain": "jwt",
  "authenticated": true,
  "remote_addr": "10.0.0.1:54321",
  "duration_ms": 412.7,
  "status": "ok",
  "error_type": "",
  "truncated": true,
  "original_request_bytes": 8388608
}
```
