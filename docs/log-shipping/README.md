# Shipping vgi-rpc Access Logs to Cloud Storage

This directory contains reference configurations for shipping the `vgi_rpc.access` JSONL log to S3, GCS, and Azure Blob Storage using either [Vector](https://vector.dev) or [Fluent Bit](https://fluentbit.io). Both are open source, run identically on Azure / GCP / fly.io / bare metal, and pick up cloud credentials from the environment (IMDS, Workload Identity, Managed Identity) so secrets stay out of the configs.

vgi-rpc deliberately does **not** include an in-process uploader. Vector and Fluent Bit already solve rotation following, batching, retries, backpressure, and multi-cloud auth — there is no need to reimplement them inside the framework.

## Recommended setup

1. Run one access-log file **per process**, using path placeholders so multiple workers in one container do not collide:

   ```bash
   vgi-rpc-test ... \
     --access-log /var/log/vgi-rpc/access-{pid}-{server_id}.jsonl \
     --access-log-max-bytes 67108864 \
     --access-log-backup-count 5
   ```

   The Python reference implementation substitutes `{pid}` → `os.getpid()` and `{server_id}` → the server's stable hex identifier at startup.

2. Run Vector or Fluent Bit as a sidecar (or DaemonSet on Kubernetes) reading `/var/log/vgi-rpc/access-*.jsonl*` and shipping to your bucket of choice. The configs in this directory are starting points — adjust the bucket name, region, and key prefix for your deployment.

3. Cloud auth: do **not** put static credentials in the config. Vector and Fluent Bit pick up:
   - **AWS**: instance profile (EC2), task role (ECS), pod identity (EKS), or `AWS_*` env vars.
   - **GCP**: workload identity, GKE node service account, or `GOOGLE_APPLICATION_CREDENTIALS`.
   - **Azure**: managed identity, or `AZURE_*` env vars.

4. Trace correlation: populate the optional `request_id` field in your access-log records (the framework already supports it on HTTP transport). Vector and Fluent Bit forward it as a structured field, which makes Athena / BigQuery / Loki queries straightforward.

## Files in this directory

| File | Shipper | Destination |
|---|---|---|
| [`vector-s3.toml`](vector-s3.toml) | Vector | AWS S3 |
| [`vector-gcs.toml`](vector-gcs.toml) | Vector | Google Cloud Storage |
| [`vector-azure.toml`](vector-azure.toml) | Vector | Azure Blob Storage |
| [`fluent-bit-s3.conf`](fluent-bit-s3.conf) | Fluent Bit | AWS S3 |
| [`fluent-bit-gcs.conf`](fluent-bit-gcs.conf) | Fluent Bit | GCS (via Stackdriver Logging or `gcs` plugin) |
| [`fluent-bit-azure.conf`](fluent-bit-azure.conf) | Fluent Bit | Azure Blob Storage |

## Validating downstream

The access-log JSON Schema at [`vgi_rpc/access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json) is authoritative. Use it to validate records after they land in your bucket — for example, with `jsonschema-cli` or DuckDB's `read_json_auto` plus a CHECK constraint.
