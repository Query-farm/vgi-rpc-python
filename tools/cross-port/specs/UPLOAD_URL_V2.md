# Upload URLs v2 + vgi protocol 3.4 — contract

Decided by Rusty (the maintainer), 2026-11-20:
- `__upload_url__/init` drops `count` and takes an array of (size, sha256), one entry per upload. Clean break, no fallback.
- The break is signalled at both levels: the vgi-rpc capability header moves to `VGI-Upload-URL-Support: 2`, AND the vgi application protocol bumps 3.3.0 → 3.4.0 (exact major+minor match, so every peer must move).
- No releases. All work lives on a branch named `protocol-3.4` in every repo, verified on the EC2 box, merged in the release wave when Rusty says. Do NOT push protocol-3.4 work to `main` / `v2.0` (pushing vgi `v2.0` auto-publishes the extension; a 3.4 extension would break every released 3.3 worker).

## Why
Client-side uploads (the DuckDB extension, or any vgi-rpc client, PUTting a request body that exceeds the server's `VGI-Max-Request-Bytes`) used blank pre-signed PUT URLs: storage accepted any bytes. Server-side writes already use sized URLs that sign `content-length` + `x-amz-checksum-sha256`, so storage itself rejects a mismatched body. v2 gives client uploads the same guarantee. Every real caller passed `count=1` right before uploading, so nothing is lost.

## Request: `POST {prefix}/__upload_url__/init`
Standard unary request, `vgi_rpc.method = "__upload_url__"`, one parameter:

| Parameter | Arrow type | Description |
|---|---|---|
| `uploads` | `list<struct<size: int64 not null, sha256: utf8 not null>>` not null | One entry per upload, 1–100 entries. |

- `size`: exact byte length of the body the client will PUT; must be ≥ 1.
- `sha256`: SHA-256 of exactly those bytes, 64 lowercase hex characters (same encoding as `vgi_rpc.location.sha256`).

Server refusals (HTTP 400, RPC error batch, before any URL is minted):
- request has a `count` parameter or no `uploads` → message contains `retired count shape` and says the server requires `VGI-Upload-URL-Support: 2`;
- 0 or >100 entries; `size` < 1; `sha256` not 64 lowercase hex;
- any `size` > the server's `VGI-Max-Upload-Bytes` (when configured) → message contains `exceeds VGI-Max-Upload-Bytes`.

## Response
One row per request entry, same order:

| Column | Arrow type | Nullable | Description |
|---|---|---|---|
| `upload_url` | `utf8` | No | Pre-signed PUT, signed for that size and hash where the backend can. |
| `download_url` | `utf8` | No | Pre-signed GET for the server to fetch it. |
| `expires_at` | `timestamp("us", tz="UTC")` | No | Expiry of both URLs. |
| `upload_headers` | `map<utf8, utf8>` | No (may be empty) | Headers the client MUST send on the PUT, verbatim. |

- `upload_headers` never contains `Content-Length`; the client sends exactly `size` bytes with a plain Content-Length (no chunked transfer encoding), which the backend signature covers.
- S3/R2 (incl. the blob vendor): sign content-length and `x-amz-checksum-sha256` (base64 of the 32 raw digest bytes); return `{"x-amz-checksum-sha256": <b64>}` (plus any other signed headers). Storage rejects a mismatched body.
- GCS: cannot verify SHA-256 at storage; sign the length (e.g. `x-goog-content-length-range: N,N`) and any content-type it signs, return those headers. The pointer check below catches a bad body.
- Blob vendor: use the sized endpoint (`POST /v1/blobs/put`, `purpose="rpc"`, size + sha256_b64), which returns the sized PUT + its headers + a GET URL. The unsized `/v1/upload_urls` path is no longer used by the server.

## Client obligations
1. Only upload to a server advertising `VGI-Upload-URL-Support: 2`. If it advertises `true` (v1) the client fails with a message containing `upload protocol is too old` (server advertises v1, client needs 2). Never fall back to count.
2. Hash the body, request one URL with (size, sha256), PUT exactly those bytes with every `upload_headers` entry.
3. The pointer request it POSTs carries `vgi_rpc.location = download_url` AND `vgi_rpc.location.sha256 = <hex>`.

## Server obligations when resolving a request pointer
- `vgi_rpc.location.sha256` is REQUIRED on request pointers; a pointer without it is refused (`missing vgi_rpc.location.sha256`).
- Verify the downloaded bytes' SHA-256 (and the size where known) before decoding; mismatch → error containing `sha256 mismatch`.
- Result pointers the server emits keep carrying `vgi_rpc.location.sha256` (already mandatory).

## Capability header
`VGI-Upload-URL-Support: 2` (was `"true"`). `VGI-Max-Upload-Bytes` unchanged.

## Provider interface (every SDK)
`generate_upload_url(size, sha256_hex) -> UploadUrl{upload_url, download_url, expires_at, headers}` replaces the schema/count form, for every backend the SDK has (vendor, S3, GCS, test/fake providers).

## Fake vendor (`vgi-rpc-fake-vendor`, vgi-rpc-python)
Its sized PUT must verify like R2: reject when the body length ≠ the signed size or the body's SHA-256 ≠ `x-amz-checksum-sha256` (HTTP 400, S3-style `BadDigest` / `IncompleteBody` XML). Add a test-route fault that corrupts the next N client uploads in transit (e.g. `/__admin__/fault?purpose=upload&action=corrupt&count=N`, flips bytes before verification) so the shared suite can prove storage refuses a corrupted upload. `/__stats__` gains counters for upload refusals.

## vgi protocol 3.4.0
Bump the vgi application protocol to 3.4.0 everywhere (extension generated header, each vgi SDK's protocol version constant / generated code, conformance expectations, fixture metadata). Nothing else in the vgi protocol changes. 3.4 also makes `vgi_rpc.location.sha256` mandatory on result pointers in the extension (a pointer without it is refused).

## Tests (definition of done)
- vgi-rpc conformance (each SDK's conformance suite / Python `vgi_rpc/conformance`): v2 request round-trip; retired count shape refused; >100 entries / bad hash / size<1 refused; size over max refused; PUT with wrong bytes refused by the (fake) storage; request pointer without sha256 refused; header value is `2`.
- Shared sqllogictests in vgi (`test/sql/external_storage/`): an oversized request round-trips through upload; the corrupt-upload fault makes the query fail with a storage checksum error; upload counters prove the path ran.
- Each SDK's existing lanes + vendor lane pass on the box against a box-built 3.4 extension.

---

## Decisions made in the Python reference (vgi-rpc-python `protocol-3.4`)

The contract above is unchanged; these settle what it left open. The
published wire spec is `docs/WIRE_PROTOCOL.md` § *Upload URL generation*.

### Wire
1. **Response nullability.** All four response fields are declared
   non-nullable. `upload_headers` uses Arrow's default map layout
   (`map<utf8, utf8>`: `entries: struct<key: utf8 not null, value: utf8>`).
   The request `uploads` field is non-nullable with non-nullable `size` and
   `sha256` struct fields, list item field named `item`. Servers read entries by
   name and do not compare schemas; a null `size`/`sha256` is refused as invalid.
2. **Refusals.** HTTP 400, RPC error batch, `error_type` `ValueError`. Exact texts
   (`i` is the 0-based entry index):
   - `__upload_url__ request uses the retired count shape: this server requires VGI-Upload-URL-Support: 2 and an 'uploads' parameter (list<struct<size: int64, sha256: utf8>>, one entry per upload)`
   - `__upload_url__ 'uploads' must have 1-100 entries, got N` (`got null` when null)
   - `__upload_url__ uploads[i].size must be an integer >= 1, got X`
   - `__upload_url__ uploads[i].sha256 must be 64 lowercase hex characters`
   - `__upload_url__ uploads[i].size N exceeds VGI-Max-Upload-Bytes (M)`
   Checked in that order, per entry: size, then sha256, then max. A request
   body over `VGI-Max-Request-Bytes` stays a 413 before any of these, so a
   101-entry request (~7.7 KB) may be a 413 on a server with a small request
   cap; the shared conformance test accepts either.
3. **`Content-Length` in provider headers.** The server strips any
   `Content-Length` from a provider's headers before writing `upload_headers`
   (the vendor's `required_headers` includes one).
4. **`Content-Type` on the PUT.** The client sends every `upload_headers` entry
   and, only when none of them is a content type,
   `Content-Type: application/octet-stream`.
5. **Header parsing.** `VGI-Upload-URL-Support`: `"true"` = v1, a positive
   decimal without leading zero = that version, anything else = unsupported.
   Clients refuse v1 (`Server upload protocol is too old: it advertises
   VGI-Upload-URL-Support v1 and this client needs 2`) and also refuse a
   version above 2 (`Server upload protocol is newer than this client: ...`).
6. **`expires_at`** is the earlier of the PUT and GET expiries (the vendor's
   grant has separate ones).
7. **Digests and compression.** The entry's `sha256` covers the stored (PUT)
   bytes; `vgi_rpc.location.sha256` covers the decompressed IPC stream. The
   reference client uploads uncompressed IPC, so they are equal. Request
   pointers carry no size, so the server verifies the digest only.

### Request-pointer resolution
8. **Where the sha256 requirement applies.** Every server-side request
   resolution path, on every transport: HTTP unary, stream `/init`, stream
   `/exchange` input; pipe/Unix/TCP unary requests and exchange inputs; and
   `vgi_rpc.wire` request decoding. It does not apply to result pointers or
   stream headers read by clients.
9. **Missing/malformed digest** is refused before any fetch:
   `Request pointer is missing vgi_rpc.location.sha256 (required on request pointers since vgi-rpc upload-URL protocol v2): <redacted url>`;
   a present-but-malformed digest:
   `Request pointer vgi_rpc.location.sha256 is not 64 lowercase hex characters: <redacted url>`.
   On HTTP the reference answers with an RPC error batch (`200` +
   `X-VGI-RPC-Error: true`, like other resolution failures). The spec does not
   mandate a status.
10. **Mismatch text** keeps the old phrase for existing matchers:
    `ExternalLocation sha256 mismatch (SHA-256 checksum mismatch) for <redacted url>: expected <hex>, got <hex>`.

### Providers
11. **S3:** the boto3 client is created with `signature_version="s3v4"`.
    Without that, botocore presigned with SigV2 in us-east-1 and signed neither
    header. The PUT is presigned with `ContentLength` + `ChecksumSHA256`, which
    gives `X-Amz-SignedHeaders=content-length;host;x-amz-checksum-sha256`.
12. **GCS:** V4 PUT signed with `content_type="application/octet-stream"` and
    `headers={"x-goog-content-length-range": "N,N"}`; both returned in
    `upload_headers`. The sha256 is validated as hex but cannot be enforced at
    storage.
13. **Blob vendor:** `VendorStorage.generate_upload_url` always uses
    `POST /v1/blobs/put` with `purpose: "rpc"` (whatever the storage's
    configured purpose), because only rpc grants carry a `download_url`. The
    vendor Worker already returns `upload_url`, `required_headers`
    (`content-length`, `x-amz-checksum-sha256`) and `download_url` for rpc, so
    **no vendor change was needed**. Note that the vendor accepts `size: 0`;
    vgi-rpc servers refuse it earlier. `/v1/upload_urls` (unsized) is no
    longer called; `_VendorApi.upload_urls` is kept as a raw API mirror.

### Test fakes
14. **Fake vendor** (`vgi-rpc-fake-vendor`), sized PUT verification order:
    length ≠ size → `400 IncompleteBody` (was 403 before); checksum header
    absent or ≠ signed (or wrong `Content-Encoding`) → `403
    SignatureDoesNotMatch`; body digest ≠ signed → `400 BadDigest`. Nothing is
    stored on a refusal.
    `GET /__admin__/fault?purpose=upload&action=corrupt&count=N` corrupts (XOR
    0xFF) the next N signed PUTs of **any** purpose, before verification. That
    includes server-side externalization PUTs, because both go through
    `/v1/blobs/put`. In a SQL test the request upload happens first, so `count=1`
    hits it. `action` must be `corrupt`; `/__admin__/reset` disarms it.
    `/__stats__` (`STATS_KEYS`) gains, appended in this order:
    `upload_refusals` (sum of the next three), `upload_bad_digest`,
    `upload_incomplete_body`, `upload_signature_mismatch`, `uploads_corrupted`.
15. **Fake storage** (conformance `/alloc`): a sized allocation is
    `{"size": N, "sha256_b64": "<b64>"}`. The response adds
    `upload_headers: {"x-amz-checksum-sha256": <b64>}`, and PUTs to that blob
    (`/upload/` and the legacy `/blob/` routes) are verified like the fake
    vendor (400 IncompleteBody / 403 SignatureDoesNotMatch / 400 BadDigest,
    XML body). `/_stats` adds `allocations`, `upload_refusals` and
    `upload_refusals_<Code>`. Other SDKs' conformance adapters must send size
    + `sha256_b64` to `/alloc` and pass `upload_headers` through, or
    `TestUploadUrlV2::test_storage_refuses_wrong_bytes` fails.
16. **Client driver protocol** (`CLIENT_DRIVER_PROTOCOL.md` §4.10):
    `request_upload_urls` takes `{"uploads": [{"size", "sha256"}]}` and returns
    `upload_headers` per URL; `capabilities` adds `upload_url_version`.

### Python API (for reference)
`UploadUrlProvider.generate_upload_url(size, sha256_hex) -> UploadUrl`;
`UploadUrl.headers: Mapping[str, str]` (default empty);
`request_upload_urls(base_url=None, *, uploads: Sequence[tuple[int, str]], ..., capabilities=None)`;
`HttpServerCapabilities.upload_url_version`; in `vgi_rpc.http`:
`UPLOAD_URL_PROTOCOL_VERSION`, `upload_put_headers()` and
`parse_upload_url_support()`; in `vgi_rpc.external`: `is_sha256_hex()`,
`sha256_b64_from_hex()` and `resolve_external_location(..., require_sha256=)`.
`upload_url_support` now means the header is present at any version.
