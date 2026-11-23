# StateRef, turn budgets and the object store: contract (vgi-rpc, protocol 3.4)

Status: **normative for all eight vgi-rpc SDKs** (Python, Go, TypeScript,
Rust, Java, C#, C, C++). Implemented in the Python reference on
`protocol-3.4` (Phase 2a); decisions it settled are at the end of this
document. The other seven follow in Phase 4.
Decided by Rusty (the maintainer), 2026-11-22, as part of the "large values in
VGI protocol 3.4" plan (v5). The Python reference implements it first
(Phase 2); the other seven follow (Phase 4). `docs/WIRE_PROTOCOL.md` is
updated when the reference lands, as was done for `UPLOAD_URL_V2.md`.

All work lives on `protocol-3.4` branches. No releases until the 3.4 release
wave.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## Contents

1. Why
2. `StateRef`: API contract
3. `StateRef`: encoding (byte layouts)
4. Sealing scopes and AAD
5. Lifecycle rules (when a value is encoded, stored, resolved)
6. Errors
7. Compression before sealing (the standard token codec)
8. The call token after 3.4
9. Turn budget and the cursor rule
10. Storage contract (`put_object`, `get`) and grant columns (`object_id`, `ref`)
11. The object cache (required framework behaviour, not wire protocol)
12. Token TTL: 6 hours
13. Removal of the vendor `upload_urls` client API
14. Observability
15. Shared conformance tests
16. Reference byte layouts in one place

---

## 1. Why

On HTTP every stream continuation and cancel echoes the stream's state back
to the server. VGI puts its whole init request in that state (vgi-python in
the call token's `call_state` slot, the other six vgi SDKs in the cursor),
and the init request embeds the serialized `BindRequest` with the function's
arguments. A 1 MB argument made the first continuation a 700 KB request
and hit `413` against `VGI_RPC_MAX_REQUEST_BYTES=32768`; a 300 MB argument
uploaded ~1.2 GB and then failed on a ~156 MB continuation.

`StateRef` is a general, opt-in way for a server to say "this part of my
stream state does not change": store it once, carry a small sealed tag in the
state, read it back on any later turn on any replica. The vgi frameworks use
it for the init request. Worker authors may use it for their own immutable
per-stream data. The client never parses it: it only echoes the opaque state
token as today, so **no client changes** are needed for `StateRef`.

## 2. `StateRef`: API contract

Idiomatic per language. Python shown:

```python
ref = ctx.state_ref(value)        # value: bytes, or a serializable record -> StateRef
value = ctx.resolve(ref)          # bytes; typed variant below
record = ctx.resolve(ref, MyRecord)
```

- `state_ref(value)` accepts raw `bytes`, or a record the SDK can serialize
  (Python: an `ArrowSerializableDataclass`; Go: a type with the SDK's
  serializer; and so on). The serialized byte form is the SDK's choice: a
  `StateRef` is minted and opened by the same server and never crosses ports.
- It returns a `StateRef`: a small value that can be stored in a stream
  state field (Python: a field annotated `StateRef`; other SDKs: a field of
  the SDK's `StateRef` type). The field serializes as the encoded bytes of
  §3.
- `resolve(ref)` returns the value's serialized bytes; SDKs SHOULD offer a
  typed variant that deserializes into a given record type. Resolve is
  valid on every turn and every transport.
- `StateRef` works identically **with or without storage configured** and
  on **every transport** (HTTP, Iroh, pipe, stdio, unix, TCP). Worker code
  never branches on either.
- The value MUST be treated as immutable after `state_ref()`. The SDK MAY
  return the same in-memory instance from `resolve` to concurrent turns.

### 2.1 `state_ref()` only on the init turn

`state_ref()` MUST be called only while the server is handling the stream's
**init** request (HTTP `/init`, or stream construction on the byte-stream
transports). On a continuation (HTTP `/exchange`, including cancel) it MUST
raise a programming error naming the method: kind `state_ref_misuse`, code
`FAILED_PRECONDITION`, message
`state_ref() may only be called while handling a stream's init request (<protocol>.<method>)`.

The tag then rides the state from turn to turn, so "store once" holds across
turns and replicas without any coordination.

### 2.2 Memoization within the init turn

Within one stream's init turn, calling `state_ref()` twice with the same
object (identity) or with byte-equal serialized values MUST return refs that
encode to the same bytes and MUST store at most one object. The memo lives
only for that init turn; it is not a cross-stream cache. (Parallel streams
on one large init each store their own object in 3.4; sharing across a
query's streams is deferred.)

A `StateRef` belongs to exactly one stream: its tag is bound to that stream's
`call_id` (§4), so a ref MUST NOT be shared across streams (a ref copied into
another stream's state fails `invalid_state_ref`). **State transitions copy
the ref; they never call `state_ref()` again.** Frameworks MUST therefore
build any framework-owned ref (the vgi `init`) in the init handler, and MUST
NOT build stream state lazily on the first exchange, which is a continuation.

## 3. `StateRef`: encoding (byte layouts)

A `StateRef` field serializes as a byte string. The first byte is the
**form**, which doubles as the format version:

| Form byte | Meaning |
|---|---|
| `0x01` | inline, v1 |
| `0x02` | stored, v1 |
| anything else | `invalid_state_ref` |

### 3.1 Value payload (common to both forms)

```
payload := codec (1 byte) || body
```

`codec` and `body` follow the token codec standard (§7): `0x00` raw,
`0x01` zstd, `0x02` deflate (a reader must accept every codec its own writer
can emit). The writer MUST compress when the compressed
form is smaller and MUST use raw otherwise. The decompressed body is the
value's serialized bytes. Readers MUST cap decompression at 64 MiB **or**
the server's `max_fetch_bytes`, whichever is larger (a stored value can be
larger than a token).

### 3.2 Inline form

```
Offset  Size  Field
0       1     form = 0x01
1       N     payload (§3.1)
```

No seal of its own: an inline `StateRef` only ever exists inside the state
token's seal (HTTP) or in process memory (other transports), so it inherits
the token's confidentiality, integrity, AAD binding and `created_at`.

### 3.3 Stored form

```
Offset  Size  Field
0       1     form = 0x02
1       ...   tag envelope: crypto.seal_bytes(tag_plaintext, token_key,
              aad = STATEREF_TAG_AAD, version = 0x01)
              = 0x01 || nonce (24) || ciphertext+tag (16)
```

`tag_plaintext`:

```
Offset  Size  Field
0       1     v = 0x01                    (tag plaintext version)
1       8     created_at: uint64 LE, seconds since Unix epoch, when stored
9       8     size: uint64 LE, byte length of the stored object
17      32    sha256: raw SHA-256 of the stored object bytes
49      2     object_id_len: uint16 LE (1..1024)
51      M     object_id: UTF-8, as returned by put_object (§10)
```

Total tag bytes ≈ 1 + 25 + 51 + M + 16, about 130 bytes for a vendor
`object_id`.

The **stored object** is the AEAD ciphertext of the payload, never plaintext:

```
object_bytes := crypto.seal_bytes(payload, token_key,
                                  aad = STATEREF_OBJECT_AAD, version = 0x01)
```

uploaded once with `put_object(object_bytes)` (§10). `size` and `sha256` in
the tag describe `object_bytes`.

**Why the tag carries `size` and `sha256`:** every `StateRef` object of one
stream is sealed under the same `STATEREF_OBJECT_AAD` (it binds the stream,
not the individual ref). Without a digest, anyone able to write the bucket
could swap two of a stream's objects (say, a worker's own ref and the `init`)
and each would still open. The digest in the sealed tag pins each tag to
exactly one object; it is checked before the object is opened (§5.3).

## 4. Sealing scopes and AAD

Let `BASE_AAD(auth, protocol)` be exactly the cursor-token AAD the server
already computes (Python `_compute_aad`, `vgi_rpc/http/server/_state_token.py`):

```
prefix  := b"vgi_rpc.state.v7\x00" if a peer-evidence binding digest is present
           else b"vgi_rpc.state.v6\x00"
tail    := b"\x00anonymous"                                  (unauthenticated)
         | b"\x01" || domain || b"\x00" || principal          (authenticated)
         then, if binding present: || b"\x00" || binding
BASE_AAD := prefix || tail || b"\x00" || protocol_wire_name
```

(`docs/WIRE_PROTOCOL.md` § *State token binary format* still documents the
older `v4` prefix and call-token version 1; it is updated with this work.)

Then:

```
STATEREF_TAG_AAD    := b"vgi_rpc.stateref.v1\x00"        || BASE_AAD(auth, protocol) || call_id
STATEREF_OBJECT_AAD := b"vgi_rpc.stateref.object.v1\x00" || BASE_AAD(auth, protocol) || call_id
ARGREF_AAD          := b"vgi_rpc.argref.v1\x00"          || BASE_AAD(auth, SERVER_SCOPE)
```

- `protocol` is the wire name of the protocol that owns the stream, as for
  the cursor token.
- `call_id` is the stream's 16-byte random identifier, minted at `/init` and
  authenticated inside every cursor token (Python `_CALL_ID_LEN`). An SDK
  whose token layout has no such id MUST add one (16 random bytes from a
  CSPRNG, carried inside the cursor seal).
- `SERVER_SCOPE` is `"\x00server"` (Python `SERVER_SCOPE`): the
  `__upload_url__` endpoint lives at the server prefix, not under a protocol,
  so the argument reference is bound to the principal only.

Consequences: a stored tag cannot be opened as a cursor, as a call token, as
an argument reference, as a tag of another stream, or under another
principal or protocol. The object cannot be opened as anything but that
stream's `StateRef` object. All four sealed kinds use one `token_key`.

**Cross-replica by construction:** every replica that can open a cursor can
open the tag, because both use the shared `token_key`. That is already a
requirement for multi-replica HTTP (with no key configured each process
draws a random one; Python `_factory.py`, `make_wsgi_app`). Deployments with
more than one replica MUST set a shared token key; documentation MUST say so
prominently.

## 5. Lifecycle rules

### 5.1 When the form is decided: at first serialization

A `StateRef` created by `state_ref()` holds its value in memory. Its form is
decided, and any store performed, **when it is first serialized into a state
token**, which is on the init turn's response, after the call token has been
minted (so `call_token_bytes` in §9 is known). The SDK then caches the
encoded bytes on the ref. Every later serialization of the same ref emits
those exact bytes: no re-compression, no re-seal, no second upload.

A `StateRef` deserialized from an inbound token holds the encoded bytes and
re-emits them unchanged.

### 5.2 Inline or stored

```
stored  iff  transport is HTTP (incl. Iroh)
         and storage is configured (§10)
         and len(payload) > state_ref_inline_bytes        (§9)
inline  otherwise
```

- On pipe, stdio, unix and TCP the state never leaves the process: the ref
  MUST be inline (in practice: held in memory, never encoded) and no storage
  call is ever made.
- With no storage configured, an over-limit value stays inline; the cursor
  rule (§9) then decides whether that is a warning or an error.
- If `len(object_bytes)` exceeds the server's `max_fetch_bytes`, the store
  MUST fail on the init turn with kind `state_ref_too_large`, code
  `RESOURCE_EXHAUSTED` (a replica could never fetch it back).

### 5.3 Resolve

1. Inline: decompress the payload, return the bytes. No storage call. There
   is no required per-turn memo; an inline value is decoded on each HTTP turn
   (see §11.1).
2. Stored, in order, all before any network I/O except step 5:
   1. strip the form byte, open the tag envelope with `STATEREF_TAG_AAD`
      (failure: `invalid_state_ref`);
   2. check `v == 0x01` and the layout (failure: `invalid_state_ref`);
   3. if `token_ttl > 0` and `now - created_at > token_ttl`: fail
      `invalid_state_ref` with message `StateRef expired` (never fetch an
      expired ref);
   4. if `size > max_fetch_bytes`: fail `state_ref_too_large`;
   5. look up the object cache by `object_id` (§11); on a miss, single-flight
      `get(object_id)` (§10);
   6. verify `len == size` and `sha256` (failure: `invalid_state_ref`,
      message names a digest or size mismatch, never the URL);
   7. open `object_bytes` with `STATEREF_OBJECT_AAD` (failure:
      `invalid_state_ref`), decompress the payload, insert into the cache,
      return.

### 5.4 C#, C++ and C: single-replica in 3.4 (decided)

vgi-rpc-csharp and vgi-rpc-cpp keep HTTP stream state in an in-memory
registry and seal only a 16-byte call id into the token; vgi-c runs its
streams stateful in process. **They stay that way for 3.4**; stateless
continuations are post-3.4. In 3.4, for these three SDKs:

- every `StateRef` is **in-memory and inline only**, as on pipe;
  `state_ref` / `resolve` work (so worker code is portable), nothing is ever
  stored, sealed per turn or fetched;
- the cursor rule (§9), tag expiry (§5.3) and the call token's `schemas_ref`
  (§8) do not apply (there is no serialized state);
- the shared conformance tests that need stored tags, cross-replica
  behaviour, the cursor rule or tag expiry (§15: 1 stored half, 2, 3, 5, 10)
  are **capability-gated skips** for them, and each lane MUST list those
  skips with their reason in its expected-skip list (`EXPECTED_SKIP_REASONS`),
  so a skip that is not on the list fails the lane;
- Phase 5 claims no cross-replica behaviour for them;
- they still implement `put_object` / `get(object_id)`, grant `object_id` /
  `ref`, argument-reference resolution and the object cache (§10, §11).

When they do go stateless (post-3.4), the whole spec applies; in particular
they MUST NOT put an init request into the call token (vgi-rpc-c's stateless
mode does, `src/http.c` `call_token`), and the codec rule (§7) applies to every
token that carries serialized state.

### 5.5 Lifetime

Objects carry **no TTL** and are never deleted by the server. Cleanup is the
bucket lifecycle rule (`rpc/` objects: ≥ 1 day; R2 lifecycle is asynchronous,
so up to ~48 h in practice). That covers the 6 h maximum stream duration
(§12). A server MUST NOT issue deletes for `StateRef` objects.

## 6. Errors

Every error below is an RPC error batch (`vgi_rpc.error_kind` / `error_code`
per `WIRE_PROTOCOL.md` § *Error model*). Messages MUST NOT contain URLs
(other than redacted), tokens, object contents or digests of client data.

| Kind | Code | When | HTTP |
|---|---|---|---|
| `state_ref_missing` | `NOT_FOUND` | `get(object_id)` reports the object absent (404 / vendor `not_found`). Means a stream older than the bucket lifecycle, which should not happen within 6 h. Message: `stream state object is gone (StateRef object expired or deleted); restart the query` | 200 + `X-VGI-RPC-Error: true` |
| `invalid_state_ref` | `INVALID_ARGUMENT` | unknown form byte, tag unseal/scope failure, bad layout, expired tag, size/digest mismatch, object unseal failure | 400 |
| `state_ref_too_large` | `RESOURCE_EXHAUSTED` | stored object over `max_fetch_bytes` (at store or resolve) | 200 + `X-VGI-RPC-Error: true` |
| `state_ref_misuse` | `FAILED_PRECONDITION` | `state_ref()` called on a continuation (§2.1) | 200 + `X-VGI-RPC-Error: true` |
| `cursor_too_large` | `RESOURCE_EXHAUSTED` | §9 hard error | 200 + `X-VGI-RPC-Error: true` |

Transient storage failures on `get` (5xx, timeout) surface as the storage
error with code `UNAVAILABLE`, never as `state_ref_missing`.

## 7. Compression before sealing (the standard token codec)

Every sealed token payload (cursor, call, and the inline/stored `StateRef`
payloads of §3.1) is **compressed, then sealed**: once sealed, a token is
ciphertext and no outer codec can find its redundancy. This is the format
Python (`_state_token.py`, `_pack_plaintext`), Go (`http_state.go`), Rust
(`http.rs`), Java (`StateToken.java`), C (`token.c`) and the TS call token
already use. It is now the standard. **C# and C++** have nothing to compress
in 3.4: their tokens carry only a 16-byte call id and they stay single-replica
(§5.4). They MUST adopt the codec when they go stateless (post-3.4).
**TypeScript MUST extend it to its cursor token**: today
vgi-rpc-typescript compresses only the call token body (`src/http/token.ts`,
`packCallToken`) and seals the cursor raw (`packStateToken`).

```
sealed_payload := codec (1 byte) || body
```

| `codec` | Body | Notes |
|---|---|---|
| `0x00` | raw framed plaintext | MUST be used when compression does not make it smaller: a small token never grows |
| `0x01` | zstd frame | the default; reference level 3 |
| `0x02` | raw DEFLATE (RFC 1951) | for runtimes without zstd |
| other | — | reserved; MUST be rejected |

**A reader MUST accept every codec its own writer can emit**, and MUST reject
tags it does not implement. `0x02` is needed only by SDKs that emit it; an SDK
that writes only `0x00`/`0x01` need not implement DEFLATE.

Requirements (unchanged from `WIRE_PROTOCOL.md` § *Compression happens inside
the seal*, now with fixed tag values):

- compress whenever the compressed form is smaller; otherwise emit `0x00`;
- bound decompressed output (64 MiB for tokens; §3.1 for `StateRef` values);
- an unknown codec or a body that fails to decompress is the same uniform
  `400 Bad Request` as any other token failure. (vgi-rpc-c's open path treats any
  non-zero tag as zstd, `src/token.c:97`; it MUST reject unknown tags.)

No cross-SDK token compatibility is required (a token is minted and opened by
the same server); the fixed tag values only make ports reviewable against one
table. Version bytes on every envelope turn a rolling-deploy mismatch into a
clean 400.

**Inline `StateRef`s** are compressed inside their own payload (§3.1), in
every SDK, even though the surrounding token is compressed again: the inline
limit (§9) is measured on the compressed payload, so it must be computed
before the token is assembled.

## 8. The call token after 3.4

vgi-rpc's call token **stays** as transport infrastructure: it carries the
stream's frozen output and input schemas, `stream_id`,
`response_limit_bytes`, and the optional `call_state` slot, for every vgi-rpc
stream. Clients keep echoing it; the extension does not change. (The
`call_state` slot stays available to vgi-rpc services; VGI stops using it,
see the vgi spec.)

One change: **the two schemas go behind a `StateRef`** so wide tables do not
blow the turn budget. Python call token v2 → **v3** (other SDKs bump their
own call-token version):

```
Offset  Size  Field                                   (inside the seal, after §7 codec)
0       8     created_at: uint64 LE
8       16    call_id
24      4     call_len: uint32 LE    | call_state_bytes   (empty when none)
...     4     type_len: uint32 LE    | call_state_type (UTF-8, empty when none)
...     4     schemas_len: uint32 LE | schemas_ref: StateRef bytes (§3) whose value is
                                       uint32 LE len || output_schema_bytes ||
                                       uint32 LE len || input_schema_bytes
...     4     sid_len: uint32 LE     | stream_id (UTF-8)
...     8     response_limit_bytes: uint64 LE (0 = unbounded)
```

`schemas_ref` follows §5.2 with its inline limit computed with
`call_token_bytes = 0`. Readers resolve it like any `StateRef` (stored form:
one GET per process per stream, then the object cache).

SDKs whose stream tokens do not have a separate call token (see the per-SDK
checklist in vgi `docs/large-values-3.4-sdk-checklist.md`) apply the same
rule to wherever they keep the frozen schemas.

## 9. Turn budget and the cursor rule

```
headroom              = 4096
turn_budget           = floor((max_request_bytes - headroom) / 1.34) - call_token_bytes
state_ref_inline_bytes = min(VGI_RPC_STATE_REF_INLINE_BYTES, floor(turn_budget / 4))
```

- `max_request_bytes` is the server's effective request cap (the value it
  advertises as `VGI-Max-Request-Bytes`, i.e. the minimum of the configured
  and hosting caps). **Unset or ≤ 0 means infinite**: `turn_budget` is
  infinite and `state_ref_inline_bytes = VGI_RPC_STATE_REF_INLINE_BYTES`.
- `1.34` converts the request's base64 metadata size into sealed (raw)
  bytes (4/3, rounded up for padding). `turn_budget` and `call_token_bytes`
  are **sealed** byte counts (after base64 decoding: envelope version, nonce,
  ciphertext, tag).
- `call_token_bytes` is the sealed length of this stream's call token; 0 for
  an SDK that does not send one, and 0 when sizing the call token's own
  `schemas_ref`.
- `VGI_RPC_STATE_REF_INLINE_BYTES`: environment variable / server option,
  default **16384**; must be a positive integer, else startup fails. Phase 0
  confirmed the default: typical inits compress to 2–5 KB; 16 KiB is crossed
  at about 1,300 output columns or 1,400 random 64-bit IN/join keys.
- **Floor:** a configured `max_request_bytes` below **16384** is not refused
  (conformance and test servers deliberately use 900–9000-byte caps). The
  server logs a startup **warning**,
  `max_request_bytes (N) is below 16384: the cursor rule is disabled and stream continuations may not fit`,
  and **disables the cursor rule** (no `cursor_large` / `cursor_too_large`) for
  its lifetime. `state_ref_inline_bytes` is still computed by the formula
  (floored at 0, so everything over-limit is stored when storage exists).
  vgi-rpc-cpp today advertises its externalization threshold (default 4096)
  as `VGI-Max-Request-Bytes` when no request cap is set
  (`src/http_transport.cpp`); it MUST stop doing that, since the advertised cap
  is `max_request_bytes` for every rule here.
- The 4 KiB headroom covers the rest of a continuation request (other
  metadata keys, the IPC framing, an empty or small input batch).

**The cursor rule.** On every turn, after `StateRef` encoding, the server
compares the sealed cursor token length with `turn_budget`:

- **Before** an SDK has finished the vgi `init: StateRef` port (vgi spec §6):
  a cursor over `turn_budget` logs a **warning** `cursor_large` naming the
  protocol, method, cursor bytes and budget, and the stream continues.
- **After** the port: a cursor over `turn_budget` is a **hard error**
  `cursor_too_large` (§6), raised while minting the token, **only when
  `max_request_bytes > 0`**. Message:
  `cursor_too_large: stream state for <protocol>.<method> is <N> bytes, over this server's turn budget of <B> bytes (VGI-Max-Request-Bytes <M>). Wrap data that does not change across turns in ctx.state_ref().`

The rule applies to exchange (input-carrying) continuations too: their input
batch counts against `max_request_bytes` separately and is externalized by the
client as today.

## 10. Storage contract and grant columns

### 10.1 `ExternalStorage` gains two methods (every vgi-rpc SDK)

```
put_object(data: bytes) -> object_id: str
get(object_id: str) -> bytes
```

- `put_object` stores `data` as one `rpc`-purpose object and returns the
  backend's identifier. It MUST send the size and SHA-256 so storage verifies
  the body (vendor `POST /v1/blobs/put` with `purpose: "rpc"`, `size`,
  `sha256_b64`, and the advisory `origin: "state_ref"`; S3/R2 a SigV4 PUT
  signing `content-length` and `x-amz-checksum-sha256`).
- `get(object_id)` mints the download server-side from the id (vendor
  `POST /v1/blobs/get`; S3 a presigned GET of the key), MUST apply the same
  host pinning as other storage fetches, and MUST cap the download at
  `max_fetch_bytes`. It raises a distinguishable "missing" error for 404 /
  vendor `not_found` (→ `state_ref_missing` or the vgi `arguments_ref_missing`)
  and a transient error otherwise. Callers verify size and SHA-256 themselves.
- `object_id` for S3/GCS is the object key; for the vendor, the vendor's
  `object_id`; for fake storage, its blob id. Max 1024 bytes UTF-8.
- Objects carry no TTL. Operators MUST set a bucket lifecycle rule ≥ 1 day on
  the `rpc` prefix; SDKs SHOULD warn at startup where they can detect a
  missing rule, and MUST document it.

### 10.2 Grant columns: `object_id` and `ref`

The `__upload_url__/init` response (upload-URL protocol v2, `UPLOAD_URL_V2.md`)
gains two columns, appended after `upload_headers`:

| Column | Arrow type | Nullable | Description |
|---|---|---|---|
| `object_id` | `utf8` | No | The storage id of the object this entry's PUT creates. |
| `ref` | `binary` | No | A sealed argument reference for that object (below). |

`ref` is `crypto.seal_bytes(argref_plaintext, token_key, aad = ARGREF_AAD, version = 0x01)`:

```
Offset  Size  Field
0       1     v = 0x01
1       8     size: uint64 LE  (the entry's declared size)
9       32    sha256: raw digest of the entry's declared sha256 hex
41      2     object_id_len: uint16 LE
43      M     object_id: UTF-8
```

The server mints `ref` from the **claimed** `(size, sha256)` without seeing the
bytes; storage enforces them on the PUT (where it can), and the resolver
verifies them on the GET. There is no `created_at` and no expiry check on an
argument reference: a prepared statement may re-send it until the object is
deleted by lifecycle, or until a replica's cache entry ages out (§11; the vgi
spec defines that error). Consequence: **on an unauthenticated worker an
argument reference is a bearer capability for about 1–2 days** (bucket
lifecycle ≥ 1 day, asynchronous), for anyone who obtains it.

**Same principal for grant and use.** `ARGREF_AAD` is computed from the
`AuthContext` of the `__upload_url__/init` request and again from that of the
request presenting the ref. A server MUST authenticate `__upload_url__/init`
exactly as it authenticates its protocol methods (same authenticators, same
peer-evidence binding digest, same anonymous fallback), so the two
`AuthContext`s agree for the same client. A shared conformance test (§15, 13)
checks it.

`UploadUrlProvider.generate_upload_url(size, sha256_hex)` returns the
`object_id` alongside the URLs (Python: `UploadUrl.object_id: str`). Every
provider (vendor, S3, GCS, fake storage, test providers) MUST supply one.

**Resolving an argument reference** (library primitive, used by the vgi
layer): open `ref` with `ARGREF_AAD`; check `v`, and that the sealed `size` and
`sha256` equal the values the caller was given alongside the ref; then
object cache by `object_id` (§11) → `get(object_id)` → verify length and
SHA-256 of the bytes → return. The vgi spec maps failures to
`invalid_arguments_ref`, `arguments_ref_integrity` and `arguments_ref_missing`.

Clients that do not use references ignore the two new columns: servers MUST
NOT require clients to read them, and clients MUST read the four v2 columns by
name.

### 10.3 Fake and demo storage

Fake storage (conformance `/alloc`, `vgi_rpc/conformance/fake_storage.py`),
the fake vendor (`vgi-rpc-fake-vendor`) and every SDK's demo/test storage
implement `put_object`, `get(object_id)` and the grant `object_id`, and expose
counters tests can read:

- fake storage `/_stats` adds `object_puts` and `object_gets` (calls of the
  object API, by id);
- fake vendor `STATS_KEYS` appends, in this order: `puts_state_ref` (grants
  with `origin: "state_ref"`), `gets_state_ref` and `gets_upload`
  (`/v1/blobs/get` calls on objects granted with `origin` `state_ref` and
  `client_upload` respectively). Existing keys are unchanged; `upload_urls`
  stays and is always 0 (§13).

## 11. The object cache (required; not wire protocol)

Every vgi-rpc SDK MUST provide one object cache per server process, used by
`resolve(StateRef)` for stored refs and by argument-reference resolution.
Nothing about it is visible on the wire, and the protocol and storage define
no caching.

1. **Key: `object_id`. Never a SHA-256.** Grants mint objects for claimed
   hashes without seeing the bytes, so a digest-keyed cache would hand one
   principal's value to another who merely knows its hash. An `object_id`
   only reaches a server inside a tag or ref sealed for the caller.
2. **Single-flight:** concurrent resolves of one `object_id` share one fetch
   (parallel scan threads, secondary inits, concurrent turns).
3. **Byte-bounded LRU:** `VGI_RPC_OBJECT_CACHE_BYTES`, default
   `min(1 GiB, 40% of the cgroup memory limit)` (or of physical memory when
   no cgroup limit is found). A value larger than the bound is resolved and
   returned but not retained, with a warning.
4. **Memos hold `object_id`, never a strong reference to the value**, and look
   it up again on use. Resident bytes are then bounded by the cache alone; a
   memo that pins the value defeats the bound. (No per-turn memo is required,
   see §11.1.)
5. Cached values are the verified, opened, decompressed bytes. Eviction is by
   size (LRU), plus one age rule:
6. **`fetched_at`.** Each entry records when it was fetched. A hit on an entry
   fetched more than **1 day** ago (the lifecycle bound, §5.5) is treated as a
   miss and re-fetched, so a cache can never outlive the object it mirrors by
   more than the lifecycle allows. User-visible consequence (vgi spec §5a): a
   prepared statement's large argument may fail at any time after 1 day,
   identically on every replica.

### 11.1 No per-turn parse memo (decision 3)

An inline `StateRef` is decompressed and parsed on every HTTP turn; off HTTP
the state stays in memory and costs nothing. A per-process memo is **not**
part of 3.4. If Phase 0 or later measurements show the parse is material for
a realistic worker, a memo returns keyed by a random `ref_id` sealed inside
the tag (never by a digest), as a compatible addition.

Phase 0 measured it (vgi `docs/large-values-3.4-measurements.md`): for a
producer doing real per-batch work the open + parse (+ re-mint) is ≤ 1.6% of
turn time in Go, TypeScript and Rust and < 1% for plain Python scans; it is
material only for trivial producers. So 3.4 ships without a memo, and existing
per-SDK parse caches that pin values (vgi-typescript's carried-init LRU) are
removed.

## 12. Token TTL: 6 hours

The maximum stream duration is 6 h. Every vgi-rpc SDK's default `token_ttl`
becomes **21600 seconds** (Python `make_wsgi_app(token_ttl=3600)` today). It
applies to cursor tokens, call tokens and stored `StateRef` tags (checked
from each one's own `created_at`, before any fetch). `0` still disables
expiry. vgi frameworks that pass their own value (vgi-python `serve.py`) pass
21600.

Defaults today, for the record: Python and TypeScript 3600 s, Go 5 min
(`vgi-rpc-go/vgirpc/http.go`, `defaultTokenTTL`), Rust 300 s
(`vgi-rpc/src/http.rs`); vgi-go and vgi-rust never override theirs, and Go
age-checks the call token on every call-cache miss, so a vgi-go stream longer
than 5 minutes already fails on any turn that lands on a cold replica. Every
vgi framework MUST pass 21600 explicitly as well as the vgi-rpc default
changing.

## 13. Removal of the vendor `upload_urls` client API

The blob vendor drops its unsized `POST /v1/upload_urls` (and its `rpc`
expiry sweep and `rpc` quota). On `protocol-3.4`:

- vgi-rpc-python removes `_VendorApi.upload_urls` and `UPLOAD_URLS_PATH`
  (`vgi_rpc/vendor.py`), and the fake vendor's `/v1/upload_urls` route
  (`vgi_rpc/conformance/fake_vendor.py`; the route answers 404 like any
  unknown path).
- The fake vendor's `upload_urls` stats key **stays, always 0**, because 3.4
  tests read it (vgi-python `test_serve_blob_vendor.py`, vgi-rpc-typescript
  `vendor.test.ts`, vgi-rpc-java `FakeVendorTest.java`).
- `VendorInfo` MUST tolerate the vendor's info response no longer carrying
  `max_upload_urls`.
- Go, Rust, TS and Java already removed their callers; C#, C and C++ remove any
  they have.

This is **not** the vgi-rpc `__upload_url__` endpoint or the client's
`request_upload_urls()`, which stay (upload-URL protocol v2).

## 14. Observability

Every SDK emits (log events and, where the SDK has metrics, counters):

| Name | Fields |
|---|---|
| `state_ref.store` | protocol, method, payload bytes, object bytes, form |
| `state_ref.resolve` | form, cache `hit`/`miss`, bytes |
| `arguments_ref.mint` | size |
| `arguments_ref.resolve` | cache `hit`/`miss`, bytes |
| `object_cache` | `hit` / `miss` / `evict`, resident bytes |
| `cursor_large` / `cursor_too_large` | protocol, method, cursor bytes, budget |

Never log tag bytes, object ids together with principals beyond what the access
log already records, digests of client data, or values.

## 15. Shared conformance tests

Added to `vgi_rpc/conformance` (and so to every SDK's runner):

1. `StateRef` inline round trip (no storage) and stored round trip (fake
   storage; `object_puts == 1`).
2. Cross-replica resolve: two server instances sharing `token_key` and storage;
   init on A, continue on B; B makes exactly one `get`.
3. A tampered tag, a tag from another stream, and a tag under another
   principal are refused with `invalid_state_ref`; a cursor token presented
   as a tag, and an argument ref presented as a tag, are refused.
4. Codec round trip: a large payload compresses (`0x01`/`0x02`), a tiny one
   stays `0x00`, an unknown codec is refused.
5. An over-budget cursor warns (`cursor_large`) before the port and errors
   (`cursor_too_large`) after it; no error when `max_request_bytes` is unset.
6. Object cache: N concurrent resolves of one stored ref → one `get`.
7. The byte bound evicts (small `VGI_RPC_OBJECT_CACHE_BYTES`, two values →
   second resolve of the first is a miss).
8. Memos do not pin (after eviction, resident bytes drop).
9. `state_ref()` on a continuation raises `state_ref_misuse`.
10. Expired tag (`token_ttl` small) is refused before any `get`.
11. Grant columns: `__upload_url__/init` returns `object_id` and a `ref` that
    the server's resolve primitive accepts for the uploaded bytes and refuses
    for a wrong size/sha256 or after the object is deleted (missing).
12. `max_request_bytes` below 16384 starts, logs the warning, and never emits
    `cursor_large` / `cursor_too_large`.
13. `__upload_url__/init` is authenticated exactly like a protocol method:
    with each authenticator the runner supports (incl. peer evidence), a ref
    minted by the grant opens in a `bind`-style call by the same client, and is
    refused (`invalid_arguments_ref`-equivalent) for a different principal; an
    unauthenticated grant on a server that requires auth is refused like any
    unauthenticated call.
14. Object cache age: with the clock advanced past 1 day, a cached entry is
    re-fetched (a deleted object then reports missing).

Tests 1 (stored half), 2, 3, 5 and 10 need serialized stream state; SDKs that
are single-replica in 3.4 (C#, C++, C; §5.4) skip them by capability and list
them in their lane's expected-skip list.

Language-local tests (not observable via the shared suite): the codec tag on
the seal path, and that a stored ref re-serializes to identical bytes without a
second `put_object`.

## 16. Reference byte layouts in one place

```
StateRef (inline)  = 0x01 || codec || body
StateRef (stored)  = 0x02 || 0x01 || nonce24 || AEAD(tag_plaintext, STATEREF_TAG_AAD)
tag_plaintext      = 0x01 || created_at u64le || size u64le || sha256[32] || id_len u16le || object_id
stored object      = 0x01 || nonce24 || AEAD(codec || body, STATEREF_OBJECT_AAD)
argument ref       = 0x01 || nonce24 || AEAD(0x01 || size u64le || sha256[32] || id_len u16le || object_id, ARGREF_AAD)
sealed token body  = codec || framed plaintext          (codec: 0x00 raw, 0x01 zstd, 0x02 deflate)
```

---

## Decisions made in the Python reference (vgi-rpc-python `protocol-3.4`, Phase 2a)

The contract above is unchanged; these settle what it left open. The
published wire summary is `docs/WIRE_PROTOCOL.md` § *StateRef: stream state
by reference* (plus the grant columns under *Upload URL generation*).

### API
1. **Where `state_ref()` is allowed.** Only on the context handed to the
   stream method itself. Every other context — `process`/`exchange`/`produce`,
   `on_cancel`, unary calls — raises `state_ref_misuse`. That includes the
   first producer turn, which HTTP folds into the `/init` request: refusing it
   keeps transport parity (on pipe the first `process()` is a continuation),
   so worker code that works on one transport works on all.
2. **Python surface** (stable for vgi-python):
   `CallContext.state_ref(value) -> StateRef`;
   `CallContext.resolve(ref) -> bytes`, `CallContext.resolve(ref, RecordType) -> RecordType`;
   `CallContext.resolve_argument_ref(ref, *, size, sha256) -> bytes`;
   a state field annotated `StateRef` (`vgi_rpc.state_ref.StateRef`, Arrow
   `binary`; the compact cursor codec carries it too). A ref still in memory
   returns the original record instance from a typed resolve.
3. **Memo** (§2.2) is per init context, keyed by object identity and by the
   serialized bytes; it returns the same `StateRef` instance, whose encoded
   bytes are cached, so it stores once.
4. **Serialization outside a token mint** (a caller serializing a state for
   its own reasons) renders an un-encoded ref inline and does **not** cache
   it, so the next token mint still decides the form.

### Storage
5. **`ObjectStore` is a separate runtime-checkable protocol** (`put_object`,
   `get`) next to `ExternalStorage`, not new members of it, so a third-party
   `ExternalStorage` keeps type-checking. "Storage is configured" (§5.2) means
   the server's storage implements `ObjectStore`; one that does not keeps every
   ref inline and cannot resolve argument references
   (`invalid_arguments_ref`). Every in-tree backend implements both.
6. `get(object_id, *, max_bytes=None)` takes the cap as a keyword (the server
   passes `max_fetch_bytes`). Missing is `vgi_rpc.external.ObjectMissingError`.
   Any other `get` failure surfaces as kind **`storage_unavailable`**, code
   `UNAVAILABLE` (§6 names the code but no kind).
7. **`UploadUrl` gains `object_id: str = ""` and `ref: bytes = b""`.**
   Providers set `object_id`; the server refuses a grant whose provider returned
   none (RPC error batch). The client fills both from the response columns
   when present.
8. **Fake storage object API**: every `/alloc` response carries `object_id`;
   an allocation with `"purpose": "object"` is a `put_object` (counted
   `object_puts`); `GET /objects/{id}` is `get` (`object_gets`, 404 when
   absent); `DELETE /objects/{id}` is a test route standing in for lifecycle
   (`object_deletes`). `/_stats` always reports `allocations`, `object_puts`,
   `object_gets`, `object_deletes`, `upload_refusals`.
9. **Fake vendor**: `puts_state_ref` counts grants with `origin: "state_ref"`
   and they are *also* counted in `puts_rpc` (they are server rpc writes).
   `VendorStorage.get` treats vendor `not_found` and `not_uploaded` (and a
   storage 404) as missing.

### Errors
10. **Argument-reference kinds live in vgi-rpc** with the vgi spec's names:
    `invalid_arguments_ref` (`INVALID_ARGUMENT`), `arguments_ref_integrity`
    (`DATA_LOSS`), `arguments_ref_missing` (`NOT_FOUND`), with generic
    messages. vgi re-raises `arguments_ref_missing` with its prepared-statement
    message.
11. `invalid_state_ref` raised while a method or state runs is answered
    HTTP 400 (with the RPC error batch), like a token failure; every other
    kind here is `200 + X-VGI-RPC-Error: true`.

### Turn budget
12. **The cursor rule is a server option**, `make_wsgi_app(cursor_rule="warn" | "error")`
    (also `serve_http`, `make_sync_client`), default `"warn"`. vgi-python
    passes `"error"` once its `init: StateRef` port lands. Under `"warn"` the
    server also sends the client a `WARN` log whose message starts
    `cursor_large:` (kind `cursor_large` in its extras) — that is what makes
    the warning observable to the shared suite.
13. `state_ref_inline_bytes` is `make_wsgi_app(state_ref_inline_bytes=...)`,
    else `VGI_RPC_STATE_REF_INLINE_BYTES`, else 16384; non-positive is a
    startup `ValueError`. `call_token_bytes` is the base64-decoded length of
    the call token (cached per stream; recomputed on a cold replica).
14. Over-budget producer turns: the cursor is now minted **before** the
    turn's data is flushed, so a `cursor_too_large` refusal never follows data
    the client already consumed.

### Object cache
15. One process-wide cache, `vgi_rpc.object_cache.default_object_cache()`,
    bound read once from `VGI_RPC_OBJECT_CACHE_BYTES` (`0` retains nothing; a
    negative or non-integer value is a `ValueError`). Default: `min(1 GiB, 40%`
    of cgroup v2 `memory.max` / v1 `memory.limit_in_bytes`, else physical
    memory`)`. `ObjectCache.age_entries(seconds)` is a test hook (exposed to
    the shared suite as the conformance method `object_cache_age`).

### Codec
16. Readers accept `0x00`, `0x01` and `0x02`; the writer emits `0x00`/`0x01`.
    zstd decompression now checks a frame's declared content size against the
    bound first: python-zstandard's `max_output_size` only bounds frames that
    do not declare one. This tightens the 64 MiB token bound too.

### Shared conformance (§15)
17. New `ConformanceService` methods every port implements:
    `state_ref_exchange(size, compressible, seed, misuse, cursor_padding)`
    (exchange; input `{override: binary}`, output
    `{size, sha256, form, ref}`; payload from `state_ref_payload`),
    `object_cache_stats()`, `object_cache_age(seconds)`,
    `resolve_argument_ref(ref, size, sha256) -> sha256 hex`. Method count
    90 → 94; describe golden and protocol-hash vector regenerated.
18. Runner fixtures (`vgi_rpc/conformance/_state_ref_pytest.py` docstring):
    `conformance_state_ref_replicas` (two workers, shared key + fake storage,
    no request cap, cursor rule error, object-cache bound 98304, principal
    required via `X-Conformance-Principal`), `conformance_state_ref_short_ttl_port`
    (`token_ttl` 2), `conformance_cursor_budget_ports` (warn / error at 32768,
    floor at 9000), and optional `conformance_state_ref_capabilities`
    (`frozenset()` for C#, C, C++). The Python worker's flags:
    `--cursor-rule`, `--token-ttl`, `--object-cache-bytes`,
    `--require-principal`, and `--max-request-bytes 0` for "no cap".
19. **Capability gating.** Tests 1 (stored half), 2, 3, 5, 10 **and 4** skip
    for the single-replica SDKs with `SINGLE_REPLICA_SKIP_REASON`: their refs
    are never encoded, so the codec bytes test 4 inspects do not exist.
20. **Tests 6, 7, 8 and 14 go through argument references**, not stored
    `StateRef`s, so the in-memory SDKs (which implement argument references
    and the object cache but never store a ref) run them too.
21. **Test 8** reads resident bytes from the worker's own cache stats; a memo
    pinning values outside the cache is not observable across languages.
22. **Test 10** cannot tell tag expiry from call-token expiry over the wire:
    both are minted at `/init` under the same `token_ttl`, and the call-state
    cache entry expires with it. The shared test asserts the expired turn is a
    400 with no fetch; the tag's own expiry check is covered language-locally.
23. **Test 13** covers the principal-header authenticator. Peer-evidence
    binding is covered by a language-local test that the argument-reference
    AAD includes the binding digest (refs do not cross bindings).
