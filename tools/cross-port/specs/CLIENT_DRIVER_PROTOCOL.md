# Client-driver control protocol

How a port exposes its **client** to the shared conformance suite.

The suite is written in Python and drives a *server*. To point it at a foreign
client instead, the port ships one small executable — a **driver** — that
speaks the newline-delimited JSON protocol below on stdin/stdout. The Python
side of the bridge is already written and is not per-port: it is
`vgi_rpc.conformance.client_driver` in the Python reference
(`ClientDriver` / `ClientDriverProxy` / `DriverStreamSession` /
`DriverSessionView`). A port implements the driver and nothing else.

This document is the contract. It is precise enough to implement against
without reading the Python.

---

## 0. The contract

> **A conforming client passes the conformance suite against the reference
> server.**

The second half is load-bearing, and is the reason this document exists.

For weeks the Rust client sent bare URL paths and no routing key. Run against
the *Rust* server it passed everything, because that server accepts both path
shapes. Run against Python — which routes only `{protocol}/{method}` — the same
client, on the same tests, produced **730 failures**. The only variable was
whether the peer was permissive.

**A permissive server cannot validate a client.** A green run against your own
server proves the two halves of one port agree with each other; it does not
prove either agrees with the protocol. So:

- The gate that counts is `VGI_CONFORMANCE_SERVER=python` — your client against
  the Python reference server.
- Runs against your own server, or another port's, are useful for triage and
  for catching server-side gaps. They are not the gate.
- Every accommodation a server makes for a client it also ships with is
  invisible to that pair and only that pair. Assume you have one.

The same asymmetry applies here: the driver is *not* where a port gets to be
lenient. A driver that quietly repairs its client's output — defaulting a method
name, inferring a stream kind, resolving an external pointer in the driver
instead of in the client — converts a client defect into a passing run.

---

## 1. Shape of the channel

- One **request** object per line on the driver's **stdin**; one **response**
  object per line on its **stdout**. UTF-8, LF-terminated, no embedded raw
  newlines (JSON escapes them).
- **Strict lockstep.** The harness writes one request and blocks until it reads
  one response. The driver must never write an unsolicited line, never batch
  responses, and must flush after every line.
- **stdout is the control channel and nothing else.** Diagnostics go to stderr.
  A `println` left in the driver desynchronises the whole run.
- Exactly **one connection per driver process**. `connect` arrives once, first.
  The harness spawns a fresh driver per connection and never reuses one.
- Binary payloads are **standard base64** (RFC 4648 §4, padded, no URL
  alphabet, no line breaks) in fields suffixed `_b64`.
- Unknown fields in a request must be ignored. Unknown fields in a response are
  ignored by the harness. This is how the protocol grows.
- Every request has a string `op`. Every response has a boolean `ok`.

### Arrow IPC framing

Every `*_b64` field that is not a schema carries a **complete Arrow IPC
*stream*** — schema message, then exactly one `RecordBatch` message, then
end-of-stream — holding **one batch and its Arrow custom metadata**. Not a
bare batch message, not a file-format buffer, not several batches.

A schema field (`params_schema_b64`, `result_schema_b64`, `header_schema_b64`)
also carries an IPC stream; only its schema message is read. Whether the stream
also contains an empty batch is immaterial — send the server's own bytes if you
have them, or synthesise a stream over an empty batch if that is easier.

The custom metadata on a request batch is where the RPC lives: `vgi_rpc.method`
names the method, and the harness also stamps `vgi_rpc.protocol`,
`vgi_rpc.protocol_version` and `vgi_rpc.request_version`. The driver reads
`vgi_rpc.method` to decide what to call and passes the **whole metadata map**
through to its client as the call's extra metadata. It must not invent, drop or
rewrite entries.

> A request batch with no `vgi_rpc.method` is a bug in the harness or in the
> driver's IPC reader. Fail loudly (`ok: false`) naming the missing key. Do
> **not** default the method — an earlier driver defaulted to `__describe__`
> and every lost-metadata bug was reported as a retired-method error instead.

---

## 2. Error model

There are two failure channels and they mean different things. Getting this
wrong is the most common driver defect.

| Response | Meaning | `error` field |
|---|---|---|
| `{"ok": false, "error": "<text>"}` | **The driver could not carry out the op at all.** Malformed control line, unknown op, no connection yet, an HTTP-only op on a byte-stream connection, a base64 or IPC decode failure. | a string |
| `{"ok": true, ..., "error": {...}}` | **The op was carried out; the peer or the client library reported an error.** | an object |

The rule: `ok` describes the *driver*, `error` describes the *call*. A
transport failure the client library surfaces as its own error type is a call
error — `ok: true` with the structured object — not `ok: false`. `ok: false`
means the harness's instruction was not executed.

The structured error object is:

```json
{"error_type": "ValueError", "error_message": "boom", "traceback": ""}
```

- `error_type` — the peer's error class name, verbatim. Tests assert on this
  string (`SessionLostError`, `ProtocolError`, `ValueError`, …). Do not
  translate it into your language's exception names.
- `error_message` — the peer's message, verbatim. **The key is
  `error_message`, not `message`.**
- `traceback` — the peer's remote traceback, or `""`. Never `null`.

A response may carry both a successful payload slot set to `null` and an
`error`; the harness reads `error` first.

---

## 3. Log relay

Any response may carry `"logs": [...]`, an array of records the server emitted
during that op. The driver **drains** its accumulated log buffer into each
response — records belong to the op during which they arrived, and a record
must be delivered exactly once.

```json
{"level": "INFO", "message": "processing", "extra": {"rows": "128"}}
```

- `level` — one of `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `EXCEPTION`,
  **uppercase**. The harness looks the name up in an enum; any other spelling
  raises.
- `message` — the log text.
- `extra` — a flat object of string→string. Non-string values are dropped by
  the harness rather than coerced.

An absent or `null` `logs` is an empty array. Tests that assert on server logs
fail if the driver forgets to attach them to the response that produced them.

---

## 4. Ops

Seventeen ops in four groups.

| Group | Ops |
|---|---|
| Connection | `connect`, `describe`, `shutdown` |
| Calls | `unary`, `stream_open` |
| Stream | `tick`, `next_with_token`, `exchange`, `cancel`, `close` |
| HTTP-only | `capabilities`, `request_upload_urls`, `session_begin`, `session_token`, `session_echo_headers`, `session_detach`, `session_end` |

### 4.1 `connect`

First op, exactly once.

```json
{"op": "connect", "transport": "http", "target": "http://127.0.0.1:8123",
 "protocol": "ConformanceService", "external": false,
 "compression_level": 1, "headers": {"X-Conformance-Principal": "alice"}}
```

| Field | Type | Meaning |
|---|---|---|
| `transport` | string | `stdio`, `shm`, `unix`, `tcp`, `http`. |
| `target` | varies | `stdio`/`shm`: **argv array** to spawn. `unix`: socket path. `tcp`: `HOST:PORT` (empty host ⇒ `127.0.0.1`). `http`: base URL. |
| `protocol` | string | **The routing key.** Required. |
| `external` | bool | When true, *the client under test* resolves external-location pointer batches. |
| `compression_level` | int \| null \| absent | HTTP request-body zstd level. Tri-state, see below. |
| `headers` | object | Default request headers for every HTTP request. |
| `shm_size` | int | `shm` only; segment size in bytes. Default 4 MiB. |

Response: `{"ok": true}`, or `{"ok": false, "error": "<text>"}`.

**`protocol` is required and must not be defaulted.** Every request names the
protocol it addresses; a single-protocol server is not an exemption, it refuses
an unrouted call like any other. Over HTTP the reference server routes only
`{protocol}/{method}`, so the driver needs this to build a path that exists —
not merely to stamp a metadata key. A driver that substitutes a hardcoded name
when the field is absent hides exactly the class of bug §0 describes.

**`compression_level` is tri-state**, and the three states are distinct:

- **absent** — the driver picks its client's default;
- **`null`** — request compression is *disabled*;
- **integer** — that zstd level.

The reference shim always sends the key, so a driver sees only the last two in
practice; handle absence anyway rather than reading a missing key as `null`.

`headers` carries only headers the *caller* set. The harness strips the header
names the HTTP client library installs for itself (`accept`,
`accept-encoding`, `user-agent`, `connection`, `host`), because the driver's
client sets its own. On non-HTTP transports `headers`, `compression_level` and
`external` are ignored.

### 4.2 `unary`

```json
{"op": "unary", "request_b64": "<IPC stream>"}
```

Read the single batch and its metadata; take the method name from
`vgi_rpc.method`; call the client's unary entry point with that method, batch
and the full metadata map.

```json
{"ok": true, "result_b64": "<IPC stream>", "logs": [], "error": null}
```

`result_b64` is the reply batch and its custom metadata, re-serialised as a
one-batch IPC stream. For a method with no return value the client still
receives a batch; relay whatever it produced. `null` means "no reply payload",
which the harness reads as `None`.

On a peer error: `{"ok": true, "result_b64": null, "logs": [...], "error": {...}}`.

### 4.3 `describe`

```json
{"op": "describe"}
```

Introspection is the co-hosted `vgi_rpc.Reflection.v1` protocol, whose reply is
two nested payloads rather than one flat batch. Relaying raw Arrow the way every
other op does would force the Python shim to re-implement the reflection schema,
so this one op relays an **already-decoded** description. (`__describe__` is
retired; a driver that calls it gets a refusal naming its replacement.)

The documented sequence is two round trips on `vgi_rpc.Reflection.v1`:
`list_protocols` for what the server hosts and its identity, then `describe` on
**the first hosted protocol whose name does not start with `vgi_rpc.`** — the
framework's own protocols are co-hosted beside the application surface, and the
one a client means by "describe this server" is the one that is not
framework-owned. If every hosted protocol is framework-owned, that is an error
(`ProtocolError`), not an empty description.

```json
{"ok": true, "logs": [], "error": null, "describe": {
  "protocol_name": "ConformanceService",
  "request_version": "1",
  "describe_version": "5",
  "protocol_hash": "<sha256 hex>",
  "server_id": "<opaque>",
  "protocol_version": "2.0.0",
  "methods": [
    {"name": "echo_string", "method_type": "unary", "has_return": true,
     "has_header": false, "is_exchange": null,
     "params_schema_b64": "...", "result_schema_b64": "...",
     "header_schema_b64": null}
  ]
}}
```

- `server_id` and `request_version` come from the **listing** hop. Server
  identity is a property of the server, so the per-protocol description
  deliberately does not carry it: two processes serving the same protocol must
  describe it identically, or the description is not a property of the protocol.
- `describe_version` is the introspection format version, currently the string
  `"5"`. A test asserts equality with the reference's `DESCRIBE_VERSION`. It is
  vestigial — introspection is a protocol whose major version is part of its own
  name — and will not move again.
- `method_type` is `"unary"` or `"stream"`, lowercase.
- `is_exchange` is `true` for an exchange stream, `false` for a producer,
  **`null`** for a unary method or when the server genuinely cannot say.
- `methods` is an array; the harness keys it by `name`. Order is irrelevant.
- A schema field may be `null` or omitted for "absent"; the harness reads that
  as the empty schema.

A driver whose client library exposes a decoded description may relay that
directly instead of walking reflection by hand. Either is conforming.

### 4.4 `stream_open`

```json
{"op": "stream_open", "request_b64": "<IPC stream>",
 "is_exchange": false, "has_header": true}
```

Open a producer or exchange stream for the method named in the request batch's
metadata.

- **`is_exchange` is authoritative.** The harness derives it from the
  protocol declaration. A driver must not infer the stream kind from the method
  name; a name-prefix heuristic is a fixture-specific accident that will not
  survive the next method added to the suite.
- `has_header` tells the client whether to expect a header payload before the
  first data batch.

Success:

```json
{"ok": true, "header_b64": "<IPC stream>", "logs": []}
```

`header_b64` is `null` when the method declares no header. If the server raised
while opening, answer with the structured error
(`{"ok": true, "error": {...}, "logs": [...]}`) and **do not** leave a stream
open.

After a successful `stream_open` the connection is in *stream state* until a
terminal event (§5).

### 4.5 `tick`

```json
{"op": "tick"}
{"op": "tick", "input_b64": "<IPC stream>"}
```

Pull the next batch from a producer stream. When `input_b64` is present, the
stream carries an **empty batch whose custom metadata** is the per-tick metadata
to send upstream: read the metadata, **ignore the batch**, pass the metadata to
the client's tick.

```json
{"ok": true, "done": false, "batch_b64": "<IPC stream>", "logs": [], "error": null}
{"ok": true, "done": true,  "batch_b64": null, "logs": [], "error": null}
```

`done: true` is end-of-stream. An error terminates the stream too, and is
reported as `{"ok": true, "done": true, "batch_b64": null, "error": {...}}`.

### 4.6 `next_with_token`

```json
{"op": "next_with_token"}
```

Like `tick`, plus the opaque resume token for the batch just returned:

```json
{"ok": true, "done": false, "batch_b64": "...", "token": "<opaque>", "logs": [], "error": null}
```

`token` is `null` on transports that carry no resumable stream state — every
byte-stream transport. Over HTTP it is the continuation token the client would
present to resume. Producer streams only.

### 4.7 `exchange`

```json
{"op": "exchange", "input_b64": "<IPC stream>"}
```

Send one batch (with its custom metadata) and return the reply, same response
shape as `tick`. A `done: true` reply to an exchange is a protocol violation by
the *server* and the harness reports it as such; the driver still relays it
faithfully.

### 4.8 `cancel`

```json
{"op": "cancel"}
```

Cancel the stream early. Respond `{"ok": true, "logs": [...]}`. Cancellation is
terminal: the stream is over afterwards. A `cancel` with no stream open is a
successful no-op.

### 4.9 `close`

```json
{"op": "close"}
```

Release the stream without cancelling it (the harness is done reading).
Respond `{"ok": true}`. Terminal. A `close` with no stream open is a successful
no-op.

### 4.10 HTTP-only ops

All seven require an `http` connection; on any other transport answer
`{"ok": false, "error": "op requires http transport"}`.

`capabilities` and `request_upload_urls` arrive on connections that make no RPC
call at all — the harness spawns a driver, connects, asks, and shuts down. A
driver that defers connection setup until the first RPC will fail them.

**`capabilities`** — what the server advertises on `OPTIONS {prefix}/health`:

```json
{"ok": true, "caps": {
  "sticky_enabled": true, "sticky_default_ttl": 300,
  "sticky_echo_headers": ["Backend"], "upload_url_support": true,
  "max_request_bytes": 1048576, "max_response_bytes": 8388608,
  "max_externalized_response_bytes": null, "externalization_enabled": true,
  "max_upload_bytes": 16777216, "supported_encodings": ["zstd", "gzip"]}}
```

Byte caps and the TTL are integers or `null`. `supported_encodings` uses the
lowercase wire tokens (`zstd`, `gzip`, `identity`) — not your enum's spelling.

**`request_upload_urls`** — `{"op": "request_upload_urls", "count": 1}`:

```json
{"ok": true, "urls": [{"upload_url": "...", "download_url": "...", "expires_at": 1767225600}]}
```

`expires_at` should be integer Unix seconds; an ISO-8601 string is also
accepted, and `null` is tolerated. No test asserts on its value.

**`session_begin`** — `{"op": "session_begin", "token": null}` opens a sticky
session scope. A non-null `token` resumes an existing session instead of letting
the server mint one. `{"ok": true}`. Every subsequent call on this connection
carries the session headers until `session_end`. A `null` or absent token means
"mint"; treat an empty string the same way.

**Scopes nest, and the ops are stack-disciplined.** A `session_begin` arriving
while a scope is already open **pushes** a new one; `session_end` **pops** and
restores the enclosing scope rather than clearing the connection. `session_token`,
`session_echo_headers`, `session_detach` and the outgoing header application all
act on the **top** of the stack. A `session_end` with an empty stack is a no-op,
not an error. Closing the connection drains the whole stack innermost-out.

Hold the session state as a stack, not a slot. A single slot passes every test
that opens one session, and `TestSticky::test_drain_rejects_new_opens` opens an
inner scope inside an outer one and keeps using the **outer** one after the
inner ends — so a slot implementation loses the outer session at the inner
`session_end`, and the failure lands *after* the nested block, on a line that
never mentions a session. Four ports built a slot first and four ports hit that,
which is why this paragraph exists rather than being inferable.

There is no scope identifier, deliberately: the ops are connection-scoped, so
"which scope is this request in" has exactly one expressible answer, and the
stack is what makes that answer well-defined. A driver that wants explicit
handles is solving a problem the nesting discipline already settles.

**`session_token`** — `{"ok": true, "token": "<opaque>"}` or `token: null` when
the server minted none.

**`session_echo_headers`** — `{"ok": true, "headers": {"Backend": "b7"}}`, the
`VGI-Echo-*` values the client captured from the session-opening response.
Empty object when there are none.

**`session_detach`** — `{"ok": true, "token": "<opaque>"}`. Detaches: the
session outlives this connection and `session_end` must **not** delete it.

**`session_end`** — `{"ok": true}`. Pops the innermost scope and restores the
enclosing one; best-effort `DELETE` for the popped scope only, and not at all if
it was detached. An empty stack is a no-op.

### 4.11 `shutdown`

```json
{"op": "shutdown"}
```

The driver must, in order:

1. terminate any stream still open;
2. close the connection to the server, releasing its transport (kill the child
   process for `stdio`/`shm`, close the socket, dispose the HTTP client);
3. write `{"ok": true}`;
4. exit with status 0.

Closing the connection is part of the contract, not an optimisation: the
harness runs thousands of connections, and a driver that leaks a subprocess or a
socket per connection exhausts the runner rather than failing a test.

The driver must also treat **EOF on stdin as `shutdown`** — same teardown, no
response — because the harness kills a driver that does not exit within five
seconds of `shutdown`.

---

## 5. Stream state, flat and nested

After `stream_open` succeeds, the connection is in stream state. It leaves
stream state on the first **terminal** event:

- a response with `done: true`,
- a response with a non-null `error`,
- `cancel`,
- `close`.

**The harness never sends a stream op after a terminal event**, and never
interleaves a non-stream op with an open stream. That guarantee is what makes
two quite different driver designs both legal:

- **Flat** — one dispatch loop; stream ops read mutable session fields. Simple,
  and admin ops during an open stream would work (the harness just never sends
  them).
- **Nested** — `stream_open` enters a sub-loop that owns stdin until the stream
  terminates, then returns to the main loop. Structurally prevents a second
  stream and mis-sequenced ops, at the cost of not answering non-stream ops
  mid-stream.

Pick either. Do not rely on the harness's forbearance in a *driver* that also
serves other purposes.

Only **one stream at a time** per connection. `stream_open` while a stream is
open is undefined; a flat driver should answer `{"ok": false, ...}` rather than
silently leaking the first stream.

---

## 6. What must *not* live in the driver

The driver is a relay. The Python side owns value marshaling; the client under
test owns everything on the wire. In particular a driver must not:

- **decode or re-encode values.** Batches cross the boundary as IPC bytes. The
  driver reads exactly one batch and its metadata, hands them to the client, and
  re-serialises whatever comes back.
- **resolve external-location pointers.** That is the client's job and is
  exactly what the `external: true` flag tests. Resolving in the driver (or on
  the Python side) makes the test pass without the client ever doing the work.
- **invent routing.** No defaulted `protocol`, no defaulted method name, no
  method-name heuristic for `is_exchange`.
- **retry.** A retry the client did not perform is a passing test for behaviour
  the client does not have.
- **normalise errors.** `error_type` is asserted verbatim.

`describe` is the single deliberate exception to "no decoding", for the reason
given in §4.3.

---

## 7. Implementing and wiring one up

The Python side is already written:

```python
from vgi_rpc.conformance.client_driver import ClientDriver

driver = ClientDriver.from_env(default=["./target/debug/my-client-driver"])
driver.install_http_overrides()        # route http_connect & friends

proxy = driver.connect("stdio", ["./my-conformance-worker"])
assert proxy.echo_string(value="hi") == "hi"
proxy.close()
```

- `ClientDriver.from_env` reads **`VGI_CLIENT_DRIVER`**, which every CI leg
  sets, and splits it with `shlex.split` — so an interpreted driver
  (`bun run driver.ts`, `java -jar driver.jar`) needs no wrapper script.
- `install_http_overrides()` re-binds `vgi_rpc.http.http_connect`,
  `http_capabilities` and `request_upload_urls`. The HTTP feature tests
  (external location, sticky sessions, response caps) import those *inside the
  test body*; without the override they quietly exercise the Python client and
  prove nothing about the port.
- `ClientDriverProxy` exposes the service's methods by name, plus `describe()`,
  `with_session_token()` and `admin()`. `DriverStreamSession` mirrors
  `StreamSession` (`tick`, `next_with_token`, `exchange`, `cancel`, `close`,
  iteration, context manager).

Bring a port up in this order; each step is a real gate:

1. `connect` + `unary` against the **Python** conformance server over `stdio`.
   Most routing-key defects surface here, in the first call.
2. `stream_open` + `tick` + `close`; then `exchange`; then `cancel`.
3. `describe`.
4. `http` transport, then `capabilities`, sticky sessions, upload URLs,
   external location.
5. The remaining transports (`unix`, `tcp`, `shm`).

Then run the whole suite against the Python server. That, and not a run against
your own server, is the gate.

---

## 8. Known divergences between the two existing drivers

Both were written independently against no written spec. Where they disagreed,
this document picked one; the other is tolerated by the Python shim but should
not be copied.

| Point | Rust | C# | Specified |
|---|---|---|---|
| `stream_open` failure | `{"ok": false, "error": {...}}` | `{"ok": true, "error": {...}}` | **C#.** The driver did carry out the op; the server refused. `ok: false` mixes the channels and hands the shim an object where it documents a string. |
| Stream dispatch | nested sub-loop | flat | **Either** (§5), because the harness guarantees sequencing. |
| `shutdown` | replies and exits; connection dropped implicitly | tears down stream, disposes client, replies, exits | **C#.** Explicit teardown (§4.11). |
| `is_exchange` | uses the flag | flag **or** `method.startsWith("exchange_")` / `== "cancellable_exchange"` | **Rust.** The name heuristic is fixture-shaped and must go. |
| `protocol` absent on `connect` | passes `""` through | defaults to `"ConformanceService"` | **Neither.** `protocol` is required; a driver that defaults it hides the §0 bug class. |
| `describe` schemas | re-encodes the client's decoded schema | relays the server's `*_schema_ipc` bytes | **Either.** Only the schema message is read. Relaying the server's bytes has one fewer place to disagree. |
| `describe_version` | from the client's description | hardcoded `"5"` | **Either**, as long as it equals the reference's `DESCRIBE_VERSION`. |
| `expires_at` | passes the client's type through | integer Unix seconds | **Integer Unix seconds** preferred; ISO-8601 and `null` tolerated. |
| Non-RPC driver exception | n/a (everything is an `RpcError`) | `{"ok": false, "error": "<ToString()>"}` | `ok: false` is correct for a genuine driver failure — but a *transport* error the client library reports is a call error (§2), not a driver failure. |
| `relax_nullability` on `connect` | read | ignored | **Not part of the protocol.** The reference shim never sends it. |

Two defects found in the shared Python side while writing this down, now fixed
in `vgi_rpc.conformance.client_driver`:

- `describe`'s error path read `message` where both drivers write
  `error_message`, so a failed `describe` surfaced with an empty message. The
  shared shim reads `error_message` and falls back to `message`.
- A `{"ok": false, "error": "<string>"}` answer to `stream_open` crashed the
  shim with a `TypeError` (it indexed the string as an object) instead of
  raising a transport error. The shared shim accepts both shapes.

---

## 9. Not covered

- **URL prefix.** `http_connect(prefix=...)` has no counterpart here: `connect`
  carries a whole URL. A server mounted under a prefix cannot currently be
  driven. No conformance test needs one.
- **`ipc_validation`, `retry`, `accepted_max_response_bytes`.** Accepted by the
  Python stand-ins for signature parity and ignored; the suite passes none of
  them. If a test ever does, they need control-protocol fields.
- **Concurrency.** One connection, one stream, strict lockstep. Nothing here
  exercises a client's thread-safety.
