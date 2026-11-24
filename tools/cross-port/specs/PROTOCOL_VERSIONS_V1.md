# Protocol version negotiation, v1 (design + spec)

Status: **normative for every vgi-rpc SDK, every vgi SDK and the extension.**
Approved by Rusty (the maintainer) on 2026-11-24 for the 3.4 patch releases.
The Python reference (vgi-rpc-python and vgi-python, `protocol-3.4`) implements
it first; decisions it settled are at the end of this document. The wire
summary is in `docs/WIRE_PROTOCOL.md` §13 ("Protocol version negotiation").

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Problem

Every request carries `vgi_rpc.protocol_version` (e.g. `3.4.0`) for the
protocol it is addressed to. Before this spec every server refused anything
that did not match on major.minor exactly. So a minor bump broke every pairing
at once, in both directions. In hosted Orchard, a deployment stays pinned to a
worker version while users' extensions update, so every future protocol minor
would break every hosted deployment until it is redeployed.

## 2. Goals

- From 3.4 on, any client can learn which versions a server speaks before or
  after one request, on every transport.
- Future extensions can support a window of versions (policy: the current minor
  and the previous one, `N-1..N`). They talk to an older worker by speaking that
  worker's version, translated at the edge. Old workers need no change.
- Most protocol changes become additive (capabilities + optional fields) and
  need no version bump at all.
- 3.4 behaviour is unchanged for 3.4 ↔ 3.4. This release only adds
  advertisement, and the client-side machinery to use it later.

Non-goals for 3.4: actually supporting 3.3, and building translators (they come
with the first breaking change after 3.4).

## 3. Terms

- **Protocol:** each separately versioned protocol has a name and a semver. The
  name is its wire routing key (`vgi_rpc.protocol`), exactly as the extension's
  generated `vgi_protocol_names.hpp` and the vgi-python `protocol_name` ClassVars
  spell it. The ones a VGI worker serves today:

  | Name | Version (3.4 wave) | Source |
  |---|---|---|
  | `vgi.v3` | `3.4.0` | `vgi.protocol.VgiProtocol` / `VGI_PROTOCOL_NAME` |
  | `vgi.secret.v1` | `1.0.0` | `vgi.secret_protocol.SecretProtocol` / `VGI_SECRET_PROTOCOL_NAME` |
  | `vgi.attach_tickets.v1` | `2.0.0` | `vgi.attach_ticket` (hosted mode) |
  | `vgi.reports.v1`, `vgi.schedules.v2`, ... | per protocol | `vgi.reporting.*`, when hosted |

  Framework protocols follow the same rule: `vgi_rpc.Reflection.v1` and
  `vgi_rpc.Identity.v2` declare no `protocol_version` (their major is in the
  name) and reflection is also exempt from the gate, so neither appears. An application protocol that
  declares no `protocol_version` is not gated and is not advertised.

  Only major.minor is negotiated; patch is informational.
- **Range / window:** an inclusive `MIN-MAX` of major.minor. Majors are never
  mixed in one range in practice, but the syntax allows it.

## 4. Wire format

### 4.0 Syntax

```
versions  = [ entry *( OWS "," OWS entry ) ]
entry     = name "=" range
range     = mm [ "-" mm ]           ; a single mm means mm-mm
mm        = 1*DIGIT "." 1*DIGIT    ; no leading zeros except "0"
name      = 1*( ALPHA / DIGIT / "_" / "." / ":" / "/" / "-" )
```

Writers MUST emit both ends (`3.4-3.4`) and SHOULD list protocols in hosting
order (primary first). Readers MUST NOT depend on order, MUST accept whitespace
around entries, and SHOULD skip an entry they cannot parse rather than reject
the whole value (a skipped protocol is treated as unadvertised — §6's fallback).
A reader MAY accept `MAJOR.MINOR.PATCH` ends and drop the patch.

### 4.1 Server advertisement

**HTTP:** a response header on EVERY response, including 400, 401, 403, 404,
413, 415, 5xx, `HEAD /health`, `OPTIONS /health` and CORS preflights:

```
VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0
```

It lists only the protocols the server actually gates (§3). A server that gates
nothing sends the header with an empty value: present-but-empty means "speaks
negotiation, gates nothing", absent means "released before this spec". Hosted
and gateway 401s carry it alongside the other `VGI-*` capability headers. It is
added to CORS `Access-Control-Expose-Headers` wherever the other `VGI-*`
headers are exposed, and a preflight MUST allow the request header
`VGI-Client-Protocol-Versions`.

**Every transport** (pipe / stdio / launch / unix / tcp / named pipe, and HTTP
too): the same string rides as the custom-metadata key
`vgi_rpc.protocol_versions` on

- every reflection result batch (`vgi_rpc.Reflection.v1` `list_protocols` and
  `describe`) — present even when empty;
- the `__transport_options__` reply;
- every `protocol_version_unsupported` error batch (§4.3).

Reflection is answerable by a client of ANY version: it is exempt from the gate
(it was already; the exemption is now normative) and a reflection request that
carries an out-of-range `vgi_rpc.protocol_version` MUST succeed.

### 4.2 Client request

- `vgi_rpc.protocol_version` keeps its meaning: the version the client is
  speaking for this request. Its value is the negotiated major.minor plus the
  client's own patch (`3.4.2` from a `3.5.2` client that negotiated 3.4).
- New, informational: request metadata `vgi_rpc.protocol_versions_client`
  (and, on HTTP, the request header `VGI-Client-Protocol-Versions`) carrying
  the client's own window for the protocol it is calling, same syntax
  (`vgi.v3=3.4-3.5`). A client MAY list several protocols. Servers log it at
  debug level and quote it in refusal messages. They MUST NOT reject because of
  it, including when it is malformed.

### 4.3 Refusal

When a request's version is absent, malformed, or not inside the server's
window for that protocol:

- the error kind is `protocol_version_unsupported` with code
  `FAILED_PRECONDITION` (HTTP 400, or the transport's ordinary error stream);
- the message names both sides and which to upgrade, and keeps the substring
  `too old` / `too new` for log-grep continuity, e.g.

  ```
  protocol_version_unsupported: client speaks vgi.v3 3.6 (supports 3.5-3.6); this server supports
  vgi.v3 3.4-3.4. The server is too old (the client is too new for it). Upgrade the worker, or use
  a client that supports vgi.v3 3.4.
  ```

  The `(supports …)` clause appears when the client sent
  `vgi_rpc.protocol_versions_client` for that protocol;
- machine-readable, in three places:
  1. `error_details` carries a `vgi_rpc.PreconditionFailure` with one violation
     `{type: "protocol_version", subject: <protocol>}` (unchanged) **and** a
     `vgi_rpc.ErrorInfo` whose `metadata` has `vgi_rpc.protocol_versions` (the
     server's full advertisement) and, when the client sent one,
     `vgi_rpc.protocol_versions_client`;
  2. the error batch's custom metadata carries `vgi_rpc.protocol_versions`;
  3. on HTTP, the response carries the §4.1 header.

The pre-negotiation kind `protocol_version_mismatch` and its "client is too
old" / "server is too old" texts are replaced by this. Clients MUST still
recognise `protocol_version_mismatch` from older servers (§6).

## 5. Server rules (all SDKs)

- Advertise per §4.1 on every response and in describe.
- Accept a request whose major.minor is within the advertised window for its
  protocol. In 3.4 every SDK advertises exactly `3.4-3.4` (window = the declared
  `protocol_version`'s major.minor), so behaviour is unchanged.
- An SDK MAY serve a wider window later. The policy is that the extension carries
  the window, so SDKs usually don't need to. The Python reference lets a Protocol
  declare `protocol_version_range` (whose `MAX` must equal `protocol_version`'s
  major.minor) and a server override it per protocol
  (`RpcServer(protocol_version_ranges={"vgi.v3": "3.3-3.4"})`); other SDKs SHOULD
  offer the override, because it is how fixture workers impersonate an older
  server (`VGI_TEST_PROTOCOL_RANGE`, §9).
- Refuse per §4.3.

## 6. Client rules (the extension, and every vgi-rpc client library)

- **Negotiation state per endpoint** (one worker LOCATION / connection pool /
  connection): the windows learned from the server, the chosen version per
  protocol, and the capability headers. Shared by every protocol bound on that
  endpoint.
- **Choosing**, per protocol:
  1. the server advertised a window for it → the highest common major.minor; if
     there is none, fail without sending (below);
  2. else a pre-spec refusal named a single server version → that version;
  3. else → the client's own newest version (today's behaviour).
- **HTTP:**
  - Before the first response is seen, send the client's max version.
  - Read `VGI-Protocol-Versions` from every response and cache it per endpoint;
    a present header replaces what was cached (a redeploy behind one URL).
  - On `protocol_version_unsupported` (or a 400 carrying the header), learn from
    it, choose again, and retry the request ONCE — only for a request that is
    idempotent (reflection, or a method declared `no_side_effects` /
    `idempotent`). This is safe because the first request at ATTACH is
    idempotent (describe/catalog). Never retry a non-idempotent request
    automatically: surface the error. The next call then speaks the learned
    version.
- **Pipe / launch / unix / tcp:** read the windows from reflection before any
  other call. A client whose own window is a single version has nothing to
  choose and MAY skip the probe (the Python reference does), relying on the
  refusal.
- **No common version:** fail (ATTACH, for the extension) with a message naming
  both windows and which side to upgrade. When the refusal that revealed it is
  in hand, surfacing that refusal satisfies this; once learned, later calls fail
  before sending.
- **Missing header** (servers released before this spec, e.g. the 3.4.0 initial
  SDK releases): treat the server as supporting exactly the version in its
  refusal message if one is present (`Server: X.Y.Z` in the message, or
  `server requires X.Y.Z` in the precondition description), otherwise exactly
  the client's own current version.
- The client advertises its own windows per §4.2.

## 7. Extension architecture

1. **`ProtocolSession`** per worker endpoint, created at ATTACH and cached with
   the connection pool and catalog. It holds the negotiated versions and the
   capability set (`VGI-Upload-URL-Support`, `VGI-Arguments-Ref-Bytes`,
   `VGI-Max-Request-Bytes`, `VGI-Max-Response-Bytes`, `VGI-Supported-Encodings`,
   externalization...).
   - Every version- or capability-dependent decision in the extension goes
     through it, e.g. `session.Supports(Feature::ArgumentsRef)`,
     `session.UploadUrlVersion()`, `session.Version(Protocol::Vgi)`.
   - Scattered header reads and checks move behind it.
2. **Codec boundary:** requests and responses pass through a per-version codec
   interface at the RPC boundary. For 3.4 the only codec is the identity, but
   the seam exists. The core only ever sees current-version types. A future
   codec translates older-version messages up and down. Codecs are registered
   by (protocol, major.minor), and removing a version deletes its codec.
3. **Supported window:** a generated constant per protocol, `MIN..MAX`, which
   for 3.4 is `3.4..3.4` (and `1.0..1.0` for `vgi.secret.v1`). It feeds §4.2
   and negotiation. vgi-python codegen emits it into `vgi_protocol_version.hpp`
   / `vgi_secret_protocol_version.hpp` as `VGI_PROTOCOL_VERSION_MIN` /
   `VGI_PROTOCOL_VERSION_MAX` (`"3.4"`) and `VGI_PROTOCOL_VERSIONS_CLIENT`
   (`"vgi.v3=3.4-3.4"`, the ready-made §4.2 value), and the secret-protocol
   equivalents.

## 8. Generator and compatibility check (vgi-python codegen)

- Check in a schema snapshot per protocol version (`schemas/<protocol>/<major.minor>/`),
  generated from the current types at release time.
- A CI check compares the current types to the latest snapshot. Additive
  changes (new optional or nullable fields, appended fields, new methods, new
  capability headers) pass. Anything else (removed or renamed fields, type
  changes, new required fields, reordering where order matters) fails unless the
  protocol version was bumped.
- The generated supported-window constants come from the same source.

## 9. Conformance (shared; vgi-rpc-python reference)

Module: `vgi_rpc/conformance/_protocol_versions_pytest.py`, collected by
`_pytest_suite.py` (every port runner) and by `vgi-rpc-test-hosted` (server
groups only).

| Group | Asserts | Runner hooks |
|---|---|---|
| `TestVersionsHeader` | header present + parseable + names exactly the gated protocols on a 200, a 404, `HEAD /health`, `OPTIONS /health`; a malformed client header is never refused | `conformance_http_port`; optional `conformance_http_prefix` |
| `TestVersionsHeaderOnRejections` | header on a 401 and a 413 | optional `conformance_http_auth_port`, `conformance_http_small_request_cap_port` |
| `TestVersionsHeaderOn5xx` | header on a 5xx | optional `conformance_http_serve_start_fail_once_port` |
| `TestVersionsHeaderIsCorsExposed` | header present and in `Access-Control-Expose-Headers` on a preflight and a real response; preflight allows `VGI-Client-Protocol-Versions` | optional `conformance_http_cors_port` |
| `TestDescribeAdvertisesVersions` | `list_protocols` and `describe` result batches carry `vgi_rpc.protocol_versions` on every transport; reflection answers a 99.0.0 request | `protocol_target` (`conformance_conn` + `conformance_protocol_connector`); byte-stream proxies must expose `_transport` |
| `TestOutOfRangeRefusal` | too-new, too-old and missing versions get `protocol_version_unsupported`/`FAILED_PRECONDITION`; message names both windows and `too old`/`too new`; `ErrorInfo` + error-batch metadata + (HTTP) 400 and header | as above, plus `conformance_http_port` for HTTP |
| `TestClientNegotiation` | against a scripted reference server: max before any response; `3.4-3.4` server + `3.4-3.5` client → 3.4 (plus the client's patch) after the first response; disjoint → failure naming both windows, then no further request; missing header → own version; legacy refusal → the version it names | optional `conformance_negotiating_client` |

`conformance_negotiating_client` is `factory(base_url, protocol, version,
version_range)` returning a context manager that yields an object with `ping()`:
a client of the port under test, bound to `conformance.Negotiated.v1`, declaring
`version` as its newest and supporting `version_range`, negotiating per §6.
`ping()` is a no-argument unary call returning utf8 and is not declared
idempotent. A port may implement it in-process or through a helper binary; the
client-driver protocol is not used, because its harness stamps
`vgi_rpc.protocol_version` itself.

Extension `.test`: attach and a query work. A fake/older-version worker (fixture
env switch `VGI_TEST_PROTOCOL_RANGE=3.3-3.3`, honoured by the vgi-python fixture
workers: it overrides the `vgi.v3` window) produces the §6 error at ATTACH with
both windows named.

## 10. Rollout

In the 3.4 patch wave with the timeout and direct-S3 fixes:

1. vgi-rpc-python + vgi-python (reference + conformance);
2. the other vgi-rpc ports;
3. the vgi SDKs (only where they own the gate or describe);
4. the extension (`ProtocolSession`, negotiation, codec seam).

The fleet re-release then carries the header, so every 3.4 worker in the field
can be identified by future extensions.

---

## Decisions made in the Python reference

Things the design left open, and how the reference settled them. Ports follow
these unless the maintainer overrides.

1. **Names.** The design's example `vgi=3.4-3.4, vgi.secret=1.0-1.0` used
   illustrative names; the wire uses the real routing keys (`vgi.v3`,
   `vgi.secret.v1`, `vgi.attach_tickets.v1`, ...), the same strings as
   `vgi_rpc.protocol` and `vgi_protocol_names.hpp`.
2. **Which protocols are listed.** Exactly the gated ones: those declaring a
   `protocol_version`, minus version-exempt bindings (reflection). Unversioned
   protocols (e.g. `conformance.Secondary.v1`, `vgi_rpc.Identity.v2`) are not
   listed.
3. **Empty advertisement.** A server gating nothing still sends
   `VGI-Protocol-Versions:` with an empty value, and `vgi_rpc.protocol_versions`
   = `""` on reflection results and transport-options replies, so "negotiates,
   gates nothing" is distinguishable from "pre-spec".
4. **Where "describe" carries it.** `vgi_rpc.Reflection.v1` replaced
   `__describe__`, so the key rides on the result batch of both reflection
   methods (`list_protocols` and `describe`). It does not change the
   `ProtocolList`/`ServiceDescription` payloads, so **the describe golden
   (`tests/golden/describe_conformance.txt`) and the protocol-hash vector
   (`tests/golden/protocol_hash_vector.json`) are unchanged.** It is also added
   to the `__transport_options__` reply, which every byte-stream server already
   answers.
5. **Error kind and code.** `protocol_version_unsupported` keeps the code the
   old kind had, `FAILED_PRECONDITION`, and keeps the `PreconditionFailure`
   detail. The window travels in an added `vgi_rpc.ErrorInfo`, the one detail
   type for string metadata, plus the error-batch metadata key. The shared
   conformance class `TestVersionMismatchCode` keeps its name (runners import it
   by name) but now asserts the new kind.
6. **Message format.** `client speaks <p> <M.m>[ (supports <range>)]; this
   server supports <p> <range>. The {client|server} is too old (the {server|client}
   is too new for it). Upgrade the {client|worker}, or use a {worker|client} that
   supports <p> <M.m>.` Both `too old` and `too new` appear in every directional
   refusal. Absent / non-UTF-8 / malformed versions get their own sentences, all
   starting `protocol_version_unsupported:` and naming the server's window.
7. **Range declaration.** `protocol_version` stays the newest version spoken (it
   is what the generated code and the request metadata use); an optional
   `protocol_version_range = "MIN-MAX"` widens the window downward and MUST end
   at `protocol_version`'s major.minor. Servers additionally accept a per-protocol
   override (`protocol_version_ranges=`), unconstrained, for fixtures.
8. **Patch on a negotiated version.** The client's own patch is sent with
   whatever major.minor is negotiated (`3.5.2` → `3.4.2`).
9. **Which client calls retry.** Reflection methods, and methods whose
   description declares `idempotency` `no_side_effects` or `idempotent`. Python
   Protocols cannot yet declare idempotency, so in practice only reflection is
   retried; every other refused call surfaces the refusal and the next call uses
   the learned version. Stream inits are never retried.
10. **No-overlap surfacing.** On the call whose refusal reveals there is no
    common version, the client surfaces the server's refusal (it already names
    both windows); later calls raise the client-side error
    (`NoCommonVersionError`, a `ProtocolVersionError` with kind
    `protocol_version_unsupported`) without sending.
11. **Byte-stream probe.** The pipe/unix/tcp client probes `list_protocols`
    before its first call only when its own window is wider than one version; a
    single-version client sends its version as before (no extra round trip, so
    3.4 ↔ 3.4 traffic is byte-for-byte unchanged apart from the new
    informational key). A server without reflection leaves the endpoint
    unlearned.
12. **Legacy parsing.** A pre-spec refusal is recognised by kind
    `protocol_version_mismatch` and read with
    `(?:Server:\s*|server requires\s+)(\d+\.\d+)` over the message and the
    precondition descriptions.
13. **Unparseable advertisement.** Entries that do not parse are skipped, not
    fatal; a repeated name keeps its first window.
14. **The client header on HTTP and the metadata key.** Both are sent; the
    server reads the metadata key (canonical on every transport) for logging and
    refusal text.
15. **Retry trigger.** The HTTP client keys the retry on the decoded
    `protocol_version_unsupported` / `protocol_version_mismatch` kind (which a
    400 refusal always carries), after caching that response's header.
