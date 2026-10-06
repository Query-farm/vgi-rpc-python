# Multi-protocol hosting and the error model — normative port contract

Reference implementation:

- `vgi_rpc/errors.py` — `Code`, the detail catalog, `StatusError`, the 4 KiB cap, `is_retryable`, `AuthUnavailableError`
- `vgi_rpc/log.py` — `Message.from_exception(exc, include_traceback=...)` writes the three layers
- `vgi_rpc/rpc/_common.py` — `RpcError.error_code` / `error_kind` / `error_details`, typed accessors, `is_retryable()`
- `vgi_rpc/rpc/_wire.py` — `_error_model_fields`, the single client decode point every path funnels through
- `vgi_rpc/rpc/_server.py` — `RpcServer(include_tracebacks=True)`, read-only `bindings` (the hosted set is sealed at construction)
- `vgi_rpc/rpc/_token_identity.py` — `IdentityImpl` translation of `AuthUnavailableError`
- `vgi_rpc/conformance/secondary.py` — the fixture protocol
- `vgi_rpc/conformance/_secondary_pytest.py`, `_hosted_pytest.py`, `hosted_protocols.py` — the groups
- tests: `tests/test_error_model.py`, `tests/test_hosted_protocols.py`

Normative text lives in `docs/WIRE_PROTOCOL.md` §3.1 ("Hosting several
application protocols"), §8 ("Error model", "Tracebacks"), §14 (`features`)
and §16 (retry hint, translation). This file records *why*, pins the fixture,
gives the test vectors, and lists what each port and SDK must deliver. Where
this file and your intuition disagree, this file wins.

**Status 2026-10-05, revision 2** (after all six ports and the SDKs implemented
revision 1; their findings are folded in and listed in §10). Phase 0 (spec) and
Phase 1 (Python reference + shared suite) landed together. Phase 2 (six ports) and Phase 3 (seven SDKs) follow
the vgi-rpc release.

---

## 1. Decisions, with reasons

### D1. The protocol is the unit of optionality

No method-subset API for application protocols, and no feature tokens.
Reflection's `features` is **reserved and emitted as `[]`**; clients ignore it.

*Why.* Every optionality mechanism the ports could invent — a narrowed method
set, a capability token — is a second answer to a question reflection already
answers: "is protocol X hosted?". Two answers drift. A capability that may be
absent becomes its own protocol, versioned and hashed on its own, and a client
discovers it with the `list_protocols` call it already makes. `features` stays
in the schema (removing a column is a breaking change) but carries nothing, so
a future token cannot change what a current client does.

`vgi_rpc.Identity.v1` keeps its hook-driven narrowing, untouched. It is
framework-owned, its narrowed hashes are pinned in `IDENTITY_V1_SPEC.md`, and
the narrowing exists to keep an oracle *absent* — a property, not a feature.

### D2. A list of `(protocol, implementation)` pairs at construction, fixed for the process

The application passes any number of pairs when the server is built. The list
may be computed from configuration or environment; after construction it does
not change, and the same list is hosted on every transport the server serves.

*Why fixed.* Reflection output and every `protocol_hash` are then stable for
the process lifetime, so a client may cache them for a connection, and an
access-log record's `protocol_hash` means the same thing at 09:00 and 17:00.
*Why every transport.* A protocol reachable over stdio but not HTTP is two
servers wearing one name; nothing in reflection can express it.

**Order is registration order, primary first** — among application protocols.
Framework protocols (`vgi_rpc.*`) may sit anywhere (the reference lists them
last); clients filter the reserved prefix out and take what remains in order.
That is what "describe this server" already did in every client and in the
client driver.

**The reserved prefix applies to every registered name however derived.**
Rust checked only the declared name, C# only `[ProtocolName]`-declared ones,
C++ none. A name synthesised from an interface called `vgi_rpc_Reflection_v1`
is still a name.

### D3. Identity stays framework-owned

Hosted through `resolve_token` / `mint_grant` hooks, not through the
application's list, on transports that authenticate callers. Two rules are new
(WIRE_PROTOCOL §16):

- `identity_unavailable` **MUST carry `vgi_rpc.RetryInfo`**. `retry_after`
  existed on the error in all seven ports and reached the wire in none.
- A hook raising the transport-auth "unavailable" error (`AuthUnavailableError`)
  **MUST** be emitted as `identity_unavailable` with that error's retry hint.
  TypeScript and Rust did this; Python, Go, C#, Java and C++ sent it
  unclassified — and vgi-python's own docs tell workers to raise it.

In the reference `AuthUnavailableError` moved from `vgi_rpc.http._unauthorized`
to `vgi_rpc.errors` (still re-exported from `vgi_rpc.http`), because the
translation runs on every transport and `IdentityImpl` must not import the
HTTP package to catch it.

### D4. Errors take gRPC's shape

Closed code set (`vgi_rpc.error_code`, the code's **name**), the existing open
`vgi_rpc.error_kind` as the reason, and `vgi_rpc.error_details`, a JSON array
from a fixed catalog. Full rules in WIRE_PROTOCOL §8. The parts that fix what
went wrong in gRPC practice:

| gRPC failure | Fix here |
|---|---|
| Details in a binary trailer that clients, proxies and logs never decoded | Plain top-level metadata, mirrored in `log_extra`, `error_code` in the access log, conformance-tested in every client |
| Proxies silently truncating oversized trailers | 4 KiB cap; over it the **whole** array is dropped, never a prefix |
| `UNKNOWN` for everything | Every kind names its code; conformance pins the table |
| `DebugInfo` / stack traces to production callers | No `DebugInfo`; a per-server traceback setting. Included by default on every transport (§1 decision 4) — an operator turns it off |

*Why names, not numbers.* A log line, a proxy rule and a client switch all read
`"UNAVAILABLE"`; nobody has to carry gRPC's numbering table.

### D5. Rowfence routing is out of scope.

### D6. The cross-SDK checks live in this repo's suite

Every port's conformance worker and every SDK's fixture worker hosts the same
`conformance.Secondary.v1`, and one set of tests serves both.

### Decisions the plan left open, settled here

1. **The fixture method is `echo_string`, not `echo`.** The plan said `echo`
   "so it collides with the primary"; `ConformanceService` has no `echo`. A
   collision is the point, so the secondary takes `echo_string`, which the
   primary has with the identical signature `(value: utf8) -> utf8`. Its reply
   is prefixed with `secondary:` so a mis-route is a wrong value, not a
   coincidentally right one.
2. **The secondary declares no `protocol_version`** while the primary declares
   `2.0.0`. A client then sends no version on secondary calls, and a server
   that gates every call against the primary's version (instead of the
   resolved binding's) refuses them. That makes §13's per-binding gate
   observable for free.
3. **A code may be sent without a kind**; the code is required on *every*
   EXCEPTION batch a server implementing the model emits (`UNKNOWN` when
   unclassified). The client reports `error_code = ""` only for a server that
   predates the model — `""` and `"UNKNOWN"` are different answers.
4. **Tracebacks are included by default on every transport** (revision 2,
   user decision). Revision 1 omitted them on HTTP and TCP; the Java agent
   found that the DuckDB extension (`vgi/src/vgi_logging.cpp:290`) puts the
   remote traceback into the user-visible error, so omitting it hid chained
   causes and broke `nest_tensor.test:172`. The setting remains, per server
   (all transports at once), for an operator who wants traces kept in the
   process. A port without exception stacks (C++, Rust) sends a synthesized
   trace — at minimum `<ErrorType>: <message>` and the `<protocol>/<method>`
   that raised it; conformance asserts non-empty only.
5. **Clients keep unknown detail types in the reported array.** "Ignore"
   means typed accessors skip them and nothing fails; deleting them from what
   the client reports would hide a server's protocol-defined details from the
   application that knows them.
6. **The cap is 4096 bytes of UTF-8 of the value as emitted.** Ports differ in
   JSON whitespace; each measures what it sends. The fixture's payloads sit far
   from the boundary in both directions, so no conformance test depends on
   encoder spelling.
7. **An array breaking the uniqueness or catalog rules is dropped at emission
   too**, not just one over the cap. The reference's `StatusError` validates
   eagerly so the author sees the mistake; the emission check is the backstop.
8. **`ProtocolVersionError` carries `PreconditionFailure`** with one violation
   `{type: "protocol_version", subject: <protocol>, description: ...}`;
   **`server_draining` carries `RetryInfo`** (reference default 1 s) and
   **`ResponseTooLargeError` is `RESOURCE_EXHAUSTED`** with no `RetryInfo`
   (not retryable — the same call against the same limits fails again).
9. **The access-log `error_code` field is optional in the schema for one
   release**, enumerated and forbidden on `status: ok`. Making it required on
   day one would turn every port's access-log job red before Phase 2 starts;
   tighten it once all seven emit it.
10. **The new pytest groups skip loudly when the runner provides no
    `conformance_protocol_connector`** (§4), mirroring the identity group. A
    skip naming that fixture is an open Phase-2 deliverable, not a pass; the
    definition of done below requires zero of them.

---

## 2. The fixture protocol: `conformance.Secondary.v1`

Hosted by every conformance worker, registered **after** `ConformanceService`
through the port's hosting API (not special-cased), on every transport,
with reflection on.

```
protocol conformance.Secondary.v1          # no protocol_version
echo_string(value: utf8 non-null) -> utf8 non-null          # returns "secondary:" + value
fail(code: utf8 non-null, kind: utf8 non-null,
     retry_delay_seconds: float64 non-null) -> void          # always raises
fail_oversized() -> void                                     # always raises
```

All three unary, `has_header = false`; `fail` and `fail_oversized` have
`has_return = false`.

**protocol_hash: `58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6`**

Canonical preimage (diff against this if your digest differs):

```json
{"methods":[{"has_header":false,"has_return":true,"name":"echo_string","params":[{"name":"value","nullable":false,"type":"utf8"}],"result":[{"name":"result","nullable":false,"type":"utf8"}],"type":"unary"},{"has_header":false,"has_return":false,"name":"fail","params":[{"name":"code","nullable":false,"type":"utf8"},{"name":"kind","nullable":false,"type":"utf8"},{"name":"retry_delay_seconds","nullable":false,"type":"float64"}],"type":"unary"},{"has_header":false,"has_return":false,"name":"fail_oversized","params":[],"type":"unary"}],"protocol":"conformance.Secondary.v1"}
```

The likeliest wrong digests: `retry_delay_seconds` as `float32`, or a
`result` entry on a void method.

### `fail(code, kind, retry_delay_seconds)`

- If `code` is not one of the sixteen names: raise `INVALID_ARGUMENT`, kind
  `invalid_code`, details
  `[{"@type":"vgi_rpc.BadRequest","field_violations":[{"field":"code","description":"must be a canonical code name"}]}]`.
  (`OK` is not a code; the suite uses it as the probe.)
- Otherwise raise with `error_code = code`, `error_kind = kind` (**absent** when
  `kind == ""`), and details, in this order:
  1. `{"@type":"vgi_rpc.ErrorInfo","metadata":{"fixture":"conformance.Secondary.v1"}}`
  2. `{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":<delay>}` — **only when `delay > 0`**
  3. `{"@type":"conformance.Secondary.v1.Probe","note":"clients ignore detail types they do not know"}`

The message text is not asserted. The third detail is a legitimate
protocol-defined type (under the protocol's own name) that no client knows.

### `fail_oversized()`

Raise `RESOURCE_EXHAUSTED`, kind `details_oversized`, with details
`[RetryInfo{retry_delay_seconds: 1}, ErrorInfo{metadata: {"padding": "x" * 5000}}]`.
The array is over the cap, so the wire carries **no** `vgi_rpc.error_details`
and no `log_extra.error_details`. The small `RetryInfo` comes first on purpose:
a server dropping only the element that does not fit keeps it, and the error
then reads as retryable.

---

## 3. Error-model test vectors

Numbers compare by value (`7` == `7.0`). The reference emits compact JSON
(`separators=(",",":")`, UTF-8, not ASCII-escaped); yours need not match
byte-for-byte except where a size is asserted, which none of the fixture's
payloads makes close.

**V1 — `fail("UNAVAILABLE", "backend_down", 7)`**, top-level metadata:

```
vgi_rpc.error_code    = UNAVAILABLE
vgi_rpc.error_kind    = backend_down
vgi_rpc.error_details = [{"@type":"vgi_rpc.ErrorInfo","metadata":{"fixture":"conformance.Secondary.v1"}},{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":7},{"@type":"conformance.Secondary.v1.Probe","note":"clients ignore detail types they do not know"}]
                        (232 bytes in the reference)
```

`log_extra` contains `"error_code":"UNAVAILABLE"`, `"error_kind":"backend_down"`
and `"error_details": [ …same three objects… ]` as a JSON **array**, and — by
default, on every transport — a non-empty `traceback`.

Client: `error_code == "UNAVAILABLE"`, `error_kind == "backend_down"`,
`error_details` == the three objects in order, `retry_info().retry_delay_seconds
== 7`, `error_info().metadata == {"fixture": "conformance.Secondary.v1"}`,
`details()` yields `[ErrorInfo, RetryInfo]` (probe skipped), `is_retryable()`.

**V2 — `fail("ABORTED", "", 0)`**: code `ABORTED`, **no** `vgi_rpc.error_kind`
key (client reports `""`), details `[ErrorInfo, Probe]`, not retryable.

**V3 — retryability**

| code | `RetryInfo` | retryable |
|---|---|---|
| `UNAVAILABLE` | absent | yes |
| `RESOURCE_EXHAUSTED` | present | yes |
| `RESOURCE_EXHAUSTED` | absent | no |
| `ABORTED` | present | no |
| `INTERNAL` | present | no |
| `""` / unknown string | — | no |

**V4 — `fail_oversized()`**: code `RESOURCE_EXHAUSTED`, kind
`details_oversized`, **no** `vgi_rpc.error_details`, **no**
`log_extra.error_details`; client `error_details == []`, `is_retryable() == false`.

**V5 — the cap boundary** (reference unit test): `[{"@type":"vgi_rpc.ErrorInfo","metadata":{"p":"<N × x>"}}]`
serializes to `51 + N` bytes compactly; `N = 4045` is exactly 4096 bytes and
is sent, `N = 4046` is dropped. Measured in UTF-8 bytes: 2048 × `é` in a
`LocalizedMessage.message` is under the cap in characters and over it in bytes,
and is dropped.

**V6 — rule violations dropped at emission**: two `vgi_rpc.RetryInfo`;
`{"@type":"vgi_rpc.Made.Up"}` (reserved prefix, not in the catalog);
`{"@type":"Unqualified"}`; an object with no `@type`. Each yields no
`vgi_rpc.error_details`.

**V7 — client tolerance**: a detail that is not an object, an unknown type, or
a known type with a malformed field (`"retry_delay_seconds":"soon"`, a negative
delay, `ErrorInfo.metadata` with a non-string value) is skipped by typed
access, kept in the raw array, and never turns the error into a decode failure.
A `vgi_rpc.error_details` value that is not a JSON array decodes as `[]`.

**V8 — every framework kind carries its code** (also in WIRE_PROTOCOL §8):

| kind | code | details |
|---|---|---|
| `method_not_implemented` | `UNIMPLEMENTED` | |
| `protocol_not_supported` | `UNIMPLEMENTED` | |
| `protocol_not_specified` | `INVALID_ARGUMENT` | |
| `protocol_version_mismatch` | `FAILED_PRECONDITION` | `PreconditionFailure{violations:[{type:"protocol_version", subject:<protocol>, …}]}` |
| `session_lost` | `ABORTED` | |
| `server_draining` | `UNAVAILABLE` | `RetryInfo` |
| `identity_unavailable` | `UNAVAILABLE` | `RetryInfo` (required) |
| `stale_auth` | `UNAUTHENTICATED` | |
| `introspection_refused` | `PERMISSION_DENIED` | |
| `grant_refused` | `PERMISSION_DENIED` | |
| `token_unresolved` | `NOT_FOUND` | |
| *(none: an unclassified error)* | `UNKNOWN` | |

**V9 — identity fixture** (`IDENTITY_CONFORMANCE_FIXTURE.md` §4.9a):
`conformance-unavailable-token` → `UNAVAILABLE`/`identity_unavailable`/RetryInfo 5;
`conformance-auth-unavailable-token` and purpose `conformance-auth-unavailable`
→ `UNAVAILABLE`/`identity_unavailable`/RetryInfo **7**.

---

## 4. The shared-suite groups, and what a port's runner must provide

Imported by `vgi_rpc.conformance._pytest_suite` (so `from … import *` in a
port's harness picks them up):

| Group | Needs | Asserts |
|---|---|---|
| `TestSecondaryIsHosted` | connector | application protocols listed `[ConformanceService, conformance.Secondary.v1]` |
| `TestSecondaryDescribes` | connector | pinned hash, no version, three unary methods, `features == []` everywhere |
| `TestRoutingByPair` | connector | `echo_string` on each protocol reaches its own binding |
| `TestSecondaryRouting` | connector | secondary echo; absent method → `method_not_implemented`/`UNIMPLEMENTED`; unhosted protocol → `protocol_not_supported`/`UNIMPLEMENTED` |
| `TestVersionMismatchCode` | connector | a `1.0.0` client → `FAILED_PRECONDITION` + `PreconditionFailure` naming `ConformanceService` |
| `TestErrorModelRoundTrip` | connector | V1–V4, V7 and all sixteen codes, **through the client under test** |
| `TestTracebackPolicy` | connector | non-empty traceback by default on every transport |
| `TestErrorModelOnTheWire` | `conformance_http_port` | V1 and V4 read off the server's bytes; `log_extra.traceback` non-empty over HTTP |
| `TestUnclassifiedErrorsAreUnknown` | `conformance_http_port` | `raise_value_error` → `UNKNOWN` |
| `TestAdversarialRawRequestContract` (extended) | `conformance_raw_conn` | unrouted request → `protocol_not_specified`/`INVALID_ARGUMENT` |
| `TestUnavailableCarriesARetryHint`, `TestErrorKindsReachTheWire::test_every_kind_carries_its_code` | identity port | V8 identity rows, V9 |

**The connector.** The groups are parametrized by your existing
`conformance_conn` fixture (used only as the transport axis — its parameter id
is read from the test's callspec), and reach the worker through one new
session-scoped fixture:

```python
@pytest.fixture(scope="session")
def conformance_protocol_connector(...) -> Callable[..., ContextManager[Any]]:
    def connect(transport: str, protocol: type, on_log=None):
        """A proxy bound to `protocol`, on the worker `transport` (a conformance_conn
        parameter id) reaches -- the SAME worker, so primary and secondary proxies
        share one server."""
    return connect
```

Server role: open your transport to the worker for `transport`, bound to
`protocol` (`_RpcProxy(protocol, transport)`, `unix_connect(protocol, path)`,
`http_connect(protocol, url)`, …). Client role:
`ClientDriver(cmd, service=protocol).connect(transport_name, target, on_log)` —
the driver's `connect` op already carries `protocol`. The reference's
implementation is `tests/conftest.py::conformance_protocol_connector`.

The omit setting is not asserted by the shared suite: a remote worker cannot
be reconfigured from the test, so the reference tests it locally
(`tests/test_error_model.py::test_the_omit_setting_reaches_the_wire`). Ports
should do the same in their own unit tests.

**What the port suite cannot see: a listing sorted by name.** `ConformanceService`
sorts before `conformance.Secondary.v1` in ASCII, so `TestSecondaryIsHosted`
passes against a server that sorts its listing instead of keeping registration
order. TypeScript had exactly that bug; only the SDK hosted-protocols check
caught it, because `vgi.v2` sorts last. The pinned names are not renamed;
instead **every port MUST also run the hosted-protocols group (§5) against a
worker whose registration order is not alphabetical** — e.g. one hosting
`zeta.Primary.v1`, `conformance.Secondary.v1`, `alpha.Extra.v1` in that order
(the reference's `tests/serve_hosted_reverse.py`, exercised by
`tests/test_hosted_protocols.py::test_a_registration_order_no_sort_produces_passes`),
with `--expect zeta.Primary.v1,conformance.Secondary.v1,alpha.Extra.v1`.

---

## 5. The hosted-protocols group, for SDK workers

An SDK fixture worker hosts its own primary (`vgi.v2`), so the groups that
compare against `ConformanceService` do not apply. `vgi-rpc-test-hosted` runs
the ones that do — `TestSecondaryDescribes`, `TestSecondaryRouting`,
`TestErrorModelRoundTrip`, `TestTracebackPolicy`, and over HTTP
`TestErrorModelOnTheWire` — plus `TestHostedProtocolList` (the declared order)
and, with `--identity`, the Identity fixture groups (all except
`TestIdentityAbsentByDefault` and `TestIdentityNarrowing`, which need other
workers).

`TestHostedProtocolList` is the conformance check that catches a server
listing protocols sorted by name rather than in registration order — but only
when the expected order is not itself sorted. Run it against at least one
worker whose order no sort produces (§4).

```bash
pip install "vgi-rpc[http,conformance]" pytest pytest-timeout   # or a checkout of this repo
# stdio
vgi-rpc-test-hosted --cmd "vgi-fixture-worker" --expect vgi.v2,conformance.Secondary.v1
# unix (start the worker on the socket first)
vgi-rpc-test-hosted --unix /tmp/vgi.sock --expect vgi.v2,conformance.Secondary.v1
# HTTP, with Identity opted in (loopback URL with explicit port)
vgi-rpc-test-hosted --url http://127.0.0.1:8123 --expect vgi.v2,conformance.Secondary.v1 --identity
# extra pytest arguments after --
vgi-rpc-test-hosted --cmd "..." --expect ... -- -x -k RoundTrip
```

`python -m vgi_rpc.conformance.hosted_protocols …` is equivalent. Exit status
is pytest's (0 = all passed). The run uses its own pytest ini, so the calling
repository's `addopts` cannot leak in. A run over stdio skips the four
HTTP-only cases; over HTTP with `--identity` nothing should skip.

---

## 6. Per-port deliverables (Phase 2)

Each verified by this suite in both directions: the port's server against the
reference client, and the port's client against the reference server.

| Port | Public hosting API | Reserved prefix on every name | Emit code + details; traceback setting | Translate auth-unavailable | Client exposes code / kind / details |
|---|---|---|---|---|---|
| TypeScript | launchers (`serveTcp`, `serveUnix`, `serveStream`) accept extra protocols (`server.ts:177`) | ok | add | ok | add both (`log-batch.ts:33`) |
| Go | exported binding constructor so `AddProtocol` works outside the package | ok | add | add (`identity_v1.go:587`) | details; `errors.As` for kind (`wire.go:251`) |
| Rust | builder method adding a protocol + dispatcher | add, on the application name (`server.rs:561`) | add (`server.rs:1989`) | ok | add both (`envelope.rs:57`) |
| C# | constructor/builder taking extra `(interface, impl)` pairs | extend to derived names | add | add (`IdentityImpl.cs:134`) | details |
| Java | `addProtocol(Class<?>, Object)` beside `setIdentity` | ok | add (`Wire.java:232`) | add (`IdentityImpl.java:198`) | details |
| C++ | `ServerBuilder::add_protocol(...)` | add (`server.cpp:129`) | add (`result.cpp:80`) | add (`token_identity.cpp:481`) | details |

Every port additionally:

1. Hosts `conformance.Secondary.v1` in its conformance worker through the new
   hosting API (§2), and reports the pinned hash (`describe_diff.py` now
   **fails** a port that does not host it or reports another digest).
2. Emits `features: []` (§14) and, on access-log records with
   `status: error`, `error_code`.
3. Adds `IDENTITY_CONFORMANCE_FIXTURE.md`'s new token and purpose to its
   identity fixture, raising its **transport-auth** unavailable error.
4. Reports `error_code` / `error_kind` / `error_details` from its client
   driver's error object (`CLIENT_DRIVER_PROTOCOL.md` §2).
5. Provides `conformance_protocol_connector` in its pytest harness, in both
   roles (§4).
6. Exposes `is_retryable()` (or the language's equivalent) and typed detail
   accessors, and does not retry RPC errors automatically.

## 7. Per-SDK deliverables (Phase 3)

- A hook on the worker returning extra `(protocol, implementation)` pairs,
  called once at server construction, its result hosted on every transport.
- Identity hosted when the worker implements `resolve_token` and/or
  `mint_grant`, on HTTP; refuse to start when introspection is enabled without
  an allowlist.
- The fixture worker hosts `conformance.Secondary.v1` through that hook (an
  SDK may copy §2 or depend on its port's implementation) and, over HTTP, opts
  into Identity with the `IDENTITY_CONFORMANCE_FIXTURE.md` policy including the
  auth-unavailable token and purpose.
- CI runs `vgi-rpc-test-hosted` (§5) on stdio, unix and HTTP with
  `--expect vgi.v2,conformance.Secondary.v1`, and `--identity` on HTTP.

## 8. Mutation-check before calling it done

Each guard below has a test that must go red when the guard is deleted. Run the
mutation, see red, revert.

| Delete | Must fail |
|---|---|
| the 4 KiB cap check | `TestErrorModelRoundTrip::test_oversized_details_are_dropped_whole`, `TestErrorModelOnTheWire::test_oversized_details_are_absent_from_both` |
| dropping the whole array (keep the elements that fit instead) | the same two — `RetryInfo` survives and the error turns retryable |
| the identity translation (either hook) | `TestUnavailableCarriesARetryHint` (token, or purpose for the mint hook) |
| `error_code` emission entirely (top-level key **and** mirror) | `TestErrorModelRoundTrip`, `TestSecondaryRouting`, `TestErrorKindsReachTheWire::test_every_kind_carries_its_code` |
| only the top-level `vgi_rpc.error_code` key | **only** the raw-wire groups: `TestErrorModelOnTheWire::test_top_level_keys_and_log_extra_agree`, `TestUnclassifiedErrorsAreUnknown`, `TestErrorKindsReachTheWire::test_every_kind_carries_its_code` (identity reads raw metadata). Client-side groups stay green, because a conforming client falls back to the `log_extra` mirror — Go, C# and C++ each found this |
| the `log_extra` mirror | `TestErrorModelOnTheWire::test_top_level_keys_and_log_extra_agree` |
| the client reading `error_kind` / `error_details` | `TestErrorModelRoundTrip` in client role |
| the traceback default (omit on any transport) | `TestTracebackPolicy[<that transport>]`, and over HTTP `TestErrorModelOnTheWire::test_http_carries_the_traceback_by_default` |
| listing in registration order (sort it by name) | `TestHostedProtocolList` against a non-alphabetical worker only (§4) — **not** `TestSecondaryIsHosted` |
| registering the secondary | `TestSecondaryIsHosted`, `describe_diff.py` |

The translation probe is resolvable on purpose: without translation the
transport-auth error still reaches the wire as `UNAVAILABLE` with a `RetryInfo`
of 7 (it carries both itself), so only the **kind** distinguishes a translated
error — which is exactly what an untranslated port gets wrong.

## 9. Constants, in one block

```
fixture protocol      conformance.Secondary.v1      (no protocol_version)
fixture hash          58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6
echo prefix           "secondary:"
probe type            conformance.Secondary.v1.Probe
kinds                 invalid_code  details_oversized
oversized padding     5000 bytes ("x")
details cap           4096 UTF-8 bytes; over it, drop the whole array

metadata keys         vgi_rpc.error_code  vgi_rpc.error_kind  vgi_rpc.error_details
log_extra mirror      error_code (string)  error_kind (string)  error_details (array)
codes                 CANCELLED UNKNOWN INVALID_ARGUMENT DEADLINE_EXCEEDED NOT_FOUND
                      ALREADY_EXISTS PERMISSION_DENIED RESOURCE_EXHAUSTED
                      FAILED_PRECONDITION ABORTED OUT_OF_RANGE UNIMPLEMENTED
                      INTERNAL UNAVAILABLE DATA_LOSS UNAUTHENTICATED
detail types          vgi_rpc.ErrorInfo vgi_rpc.RetryInfo vgi_rpc.BadRequest
                      vgi_rpc.PreconditionFailure vgi_rpc.QuotaFailure
                      vgi_rpc.ResourceInfo vgi_rpc.Help vgi_rpc.LocalizedMessage
tracebacks            included by default on every transport; per-server setting turns them off
                      stackless ports: synthesized "<ErrorType>: <message>" + "<protocol>/<method>"
registration          sealed no later than first serve; registering after MUST fail
identity fixture      conformance-auth-unavailable-token / purpose conformance-auth-unavailable
                      -> transport-auth error, retry 7 -> identity_unavailable + RetryInfo 7
                      conformance-unavailable-token -> RetryInfo 5
runner fixture        conformance_protocol_connector(transport, protocol, on_log=None)
SDK command           vgi-rpc-test-hosted --cmd|--unix|--url ... --expect vgi.v2,conformance.Secondary.v1 [--identity]
```

## 10. Revision 2 — what changed after the ports implemented revision 1

| # | Finding | Change |
|---|---|---|
| 1 | DuckDB shows the remote traceback to users; omitting it on HTTP hid chained causes (Java) | Tracebacks **included by default on every transport**; the setting is per server and only turns them off. `TestTracebackPolicy` asserts non-empty everywhere |
| 2 | Stackless languages had no defined traceback (C++, Rust) | Synthesized trace, at minimum `<ErrorType>: <message>` and `<protocol>/<method>`; non-empty only is asserted |
| 3 | §8 said framework faults get `INTERNAL`, the reference never emitted it (Go, Java) | Reworded: `UNKNOWN` for unclassified errors; `INTERNAL` reserved for implementation-detected framework faults, MAY be emitted, not required by conformance |
| 4 | Registration after serving started was unspecified (TS and Java seal on first serve) | §3.1: sealed no later than first serve; registering after MUST fail. The reference seals at construction (no later API; `bindings` is read-only) |
| 5 | The port suite cannot detect a listing sorted by name (TypeScript) | Ports MUST run the hosted group against a non-alphabetical worker; reference test added |
| 6 | Deleting only the top-level `error_code` key leaves client groups green (Go, C#, C++) | §8 mutation table corrected |
| 7 | `IDENTITY_CONFORMANCE_FIXTURE.md` quoted a stale primary hash | Corrected to the computed value |
| 8 | Rust gated on major only | WIRE_PROTOCOL §13 already says major **and** minor; `PreconditionFailure` text matches. Rust fix tracked in that port |
