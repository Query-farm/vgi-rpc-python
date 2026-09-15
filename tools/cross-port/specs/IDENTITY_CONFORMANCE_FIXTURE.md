# `vgi_rpc.Identity.v1` — conformance fixture contract

Companion to `IDENTITY_V1_SPEC.md`, which is the normative contract for the
*implementation*. This is the normative contract for the *fixture* a port must
provide so the shared conformance group can run against it, and a statement of
what that group asserts.

Reference implementation of the fixture policy:
`vgi-rpc-python/vgi_rpc/conformance/identity_fixture.py`. Reference group:
`vgi-rpc-python/vgi_rpc/conformance/_identity_pytest.py`. You do not need to
read either — everything normative is below.

---

## 0. Why a fixture is needed at all

`vgi_rpc.Identity.v1` is almost entirely *guards*, and every guard reads
deployment policy: who may introspect, what a credential resolves to, whether a
grant is minted, how recently the caller authenticated. Against a worker whose
allowlist and hooks are unknown, no cross-port assertion exists — every answer
is explicable as policy.

So the policy is pinned. Every value below is a value six ports must configure
identically. They are kept to the minimum that makes the properties observable;
anything that could be left out was.

---

## 1. Two fixture workers

Both are the **same binary** with a different flag. Name the flag whatever your
port's conformance worker already uses for its other modes; the Python reference
spells it `--identity {off,both,introspect-only}`.

| Runner fixture name | Worker configuration |
|---|---|
| `conformance_http_identity_port` | resolve hook **and** mint hook configured |
| `conformance_http_identity_introspect_only_port` | resolve hook only; **no** mint hook |

Both must be HTTP. See §2 for why HTTP and only HTTP.

A third worker is *not* needed for "identity absent": the group asserts that
against your **existing plain conformance worker** (`conformance_http_port`),
which configures no hook and must therefore host no identity protocol at all.
Do not add identity to it.

**If your port provides neither fixture, the group skips.** Absence is
legitimate — a worker configuring no hook hosts no protocol. But the skip names
the missing fixture and says what it must configure, so a gap reads as a gap
rather than as a clean run.

---

## 2. Authentication: two request headers, no identity provider

This is the part that could not be solved by wishing. `introspect_token`
requires an authenticated caller on an allowlist, and `issue_grant` requires an
`auth_time` claim — which in a real deployment means a JWT from an IdP. Six
language ports cannot each stand up an IdP, and a baked JWT would expire.

**The conformance worker installs an `authenticate` callback that derives the
caller's identity from two request headers.** This is exactly the mechanism the
sticky-session fixture already uses (`sticky-sessions-spec.md` §9), extended by
one header.

| Header | Effect |
|---|---|
| `X-Conformance-Principal` | The authenticated principal. **Absent ⇒ the request is unauthenticated** — not "anonymous but authenticated". |
| `X-Conformance-Auth-Time` | Placed in the claim map under key `auth_time`, **verbatim as a string, unparsed**. Absent ⇒ no `auth_time` claim at all. |

Three rules, all load-bearing:

1. **Absent `X-Conformance-Principal` means `authenticated = false` and no
   principal.** Health checks and capability probes must keep working
   unauthenticated, and the group relies on the absence to test fail-closed
   behaviour.
2. **`X-Conformance-Auth-Time` is passed through verbatim.** A fixture that
   parses the value and drops it when parsing fails collapses "credential
   carries an unusable `auth_time`" into "credential carries no `auth_time`".
   Both answer `stale_auth`, so the test stays green while the property it
   names goes untested. The *guard* parses; the fixture only transports.
3. Nothing else goes in the claim map.

> **This authentication is trivially spoofable by anyone who can reach the
> port.** It is a test fixture and must never be deployed. Say so in your
> worker's source, as the reference does.

### What this does and does not exercise

It exercises everything Identity's guards actually read: `authenticated`,
`principal`, `claims["auth_time"]`. It does not exercise JWT signature
validation, which is not part of Identity's contract — it happens in your
port's authenticator, before Identity sees anything, and has its own tests.

### Why the group is HTTP-only

Identity's guards are all about an authenticated caller, and HTTP is the
transport that carries one. On stdio and unix there is no authenticated
principal at all, so both methods fail closed — which is covered here by the
unauthenticated-caller cases (they take the identical code path), and should be
covered end-to-end in your port's own suite. The reference does that in
`tests/test_token_identity.py::TestDispatchOverARawTransport`, and it found a
real bug: see §7.

---

## 3. Deployment policy to configure

### 3.1 Scalars

| Setting | Value | Why this value |
|---|---|---|
| introspector allowlist | exactly `["conformance-introspector"]` | one principal, so "on the list" and "authenticated but not on the list" are both reachable |
| `max_auth_age` | `900.0` | the documented default |
| `introspect_rate_limit` | `100000` | **deliberately far above the default 20.** Nearly every case in this group is an introspection; a production-tuned limiter would fire mid-group and every resulting failure would read as the wrong guard. The limiter is not asserted here — see §6. |

### 3.2 Principals the group sends

| Constant | Value |
|---|---|
| introspector (allowlisted) | `conformance-introspector` |
| outsider (authenticated, **not** allowlisted) | `conformance-outsider` |
| minter | `minter@conformance.example` |
| other minter | `other-minter@conformance.example` |

### 3.3 The resolve hook

**The resolver resolves almost everything.** This is the single most important
thing on this page, and it is not an accident.

Rejections are deliberately uniform — unknown, expired, malformed and over-long
are one answer — so an over-long credential is *also* an unknown one. A test
that probes the cap with a credential the resolver does not know cannot
distinguish "the cap refused it" from "the cap let it through and the resolver
refused it": **delete the cap and the test stays green.** Spec §5b calls this
the vacuity trap; the reference had it, and two ports found it independently.

With a resolver that answers for whatever it is handed, a rejection can only
have come from a guard — and a guard that fails to fire produces a *success*,
which uniformity cannot disguise.

```
resolve_token(token) ->
    token == "conformance-unavailable-token"  -> raise IdentityUnavailable("conformance: mapping store unreachable")
    token == "conformance-unknown-token"      -> unknown  (None / null / "not found")
    token == "conformance-zero-ttl-token"     -> Identity(principal=SUBJECT, token_name="conformance-subject", ttl_seconds=0)
    token == "conformance-minimal-token"      -> Identity(principal=SUBJECT)   # other fields OMITTED, not zeroed
    token == "  conformance-padded-probe  "   -> Identity(principal=SUBJECT, token_name="conformance-padded", ttl_seconds=300)
    anything else                             -> Identity(principal=SUBJECT, token_name="conformance-subject", ttl_seconds=300)
```

where `SUBJECT = "subject@conformance.example"`.

Three of those rules need their reasons stated:

- **`conformance-zero-ttl-token`** returns `ttl_seconds = 0` and the group
  asserts the wire carries `0`. Spec §5a: a resolver naming zero is saying *do
  not cache this*, and the tempting normalisation of `<= 0` up to the 300
  default silently converts that into five minutes of continued access after
  revocation. If your port's identity type cannot express "zero as a value the
  hook set", that is the bug this finds.

- **`conformance-minimal-token`** must be built with **only** the principal
  supplied, so `token_name` and `ttl_seconds` land on their documented defaults
  (`""` and `300`). Use your port's builder or constructor that supplies them.
  Do **not** pass `""` and `300` explicitly — that tests the wrong thing.

- **`"  conformance-padded-probe  "`** is the exact string with **two leading
  and two trailing ASCII spaces (U+0020)**, and resolves to a *distinguishable*
  `token_name`. §4 says the shape test runs on the trimmed credential while the
  resolver receives the untrimmed original. A port that trims once, up front,
  and resolves the result passes every other case in this group — the padded
  credential still resolves, just via the catch-all rule — so this is the only
  thing that can see it.

### 3.4 The mint hook

```
mint_grant(principal, purpose, scopes, ttl_seconds) ->
    purpose == "conformance-refused"  -> raise GrantRefused("conformance: this purpose is refused")
    purpose == "conformance-minimal"  -> Grant(token=T, expires_at=1893456000.0)            # grant_id OMITTED
    anything else                     -> Grant(token=T, expires_at=1893456000.0, grant_id="conformance-grant-id")

where T = "conformance-grant-for:" + principal + "|" + join(scopes, ",")
```

- **`ttl_seconds` is ignored.** It is a request, the returned `expires_at` is
  authoritative, and honouring it would need a clock and make the value
  unassertable.
- **`expires_at` is the fixed constant `1893456000.0`** (2030-01-01T00:00:00Z),
  not `now + ttl`. A constant can be asserted exactly, which also pins the
  float64 round trip. `expires_at` is a declaration rather than an enforcement
  — the real lifetime lives inside the opaque token — so nothing is lost.
- **The token embeds the caller's principal**, which is how "the subject is the
  caller, never a parameter" becomes observable: two callers making identical
  requests get two different tokens.
- **The token echoes the scopes**, joined with `,` after a `|`, so the list's
  round trip is visible in the response. An empty scope list yields a token
  ending in `|`.
- **`conformance-minimal`** omits `grant_id` so its documented default (`""`)
  is observable. Omit it; do not pass `""`.

Both hooks must be **pure functions of their arguments** — no clock, no
counter, no shared state. A conformance worker must answer identically on the
first call and the thousandth, and on a runtime that dispatches the two methods
on different threads.

---

## 4. What the group asserts

Seventy-seven cases in the reference after parametrisation, across twelve
groups. Grouped below by the property, with the expected `error_kind` where one
applies.

| group | cases |
|---|---|
| wire shape (§4.1) | 5 |
| narrowing (§4.2) | 4 |
| absent by default (§4.3) | 1 |
| introspection happy path (§4.4) | 4 |
| authorization and its order (§4.5) | 7 |
| the JWS trap (§4.6) | 30 |
| the size cap (§4.7) | 8 |
| uniform rejections (§4.8) | 4 |
| transient vs definitive (§4.9) | 1 |
| grant issuance (§4.10) | 7 |
| freshness (§4.11) | 5 |
| all five kinds (§4.12) | 1 |

### 4.1 Wire shape, read back through reflection

Not asserted against a transcribed literal. A locally computed digest compared
to a copied constant proves the copy was made; reading it off a running server
proves the wire shape. Drive `vgi_rpc.Reflection.v1`:

| Assertion | Mechanism |
|---|---|
| the protocol is hosted under `vgi_rpc.Identity.v1` | `list_protocols` includes it |
| both-methods `protocol_hash` = `8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5` | `list_protocols` |
| `describe` lists exactly `introspect_token`, `issue_grant`, both `unary`, both `has_return`, neither `has_header` | `describe` |
| **`issue_grant` publishes exactly `["purpose", "scopes", "ttl_seconds"]`** — no subject parameter, in that order | parameter schema decoded from the IPC the server sent |
| `scopes`'s **list item is nullable** | same schema |
| `introspect_token` publishes exactly `["token"]`, non-null utf8 | same schema |

The no-subject-parameter assertion is read off the published schema rather than
from a handler signature because a port can add the parameter to one without
adding it to the other, in either direction.

### 4.2 Narrowing (needs the introspect-only worker)

| Assertion | Expected |
|---|---|
| the narrowed worker still hosts the protocol | present in `list_protocols` |
| its `protocol_hash` = `27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385` | one-method digest |
| `describe` lists only `introspect_token` | — |
| calling `issue_grant` on it does not succeed | either a transport-level 404 or a typed refusal; **the shape is deliberately not pinned** |
| the two workers' hashes **differ** | stated separately, because a port could pin both digests to one wrong constant and pass them individually |

### 4.3 Identity absent by default — against your plain worker

| Assertion | Expected |
|---|---|
| a worker with no hooks does not list `vgi_rpc.Identity.v1` | absent from `list_protocols` |

Not hosted, not hosted-and-refusing. This is the property that lets the
protocol exist in the framework without every deployment inheriting a
credential-to-identity oracle at its next dependency upgrade.

### 4.4 Introspection, happy path

| Probe | Expected |
|---|---|
| `conformance-opaque-subject-token` as the introspector | principal `subject@conformance.example`, `token_name` `conformance-subject`, `ttl_seconds` `300` |
| `conformance-zero-ttl-token` | `ttl_seconds == 0` — **not** coerced to 300 |
| `conformance-minimal-token` | `token_name == ""`, `ttl_seconds == 300` |
| `"  conformance-padded-probe  "` | `token_name == "conformance-padded"` — the resolver saw the untrimmed original |

### 4.5 Authorization, and its order

| Probe | Expected `error_kind` |
|---|---|
| no `X-Conformance-Principal` | `introspection_refused` |
| principal `conformance-outsider` | `introspection_refused` |

Then the **order** case, parametrised over five credentials — `unknown`,
`jws_shaped`, `padded_jws`, `oversize`, `blank`. For each:

1. **control**: as the *introspector*, the credential must be refused
   `token_unresolved`;
2. **assertion**: as the *outsider*, the same credential must be refused
   `introspection_refused`, never `token_unresolved`.

The control half is not decoration. C#'s equivalent asserted only the allowlist
refusal, which fires first whatever the credential guards do — so despite its
name it never touched them. A test that passes for a reason its name disclaims
reads as coverage in review, which is worse than a missing test.

### 4.6 The JWS trap

All expect `token_unresolved`, and all use a credential the resolver **would**
resolve — so a port with no shape guard answers with an identity and fails
loudly.

| Probe | Count |
|---|---|
| bare `eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl` | 1 |
| the same, padded with each of **U+0009 U+000A U+000B U+000C U+000D U+0020 U+0085 U+00A0**, ×(leading, trailing, both) | 24 |
| the same, with two trailing newlines | 1 |

Plus the discrimination half — these must **succeed**, resolving to the subject
principal: `opaque-token`, `two.segments`, `a.b.c.d`, `sk_live_abc123`. Two and
four segments are not JWS shapes; a guard that refuses them refuses ordinary API
keys.

The trim set is a **floor**, not an exact set: trimming wider can only add
refusals. `U+0085` is where JavaScript and Java split (both `isWhitespace`
implementations exclude it); `U+00A0` is where an ASCII-only hand-rolled matcher
splits.

### 4.7 The size cap

All expect `token_unresolved`, all with credentials the resolver would resolve.

| Probe | What it pins |
|---|---|
| 5000 × `a` | the cap exists on the dispatch path |
| **2100 × `é`** — 2100 codepoints, 2100 UTF-16 units, **4200 UTF-8 bytes** | the unit is bytes. Under the cap in the other two units, so a port measuring codepoints or UTF-16 answers 200 and fails |
| `"x"` + 9000 × space | the cap measures the **untrimmed** credential. This trims to one character, so nothing but the cap can refuse it |
| `""`, `" "`, `"   "`, `"\n"`, `"\t\r\n"` | a blank credential is not a credential |

> **Expect the multibyte case to fail in ports that measure codepoints or
> UTF-16 code units.** That is the point — spec §2 pins bytes, and the
> divergence is invisible for the ASCII credentials every real bearer token is.

### 4.8 Uniform rejections

Drive five causes — `unknown`, `jws_shaped`, `padded_jws`, `oversize`, `blank`
— and assert **all three of**:

- the same `error_kind` (`token_unresolved`) for all five;
- the same error **type** for all five;
- the same error **message** for all five.

The message half is not fussiness. Checking only `error_kind` lets a cap that
names itself ("credential too long") through, which tells an attacker probing a
stolen credential which guesses were even the right shape. **One consequence is
deliberate: nothing credential-derived may appear in a refusal message —
digests included.** Diagnostics are what the digest is for.

Then, parametrised over the three paths that can refuse (guard, resolver,
authorization), assert **the credential never appears in the message**. Probe
each path with a credential distinctive enough to find by substring. Probing
only the authorization path proves nothing: it refuses without ever looking at
the credential.

### 4.9 Transient vs definitive

| Probe | Expected |
|---|---|
| `conformance-unavailable-token` | `error_kind == "identity_unavailable"` |
| vs `conformance-unknown-token` | **different** `error_kind` **and different error type** |

The type half is the wire-observable proxy for "`IdentityUnavailable` must not
be catchable as your port's invalid-argument type". A chain that advances on
that type reads an outage as "try the next authenticator" and turns a
thirty-second blip into a fleet-wide re-login.

### 4.10 Grant issuance

Caller `minter@conformance.example` with `X-Conformance-Auth-Time` set to
roughly `now - 60`, unless stated.

| Probe | Expected |
|---|---|
| purpose `conformance`, scopes `["read","write"]` | token `conformance-grant-for:minter@conformance.example\|read,write`, `expires_at == 1893456000.0` exactly, `grant_id == "conformance-grant-id"` |
| the same call as two different principals | two **different** tokens, each prefixed with its own caller |
| scopes `[]`, `["read"]`, `["read","write","admin"]` | echoed portion of the token equals `join(scopes, ",")` |
| purpose `conformance-minimal` | `grant_id == ""` |
| purpose `conformance-refused` | `grant_refused` |

### 4.11 Freshness

| Probe | Expected `error_kind` |
|---|---|
| no `X-Conformance-Auth-Time` header | `stale_auth` |
| `X-Conformance-Auth-Time: not-a-timestamp` | `stale_auth` |
| `X-Conformance-Auth-Time: now - 86400` (against the 900s ceiling) | `stale_auth` |
| no `X-Conformance-Principal` at all | `stale_auth` |
| `X-Conformance-Auth-Time: now - 60` | **mints** — this is the control |

The control matters more than it looks: without it, every case above passes
against a worker that refuses every mint, which is the likeliest failure mode
for a freshness check because refusing is the safe direction and nothing else
complains.

### 4.12 All five kinds on the wire

One probe per kind, then compare the whole **set** to
`{introspection_refused, token_unresolved, stale_auth, grant_refused,
identity_unavailable}`. Each kind is asserted at its own site above; this is the
assertion that fails when a port surfaces four of five and folds the fifth into
a generic error.

---

## 5. How to read the errors off the wire

An RPC failure travels as an EXCEPTION batch in a **200** response body — the
call reached the method and the method raised, so the status line says nothing.
Your port already decodes this for every other error test; the fields the group
reads are:

| Field | Source |
|---|---|
| `error_kind` | batch custom-metadata key `vgi_rpc.error_kind` |
| error type | `exception_type` inside `vgi_rpc.log_extra` |
| message | `exception_message` inside `vgi_rpc.log_extra` |

---

## 6. Deliberately **not** asserted

Stated so a port does not go looking for the gap and so nobody mistakes the
omissions for oversights.

| Property | Why not |
|---|---|
| **the rate limiter** | It is the one guard that poisons its own neighbours: nearly every case here is an introspection, and a production-tuned limiter would fire mid-group with every resulting failure reading as the wrong guard. The fixture raises it to 100000 to get it out of the way. Spec §5b measured it as *already soundly covered in every port*, because its refusal is distinguishable by message and so cannot be tested vacuously. **Keep testing it port-locally.** |
| **allowlist required at construction** | A worker that refuses to start cannot be probed over the wire. Port-local. |
| **`token_digest`** | A diagnostics helper; nothing puts it on the wire. |
| **the limiter's whole-map reset** | In-process state, invisible from outside. |
| **`IdentityUnavailable`'s supertype** | A language-level property. Its wire-observable proxy — a distinct `error_kind` *and* a distinct error type — **is** asserted (§4.9). |
| **a null *inside* the scopes list** | The item's nullability is already pinned byte-for-byte by the `protocol_hash` (§4.1), which the group reads off a running server. Sending an actual null would additionally require every port's mint hook to accept `list<optional<string>>`, which several cannot express without changing the hook signature — a large ask for a property the hash already covers. |
| **the exact `max_auth_age` boundary** | A knife-edge across a clock the test does not share with the worker. The group probes a minute old and a day old. |
| **a future `auth_time`** | `age` is negative and therefore passes. The spec takes no position, and a conformance group is the wrong place to invent one. |
| **trimming *wider* than the floor** | Spec §4 explicitly permits it. |
| **raw-transport dispatch** | Identity's guards all read an authenticated caller and only HTTP carries one; the fail-closed behaviour of transports that carry none is covered by the unauthenticated-caller cases, which take the identical path. Do cover it port-locally — see §7. |

---

## 7. A bug this found, which every port should check

Writing the group surfaced a framework bug in the reference that had made
`vgi_rpc.Identity.v1` **completely uncallable over every transport**, unnoticed,
since it landed.

`CallContext` injection resolved "does this method want a ctx?" against the
**primary** binding's method set rather than against the binding that owns the
dispatched method. Identity is a *secondary* protocol whose methods both take a
ctx — they need the caller's `AuthContext` to apply their guards — so they
received none of them and every call failed with a missing-argument error before
any guard ran.

It was invisible because every identity test in the reference constructed the
implementation directly and handed it a context it had built itself. Nothing
called the protocol end to end.

**If your port dispatches secondary protocols through a shared per-server map of
ctx-taking method names, check it is keyed per binding.** And add a port-local
test that drives `introspect_token` over a raw transport with an allowlisted
connection identity — the reference's is
`tests/test_token_identity.py::TestDispatchOverARawTransport`, three cases: an
allowlisted caller resolves, a non-allowlisted caller on the same transport is
refused (so a dispatch path supplying an *empty* context, which refuses
everything, is distinguishable from a working allowlist), and an anonymous raw
transport cannot mint.

---

## 8. Mutation-check before you call it done

Spec §5b is not advisory here. Break each guard on purpose and confirm the group
goes red; a guard test that passes against a deliberately broken guard is worse
than no test, because it is counted as coverage.

The reference ran twenty mutations and killed all twenty. **Two of its
first-draft assertions survived and had to be rewritten**, both in exactly the
shape this whole effort is about:

- the uniformity case checked `error_kind` and error type but not the
  **message**, so a cap rewritten to answer "credential too long" passed it;
- the never-echoes-the-credential case probed only the **authorization** path,
  which refuses before looking at the credential — so it passed against a build
  that echoed the credential from every other path, under a name that read as
  coverage.

Neither would have been found by reading the tests. Mutate these at minimum:

| Mutation | Must kill |
|---|---|
| remove the length cap | §4.7 |
| measure the cap in codepoints | §4.7 (multibyte) |
| measure the cap on the trimmed form | §4.7 (padded) |
| remove the trim before the shape test | §4.6 |
| narrow the trim set to ASCII | §4.6 (NEL, NBSP) |
| remove the JWS shape test | §4.6 |
| run the credential guards before the authorization check | §4.5 |
| accept any authenticated caller (ignore the allowlist) | §4.5 |
| coerce `ttl_seconds <= 0` up to 300 | §4.4 |
| hand the resolver the trimmed credential | §4.4 (padded probe) |
| pass the mint hook a constant subject | §4.10 |
| stop requiring `auth_time` | §4.11 |
| accept a stale `auth_time` | §4.11 |
| swallow an unparseable `auth_time` | §4.11 |
| give `IdentityUnavailable` the `token_unresolved` kind | §4.9 |
| make the cap name itself in the refusal message | §4.8 |
| echo the credential into the refusal message | §4.8 |
| host both methods regardless of configured hooks | §4.2 |
| register identity unconditionally | §4.3 |
| resolve ctx-taking methods against the primary binding | §7 |

---

## 9. Constants, in one block

```
protocol            vgi_rpc.Identity.v1

headers             X-Conformance-Principal
                    X-Conformance-Auth-Time          (verbatim into claims["auth_time"])

allowlist           ["conformance-introspector"]
max_auth_age        900.0
rate limit          100000

principals          conformance-introspector
                    conformance-outsider
                    minter@conformance.example
                    other-minter@conformance.example

subject identity    subject@conformance.example / "conformance-subject" / 300

tokens              conformance-opaque-subject-token    -> subject identity
                    conformance-unknown-token           -> unknown
                    conformance-unavailable-token       -> IdentityUnavailable
                    conformance-zero-ttl-token          -> ttl_seconds = 0
                    conformance-minimal-token           -> principal only, other fields omitted
                    "  conformance-padded-probe  "      -> token_name "conformance-padded"
                    (anything else)                     -> subject identity

jws trap            eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl

purposes            conformance-refused                 -> GrantRefused
                    conformance-minimal                 -> grant_id omitted
                    (anything else)                     -> full grant

grant token         "conformance-grant-for:" + principal + "|" + join(scopes, ",")
expires_at          1893456000.0
grant_id            conformance-grant-id

digests             both            8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5
                    introspect only 27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385
                    grant only      c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8

trim floor          U+0009 U+000A U+000B U+000C U+000D U+0020 U+0085 U+00A0
cap                 4096 UTF-8 bytes
error kinds         introspection_refused token_unresolved stale_auth
                    grant_refused identity_unavailable
```

Do NOT move the primary conformance protocol hash
(`5cc768771c2e8a54e19ebb7546c97c119823eb13e20a5ff62ca5ce7ed2a1334e`) or the
three digests above. If your change moves one, you broke something.
