# `vgi_rpc.Identity.v1` — normative port contract

Reference implementation: `vgi-rpc-python/vgi_rpc/rpc/_token_identity.py` (515 lines),
tests in `vgi-rpc-python/tests/test_token_identity.py` (29 tests).
Read both before writing code. This file is the contract; where it and your
intuition disagree, this file wins.

## 1. Wire shape — must match byte for byte in the hash

Protocol name: `vgi_rpc.Identity.v1` (reserved `vgi_rpc.` prefix — register it the
way your port registers `vgi_rpc.Reflection.v1`, with the reserved-prefix escape).

Both methods are UNARY, `has_return = true`, `has_header = false`.

```
introspect_token(token: utf8 non-null) -> TokenIdentity
issue_grant(purpose: utf8 non-null,
            scopes: list<item?: utf8> non-null,     # item IS nullable
            ttl_seconds: int64 non-null) -> IssuedGrant
```

Result column for both is the single `result: binary non-null` your port already
uses for a returned `ArrowSerializableDataclass` equivalent.

```
TokenIdentity:  principal: utf8 nn         (no default — required)
                token_name: utf8 nn        default ""
                ttl_seconds: int64 nn      default 300

IssuedGrant:    token: utf8 nn             (no default — required)
                expires_at: float64 nn     (no default — required)
                grant_id: utf8 nn          default ""
```

Field **declaration order is significant** (it is part of the schema and so of the
hash). `scopes`'s list item being nullable is the single most likely thing to get
wrong — TypeScript already shipped that bug once across every list type.

### Required test: the hash vector

Assert these three digests in a unit test. They are computed by the reference
and are not negotiable:

| methods hosted | protocol_hash |
|---|---|
| both | `8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5` |
| `introspect_token` only | `27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385` |
| `issue_grant` only | `c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8` |

Canonical preimage for the both-methods case (diff against this if a digest is wrong):

```json
{"methods":[{"has_header":false,"has_return":true,"name":"introspect_token","params":[{"name":"token","nullable":false,"type":"utf8"}],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"},{"has_header":false,"has_return":true,"name":"issue_grant","params":[{"name":"purpose","nullable":false,"type":"utf8"},{"name":"scopes","nullable":false,"type":"list<item?:utf8>"},{"name":"ttl_seconds","nullable":false,"type":"int64"}],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"}],"protocol":"vgi_rpc.Identity.v1"}
```

The two single-method digests are not decoration: they prove **method-level
narrowing** (§4) actually narrows the hash rather than hosting a method that
refuses.

## 2. Constants — identical in every port

```
MAX_TOKEN_BYTES      = 4096   # UTF-8 BYTES -- see below
JWS_SHAPED regex     = ^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$
default introspect_rate_limit = 20      (per caller, per 1.0s window)
default max_auth_age          = 900.0   seconds
IdentityUnavailableError.retry_after default = 5
```

### The cap is measured in UTF-8 BYTES

The ports reached for three different units: codepoints (Python, Rust), UTF-16
code units (Java, C#, TypeScript), and bytes (Go, C++). All four agree for an
ASCII credential -- which every real bearer token is -- so this is invisible
today and matters only for a multibyte one.

It is pinned anyway, because "approximately the same limit" is how every other
divergence in this document started, and each turned out to be a hole once
somebody measured it. Bytes is the unit the purpose implies (what a resolver
would have to handle) and the most conservative of the three, so standardising
on it can only refuse earlier. Python renamed the constant to `MAX_TOKEN_BYTES`
so the unit is not left to the reader.

## 3. Error taxonomy — `error_kind` strings are the wire contract

These used to be an HTTP route where callers classified definitive-vs-transient on
the status code (404 vs 503). As protocol methods every handler exception surfaces
as HTTP 500, so `error_kind` is now the **only** signal a caller has. A caller that
negative-caches a transient failure locks out valid users; one that retries a
definitive rejection hammers the worker.

| error | `error_kind` | class | meaning |
|---|---|---|---|
| IntrospectionRefused | `introspection_refused` | permission-denied | definitive; caller may cache |
| TokenUnresolved | `token_unresolved` | invalid-argument | definitive; **uniform** across unknown/expired/malformed |
| StaleAuth | `stale_auth` | permission-denied | definitive but **actionable** — names the reason |
| GrantRefused | `grant_refused` | permission-denied | definitive |
| IdentityUnavailable | `identity_unavailable` | **not** invalid-argument | **transient**; carries `retry_after` |

`IdentityUnavailable` must NOT subclass / be catchable as your port's
"invalid argument" or "not my credential" type. In Python it is deliberately not a
`ValueError` because `chain_authenticate` advances on `ValueError` — a sidecar
outage raised as one reads as "try the next authenticator" and becomes a 401 from
the end of the chain, restarting every session in the fleet over a 30-second blip.
Find the equivalent hazard in your port and avoid it.

## 4. Guards — order is load-bearing

### The JWS shape test runs on the TRIMMED credential (decided after C++ hit it)

Trim leading/trailing whitespace from the credential, run the shape test on the
trimmed form, and pass the **untrimmed original** to the resolver.

Anchor semantics are the least portable corner of seven regex dialects and the
ports split three ways on `"a.b.c\n"`: Python's `$` matched before a single
trailing newline and refused it; Go's `\A..\z` and JavaScript's unflagged `$`
matched strictly and routed it **to the resolver** -- the one outcome this guard
exists to prevent. Python was not self-consistent either, refusing one trailing
newline and admitting two.

So do not replicate any dialect's anchor behaviour. Trimming first can only add
refusals, never remove one, and it means the same thing everywhere -- including
where the matcher is hand-rolled because the standard regex engine backtracks on
attacker-controlled input.

A whitespace-only credential is refused: it is not a credential.

**The trim set is enumerated, not delegated to the language.** "Whitespace" is
itself a divergence one layer down -- measured, not assumed:

| codepoint | Python | Go | C# | JS/TS | Java | C++ (ASCII) |
|---|---|---|---|---|---|---|
| `U+0009`-`U+000D`, `U+0020` | yes | yes | yes | yes | yes | yes |
| `U+0085` NEL | yes | yes | yes | **no** | **no** | **no** |
| `U+00A0` NBSP | yes | yes | yes | yes | yes | **no** |

A port trimming a narrower set **routes a padded JWS that another port
refuses**, which is the same hole one level down. So every port MUST trim at
least:

```
U+0009 U+000A U+000B U+000C U+000D U+0020 U+0085 U+00A0
```

A port MAY trim more (Python, Go and Rust trim the full Unicode `White_Space`
property). Trimming wider can only add refusals, so a wider set is a safe
difference; a narrower one is a leak. Do not reach for
`Character.isWhitespace`, `isspace()`, or an ASCII-only literal without
checking it against the table above -- `Character.isWhitespace` excludes NBSP
by design, and `isSpaceChar` excludes NEL.

Trim for the shape test **only**. Rewriting a credential before resolving it
would make the worker answer about a string the caller never sent.

### `introspect_token`

1. hook absent → `IntrospectionRefused("this worker does not resolve credentials")`
2. **authorization**: caller must be authenticated AND its principal in the
   allowlist → else `IntrospectionRefused("caller is not an introspector")`
3. rate limit, keyed by caller principal → `IntrospectionRefused("introspection rate limit exceeded")`
4. `reject_jws_shaped`: empty OR `len > MAX_TOKEN_CHARS` OR matches the JWS regex
   → `TokenUnresolved("unresolved")`
5. resolve; hook returned "unknown" → `TokenUnresolved("unresolved")`

Steps 2–3 come **before** step 4 on purpose: an unauthorized caller must learn
nothing about the subject credential, *including how long looking at it took*.
Do not reorder for tidiness. A test must pin the order (an unauthorized caller
presenting an over-long or JWS-shaped token still gets `introspection_refused`,
never `token_unresolved`).

Rejections are **uniform**: unknown, expired and malformed are one answer, because
reporting which would confirm a guessed credential exists.

### `issue_grant`

1. hook absent → `GrantRefused("this worker does not mint grants")`
2. freshness, in this order, all → `StaleAuth`:
   - not authenticated / no principal
   - no `auth_time` claim: *"credential carries no auth_time; only a recently
     authenticated user may mint a grant"*
   - `auth_time` present but unparseable as a number
   - `now - auth_time > max_auth_age`
3. call the hook as `(caller_principal, purpose, scopes, ttl_seconds)`

**There is no subject parameter.** The subject is always the caller's authenticated
principal, so cross-subject minting is closed by construction rather than by a
check that could be forgotten in one of six ports. Do not add one.

Requiring `auth_time` is what stops a grant minting another grant (a grant is not
IdP-issued so carries no `auth_time`, so the lineage cannot escape the IdP) and
makes subprocess/unix transports fail closed for free — there is no authenticated
principal there at all. A static bearer proves a machine holds a secret, never that
a human just authenticated, so it is refused here too.

### Allowlist

An allowlist is **required whenever the resolve hook is supplied**, validated at
**construction** (a worker that would refuse every introspection must fail to start,
not serve traffic until someone tries). Empty or absent → construction error.
There is no permissive default: "any authenticated caller" lets any user resolve
any other user's credential to its owner, and test that omission is an error.

### Rate limiter

Fixed window (not a token bucket), keyed by caller. On window roll, clear the
**whole map** rather than ageing per key — an attacker cycling keys then cannot
grow the map beyond one window's worth. Must be safe under your port's concurrency
model.

## 5. Method-level narrowing

**A method whose hook the deployment did not configure is not hosted at all**, and
the binding's method set — and therefore its `protocol_hash` — narrows with it.
A worker that resolves credentials but does not mint grants hosts
`introspect_token` and not `issue_grant`, and a client discovers that through
ordinary reflection rather than by calling and reading an error.

Absent beats routed-and-refusing: it is what keeps a dependency upgrade from
growing a credential-to-identity oracle on every existing worker. If neither hook
is configured, the protocol is not registered at all.

The per-method guard in §4 step 1 is the belt to this braces — both exist.

## 5a. Zero values vs. absent values (decided after Go hit it first)

`ttl_seconds` defaults to 300 and `token_name`/`grant_id` default to `""`. Those
are **decode-side defaults for an absent column**, not coercions to apply to a
value a hook actually supplied.

Several ports have a zero value where Python has "field omitted" -- a Go struct
literal, a Rust `Default`, a C# `default(T)` -- so a hook that names no TTL
produces `0` rather than an absent column. The tempting fix is to normalise
`ttl_seconds <= 0` up to 300. **Do not.**

`ttl_seconds` is how long the caller may cache the answer, which for any path the
asker serves without re-presenting the credential is an authorization window and
therefore the revocation lag. Coercing `0` to `300` silently converts a resolver
saying *"do not cache this"* into five minutes of continued access after
revocation. A port that honours `0` and is handed one by accident fails the other
way: more introspection traffic, no extended window.

So: **honour what the hook returned, including `0`.** Where the language permits,
offer a constructor or builder that supplies 300 so the omission case still lands
on the documented default -- but never override a value that was actually set.

## 5b. Testing guards behind uniform rejections (the vacuity trap)

Rejections are deliberately uniform -- unknown, expired, malformed and
over-long are one answer. That makes the obvious guard test **prove nothing**.

An over-long credential is also an unknown one, so probing `introspect_token`
with a credential the resolver does not know cannot distinguish *"the cap
refused it"* from *"the cap let it through and the resolver refused it"*.
Delete the length check from the dispatch path and such a test stays green.
This was found in two ports independently, and the reference had it too: its
`test_rejections_are_uniform` covers the over-long case and passes with the cap
removed, because it is a test about uniformity and never was a test that the cap
fires.

**The resolvable-probe form.** Use a hook that resolves *anything*, so a
rejection can only have come from the guard, and assert the hook was never
reached. That second assertion is the half that fails when the guard is skipped.

**The hole tracks exactly where uniformity applies.** Measured across the ports:
the rate limit and the freshness check were already soundly tested everywhere,
because their refusals are distinguishable by type or by message. Only guards
whose refusal collapses into the *uniform* `token_unresolved` answer -- the
length cap and the JWS trap -- can be tested vacuously. That is the rule for
deciding where to look first in a port you have not audited.

Watch for the subtler form too: a test can pass for a reason its name disclaims.
C#'s `AuthorizationPrecedesTheLengthCheck` asserts the allowlist refusal, which
fires first whatever the cap does, so despite its name it never touched the cap.
A test whose name claims one property while asserting another is worse than a
missing test, because it reads as coverage in review.

**Mutation-check every guard test.** Break the guard on purpose and confirm the
test goes red. A guard test that passes against a deliberately broken guard is
worse than no test, because it is counted as coverage. Every port should do this
for the JWS trap, the length cap, the allowlist, the rate limit and the freshness
check.

## 5c. The routing key on HTTP: required on raw transports, optional on HTTP

The plan says `vgi_rpc.protocol` is required on every request, absent being an
error even on a single-protocol server. **That is enforced on raw transports
and deliberately not on HTTP.** Two ports reached this independently and one
implemented the strict reading and became unshippable, so it is written down.

On stdio, unix and named pipes the metadata field is the only carrier, so absent
really is unroutable. On HTTP the path segment already resolved the binding, and
the shared conformance harness actively tests the permissive behaviour: the
`_adversarial_http.py` recovery probe requires a **200** for a namespaced request
carrying no `vgi_rpc.protocol`. A worker that rejects it fails conformance.

Tightening the reference to match the strict reading fails 100 tests (69 after
fixing the central request builder), most in the cross-language harness that
drives all six ports. It has to move the harness and the ports together.

**What this gives up, stated plainly:** requiring the key on HTTP is what would
make a *path rewrite* by an intermediary detectable, since an intermediary that
rewrites the path cannot touch the metadata. Accepting absent means routing on
the projection alone in exactly that case. It is a real gap, taken deliberately.

Clients MUST still send it (the reference HTTP client did not, which is how this
surfaced). Servers MUST still refuse *disagreement*.

## 6. Hygiene

- The credential must never reach a log, a span, or an error message. Provide a
  `token_digest` (SHA-256 hex) for diagnostics — stable enough to correlate one
  credential's failures without being the credential.
- `TokenIdentity` **never carries claims**. A pass-through claims field would let a
  worker choose its caller's tenant routing, row scope and policy branch. The
  asker derives what it needs from the principal alone. Do not add one.
- `expires_at` on a grant is a *declaration*, not enforcement — the real lifetime
  lives inside the opaque token. The framework never parses the token.

## 7. What to deliver

1. The protocol + the two payload types, in your port's idiom.
2. The guard/hook-delegating implementation — framework owns the guards, worker
   owns all policy. The split is deliberate: the guards are identical in every
   deployment and catastrophic to get wrong; the policy differs in every
   deployment and cannot be guessed.
3. Server wiring, modelled on how your port registers `vgi_rpc.Reflection.v1`:
   registered **after** reflection so it appears in reflection's output, with
   `only`-style narrowing to the configured methods.
4. Tests: the three hash vectors, the guard order, allowlist-required,
   rate limiting, JWS/oversize/empty rejection, uniform rejections, freshness
   including the no-`auth_time` case, no-subject-parameter, narrowing changes the
   hash, and `identity_unavailable` not being caught as invalid-argument.
   Port the 29 reference tests; do not invent a thinner set.
5. Your port's full suite green, formatter and linter clean.

Do NOT change the primary conformance protocol or its hash
(`5cc768771c2e8a54e19ebb7546c97c119823eb13e20a5ff62ca5ce7ed2a1334e`) — it is
verified across all seven ports. If your change moves it, you broke something.
