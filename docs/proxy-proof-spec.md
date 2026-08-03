# vgi-rpc Proxy Proof Specification

This document is the cross-language contract for **proxy proof** in vgi-rpc. The Python implementation in this repository is the reference; other-language implementations (Go, Rust, TypeScript, Java, …) that wish to claim proxy-proof conformance MUST implement the contract below so the canonical `TestProxyProof` conformance group in `vgi_rpc/conformance/_pytest_suite.py` passes against them.

## 1. Scope and threat model

Proxy proof lets a worker **refuse any request that did not arrive through a trusted proxy**, while still receiving the end user's own credential. A proxy mints a per-request HMAC-SHA256 proof over a timestamp, a nonce, and the worker's identity, keyed by a secret shared only with that worker. The worker recomputes and compares.

- **It proves the hop, never the user.** A verified proof says "a process holding this secret forwarded this request". It says nothing about who the end user is — that remains the job of bearer/JWT/mTLS authentication, which runs alongside (§8).
- **Unforgeable without the secret.** Unlike credentials that assert what happened at a TLS terminator, a proof cannot be replayed by anyone who merely reaches the worker directly. This is the property the feature exists for.
- **Audience-bound by construction.** The worker's own identifier is folded into both the key derivation and the MAC input, so a proof minted for one worker is invalid at every other worker even if secrets were misconfigured to overlap.
- **Not request-bound.** The proof covers a timestamp, a nonce and the audience — **not** the method, path, or body. Load balancers and CDNs rewrite paths, re-chunk bodies, and recompress; binding to any of them produces intermittent, undiagnosable rejections. The timestamp window plus the nonce cache is the replay bound.
- **HTTP-only.** Pipe / subprocess / unix / tcp transports do not invoke the authenticate callback at all. A worker started on those transports ignores this feature entirely.
- **Opt-in.** An unconfigured worker is byte-identical to the pre-feature framework: no header is read, none is emitted, and no code path changes.

## 2. Wire contract

### 2.1 Request header

| Header | Required | Purpose |
|---|---|---|
| `VGI-Proxy-Proof: <token>` | in `require` mode | The proof. Format in §3. Exactly one instance; a request carrying more than one MUST be rejected. |

The header is **not** an `Authorization` credential and MUST NOT be sent as one. `Authorization` continues to carry the end user's token, untouched.

### 2.2 Capability header

When mode is `require`, the server MUST advertise this on every response (cheapest discovery via `OPTIONS /health`, which is exempt per §2.3):

| Header | Value | Notes |
|---|---|---|
| `VGI-Proxy-Proof-Required` | `"true"` | Discovery flag. Lets a proxy detect that it is minting proofs for a worker that is not checking them — otherwise a misconfiguration is silent. |

Writers MUST emit it only in `require` mode — never as `"false"` in `off` or
`allow`. Readers MUST treat an absent header as "not required", and SHOULD
accept a literal `"false"` as the same thing for forward compatibility.

It is advertisement only: it enables and enforces nothing, and a worker that
emits it while its gate is misconfigured is still just a worker that 401s.

**It is operator-declared, not derived.** The gate is installed through the
framework's existing `authenticate` seam as an opaque callback (§3), so the
server has no way to introspect it and discover the mode. Every implementation
therefore takes the posture as its own server-configuration flag, set beside
the gate. A port that tries to infer it from the callback will get it wrong
for `require_all(gate, inner)` — which is the shape every real deployment uses.

Treat it like the other capability headers (`VGI-Sticky-Enabled` and friends):
emitted on every response including the exempt ones, and listed in
`Access-Control-Expose-Headers`.

### 2.3 Exemptions

The gate runs inside the `authenticate` callback and therefore inherits the framework's existing exemptions. In **all** modes the following are reachable without a proof:

- `OPTIONS` on any path (CORS preflight cannot carry the header)
- any path under `/.well-known/`
- `{prefix}/health`

Health probes come from load balancers and orchestrators directly, not through the proxy, so exempting them is required, not a concession. Implementations MUST NOT extend this list without an explicit configuration option.

### 2.4 Mount prefix and dispatch order

The RPC mount prefix is **operator configuration, not part of this contract**. A
worker may serve at the root or under any prefix — implementations expose it as
`prefix` / `SetPrefix` / `.prefix(...)`. Nothing here may assume `/vgi`, and a
conformance runner declares its worker's prefix so shared tests address a route
that actually exists. Asserting a rejection against a path the worker does not
serve is how a test passes for the wrong reason.

Two properties **are** normative:

- **Authentication precedes method dispatch.** Within the prefix, an
  unauthenticated request is refused before the method name is resolved, so a
  caller cannot enumerate which methods a worker implements by comparing 401
  against 404. Verified across all five ports.
- **Behaviour outside the prefix is unspecified.** A request to a path the RPC
  application does not serve may be refused (implementations whose auth
  middleware is mounted app-wide) or 404'd (implementations that route to the
  prefix first). Both disclose nothing — the prefix is public configuration —
  and tests MUST NOT depend on either.

## 3. Token format

```
VGI-Proxy-Proof: v1.<kid>.<ts>.<nonce>.<mac>
```

| Field | Charset / encoding | Notes |
|---|---|---|
| `v1` | literal | Version tag. Any other value MUST be rejected before further parsing. |
| `kid` | `[A-Za-z0-9_-]{1,64}` | Key id. **No dots**, so field splitting is unambiguous. Opaque on the wire — see §5. |
| `ts` | `[0-9]{1,20}` | Unix seconds, decimal, unsigned, no leading `+`. |
| `nonce` | base64url, unpadded, 22 chars | 16 random bytes from a CSPRNG. |
| `mac` | base64url, unpadded, 43 chars | HMAC-SHA256 output (32 bytes). |

Parsing MUST use a **left split on `.` into exactly 5 fields** and reject any other count. The MAC is base64-encoded before splitting deliberately: a *raw* 32-byte tag can itself contain the delimiter byte, a bug documented in `vgi-rpc-rust/vgi-rpc/src/auth/pkce.rs` and worked around in Java's `SignedCookie.verify` by splitting on the last delimiter instead.

Every field including `mac` MUST be validated against its charset **before decoding**, and a violation is `malformed`. This is not redundant with the MAC comparison: base64 decoders disagree about invalid input — Python's `urlsafe_b64decode` silently discards non-alphabet bytes while Go, Rust and Java raise — so a port that relied on its decoder would report `bad_mac` where another reports `malformed`. The reason code is part of this contract, so the check is explicit.

Total length is bounded: implementations MUST reject a header value longer than **512 bytes** before parsing.

## 4. Canonical string

The MAC input is NUL-separated with a domain-separating prefix, following `_compute_aad` in `vgi_rpc/http/server/_state_token.py`:

```
b"vgi.proxy.proof.v1\x00" + kid + b"\x00" + ts + b"\x00" + nonce + b"\x00" + origin_id
```

`kid`, `ts` and `nonce` use the charsets in §3; `origin_id` MUST match `[A-Za-z0-9._:/-]{1,255}`. None of these can contain NUL, so the framing is unambiguous **provided implementations validate the charsets before computing the MAC**. Escaping is not defined; an out-of-charset value is a rejection, not something to encode around.

`origin_id` is **not transmitted**. The worker folds in its own configured value, which is what makes audience binding hold even under key misconfiguration.

## 5. Keys

### 5.1 Derivation

```
secret = HMAC-SHA256(base_key, b"vgi.proxy.proof.v1/" + proxy_id + b"\x00" + origin_id)
```

`base_key` is 32 raw bytes held only by the proxy. `proxy_id` and `origin_id` use the §4 `origin_id` charset. The label is distinct from every other key use in the framework — see `_derive_session_key` in `vgi_rpc/http/_oauth_pkce.py`, whose comment states the rationale: *"prevents cross-protocol forgery with stream state tokens"*.

A worker is configured with its **derived secret only**, never `base_key` — otherwise it could mint proofs for its siblings.

Independent per-proxy base keys are also conformant; derivation is a key-management convenience, not a wire requirement. The worker only ever sees a 32-byte secret.

### 5.2 `kid` identifies the calling proxy

`kid` is opaque on the wire. The worker's configuration maps it to **both a secret and a label**, so the worker knows which proxy made each request:

```
VGI_PROXY_PROOF_SECRETS=prod-use1:<64 hex>,prod-euw1:<64 hex>,staging:<64 hex>
```

> **The label attaches only after the MAC verifies.** Anyone can put `prod-use1` in the header; nobody can produce a MAC for it without that secret. Attribution therefore derives from *which secret verified*, never from the transmitted field. Before verification `kid` is a **claimed** value: safe to log as such, never to act on, and never to echo in a response body.

Rotation composes cleanly — `prod-use1` and `prod-use1-v2` are two entries carrying the same label, so an overlap window does not disturb attribution.

### 5.3 Configuration is strict and fails closed

Secrets are exactly **32 bytes, hex-encoded** (64 characters) in configuration. An implementation MUST:

- reject a secret that is not 64 hex characters, and **abort startup** rather than degrade to `off`
- abort startup when mode is `allow` or `require` and no secrets are configured
- abort startup when `origin_id` is unset in `allow` or `require` mode

A shared secret spans two independently deployed processes. A lax parse means a typo silently produces different keys on each side, and `require` mode becomes a 100% rejection outage with no diagnostic. This mirrors `parseSigningKeyHex` in vgi-typescript and the `VGI_BEARER_TOKENS` handling in vgi-rust, both of which refuse to start rather than serve something weaker than the operator believes they configured.

## 6. Verifier algorithm

Steps 1–4 involve no MAC computation and MUST be performed first.

| # | Condition | Reason code |
|---|---|---|
| 1 | Header absent | `no_proof` |
| 2 | More than one header instance, value empty, or > 512 bytes | `malformed` |
| 3 | Not exactly 5 dot-separated fields, or field 0 ≠ `v1` | `malformed` |
| 4 | `kid`, `ts`, `nonce`, or `mac` fails its §3 charset | `malformed` |
| 5 | `kid` not present in the configured secret map | `unknown_kid` |
| 6 | `now - ts > skew` | `expired` |
| 7 | `ts - now > skew` | `not_yet_valid` |
| 8 | Recomputed MAC does not match | `bad_mac` |
| 9 | `nonce` already seen within the window | `replayed` |

**The time window is two-sided.** Checking only the upper bound lets a far-future timestamp pass forever; Java's `SignedCookie.TimestampedPayload.unpack` has exactly that defect and MUST NOT be copied. Default `skew` is **30 seconds**, configurable.

**The MAC comparison MUST be constant-time.** Use each language's existing primitive — `hmac.compare_digest`, `subtle.ConstantTimeCompare`, `Mac::verify_slice`, `MessageDigest.isEqual`, `constantTimeEqual`. Because `kid` is public, selecting the single candidate secret by `kid` is a legitimate branch; only the selected MAC is compared. This differs from static bearer-token verification, which must scan every entry because the token *is* the secret.

**Rejection is uniform.** The response body MUST NOT contain the verifier's message, the reason code, or any echo of `kid` — an attacker controls that field. Detail goes to logs and metrics only. In `require` mode a failure maps to **HTTP 401**, matching every existing authenticate-callback failure in the framework.

That 401 follows [`docs/unauthorized-spec.md`](unauthorized-spec.md), which is compatible with the paragraph above rather than an exception to it. Every outcome in the table above — `no_proof`, `malformed`, `unknown_kid`, `expired`, `not_yet_valid`, `bad_mac`, `replayed` — collapses onto the single reason code `proxy_required`, with an identical detail string. The code names the stage that refused the request, which the response already said by rejecting it at all; it never names which check was tripped. A `require`-mode worker also carries that spec's proxy-configuration note, again identical on every 401 and derived from configuration rather than from the request, so it cannot be used to probe anything.

## 7. Modes

| Mode | Behavior |
|---|---|
| `off` | The gate is not installed. Zero per-request cost; not "installed and always passes". |
| `allow` | The proof is verified and recorded but never denies. On success the request carries proof attribution; on failure it proceeds anonymously. Intended as a rollout and rollback lever. |
| `require` | Verification failure returns 401. |

`allow` exists so an operator can deploy the worker, confirm attribution appears for 100% of traffic, and only then flip to `require`. Implementations SHOULD expose counters for verified and unverified requests so that cutover is verifiable rather than hopeful.

## 8. Composition with user authentication

Proxy proof is a **precondition**, not an alternative credential. It is ANDed with whatever user authentication is configured.

Every language's chain helper is an **OR** combinator — first success wins — so a proof gate MUST NOT be passed to one. Implementations expose `require_all(gate, inner)` (or the local equivalent), which:

1. runs the gate first and, on failure, fails the request **without invoking `inner`**
2. otherwise delegates to `inner` for the caller's identity
3. returns `inner`'s `domain` and `principal` unchanged, with the proof attribution merged into claims

The gate MUST signal failure with the error class that the chain combinator does **not** swallow (`PermissionError` in Python and Rust, `PermissionError`-typed `RpcError` in Go, `SecurityException`-adjacent handling in Java, a `PermissionError`-named error in TypeScript). Implementations SHOULD make the gate a distinct type and raise at construction time if it is handed to the chain combinator, so the mistake surfaces at import rather than in production.

`inner` may be absent: proof-only means "only my proxy may call this worker", with user identity handled entirely upstream.

## 9. Attribution

A verified proof is surfaced in the request's auth context **claims**, never in its `domain` or `principal` — those belong to the end user. Conflating them would let worker authorization logic or an access-log consumer mistake the proxy for the caller.

```
claims["vgi_proxy_proof"] = {
    "verified":  "true" | "false",
    "proxy":     <configured label of the secret that verified>,   # only when verified
    "kid":       <claimed kid>,                                    # informational
    "origin_id": <this worker's configured id>,
    "reason":    <reason code from §6, or "ok">,
}
```

Values are **strings**. The claims container's value type differs across languages (`map[string]any`, `BTreeMap<String,String>`, `Map<String,Object>`, `Record<string, unknown>`), so a flat string-valued map is the only shape that round-trips everywhere.

Implementations SHOULD also emit `proxy` as a first-class access-log field so "which proxy served this request" is queryable without parsing claims.

## 10. Replay cache

A verified proof is replayable for the width of the timestamp window unless nonces are tracked. Implementations MUST provide a nonce cache and SHOULD enable it by default.

- Entries expire after `skew` seconds — a nonce older than the window can no longer verify anyway.
- The cache MUST have a **hard capacity cap** in addition to the TTL. An unbounded seen-set is a trivially remote-triggerable memory-exhaustion vector: an attacker sends distinct nonces at line rate and the process grows without limit.
- On overflow, implementations SHOULD evict oldest and emit a metric rather than rejecting. A traffic burst should not become an outage, and the timestamp window still bounds exposure.
- Default capacity is 100 000 entries, configurable. Size it as `expected_rps × skew × margin`.

The proxy MUST mint a **fresh nonce per request**. Reusing a token across requests trips the receiver's cache.

## 11. Rotation

`kid` carries rotation with no cache, no TTL, and no fetched document.

1. **Distribute** the new `kid` to every worker alongside the current one. The proxy still mints with the old one.
2. **Flip** the proxy to mint with the new `kid`. In-flight proofs under the old one remain valid because workers accept both.
3. **Remove** the old `kid` from workers after one skew window plus a deploy margin.

Emergency revocation is step 3 alone, bounded only by config-push time.

## 12. Conformance

A worker claiming proxy-proof conformance MUST pass `TestProxyProof` in `vgi_rpc/conformance/_pytest_suite.py`, which covers: acceptance of a valid proof; rejection of a missing proof in `require` and anonymous pass-through in `allow`; rejection of a tampered `ts`, `nonce`, or `mac`, and of a truncated MAC; rejection of a MAC computed over an incorrectly framed canonical string; both time-window bounds; cross-origin rejection in both directions; nonce replay rejection with a distinct nonce at the same timestamp still accepted; rotation overlap; correct proxy attribution across two distinct secrets; rejection when the claimed `kid` and the signing secret disagree; health and `OPTIONS` reachability without a proof; and the AND property against a configured user authenticator.

Rejections MUST be 401, never 500. A 5xx on any malformed input is a conformance failure in its own right.

## 13. Out of scope

- **Request binding** (method, path, body digest) — see §1.
- **Distributed replay tracking.** The nonce cache is per process. A multi-process worker fleet has a per-process window, which is acceptable because the timestamp window already bounds replay and nonces are unique per mint.
- **Secret distribution.** Secrets reach workers through configuration or a secret manager. There is deliberately no fetched trust document: a secret cannot be published, and the pull machinery it would require (TTL, refresh-ahead, serve-stale, negative caching, bootstrap ordering) buys nothing that `kid` rotation does not already provide.
