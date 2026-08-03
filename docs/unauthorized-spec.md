# vgi-rpc Unauthorized Response Specification

This document is the cross-language contract for the shape of an **HTTP 401** from a vgi-rpc service. The Python implementation in this repository is the reference; other-language implementations (Go, Rust, TypeScript, Java, …) MUST implement the contract below so the canonical `TestUnauthorized` conformance group in `vgi_rpc/conformance/_pytest_suite.py` passes against them.

## 1. Scope and motivation

Before this contract, a 401 was whatever the server's web framework happened to produce. Two things went wrong with that.

**A client could not branch on it.** The only machine-readable signal was the status code, which does not distinguish "send a credential" from "your credential expired, refresh it" from "you are who you say and still may not do this". A client wanting to retry after a token refresh had to substring-match an English sentence.

**The most common production cause was invisible.** Authentication schemes that read a header injected by a reverse proxy — mTLS forwarded as `X-SSL-Client-Cert` or `x-forwarded-client-cert`, or a proxy proof — fail identically whether the caller sent a bad certificate or the proxy was never configured to forward one. The second is far more common during a deployment, and the 401 said nothing about it. Operators rotated credentials that were never the problem.

This spec fixes both: a coarse reason code from a closed set, and a **static** note on services whose authentication depends on a proxy.

**HTTP-only.** Pipe / subprocess / shared-memory / unix / tcp transports do not invoke an authenticate callback at all and never produce a 401.

## 2. What the reason code is, and is not

The reason code names the **stage that refused the request**. It never carries a verifier's internal diagnosis.

The distinction matters. Telling a caller "your JWT signature did not verify" versus "your JWT expired" is fine — both are facts about a token they hold. Telling them "the `kid` you supplied is not in my secret map" is not: the caller chose that `kid`, and echoing per-attempt verifier state back to whoever is probing turns a rejection into an oracle. `docs/proxy-proof-spec.md` §6 states this as a hard requirement for proxy proof, and this spec keeps it: every proxy-proof outcome — absent, malformed, unknown key, expired, bad MAC, replayed — collapses onto the single code `proxy_required`.

Implementations MUST NOT add codes outside §3. A failure that maps onto none of them uses `unauthorized`. Keeping the set closed is what makes it usable: a client that switches on the code needs to know the set will not grow under it in a language it does not control.

## 3. Reason codes

| Code | Meaning | What the caller should do |
|---|---|---|
| `missing_credential` | No credential was presented at all. | Send one. |
| `invalid_credential` | A credential was presented and rejected. | Fix or re-obtain it; do not retry unchanged. |
| `expired_credential` | A well-formed credential outside its validity window. | Refresh and retry. |
| `insufficient_scope` | The caller was identified but is not permitted. | Do not retry; escalate. |
| `proxy_required` | The request did not carry evidence that it arrived through the trusted proxy. | Almost always an operator's problem, not the caller's — see §5. |
| `unauthorized` | Refused, unclassified. | Fallback. |

`insufficient_scope` is deliberately a **401**, not a 403, because the framework's authenticate callback runs before any method is resolved: there is no route yet whose permissions could be evaluated. A service that wants a true 403 raises it from the method body.

### 3.1 Composition

When several alternative credentials are tried (`chain_authenticate` and its equivalents), the code reported is:

- `missing_credential` only when **every** alternative agreed nothing was presented — the one case where "send a credential" is actionable advice.
- otherwise the first code that is not `missing_credential`.

When a precondition gate and a credential are ANDed (`require_all`), the gate runs first, so a gate failure reports the gate's code and the credential is never consulted.

## 4. Response

### 4.1 Headers

| Header | When | Value |
|---|---|---|
| `VGI-Auth-Reason` | every 401 | One code from §3. |
| `VGI-Auth-Proxy-Required` | 401s from a service whose auth depends on a proxy (§5) | `"true"`. Omitted otherwise — never `"false"`. |
| `Cache-Control` | every 401 | MUST prevent shared caching (`no-store` in the reference). A 401 is per-request and flips to 200 on the next attempt with a credential. |
| `WWW-Authenticate` | when the service declares a challenge | Unchanged from RFC 7235 / RFC 9728 behaviour. |

Both `VGI-` headers describe a rejection, so they MUST NOT be emitted on successful responses — they are not capability advertisements. When the service uses CORS, both MUST appear in `Access-Control-Expose-Headers`, otherwise a browser client cannot read them cross-origin and is back to guessing from the body.

### 4.2 Content negotiation

| Request `Accept` contains `text/html` | Response |
|---|---|
| yes | The service's styled HTML 401 page. |
| no (including `*/*` and an absent header) | The JSON envelope of §4.3. |

Substring-matching `Accept` rather than full media-type negotiation is intentional and matches the OAuth browser-redirect rule: the only clients that ask for `text/html` are browsers, and an RPC client sends `*/*`, which MUST resolve to JSON. A service MAY skip the HTML page entirely and always answer with JSON; a service MUST NOT answer a non-HTML request with HTML.

The HTML page is presentation, not contract — nothing may be parsed out of it. When a service renders one it SHOULD show the reason code and, when applicable, the §5 note, since a human reading the page is exactly the audience for both.

### 4.3 JSON envelope

```json
{
  "error": "unauthorized",
  "reason": "proxy_required",
  "detail": "Missing x-forwarded-client-cert header",
  "proxy_hint": "This service only accepts requests that arrive through its configured reverse proxy, which must set the x-forwarded-client-cert header. …"
}
```

| Field | Required | Notes |
|---|---|---|
| `error` | yes | Always the literal `"unauthorized"`. Marks the envelope kind so a client can tell it from a framework's default error JSON. |
| `reason` | yes | A code from §3. |
| `detail` | yes | Human-readable, may be `""`. Free text, subject to §2 — never a verifier's per-attempt state. |
| `proxy_hint` | no | Present only under §5. **Absent, not empty**, when it does not apply, so its presence alone is a usable signal. |

`Content-Type` MUST be `application/json`. Readers MUST ignore unknown fields, and MUST treat an unrecognised `reason` as `unauthorized` — that means the server is newer, not broken.

## 5. The proxy note

A service whose authentication can only succeed on requests a reverse proxy has stamped MUST set `VGI-Auth-Proxy-Required: true` and include `proxy_hint` on **every** 401 it produces.

**The note is derived from server configuration, not from what failed on this request.** A worker that requires proxy-injected evidence emits the identical note whether the proof was absent, the certificate was expired, or the bearer token behind the proxy was simply wrong. Two consequences follow, and both are the point:

1. It discloses nothing. The note restates a static property the service already advertises through its capability headers, so it cannot be used to probe which stage rejected a given attempt. This is what lets it coexist with the uniform-rejection rule of `docs/proxy-proof-spec.md` §6.
2. It is still right in the case that matters. When a deployment's proxy is not forwarding the header, *every* request 401s, and every one of those 401s says so.

A service MUST NOT emit the note when its authentication does not depend on a proxy — a note that appears everywhere teaches operators to ignore it.

### 5.1 Discovering the dependency

An implementation SHOULD discover the dependency from the authenticators it has installed rather than requiring the operator to restate it: the built-in mTLS and proxy-proof authenticators know which header they read. Composition helpers (chain, require-all) MUST carry declarations through, otherwise wrapping an authenticator silently drops the note. An implementation MUST also offer a direct way for an operator to state header names for a custom authenticator the framework cannot introspect — `make_wsgi_app(proxy_auth_headers=[...])` in the reference.

A proxy-proof gate contributes its header only in `require` mode. In `allow` mode an absent proof never denies, so the note would misdirect.

### 5.2 Wording

The note text is not normative — it is prose for a human. It MUST convey: that the service is only reachable through its proxy; which header names the proxy must set; and that a rejection here is at least as likely to be a proxy misconfiguration as a bad credential.

## 6. Client behaviour

A client receiving a 401 MUST:

- read `reason` from the JSON body when present, falling back to `VGI-Auth-Reason`, falling back to `unauthorized`;
- surface `proxy_hint` to whoever sees the error. The reference appends it to the exception message rather than leaving it on an attribute alone, because the place it actually gets read is a traceback in a deployment log;
- degrade without raising when the body is not the §4.3 envelope. A 401 can come from an intermediary the service never sees — a gateway, a WAF, an SSO portal — with its own idea of an error body. The reference keeps a bounded prefix of such a body, and replaces an HTML page with a one-line note rather than pasting markup into an exception message.

A client MUST NOT attempt to parse an Arrow IPC stream from a 401 body. The rejection happens before any method is resolved, so no output schema exists.

## 7. Conformance

`TestUnauthorized` in `vgi_rpc/conformance/_pytest_suite.py` is the canonical suite. It is capability-gated on the server emitting `VGI-Auth-Reason`, so a port that has not adopted this contract skips rather than fails.

| Test | Asserts |
|---|---|
| `test_reason_header_present` | Every 401 carries `VGI-Auth-Reason`. |
| `test_reason_in_closed_set` | The code is one of §3. |
| `test_json_envelope_for_machine_clients` | `Accept: */*` yields `application/json` matching §4.3, with `reason` agreeing with the header. |
| `test_html_page_for_browsers` | `Accept: text/html` yields `text/html`, and the reason header is still present. |
| `test_not_cached` | `Cache-Control` forbids shared caching. |
| `test_no_proxy_header_without_proxy_auth` | A service with no proxy dependency omits `VGI-Auth-Proxy-Required` and `proxy_hint`. |
| `test_proxy_hint_when_proxy_required` | A require-mode proxy-proof worker sets the header and includes a non-empty `proxy_hint`. Gated on the `proof_worker_factory` fixture. |
| `test_proxy_rejection_is_uniform` | Absent, malformed, and bad-MAC proofs produce the *same* reason code and the same detail — the uniform-rejection rule of `docs/proxy-proof-spec.md` §6, asserted from the outside. |

Runners supply the existing `conformance_http_auth_port` fixture (a server whose RPC endpoints all 401) and, for the last two tests, `proof_worker_factory`.
