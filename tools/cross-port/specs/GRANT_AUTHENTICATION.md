# Grant authentication — decision record (2026-10-06)

Normative text: `docs/WIRE_PROTOCOL.md` §16 "Accepting identity credentials"
and `IDENTITY_V1_SPEC.md` §9. Fixture: `IDENTITY_CONFORMANCE_FIXTURE.md` §10.
Vectors: `vgi_rpc/conformance/grant_token_vectors.json`.

**Finding.** `issue_grant` mints a credential for automation to present later
as an ordinary bearer. No port and no SDK accepted one, and nothing fed
`resolve_token` back into authentication either. The grant loop was open
everywhere.

| # | Decision | Why |
|---|---|---|
| G1 | **Both** built-in sealed grants (opt-in by key) **and** `resolve_token`-backed bearer auth | Sealed grants close the loop with no storage or author code; `resolve_token` covers deployments whose credentials live elsewhere |
| G2 | Opt-in: no key ⇒ nothing changes | Absent beats hosted-and-refusing; upgrading must not grow a credential issuer |
| G3 | Reuse the state-token AEAD (XChaCha20-Poly1305, `version‖nonce‖ct+tag`) | Every port already implements and tests it; a second cipher is a second thing to get wrong |
| G4 | Fixed binary payload, not JSON | Byte-exact vectors across seven languages; the canonical-JSON code ports have rejects numbers |
| G5 | `kid` in the clear, bound by AAD; audience in the AAD | Key selection without trial decryption; a shared key across deployments still does not cross audiences |
| G6 | Version in the prefix (`vgig1.`) | An incompatible format routes elsewhere instead of half-parsing |
| G7 | Canonical unpadded base64url only | One token, one spelling |
| G8 | No `auth_time` in grant/token AuthContext | Grants never mint grants — the existing freshness guard does it, no new check |
| G9 | Bad `vgig1.` token stops the chain (401) | A forged or stale grant must not get a second chance from a resolver that may answer for anything |
| G10 | Not individually revocable | No storage is the point; short max TTL (default 7 days), re-issue, key removal revokes all |
| G11 | `resolve_token` outage ⇒ 503 + `Retry-After` | Same rule as every authenticator; a blip must not log a fleet out |
| G12 | Refuse to OR alternatives beside a proxy-evidence authenticator | OR semantics would bypass the gate |
