# Two fleet-wide changes: retire `__describe__`, and label access records per binding

Both are finished in `vgi-rpc-python`. Read the commits before starting:
`3a1f0ad` (retirement), `27be314` (access-log hash), `320df5e` (the conformance
assertion that now enforces it).

---

# Task A — retire `__describe__` completely

## Where things actually stand

The retirement is half-finished across the fleet, which is worse than either
finishing or not starting it: cross-implementation introspection currently works
by accident of which client/server pair you connect.

| | server answers it | client asked for it |
|---|---|---|
| Python | no (retired) | no |
| Go | **yes** | no |
| TypeScript | **yes** | **yes** (introspection bootstrap) |
| Rust | no | **yes** (`introspect.rs`) |
| Java | no | no |
| C# | no | no |
| C++ | no | **fixed this week** |

Plus: every port has a conformance harness (`test_<port>_conformance.py`) with a
fixture that calls `__describe__` over the wire, and Go's `hooks.go:35` still
defines `ProtocolHash` as "SHA-256 hex of canonical `__describe__` payload",
which has not been true since the canonical hash landed.

## Target state

1. **No server answers `__describe__` as a method.** Delete the handler, the
   registration, and any `enable_describe`-style flag that only existed to turn
   it on. Where a flag also controls *reflection* registration, keep that.

2. **`__describe__` is refused with a message naming its replacement.** Not
   "unknown method" — that is indistinguishable from "this server was built
   without introspection", and the two need opposite fixes (update the client
   vs. reconfigure the server). Match the reference's text:

   > `'__describe__'` was retired. Introspection is now the
   > `'vgi_rpc.Reflection.v1'` protocol: call `'list_protocols'` for what this
   > server hosts, then `'describe'` for one protocol's methods.

   Only `__describe__` is special-cased. Every other reserved name keeps the
   plain capability answer, which is what a client probing for an optional
   method needs.

3. **No client calls it.** Introspection is `list_protocols` then
   `describe(protocol)`, both on `vgi_rpc.Reflection.v1`, which is exempt from
   the version gate. Accept an explicit protocol name to skip the first hop.

4. **Harnesses and docs move too.** The `test_<port>_conformance.py` describe
   fixture uses reflection; comments defining the protocol hash "over the
   `__describe__` payload" are corrected — the hash is over the canonical
   description (`WIRE_PROTOCOL.md §14`), and has been since the seven ports
   agreed on a digest.

5. **Keep any legacy machinery `__describe__` shares with something live.** C++
   found `describe.cpp` also owns the `compute_protocol_hash` feeding the access
   log; untangling that is part of this task, not a reason to skip it, but do it
   deliberately rather than by deleting the file.

---

# Task B — access records carry the OWNING binding's protocol and hash

## The rule (already normative, `docs/access-log-spec.md` §3)

- `protocol` — "the wire name of the protocol that **owns the dispatched
  method** … not a server-wide default".
- `protocol_hash` — the canonical digest of **that** protocol, and "the registry
  key when decoding archived records".

Framework endpoints owned by no protocol (`__transport_options__`,
`__upload_url__`) log the server's primary. That is the specified behaviour, not
a fallback to be cleaned up.

## Why this is ranked highest in the plan

It is the only failure in the whole multi-service change that **fails silently**.
A mislabelled record is well-formed, passes the schema, and produces a plausible
dashboard. Nothing errors. A consumer keying its registry on `protocol_hash`
decodes the record against the wrong protocol's description and gets fields that
look real.

And it is invisible to every test that does not call a *secondary* protocol,
because for an application method the primary **is** the owning binding.

## What the ports currently do — three positions, and the two deferrals are honest

- **Python, C#:** log the owning protocol.
- **Go:** logs, but with the server's service name. Go deferred deliberately:
  its raw-transport path does the same, so fixing HTTP alone would have made
  the two transports disagree. It is a port-wide gap, not an HTTP one.
- **Java:** does not log framework protocols at all, mirroring how it treats
  Reflection. Also deliberate: its `DispatchInfo.protocol` is populated from the
  application protocol, so routing identity through the hook unchanged would
  file it under the wrong name — and Java judged that no record beats a
  confidently mislabelled one.

Both deferrals point at the same root cause: **the protocol is captured at
hook-registration or server level rather than read from the resolved binding.**
Fix that, and both positions collapse into the correct one. Neither port was
wrong to wait; the fix was cross-cutting and is now specified.

**The reference had a third version of the same bug** — `protocol` was
per-binding but `protocol_hash` was the primary's at all six binding-owned emit
sites, so a reflection record named one protocol and carried another's digest.
Worse than either field being wrong alone. Fixed in `27be314`.

## What to do

1. Read the protocol name **and** hash from the resolved binding at every emit
   site, on every transport. The reference added `protocol_hash_for(info)`
   alongside its existing `implementation_for(info)`; mirror whatever your port
   calls that.
2. Count your emit sites first and fix them all. The reference had seven, spread
   across two HTTP dispatchers, a stream resource and three raw-transport paths.
   A missed one reintroduces this silently.
3. Java specifically: start logging framework protocols, now that the label will
   be right.
4. Go specifically: fix both transports together, which is what you were waiting
   for. Note your own warning that `binding.Hash` is the canonical hash while
   `ProtocolHash()` is the legacy byte-based one — the access-log conformance
   validator asserts the canonical digest.

## The check that now enforces it

`vgi_rpc/conformance/_pytest_suite.py` gained
`test_a_secondary_protocols_record_carries_its_own_identity`: it drives a
reflection call, then asserts the record names `vgi_rpc.Reflection.v1` and does
not carry the application protocol's digest. Gated on the optional
`conformance_http_access_log` fixture and skipped where the worker does not host
reflection over HTTP.

**If your runner provides that fixture, this test must pass, not skip.** If it
skips, say so and say which of the two reasons applies — a skip that should have
been a pass is how this stayed hidden.

---

# Constraints for both tasks

- Do NOT move the primary conformance hash
  `5cc768771c2e8a54e19ebb7546c97c119823eb13e20a5ff62ca5ce7ed2a1334e` or the
  three `vgi_rpc.Identity.v1` digests (`8317f2ad…`, `27b75bef…`, `c71b12f4…`).
- Mutation-check what you add: break the per-binding lookup and confirm a test
  goes red. A test that passes against a deliberately broken implementation is
  worse than no test, because it is counted as coverage.
- Any warning or test failure in code you touched is yours. Verify a
  pre-existing claim by stashing and re-running, and say that you did.
