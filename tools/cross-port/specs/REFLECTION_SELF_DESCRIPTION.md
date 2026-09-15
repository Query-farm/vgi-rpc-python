# `vgi_rpc.Reflection.v1` must describe its own methods

Found by extending `describe_diff.py` to compare **every** hosted protocol
rather than only the application one. Four ports register reflection's binding
without registering its methods into it, so `describe("vgi_rpc.Reflection.v1")`
returns an empty method list.

```
python        ConformanceService(87), vgi_rpc.Reflection.v1(2)
go            ConformanceService(87), vgi_rpc.Reflection.v1(2)
typescript    ConformanceService(87), vgi_rpc.Reflection.v1(2)
rust          ConformanceService(87), vgi_rpc.Reflection.v1(0)   <-- 
java          ConformanceService(87), vgi_rpc.Reflection.v1(0)   <-- 
csharp        ConformanceService(87), vgi_rpc.Reflection.v1(0)   <-- 
cpp           ConformanceService(87), vgi_rpc.Reflection.v1(0)   <-- 

vgi_rpc.Reflection.v1: 2 distinct hashes
  fafffd66fd1b98ee355cbcbd6a0fbe6f32b09088512bd1f04268a0a9427d79e9  cpp, csharp, java, rust
  3c7db4cae8cdfc93dc4a76e73b8b759e18e45e6a5811adba4e520366344b919a  go, python, typescript  <- reference
    missing: describe, list_protocols
```

## Why it is a bug, not a defensible choice

The plan states it directly: *"Self-description is not special-cased"* and
*"Reflection describes itself"*. The reference's own test says so in its name —
`test_lists_every_hosted_protocol`: *"Including reflection itself — self-description
is not special-cased."*

A client discovers a server the documented way: `list_protocols`, then `describe`
for each protocol it cares about. On these four ports that sequence advertises
`vgi_rpc.Reflection.v1` and then says it has no methods, so a client cannot learn
how to call the protocol it is already calling. Each port is internally
self-consistent — `list_protocols` advertises the same digest its `describe`
returns — which is exactly why no port's own tests caught it. **It is only visible
by comparing ports to each other**, and the guard could not do that until now.

One rationalisation is already on record and should not be re-derived: that
reflection's method *table* is "honestly empty" while its dispatchable *names* are
the two it answers. That inverts the contract. The table is what `describe`
reports and what the hash is computed over, so an empty table is not honesty
about an empty protocol — it is a protocol lying about itself.

## Target

`describe("vgi_rpc.Reflection.v1")` returns both methods, and the binding hashes
to **`3c7db4cae8cdfc93dc4a76e73b8b759e18e45e6a5811adba4e520366344b919a`**.

Canonical preimage — diff against this if your digest disagrees:

```json
{"methods":[{"has_header":false,"has_return":true,"name":"describe","params":[{"name":"protocol","nullable":false,"type":"utf8"}],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"},{"has_header":false,"has_return":true,"name":"list_protocols","params":[],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"}],"protocol":"vgi_rpc.Reflection.v1"}
```

Both methods are UNARY, `has_return = true`, `has_header = false`, result column
`result: binary non-null`. `describe` takes one `utf8 non-null` parameter named
`protocol`; `list_protocols` takes none. Methods are sorted by name in the
preimage, so `describe` precedes `list_protocols`.

## Constraints

- The application protocol hash `5cc768771c2e8a54e19ebb7546c97c119823eb13e20a5ff62ca5ce7ed2a1334e`
  MUST NOT move, nor the three `vgi_rpc.Identity.v1` digests (`8317f2ad…`,
  `27b75bef…`, `c71b12f4…`). Only reflection's own digest changes.
- Registering the methods must not change how reflection **dispatches** — it
  already answers both. This is about what its binding *contains*.
- Reflection stays exempt from the version gate.

## Verify

```
cd ~/Development/vgi-rpc-python && uv run python ~/Development/vgi-rpc-python/tools/cross-port/describe_diff.py
```

must report `vgi_rpc.Reflection.v1  7/7 ports  AGREED` at `3c7db4ca…`, with
`ConformanceService` still AGREED at `5cc76877…`.

Add a port-local test pinning reflection's digest and its two-method shape, so
this cannot regress without the port's own suite failing — the whole reason it
survived this long is that nothing local could see it.
