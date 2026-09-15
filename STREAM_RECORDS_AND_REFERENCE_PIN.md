# Two consistency items: stream access records, and which reference you test against

---

# Task A — HTTP streams must emit access records

## The rule

`docs/access-log-spec.md`: **one record per RPC call.** For a stream that is the
`init` **and every continuation**, all sharing one `stream_id`.

| field | on a stream record |
|---|---|
| `method_type` | `"stream"` |
| `stream_id` | required; 32 lowercase hex, no dashes; **identical across init and every continuation of the same call** |
| `request_data` | required on the **init** record; **absent** on continuations |
| `response_state` | base64 of the *decrypted* outbound state on init and on any continuation that produces a continuation token; absent on the terminal continuation |

## Why nothing caught this

A port that emits **nothing** for streams passes every existing check. The record
validator validates records that exist; no records means nothing to validate, and
the result reads as "clean" rather than "unexamined". Two ports were in exactly
that state and it took reading their source to find out.

Streams are the calls that run longest and move the most data, so a transport
that logs unary and not streams drops precisely the traffic an operator most
wants, while its log still looks well-formed.

## The check that now enforces it

`vgi_rpc/conformance/_pytest_suite.py::TestRequestId::test_a_stream_call_emits_one_record_per_turn`
drives `produce_n` over HTTP and asserts records exist, carry
`method_type == "stream"`, carry a well-formed `stream_id`, and that one call's
records share one id. Mutation-checked against the reference: suppressing its
stream emit turns the test red.

**If your runner provides `conformance_http_access_log`, this must PASS, not
skip.** If it skips, say which reason applies — no fixture, or no HTTP streams.

## What each port needs (verify, don't assume — my earlier table was wrong twice)

- **Rust, C++** — known to emit nothing on the HTTP stream path; both documented
  it as a deliberate gap. Close it.
- **TypeScript** — its four emit sites are stdio, HTTP, unix and tcp; confirm the
  HTTP one covers `init` **and** `exchange`, not just unary.
- **Go, Java, C#** — believed covered (Go tested both turns, Java emits from
  `HttpStreamHandler.beginTurn`, C# routes streams through a shared helper taking
  `streamId`). **Confirm** and make the conformance case pass; if it skips for
  want of a fixture, say so.

Where you close a gap, both identity fields must come from the resolved binding,
exactly as in the previous round — the source-walking or compile-time guards
several ports added should already fail the build if a new emit site forgets.

C++ additionally has `__upload_url__` emitting no record. It is a framework
endpoint owned by no protocol, so it logs the server's **primary** — that part is
specified. Emitting nothing is still a gap; close it if the entry point allows,
and say so if it needs new public surface.

---

# Task B — test against the reference that has the work in it

## The problem

There are two Python repos and they are **not** the same:

| path | what it is |
|---|---|
| `~/Development/vgi-rpc` | `main`, v0.45.3. **No multiservice work at all** — no routing key, flat routes, `__describe__` still live. |
| `~/Development/vgi-rpc-python` | branch `multiservice/pr1-internal`. The canonical reference for everything in this effort. |

The version numbers are misleading: the stale one is *numerically higher*.

A harness pinned to the first tests against a reference that refuses every
namespaced call, which is why several ports saw large "pre-existing failure"
counts that were really "wrong reference". It also means an agent reading
`CLAUDE.md` is told the canonical Python lives in a tree with none of this work.

## Known stale pins

- **Rust** — `scripts/conf.py:33` hardcodes
  `/Users/rusty/Development/vgi-rpc/.venv/bin/python` (an absolute path, not
  overridable), plus `README.md:126` and `CLAUDE.md:24`.
- **Java** — `CLAUDE.md` lines 47, 109, 156, 157 still name the stale tree as
  authoritative, although `run_tests.sh` was fixed.
- **C#** — `docs/wire-protocol.md` and `test_csharp_conformance.py:774` cite
  specs in the stale tree.
- **TypeScript** — `test/launcher.hash.test.ts:10`; also check whatever
  `VGI_RPC_PYTHON_BIN` defaults to.
- **Go, C++** — already clean; C++ fixed an unconditional `sys.path` insert that
  shadowed the current reference.

## What to do

Point at `~/Development/vgi-rpc-python`, and make it **overridable by
environment variable** rather than hardcoded — a machine-specific absolute path
in a committed script is why this went unnoticed. Follow the existing convention
in your repo (`VGI_RPC_PYTHON`, `VGI_RPC_PYTHON_BIN`, …); if there is none, add
one and document it.

Then **re-measure your baseline**. Several ports' failure counts were inflated by
the stale reference; the number you report after this may be very different, and
that difference is information, not noise. Say what it was and what it became.
