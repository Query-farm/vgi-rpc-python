# Cross-Language Conformance Testing

The `vgi-rpc-test` CLI tool runs the conformance suite against any worker that speaks the vgi-rpc wire protocol (Arrow IPC over stdin/stdout, HTTP, Unix sockets, or TCP sockets).

The CLI validates a worker/server implementation by driving it with the
Python reference client. It does not exercise the client implementation in a
foreign-language SDK. Each port must therefore also run local client tests
against the Python reference worker, especially for schema-sensitive exchange
inputs such as all-null, dictionary-encoded, and nested columns.

## Install

```bash
pip install vgi-rpc
```

## Transport Options (pick one)

```bash
# Pipe transport — launches your worker as a subprocess
vgi-rpc-test --cmd "./my-worker"
vgi-rpc-test --cmd "java -jar my-worker.jar"
vgi-rpc-test -c "./path/to/worker --some-flag"

# HTTP transport — connects to a running HTTP server
vgi-rpc-test --url http://localhost:8000
vgi-rpc-test --url http://localhost:8000 --prefix /custom  # default prefix: /vgi

# Unix socket transport
vgi-rpc-test --unix /tmp/my-worker.sock

# TCP socket transport — raw framing, no auth/TLS (trusted networks only)
vgi-rpc-test --tcp 127.0.0.1:9000
vgi-rpc-test --tcp 9000              # host defaults to 127.0.0.1

# Pipe + shared memory
vgi-rpc-test --cmd "./my-worker" --shm 4194304  # 4MB segment
```

## Test Selection

```bash
# List all available tests
vgi-rpc-test --list

# Filter by glob patterns (comma-separated)
vgi-rpc-test --cmd "./my-worker" --filter "scalar_echo*,void*"
vgi-rpc-test --cmd "./my-worker" -k "producer_stream*"

# List tests matching a filter
vgi-rpc-test --list --filter "exchange*"

# Prefix a pattern with ! to exclude it; an exclusion always wins, and a
# filter made only of exclusions means "everything except these"
vgi-rpc-test --cmd "./my-worker" --filter '!large_payload.echo_binary_over_int32_max'
```

## Large payloads

`large_payload.echo_binary_4mib` crosses the pipe buffer many times over. It
catches a writer that never loops at all.

`large_payload.echo_binary_over_int32_max` allocates over 2 GiB on both the
client and the worker, takes seconds to run, and is **required** — it is the
only test that reaches the size where a single `write(2)` / `send(2)` stops
accepting a whole buffer, and the two failure modes it catches are both nasty:

- **Pipes on macOS** return a short count of exactly `INT_MAX` with *no error*.
  An implementation that trusts the return value drops the tail, and the peer
  then blocks forever waiting for bytes the Arrow IPC header promised. The
  symptom is a deadlock, not an exception.
- **Sockets on macOS** instead fail outright with `EINVAL`.

Handling both means your writer must loop on the returned count *and* clamp
each call to something below `INT_MAX`. Looping alone passes on pipes and
fails on sockets. Linux hides the whole problem — it silently caps a single
transfer at `0x7ffff000` and returns a short count that a correct loop
absorbs — so a Linux-only CI will not tell you whether you got this right.

**The read side needs the same treatment.** `recv` refuses an over-`INT_MAX`
buffer with `EINVAL` exactly as `send` does, so a reader must clamp its
requests too — and it must then *loop* to refill, because Arrow asks for a
whole message body in one call and reports a short read as a corrupt stream
rather than retrying. The reference had this bug on Unix and TCP sockets and
did not know it: buffering meant only some requests crossed `INT_MAX`, so
roughly one connection in two died mid-request while the rest passed. If your
port's huge-payload test is flaky rather than failing, look here first.

The test is limited to the `pipe`, `unix`, and `tcp` transports; HTTP bodies
take a different path with their own size limits. It carries a 300-second
timeout instead of the default 5, so it fails because the bytes were wrong
rather than because a slow machine ran out of stopwatch.

It was briefly opt-in, behind an environment variable, on the theory that no
CI should pay multiple GiB by default. That was a mistake: a conformance test
nobody runs enforces nothing, and this one guards a failure that presents as a
hung process rather than an error.

### Two acceptable answers

Required does not mean every port must round-trip 2 GiB. It means every port
must be *asked*, and must answer in one of two ways:

1. **Echo the payload back intact.** Head and tail are both checked, so a
   short write loses the tail and a misaligned resume corrupts the head.
2. **Refuse it with a typed error, and stay usable.** The test then issues an
   ordinary `echo_string` on the same connection; if that succeeds, the refusal
   passes and the report carries a `note:` naming it. If the connection is
   wedged, the test fails — a hung transport is the deadlock wearing a
   different hat.

The second answer exists because the ceiling is sometimes the *language's*.
The JVM caps a `byte[]` at `Integer.MAX_VALUE` **elements**, so a Java worker
cannot materialise 2³¹+1 bytes at any heap size — `-Xmx` is irrelevant. Holding
that port to a test it could only ever fail would get the test excluded from
its CI, and an excluded test enforces nothing either. So vgi-rpc-java refuses
with a message naming the field, the real size, and whose limit it is, and that
is conformant.

What is never acceptable is a **silent** answer: a short body, or a peer that
blocks forever waiting for bytes the header promised. No port is exempt from
those, and no runtime limit excuses them.

### If a runner genuinely cannot afford the memory

Exclude the test by name — an exclusion is a visible decision in the CI
configuration, which a silent skip is not:

```bash
vgi-rpc-test --cmd "./my-worker" --filter '!large_payload.echo_binary_over_int32_max'
```

Reach for this only for real resource limits on the runner. A *language*
ceiling does not need it: answer (2) above already covers that case, and it
keeps the test running.

## Output

```bash
# Auto-detect (table for TTY, JSON for pipes)
vgi-rpc-test --cmd "./my-worker"

# Force format
vgi-rpc-test --cmd "./my-worker" --format json
vgi-rpc-test --cmd "./my-worker" --format table

# Write to file
vgi-rpc-test --cmd "./my-worker" --format json --output results.json
```

## Debugging

```bash
# Show server log messages on stderr
vgi-rpc-test --cmd "./my-worker" --verbose

# Full debug logging (all vgi_rpc loggers at DEBUG)
vgi-rpc-test --cmd "./my-worker" --debug

# Target specific loggers at a specific level
vgi-rpc-test --cmd "./my-worker" --log-level DEBUG --log-logger vgi_rpc.wire.request
vgi-rpc-test --cmd "./my-worker" --log-level DEBUG --log-format json
```

## Access Log Conformance

Every conformant vgi-rpc worker MUST accept a `--access-log <path>` flag and write JSONL access-log records to that path. The CLI can validate them in the same run:

```bash
# Worker writes its access log to /tmp/worker.log; the CLI passes the
# same path through to the worker and validates the file afterwards.
vgi-rpc-test --cmd "./my-worker --access-log /tmp/worker.log" --access-log /tmp/worker.log
```

Validation is performed against [`access-log-spec.md`](access-log-spec.md) (machine-checkable form: [`vgi_rpc/access_log.schema.json`](https://github.com/Query-farm/vgi-rpc-python/blob/main/vgi_rpc/access_log.schema.json)). The CLI exit code reflects both suite success and access-log conformance.

## Exit Codes

- `0` — all tests passed
- `1` — one or more tests failed
- `2` — runner error (transport failure, missing arguments, etc.)

## Other

```bash
# Show version
vgi-rpc-test --version
```

## Reference

The conformance service protocol definition, data types, and reference implementation are in the `vgi_rpc.conformance` package. The reference Python worker can be tested with:

```bash
vgi-rpc-test --cmd "python -m tests.serve_conformance_pipe"
```

For wire protocol details, see [WIRE_PROTOCOL.md](WIRE_PROTOCOL.md).

## Capability-gated test groups

Some conformance tests target opt-in HTTP features. They run only when the server's `OPTIONS /health` capability headers advertise the feature, and skip cleanly otherwise — so a port that doesn't implement the feature stays fully conformant on the core wire surface while the dedicated suite verifies anyone who does opt in.

| Capability header | Test group | Spec |
|---|---|---|
| `VGI-Sticky-Enabled: true` | `Sticky::*` | [sticky-sessions-spec.md](sticky-sessions-spec.md) |

### Fixture-gated tests

Some tests can't run against one already-running server, because the state under test *is* a server configuration — a short session TTL, two workers sharing a key, a worker that authenticates. Those are supplied by the runner as named pytest fixtures, so they are reachable from the pytest suite but not from `vgi-rpc-test --url`.

| Fixture | Tests | Missing fixture means | Spec |
|---|---|---|---|
| `proof_worker_factory` | `TestProxyProof` | skip — the feature is wholly optional | [proxy-proof-spec.md](proxy-proof-spec.md) |
| `conformance_http_no_compression_port` | `test_empty_advertisement_means_never_compressed` | skip | — |
| `conformance_http_sticky_short_ttl_port`<br>`conformance_http_sticky_peer_ports`<br>`conformance_http_sticky_auth_port` | the three `TestSticky` failure paths | **fail**, if the server advertises `VGI-Sticky-Enabled` — skip otherwise | [sticky-sessions-spec.md](sticky-sessions-spec.md) §9.1 |

The sticky row is the one to note: a port may decline sticky entirely, but a port that *claims* it cannot quietly omit the tests that prove sessions are refused when they should be. Everything else on this page skips silently when unsupplied, which is why the sticky fixtures name themselves in the failure message.

`VGI-Proxy-Proof-Required: true` is what an *operator or proxy* reads to confirm a deployed worker enforces (see [proxy-proof-spec.md](proxy-proof-spec.md) §2.2); the conformance suite asserts the header rather than being gated on it.

Filter for one group with `--filter`:

```bash
vgi-rpc-test --url http://<server> --filter "Sticky::*"
```
