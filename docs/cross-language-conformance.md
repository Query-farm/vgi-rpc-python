# Cross-Language Conformance Testing

The `vgi-rpc-test` CLI tool runs the conformance suite against any worker that speaks the vgi-rpc wire protocol (Arrow IPC over stdin/stdout, HTTP, Unix sockets, or TCP sockets).

The CLI validates a worker/server implementation by driving it with the
Python reference client. It does not exercise the client implementation in a
foreign-language SDK. Each port must therefore also run local client tests
against the Python reference worker, especially for schema-sensitive exchange
inputs such as all-null, dictionary-encoded, and nested columns. The exact
worker and schema contract are defined in
[Native-client conformance](native-client-conformance.md).

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

Some conformance tests target opt-in HTTP features. They run only when the server's `OPTIONS /health` capability headers advertise the feature, and skip cleanly otherwise — so a port that doesn't implement the feature stays fully conformant on the core wire surface while the dedicated suite verifies anyone who does opt in. HTTP body compression is no longer in that category: the primary CI conformance worker must advertise and implement both `zstd` and `gzip` in both directions.

| Capability header | Test group | Spec |
|---|---|---|
| `VGI-Sticky-Enabled: true` | `Sticky::*` | [sticky-sessions-spec.md](sticky-sessions-spec.md) |
| `VGI-Accept-Max-Response-Bytes-Support: true` | `TestHttpResponseCap*` | [http-response-budgets.md](http-response-budgets.md) |

### Fixture-gated tests

Some tests can't run against one already-running server, because the state under test *is* a server configuration — a short session TTL, two workers sharing a key, a worker that authenticates. Those are supplied by the runner as named pytest fixtures, so they are reachable from the pytest suite but not from `vgi-rpc-test --url`.

| Fixture | Tests | Missing fixture means | Spec |
|---|---|---|---|
| `proof_worker_factory` | `TestProxyProof` | skip — the feature is wholly optional | [proxy-proof-spec.md](proxy-proof-spec.md) |
| `conformance_http_no_compression_port` | `test_empty_advertisement_means_never_compressed` | skip | — |
| `conformance_http_small_request_cap_port` | `TestCompressedHttpRequestCap` | skip — requires a worker advertising a deliberately small `max_request_bytes` cap | [WIRE_PROTOCOL.md](WIRE_PROTOCOL.md#content-encoding-negotiation) |
| `conformance_http_strict_cap_port` | `TestHttpResponseCap`, `TestHttpResponseCapProducer` | skip — requires a worker with a deliberately small decoded response cap | [http-response-budgets.md](http-response-budgets.md) |
| `conformance_http_external_security_port` | `TestExternalFetchSecurity` | skip — requires a server wired to the fake-storage redirect routes and small encoded/decoded caps | [WIRE_PROTOCOL.md](WIRE_PROTOCOL.md#fetch-safety) |
| `conformance_http_sticky_short_ttl_port`<br>`conformance_http_sticky_peer_ports`<br>`conformance_http_sticky_auth_port` | the three `TestSticky` failure paths | **fail**, if the server advertises `VGI-Sticky-Enabled` — skip otherwise | [sticky-sessions-spec.md](sticky-sessions-spec.md) §9.1 |
| `conformance_resource_soak_target` | `TestResourceSoak` | skip — requires a dedicated process PID and connection factory | Resource soak contract below |

The external-security fixture uses the shared fake-storage service, a 4 KiB
encoded cap, an 8 KiB decoded cap, and a URL policy that permits
`127.0.0.1` but rejects the fake service's `localhost` redirect alias. Ports
may express those settings through their native configuration API; they do not
need to copy the Python worker's command-line flags.

The small-request-cap fixture advertises a 4 KiB `max_request_bytes` limit and
enables the mandatory `zstd` and `gzip` request codecs. The shared group checks
the cap independently on the encoded HTTP body and the decoded Arrow body,
then reuses the same HTTP client after each rejection. The separate
`conformance_http_no_compression_port` fixture remains valid: it exercises an
explicitly compression-disabled deployment, not the primary CI worker profile.

Every port also consumes
`vgi_rpc/conformance/http_response_budget_vectors.json`. It freezes the ASCII
numeric grammar, the `2^53-1` portable maximum, minimum-based precedence, and
the cursor-free `ResponseTooLargeError` envelope.

The sticky row is the one to note: a port may decline sticky entirely, but a port that *claims* it cannot quietly omit the tests that prove sessions are refused when they should be. Everything else on this page skips silently when unsupplied, which is why the sticky fixtures name themselves in the failure message.

### Resource soak contract

`TestResourceSoak` is a black-box retained-resource check rather than a wire
feature. A runner opts in with a **function-scoped**
`conformance_resource_soak_target` fixture returning
`ResourceSoakTarget(name, pid, connect, limits, warmup_multiplier=1)` from
`vgi_rpc.conformance._resource_soak_pytest`:

- `pid` identifies one newly started, otherwise idle worker process;
- `connect()` returns a fresh context-managed `ConformanceService` client;
- `limits` supplies runtime-specific RSS budgets while descriptor/handle,
  thread, and child-process drift stays near exact; and
- `warmup_multiplier` may be raised for runtimes whose allocator, JIT, or
  thread pools need more representative traffic before reaching steady state.
  Warm-up is one full measured workload times this value, so it remains
  representative when `VGI_RPC_SOAK_SCALE` changes; it never reduces the
  measured operation count or relaxes a budget; and
- teardown stops and reaps that worker after the scenario.

Do not point this fixture at a session-wide worker shared with unrelated tests.
Warm-up deliberately precedes the baseline, but another test opening sockets,
loading modules, or triggering a JVM/JIT in the measured PID still makes the
result meaningless.

The default PR-scale tranche runs 1,000 unary operations, 150 stream
completion/error/cancellation operations, and 100 fresh connections. Set
`VGI_RPC_SOAK_SCALE` to an integer from 1 through 20 to multiply each epoch
without letting any one pytest case exceed the suite's 50-second ceiling.
`VGI_RPC_SOAK_REPORT_DIR` writes one JSON file per scenario containing the
post-warm-up baseline, every epoch sample, operation count, and least-squares
RSS slope. CI should retain these reports when a job fails so a threshold
failure is diagnosable rather than merely red.

The portable sampler observes RSS, Unix file descriptors or Windows handles,
native threads, and recursive child processes. It intentionally complements,
rather than replaces, language-local exact accounting for Arrow allocators,
shared-memory blocks, task/goroutine registries, and sticky-session entries.

`VGI-Proxy-Proof-Required: true` is what an *operator or proxy* reads to confirm a deployed worker enforces (see [proxy-proof-spec.md](proxy-proof-spec.md) §2.2); the conformance suite asserts the header rather than being gated on it.

Filter for one group with `--filter`:

```bash
vgi-rpc-test --url http://<server> --filter "Sticky::*"
```
