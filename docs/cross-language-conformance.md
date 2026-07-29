# Cross-Language Conformance Testing

The `vgi-rpc-test` CLI tool runs the conformance suite against any worker that speaks the vgi-rpc wire protocol (Arrow IPC over stdin/stdout, HTTP, Unix sockets, or TCP sockets).

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
```

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
