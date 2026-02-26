# Cross-Language Conformance Testing

The `vgi-rpc-test` CLI tool runs the conformance suite against any worker that speaks the vgi-rpc wire protocol (Arrow IPC over stdin/stdout, HTTP, or Unix sockets).

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
