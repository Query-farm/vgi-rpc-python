# Feature: raw-TCP socket transport across all ports

**Status:** RESOLVED — landed + released in all five mature ports (2026-06-26)
**Reference:** ~/Development/vgi-rpc (Python), commit `8ea49aa`

## Summary

A raw-TCP socket transport — the network analog of the existing Unix-socket
transport. It speaks the **same raw Arrow-IPC framing** as the pipe/unix
transports (no HTTP envelope); only the listening socket differs (AF_INET,
`host:port`). Loopback-only by default (`127.0.0.1`), `port 0` auto-selects,
`TCP_NODELAY` enabled. **No auth/TLS** — trusted networks only; use HTTP
otherwise.

Surface (Python reference; each port mirrors the *behaviour* with its own
idioms/libraries):

- `TcpTransport` / `serve_tcp` / `tcp_connect`, `make_tcp_pair`.
- `TransportKind.TCP`; workers see `ctx.kind == TCP`.
- `run_server --tcp [HOST:]PORT` worker flag, emitting a `TCP:<host>:<port>`
  discovery line (analogous to `UNIX:<path>`).
- Conformance harness: `vgi-rpc-test --tcp <host>:<port>` and
  `vgi-rpc-conformance --tcp [HOST:]PORT`.

## Per-port status

| Port | Repo | TCP commit | Released version | Conformance over `--tcp` |
|------|------|-----------|------------------|--------------------------|
| Python (reference) | vgi-rpc / vgi-rpc-python | `8ea49aa` | **0.21.0** (PyPI) | full gate green |
| Go | vgi-rpc-go | `3ed0aaa` (+ CI fix `b38d0cb`) | **v0.10.0** (module tag) | 98 pass / 4 skip = `--unix` baseline |
| TypeScript | vgi-rpc-typescript | `f1563f5` | **0.8.0** (npm) | 98 pass / 4 skip = pipe baseline |
| Rust | vgi-rpc-rust | `87ec06c` | **0.6.0** (crates.io) | 98 pass / 4 skip = `--unix` baseline |
| Java | vgi-rpc-java | `36aae83` | **0.11.0** (Maven Central) | 98 pass / 4 skip = `--unix` baseline |

The 4 skips are identical everywhere — the HTTP-only `http_response_cap.*`
tests, correctly excluded for a raw-framing transport.

## Notes

- **Out of scope:** C++ (no socket-transport scaffold yet) and Swift (empty
  repo). TLS/auth for raw TCP (HTTP remains the secure-network path).
- **No worker-to-worker interop tests** were added — cross-language conformance
  remains single-worker (the Python `vgi-rpc-test` harness drives one port's
  worker), which is the established pattern.
- **Go CI** installs the `vgi-rpc` reference from the default branch (source),
  matching Java's pattern, so the TCP conformance fixture resolves `tcp_connect`
  ahead of a PyPI release. (Now moot since 0.21.0 is on PyPI, but kept for
  robustness against future unreleased-reference drift.)
