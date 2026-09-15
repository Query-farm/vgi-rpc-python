# vgi-rpc-sync

Lightweight sync tool across the vgi-rpc implementations (Python, Go, TypeScript, Rust, Java, C#, C++).

## Related Repositories

- **Python (reference)**: ~/Development/vgi-rpc-python — features land here first
- **Go**: ~/Development/vgi-rpc-go
- **TypeScript**: ~/Development/vgi-rpc-typescript
- **Rust**: ~/Development/vgi-rpc-rust
- **Java**: ~/Development/vgi-rpc-java
- **C#**: ~/Development/vgi-rpc-csharp
- **C++**: ~/Development/vgi-rpc-c++

## Usage

```bash
make status       # show versions, git state, describe versions across all repos
make test-all     # run conformance tests in all three repos sequentially
make test-go      # run Go conformance only
make test-ts      # run TypeScript conformance only

./port.py <feature> --to <go|typescript>   # generate Claude porting prompt
./port.py <feature> --to go | claude       # pipe directly to Claude Code

uv run python describe_diff.py             # compare __describe__ across implementations
```

## Architecture

This is intentionally minimal — a Makefile and two small Python scripts.

- **Makefile** — Wraps `make test` in each repo. Shows versions by grepping config files.
- **port.py** — Generates structured prompts for Claude Code to port features from Python to Go/TypeScript. Convention-based file lookup: feature `foo` maps to `vgi_rpc/foo.py` + `tests/test_foo.py`. Use `--source` to override if convention fails.
- **describe_diff.py** — Runs Python conformance in-process, spawns every other port's conformance worker, and describes each over `vgi_rpc.Reflection.v1`. The headline check is **`protocol_hash` equality**: the hash is defined over canonical JSON of the decoded description (WIRE_PROTOCOL §14) precisely so it compares across ports, which makes this the one mechanical drift guard. A mismatch is a JSON diff, not a guess — every port ships the preimage beside its digest. Exits 0 when all reachable ports agree, 1 on drift, 2 when some ports could not be reached (a port not checked is not a port that agrees). It resolves `vgi-rpc` from the local `../vgi-rpc-python` working copy, not a release: the job is to compare ports against what the reference *currently* does.

## Key Context

- Python is always the reference. Features are ported FROM Python TO Go/TypeScript.
- The conformance test suite (48 methods, ~217 tests) is defined in Python (`vgi_rpc.conformance`) and drives tests against Go/TS workers via subprocess.
- **All six non-Python ports are pre-2.0 as of 2026-09-15.** Python has landed multi-protocol hosting: dispatch resolves `(vgi_rpc.protocol, vgi_rpc.method)`, introspection is the co-hosted `vgi_rpc.Reflection.v1` rather than a `__describe__` method name, identity is `vgi_rpc.Identity.v1` rather than an HTTP JSON route, and `protocol_hash` is defined over canonical JSON so it is comparable across ports. `describe_diff.py` reports each port's state; Go, TypeScript and Rust connect and answer "does not host vgi_rpc.Reflection.v1", which is the expected pre-port state. See `~/.claude/plans/okay-so-write-up-binary-russell.md`.
- Go conformance worker: built via `make build` in vgi-rpc-go, binary at `conformance-worker`
- TS conformance worker: `bun run examples/conformance.ts` in vgi-rpc-typescript

## Dependencies

- `make` (system)
- `uv` (for Python repo tests)
- `bun` (for TypeScript repo tests)
- `go` (for Go repo tests)
- `vgi-rpc` Python package (for describe_diff.py only): `pip install vgi-rpc`
