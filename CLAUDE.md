# vgi-rpc-sync

Lightweight sync tool for the three vgi-rpc implementations (Python, Go, TypeScript).

## Related Repositories

- **Python (reference)**: ~/Development/vgi-rpc — features land here first
- **Go**: ~/Development/vgi-rpc-go
- **TypeScript**: ~/Development/vgi-rpc-typescript

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
- **describe_diff.py** — Runs Python conformance in-process, spawns Go/TS conformance workers, calls `__describe__` via `vgi_rpc.introspect.introspect()`, compares method sets and schema versions. Run via `uv run python describe_diff.py` (uses vgi-rpc from pyproject.toml deps).

## Key Context

- Python is always the reference. Features are ported FROM Python TO Go/TypeScript.
- The conformance test suite (48 methods, ~217 tests) is defined in Python (`vgi_rpc.conformance`) and drives tests against Go/TS workers via subprocess.
- `__describe__` versions currently diverge: Python=3, Go=2, TypeScript=2. Go/TS are missing `is_exchange` and `param_docs_json` fields.
- Go conformance worker: built via `make build` in vgi-rpc-go, binary at `conformance-worker`
- TS conformance worker: `bun run examples/conformance.ts` in vgi-rpc-typescript

## Dependencies

- `make` (system)
- `uv` (for Python repo tests)
- `bun` (for TypeScript repo tests)
- `go` (for Go repo tests)
- `vgi-rpc` Python package (for describe_diff.py only): `pip install vgi-rpc`
