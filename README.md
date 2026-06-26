# vgi-rpc-sync

Keeps the [vgi-rpc](https://vgi-rpc.query.farm) implementations in sync:

| Language | Repo | Latest release | Role |
|----------|------|----------------|------|
| Python | `~/Development/vgi-rpc` | 0.21.0 (PyPI) | Reference implementation (features land here first) |
| Go | `~/Development/vgi-rpc-go` | v0.10.0 (module tag) | |
| TypeScript | `~/Development/vgi-rpc-typescript` | 0.8.0 (npm) | |
| Rust | `~/Development/vgi-rpc-rust` | 0.6.0 (crates.io) | |
| Java | `~/Development/vgi-rpc-java` | 0.11.0 (Maven Central) | |

> The `make`/`port.py` tooling below currently automates **Python, Go, and TypeScript**;
> Rust and Java are tracked here but not yet wired into the Makefile targets.
> Recent cross-port work is recorded under [`issues/`](issues/README.md) (see
> [feature-tcp-transport](issues/feature-tcp-transport.md)).

## Quick start

```bash
make status     # versions, git state, describe versions
make describe   # compare __describe__ across all three implementations
make test-all   # run conformance tests in all repos
```

## Porting features

Generate a Claude Code prompt for porting a Python feature to another language:

```bash
./port.py external --to go             # print prompt to stdout
./port.py otel --to ts | claude        # pipe directly to Claude Code
./port.py bearer --to go --source ~/Development/vgi-rpc/vgi_rpc/http/_bearer.py
```

The script finds the Python source and tests by convention (`vgi_rpc/{feature}.py` + `tests/test_{feature}.py`). Use `--source` to override when the convention doesn't match.

## Commands

| Command | What it does |
|---------|-------------|
| `make status` | Show versions, latest commits, and `DESCRIBE_VERSION` for each repo |
| `make describe` | Spawn conformance workers, call `__describe__`, compare method sets and schema versions |
| `make test-all` | Run conformance tests in all three repos |
| `make test-python` | Run Python tests only |
| `make test-go` | Run Go conformance only |
| `make test-ts` | Run TypeScript conformance only |
| `./port.py <feature> --to <go\|ts>` | Generate a porting prompt for Claude Code |

## Prerequisites

- `uv` (Python package manager)
- `go` (for Go conformance)
- `bun` (for TypeScript conformance)
- `vgi-rpc` Python package (installed automatically via `uv sync`)

## Setup

```bash
uv sync   # install vgi-rpc dependency (needed for describe_diff.py)
```

## License

Apache-2.0
