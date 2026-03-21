# vgi-rpc-sync

Keeps the three [vgi-rpc](https://vgi-rpc.query.farm) implementations in sync:

| Language | Repo | Role |
|----------|------|------|
| Python | `~/Development/vgi-rpc` | Reference implementation (features land here first) |
| Go | `~/Development/vgi-rpc-go` | |
| TypeScript | `~/Development/vgi-rpc-typescript` | |

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
