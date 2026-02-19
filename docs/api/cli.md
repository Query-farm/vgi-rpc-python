# CLI

Command-line interface for introspecting and calling methods on vgi-rpc services. Requires `pip install vgi-rpc[cli]`.

Registered as the `vgi-rpc` entry point. Provides `describe`, `call`, and `loggers` commands.

## Usage

### Describe a service

```bash
# Subprocess transport
vgi-rpc describe --cmd "python worker.py"

# HTTP transport
vgi-rpc describe --url http://localhost:8000

# JSON output
vgi-rpc describe --cmd "python worker.py" --format json
```

### Call a method

```bash
# Unary call with key=value parameters
vgi-rpc call add --cmd "python worker.py" a=1.0 b=2.0

# JSON parameter input
vgi-rpc call add --url http://localhost:8000 --json '{"a": 1.0, "b": 2.0}'

# Producer stream (table output)
vgi-rpc call countdown --cmd "python worker.py" n=5 --format table

# Exchange stream (pipe JSON lines to stdin)
echo '{"value": 1.0}' | vgi-rpc call accumulate --url http://localhost:8000
```

### List loggers

```bash
# Human-readable table
vgi-rpc loggers

# JSON (for scripting / LLM consumption)
vgi-rpc loggers --format json
```

### Debug logging

```bash
# Enable DEBUG on all vgi_rpc loggers
vgi-rpc --debug describe --cmd "python worker.py"

# Set a specific level
vgi-rpc --log-level INFO describe --cmd "python worker.py"

# Target specific loggers
vgi-rpc --log-level DEBUG --log-logger vgi_rpc.wire.request call add --cmd "python worker.py" a=1 b=2

# JSON log format on stderr
vgi-rpc --debug --log-format json call add --cmd "python worker.py" a=1 b=2
```

### Output format

Results are printed as **one JSON object per row** (NDJSON). When a method returns a batch with multiple rows, each row is emitted as its own line rather than a single object with array values. This applies to unary results, producer streams, and exchange responses.

```bash
# A stream that emits 2 batches of 3 rows each produces 6 lines:
$ vgi-rpc call generate_rows --cmd "python worker.py" count=6 rows_per_batch=3
{"i": 0, "value": 0}
{"i": 1, "value": 10}
{"i": 2, "value": 20}
{"i": 3, "value": 30}
{"i": 4, "value": 40}
{"i": 5, "value": 50}
```

With `--format table`, rows from all batches are collected and displayed as a single aligned table:

```bash
$ vgi-rpc call generate_rows --cmd "python worker.py" count=4 --format table
i   value
--  -----
0   0
1   10
2   20
3   30
```

With `--format auto` (the default), the CLI uses pretty-printed JSON when stdout is a TTY and compact NDJSON when piped.

### Options

| Option | Short | Description |
|---|---|---|
| `--url` | `-u` | HTTP base URL |
| `--cmd` | `-c` | Subprocess command |
| `--prefix` | `-p` | URL path prefix (default `/vgi`) |
| `--format` | `-f` | Output format: `auto`, `json`, or `table` |
| `--verbose` | `-v` | Show server log messages on stderr |
| `--debug` | | Enable DEBUG on all `vgi_rpc` loggers to stderr |
| `--log-level` | | Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-logger` | | Target specific logger(s), repeatable |
| `--log-format` | | Stderr log format: `text` (default) or `json` |
| `--json` | `-j` | Pass parameters as a JSON string (for `call`) |

`--debug` is shorthand for `--log-level DEBUG`. When both are given, `--debug` wins. `--verbose` (server-to-client log messages) remains orthogonal to `--debug`/`--log-level` (Python logging).

## Module Reference

::: vgi_rpc.cli
