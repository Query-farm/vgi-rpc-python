# CLI

Command-line interface for introspecting and calling methods on vgi-rpc services. Requires `pip install vgi-rpc[cli]`.

Registered as the `vgi-rpc` entry point. Provides `describe` and `call` commands.

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

### Options

| Option | Short | Description |
|---|---|---|
| `--url` | `-u` | HTTP base URL |
| `--cmd` | `-c` | Subprocess command |
| `--prefix` | `-p` | URL path prefix (default `/vgi`) |
| `--format` | `-f` | Output format: `auto`, `json`, or `table` |
| `--verbose` | `-v` | Show server log messages on stderr |
| `--json` | `-j` | Pass parameters as a JSON string (for `call`) |

## Module Reference

::: vgi_rpc.cli
