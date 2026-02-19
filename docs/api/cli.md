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

With `--format arrow`, raw Arrow IPC streaming data is written to the output. Use `--output`/`-o` to direct binary output to a file:

```bash
# Unary result as Arrow IPC
vgi-rpc call add --cmd "python worker.py" a=1.0 b=2.0 --format arrow -o result.arrow

# Producer stream as Arrow IPC
vgi-rpc call generate --cmd "python worker.py" count=100 --format arrow -o data.arrow

# Stream with header: two concatenated IPC streams (header + data)
vgi-rpc call generate_with_header --cmd "python worker.py" count=5 --format arrow -o stream.arrow
```

The output file contains standard Arrow IPC streaming format data. For unary calls and headerless streams, this is a single IPC stream (schema + batches + EOS). For streams with headers, the file contains two concatenated IPC streams: the header stream first, then the data stream.

Read back a unary or headerless stream result:

```python
import pyarrow as pa
from pyarrow import ipc

with ipc.open_stream(pa.OSFile("result.arrow", "rb")) as reader:
    for batch in reader:
        print(batch.to_pandas())
```

Read back a stream with header (two concatenated IPC streams):

```python
import pyarrow as pa
from pyarrow import ipc

with open("stream.arrow", "rb") as f:
    # First IPC stream: header
    header_reader = ipc.open_stream(f)
    header_batch = header_reader.read_next_batch()
    print("Header:", header_batch.to_pydict())
    # Drain remaining batches to reach EOS
    for _ in header_reader:
        pass

    # Second IPC stream: data
    data_reader = ipc.open_stream(f)
    for batch in data_reader:
        print(batch.to_pandas())
```

Without `--output`, arrow data is written to stdout. A warning is printed to stderr if stdout is a TTY.

### Stream headers

Stream methods that declare a header type (e.g. `Stream[MyState, MyHeader]`) emit a one-time header before the data rows. The CLI reads this header and surfaces it in all output formats.

**JSON** (`--format json`): a `{"__header__": {...}}` line before data rows:

```bash
$ vgi-rpc call generate_with_header --cmd "python worker.py" count=3 --format json
{"__header__": {"total_count": 3, "label": "generate"}}
{"i": 0, "value": 0}
{"i": 1, "value": 10}
{"i": 2, "value": 20}
```

**Table** (`--format table`): a `Header:` section with indented key-value pairs before the data table:

```bash
$ vgi-rpc call generate_with_header --cmd "python worker.py" count=3 --format table
Header:
  total_count: 3
  label: generate

i  value
-  -----
0  0
1  10
2  20
```

**Arrow** (`--format arrow`): the header is written as a separate IPC stream before the data IPC stream (see [reading back concatenated streams](#output-format) above).

Streams without headers are unaffected — no `__header__` line or `Header:` section appears.

### Options

| Option | Short | Description |
|---|---|---|
| `--url` | `-u` | HTTP base URL |
| `--cmd` | `-c` | Subprocess command |
| `--prefix` | `-p` | URL path prefix (default `/vgi`) |
| `--format` | `-f` | Output format: `auto`, `json`, `table`, or `arrow` |
| `--output` | `-o` | Output file path (default: stdout) |
| `--verbose` | `-v` | Show server log messages on stderr |
| `--debug` | | Enable DEBUG on all `vgi_rpc` loggers to stderr |
| `--log-level` | | Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-logger` | | Target specific logger(s), repeatable |
| `--log-format` | | Stderr log format: `text` (default) or `json` |
| `--json` | `-j` | Pass parameters as a JSON string (for `call`) |

`--debug` is shorthand for `--log-level DEBUG`. When both are given, `--debug` wins. `--verbose` (server-to-client log messages) remains orthogonal to `--debug`/`--log-level` (Python logging).

## Module Reference

::: vgi_rpc.cli
