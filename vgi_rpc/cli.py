# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for vgi-rpc services.

Provides ``describe``, ``call``, and ``loggers`` commands for introspecting
and invoking methods on any vgi-rpc service that has ``enable_describe=True``.

Usage::

    vgi-rpc describe --cmd "my-worker"
    vgi-rpc call add --cmd "my-worker" a=1.0 b=2.0
    vgi-rpc call generate --url http://localhost:8000 count=3
    vgi-rpc loggers

"""

from __future__ import annotations

import contextlib
import importlib.metadata
import json
import logging
import select
import shlex
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from io import BytesIO, IOBase, TextIOWrapper
from typing import Annotated, Protocol, cast

import pyarrow as pa
import typer
from pyarrow import ipc

from vgi_rpc.introspect import MethodDescription, ServiceDescription, introspect
from vgi_rpc.log import Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    MethodType,
    RpcError,
    RpcTransport,
    StreamSession,
    SubprocessTransport,
    UnixTransport,
    _drain_stream,
    _read_batch_with_log_check,
    _read_raw_stream_header,
    _write_request,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader

# ---------------------------------------------------------------------------
# Output format enum
# ---------------------------------------------------------------------------


class OutputFormat(StrEnum):
    """Output format for CLI commands."""

    auto = "auto"
    json = "json"
    table = "table"
    arrow = "arrow"


# ---------------------------------------------------------------------------
# Logging enums
# ---------------------------------------------------------------------------


class LogLevel(StrEnum):
    """Python logging level for ``--log-level``."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogFormat(StrEnum):
    """Stderr log format for ``--log-format``."""

    text = "text"
    json = "json"


# ---------------------------------------------------------------------------
# Known loggers registry
# ---------------------------------------------------------------------------

# (name, description, scenario) for every logger in the codebase.
_KNOWN_LOGGERS: tuple[tuple[str, str, str], ...] = (
    ("vgi_rpc", "Root logger for all vgi-rpc output", "Enable to see all framework logging"),
    ("vgi_rpc.access", "One structured record per completed RPC call", "Monitor request throughput and errors"),
    ("vgi_rpc.rpc", "RPC framework lifecycle", "Debug server dispatch and method resolution"),
    ("vgi_rpc.http", "HTTP transport lifecycle", "Debug Falcon WSGI app and middleware"),
    ("vgi_rpc.http.retry", "HTTP client retry logic", "Debug retry decisions and backoff"),
    ("vgi_rpc.pool", "Worker pool operations", "Debug subprocess pool borrow/return/eviction"),
    ("vgi_rpc.shm", "Shared memory transport", "Debug SHM segment allocation and pointer batches"),
    ("vgi_rpc.external", "External storage operations", "Debug externalize/resolve for large batches"),
    ("vgi_rpc.external_fetch", "Parallel URL fetching", "Debug range-request fetching and hedging"),
    ("vgi_rpc.s3", "S3 storage backend", "Debug S3 uploads and pre-signed URL generation"),
    ("vgi_rpc.gcs", "GCS storage backend", "Debug GCS uploads and signed URL generation"),
    ("vgi_rpc.otel", "OpenTelemetry integration", "Debug span creation and propagation"),
    ("vgi_rpc.subprocess.stderr", "Child process stderr capture", "See subprocess stderr output"),
    ("vgi_rpc.wire.request", "Request serialization/deserialization", "Debug method calls returning wrong results"),
    ("vgi_rpc.wire.response", "Response serialization/deserialization", "Debug result schema and type mismatches"),
    ("vgi_rpc.wire.batch", "Batch classification (log/error/data)", "Debug log or error batches not arriving"),
    ("vgi_rpc.wire.stream", "Stream session lifecycle", "Debug streaming batches lost or out of order"),
    ("vgi_rpc.wire.transport", "Transport lifecycle (pipe, subprocess)", "Debug connection hangs or fd issues"),
    ("vgi_rpc.wire.http", "HTTP client requests/responses", "Debug HTTP transport request/response issues"),
)


# ---------------------------------------------------------------------------
# CLI config
# ---------------------------------------------------------------------------


@dataclass
class _CliConfig:
    """Holds resolved CLI options."""

    url: str | None = None
    cmd: str | None = None
    unix: str | None = None
    prefix: str = "/vgi"
    format: OutputFormat = OutputFormat.auto
    output: str | None = None
    verbose: bool = False
    no_stdin: bool = False
    input_file: str | None = None
    log_level: LogLevel | None = None
    log_loggers: list[str] = field(default_factory=list)
    log_format: LogFormat = LogFormat.text


app = typer.Typer(
    name="vgi-rpc",
    help="CLI client for vgi-rpc services.",
    add_completion=False,
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

_KNOWN_LOGGER_NAMES: frozenset[str] = frozenset(name for name, _, _ in _KNOWN_LOGGERS)


def _configure_logging(config: _CliConfig) -> None:
    """Attach a stderr handler to the target loggers at the requested level.

    Called from ``_main`` when ``--debug`` or ``--log-level`` is set.
    """
    level = config.log_level
    if level is None:
        return

    # Build handler with chosen format
    handler = logging.StreamHandler(sys.stderr)
    if config.log_format == LogFormat.json:
        from vgi_rpc.logging_utils import VgiJsonFormatter

        handler.setFormatter(VgiJsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(name)-30s %(levelname)-5s %(message)s"))

    numeric_level = logging.getLevelNamesMapping()[level.value]

    # Determine target loggers
    targets = config.log_loggers if config.log_loggers else ["vgi_rpc"]

    for name in targets:
        if name not in _KNOWN_LOGGER_NAMES and not name.startswith("vgi_rpc.service."):
            sys.stderr.write(f"Warning: unknown logger '{name}'\n")
            sys.stderr.flush()
        logger = logging.getLogger(name)
        logger.setLevel(numeric_level)
        logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Global callback
# ---------------------------------------------------------------------------


def _version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        version = importlib.metadata.version("vgi-rpc")
        typer.echo(f"vgi-rpc {version}")
        raise typer.Exit()


@app.callback()
def _main(
    ctx: typer.Context,
    url: Annotated[str | None, typer.Option("--url", "-u", help="HTTP base URL")] = None,
    cmd: Annotated[str | None, typer.Option("--cmd", "-c", help="Subprocess command")] = None,
    unix: Annotated[str | None, typer.Option("--unix", help="Unix domain socket path")] = None,
    prefix: Annotated[str, typer.Option("--prefix", "-p", help="URL path prefix")] = "/vgi",
    fmt: Annotated[OutputFormat, typer.Option("--format", "-f", help="Output format")] = OutputFormat.auto,
    output: Annotated[str | None, typer.Option("--output", "-o", help="Output file path (default: stdout)")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show server log messages on stderr")] = False,
    debug: Annotated[bool, typer.Option("--debug", help="Enable DEBUG on all vgi_rpc loggers to stderr")] = False,
    log_level: Annotated[LogLevel | None, typer.Option("--log-level", help="Python logging level for vgi_rpc")] = None,
    log_logger: Annotated[list[str] | None, typer.Option("--log-logger", help="Target specific logger(s)")] = None,
    log_format: Annotated[
        LogFormat, typer.Option("--log-format", help="Stderr log format: text or json")
    ] = LogFormat.text,
    version: Annotated[
        bool, typer.Option("--version", "-V", help="Show version and exit", callback=_version_callback, is_eager=True)
    ] = False,
) -> None:
    """Configure transport and output options."""
    given = sum(1 for opt in (url, cmd, unix) if opt)
    if given > 1:
        raise typer.BadParameter("--url, --cmd, and --unix are mutually exclusive")

    # Resolve effective log level: --debug overrides --log-level
    effective_level: LogLevel | None = log_level
    if debug:
        effective_level = LogLevel.DEBUG

    config = _CliConfig(
        url=url,
        cmd=cmd,
        unix=unix,
        prefix=prefix,
        format=fmt,
        output=output,
        verbose=verbose,
        log_level=effective_level,
        log_loggers=log_logger or [],
        log_format=log_format,
    )
    _configure_logging(config)
    ctx.obj = config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log_to_stderr(msg: Message) -> None:
    """Write a log message to stderr."""
    sys.stderr.write(f"[{msg.level.value}] {msg.message}\n")
    sys.stderr.flush()


def _is_integer_type(arrow_type: pa.DataType) -> bool:
    """Check if arrow_type is any integer type."""
    return (
        pa.types.is_int8(arrow_type)
        or pa.types.is_int16(arrow_type)
        or pa.types.is_int32(arrow_type)
        or pa.types.is_int64(arrow_type)
        or pa.types.is_uint8(arrow_type)
        or pa.types.is_uint16(arrow_type)
        or pa.types.is_uint32(arrow_type)
        or pa.types.is_uint64(arrow_type)
    )


def _is_float_type(arrow_type: pa.DataType) -> bool:
    """Check if arrow_type is any float type."""
    return pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type) or pa.types.is_float64(arrow_type)


def _coerce_value(value_str: str, arrow_type: pa.DataType) -> object:
    """Coerce a string value to the expected Arrow type.

    Args:
        value_str: Raw string from CLI key=value arg.
        arrow_type: Target Arrow type from params_schema.

    Returns:
        Coerced Python value suitable for Arrow serialization.

    """
    if _is_integer_type(arrow_type):
        return int(value_str)
    if _is_float_type(arrow_type):
        return float(value_str)
    if pa.types.is_boolean(arrow_type):
        return value_str.lower() in ("true", "1", "yes")
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return value_str
    if pa.types.is_dictionary(arrow_type):
        # Enum fields use dictionary type — pass the name as a string
        return value_str
    # Complex types: try JSON parse
    return json.loads(value_str)


def _parse_key_value_args(args: list[str], params_schema: pa.Schema) -> dict[str, object]:
    """Parse key=value args using schema-driven type coercion.

    Args:
        args: List of "key=value" strings from CLI.
        params_schema: Arrow schema with field names and types.

    Returns:
        Dict of parameter name to coerced value.

    Raises:
        typer.BadParameter: If a key is not in the schema or format is invalid.

    """
    result: dict[str, object] = {}
    schema_fields = {f.name: f.type for f in params_schema}
    for arg in args:
        if "=" not in arg:
            raise typer.BadParameter(f"Expected key=value, got: {arg}")
        key, value = arg.split("=", 1)
        if key not in schema_fields:
            available = ", ".join(sorted(schema_fields.keys()))
            raise typer.BadParameter(f"Unknown parameter '{key}'. Available: {available}")
        result[key] = _coerce_value(value, schema_fields[key])
    return result


def _coerce_map_value(val: object) -> object:
    """Convert Arrow map values (list-of-tuples) to dicts."""
    if isinstance(val, list) and val and isinstance(val[0], tuple):
        return dict(cast("list[tuple[object, object]]", val))
    return val


def _batch_to_rows(batch: pa.RecordBatch) -> list[dict[str, object]]:
    """Convert a RecordBatch to a list of dicts, one per row.

    Each row becomes its own ``{column: scalar_value}`` dict.
    Map-typed columns (list-of-tuples from Arrow) are converted to dicts.
    """
    d = batch.to_pydict()
    columns = list(d.keys())
    num_rows = batch.num_rows
    rows: list[dict[str, object]] = []
    for i in range(num_rows):
        row: dict[str, object] = {}
        for col in columns:
            row[col] = _coerce_map_value(d[col][i])
        rows.append(row)
    return rows


def _format_table(rows: list[dict[str, object]]) -> str:
    """Format rows as a simple column-aligned text table.

    Args:
        rows: List of dicts (all with the same keys).

    Returns:
        A formatted table string.

    """
    if not rows:
        return "(empty)"
    columns = list(rows[0].keys())
    # Compute column widths
    widths = {col: len(col) for col in columns}
    str_rows: list[dict[str, str]] = []
    for row in rows:
        sr: dict[str, str] = {}
        for col in columns:
            s = str(row.get(col, ""))
            sr[col] = s
            widths[col] = max(widths[col], len(s))
        str_rows.append(sr)

    lines: list[str] = []
    header = "  ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("  ".join("-" * widths[col] for col in columns))
    lines.extend("  ".join(sr[col].ljust(widths[col]) for col in columns) for sr in str_rows)
    return "\n".join(lines)


def _print_json(data: object, *, pretty: bool = False) -> None:
    """Print JSON to stdout.

    Args:
        data: Python object to serialize.
        pretty: Use indented formatting.

    """
    if pretty:
        typer.echo(json.dumps(data, indent=2, default=str))
    else:
        typer.echo(json.dumps(data, default=str))


def _ensure_transport(config: _CliConfig) -> None:
    """Validate that a transport option was given."""
    if not config.url and not config.cmd and not config.unix:
        raise typer.BadParameter("Either --url, --cmd, or --unix is required")


def _get_on_log(config: _CliConfig) -> Callable[[Message], None] | None:
    """Return log callback if verbose, else None."""
    return _log_to_stderr if config.verbose else None


def _has_exchange_input(config: _CliConfig) -> bool:
    """Check whether the caller supplied exchange input.

    Returns ``True`` when ``--input`` points to an Arrow IPC file **or**
    when stdin has piped data.  Returns ``False`` (producer mode) when
    ``--no-stdin`` is set, stdin is a TTY, or stdin is empty and no
    ``--input`` file was given.
    """
    if config.input_file:
        return True
    if config.no_stdin:
        return False
    try:
        if sys.stdin.isatty():
            return False
    except (AttributeError, ValueError):
        pass
    # Non-TTY or unknown — check if there's actual data
    try:
        pos = sys.stdin.tell()
        sys.stdin.seek(0, 2)  # seek to end
        end = sys.stdin.tell()
        sys.stdin.seek(pos)  # restore
        return end > pos
    except (OSError, AttributeError, ValueError):
        # Real pipes don't support seek — probe with select to check
        # whether data is actually ready.  A short timeout avoids
        # blocking forever on an inherited pipe that has no writer.
        try:
            readable, _, _ = select.select([sys.stdin], [], [], 0.1)
            return bool(readable)
        except (ValueError, OSError, TypeError):
            return False


def _iter_exchange_inputs(config: _CliConfig) -> Iterator[AnnotatedBatch]:
    """Yield ``AnnotatedBatch`` exchange inputs from ``--input`` file or stdin.

    When ``--input`` is set, batches are read from the Arrow IPC file.
    Otherwise, JSON lines are read from stdin (one row per line).
    """
    if config.input_file:
        with ipc.open_stream(pa.OSFile(config.input_file, "rb")) as reader:
            for batch in reader:
                yield AnnotatedBatch(batch=batch)
    else:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            input_data = json.loads(line)
            input_dict: dict[str, list[object]] = {k: [v] for k, v in input_data.items()}
            input_batch = pa.RecordBatch.from_pydict(input_dict)
            yield AnnotatedBatch(batch=input_batch)


def _emit_rpc_error(e: RpcError) -> None:
    """Write an RpcError to stderr as JSON."""
    err: dict[str, object] = {"type": e.error_type, "message": e.error_message}
    if e.remote_traceback:
        err["traceback"] = e.remote_traceback
    typer.echo(json.dumps({"error": err}, default=str), err=True)


# ---------------------------------------------------------------------------
# Arrow / file output helpers
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _open_arrow_dest(config: _CliConfig) -> Iterator[IOBase]:
    """Open the arrow output destination as a context manager.

    Yields a binary writable stream: a file when ``--output`` is set,
    otherwise ``sys.stdout.buffer`` (with a TTY warning on stderr).
    The file is closed on exit; stdout.buffer is left open.
    """
    if config.output:
        f = open(config.output, "wb")  # noqa: SIM115
        try:
            yield cast("IOBase", f)
        finally:
            f.close()
    else:
        if sys.stdout.isatty():
            sys.stderr.write("Warning: writing binary Arrow IPC to terminal; consider --output/-o\n")
            sys.stderr.flush()
        yield cast("IOBase", sys.stdout.buffer)


def _write_arrow_output(config: _CliConfig, batch: pa.RecordBatch) -> None:
    """Write a single batch as a complete IPC stream to the configured destination."""
    with _open_arrow_dest(config) as dest, ipc.new_stream(dest, batch.schema) as writer:
        writer.write_batch(batch)


@contextlib.contextmanager
def _open_text_dest(config: _CliConfig) -> Iterator[TextIOWrapper | None]:
    """Open a text output destination when ``--output`` is set.

    Yields the file handle, or ``None`` when writing to stdout.
    Callers use the returned handle to write explicitly, falling back
    to ``typer.echo`` / ``_print_json`` for stdout (which go through
    Click's own output machinery and work correctly under CliRunner).
    """
    if not config.output:
        yield None
        return
    f = open(config.output, "w")  # noqa: SIM115
    try:
        yield f
    finally:
        f.close()


def _echo(text: str, dest: TextIOWrapper | None) -> None:
    """Write text to *dest* if given, otherwise to stdout via typer.echo."""
    if dest is not None:
        dest.write(text + "\n")
    else:
        typer.echo(text)


def _print_json_to(data: object, dest: TextIOWrapper | None, *, pretty: bool = False) -> None:
    """Serialize *data* as JSON and write to *dest* or stdout."""
    text = json.dumps(data, indent=2 if pretty else None, default=str)
    _echo(text, dest)


def _header_batch_to_dict(batch: pa.RecordBatch) -> dict[str, object]:
    """Convert a single-row header batch to a dict.

    Returns an empty dict for zero-row batches (defensive).
    """
    if batch.num_rows == 0:
        return {}
    d = batch.to_pydict()
    return {col: _coerce_map_value(vals[0]) for col, vals in d.items()}


# ---------------------------------------------------------------------------
# Shared stream protocol + deduplication
# ---------------------------------------------------------------------------


class _StreamLike(Protocol):
    """Minimal interface shared by StreamSession and HttpStreamSession."""

    def __iter__(self) -> Iterator[AnnotatedBatch]: ...

    def exchange(self, input: AnnotatedBatch, /) -> AnnotatedBatch: ...

    def close(self) -> None: ...


def _print_unary_result(ab: AnnotatedBatch, method: MethodDescription, config: _CliConfig) -> None:
    """Format and print a unary call result."""
    fmt = config.format
    if fmt == OutputFormat.arrow:
        if not method.has_return:
            _write_arrow_output(config, pa.record_batch({}))
        else:
            _write_arrow_output(config, ab.batch)
        return
    with _open_text_dest(config) as dest:
        if not method.has_return:
            _print_json_to(None, dest)
            return
        rows = _batch_to_rows(ab.batch)
        is_tty = sys.stdout.isatty() and dest is None
        if fmt == OutputFormat.table:
            _echo(_format_table(rows), dest)
        else:
            for row in rows:
                _print_json_to(row, dest, pretty=(fmt == OutputFormat.auto and is_tty))


def _emit_header_text(
    header_batch: pa.RecordBatch | None,
    fmt: OutputFormat,
    is_tty: bool,
    dest: TextIOWrapper | None,
) -> None:
    """Emit stream header in text formats (JSON or table) before data rows."""
    if header_batch is None:
        return
    hdr = _header_batch_to_dict(header_batch)
    if fmt == OutputFormat.table:
        _echo("Header:", dest)
        for k, v in hdr.items():
            _echo(f"  {k}: {v}", dest)
        _echo("", dest)
    else:
        _print_json_to({"__header__": hdr}, dest, pretty=(fmt == OutputFormat.auto and is_tty))


def _run_stream_arrow(
    session: _StreamLike,
    config: _CliConfig,
    header_batch: pa.RecordBatch | None,
) -> None:
    """Run a stream session and write Arrow IPC output."""
    has_input = _has_exchange_input(config)
    with _open_arrow_dest(config) as dest:
        # Write header as its own IPC stream if present
        if header_batch is not None:
            with ipc.new_stream(dest, header_batch.schema) as hw:
                hw.write_batch(header_batch)

        writer: ipc.RecordBatchStreamWriter | None = None
        try:
            if not has_input:
                for ab in session:
                    if writer is None:
                        writer = ipc.new_stream(dest, ab.batch.schema)
                    writer.write_batch(ab.batch)
            else:
                for ab_input in _iter_exchange_inputs(config):
                    ab_output = session.exchange(ab_input)
                    if writer is None:
                        writer = ipc.new_stream(dest, ab_output.batch.schema)
                    writer.write_batch(ab_output.batch)
        except RpcError as e:
            _emit_rpc_error(e)
            raise typer.Exit(1) from None
        finally:
            if has_input:
                with contextlib.suppress(Exception):
                    session.close()
            if writer is not None:
                writer.close()


def _run_stream(
    session: _StreamLike,
    config: _CliConfig,
    header_batch: pa.RecordBatch | None = None,
) -> None:
    """Run a stream session (producer or exchange) with config-driven output."""
    fmt = config.format

    if fmt == OutputFormat.arrow:
        _run_stream_arrow(session, config, header_batch)
        return

    has_input = _has_exchange_input(config)

    with _open_text_dest(config) as dest:
        is_tty = sys.stdout.isatty() and dest is None
        _emit_header_text(header_batch, fmt, is_tty, dest)

        if not has_input:
            # Producer mode
            table_rows: list[dict[str, object]] = []
            try:
                for ab in session:
                    batch_rows = _batch_to_rows(ab.batch)
                    if fmt == OutputFormat.table:
                        table_rows.extend(batch_rows)
                    else:
                        for row in batch_rows:
                            _print_json_to(row, dest, pretty=(fmt == OutputFormat.auto and is_tty))
                        if dest is None:
                            sys.stdout.flush()
                        else:
                            dest.flush()
            except RpcError as e:
                _emit_rpc_error(e)
                raise typer.Exit(1) from None
            if fmt == OutputFormat.table and table_rows:
                _echo(_format_table(table_rows), dest)
        else:
            # Exchange mode
            try:
                for ab_input in _iter_exchange_inputs(config):
                    ab_output = session.exchange(ab_input)
                    for row in _batch_to_rows(ab_output.batch):
                        _print_json_to(row, dest, pretty=(fmt == OutputFormat.auto and is_tty))
                    if dest is None:
                        sys.stdout.flush()
                    else:
                        dest.flush()
            except RpcError as e:
                _emit_rpc_error(e)
                raise typer.Exit(1) from None
            finally:
                session.close()


# ---------------------------------------------------------------------------
# loggers subcommand
# ---------------------------------------------------------------------------


@app.command()
def loggers(
    ctx: typer.Context,
) -> None:
    """List all known vgi-rpc logger names with descriptions."""
    config: _CliConfig = ctx.obj
    is_tty = sys.stdout.isatty()
    fmt = config.format

    if fmt == OutputFormat.json or (fmt == OutputFormat.auto and not is_tty):
        data = [{"name": name, "description": desc, "scenario": scenario} for name, desc, scenario in _KNOWN_LOGGERS]
        _print_json(data)
    else:
        rows: list[dict[str, object]] = [
            {"name": name, "description": desc, "scenario": scenario} for name, desc, scenario in _KNOWN_LOGGERS
        ]
        typer.echo(_format_table(rows))


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------


def _introspect_pipe(cmd: str) -> tuple[ServiceDescription, SubprocessTransport]:
    """Introspect a subprocess transport, returning both description and open transport."""
    transport = SubprocessTransport(shlex.split(cmd, posix=sys.platform != "win32"))
    try:
        desc = introspect(transport)
    except BaseException:
        transport.close()
        raise
    return desc, transport


def _introspect_unix(path: str) -> tuple[ServiceDescription, UnixTransport]:
    """Introspect a Unix socket transport, returning both description and open transport."""
    import socket as _socket

    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    try:
        sock.connect(path)
    except BaseException:
        sock.close()
        raise
    transport = UnixTransport(sock)
    try:
        desc = introspect(transport)
    except BaseException:
        transport.close()
        raise
    return desc, transport


def _introspect_http(url: str, prefix: str) -> ServiceDescription:
    """Introspect an HTTP transport."""
    from vgi_rpc.http import http_introspect

    return http_introspect(url, prefix=prefix)


def _get_service_description(
    config: _CliConfig,
) -> tuple[ServiceDescription, RpcTransport | None]:
    """Get ServiceDescription from configured transport.

    Returns:
        A tuple of (description, transport). The transport is non-None only
        for pipe/subprocess/unix transports, where it is left open for reuse
        by the caller (who must close it).

    """
    _ensure_transport(config)
    if config.cmd:
        return _introspect_pipe(config.cmd)
    if config.unix:
        return _introspect_unix(config.unix)
    assert config.url is not None
    return _introspect_http(config.url, config.prefix), None


# ---------------------------------------------------------------------------
# describe command
# ---------------------------------------------------------------------------


@app.command()
def describe(ctx: typer.Context) -> None:
    """Introspect a vgi-rpc service and show its methods."""
    config: _CliConfig = ctx.obj
    transport: RpcTransport | None = None
    try:
        desc, transport = _get_service_description(config)
    except RpcError as e:
        _emit_rpc_error(e)
        raise typer.Exit(1) from None
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None
    finally:
        if transport is not None:
            transport.close()

    is_tty = sys.stdout.isatty()
    fmt = config.format

    if fmt == OutputFormat.table or (fmt == OutputFormat.auto and is_tty):
        # Table output — use ServiceDescription.__str__()
        output = str(desc)
        # If HTTP, append capabilities
        if config.url:
            try:
                from vgi_rpc.http import http_capabilities

                caps = http_capabilities(config.url, prefix=config.prefix)
                output += "\nCapabilities:\n"
                output += f"  max_request_bytes: {caps.max_request_bytes}\n"
                output += f"  upload_url_support: {caps.upload_url_support}\n"
                output += f"  max_upload_bytes: {caps.max_upload_bytes}\n"
            except Exception:
                pass
        typer.echo(output)
    else:
        # JSON output
        data: dict[str, object] = {
            "protocol_name": desc.protocol_name,
            "server_id": desc.server_id,
            "request_version": desc.request_version,
            "describe_version": desc.describe_version,
            "methods": {
                name: {
                    "method_type": md.method_type.value,
                    "doc": md.doc,
                    "has_return": md.has_return,
                    "param_types": md.param_types,
                    "param_defaults": md.param_defaults,
                }
                for name, md in sorted(desc.methods.items())
            },
        }
        if config.url:
            try:
                from vgi_rpc.http import http_capabilities

                caps = http_capabilities(config.url, prefix=config.prefix)
                data["capabilities"] = {
                    "max_request_bytes": caps.max_request_bytes,
                    "upload_url_support": caps.upload_url_support,
                    "max_upload_bytes": caps.max_upload_bytes,
                }
            except Exception:
                pass
        is_pretty = fmt == OutputFormat.auto and is_tty
        _print_json(data, pretty=is_pretty)


# ---------------------------------------------------------------------------
# call command — pipe transport
# ---------------------------------------------------------------------------


def _call_unary_pipe(
    transport: RpcTransport,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a unary method over pipe/unix transport."""
    _write_request(transport.writer, method.name, method.params_schema, kwargs)
    reader = ValidatedReader(ipc.open_stream(transport.reader), IpcValidation.FULL)
    try:
        ab = _read_batch_with_log_check(reader, on_log)
    except RpcError:
        _drain_stream(reader)
        raise
    _drain_stream(reader)
    _print_unary_result(ab, method, config)


def _call_stream_pipe(
    transport: RpcTransport,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a stream method over pipe/unix transport."""
    _write_request(transport.writer, method.name, method.params_schema, kwargs)
    header_batch: pa.RecordBatch | None = None
    if method.has_header:
        header_batch = _read_raw_stream_header(transport.reader, IpcValidation.FULL, on_log)
    # header_batch is a raw RecordBatch (not a deserialized dataclass), so we
    # don't pass it to StreamSession.header which expects the typed object.
    # Instead we pass it directly to _run_stream for display.
    session = StreamSession(transport.writer, transport.reader, on_log)
    _run_stream(session, config, header_batch)


# ---------------------------------------------------------------------------
# call command — HTTP transport
# ---------------------------------------------------------------------------


def _call_unary_http(
    url: str,
    prefix: str,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a unary method over HTTP transport."""
    import httpx

    from vgi_rpc.http._client import _open_response_stream
    from vgi_rpc.http._common import _ARROW_CONTENT_TYPE

    req_buf = BytesIO()
    _write_request(req_buf, method.name, method.params_schema, kwargs)

    client = httpx.Client(base_url=url, follow_redirects=True)
    try:
        resp = client.post(
            f"{prefix}/{method.name}",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        reader = _open_response_stream(resp.content, resp.status_code, IpcValidation.FULL)
        try:
            ab = _read_batch_with_log_check(reader, on_log)
        except RpcError:
            _drain_stream(reader)
            raise
        _drain_stream(reader)
        _print_unary_result(ab, method, config)
    finally:
        client.close()


def _call_stream_http(
    url: str,
    prefix: str,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a stream method over HTTP transport."""
    import httpx

    from vgi_rpc.http._client import _init_http_stream_session, _open_response_stream
    from vgi_rpc.http._common import _ARROW_CONTENT_TYPE

    req_buf = BytesIO()
    _write_request(req_buf, method.name, method.params_schema, kwargs)

    client = httpx.Client(base_url=url, follow_redirects=True)
    try:
        resp = client.post(
            f"{prefix}/{method.name}/init",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
        )
        header_batch: pa.RecordBatch | None = None
        resp_stream = BytesIO(resp.content)
        if method.has_header:
            # Check for auth errors first (plain text, not Arrow IPC)
            if resp.status_code == 401:
                _open_response_stream(resp.content, resp.status_code, IpcValidation.FULL)
            header_batch = _read_raw_stream_header(resp_stream, IpcValidation.FULL, on_log)
        remaining = resp_stream.read()
        reader = _open_response_stream(remaining, resp.status_code, IpcValidation.FULL)
        # header_batch is a raw RecordBatch, not passed to the session.
        session = _init_http_stream_session(
            client=client,
            url_prefix=prefix,
            method_name=method.name,
            reader=reader,
            on_log=on_log,
        )
        _run_stream(session, config, header_batch)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# call command
# ---------------------------------------------------------------------------


@app.command()
def call(
    ctx: typer.Context,
    method: Annotated[str, typer.Argument(help="Method name to call")],
    args: Annotated[list[str] | None, typer.Argument(help="key=value parameters")] = None,
    json_input: Annotated[str | None, typer.Option("--json", "-j", help="JSON params")] = None,
    input_file: Annotated[str | None, typer.Option("--input", "-i", help="Arrow IPC file for exchange input")] = None,
    no_stdin: Annotated[
        bool, typer.Option("--no-stdin", help="Force producer mode (ignore stdin)", hidden=True)
    ] = False,
) -> None:
    """Call a method on a vgi-rpc service."""
    config: _CliConfig = ctx.obj
    if input_file:
        config.input_file = input_file
    if no_stdin:
        config.no_stdin = True

    transport: RpcTransport | None = None
    try:
        desc, transport = _get_service_description(config)
    except RpcError as e:
        _emit_rpc_error(e)
        raise typer.Exit(1) from None
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None

    if method not in desc.methods:
        if transport is not None:
            transport.close()
        available = ", ".join(sorted(desc.methods.keys()))
        typer.echo(
            f"Error: Unknown method '{method}'. Available: {available}",
            err=True,
        )
        raise typer.Exit(1) from None

    md = desc.methods[method]

    # Parse params
    if json_input and args:
        if transport is not None:
            transport.close()
        raise typer.BadParameter("--json and key=value args are mutually exclusive")

    kwargs: dict[str, object]
    if json_input:
        kwargs = json.loads(json_input)
    elif args:
        kwargs = _parse_key_value_args(args, md.params_schema)
    else:
        kwargs = {}

    # Merge defaults
    merged: dict[str, object] = {**md.param_defaults, **kwargs}

    on_log = _get_on_log(config)

    try:
        if config.cmd or config.unix:
            # Reuse the transport from introspect for the call
            assert transport is not None
            if md.method_type == MethodType.UNARY:
                _call_unary_pipe(transport, md, merged, on_log, config)
            else:
                _call_stream_pipe(transport, md, merged, on_log, config)
        elif config.url:
            if transport is not None:
                transport.close()
                transport = None
            if md.method_type == MethodType.UNARY:
                _call_unary_http(config.url, config.prefix, md, merged, on_log, config)
            else:
                _call_stream_http(
                    config.url,
                    config.prefix,
                    md,
                    merged,
                    on_log,
                    config,
                )
    except RpcError as e:
        _emit_rpc_error(e)
        raise typer.Exit(1) from None
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None
    finally:
        if transport is not None:
            transport.close()
