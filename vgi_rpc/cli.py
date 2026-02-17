"""Command-line interface for vgi-rpc services.

Provides ``describe`` and ``call`` commands for introspecting and invoking
methods on any vgi-rpc service that has ``enable_describe=True``.

Usage::

    vgi-rpc describe --cmd "my-worker"
    vgi-rpc call add --cmd "my-worker" a=1.0 b=2.0
    vgi-rpc call generate --url http://localhost:8000 count=3

"""

from __future__ import annotations

import json
import shlex
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from io import BytesIO
from typing import Annotated, Protocol

import pyarrow as pa
import typer
from pyarrow import ipc

from vgi_rpc.introspect import MethodDescription, ServiceDescription, introspect
from vgi_rpc.log import Message
from vgi_rpc.rpc import (
    AnnotatedBatch,
    MethodType,
    RpcError,
    StreamSession,
    SubprocessTransport,
    _drain_stream,
    _read_batch_with_log_check,
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


# ---------------------------------------------------------------------------
# CLI config
# ---------------------------------------------------------------------------


@dataclass
class _CliConfig:
    """Holds resolved CLI options."""

    url: str | None = None
    cmd: str | None = None
    prefix: str = "/vgi"
    format: OutputFormat = OutputFormat.auto
    verbose: bool = False
    no_stdin: bool = False


app = typer.Typer(
    name="vgi-rpc",
    help="CLI client for vgi-rpc services.",
    add_completion=False,
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# Global callback
# ---------------------------------------------------------------------------


@app.callback()
def _main(
    ctx: typer.Context,
    url: Annotated[str | None, typer.Option("--url", "-u", help="HTTP base URL")] = None,
    cmd: Annotated[str | None, typer.Option("--cmd", "-c", help="Subprocess command")] = None,
    prefix: Annotated[str, typer.Option("--prefix", "-p", help="URL path prefix")] = "/vgi",
    fmt: Annotated[OutputFormat, typer.Option("--format", "-f", help="Output format")] = OutputFormat.auto,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show server log messages on stderr")] = False,
) -> None:
    """Configure transport and output options."""
    if url and cmd:
        raise typer.BadParameter("--url and --cmd are mutually exclusive")
    ctx.obj = _CliConfig(url=url, cmd=cmd, prefix=prefix, format=fmt, verbose=verbose)


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


def _batch_to_dict(batch: pa.RecordBatch) -> dict[str, object]:
    """Convert a RecordBatch to a Python dict for JSON output.

    Single-row batches are unwrapped to scalar values per column.
    Multi-row batches produce list values per column.  Map-typed
    columns (list-of-tuples from Arrow) are converted to dicts.
    """
    d = batch.to_pydict()
    result: dict[str, object] = {}
    for key, values in d.items():
        if len(values) == 1:
            val = values[0]
            # Map columns come back as list of tuples — convert to dict
            if isinstance(val, list) and val and isinstance(val[0], tuple):
                result[key] = dict(val)
            else:
                result[key] = val
        else:
            converted: list[object] = []
            for val in values:
                if isinstance(val, list) and val and isinstance(val[0], tuple):
                    converted.append(dict(val))
                else:
                    converted.append(val)
            result[key] = converted
    return result


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
    if not config.url and not config.cmd:
        raise typer.BadParameter("Either --url or --cmd is required")


def _get_on_log(config: _CliConfig) -> Callable[[Message], None] | None:
    """Return log callback if verbose, else None."""
    return _log_to_stderr if config.verbose else None


def _stdin_has_data(config: _CliConfig) -> bool:
    """Check whether stdin has piped data for exchange mode.

    Returns ``False`` (producer mode) when ``--no-stdin`` is set, when
    stdin is a TTY, or when stdin is empty.  Returns ``True`` (exchange
    mode) when stdin is a non-TTY pipe with data.
    """
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
        # Real pipes don't support seek — assume data is available
        # (this is the normal piped-input case on a real terminal)
        return True


def _emit_rpc_error(e: RpcError) -> None:
    """Write an RpcError to stderr as JSON."""
    err: dict[str, object] = {"type": e.error_type, "message": e.error_message}
    if e.remote_traceback:
        err["traceback"] = e.remote_traceback
    typer.echo(json.dumps({"error": err}, default=str), err=True)


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
    if not method.has_return:
        _print_json(None)
        return
    result = _batch_to_dict(ab.batch)
    is_tty = sys.stdout.isatty()
    fmt = config.format
    if fmt == OutputFormat.table:
        typer.echo(_format_table([result]))
    else:
        _print_json(result, pretty=(fmt == OutputFormat.auto and is_tty))


def _run_stream(session: _StreamLike, config: _CliConfig) -> None:
    """Run a stream session (producer or exchange) with config-driven output."""
    has_stdin = _stdin_has_data(config)
    is_tty = sys.stdout.isatty()
    fmt = config.format

    if not has_stdin:
        # Producer mode
        rows: list[dict[str, object]] = []
        try:
            for ab in session:
                row = _batch_to_dict(ab.batch)
                if fmt == OutputFormat.table:
                    rows.append(row)
                else:
                    _print_json(row, pretty=(fmt == OutputFormat.auto and is_tty))
                    sys.stdout.flush()
        except RpcError as e:
            _emit_rpc_error(e)
            raise typer.Exit(1) from None
        if fmt == OutputFormat.table and rows:
            typer.echo(_format_table(rows))
    else:
        # Exchange mode
        try:
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                input_data = json.loads(line)
                input_dict: dict[str, list[object]] = {k: [v] for k, v in input_data.items()}
                input_batch = pa.RecordBatch.from_pydict(input_dict)
                ab_input = AnnotatedBatch(batch=input_batch)
                ab_output = session.exchange(ab_input)
                row = _batch_to_dict(ab_output.batch)
                _print_json(row, pretty=(fmt == OutputFormat.auto and is_tty))
                sys.stdout.flush()
        except RpcError as e:
            _emit_rpc_error(e)
            raise typer.Exit(1) from None
        finally:
            session.close()


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


def _introspect_http(url: str, prefix: str) -> ServiceDescription:
    """Introspect an HTTP transport."""
    from vgi_rpc.http import http_introspect

    return http_introspect(url, prefix=prefix)


def _get_service_description(
    config: _CliConfig,
) -> tuple[ServiceDescription, SubprocessTransport | None]:
    """Get ServiceDescription from configured transport.

    Returns:
        A tuple of (description, transport). The transport is non-None only
        for pipe/subprocess transports, where it is left open for reuse by
        the caller (who must close it).

    """
    _ensure_transport(config)
    if config.cmd:
        return _introspect_pipe(config.cmd)
    assert config.url is not None
    return _introspect_http(config.url, config.prefix), None


# ---------------------------------------------------------------------------
# describe command
# ---------------------------------------------------------------------------


@app.command()
def describe(ctx: typer.Context) -> None:
    """Introspect a vgi-rpc service and show its methods."""
    config: _CliConfig = ctx.obj
    transport: SubprocessTransport | None = None
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
    transport: SubprocessTransport,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a unary method over pipe transport."""
    _write_request(transport.writer, method.name, method.params_schema, kwargs)
    reader = ValidatedReader(ipc.open_stream(transport.reader), IpcValidation.NONE)
    try:
        ab = _read_batch_with_log_check(reader, on_log)
    except RpcError:
        _drain_stream(reader)
        raise
    _drain_stream(reader)
    _print_unary_result(ab, method, config)


def _call_stream_pipe(
    transport: SubprocessTransport,
    method: MethodDescription,
    kwargs: dict[str, object],
    on_log: Callable[[Message], None] | None,
    config: _CliConfig,
) -> None:
    """Call a stream method over pipe transport."""
    _write_request(transport.writer, method.name, method.params_schema, kwargs)
    session = StreamSession(transport.writer, transport.reader, on_log)
    _run_stream(session, config)


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
        reader = _open_response_stream(resp.content, resp.status_code, IpcValidation.NONE)
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
        reader = _open_response_stream(resp.content, resp.status_code, IpcValidation.NONE)
        session = _init_http_stream_session(
            client=client,
            url_prefix=prefix,
            method_name=method.name,
            reader=reader,
            on_log=on_log,
        )
        _run_stream(session, config)
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
    no_stdin: Annotated[
        bool, typer.Option("--no-stdin", help="Force producer mode (ignore stdin)", hidden=True)
    ] = False,
) -> None:
    """Call a method on a vgi-rpc service."""
    config: _CliConfig = ctx.obj
    if no_stdin:
        config.no_stdin = True

    transport: SubprocessTransport | None = None
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
        if config.cmd:
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
