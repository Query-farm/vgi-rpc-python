# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Standalone conformance test runner CLI.

Uses argparse (stdlib) to avoid requiring optional dependencies.
Provides full debug logging parity with the ``vgi-rpc`` CLI.

Usage::

    vgi-rpc-test --cmd "./my-server"
    vgi-rpc-test --url http://localhost:8000
    vgi-rpc-test --unix /tmp/server.sock
    vgi-rpc-test --cmd "./my-server" --shm 4194304
    vgi-rpc-test --cmd "./my-server" --filter "scalar*,void*"
    vgi-rpc-test --list
    vgi-rpc-test --cmd "./my-server" --debug
    vgi-rpc-test --cmd "./my-server" --format json

"""

from __future__ import annotations

import argparse
import contextlib
import importlib.metadata
import json
import logging
import shlex
import sys
from collections.abc import Callable, Iterator
from io import IOBase

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.conformance._runner import (
    ConformanceResult,
    ConformanceSuite,
    LogCollector,
    list_conformance_tests,
    run_conformance,
)
from vgi_rpc.log import Message
from vgi_rpc.rpc import RpcConnection, SubprocessTransport

# ---------------------------------------------------------------------------
# Known loggers registry (same as cli.py)
# ---------------------------------------------------------------------------

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

_KNOWN_LOGGER_NAMES: frozenset[str] = frozenset(name for name, _, _ in _KNOWN_LOGGERS)

# ---------------------------------------------------------------------------
# SHM transport wrapper
# ---------------------------------------------------------------------------


class _ShmTransportWrapper:
    """Wrap SubprocessTransport to add a ``.shm`` property."""

    __slots__ = ("_inner", "_shm")

    def __init__(self, inner: SubprocessTransport, shm: object) -> None:
        """Initialize with inner transport and ShmSegment."""
        self._inner = inner
        self._shm = shm

    @property
    def reader(self) -> IOBase:
        """Readable binary stream."""
        return self._inner.reader

    @property
    def writer(self) -> IOBase:
        """Writable binary stream."""
        return self._inner.writer

    @property
    def shm(self) -> object:
        """The shared memory segment."""
        return self._shm

    def close(self) -> None:
        """Close the inner transport."""
        self._inner.close()


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------


def _configure_logging(args: argparse.Namespace) -> None:
    """Attach a stderr handler to the target loggers at the requested level."""
    level: str | None = args.log_level
    if args.debug:
        level = "DEBUG"
    if level is None:
        return

    handler = logging.StreamHandler(sys.stderr)
    if args.log_format == "json":
        from vgi_rpc.logging_utils import VgiJsonFormatter

        handler.setFormatter(VgiJsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(name)-30s %(levelname)-5s %(message)s"))

    numeric_level = logging.getLevelNamesMapping()[level]
    targets: list[str] = args.log_logger if args.log_logger else ["vgi_rpc"]

    for name in targets:
        if name not in _KNOWN_LOGGER_NAMES and not name.startswith("vgi_rpc.service."):
            sys.stderr.write(f"Warning: unknown logger '{name}'\n")
            sys.stderr.flush()
        logger = logging.getLogger(name)
        logger.setLevel(numeric_level)
        logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Verbose log callback
# ---------------------------------------------------------------------------


def _log_to_stderr(msg: Message) -> None:
    """Write a log message to stderr."""
    sys.stderr.write(f"[{msg.level.value}] {msg.message}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _format_table(suite: ConformanceSuite) -> str:
    """Format results as a human-readable table."""
    lines: list[str] = []
    lines.append(f"vgi-rpc-test: {suite.passed} passed, {suite.failed} failed ({suite.duration_ms / 1000:.2f}s)")
    lines.append("")

    for r in suite.results:
        status = "PASS" if r.passed else "FAIL"
        lines.append(f"  {r.name:<45s} {status:>4s}  {r.duration_ms:>7.1f}ms")
        if r.error:
            lines.append(f"    {r.error}")

    return "\n".join(lines)


def _format_json(suite: ConformanceSuite) -> str:
    """Format results as JSON."""
    data: dict[str, object] = {
        "total": suite.total,
        "passed": suite.passed,
        "failed": suite.failed,
        "skipped": suite.skipped,
        "duration_ms": round(suite.duration_ms, 1),
        "results": [
            {
                "name": r.name,
                "category": r.category,
                "passed": r.passed,
                "duration_ms": round(r.duration_ms, 1),
                "error": r.error,
            }
            for r in suite.results
        ],
    }
    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Transport creation
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _open_pipe_transport(
    cmd: str,
    shm_size: int | None,
    log_collector: LogCollector,
) -> Iterator[ConformanceService]:
    """Create a pipe transport connection to the server under test."""
    cmd_parts = shlex.split(cmd, posix=sys.platform != "win32")
    transport = SubprocessTransport(cmd_parts)

    effective_transport: SubprocessTransport | _ShmTransportWrapper = transport
    shm_segment = None

    if shm_size is not None:
        from vgi_rpc.shm import ShmSegment

        shm_segment = ShmSegment.create(shm_size)
        effective_transport = _ShmTransportWrapper(transport, shm_segment)

    try:
        with RpcConnection(ConformanceService, effective_transport, on_log=log_collector) as proxy:  # type: ignore[type-abstract]
            yield proxy
    finally:
        if shm_segment is not None:
            shm_segment.unlink()
            with contextlib.suppress(BufferError):
                shm_segment.close()
        transport.close()


@contextlib.contextmanager
def _open_http_transport(
    url: str,
    prefix: str,
    log_collector: LogCollector,
) -> Iterator[ConformanceService]:
    """Create an HTTP transport connection to the server under test."""
    try:
        from vgi_rpc.http import http_connect
    except ImportError:
        sys.stderr.write("HTTP transport requires vgi-rpc[http]: pip install vgi-rpc[http]\n")
        sys.exit(2)

    with http_connect(ConformanceService, url, prefix=prefix, on_log=log_collector) as proxy:  # type: ignore[type-abstract]
        yield proxy


@contextlib.contextmanager
def _open_unix_transport(
    path: str,
    log_collector: LogCollector,
) -> Iterator[ConformanceService]:
    """Create a Unix socket transport connection to the server under test."""
    from vgi_rpc.rpc import unix_connect

    with unix_connect(ConformanceService, path, on_log=log_collector) as proxy:  # type: ignore[type-abstract]
        yield proxy


# ---------------------------------------------------------------------------
# Progress callback
# ---------------------------------------------------------------------------


def _make_progress_callback() -> Callable[[ConformanceResult], None] | None:
    """Create a progress callback for real-time output on TTY stderr."""
    if not sys.stderr.isatty():
        return None

    def _progress(result: ConformanceResult) -> None:
        status = "PASS" if result.passed else "FAIL"
        sys.stderr.write(f"  {result.name:<45s} {status}\n")
        sys.stderr.flush()

    return _progress


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="vgi-rpc-test",
        description="Standalone conformance test runner for vgi-rpc implementations.",
    )

    # Transport
    transport_group = parser.add_argument_group("transport")
    transport_excl = transport_group.add_mutually_exclusive_group()
    transport_excl.add_argument("--cmd", "-c", metavar="CMD", help="Subprocess command to test (pipe transport)")
    transport_excl.add_argument("--url", "-u", metavar="URL", help="HTTP base URL to test")
    transport_excl.add_argument("--unix", metavar="PATH", help="Unix domain socket path to test")
    transport_group.add_argument("--prefix", default="/vgi", help="URL path prefix (default: /vgi)")
    transport_group.add_argument(
        "--shm",
        type=int,
        metavar="SIZE",
        default=None,
        help="Enable shared memory transport with SIZE bytes (only with --cmd)",
    )

    # Test selection
    selection_group = parser.add_argument_group("test selection")
    selection_group.add_argument(
        "--filter", "-k", metavar="PATTERN", help="Comma-separated glob patterns (e.g. 'scalar*,void*')"
    )
    selection_group.add_argument("--list", "-l", action="store_true", help="List available tests and exit")

    # Output
    output_group = parser.add_argument_group("output")
    output_group.add_argument(
        "--format", "-f", choices=["auto", "json", "table"], default="auto", help="Output format (default: auto)"
    )
    output_group.add_argument("--output", "-o", metavar="FILE", help="Output file (default: stdout)")

    # Logging
    log_group = parser.add_argument_group("logging")
    log_group.add_argument("--verbose", "-v", action="store_true", help="Show server log messages on stderr")
    log_group.add_argument("--debug", action="store_true", help="Enable DEBUG on all vgi_rpc loggers to stderr")
    log_group.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level for vgi_rpc loggers"
    )
    log_group.add_argument("--log-logger", action="append", metavar="NAME", help="Target specific logger(s)")
    log_group.add_argument("--log-format", choices=["text", "json"], default="text", help="Stderr log format")

    # Other
    parser.add_argument("--version", "-V", action="store_true", help="Show version and exit")

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Run the conformance test CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        try:
            version = importlib.metadata.version("vgi-rpc")
        except importlib.metadata.PackageNotFoundError:
            version = "unknown"
        print(f"vgi-rpc-test {version}")
        sys.exit(0)

    # Parse filter patterns
    filter_patterns: list[str] | None = None
    if args.filter:
        filter_patterns = [p.strip() for p in args.filter.split(",") if p.strip()]

    # --list mode
    if args.list:
        tests = list_conformance_tests(filter_patterns)
        for name in tests:
            print(name)
        sys.exit(0)

    # Validate transport
    if not args.cmd and not args.url and not args.unix:
        parser.error("Either --cmd, --url, or --unix is required (unless using --list)")

    if args.shm is not None and not args.cmd:
        parser.error("--shm requires --cmd")

    # Configure logging
    _configure_logging(args)

    # Create log collector with optional verbose output
    effective_collector: LogCollector
    if args.verbose:

        class _VerboseLogCollector(LogCollector):
            """Log collector that also prints to stderr."""

            def __call__(self, msg: Message) -> None:
                _log_to_stderr(msg)
                super().__call__(msg)

        effective_collector = _VerboseLogCollector()
    else:
        effective_collector = LogCollector()

    # Open transport and run
    try:
        if args.cmd:
            ctx_manager = _open_pipe_transport(args.cmd, args.shm, effective_collector)
        elif args.unix:
            ctx_manager = _open_unix_transport(args.unix, effective_collector)
        else:
            ctx_manager = _open_http_transport(args.url, args.prefix, effective_collector)

        with ctx_manager as proxy:
            # Determine output format
            fmt = args.format
            if fmt == "auto":
                is_tty = sys.stdout.isatty()
                fmt = "table" if is_tty else "json"

            # Run tests
            progress_cb = _make_progress_callback() if fmt == "table" else None
            suite = run_conformance(
                proxy,
                effective_collector,
                filter_patterns=filter_patterns,
                on_progress=progress_cb,
            )

            # Format output
            output_text = _format_json(suite) if fmt == "json" else _format_table(suite)

            # Write output
            if args.output:
                with open(args.output, "w") as f:
                    f.write(output_text + "\n")
            else:
                print(output_text)

            # Exit code
            sys.exit(0 if suite.success else 1)

    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted\n")
        sys.exit(2)
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(2)


if __name__ == "__main__":
    main()
