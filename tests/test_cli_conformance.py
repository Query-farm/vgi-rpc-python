"""Tests for the vgi-rpc CLI tool against the conformance service."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from pyarrow import ipc
from typer.testing import CliRunner

from vgi_rpc.cli import app

if TYPE_CHECKING:
    from collections.abc import Iterator

runner = CliRunner()

# ---------------------------------------------------------------------------
# Worker paths
# ---------------------------------------------------------------------------

_DESCRIBE_WORKER = str(Path(__file__).parent / "serve_conformance_describe.py")
_PIPE_CMD = f"{sys.executable} {_DESCRIBE_WORKER}"

_DESCRIBE_HTTP_WORKER = str(Path(__file__).parent / "serve_conformance_describe_http.py")
_DESCRIBE_UNIX_WORKER = str(Path(__file__).parent / "serve_conformance_describe_unix.py")
_DESCRIBE_UNIX_THREADED_WORKER = str(Path(__file__).parent / "serve_conformance_describe_unix_threaded.py")


# ---------------------------------------------------------------------------
# HTTP fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conformance_describe_http_port() -> Iterator[int]:
    """Spawn an HTTP server with describe enabled for the conformance service."""
    from tests.conftest import _wait_for_http

    proc = subprocess.Popen(
        [sys.executable, _DESCRIBE_HTTP_WORKER],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="module")
def conformance_describe_unix_path() -> Iterator[str]:
    """Spawn a Unix socket server with describe enabled for the conformance service."""
    from tests.conftest import _short_unix_path, _wait_for_unix

    path = _short_unix_path("cli-conf")
    proc = subprocess.Popen(
        [sys.executable, _DESCRIBE_UNIX_WORKER, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture(scope="module")
def conformance_describe_unix_threaded_path() -> Iterator[str]:
    """Spawn a threaded Unix socket server with describe enabled for the conformance service."""
    from tests.conftest import _short_unix_path, _wait_for_unix

    path = _short_unix_path("cli-ct")
    proc = subprocess.Popen(
        [sys.executable, _DESCRIBE_UNIX_THREADED_WORKER, path],
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Parametrized transport fixture
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pipe", "http", "unix", "unix_threaded"])
def transport_args(request: pytest.FixtureRequest) -> list[str]:
    """Return CLI args that select a transport (``--cmd …``, ``--url …``, or ``--unix …``)."""
    if request.param == "pipe":
        return ["--cmd", _PIPE_CMD]
    elif request.param == "unix":
        path: str = request.getfixturevalue("conformance_describe_unix_path")
        return ["--unix", path]
    elif request.param == "unix_threaded":
        path = request.getfixturevalue("conformance_describe_unix_threaded_path")
        return ["--unix", path]
    port: int = request.getfixturevalue("conformance_describe_http_port")
    return ["--url", f"http://127.0.0.1:{port}"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _invoke(args: list[str], input: str | None = None) -> Any:
    """Invoke the CLI app with the given args.

    Returns ``Any`` because typer has no type stubs — ``runner.invoke``
    returns ``click.testing.Result`` at runtime but ``Any`` to mypy.
    """
    return runner.invoke(app, args, input=input, catch_exceptions=False)


# ---------------------------------------------------------------------------
# describe tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceDescribe:
    """Tests for the describe command against the conformance service."""

    def test_describe_json(self, transport_args: list[str]) -> None:
        """Describe with JSON output lists all conformance methods."""
        result = _invoke([*transport_args, "--format", "json", "describe"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["protocol_name"] == "ConformanceService"
        methods = data["methods"]
        # Spot-check a selection of methods and their types
        assert methods["echo_string"]["method_type"] == "unary"
        assert methods["echo_int"]["method_type"] == "unary"
        assert methods["void_noop"]["method_type"] == "unary"
        assert methods["produce_n"]["method_type"] == "stream"
        assert methods["exchange_scale"]["method_type"] == "stream"
        # Should have many methods (40+)
        assert len(methods) >= 40

    def test_describe_table(self, transport_args: list[str]) -> None:
        """Describe with table output shows service name and methods."""
        result = _invoke([*transport_args, "--format", "table", "describe"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "ConformanceService" in result.output
        assert "echo_string" in result.output
        assert "produce_n" in result.output
        assert "exchange_scale" in result.output


# ---------------------------------------------------------------------------
# call — scalar echo tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallScalar:
    """Tests for scalar echo methods via the CLI."""

    def test_echo_string(self, transport_args: list[str]) -> None:
        """Echo a string value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_string", "value=hello"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "hello"

    def test_echo_int(self, transport_args: list[str]) -> None:
        """Echo an integer value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_int", "value=42"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == 42

    def test_echo_float(self, transport_args: list[str]) -> None:
        """Echo a float value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_float", "value=3.14"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert abs(data["result"] - 3.14) < 0.001

    def test_echo_bool(self, transport_args: list[str]) -> None:
        """Echo a boolean value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_bool", "value=true"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] is True

    def test_echo_enum(self, transport_args: list[str]) -> None:
        """Echo an enum value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_enum", "status=ACTIVE"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "ACTIVE"


# ---------------------------------------------------------------------------
# call — annotated type tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallAnnotated:
    """Tests for annotated type methods via the CLI."""

    def test_echo_int32(self, transport_args: list[str]) -> None:
        """Echo an int32 value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_int32", "value=99"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == 99

    def test_echo_float32(self, transport_args: list[str]) -> None:
        """Echo a float32 value."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_float32", "value=1.5"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert abs(data["result"] - 1.5) < 0.001


# ---------------------------------------------------------------------------
# call — multi-param and defaults tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallMultiParam:
    """Tests for multi-param and default-value methods via the CLI."""

    def test_add_floats(self, transport_args: list[str]) -> None:
        """Add two floats."""
        result = _invoke([*transport_args, "--format", "json", "call", "add_floats", "a=1.5", "b=2.5"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == 4.0

    def test_concatenate_default(self, transport_args: list[str]) -> None:
        """Concatenate with default separator."""
        result = _invoke([*transport_args, "--format", "json", "call", "concatenate", "prefix=hello", "suffix=world"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "hello-world"

    def test_concatenate_custom_sep(self, transport_args: list[str]) -> None:
        """Concatenate with custom separator."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "concatenate", "prefix=a", "suffix=b", "separator=_"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "a_b"

    def test_with_defaults_all(self, transport_args: list[str]) -> None:
        """All params specified for with_defaults."""
        result = _invoke(
            [
                *transport_args,
                "--format",
                "json",
                "call",
                "with_defaults",
                "required=7",
                "optional_str=custom",
                "optional_int=99",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "required=7, optional_str=custom, optional_int=99"

    def test_with_defaults_partial(self, transport_args: list[str]) -> None:
        """Only required param specified — defaults applied."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "with_defaults", "required=5"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "required=5, optional_str=default, optional_int=42"


# ---------------------------------------------------------------------------
# call — void return tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallVoid:
    """Tests for void-returning methods via the CLI."""

    def test_void_noop(self, transport_args: list[str]) -> None:
        """No-op returning void."""
        result = _invoke([*transport_args, "--format", "json", "call", "void_noop"])
        assert result.exit_code == 0, f"Failed: {result.output}"

    def test_void_with_param(self, transport_args: list[str]) -> None:
        """Void with a parameter."""
        result = _invoke([*transport_args, "--format", "json", "call", "void_with_param", "value=42"])
        assert result.exit_code == 0, f"Failed: {result.output}"


# ---------------------------------------------------------------------------
# call — complex types via --json (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallComplexViaJson:
    """Tests for complex type methods via ``--json``."""

    def test_echo_list(self, transport_args: list[str]) -> None:
        """Echo a list of strings via --json."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "echo_list", "--json", '{"values": ["a", "b", "c"]}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == ["a", "b", "c"]

    def test_echo_dict(self, transport_args: list[str]) -> None:
        """Echo a dict mapping via --json."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "echo_dict", "--json", '{"mapping": {"x": 1, "y": 2}}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == {"x": 1, "y": 2}

    def test_echo_nested_list(self, transport_args: list[str]) -> None:
        """Echo a nested list via --json."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "echo_nested_list", "--json", '{"matrix": [[1,2],[3,4]]}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == [[1, 2], [3, 4]]

    def test_echo_optional_none(self, transport_args: list[str]) -> None:
        """Echo an optional string with None via --json."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "echo_optional_string", "--json", '{"value": null}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] is None

    def test_echo_optional_non_none(self, transport_args: list[str]) -> None:
        """Echo an optional string with a value via --json."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "echo_optional_string", "--json", '{"value": "hello"}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "hello"


# ---------------------------------------------------------------------------
# call — error propagation tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallErrors:
    """Tests for error propagation via the CLI."""

    def test_raise_value_error(self, transport_args: list[str]) -> None:
        """ValueError propagated as exit 1."""
        result = _invoke([*transport_args, "--format", "json", "call", "raise_value_error", "message=boom"])
        assert result.exit_code == 1

    def test_raise_runtime_error(self, transport_args: list[str]) -> None:
        """RuntimeError propagated as exit 1."""
        result = _invoke([*transport_args, "--format", "json", "call", "raise_runtime_error", "message=crash"])
        assert result.exit_code == 1

    def test_raise_type_error(self, transport_args: list[str]) -> None:
        """TypeError propagated as exit 1."""
        result = _invoke([*transport_args, "--format", "json", "call", "raise_type_error", "message=bad"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# call — logging tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceCallLogging:
    """Tests for client-directed logging via the CLI."""

    def test_echo_with_info_log(self, transport_args: list[str]) -> None:
        """Verbose mode shows server log messages."""
        result = _invoke(
            [*transport_args, "--format", "json", "--verbose", "call", "echo_with_info_log", "value=test"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        # CliRunner mixes stderr into output; look for the log line
        assert "[INFO]" in result.output
        # The JSON result should also be present
        json_lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("{")]
        assert len(json_lines) >= 1
        data = json.loads(json_lines[0])
        assert data["result"] == "test"


# ---------------------------------------------------------------------------
# call — producer stream tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceProducerStream:
    """Tests for producer stream methods via the CLI."""

    def test_produce_n_json(self, transport_args: list[str]) -> None:
        """Produce 3 batches in JSON format."""
        result = _invoke([*transport_args, "--format", "json", "call", "--no-stdin", "produce_n", "count=3"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 3
        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["index"] == i
            assert data["value"] == i * 10

    def test_produce_n_table(self, transport_args: list[str]) -> None:
        """Produce 3 batches in table format."""
        result = _invoke([*transport_args, "--format", "table", "call", "--no-stdin", "produce_n", "count=3"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "index" in result.output
        assert "value" in result.output

    def test_produce_empty(self, transport_args: list[str]) -> None:
        """Produce zero batches."""
        result = _invoke([*transport_args, "--format", "json", "call", "--no-stdin", "produce_empty"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        # No data lines
        lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("{")]
        assert len(lines) == 0

    def test_produce_single(self, transport_args: list[str]) -> None:
        """Produce exactly one batch."""
        result = _invoke([*transport_args, "--format", "json", "call", "--no-stdin", "produce_single"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("{")]
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["index"] == 0

    def test_produce_large_batches(self, transport_args: list[str]) -> None:
        """Multi-row batches expand correctly."""
        result = _invoke(
            [
                *transport_args,
                "--format",
                "json",
                "call",
                "--no-stdin",
                "produce_large_batches",
                "rows_per_batch=3",
                "batch_count=2",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 6  # 3 rows * 2 batches
        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["index"] == i
            assert data["value"] == i * 10

    def test_produce_error_mid_stream(self, transport_args: list[str]) -> None:
        """Error mid-stream produces exit 1."""
        result = _invoke(
            [
                *transport_args,
                "--format",
                "json",
                "call",
                "--no-stdin",
                "produce_error_mid_stream",
                "emit_before_error=2",
            ],
        )
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# call — producer stream header tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceProducerStreamHeaders:
    """Tests for producer streams with headers via the CLI."""

    def test_header_json(self, transport_args: list[str]) -> None:
        """Stream header appears as ``__header__`` wrapper in JSON output."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "--no-stdin", "produce_with_header", "count=2"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        # First line is the header wrapper
        header_line = json.loads(lines[0])
        assert "__header__" in header_line
        hdr = header_line["__header__"]
        assert hdr["total_expected"] == 2
        assert "producing" in hdr["description"]
        # Remaining lines are data rows
        data_lines = [json.loads(line) for line in lines[1:]]
        assert len(data_lines) == 2
        assert data_lines[0]["index"] == 0

    def test_header_table(self, transport_args: list[str]) -> None:
        """Stream header appears as ``Header:`` section in table output."""
        result = _invoke(
            [*transport_args, "--format", "table", "call", "--no-stdin", "produce_with_header", "count=2"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "Header:" in result.output
        assert "total_expected: 2" in result.output
        # Data table should follow
        assert "index" in result.output
        assert "value" in result.output

    def test_header_and_logs(self, transport_args: list[str]) -> None:
        """Verbose mode with header shows log + header + data."""
        result = _invoke(
            [
                *transport_args,
                "--format",
                "json",
                "--verbose",
                "call",
                "--no-stdin",
                "produce_with_header_and_logs",
                "count=2",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        # Log lines (from stderr, mixed into output by CliRunner)
        log_lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("[")]
        assert len(log_lines) >= 1  # at least the init log
        # JSON lines — header + data
        json_lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("{")]
        assert len(json_lines) >= 3  # header + 2 data rows


# ---------------------------------------------------------------------------
# call — exchange stream tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestConformanceExchangeStream:
    """Tests for exchange stream methods via the CLI."""

    def test_exchange_scale(self, transport_args: list[str]) -> None:
        """Scale input values by a factor."""
        stdin_data = '{"value": 5.0}\n{"value": 10.0}\n'
        result = _invoke(
            [*transport_args, "--format", "json", "call", "exchange_scale", "factor=2.0"],
            input=stdin_data,
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 2
        assert json.loads(lines[0])["value"] == 10.0
        assert json.loads(lines[1])["value"] == 20.0

    def test_exchange_accumulate(self, transport_args: list[str]) -> None:
        """Running sum across exchanges."""
        stdin_data = '{"value": 3.0}\n{"value": 7.0}\n'
        result = _invoke(
            [*transport_args, "--format", "json", "call", "exchange_accumulate"],
            input=stdin_data,
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["running_sum"] == 3.0
        assert first["exchange_count"] == 1
        second = json.loads(lines[1])
        assert second["running_sum"] == 10.0
        assert second["exchange_count"] == 2


# ---------------------------------------------------------------------------
# Arrow format tests (pipe only)
# ---------------------------------------------------------------------------


class TestConformanceArrowFormat:
    """Tests for ``--format arrow`` output with conformance methods."""

    def test_arrow_unary(self, tmp_path: Path) -> None:
        """Unary result written as Arrow IPC to a file."""
        out = tmp_path / "result.arrow"
        result = _invoke(
            ["--cmd", _PIPE_CMD, "--format", "arrow", "--output", str(out), "call", "echo_int", "value=42"],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        with ipc.open_stream(pa.OSFile(str(out), "rb")) as reader:
            batch = reader.read_next_batch()
            assert batch.num_rows == 1
            assert batch.column("result")[0].as_py() == 42

    def test_arrow_stream_producer(self, tmp_path: Path) -> None:
        """Producer stream written as Arrow IPC to a file."""
        out = tmp_path / "stream.arrow"
        result = _invoke(
            [
                "--cmd",
                _PIPE_CMD,
                "--format",
                "arrow",
                "--output",
                str(out),
                "call",
                "--no-stdin",
                "produce_n",
                "count=3",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        index_values: list[object] = []
        with ipc.open_stream(pa.OSFile(str(out), "rb")) as reader:
            for batch in reader:
                index_values.extend(batch.to_pydict()["index"])
        assert index_values == [0, 1, 2]

    def test_arrow_stream_with_header(self, tmp_path: Path) -> None:
        """Stream with header writes header IPC stream followed by data IPC stream."""
        out = tmp_path / "header_stream.arrow"
        result = _invoke(
            [
                "--cmd",
                _PIPE_CMD,
                "--format",
                "arrow",
                "--output",
                str(out),
                "call",
                "--no-stdin",
                "produce_with_header",
                "count=2",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        with open(out, "rb") as f:
            # Header stream
            header_reader = ipc.open_stream(f)
            hdr_batch = header_reader.read_next_batch()
            assert hdr_batch.num_rows == 1
            assert hdr_batch.column("total_expected")[0].as_py() == 2
            assert "producing" in hdr_batch.column("description")[0].as_py()
            # Drain header stream
            try:
                while True:
                    header_reader.read_next_batch()
            except StopIteration:
                pass
            # Data stream
            data_reader = ipc.open_stream(f)
            data_rows: list[object] = []
            try:
                while True:
                    batch = data_reader.read_next_batch()
                    data_rows.extend(batch.column("index").to_pylist())
            except StopIteration:
                pass
            assert data_rows == [0, 1]

    def test_arrow_exchange(self, tmp_path: Path) -> None:
        """Exchange stream written as Arrow IPC with stdin input."""
        out = tmp_path / "exchange.arrow"
        stdin_data = '{"value": 5.0}\n{"value": 10.0}\n'
        result = _invoke(
            [
                "--cmd",
                _PIPE_CMD,
                "--format",
                "arrow",
                "--output",
                str(out),
                "call",
                "exchange_scale",
                "factor=2.0",
            ],
            input=stdin_data,
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        values: list[object] = []
        with ipc.open_stream(pa.OSFile(str(out), "rb")) as reader:
            for batch in reader:
                values.extend(batch.to_pydict()["value"])
        assert values == [10.0, 20.0]
