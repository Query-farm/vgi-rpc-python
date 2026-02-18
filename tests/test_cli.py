"""Tests for the vgi-rpc CLI tool."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from typer.testing import CliRunner

from vgi_rpc.cli import app

if TYPE_CHECKING:
    from collections.abc import Iterator

runner = CliRunner()

# ---------------------------------------------------------------------------
# Worker paths
# ---------------------------------------------------------------------------

_DESCRIBE_WORKER = str(Path(__file__).parent / "serve_fixture_describe.py")
_PIPE_CMD = f"{sys.executable} {_DESCRIBE_WORKER}"

_DESCRIBE_HTTP_WORKER = str(Path(__file__).parent / "serve_fixture_describe_http.py")


# ---------------------------------------------------------------------------
# HTTP fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def describe_http_port() -> Iterator[int]:
    """Spawn an HTTP server with describe enabled for the test module."""
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


# ---------------------------------------------------------------------------
# Parametrized transport fixture
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pipe", "http"])
def transport_args(request: pytest.FixtureRequest) -> list[str]:
    """Return CLI args that select a transport (``--cmd …`` or ``--url …``)."""
    if request.param == "pipe":
        return ["--cmd", _PIPE_CMD]
    port: int = request.getfixturevalue("describe_http_port")
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
# Error case tests (no transport needed)
# ---------------------------------------------------------------------------


class TestVersion:
    """Tests for the --version flag."""

    def test_version_flag(self) -> None:
        """``--version`` prints version and exits."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert result.output.startswith("vgi-rpc ")

    def test_version_short_flag(self) -> None:
        """``-V`` prints version and exits."""
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert result.output.startswith("vgi-rpc ")


class TestErrorCases:
    """Tests for CLI error handling."""

    def test_no_transport(self) -> None:
        """Neither --url nor --cmd given."""
        result = runner.invoke(app, ["describe"])
        assert result.exit_code != 0

    def test_both_transports(self) -> None:
        """Both --url and --cmd given."""
        result = runner.invoke(app, ["--url", "http://x", "--cmd", "y", "describe"])
        assert result.exit_code != 0

    def test_unknown_method(self) -> None:
        """Call a method that doesn't exist."""
        result = _invoke(["--cmd", _PIPE_CMD, "call", "nonexistent"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# describe tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestDescribe:
    """Tests for the describe command over both transports."""

    def test_describe_json(self, transport_args: list[str]) -> None:
        """Describe with JSON output."""
        result = _invoke([*transport_args, "--format", "json", "describe"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["protocol_name"] == "RpcFixtureService"
        assert "add" in data["methods"]
        assert "generate" in data["methods"]
        assert data["methods"]["add"]["method_type"] == "unary"
        assert data["methods"]["generate"]["method_type"] == "stream"

    def test_describe_table(self, transport_args: list[str]) -> None:
        """Describe with table output."""
        result = _invoke([*transport_args, "--format", "table", "describe"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "RPC Service: RpcFixtureService" in result.output
        assert "add" in result.output
        assert "generate" in result.output


# ---------------------------------------------------------------------------
# call — unary tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestCallUnary:
    """Tests for unary method calls over both transports."""

    def test_add(self, transport_args: list[str]) -> None:
        """Call add with key=value params."""
        result = _invoke([*transport_args, "--format", "json", "call", "add", "a=1.0", "b=2.0"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == 3.0

    def test_greet(self, transport_args: list[str]) -> None:
        """Call greet with string param."""
        result = _invoke([*transport_args, "--format", "json", "call", "greet", "name=world"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "Hello, world!"

    def test_add_json(self, transport_args: list[str]) -> None:
        """Call add with --json param."""
        result = _invoke(
            [*transport_args, "--format", "json", "call", "add", "--json", '{"a": 1.0, "b": 2.0}'],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == 3.0

    def test_noop(self, transport_args: list[str]) -> None:
        """Call noop (no return value)."""
        result = _invoke([*transport_args, "--format", "json", "call", "noop"])
        assert result.exit_code == 0, f"Failed: {result.output}"

    def test_fail_unary(self, transport_args: list[str]) -> None:
        """Call fail_unary and verify error."""
        result = _invoke([*transport_args, "--format", "json", "call", "fail_unary"])
        assert result.exit_code == 1

    def test_echo_color(self, transport_args: list[str]) -> None:
        """Call echo_color with enum param."""
        result = _invoke([*transport_args, "--format", "json", "call", "echo_color", "color=RED"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["result"] == "RED"


# ---------------------------------------------------------------------------
# call — stream tests (parametrized over pipe/http)
# ---------------------------------------------------------------------------


class TestCallStream:
    """Tests for stream method calls over both transports."""

    def test_generate_producer(self, transport_args: list[str]) -> None:
        """Call generate in producer mode (no stdin data)."""
        result = _invoke([*transport_args, "--format", "json", "call", "--no-stdin", "generate", "count=3"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 3
        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["i"] == i
            assert data["value"] == i * 10

    def test_generate_table(self, transport_args: list[str]) -> None:
        """Call generate with table format."""
        result = _invoke([*transport_args, "--format", "table", "call", "--no-stdin", "generate", "count=3"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "i" in result.output
        assert "value" in result.output

    def test_generate_with_logs(self, transport_args: list[str]) -> None:
        """Call generate_with_logs with verbose mode."""
        result = _invoke(
            [
                *transport_args,
                "--format",
                "json",
                "--verbose",
                "call",
                "--no-stdin",
                "generate_with_logs",
                "count=2",
            ],
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        # CliRunner mixes stderr (logs) into output; filter for JSON data lines
        json_lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("{")]
        log_lines = [line for line in result.output.strip().split("\n") if line.strip().startswith("[")]
        assert len(json_lines) == 2
        assert len(log_lines) >= 2  # at least pre-stream log + per-batch logs

    def test_exchange_transform(self, transport_args: list[str]) -> None:
        """Call transform in exchange mode (stdin piped)."""
        stdin_data = '{"value": 5.0}\n{"value": 10.0}\n'
        result = _invoke(
            [*transport_args, "--format", "json", "call", "transform", "factor=2.0"],
            input=stdin_data,
        )
        assert result.exit_code == 0, f"Failed: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 2
        assert json.loads(lines[0])["value"] == 10.0
        assert json.loads(lines[1])["value"] == 20.0

    def test_fail_stream(self, transport_args: list[str]) -> None:
        """Call fail_stream and verify error after partial output."""
        result = _invoke([*transport_args, "--format", "json", "call", "--no-stdin", "fail_stream"])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Adaptive output tests (formatting-only, pipe transport is sufficient)
# ---------------------------------------------------------------------------


class TestAdaptiveOutput:
    """Tests for adaptive output formatting."""

    def test_default_format_non_tty(self) -> None:
        """CliRunner is non-TTY, so auto format should produce compact JSON."""
        result = _invoke(["--cmd", _PIPE_CMD, "call", "add", "a=1.0", "b=2.0"])
        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output.strip())
        assert data["result"] == 3.0

    def test_format_json_forces_compact(self) -> None:
        """--format json always produces compact JSON."""
        result = _invoke(["--cmd", _PIPE_CMD, "--format", "json", "call", "add", "a=1.0", "b=2.0"])
        assert result.exit_code == 0
        assert "\n" not in result.output.strip()

    def test_format_table(self) -> None:
        """--format table forces table output."""
        result = _invoke(["--cmd", _PIPE_CMD, "--format", "table", "call", "add", "a=1.0", "b=2.0"])
        assert result.exit_code == 0
        assert "result" in result.output
        assert "---" in result.output
