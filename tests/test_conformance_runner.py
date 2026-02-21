# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the conformance test runner."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from vgi_rpc.conformance import (
    ConformanceResult,
    ConformanceService,
    ConformanceServiceImpl,
    LogCollector,
    list_conformance_tests,
    run_conformance,
)
from vgi_rpc.rpc import serve_pipe

_CONFORMANCE_PIPE = str(Path(__file__).parent / "serve_conformance_pipe.py")


class TestRunnerViaPipe:
    """Run the full conformance suite against the reference Python implementation."""

    def test_full_suite_all_pass(self) -> None:
        """All conformance tests should pass against the reference implementation."""
        log_collector = LogCollector()
        with serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=log_collector) as proxy:
            suite = run_conformance(proxy, log_collector)
        assert suite.success, f"Failed tests: {[r.name for r in suite.results if not r.passed]}"
        assert suite.total > 0
        assert suite.passed == suite.total
        assert suite.failed == 0

    def test_filter_mechanism(self) -> None:
        """Filter should limit which tests run."""
        log_collector = LogCollector()
        with serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=log_collector) as proxy:
            suite = run_conformance(proxy, log_collector, filter_patterns=["scalar_echo*"])
        assert suite.total == 5
        assert suite.passed == 5

    def test_filter_multiple_patterns(self) -> None:
        """Multiple filter patterns should match union."""
        log_collector = LogCollector()
        with serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=log_collector) as proxy:
            suite = run_conformance(proxy, log_collector, filter_patterns=["scalar_echo*", "void*"])
        assert suite.total == 7  # 5 scalar_echo + 2 void
        assert suite.passed == 7

    def test_progress_callback(self) -> None:
        """Progress callback should be called for each test."""
        progress_results: list[ConformanceResult] = []
        log_collector = LogCollector()
        with serve_pipe(ConformanceService, ConformanceServiceImpl(), on_log=log_collector) as proxy:
            suite = run_conformance(
                proxy, log_collector, filter_patterns=["void*"], on_progress=progress_results.append
            )
        assert len(progress_results) == suite.total
        assert all(r.passed for r in progress_results)


class TestListConformanceTests:
    """Test the list_conformance_tests utility."""

    def test_list_all(self) -> None:
        """Should list all registered tests."""
        tests = list_conformance_tests()
        assert len(tests) > 50
        assert all("." in t for t in tests)
        # Should be sorted
        assert tests == sorted(tests)

    def test_list_filtered(self) -> None:
        """Should filter tests by pattern."""
        tests = list_conformance_tests(["scalar_echo*"])
        assert len(tests) == 5
        assert all(t.startswith("scalar_echo.") for t in tests)

    def test_list_no_match(self) -> None:
        """Should return empty for non-matching pattern."""
        tests = list_conformance_tests(["nonexistent*"])
        assert tests == []


class TestCliEntryPoint:
    """Test the CLI entry point via subprocess."""

    def test_list(self) -> None:
        """--list should print test names and exit 0."""
        result = subprocess.run(
            [sys.executable, "-m", "vgi_rpc.conformance._test_cli", "--list"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        assert len(lines) > 50
        assert "scalar_echo.echo_string" in lines

    def test_list_filtered(self) -> None:
        """--list with --filter should print only matching tests."""
        result = subprocess.run(
            [sys.executable, "-m", "vgi_rpc.conformance._test_cli", "--list", "--filter", "void*"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        assert len(lines) == 2
        assert all(line.startswith("void.") for line in lines)

    def test_json_output(self) -> None:
        """--format json with --cmd should produce valid JSON and exit 0."""
        cmd = f"{sys.executable} {_CONFORMANCE_PIPE}"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "vgi_rpc.conformance._test_cli",
                "--cmd",
                cmd,
                "--filter",
                "scalar_echo*",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        data = json.loads(result.stdout)
        assert data["total"] == 5
        assert data["passed"] == 5
        assert data["failed"] == 0

    def test_table_output(self) -> None:
        """--format table with --cmd should produce readable output and exit 0."""
        cmd = f"{sys.executable} {_CONFORMANCE_PIPE}"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "vgi_rpc.conformance._test_cli",
                "--cmd",
                cmd,
                "--filter",
                "void*",
                "--format",
                "table",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "vgi-rpc-test:" in result.stdout
        assert "PASS" in result.stdout

    def test_version(self) -> None:
        """--version should print version and exit 0."""
        result = subprocess.run(
            [sys.executable, "-m", "vgi_rpc.conformance._test_cli", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "vgi-rpc-test" in result.stdout

    def test_no_transport_error(self) -> None:
        """Missing transport should exit 2."""
        result = subprocess.run(
            [sys.executable, "-m", "vgi_rpc.conformance._test_cli"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 2
