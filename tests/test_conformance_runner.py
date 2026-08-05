# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the conformance test runner."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from vgi_rpc.conformance import (
    ConformanceResult,
    ConformanceService,
    ConformanceServiceImpl,
    LogCollector,
    _runner,
    list_conformance_tests,
    run_conformance,
)
from vgi_rpc.rpc import RpcError, serve_pipe

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
        # When ``transport`` is not specified, ``run_conformance`` runs every
        # registered test regardless of the per-test ``transports`` filter.
        # HTTP-only tests (``http_response_cap.*``) self-skip via
        # ``_ConformanceSkip`` because the LogCollector has no
        # ``http_base_url``; they appear as skipped, not failed.
        assert suite.passed + suite.skipped == suite.total
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

    def test_exclusion_only_keeps_everything_else(self) -> None:
        """A filter of only exclusions should mean 'all but these'."""
        everything = list_conformance_tests()
        kept = list_conformance_tests(["!large_payload.echo_binary_over_int32_max"])
        assert "large_payload.echo_binary_over_int32_max" in everything
        assert "large_payload.echo_binary_over_int32_max" not in kept
        assert kept == [t for t in everything if t != "large_payload.echo_binary_over_int32_max"]

    def test_exclusion_beats_inclusion(self) -> None:
        """An exclusion should win over an include that also matches."""
        tests = list_conformance_tests(["large_payload*", "!*over_int32_max"])
        assert tests == ["large_payload.echo_binary_4mib"]

    def test_exclusion_matches_a_whole_category(self) -> None:
        """Category-level globs should exclude as well as include."""
        tests = list_conformance_tests(["!large_payload"])
        assert not any(t.startswith("large_payload.") for t in tests)


class TestHugePayloadIsRequired:
    """The >2 GiB test must not be skippable by ambient configuration."""

    def test_registered_and_not_env_gated(self) -> None:
        """It should be in the default list, with no environment opt-in."""
        # It was briefly gated behind VGI_RPC_CONFORMANCE_HUGE. A test the
        # ports can silently not run enforces nothing, so the gate is gone;
        # opting out has to be an explicit --filter in the CI configuration.
        assert "large_payload.echo_binary_over_int32_max" in list_conformance_tests()
        source = Path(_runner.__file__).read_text()
        assert "VGI_RPC_CONFORMANCE_HUGE" not in source


class TestTypedRefusalIsConformant:
    """A port that cannot represent 2 GiB may refuse, but not silently.

    Exercised through the helper rather than the test itself so these stay
    cheap: the test body allocates 2**31+1 bytes before it can reach this
    path, and that is not a cost a unit test should pay.
    """

    @staticmethod
    def _refusal() -> RpcError:
        return RpcError(
            error_type="NotImplementedError",
            error_message="value is 2147483649 bytes; the JVM caps an array at 2147483647 elements",
            remote_traceback="",
        )

    def test_refusal_passes_when_connection_survives(self) -> None:
        """A typed error plus a working connection should be accepted."""

        class _Survives:
            def echo_string(self, value: str) -> str:
                return value

        _runner._CURRENT_NOTE = None
        _runner._accept_typed_refusal(cast("ConformanceService", _Survives()), self._refusal())
        # The pass must be qualified in the report, or it reads as a round-trip.
        assert _runner._CURRENT_NOTE is not None
        assert "refused" in _runner._CURRENT_NOTE
        assert "NotImplementedError" in _runner._CURRENT_NOTE

    def test_refusal_fails_when_connection_is_wedged(self) -> None:
        """A refusal that breaks the transport is the deadlock in disguise."""

        class _Wedged:
            def echo_string(self, value: str) -> str:
                raise TimeoutError("no response")

        with pytest.raises(AssertionError, match="wedged the transport"):
            _runner._accept_typed_refusal(cast("ConformanceService", _Wedged()), self._refusal())

    def test_refusal_fails_when_next_call_is_wrong(self) -> None:
        """A live-but-desynced connection should not count as survival."""

        class _Desynced:
            def echo_string(self, value: str) -> str:
                return "some other response"

        with pytest.raises(AssertionError, match="out of sync"):
            _runner._accept_typed_refusal(cast("ConformanceService", _Desynced()), self._refusal())


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
