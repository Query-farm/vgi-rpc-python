# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``vgi-rpc-test-hosted`` against the reference worker.

The hosted-protocols group is what every VGI SDK's CI runs against its fixture
worker, so it has to pass against the reference -- and has to *fail* when the
declared protocol list is wrong, or a green SDK run would mean nothing.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from vgi_rpc.conformance.secondary import SECONDARY_PROTOCOL_NAME

_EXPECT = f"ConformanceService,{SECONDARY_PROTOCOL_NAME}"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the CLI in a fresh interpreter, as a foreign CI job would."""
    return subprocess.run(
        [sys.executable, "-m", "vgi_rpc.conformance.hosted_protocols", *args],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def _pipe_worker() -> str:
    """Command line of the reference conformance worker over stdio, reflection on."""
    return f'"{sys.executable}" -m vgi_rpc.conformance._cli --pipe --describe'


@pytest.mark.timeout(180)
def test_the_reference_passes_over_stdio() -> None:
    """Every collected case passes; only the HTTP-only ones skip."""
    result = _run("--cmd", _pipe_worker(), "--expect", _EXPECT)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    assert " passed" in result.stdout and "failed" not in result.stdout, result.stdout[-2000:]


@pytest.mark.timeout(180)
def test_the_reference_passes_over_http_with_identity(conformance_http_identity_port: int) -> None:
    """Over HTTP the raw-wire and Identity groups join in."""
    result = _run("--url", f"http://127.0.0.1:{conformance_http_identity_port}", "--expect", _EXPECT, "--identity")
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    assert "skipped" not in result.stdout, f"nothing should skip over HTTP with identity: {result.stdout[-2000:]}"


@pytest.mark.timeout(180)
def test_a_wrong_protocol_order_fails() -> None:
    """The declared order is asserted, not merely the membership."""
    result = _run("--cmd", _pipe_worker(), "--expect", f"{SECONDARY_PROTOCOL_NAME},ConformanceService")
    assert result.returncode != 0
    assert "test_application_protocols_are_listed_in_order" in result.stdout


@pytest.mark.timeout(180)
def test_a_registration_order_no_sort_produces_passes() -> None:
    """The check that catches a listing sorted by name.

    The port suite cannot: ``ConformanceService`` sorts before
    ``conformance.Secondary.v1`` anyway.  This worker registers its protocols
    in reverse-alphabetical order, so a server that sorted its listing would
    fail here.
    """
    from pathlib import Path

    from tests.serve_hosted_reverse import REVERSE_ORDER

    worker = f'"{sys.executable}" "{Path(__file__).with_name("serve_hosted_reverse.py")}"'
    result = _run("--cmd", worker, "--expect", ",".join(REVERSE_ORDER))
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]


def test_identity_requires_http() -> None:
    """Identity is hosted where callers authenticate; the CLI refuses it elsewhere."""
    result = _run("--cmd", _pipe_worker(), "--expect", _EXPECT, "--identity")
    assert result.returncode == 2
    assert "--identity requires --url" in result.stderr
