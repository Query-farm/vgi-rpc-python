# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for portable resource-soak accounting."""

from __future__ import annotations

import pytest

from vgi_rpc.conformance._resource_soak_pytest import (
    ProcessResourceSnapshot,
    ResourceSoakLimits,
    ResourceSoakReport,
    _assert_report,
    _rss_slope,
)


def _snapshot(rss: int, *, descriptors: int = 5, threads: int = 2, children: int = 0) -> ProcessResourceSnapshot:
    return ProcessResourceSnapshot(rss, descriptors, threads, children)


def test_rss_slope_uses_all_epochs() -> None:
    """Least-squares slope identifies steady retained growth."""
    assert _rss_slope(tuple(_snapshot(value) for value in (100, 120, 140, 160))) == pytest.approx(20.0)


def test_report_accepts_a_plateau_after_warmup() -> None:
    """Allocator noise below the configured budgets remains acceptable."""
    baseline = _snapshot(100)
    report = ResourceSoakReport("test", "plateau", 10, baseline, (_snapshot(120), _snapshot(110)), 5.0)
    _assert_report(
        report,
        ResourceSoakLimits(
            rss_growth_bytes=50,
            rss_slope_bytes_per_epoch=10,
            descriptor_growth=0,
            thread_growth=0,
            child_growth=0,
        ),
    )


def test_report_rejects_exact_resource_and_memory_drift() -> None:
    """Retained descriptors and a positive memory slope fail together."""
    baseline = _snapshot(100)
    report = ResourceSoakReport(
        "test",
        "leak",
        10,
        baseline,
        (_snapshot(200, descriptors=7),),
        100.0,
    )
    with pytest.raises(AssertionError, match=r"RSS retained.*RSS slope.*descriptors retained"):
        _assert_report(
            report,
            ResourceSoakLimits(
                rss_growth_bytes=50,
                rss_slope_bytes_per_epoch=10,
                descriptor_growth=0,
                thread_growth=0,
                child_growth=0,
            ),
        )
