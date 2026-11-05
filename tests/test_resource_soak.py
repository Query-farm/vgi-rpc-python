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
    _time_limit,
)


def _snapshot(rss: int, *, descriptors: int = 5, threads: int = 2, children: int = 0) -> ProcessResourceSnapshot:
    return ProcessResourceSnapshot(rss, descriptors, threads, children)


def test_rss_slope_identifies_steady_retained_growth() -> None:
    """The median epoch delta identifies steady retained growth."""
    assert _rss_slope(tuple(_snapshot(value) for value in (100, 120, 140, 160))) == pytest.approx(20.0)


def test_rss_slope_ignores_one_time_allocator_step() -> None:
    """One arena reservation followed by a plateau is not a steady leak."""
    assert _rss_slope(tuple(_snapshot(value) for value in (100, 100, 180, 180, 180, 180))) == 0.0


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
        (_snapshot(200, descriptors=6), _snapshot(200, descriptors=7)),
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


_STRICT_LIMITS = ResourceSoakLimits(
    rss_growth_bytes=50,
    rss_slope_bytes_per_epoch=10,
    descriptor_growth=0,
    thread_growth=0,
    child_growth=0,
)


def _report(**series: tuple[int, ...]) -> ResourceSoakReport:
    """Build a report from one resource's values, the first being the baseline."""
    [(name, values)] = series.items()
    baseline, *samples = (_snapshot(100, **{name: value}) for value in values)
    return ResourceSoakReport("test", "series", 10, baseline, tuple(samples), 0.0)


@pytest.mark.parametrize("name", ["threads", "descriptors"])
@pytest.mark.parametrize(
    "values",
    [
        (4, 6, 6, 6, 6, 6),  # The Windows python-http pool: grows in the first epoch.
        (4, 4, 4, 6, 6, 6),  # The same step, later in the soak.
        (4, 4, 4, 4, 4, 6),  # The same step, in the last epoch.
        (4, 6, 5, 5, 5, 5),  # A step that partly drains.
    ],
)
def test_report_accepts_one_time_pool_step(name: str, values: tuple[int, ...]) -> None:
    """A pool that grows once and then holds steady is not a leak."""
    _assert_report(_report(**{name: values}), _STRICT_LIMITS)


@pytest.mark.parametrize("name", ["threads", "descriptors"])
@pytest.mark.parametrize(
    "values",
    [
        (4, 5, 6, 7, 8, 9),  # Steady growth every epoch.
        (4, 6, 6, 7, 8, 9),  # One step, then more growth.
        (4, 4, 6, 6, 8, 8),  # Growth in bursts.
    ],
)
def test_report_rejects_growth_after_a_step(name: str, values: tuple[int, ...]) -> None:
    """Growth that continues after the largest step still fails."""
    with pytest.raises(AssertionError, match=f"{name} retained"):
        _assert_report(_report(**{name: values}), _STRICT_LIMITS)


def test_report_rejects_any_retained_child() -> None:
    """Child processes get no one-time allowance."""
    with pytest.raises(AssertionError, match="children retained 1"):
        _assert_report(_report(children=(0, 1, 1, 1, 1, 1)), _STRICT_LIMITS)


@pytest.mark.parametrize(
    ("scale", "limit"),
    [(None, 50.0), ("1", 50.0), ("20", 240.0), ("not-a-number", 50.0), ("21", 50.0)],
)
def test_time_limit_follows_scale(monkeypatch: pytest.MonkeyPatch, scale: str | None, limit: float) -> None:
    """The time limit grows with the workload, and a bad scale keeps the base limit."""
    if scale is None:
        monkeypatch.delenv("VGI_RPC_SOAK_SCALE", raising=False)
    else:
        monkeypatch.setenv("VGI_RPC_SOAK_SCALE", scale)
    assert _time_limit() == limit
