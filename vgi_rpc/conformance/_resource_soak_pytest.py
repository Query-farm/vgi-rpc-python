# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Portable black-box resource-soak tests for conformance workers.

The ordinary conformance suite proves that one operation behaves correctly.
These tests repeat representative lifecycles against a dedicated worker and
sample that worker between epochs.  Warm-up happens before the baseline so a
runtime's lazy imports, JIT compilation, thread pools, and allocator arenas are
not mistaken for leaks.

Language runners opt in by supplying ``conformance_resource_soak_target`` as a
function-scoped fixture.  Keeping the fixture optional lets an older runner
import the current shared suite and skip this tranche until it exposes a
dedicated process PID and connection factory.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol, cast

import psutil
import pytest

from vgi_rpc.rpc import RpcError

pytestmark = pytest.mark.timeout(50)

_MIB = 1024 * 1024
_REPORT_ENV = "VGI_RPC_SOAK_REPORT_DIR"


class ResourceSoakStream(Protocol):
    """Minimum stream surface exercised by the portable soak scenarios."""

    def __iter__(self) -> Iterator[object]: ...

    def cancel(self) -> None: ...


class ResourceSoakConnection(Protocol):
    """Minimum conformance proxy surface required by this tranche."""

    def echo_int(self, *, value: int) -> int: ...

    def add_floats(self, *, a: float, b: float) -> float: ...

    def produce_n(self, *, count: int) -> Iterable[object]: ...

    def produce_error_mid_stream(self, *, emit_before_error: int) -> Iterable[object]: ...

    def cancellable_producer(self) -> ResourceSoakStream: ...


class _WindowsProcess(Protocol):
    """Windows-only psutil surface omitted by non-Windows type stubs."""

    def num_handles(self) -> int: ...


@dataclass(frozen=True, slots=True)
class ResourceSoakLimits:
    """Maximum retained resource growth after warm-up.

    Attributes:
        rss_growth_bytes: Maximum last-sample RSS growth above baseline.
        rss_slope_bytes_per_epoch: Maximum positive least-squares RSS slope.
        descriptor_growth: Maximum file-descriptor or Windows-handle growth.
        thread_growth: Maximum worker thread growth.
        child_growth: Maximum descendant-process growth.

    """

    rss_growth_bytes: int = 32 * _MIB
    rss_slope_bytes_per_epoch: int = 2 * _MIB
    descriptor_growth: int = 3
    thread_growth: int = 1
    child_growth: int = 0


@dataclass(frozen=True, slots=True)
class ResourceSoakTarget:
    """One isolated worker process and the client factory that drives it.

    Attributes:
        name: Stable runner/transport label used in diagnostics.
        pid: Operating-system process ID of the worker under observation.
        connect: Create a fresh client connection to the worker.
        limits: Runtime-specific retained-resource budgets.

    """

    name: str
    pid: int
    connect: Callable[[], AbstractContextManager[ResourceSoakConnection]]
    limits: ResourceSoakLimits = ResourceSoakLimits()


@dataclass(frozen=True, slots=True)
class ProcessResourceSnapshot:
    """One operating-system resource sample.

    Attributes:
        rss_bytes: Resident set size reported by the operating system.
        descriptors: Open file descriptors, or handles on Windows.
        threads: Native thread count.
        children: Recursive live descendant-process count.

    """

    rss_bytes: int
    descriptors: int
    threads: int
    children: int


@dataclass(frozen=True, slots=True)
class ResourceSoakReport:
    """Measurements and operation count for one soak scenario.

    Attributes:
        target: Stable target label.
        scenario: Scenario name.
        operations: Number of measured logical operations.
        baseline: Post-warm-up resource sample.
        samples: Resource sample after every measured epoch.
        rss_slope_bytes_per_epoch: Least-squares slope across baseline and epochs.

    """

    target: str
    scenario: str
    operations: int
    baseline: ProcessResourceSnapshot
    samples: tuple[ProcessResourceSnapshot, ...]
    rss_slope_bytes_per_epoch: float

    def as_json(self) -> str:
        """Serialize the report into stable, human-readable JSON."""
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def sample_process_resources(pid: int) -> ProcessResourceSnapshot:
    """Capture the portable process resources used by the soak contract.

    Args:
        pid: Worker process ID.

    Returns:
        The current process-resource snapshot.

    Raises:
        AssertionError: If the worker exited before it could be sampled.

    """
    try:
        process = psutil.Process(pid)
        with process.oneshot():
            descriptors = cast("_WindowsProcess", process).num_handles() if os.name == "nt" else process.num_fds()
            return ProcessResourceSnapshot(
                rss_bytes=process.memory_info().rss,
                descriptors=descriptors,
                threads=process.num_threads(),
                children=len(process.children(recursive=True)),
            )
    except (psutil.NoSuchProcess, psutil.ZombieProcess) as exc:
        raise AssertionError(f"resource-soak worker {pid} exited before sampling") from exc


def _rss_slope(samples: tuple[ProcessResourceSnapshot, ...]) -> float:
    """Return the least-squares RSS slope in bytes per epoch."""
    if len(samples) < 2:
        return 0.0
    count = len(samples)
    mean_x = (count - 1) / 2
    mean_y = sum(sample.rss_bytes for sample in samples) / count
    numerator = sum((index - mean_x) * (sample.rss_bytes - mean_y) for index, sample in enumerate(samples))
    denominator = sum((index - mean_x) ** 2 for index in range(count))
    return numerator / denominator if denominator else 0.0


def _scale() -> int:
    """Return the bounded soak multiplier selected by the environment."""
    raw = os.environ.get("VGI_RPC_SOAK_SCALE", "1")
    try:
        scale = int(raw)
    except ValueError as exc:
        raise pytest.UsageError("VGI_RPC_SOAK_SCALE must be an integer from 1 through 20") from exc
    if not 1 <= scale <= 20:
        raise pytest.UsageError("VGI_RPC_SOAK_SCALE must be an integer from 1 through 20")
    return scale


def _settled_sample(target: ResourceSoakTarget) -> ProcessResourceSnapshot:
    """Allow request teardown to quiesce, then sample the worker."""
    time.sleep(0.05)
    return sample_process_resources(target.pid)


def _build_report(
    target: ResourceSoakTarget,
    scenario: str,
    operations: int,
    baseline: ProcessResourceSnapshot,
    samples: list[ProcessResourceSnapshot],
) -> ResourceSoakReport:
    """Build and optionally persist one scenario report."""
    all_samples = (baseline, *samples)
    report = ResourceSoakReport(
        target=target.name,
        scenario=scenario,
        operations=operations,
        baseline=baseline,
        samples=tuple(samples),
        rss_slope_bytes_per_epoch=_rss_slope(all_samples),
    )
    if report_dir := os.environ.get(_REPORT_ENV):
        path = Path(report_dir)
        path.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", f"{target.name}-{scenario}").strip("-")
        (path / f"{safe_name}.json").write_text(report.as_json() + "\n", encoding="utf-8")
    return report


def _assert_report(report: ResourceSoakReport, limits: ResourceSoakLimits) -> None:
    """Enforce exact-resource drift and allocator-tolerant RSS budgets."""
    assert report.samples, "resource-soak report has no measured epochs"
    final = report.samples[-1]
    baseline = report.baseline
    problems: list[str] = []
    rss_growth = final.rss_bytes - baseline.rss_bytes
    if rss_growth > limits.rss_growth_bytes:
        problems.append(f"RSS retained {rss_growth} bytes (limit {limits.rss_growth_bytes})")
    if report.rss_slope_bytes_per_epoch > limits.rss_slope_bytes_per_epoch:
        problems.append(
            f"RSS slope is {report.rss_slope_bytes_per_epoch:.0f} bytes/epoch "
            f"(limit {limits.rss_slope_bytes_per_epoch})"
        )
    for label, actual, start, limit in (
        ("descriptors", final.descriptors, baseline.descriptors, limits.descriptor_growth),
        ("threads", final.threads, baseline.threads, limits.thread_growth),
        ("children", final.children, baseline.children, limits.child_growth),
    ):
        growth = actual - start
        if growth > limit:
            problems.append(f"{label} retained {growth} (limit {limit})")
    assert not problems, "; ".join(problems) + "\n" + report.as_json()


def _target(request: pytest.FixtureRequest) -> ResourceSoakTarget:
    """Resolve the optional runner fixture or skip this tranche."""
    try:
        target = request.getfixturevalue("conformance_resource_soak_target")
    except pytest.FixtureLookupError:
        pytest.skip("runner does not expose an isolated conformance resource-soak target")
    if not isinstance(target, ResourceSoakTarget):
        raise TypeError("conformance_resource_soak_target must return ResourceSoakTarget")
    return target


def _run_epochs(
    target: ResourceSoakTarget,
    scenario: str,
    warm_up: Callable[[ResourceSoakConnection], None],
    epoch: Callable[[ResourceSoakConnection], int],
) -> ResourceSoakReport:
    """Run warm-up and five measured epochs on one persistent connection."""
    samples: list[ProcessResourceSnapshot] = []
    operations = 0
    with target.connect() as proxy:
        warm_up(proxy)
        baseline = _settled_sample(target)
        for _ in range(5):
            operations += epoch(proxy)
            assert proxy.echo_int(value=operations) == operations
            samples.append(_settled_sample(target))
    return _build_report(target, scenario, operations, baseline, samples)


@pytest.mark.timeout(50)
class TestResourceSoak:
    """Fast black-box resource-retention checks for an isolated worker."""

    def test_unary_reuse_plateaus(self, request: pytest.FixtureRequest) -> None:
        """Repeated calls on one connection do not retain worker resources."""
        target = _target(request)
        per_epoch = 100 * _scale()

        def warm_up(proxy: ResourceSoakConnection) -> None:
            for value in range(50):
                assert proxy.echo_int(value=value) == value

        def epoch(proxy: ResourceSoakConnection) -> int:
            for value in range(per_epoch):
                assert proxy.echo_int(value=value) == value
                assert math.isclose(proxy.add_floats(a=float(value), b=0.5), value + 0.5)
            return per_epoch * 2

        report = _run_epochs(target, "unary-reuse", warm_up, epoch)
        _assert_report(report, target.limits)

    def test_stream_error_and_cancel_churn_plateaus(self, request: pytest.FixtureRequest) -> None:
        """Stream completion, failure, and cancellation release every turn."""
        target = _target(request)
        per_epoch = 10 * _scale()

        def cycle(proxy: ResourceSoakConnection) -> None:
            assert len(list(proxy.produce_n(count=3))) == 3
            with pytest.raises(RpcError):
                list(proxy.produce_error_mid_stream(emit_before_error=1))
            session = proxy.cancellable_producer()
            next(iter(session))
            session.cancel()

        def warm_up(proxy: ResourceSoakConnection) -> None:
            for _ in range(3):
                cycle(proxy)

        def epoch(proxy: ResourceSoakConnection) -> int:
            for _ in range(per_epoch):
                cycle(proxy)
            return per_epoch * 3

        report = _run_epochs(target, "stream-error-cancel", warm_up, epoch)
        _assert_report(report, target.limits)

    def test_connection_churn_plateaus(self, request: pytest.FixtureRequest) -> None:
        """Repeated connect/call/close cycles do not retain server resources."""
        target = _target(request)
        per_epoch = 20 * _scale()
        for value in range(5):
            with target.connect() as proxy:
                assert proxy.echo_int(value=value) == value
        baseline = _settled_sample(target)
        samples: list[ProcessResourceSnapshot] = []
        operations = 0
        for _ in range(5):
            for value in range(per_epoch):
                with target.connect() as proxy:
                    assert proxy.echo_int(value=value) == value
                operations += 1
            samples.append(_settled_sample(target))
        with target.connect() as proxy:
            assert proxy.echo_int(value=operations) == operations
        report = _build_report(target, "connection-churn", operations, baseline, samples)
        _assert_report(report, target.limits)


__all__ = [
    "ProcessResourceSnapshot",
    "ResourceSoakConnection",
    "ResourceSoakLimits",
    "ResourceSoakReport",
    "ResourceSoakStream",
    "ResourceSoakTarget",
    "TestResourceSoak",
    "sample_process_resources",
]
