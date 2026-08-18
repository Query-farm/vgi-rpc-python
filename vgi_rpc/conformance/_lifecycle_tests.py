# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Shared lifecycle assertions for both conformance front ends.

The standalone runner and the pytest reference suite intentionally exercise
the same service surface.  Keeping the multi-step recovery assertions here
prevents the two front ends from quietly assigning different meanings to
"the connection remained usable" or "cancellation ran exactly once".
"""

from __future__ import annotations

from collections.abc import Callable

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc import AnnotatedBatch, RpcError


def _expect_rpc_error(call: Callable[[], object], *, operation: str) -> None:
    """Require *call* to surface a server-side ``RpcError``."""
    try:
        call()
    except RpcError:
        return
    raise AssertionError(f"{operation} did not raise RpcError")


def _assert_same_proxy_healthy(proxy: ConformanceService) -> None:
    """Exercise unary and streaming dispatch again through the same proxy."""
    assert proxy.echo_int(value=42) == 42
    with proxy.exchange_scale(factor=2.0) as session:
        output = session.exchange(AnnotatedBatch.from_pydict({"value": [5.0]}))
        actual = output.batch.column("value")[0].as_py()
        assert actual is not None and abs(float(actual) - 10.0) < 1e-6
    assert proxy.echo_string(value="recovered") == "recovered"


def assert_unary_error_recovery(proxy: ConformanceService) -> None:
    """Verify that a unary handler error does not poison later calls."""
    _expect_rpc_error(
        lambda: proxy.raise_runtime_error(message="intentional recovery probe"),
        operation="raise_runtime_error",
    )
    _assert_same_proxy_healthy(proxy)


def assert_producer_error_recovery(proxy: ConformanceService) -> None:
    """Verify that a producer-turn error leaves the proxy usable."""

    def _drain_faulting_producer() -> None:
        for _batch in proxy.produce_error_mid_stream(emit_before_error=1):
            pass

    _expect_rpc_error(_drain_faulting_producer, operation="produce_error_mid_stream")
    _assert_same_proxy_healthy(proxy)


def assert_exchange_error_recovery(proxy: ConformanceService) -> None:
    """Verify that an Nth exchange error leaves the proxy usable."""
    session = proxy.exchange_error_on_nth(fail_on=3)
    session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
    session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
    _expect_rpc_error(
        lambda: session.exchange(AnnotatedBatch.from_pydict({"value": [3.0]})),
        operation="third exchange_error_on_nth turn",
    )
    _assert_same_proxy_healthy(proxy)


def assert_cancel_producer_once(proxy: ConformanceService) -> None:
    """Explicit producer cancellation must run its server hook exactly once."""
    proxy.reset_cancel_probe()
    session = proxy.cancellable_producer()
    iterator = iter(session)
    for _ in range(3):
        next(iterator)
    session.cancel()
    produce_calls, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert produce_calls >= 3, f"expected produce_calls>=3, got {produce_calls}"
    assert exchange_calls == 0, f"expected exchange_calls=0, got {exchange_calls}"
    assert on_cancel_calls == 1, f"expected on_cancel_calls=1, got {on_cancel_calls}"
    _assert_same_proxy_healthy(proxy)


def assert_cancel_exchange_once(proxy: ConformanceService) -> None:
    """Explicit exchange cancellation must run once after exactly two turns."""
    proxy.reset_cancel_probe()
    session = proxy.cancellable_exchange()
    session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
    session.exchange(AnnotatedBatch.from_pydict({"value": [2.0]}))
    session.cancel()
    produce_calls, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert produce_calls == 0, f"expected produce_calls=0, got {produce_calls}"
    assert exchange_calls == 2, f"expected exchange_calls=2, got {exchange_calls}"
    assert on_cancel_calls == 1, f"expected on_cancel_calls=1, got {on_cancel_calls}"


def assert_cancel_before_exchange_once(proxy: ConformanceService) -> None:
    """Verify that cancelling an unused exchange runs exactly one hook."""
    proxy.reset_cancel_probe()
    session = proxy.cancellable_exchange()
    session.cancel()
    produce_calls, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert produce_calls == 0, f"expected produce_calls=0, got {produce_calls}"
    assert exchange_calls == 0, f"expected exchange_calls=0, got {exchange_calls}"
    assert on_cancel_calls == 1, f"expected on_cancel_calls=1, got {on_cancel_calls}"
    _assert_same_proxy_healthy(proxy)


def assert_cancel_idempotent_once(proxy: ConformanceService) -> None:
    """Repeated explicit cancellation of one session must not duplicate cleanup."""
    proxy.reset_cancel_probe()
    session = proxy.cancellable_exchange()
    session.cancel()
    session.cancel()
    produce_calls, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert produce_calls == 0, f"expected produce_calls=0, got {produce_calls}"
    assert exchange_calls == 0, f"expected exchange_calls=0, got {exchange_calls}"
    assert on_cancel_calls == 1, f"expected on_cancel_calls=1, got {on_cancel_calls}"
    _assert_same_proxy_healthy(proxy)


def assert_cancel_close_ordering_safe(proxy: ConformanceService) -> None:
    """Both close/cancel orderings must be idempotent and leave a clean transport."""
    proxy.reset_cancel_probe()
    close_then_cancel = proxy.cancellable_exchange()
    close_then_cancel.close()
    close_then_cancel.cancel()

    cancel_then_close = proxy.cancellable_exchange()
    cancel_then_close.cancel()
    cancel_then_close.close()

    produce_calls, exchange_calls, on_cancel_calls = proxy.cancel_probe_counters()
    assert produce_calls == 0, f"expected produce_calls=0, got {produce_calls}"
    assert exchange_calls == 0, f"expected exchange_calls=0, got {exchange_calls}"
    # Stateless HTTP may close an unused stream without a live cancel token;
    # persistent transports observe that close as cancellation.
    assert on_cancel_calls in (1, 2), f"expected on_cancel_calls in (1, 2), got {on_cancel_calls}"
    _assert_same_proxy_healthy(proxy)
