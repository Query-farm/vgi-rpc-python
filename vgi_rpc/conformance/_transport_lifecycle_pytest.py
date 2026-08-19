# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Opt-in transport-kind and lifecycle conformance contracts.

Both contracts are fixture-gated so existing language runners remain valid.
They intentionally avoid adding a method to the versioned
``ConformanceService`` surface.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from io import BytesIO

import pytest
from pyarrow import ipc

from vgi_rpc.conformance._adversarial_http import _ARROW_CONTENT_TYPE, _valid_request

pytestmark = pytest.mark.timeout(5)


class TestTransportKindContext:
    """A call observes the concrete transport kind selected by its runner."""

    @pytest.mark.timeout(30)
    def test_each_transport_reports_its_kind(self, request: pytest.FixtureRequest) -> None:
        """Run every opt-in probe and require its reported kind to match.

        Runners may spawn one native worker per supported transport, so this
        aggregate test needs more than the module's single-RPC 5s budget while
        retaining a finite deadline below the suite-wide 50s ceiling.
        """
        try:
            probes: Sequence[tuple[str, Callable[[], str]]] = request.getfixturevalue(
                "conformance_transport_kind_probes"
            )
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_transport_kind_probes")

        assert probes, "transport-kind fixture must expose at least one wire probe"
        allowed = {"pipe", "http", "unix", "tcp"}
        for expected, probe in probes:
            assert expected in allowed, f"unknown TransportKind conformance label: {expected!r}"
            assert probe() == expected


class TestServeStartLifecycle:
    """A transient startup-hook failure leaves the HTTP worker retryable."""

    @pytest.mark.timeout(30)
    def test_first_failure_retries_and_keeps_listener_reusable(self, request: pytest.FixtureRequest) -> None:
        """Observe failure, retry on one persistent client, and decode success.

        Native runners may need to cold-start a VM before reaching the two
        bounded HTTP requests, so use the same finite multi-process budget as
        the transport-kind probe.
        """
        try:
            port: int = request.getfixturevalue("conformance_http_serve_start_fail_once_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_serve_start_fail_once_port")

        import httpx2

        body = _valid_request("add_floats")
        url = f"http://127.0.0.1:{port}/add_floats"
        headers = {"Content-Type": _ARROW_CONTENT_TYPE, "Accept-Encoding": "identity"}
        with httpx2.Client(timeout=5.0) as client:
            failed = client.post(url, content=body, headers=headers)
            assert failed.status_code == 500, (
                f"the injected on_serve_start failure was not exposed: expected HTTP 500, got {failed.status_code}"
            )
            recovered = client.post(url, content=body, headers=headers)

        assert recovered.status_code == 200, (
            "listener/process was not reusable after on_serve_start failure: "
            f"follow-up returned HTTP {recovered.status_code}"
        )
        result = ipc.open_stream(BytesIO(recovered.content)).read_next_batch()
        assert result.column("result")[0].as_py() == pytest.approx(3.0)
