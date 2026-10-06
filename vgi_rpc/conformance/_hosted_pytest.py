# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The hosted-protocols conformance group, for workers that are not vgi-rpc conformance workers.

A VGI SDK's fixture worker hosts its own primary protocol (``vgi.v2``) rather
than ``ConformanceService``, so the port-facing groups -- which compare the
secondary against the conformance primary -- do not apply to it.  What does
apply is everything the secondary says about itself and about the error model,
plus the one assertion only the worker's author can state: *which* protocols
it hosts, in which order.

This module is collected by :mod:`vgi_rpc.conformance.hosted_protocols`
(``vgi-rpc-test-hosted``), which passes its configuration through environment
variables, so a pytest run here is one worker on one transport:

=========================== ====================================================
``VGI_HOSTED_TRANSPORT``    ``pipe``, ``unix`` or ``http``
``VGI_HOSTED_TARGET``       command line, socket path, or base URL
``VGI_HOSTED_PREFIX``       HTTP route prefix (default ``""``)
``VGI_HOSTED_EXPECT``       comma-separated application protocols, in order
``VGI_HOSTED_IDENTITY``     ``1``: the worker hosts ``vgi_rpc.Identity.v1`` with
                            the ``IDENTITY_CONFORMANCE_FIXTURE.md`` policy
=========================== ====================================================
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urlparse

import pytest

from vgi_rpc._command import split_command
from vgi_rpc.conformance._secondary_pytest import (  # noqa: F401 -- collected by pytest
    ProtocolTarget,
    TestErrorModelOnTheWire,
    TestErrorModelRoundTrip,
    TestSecondaryDescribes,
    TestSecondaryRouting,
    TestTracebackPolicy,
    application_protocols,
    list_protocols,
    protocol_target,
)
from vgi_rpc.conformance.secondary import SECONDARY_PROTOCOL_NAME

TRANSPORT_ENV = "VGI_HOSTED_TRANSPORT"
TARGET_ENV = "VGI_HOSTED_TARGET"
PREFIX_ENV = "VGI_HOSTED_PREFIX"
EXPECT_ENV = "VGI_HOSTED_EXPECT"
IDENTITY_ENV = "VGI_HOSTED_IDENTITY"

_TRANSPORT = os.environ.get(TRANSPORT_ENV, "")
_TARGET = os.environ.get(TARGET_ENV, "")
_PREFIX = os.environ.get(PREFIX_ENV, "")
_EXPECT = [p.strip() for p in os.environ.get(EXPECT_ENV, "").split(",") if p.strip()]
_IDENTITY = os.environ.get(IDENTITY_ENV, "") == "1"

if _TRANSPORT not in ("pipe", "unix", "http") or not _TARGET:
    pytest.skip(
        f"the hosted-protocols group is driven by vgi-rpc-test-hosted, which sets {TRANSPORT_ENV} and "
        f"{TARGET_ENV}; run it through that command rather than collecting this module directly",
        allow_module_level=True,
    )

if _IDENTITY:
    # Identity is hosted over HTTP only; the CLI refuses --identity elsewhere.
    # TestIdentityAbsentByDefault and TestIdentityNarrowing are deliberately not
    # imported: the first asserts against a worker with no hooks, the second
    # needs a second, narrowed worker -- neither is this one.
    from vgi_rpc.conformance._identity_pytest import (  # noqa: F401 -- collected by pytest
        TestErrorKindsReachTheWire,
        TestGrantFreshness,
        TestGrantIssuance,
        TestIdentityWireShape,
        TestIntrospectionAuthorization,
        TestIntrospectionHappyPath,
        TestIntrospectionIsNotThrottled,
        TestRejectionsAreUniform,
        TestTheCredentialSizeCap,
        TestTheJwsTrap,
        TestUnavailableCarriesARetryHint,
        TestUnavailableIsTransient,
    )


def _loopback_port() -> int:
    """Return the HTTP target's port, requiring a loopback host.

    The raw-wire and identity groups address ``127.0.0.1:<port>`` directly.

    Returns:
        The port.

    Raises:
        pytest.skip.Exception: The target is not HTTP or not loopback.

    """
    if _TRANSPORT != "http":
        pytest.skip("raw-wire HTTP checks need an HTTP target")
    parsed = urlparse(_TARGET)
    if parsed.hostname not in ("127.0.0.1", "localhost") or parsed.port is None:
        pytest.skip(f"raw-wire HTTP checks need a loopback URL with an explicit port, got {_TARGET!r}")
    return int(parsed.port)


@pytest.fixture(scope="session", params=[_TRANSPORT])
def conformance_conn(request: pytest.FixtureRequest) -> str:
    """Return the transport axis :func:`protocol_target` reads: one worker, one transport."""
    return str(request.param)


@pytest.fixture(scope="session")
def conformance_http_port() -> int:
    """Loopback port of the HTTP worker under test, for the raw-wire group."""
    return _loopback_port()


@pytest.fixture(scope="session")
def conformance_http_identity_port() -> int:
    """Return the same worker's port, when it opts into Identity."""
    if not _IDENTITY:
        pytest.skip(f"{IDENTITY_ENV} is not set")
    return _loopback_port()


@pytest.fixture(scope="session")
def conformance_protocol_connector() -> Iterator[Callable[..., contextlib.AbstractContextManager[Any]]]:
    """Bind a proxy to any protocol on the worker under test.

    One subprocess for the whole run on ``pipe``, so the worker is exercised as
    a long-lived process, the way a host uses it.
    """
    from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

    subprocess_transport: SubprocessTransport | None = None
    if _TRANSPORT == "pipe":
        subprocess_transport = SubprocessTransport(split_command(_TARGET))

    def connect(transport: str, protocol: type, on_log: Any = None) -> contextlib.AbstractContextManager[Any]:
        del transport
        if subprocess_transport is not None:

            @contextlib.contextmanager
            def _pipe() -> Iterator[Any]:
                yield _RpcProxy(protocol, subprocess_transport, on_log)

            return _pipe()
        if _TRANSPORT == "unix":
            from vgi_rpc.rpc import unix_connect

            return unix_connect(protocol, _TARGET, on_log=on_log)
        from vgi_rpc.http import http_connect

        return http_connect(protocol, _TARGET, prefix=_PREFIX, on_log=on_log)

    try:
        yield connect
    finally:
        if subprocess_transport is not None:
            subprocess_transport.close()


class TestHostedProtocolList:
    """The worker hosts exactly the protocols its author declared, in order."""

    def test_application_protocols_are_listed_in_order(self, protocol_target: ProtocolTarget) -> None:  # noqa: F811
        """Registration order, primary first (WIRE_PROTOCOL.md §3.1)."""
        assert _EXPECT, f"{EXPECT_ENV} is empty; pass --expect"
        hosted = application_protocols(list_protocols(protocol_target))
        assert hosted == _EXPECT, (
            f"the worker lists application protocols {hosted}; expected {_EXPECT}. Order is contract: a "
            f"client's 'describe this server' takes the first non-reserved protocol."
        )

    def test_the_secondary_is_among_them(self) -> None:
        """Every SDK fixture worker hosts the shared fixture protocol."""
        assert SECONDARY_PROTOCOL_NAME in _EXPECT, (
            f"--expect must include {SECONDARY_PROTOCOL_NAME}: every fixture worker hosts it "
            f"(MULTI_PROTOCOL_HOSTING.md, Phase 3)"
        )

    def test_identity_is_listed_when_opted_in(self, protocol_target: ProtocolTarget) -> None:  # noqa: F811
        """Over HTTP, a worker that opts into Identity lists it."""
        if not _IDENTITY:
            pytest.skip("the worker was not declared to host vgi_rpc.Identity.v1 (--identity)")
        hosted = [p.protocol for p in list_protocols(protocol_target).protocols]
        assert "vgi_rpc.Identity.v1" in hosted, f"--identity was given but the worker hosts {hosted}"
