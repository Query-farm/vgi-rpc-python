# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Adversarial request-contract checks for persistent raw transports.

Unlike arbitrary garbage or a truncated Arrow stream, these requests remain
fully framed IPC messages.  A server can therefore reject them and continue on
the same connection, which is the recovery contract this module exercises.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from pyarrow import ipc

from vgi_rpc.conformance._adversarial_http import (
    _METADATA_MUTATIONS,
    _ROUTING_KEY_MUTATION,
    _SCHEMA_MUTATIONS,
    _metadata_mutation_body,
    _schema_mutation_body,
)
from vgi_rpc.rpc import RpcError, _dispatch_log_or_error, _drain_stream
from vgi_rpc.utils import IpcValidation, ValidatedReader

if TYPE_CHECKING:
    from vgi_rpc.conformance._protocol import ConformanceService


def _raw_transport(proxy: ConformanceService) -> Any:
    """Return the proxy's byte-stream transport supplied by the raw-only fixture."""
    transport = getattr(proxy, "_transport", None)
    if transport is None or not hasattr(transport, "reader") or not hasattr(transport, "writer"):
        raise AssertionError("conformance_raw_conn supplied a non-raw transport")
    return transport


def _send_invalid_request(proxy: ConformanceService, body: bytes) -> RpcError:
    """Write one framed invalid request and return its typed error response."""
    transport = _raw_transport(proxy)
    writer = transport.writer
    writer.write(body)
    writer.flush()

    reader = ValidatedReader(ipc.open_stream(transport.reader), IpcValidation.NONE)
    try:
        while True:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
            _dispatch_log_or_error(batch, metadata)
    except RpcError as exc:
        _drain_stream(reader)
        return exc
    except StopIteration:
        pass
    raise AssertionError("raw request-contract rejection did not carry a typed RPC error")


def _assert_rejected_and_reusable(proxy: ConformanceService, body: bytes) -> None:
    """Require typed rejection and a successful call on the same connection."""
    error = _send_invalid_request(proxy, body)
    assert error.error_type, "raw rejection must identify its RPC error type"
    assert proxy.add_floats(a=1.25, b=2.5) == pytest.approx(3.75)


class TestAdversarialRawRequestContract:
    """Persistent raw transports enforce the declared request contract."""

    @pytest.mark.parametrize("mutation", _SCHEMA_MUTATIONS)
    def test_parameter_contract(self, conformance_raw_conn: Any, mutation: str) -> None:
        """Schema and row-count drift is rejected without poisoning framing."""
        with conformance_raw_conn() as proxy:
            _assert_rejected_and_reusable(proxy, _schema_mutation_body("add_floats", mutation))

    @pytest.mark.parametrize("mutation", _METADATA_MUTATIONS)
    def test_dispatch_metadata_contract(self, conformance_raw_conn: Any, mutation: str) -> None:
        """Required dispatch metadata is enforced on raw transports too."""
        with conformance_raw_conn() as proxy:
            _assert_rejected_and_reusable(proxy, _metadata_mutation_body("add_floats", mutation))

    def test_an_absent_routing_key_is_refused(self, conformance_raw_conn: Any) -> None:
        """Here the metadata field is the only carrier, so absent is unroutable.

        The HTTP counterpart *accepts* this, because the path segment resolves
        the binding there. Both halves are pinned because the asymmetry is the
        contract -- a port enforcing the strict reading on HTTP fails
        conformance, and one relaxing it here lets a request land on whichever
        protocol the dispatcher reaches first.
        """
        with conformance_raw_conn() as proxy:
            _assert_rejected_and_reusable(proxy, _metadata_mutation_body("add_floats", _ROUTING_KEY_MUTATION))


__all__ = ["TestAdversarialRawRequestContract"]
