# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""One server hosting several independently-versioned protocols."""

from __future__ import annotations

import logging
import re
from typing import ClassVar, Protocol

import pytest

from vgi_rpc.rpc import RpcServer


class Alpha(Protocol):
    """Primary protocol."""

    protocol_name: ClassVar[str] = "demo.Alpha.v1"
    protocol_version: ClassVar[str] = "1.0.0"

    def ping(self) -> str:
        """Return a greeting."""
        ...

    def status(self) -> str:
        """Deliberately shares a name with Beta.status."""
        ...


class Beta(Protocol):
    """Secondary protocol, independently versioned."""

    protocol_name: ClassVar[str] = "demo.Beta.v1"
    protocol_version: ClassVar[str] = "2.3.0"

    def status(self) -> str:
        """Return a status — deliberately shares a name with Alpha.status."""
        ...


class Unnamed(Protocol):
    """Declares no protocol_name; falls back to the class name."""

    def solo(self) -> str:
        """Do nothing interesting."""
        ...


class AlphaImpl:
    """Implements Alpha."""

    def ping(self) -> str:
        """Return a greeting."""
        return "a"

    def status(self) -> str:
        """Return Alpha's status."""
        return "alpha"


class BetaImpl:
    """Implements Beta."""

    def status(self) -> str:
        """Return Beta's status."""
        return "beta"


class BothImpl:
    """One object implementing both protocols — a supported shape."""

    def ping(self) -> str:
        """Return a greeting."""
        return "a"

    def status(self) -> str:
        """Return a status."""
        return "both"


class UnnamedImpl:
    """Implements Unnamed."""

    def solo(self) -> str:
        """Return a value."""
        return "s"


def _server() -> RpcServer:
    return RpcServer(Alpha, AlphaImpl(), extra_protocols=((Beta, BetaImpl()),))


class TestBindings:
    """Each protocol gets its own binding."""

    def test_both_protocols_bound(self) -> None:
        """Routing keys are the declared names, not the class names."""
        assert sorted(_server()._bindings) == ["demo.Alpha.v1", "demo.Beta.v1"]

    def test_versions_are_independent(self) -> None:
        """The point of the exercise: one protocol's version says nothing about another's."""
        b = _server()._bindings
        assert b["demo.Alpha.v1"].version == "1.0.0"
        assert b["demo.Beta.v1"].version == "2.3.0"

    def test_hashes_are_independent(self) -> None:
        """A change to one protocol must not rotate another's fingerprint."""
        b = _server()._bindings
        assert b["demo.Alpha.v1"].protocol_hash != b["demo.Beta.v1"].protocol_hash
        assert len(b["demo.Alpha.v1"].protocol_hash) == 64

    def test_method_tables_do_not_leak(self) -> None:
        """Beta has no `ping`, even though Alpha does."""
        b = _server()._bindings
        assert "ping" in b["demo.Alpha.v1"].methods
        assert "ping" not in b["demo.Beta.v1"].methods

    def test_colliding_method_names_are_allowed(self) -> None:
        """Namespacing, not a collision policy — both protocols keep their `status`."""
        b = _server()._bindings
        assert "status" in b["demo.Alpha.v1"].methods
        assert "status" in b["demo.Beta.v1"].methods

    def test_primary_is_the_first_argument(self) -> None:
        """Not a list position anyone has to remember."""
        srv = _server()
        assert srv.protocol_name == "demo.Alpha.v1"
        assert srv.protocol_hash == srv._bindings["demo.Alpha.v1"].protocol_hash

    def test_undeclared_name_falls_back_to_the_class(self) -> None:
        """Every existing Protocol keeps working without declaring anything."""
        assert RpcServer(Unnamed, UnnamedImpl()).protocol_name == "Unnamed"


class TestConstructionGuards:
    """What the server refuses, and what it merely warns about."""

    def test_duplicate_protocol_name_is_rejected(self) -> None:
        """The name is the routing key, so a duplicate is unresolvable."""

        class Clash(Protocol):
            protocol_name: ClassVar[str] = "demo.Alpha.v1"

            def other(self) -> str:
                """Do nothing."""
                ...

        class ClashImpl:
            def other(self) -> str:
                return "x"

        with pytest.raises(ValueError, match=re.escape("same name 'demo.Alpha.v1'")):
            RpcServer(Alpha, AlphaImpl(), extra_protocols=((Clash, ClashImpl()),))

    def test_duplicate_message_names_both_protocols(self) -> None:
        """An operator has to be able to tell which two collided."""

        class Clash(Protocol):
            protocol_name: ClassVar[str] = "demo.Alpha.v1"

            def other(self) -> str:
                """Do nothing."""
                ...

        class ClashImpl:
            def other(self) -> str:
                return "x"

        with pytest.raises(ValueError, match="Alpha and Clash"):
            RpcServer(Alpha, AlphaImpl(), extra_protocols=((Clash, ClashImpl()),))

    def test_method_collision_warns_but_is_allowed(self, caplog: pytest.LogCaptureFixture) -> None:
        """Legal and intended — but an operator should hear about it at startup.

        Anything keyed on the bare method name (dashboards, alerts, proxy
        policy) silently merges the two, and that is far cheaper to learn now
        than from a confusing graph months later.
        """
        with caplog.at_level(logging.WARNING, logger="vgi_rpc.rpc"):
            _server()
        assert any("'status' is defined by both" in r.getMessage() for r in caplog.records)

    def test_no_warning_for_a_single_protocol(self, caplog: pytest.LogCaptureFixture) -> None:
        """The check must not fire on the overwhelmingly common case."""
        with caplog.at_level(logging.WARNING, logger="vgi_rpc.rpc"):
            RpcServer(Alpha, AlphaImpl())
        assert not [r for r in caplog.records if "defined by both" in str(r.msg)]


class TestImplementationRouting:
    """`ctx` and implementations are per binding, not per server."""

    def test_implementation_for_returns_the_owning_object(self) -> None:
        """Stream rehydration depends on this; the wrong object corrupts state silently."""
        srv = _server()
        alpha_status = srv._bindings["demo.Alpha.v1"].methods["status"]
        beta_status = srv._bindings["demo.Beta.v1"].methods["status"]
        assert isinstance(srv.implementation_for(alpha_status), AlphaImpl)
        assert isinstance(srv.implementation_for(beta_status), BetaImpl)

    def test_one_object_may_implement_several_protocols(self) -> None:
        """A supported shape — a worker serving both is likely."""
        shared = BothImpl()
        srv = RpcServer(Alpha, shared, extra_protocols=((Beta, shared),))
        assert srv.implementation_for(srv._bindings["demo.Beta.v1"].methods["status"]) is shared

    def test_info_carries_its_own_protocol(self) -> None:
        """What makes telemetry and access logs attribute correctly."""
        b = _server()._bindings
        assert b["demo.Alpha.v1"].methods["status"].protocol_name == "demo.Alpha.v1"
        assert b["demo.Beta.v1"].methods["status"].protocol_name == "demo.Beta.v1"


class TestSingleProtocolUnchanged:
    """The existing shape must not move."""

    def test_two_positional_args_still_work(self) -> None:
        """~397 call sites use this form."""
        srv = RpcServer(Alpha, AlphaImpl())
        assert srv.protocol_name == "demo.Alpha.v1"
        assert len(srv._bindings) == 1

    def test_methods_property_is_the_primarys(self) -> None:
        """`server.methods` keeps meaning what it meant."""
        srv = _server()
        assert set(srv.methods) == set(srv._bindings["demo.Alpha.v1"].methods)
