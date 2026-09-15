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


class TestRouting:
    """Dispatch resolves on (protocol, method).

    Exercised against ``_resolve`` directly: it is the whole routing decision,
    and driving raw IPC here would reimplement the transport to test a dict
    lookup. End-to-end coverage comes from the conformance suite, which routes
    every call in the ordinary way.
    """

    @staticmethod
    def _resolve(srv: RpcServer, method: str, protocol: bytes | None) -> str:
        """Resolve one call, returning ``ok:<protocol>`` or the error text."""
        import pyarrow as pa

        from vgi_rpc.metadata import PROTOCOL_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY
        from vgi_rpc.rpc._common import _current_request_metadata

        md: dict[bytes, bytes] = {RPC_METHOD_KEY: method.encode(), REQUEST_VERSION_KEY: REQUEST_VERSION}
        if protocol is not None:
            md[PROTOCOL_KEY] = protocol
        token = _current_request_metadata.set(pa.KeyValueMetadata(md))
        try:
            return f"ok:{srv._resolve(method).protocol_name}"
        except Exception as exc:
            return str(exc)
        finally:
            _current_request_metadata.reset(token)

    def test_each_protocol_gets_its_own_method(self) -> None:
        """The property the whole change exists for: `status` means two different things."""
        srv = _server()
        assert self._resolve(srv, "status", b"demo.Alpha.v1") == "ok:demo.Alpha.v1"
        assert self._resolve(srv, "status", b"demo.Beta.v1") == "ok:demo.Beta.v1"

    def test_method_not_on_the_named_protocol_is_unknown(self) -> None:
        """Alpha has `ping`; Beta does not. Addressing Beta must not reach Alpha's."""
        assert "has no method 'ping'" in self._resolve(_server(), "ping", b"demo.Beta.v1")

    def test_absent_routing_key_is_refused(self) -> None:
        """Required even on a single-protocol server.

        An exemption would let an intermediary that rebuilds a request and drops
        the field land silently on whichever protocol happened to be first.
        """
        out = self._resolve(RpcServer(Alpha, AlphaImpl()), "ping", None)
        assert "no 'vgi_rpc.protocol' routing key" in out

    def test_unknown_protocol_is_distinct_from_unknown_method(self) -> None:
        """A client probing for an optional protocol has to tell the two apart."""
        out = self._resolve(_server(), "ping", b"demo.Nope.v1")
        assert "does not host protocol 'demo.Nope.v1'" in out
        assert "has no method" not in out

    def test_describe_needs_no_routing_key(self) -> None:
        """__describe__ is the diagnostic path a mismatched client uses.

        Requiring it to name a protocol first would remove the tool exactly when
        it is needed.
        """
        srv = RpcServer(Alpha, AlphaImpl(), enable_describe=True)
        assert self._resolve(srv, "__describe__", None).startswith("ok:")

    def test_reserved_but_unimplemented_answers_method_not_implemented(self) -> None:
        """Not 'you failed to route' — the caller's routing was never the problem."""
        out = self._resolve(RpcServer(Alpha, AlphaImpl()), "__describe__", None)
        assert "does not implement the reserved method" in out


class TestVersionGate:
    """The gate reads the version of the binding that owns the resolved method.

    A server hosting two independently-versioned protocols has no single
    "server version" to compare against, so gating every call against the
    primary's would reject correct callers of the secondary and produce a
    mismatch message naming the wrong protocol.
    """

    @staticmethod
    def _gate(srv: RpcServer, method: str, protocol: bytes, version: bytes | None) -> str:
        """Resolve and gate one call; return ``"ok"`` or the error text."""
        import pyarrow as pa

        from vgi_rpc.metadata import (
            PROTOCOL_KEY,
            PROTOCOL_VERSION_KEY,
            REQUEST_VERSION,
            REQUEST_VERSION_KEY,
            RPC_METHOD_KEY,
        )
        from vgi_rpc.rpc._common import _current_request_metadata

        md: dict[bytes, bytes] = {
            RPC_METHOD_KEY: method.encode(),
            REQUEST_VERSION_KEY: REQUEST_VERSION,
            PROTOCOL_KEY: protocol,
        }
        if version is not None:
            md[PROTOCOL_VERSION_KEY] = version
        token = _current_request_metadata.set(pa.KeyValueMetadata(md))
        try:
            srv.gate_version(srv._resolve(method))
            return "ok"
        except Exception as exc:
            return str(exc)
        finally:
            _current_request_metadata.reset(token)

    def test_each_binding_gates_on_its_own_version(self) -> None:
        """Alpha is 1.0.0 and Beta is 2.3.0; each caller declares its own."""
        srv = _server()
        assert self._gate(srv, "status", b"demo.Alpha.v1", b"1.0.0") == "ok"
        assert self._gate(srv, "status", b"demo.Beta.v1", b"2.3.0") == "ok"

    def test_mismatch_on_one_protocol_leaves_the_other_callable(self) -> None:
        """The isolation property: a stale Alpha client does not break Beta."""
        srv = _server()
        assert "mismatch" in self._gate(srv, "status", b"demo.Alpha.v1", b"1.4.0")
        assert self._gate(srv, "status", b"demo.Beta.v1", b"2.3.0") == "ok"

    def test_the_other_protocols_version_is_not_accepted(self) -> None:
        """Beta's version against Alpha is a mismatch, not a pass.

        This is the failure a single shared gate would have let through in one
        direction and produced a misleading message for in the other.
        """
        assert "mismatch" in self._gate(_server(), "status", b"demo.Alpha.v1", b"2.3.0")

    def test_patch_still_ignored_per_binding(self) -> None:
        """The comparison rule is unchanged; only what it compares against moved."""
        assert self._gate(_server(), "status", b"demo.Beta.v1", b"2.3.99") == "ok"

    def test_mismatch_message_names_the_protocol(self) -> None:
        """With N bindings, "Server: 1.0.0" alone does not say which server."""
        msg = self._gate(_server(), "status", b"demo.Alpha.v1", b"1.4.0")
        assert "'demo.Alpha.v1'" in msg

    def test_absent_version_against_a_versioned_binding_is_an_error(self) -> None:
        """Stripping the field must not silently disable the gate."""
        assert "did not send" in self._gate(_server(), "status", b"demo.Beta.v1", None)


class TestStreamIsolation:
    """A stream's continuation cannot be replayed against another protocol.

    The protocol is bound into the AAD of both the cursor and call tokens
    rather than compared in application code, so a cross-protocol
    continuation fails the AEAD tag check -- rejected as an invalid token,
    which is what a client presenting a token for the wrong endpoint is.
    """

    @staticmethod
    def _aads(protocol: str) -> tuple[bytes, bytes]:
        from vgi_rpc.http.server._state_token import _compute_aad, _compute_call_aad

        return (
            _compute_aad(None, protocol=protocol),
            _compute_call_aad(None, protocol=protocol),
        )

    def test_cursor_token_does_not_open_under_another_protocol(self) -> None:
        """The cache-hit path's guard: the cursor is always opened first."""
        import pytest

        from vgi_rpc.http.server._state_token import _open_cursor_token, _seal_cursor_token

        key = b"\x11" * 32
        call_id = b"\x22" * 16
        token = _seal_cursor_token(b"state", call_id, key, self._aads("demo.Alpha.v1")[0], 1000)

        assert _open_cursor_token(token, key, self._aads("demo.Alpha.v1")[0]) == (b"state", call_id)
        with pytest.raises(Exception, match="verification failed"):
            _open_cursor_token(token, key, self._aads("demo.Beta.v1")[0])

    def test_call_token_does_not_open_under_another_protocol(self) -> None:
        """The cache-miss path's guard, for a node that never saw ``/init``."""
        import pytest

        from vgi_rpc.http.server._state_token import _open_call_token, _seal_call_token

        key = b"\x33" * 32
        call_id = b"\x44" * 16
        token = _seal_call_token(b"c", "T", b"sch", b"in", call_id, "sid", key, self._aads("demo.Alpha.v1")[1], 1000)

        assert _open_call_token(token, key, self._aads("demo.Alpha.v1")[1])[4] == call_id
        with pytest.raises(Exception, match="verification failed"):
            _open_call_token(token, key, self._aads("demo.Beta.v1")[1])

    def test_server_scoped_tokens_are_not_protocol_tokens(self) -> None:
        """A session token is server-scoped; its AAD must not match any protocol.

        ``SERVER_SCOPE`` leads with a NUL, which the protocol-name grammar
        forbids, so no protocol can ever collide with it.
        """
        from vgi_rpc.http.server._state_token import SERVER_SCOPE

        assert SERVER_SCOPE.startswith("\x00")
        assert self._aads(SERVER_SCOPE)[0] != self._aads("demo.Alpha.v1")[0]

    def test_state_types_are_keyed_per_binding(self) -> None:
        """Two protocols may each declare a stream of the same name.

        Union state tags are positional, so resolving one protocol's stream
        against the other's state info mints a mis-tagged cursor rather than
        failing -- which is why the key is the pair, not the method name.
        """
        srv = _server()
        assert all(isinstance(k, tuple) and len(k) == 2 for k in _state_types(srv))


def _state_types(srv: RpcServer) -> dict[tuple[str, str], object]:
    from vgi_rpc.http.server._state_token import _resolve_state_types

    return dict(_resolve_state_types(srv))
