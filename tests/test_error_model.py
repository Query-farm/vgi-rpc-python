# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The error model (WIRE_PROTOCOL.md §8) in the reference implementation.

The cross-language half -- what reaches the wire and what each client reports
-- is in ``vgi_rpc/conformance/_secondary_pytest.py``.  This pins the pieces
that are Python API: the catalog's JSON forms, the cap, the rules, the
classification, the traceback setting and the identity translation.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from vgi_rpc.conformance.secondary import SECONDARY_PROTOCOL_HASH, Secondary
from vgi_rpc.errors import (
    MAX_ERROR_DETAILS_BYTES,
    AuthUnavailableError,
    BadRequest,
    Code,
    ErrorInfo,
    FieldViolation,
    Help,
    HelpLink,
    LocalizedMessage,
    PreconditionFailure,
    PreconditionViolation,
    QuotaFailure,
    QuotaViolation,
    ResourceInfo,
    RetryInfo,
    StatusError,
    decode_error_details,
    encode_error_details,
    is_retryable,
    parse_error_detail,
)
from vgi_rpc.log import Message
from vgi_rpc.metadata import ERROR_CODE_KEY, ERROR_DETAILS_KEY, ERROR_KIND_KEY
from vgi_rpc.rpc import AuthContext, CallContext, RpcError, RpcServer, rpc_methods
from vgi_rpc.rpc._common import (
    MethodNotImplementedError,
    ProtocolNotSpecifiedError,
    ProtocolNotSupportedError,
    ProtocolVersionError,
    ServerDrainingError,
    SessionLostError,
)
from vgi_rpc.rpc._protocol_hash import compute_protocol_hash
from vgi_rpc.rpc._token_identity import (
    GrantRefusedError,
    IdentityImpl,
    IdentityUnavailableError,
    IntrospectionRefusedError,
    IssuedGrant,
    StaleAuthError,
    TokenIdentity,
    TokenUnresolvedError,
)

# ---------------------------------------------------------------------------
# Codes
# ---------------------------------------------------------------------------


def test_the_code_set_is_grpcs_sixteen() -> None:
    """Closed, sixteen members, value == name."""
    assert len(Code) == 16
    assert "OK" not in Code.__members__
    assert all(member.value == name for name, member in Code.__members__.items())


@pytest.mark.parametrize("raw", [None, "", "OK", "unavailable", 14, "NOT_A_CODE"])
def test_an_unrecognised_code_reads_as_unknown(raw: object) -> None:
    """The set is closed, so anything outside it is UNKNOWN rather than an error."""
    assert Code.parse(raw) is Code.UNKNOWN


@pytest.mark.parametrize(
    ("exc", "kind", "code"),
    [
        (MethodNotImplementedError("x"), "method_not_implemented", Code.UNIMPLEMENTED),
        (ProtocolNotSupportedError("x"), "protocol_not_supported", Code.UNIMPLEMENTED),
        (ProtocolNotSpecifiedError("x"), "protocol_not_specified", Code.INVALID_ARGUMENT),
        (ProtocolVersionError("x"), "protocol_version_mismatch", Code.FAILED_PRECONDITION),
        (SessionLostError("x"), "session_lost", Code.ABORTED),
        (ServerDrainingError("x"), "server_draining", Code.UNAVAILABLE),
        (IdentityUnavailableError("x"), "identity_unavailable", Code.UNAVAILABLE),
        (StaleAuthError("x"), "stale_auth", Code.UNAUTHENTICATED),
        (IntrospectionRefusedError("x"), "introspection_refused", Code.PERMISSION_DENIED),
        (GrantRefusedError("x"), "grant_refused", Code.PERMISSION_DENIED),
        (TokenUnresolvedError("x"), "token_unresolved", Code.NOT_FOUND),
    ],
)
def test_every_kind_declares_its_code(exc: BaseException, kind: str, code: Code) -> None:
    """The table in WIRE_PROTOCOL.md §8, one row per kind."""
    md = Message.from_exception(exc).add_to_metadata()
    assert md[ERROR_KIND_KEY.decode()] == kind
    assert md[ERROR_CODE_KEY.decode()] == code.value


def test_an_unclassified_error_is_unknown() -> None:
    """No kind, no declared code: UNKNOWN -- and still present."""
    md = Message.from_exception(ValueError("boom")).add_to_metadata()
    assert md[ERROR_CODE_KEY.decode()] == "UNKNOWN"
    assert ERROR_KIND_KEY.decode() not in md
    assert ERROR_DETAILS_KEY.decode() not in md


def test_non_exception_logs_never_carry_error_keys() -> None:
    """A WARN whose extras happen to say error_code is a log, not a classification."""
    md = Message.warn("hello", error_code="UNAVAILABLE", error_details=[]).add_to_metadata()
    assert ERROR_CODE_KEY.decode() not in md
    assert ERROR_DETAILS_KEY.decode() not in md


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------

_SAMPLES: list[tuple[Any, dict[str, Any]]] = [
    (ErrorInfo(metadata={"k": "v"}), {"@type": "vgi_rpc.ErrorInfo", "metadata": {"k": "v"}}),
    (RetryInfo(retry_delay_seconds=7), {"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": 7}),
    (RetryInfo(retry_delay_seconds=2.5), {"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": 2.5}),
    (
        BadRequest(field_violations=(FieldViolation(field="code", description="bad"),)),
        {"@type": "vgi_rpc.BadRequest", "field_violations": [{"field": "code", "description": "bad"}]},
    ),
    (
        PreconditionFailure(violations=(PreconditionViolation(type="t", subject="s", description="d"),)),
        {"@type": "vgi_rpc.PreconditionFailure", "violations": [{"type": "t", "subject": "s", "description": "d"}]},
    ),
    (
        QuotaFailure(violations=(QuotaViolation(subject="s", description="d"),)),
        {"@type": "vgi_rpc.QuotaFailure", "violations": [{"subject": "s", "description": "d"}]},
    ),
    (
        ResourceInfo(resource_type="report", resource_name="r1", owner="o", description="d"),
        {
            "@type": "vgi_rpc.ResourceInfo",
            "resource_type": "report",
            "resource_name": "r1",
            "owner": "o",
            "description": "d",
        },
    ),
    (
        Help(links=(HelpLink(description="docs", url="https://example.com"),)),
        {"@type": "vgi_rpc.Help", "links": [{"description": "docs", "url": "https://example.com"}]},
    ),
    (
        LocalizedMessage(locale="en-US", message="Try later"),
        {"@type": "vgi_rpc.LocalizedMessage", "locale": "en-US", "message": "Try later"},
    ),
]


@pytest.mark.parametrize(("detail", "wire"), _SAMPLES, ids=lambda v: getattr(v, "TYPE", ""))
def test_each_catalog_type_round_trips(detail: Any, wire: dict[str, Any]) -> None:
    """``to_json`` is the documented form, and ``from_json`` inverts it."""
    assert detail.to_json() == wire
    assert parse_error_detail(wire) == detail


@pytest.mark.parametrize(
    "obj",
    [
        {"@type": "conformance.Secondary.v1.Probe"},
        {"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": "soon"},
        {"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": -1},
        {"@type": "vgi_rpc.ErrorInfo", "metadata": {"k": 1}},
        {"no": "type"},
        "not an object",
    ],
)
def test_unknown_or_malformed_details_are_ignored(obj: object) -> None:
    """Typed access skips them; it never raises."""
    assert parse_error_detail(obj) is None


def test_encode_drops_the_whole_array_over_the_cap() -> None:
    """At the cap it fits; one byte over and nothing is sent -- not a prefix."""
    small = RetryInfo(retry_delay_seconds=1)
    overhead = len(encode_error_details([small, ErrorInfo(metadata={"p": ""})]) or "")
    exact = ErrorInfo(metadata={"p": "x" * (MAX_ERROR_DETAILS_BYTES - overhead)})
    fits = encode_error_details([small, exact])
    assert fits is not None and len(fits.encode()) == MAX_ERROR_DETAILS_BYTES
    over = ErrorInfo(metadata={"p": "x" * (MAX_ERROR_DETAILS_BYTES - overhead + 1)})
    assert encode_error_details([small, over]) is None


def test_the_cap_is_measured_in_utf8_bytes() -> None:
    """Multibyte text counts by bytes, the unit the metadata value is."""
    text = "é" * (MAX_ERROR_DETAILS_BYTES // 2)  # under the cap in characters, over it in bytes
    assert encode_error_details([LocalizedMessage(locale="fr", message=text)]) is None


@pytest.mark.parametrize(
    "details",
    [
        [RetryInfo(retry_delay_seconds=1), RetryInfo(retry_delay_seconds=2)],
        [{"@type": "vgi_rpc.Made.Up"}],
        [{"@type": "Unqualified"}],
        [{"note": "no type"}],
    ],
)
def test_rule_violations_are_dropped_at_emission(details: list[Any]) -> None:
    """Repeated type, a fake catalog name, an unqualified name, no type: nothing is sent."""
    assert encode_error_details(details) is None


def test_status_error_validates_eagerly() -> None:
    """A rule violation fails in the code that made it, not silently on the wire."""
    with pytest.raises(ValueError, match="more than once"):
        StatusError("x", code=Code.INTERNAL, details=[RetryInfo(retry_delay_seconds=1)] * 2)
    with pytest.raises(ValueError, match="canonical"):
        StatusError("x", code="OK")


def test_decode_is_tolerant() -> None:
    """Anything that is not a JSON array decodes as no details."""
    assert decode_error_details(None) == []
    assert decode_error_details("{") == []
    assert decode_error_details('{"@type": "vgi_rpc.RetryInfo"}') == []
    assert decode_error_details('[1, {"@type": "x.Y"}]') == [{"@type": "x.Y"}]


@pytest.mark.parametrize(
    ("code", "details", "expected"),
    [
        ("UNAVAILABLE", [], True),
        ("RESOURCE_EXHAUSTED", [RetryInfo(retry_delay_seconds=1).to_json()], True),
        ("RESOURCE_EXHAUSTED", [], False),
        ("ABORTED", [RetryInfo(retry_delay_seconds=1).to_json()], False),
        ("UNKNOWN", [], False),
        ("", [], False),
    ],
)
def test_retryability(code: str, details: list[dict[str, Any]], expected: bool) -> None:
    """The rule, and the same answer through ``RpcError``."""
    assert is_retryable(code, details) is expected
    assert RpcError("E", "m", "", error_code=code, error_details=details).is_retryable() is expected


# ---------------------------------------------------------------------------
# Message.from_exception
# ---------------------------------------------------------------------------


def test_details_ride_top_level_and_in_log_extra() -> None:
    """The same array in both places, compact JSON at the top level."""
    exc = StatusError("x", code=Code.UNAVAILABLE, kind="down", details=[RetryInfo(retry_delay_seconds=3)])
    md = Message.from_exception(exc).add_to_metadata()
    top = json.loads(md[ERROR_DETAILS_KEY.decode()])
    extra = json.loads(md["vgi_rpc.log_extra"])
    assert top == extra["error_details"] == [{"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": 3}]
    assert extra["error_code"] == "UNAVAILABLE" and extra["error_kind"] == "down"


def test_oversized_details_are_dropped_from_both() -> None:
    """Dropped whole: no top-level key and no mirror; code and kind survive."""
    exc = StatusError("x", code=Code.RESOURCE_EXHAUSTED, kind="big", details=[ErrorInfo(metadata={"p": "x" * 5000})])
    md = Message.from_exception(exc).add_to_metadata()
    assert ERROR_DETAILS_KEY.decode() not in md
    assert "error_details" not in json.loads(md["vgi_rpc.log_extra"])
    assert md[ERROR_CODE_KEY.decode()] == "RESOURCE_EXHAUSTED"


def test_omitting_the_traceback_omits_all_of_it() -> None:
    """Traceback, frames, cause and context go; type, message and model stay."""

    def raise_chained() -> None:
        try:
            raise KeyError("inner")
        except KeyError as inner:
            raise ValueError("outer") from inner

    try:
        raise_chained()
    except ValueError as exc:
        with_tb = Message.from_exception(exc).extra or {}
        without = Message.from_exception(exc, include_traceback=False).extra or {}
    assert {"traceback", "frames", "cause"} <= set(with_tb)
    assert not {"traceback", "frames", "cause", "context"} & set(without)
    assert without["exception_type"] == "ValueError" and without["error_code"] == "UNKNOWN"


def test_a_broken_error_details_attribute_does_not_break_reporting() -> None:
    """The error still reaches the client; only the details are lost."""

    class Broken(Exception):
        @property
        def error_details(self) -> list[Any]:
            raise RuntimeError("nope")

    md = Message.from_exception(Broken("x")).add_to_metadata()
    assert md[ERROR_CODE_KEY.decode()] == "UNKNOWN"
    assert ERROR_DETAILS_KEY.decode() not in md


# ---------------------------------------------------------------------------
# Retry hints on the transient kinds
# ---------------------------------------------------------------------------


def test_identity_unavailable_carries_retry_info() -> None:
    """Required on this kind (WIRE_PROTOCOL.md §16)."""
    md = Message.from_exception(IdentityUnavailableError("down", retry_after=9)).add_to_metadata()
    assert json.loads(md[ERROR_DETAILS_KEY.decode()]) == [{"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": 9}]


def test_auth_unavailable_carries_retry_info() -> None:
    """The transport-auth error carries its hint too, in case it reaches the wire by another path."""
    md = Message.from_exception(AuthUnavailableError("down", retry_after=4)).add_to_metadata()
    assert md[ERROR_CODE_KEY.decode()] == "UNAVAILABLE"
    assert json.loads(md[ERROR_DETAILS_KEY.decode()]) == [{"@type": "vgi_rpc.RetryInfo", "retry_delay_seconds": 4}]


def test_auth_unavailable_is_reexported_from_http() -> None:
    """Moved to the core so IdentityImpl can catch it; the old import still works."""
    from vgi_rpc.http import AuthUnavailableError as FromHttp

    assert FromHttp is AuthUnavailableError


def test_version_mismatch_names_the_protocol_in_a_precondition() -> None:
    """With several bindings, the detail says which protocol's gate refused."""
    exc = ProtocolVersionError("x", protocol="P", client_version="1.0.0", server_version="2.0.0")
    md = Message.from_exception(exc).add_to_metadata()
    (detail,) = json.loads(md[ERROR_DETAILS_KEY.decode()])
    assert detail["@type"] == "vgi_rpc.PreconditionFailure"
    assert detail["violations"][0]["type"] == "protocol_version"
    assert detail["violations"][0]["subject"] == "P"


# ---------------------------------------------------------------------------
# Identity translation
# ---------------------------------------------------------------------------


def _ctx(principal: str, *, auth_time: float | None = None) -> CallContext:
    claims: dict[str, object] = {} if auth_time is None else {"auth_time": auth_time}
    auth = AuthContext(authenticated=True, principal=principal, domain="test", claims=claims)
    return CallContext(auth=auth, implementation=None, transport_metadata={}, emit_client_log=lambda *a, **k: None)


def test_resolve_token_translates_auth_unavailable() -> None:
    """The hook's transport-auth error becomes identity_unavailable with its own hint."""

    def resolver(token: str) -> TokenIdentity | None:
        raise AuthUnavailableError("sidecar down", retry_after=7)

    impl = IdentityImpl(resolve_token=resolver, introspect_principals=["proxy"])
    with pytest.raises(IdentityUnavailableError) as excinfo:
        impl.introspect_token("opaque", _ctx("proxy"))
    assert excinfo.value.retry_after == 7
    assert isinstance(excinfo.value.__cause__, AuthUnavailableError)


def test_mint_grant_translates_auth_unavailable() -> None:
    """Both hooks, not one."""
    import time

    def minter(principal: str, purpose: str, scopes: list[str], ttl_seconds: int) -> IssuedGrant:
        raise AuthUnavailableError("store down", retry_after=7)

    impl = IdentityImpl(mint_grant=minter)
    with pytest.raises(IdentityUnavailableError) as excinfo:
        impl.issue_grant("p", [], 60, _ctx("alice", auth_time=time.time()))
    assert excinfo.value.retry_after == 7


# ---------------------------------------------------------------------------
# Server and client
# ---------------------------------------------------------------------------


def test_tracebacks_are_included_by_default_and_the_setting_omits_them() -> None:
    """Default on; ``include_tracebacks=False`` is the per-server off switch."""
    assert RpcServer(Secondary, _SecondaryImpl()).include_tracebacks is True
    assert RpcServer(Secondary, _SecondaryImpl(), include_tracebacks=False).include_tracebacks is False


@pytest.mark.parametrize("transport", ["pipe", "http"])
def test_the_omit_setting_reaches_the_wire(transport: str) -> None:
    """Reference-local: ports cannot be reconfigured remotely, so this lives here.

    With the setting off, the client sees no traceback on either transport,
    and the error model is untouched.
    """
    from vgi_rpc.conformance.secondary import SecondaryImpl
    from vgi_rpc.rpc import _RpcProxy, make_pipe_pair

    server = RpcServer(Secondary, SecondaryImpl(), include_tracebacks=False)
    if transport == "http":
        from vgi_rpc.http import http_connect, make_sync_client

        client = make_sync_client(server)
        with http_connect(Secondary, client=client) as proxy, pytest.raises(RpcError) as excinfo:
            proxy.fail(code="INTERNAL", kind="", retry_delay_seconds=0.0)
    else:
        import threading

        client_transport, server_transport = make_pipe_pair()
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            with pytest.raises(RpcError) as excinfo:
                _RpcProxy(Secondary, client_transport, None).fail(code="INTERNAL", kind="", retry_delay_seconds=0.0)
        finally:
            client_transport.close()
            thread.join(timeout=5)
    assert excinfo.value.remote_traceback == ""
    assert excinfo.value.error_code == "INTERNAL"


class _SecondaryImpl:
    def echo_string(self, value: str) -> str:
        return value

    def fail(self, code: str, kind: str, retry_delay_seconds: float) -> None:
        return None

    def fail_oversized(self) -> None:
        return None


def test_rpc_error_typed_accessors() -> None:
    """Each accessor returns its catalog type, unknown types are skipped."""
    details = [s[1] for s in _SAMPLES[:2]] + [{"@type": "x.Y"}] + [s[1] for s in _SAMPLES[3:]]
    err = RpcError("E", "m", "", error_code="UNAVAILABLE", error_kind="k", error_details=details)
    assert err.code is Code.UNAVAILABLE
    assert err.error_info() == ErrorInfo(metadata={"k": "v"})
    assert err.retry_info() == RetryInfo(retry_delay_seconds=7)
    assert err.bad_request() is not None and err.precondition_failure() is not None
    assert err.quota_failure() is not None and err.resource_info() is not None
    assert err.help() is not None and err.localized_message() is not None
    assert len(err.details()) == 8


def test_secondary_hash_is_pinned() -> None:
    """The reference computes the digest MULTI_PROTOCOL_HOSTING.md pins."""
    assert compute_protocol_hash("conformance.Secondary.v1", rpc_methods(Secondary)) == SECONDARY_PROTOCOL_HASH


def test_the_hosted_set_is_sealed_at_construction() -> None:
    """WIRE_PROTOCOL.md §3.1: no registration once serving can begin; the view is read-only."""
    server = RpcServer(Secondary, _SecondaryImpl())
    with pytest.raises(TypeError):
        server.bindings["late.Protocol.v1"] = server.bindings["conformance.Secondary.v1"]  # type: ignore[index]
    assert not hasattr(server, "add_protocol")
