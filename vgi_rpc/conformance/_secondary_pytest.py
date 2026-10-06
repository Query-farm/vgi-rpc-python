# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Cross-language conformance for multi-protocol hosting and the error model.

Normative in ``tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md``.  Every port's
conformance worker hosts ``conformance.Secondary.v1`` beside
``ConformanceService``; these groups assert what is only visible *between*
implementations:

- the secondary is listed after the primary, describes, and hashes to the
  pinned digest -- read back off the running worker, never against a literal
  computed locally;
- a method name shared by both protocols resolves by ``(protocol, method)``;
- the three layers of the error model -- ``error_code``, ``error_kind``,
  ``error_details`` -- round-trip through the *client* under test, including a
  detail type no client knows and an array over the 4 KiB cap;
- every framework kind carries the code the spec's table assigns it;
- tracebacks are present by default on every transport.

**Runner contract.**  Tests are parametrized by the runner's existing
``conformance_conn`` fixture, used only as the transport axis.  They reach the
worker through one more fixture the runner provides,
``conformance_protocol_connector``: ``connector(transport, protocol,
on_log=None)`` returns a context manager yielding a proxy bound to *protocol*
on the worker *transport* reaches.  In client role, a port implements it as
``ClientDriver(cmd, service=protocol).connect(...)``.  A runner that does not
provide it is skipped loudly, naming the fixture.

The raw-wire groups (``TestErrorModelOnTheWire``) need only
``conformance_http_port`` and check the server's bytes directly, so a
truncating server cannot pass by being decoded leniently.
"""

from __future__ import annotations

import contextlib
import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

import pytest
from pyarrow import ipc

from vgi_rpc.conformance.secondary import (
    FAIL_ERROR_INFO,
    INVALID_CODE_KIND,
    OVERSIZED_KIND,
    PROBE_DETAIL,
    SECONDARY_ECHO_PREFIX,
    SECONDARY_PROTOCOL_HASH,
    SECONDARY_PROTOCOL_NAME,
    Secondary,
    expected_fail_details,
)
from vgi_rpc.errors import MAX_ERROR_DETAILS_BYTES, ErrorInfo
from vgi_rpc.metadata import (
    ERROR_CODE_KEY,
    ERROR_DETAILS_KEY,
    ERROR_KIND_KEY,
    LOG_EXTRA_KEY,
    LOG_LEVEL_KEY,
)
from vgi_rpc.rpc import RpcError
from vgi_rpc.rpc._reflection import ProtocolList, Reflection, ServiceDescription

if TYPE_CHECKING:
    import httpx2

#: Several round trips per test, some against a freshly connected worker.
pytestmark = pytest.mark.timeout(30)

#: The fixture a runner supplies; quoted verbatim in every skip.
CONNECTOR_FIXTURE = "conformance_protocol_connector"

#: The primary conformance protocol's wire name.
PRIMARY_PROTOCOL_NAME = "ConformanceService"

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

#: The canonical codes, spelled out rather than read from :class:`Code` so a
#: reference that silently grew or renamed one would still fail here.
CANONICAL_CODES = (
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
)


# ---------------------------------------------------------------------------
# Probe protocols: same wire names, different surfaces
# ---------------------------------------------------------------------------


class SecondaryProbe(Protocol):
    """``conformance.Secondary.v1`` plus a method the worker does not host."""

    protocol_name: ClassVar[str] = SECONDARY_PROTOCOL_NAME

    def not_a_method(self) -> None:
        """Absent on the worker: answered ``method_not_implemented``."""
        ...


class UnhostedProtocol(Protocol):
    """A protocol no conformance worker hosts."""

    protocol_name: ClassVar[str] = "conformance.Unhosted.v1"

    def echo_string(self, value: str) -> str:
        """Never dispatched: answered ``protocol_not_supported``."""
        ...


class StalePrimary(Protocol):
    """``ConformanceService`` as a client one minor version behind would declare it."""

    protocol_name: ClassVar[str] = PRIMARY_PROTOCOL_NAME
    protocol_version: ClassVar[str] = "1.0.0"

    def echo_string(self, value: str) -> str:
        """Gated: answered ``protocol_version_mismatch``."""
        ...


class PrimaryEcho(Protocol):
    """The one primary method the routing test needs, at the primary's version."""

    protocol_name: ClassVar[str] = PRIMARY_PROTOCOL_NAME
    protocol_version: ClassVar[str] = "2.0.0"

    def echo_string(self, value: str) -> str:
        """Echo verbatim -- the primary's behaviour."""
        ...


# ---------------------------------------------------------------------------
# The connector
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProtocolTarget:
    """One transport to the worker under test, able to bind any protocol.

    Attributes:
        transport: The ``conformance_conn`` parameter id.
        connect: ``connect(protocol)`` -> context manager yielding a proxy.

    """

    transport: str
    connect: Callable[[type], contextlib.AbstractContextManager[Any]]


@pytest.fixture
def protocol_target(request: pytest.FixtureRequest, conformance_conn: Any) -> ProtocolTarget:
    """Return a :class:`ProtocolTarget` on the transport this test is parametrized over.

    ``conformance_conn`` is requested only as the transport axis, so every
    runner's existing transport matrix -- narrowed to what its client can dial
    in client role -- applies unchanged.

    Args:
        request: The test's fixture request.
        conformance_conn: The runner's transport fixture; only its parameter is read.

    Returns:
        The target for this test's transport.

    Raises:
        pytest.skip.Exception: The runner provides no connector.

    """
    del conformance_conn
    transport = str(request.node.callspec.params.get("conformance_conn"))
    try:
        connector = request.getfixturevalue(CONNECTOR_FIXTURE)
    except pytest.FixtureLookupError:
        pytest.skip(
            f"runner provides no {CONNECTOR_FIXTURE!r}. Multi-protocol hosting and the error model are "
            f"asserted through {SECONDARY_PROTOCOL_NAME}, which every conformance worker hosts beside "
            f"{PRIMARY_PROTOCOL_NAME}; the connector binds a proxy to a named protocol on the same worker. "
            f"See MULTI_PROTOCOL_HOSTING.md. A skip here is an open Phase-2 deliverable, not a pass."
        )
    return ProtocolTarget(transport=transport, connect=lambda protocol: connector(transport, protocol, None))


def _raised(call: Callable[[], object]) -> RpcError:
    """Run *call*, requiring an :class:`RpcError`."""
    try:
        result = call()
    except RpcError as exc:
        return exc
    raise AssertionError(f"the call succeeded and returned {result!r}; an error was required")


def _same_details(got: list[dict[str, Any]], expected: list[dict[str, Any]]) -> bool:
    """Compare detail arrays as JSON *values*: ``7`` and ``7.0`` are one number."""

    def norm(value: Any) -> Any:
        if isinstance(value, bool) or value is None or isinstance(value, str):
            return value
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, list):
            return [norm(v) for v in value]
        if isinstance(value, dict):
            return {k: norm(v) for k, v in value.items()}
        return value

    return bool(norm(got) == norm(expected))


# ---------------------------------------------------------------------------
# Hosting
# ---------------------------------------------------------------------------


def list_protocols(target: ProtocolTarget) -> ProtocolList:
    """Ask the worker what it hosts, through ordinary reflection."""
    with target.connect(Reflection) as proxy:
        listing = proxy.list_protocols()
    assert isinstance(listing, ProtocolList), f"list_protocols returned {type(listing).__name__}"
    return listing


def application_protocols(listing: ProtocolList) -> list[str]:
    """Return the non-framework protocols, in the order the worker listed them."""
    return [p.protocol for p in listing.protocols if not p.protocol.startswith("vgi_rpc.")]


class TestSecondaryIsHosted:
    """The secondary is registered, discoverable, and hashes to the pinned digest."""

    def test_listed_after_the_primary(self, protocol_target: ProtocolTarget) -> None:
        """Application protocols are listed in registration order, primary first."""
        hosted = application_protocols(list_protocols(protocol_target))
        assert hosted == [PRIMARY_PROTOCOL_NAME, SECONDARY_PROTOCOL_NAME], (
            f"the conformance worker lists application protocols {hosted}; required "
            f"[{PRIMARY_PROTOCOL_NAME!r}, {SECONDARY_PROTOCOL_NAME!r}] in that order (WIRE_PROTOCOL.md §3.1). "
            f"A client's 'describe this server' takes the first non-reserved protocol, so order is contract."
        )


class TestSecondaryDescribes:
    """What the secondary says about itself -- valid on any worker that hosts it."""

    def test_reports_the_pinned_hash(self, protocol_target: ProtocolTarget) -> None:
        """The listing's digest for the secondary is the pinned one."""
        summaries = {p.protocol: p for p in list_protocols(protocol_target).protocols}
        summary = summaries.get(SECONDARY_PROTOCOL_NAME)
        assert summary is not None, f"{SECONDARY_PROTOCOL_NAME} is not hosted (hosts {sorted(summaries)})"
        assert summary.protocol_hash == SECONDARY_PROTOCOL_HASH, (
            f"{SECONDARY_PROTOCOL_NAME} hashes to {summary.protocol_hash!r}, not {SECONDARY_PROTOCOL_HASH!r}. "
            f"The canonical preimage is in MULTI_PROTOCOL_HOSTING.md; the likeliest cause is fail's "
            f"retry_delay_seconds declared as float32, or a has_return on a void method."
        )
        assert summary.protocol_version == "", (
            f"{SECONDARY_PROTOCOL_NAME} must declare no protocol_version; it reports {summary.protocol_version!r}"
        )

    def test_describe_lists_the_three_methods(self, protocol_target: ProtocolTarget) -> None:
        """``describe`` answers for the secondary, unary methods only."""
        with protocol_target.connect(Reflection) as proxy:
            description = proxy.describe(protocol=SECONDARY_PROTOCOL_NAME)
        assert isinstance(description, ServiceDescription)
        by_name = {m.name: m for m in description.methods}
        assert sorted(by_name) == ["echo_string", "fail", "fail_oversized"], sorted(by_name)
        assert description.protocol_hash == SECONDARY_PROTOCOL_HASH
        for name, method in by_name.items():
            assert method.method_type == "unary", f"{name} is {method.method_type!r}"
        assert by_name["echo_string"].has_return
        assert not by_name["fail"].has_return
        assert not by_name["fail_oversized"].has_return

    def test_features_are_emitted_empty(self, protocol_target: ProtocolTarget) -> None:
        """``features`` is reserved: every protocol emits ``[]`` (WIRE_PROTOCOL.md §14).

        The protocol is the unit of optionality.  A capability that may be
        absent becomes its own protocol rather than a token here, so a
        non-empty list is a port inventing a mechanism the spec declined.
        """
        listing = list_protocols(protocol_target)
        offenders = {p.protocol: list(p.features) for p in listing.protocols if list(p.features)}
        assert not offenders, f"features must be emitted empty in this version; got {offenders}"


class TestRoutingByPair:
    """``echo_string`` exists on both protocols and resolves by ``(protocol, method)``."""

    def test_the_same_name_reaches_two_bindings(self, protocol_target: ProtocolTarget) -> None:
        """The primary echoes verbatim; the secondary prefixes.

        Also the per-binding version gate: the secondary declares no version,
        so its call carries none, and a server gating every call against the
        primary's ``2.0.0`` refuses it.
        """
        with protocol_target.connect(PrimaryEcho) as primary:
            assert primary.echo_string(value="ping") == "ping"
        with protocol_target.connect(Secondary) as secondary:
            got = secondary.echo_string(value="ping")
        assert got == SECONDARY_ECHO_PREFIX + "ping", (
            f"{SECONDARY_PROTOCOL_NAME}.echo_string answered {got!r}. 'ping' means the call was dispatched "
            f"to {PRIMARY_PROTOCOL_NAME}.echo_string: the server resolved on the bare method name."
        )


class TestSecondaryRouting:
    """Routing answers for the secondary -- valid on any worker that hosts it."""

    def test_echo_routes_to_the_secondary(self, protocol_target: ProtocolTarget) -> None:
        """The secondary's own behaviour, with no protocol_version on the request."""
        with protocol_target.connect(Secondary) as secondary:
            assert secondary.echo_string(value="pong") == SECONDARY_ECHO_PREFIX + "pong"

    def test_an_absent_method_is_unimplemented(self, protocol_target: ProtocolTarget) -> None:
        """``method_not_implemented`` carries ``UNIMPLEMENTED``."""
        with protocol_target.connect(SecondaryProbe) as probe:
            err = _raised(probe.not_a_method)
        assert (err.error_kind, err.error_code) == ("method_not_implemented", "UNIMPLEMENTED"), (
            f"kind={err.error_kind!r} code={err.error_code!r}"
        )

    def test_an_unhosted_protocol_is_unimplemented(self, protocol_target: ProtocolTarget) -> None:
        """``protocol_not_supported`` carries ``UNIMPLEMENTED``, gRPC's answer for both."""
        with protocol_target.connect(UnhostedProtocol) as unhosted:
            err = _raised(lambda: unhosted.echo_string(value="x"))
        assert (err.error_kind, err.error_code) == ("protocol_not_supported", "UNIMPLEMENTED"), (
            f"kind={err.error_kind!r} code={err.error_code!r}"
        )


class TestVersionMismatchCode:
    """``protocol_version_mismatch`` carries ``FAILED_PRECONDITION`` and names the protocol."""

    def test_a_stale_client_gets_a_precondition_failure(self, protocol_target: ProtocolTarget) -> None:
        """A ``1.0.0`` client against the ``2.0.0`` primary."""
        with protocol_target.connect(StalePrimary) as stale:
            err = _raised(lambda: stale.echo_string(value="x"))
        assert (err.error_kind, err.error_code) == ("protocol_version_mismatch", "FAILED_PRECONDITION"), (
            f"kind={err.error_kind!r} code={err.error_code!r}"
        )
        failure = err.precondition_failure()
        assert failure is not None, f"no vgi_rpc.PreconditionFailure in {err.error_details!r}"
        subjects = [(v.type, v.subject) for v in failure.violations]
        assert ("protocol_version", PRIMARY_PROTOCOL_NAME) in subjects, (
            f"the precondition must name the gated protocol: with several bindings, 'Server: 2.0.0' alone "
            f"does not say which server. Got {subjects}"
        )


# ---------------------------------------------------------------------------
# The error model, through the client under test
# ---------------------------------------------------------------------------


class TestErrorModelRoundTrip:
    """``fail`` round-trips code, kind and details through the client."""

    @pytest.mark.parametrize(
        ("code", "kind", "delay"),
        [
            ("NOT_FOUND", "widget_missing", 0.0),
            ("UNAVAILABLE", "backend_down", 7.0),
            ("RESOURCE_EXHAUSTED", "quota_spent", 2.5),
        ],
    )
    def test_code_kind_and_details_arrive(
        self, protocol_target: ProtocolTarget, code: str, kind: str, delay: float
    ) -> None:
        """All three layers, exactly, in wire order, unknown type included."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code=code, kind=kind, retry_delay_seconds=delay))
        assert err.error_code == code, f"error_code={err.error_code!r}, expected {code!r}"
        assert err.error_kind == kind, f"error_kind={err.error_kind!r}, expected {kind!r}"
        expected = expected_fail_details(delay)
        assert _same_details(err.error_details, expected), (
            f"error_details={err.error_details!r}; expected {expected!r}. The client must surface the "
            f"array as received -- every element, in order, unknown types included."
        )
        retry = err.retry_info()
        if delay > 0:
            assert retry is not None and math.isclose(retry.retry_delay_seconds, delay), f"retry_info={retry!r}"
        else:
            assert retry is None, f"no RetryInfo was sent, yet retry_info()={retry!r}"
        info = err.error_info()
        assert info is not None and dict(info.metadata) == FAIL_ERROR_INFO["metadata"], f"error_info={info!r}"

    @pytest.mark.parametrize(
        ("code", "delay", "retryable"),
        [
            ("UNAVAILABLE", 0.0, True),
            ("RESOURCE_EXHAUSTED", 3.0, True),
            ("RESOURCE_EXHAUSTED", 0.0, False),
            ("ABORTED", 3.0, False),
            ("INTERNAL", 3.0, False),
        ],
    )
    def test_retryability_follows_the_code(
        self, protocol_target: ProtocolTarget, code: str, delay: float, retryable: bool
    ) -> None:
        """``UNAVAILABLE``; ``RESOURCE_EXHAUSTED`` only with ``RetryInfo``; nothing else."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code=code, kind="retry_probe", retry_delay_seconds=delay))
        assert err.is_retryable() is retryable, (
            f"{code} with delay {delay} must be {'retryable' if retryable else 'final'}; "
            f"is_retryable()={err.is_retryable()}"
        )

    def test_an_absent_kind_is_empty(self, protocol_target: ProtocolTarget) -> None:
        """A code with no reason: the kind reads ``""``, the code still arrives."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code="ABORTED", kind="", retry_delay_seconds=0.0))
        assert (err.error_code, err.error_kind) == ("ABORTED", ""), (err.error_code, err.error_kind)

    def test_an_unknown_detail_type_is_ignored(self, protocol_target: ProtocolTarget) -> None:
        """The error survives a detail no client knows; typed access skips it."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code="INTERNAL", kind="probe", retry_delay_seconds=0.0))
        assert PROBE_DETAIL["@type"] in [d.get("@type") for d in err.error_details], (
            f"the unknown detail was dropped from error_details: {err.error_details!r}. Clients ignore "
            f"unknown types in typed access; they do not delete them from what they report."
        )
        known = err.details()
        assert [type(d) for d in known] == [ErrorInfo], (
            f"typed access must yield the catalog entries and skip the unknown type; got {known!r}"
        )

    @pytest.mark.parametrize("code", CANONICAL_CODES)
    def test_every_code_round_trips(self, protocol_target: ProtocolTarget, code: str) -> None:
        """The closed set, one call each: a client must not collapse any of them."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code=code, kind="every_code", retry_delay_seconds=0.0))
        assert err.error_code == code, f"sent {code}, client reported {err.error_code!r}"

    def test_a_code_outside_the_set_is_a_bad_request(self, protocol_target: ProtocolTarget) -> None:
        """The fixture refuses ``OK``, which is not an error code, with a ``BadRequest``."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code="OK", kind="x", retry_delay_seconds=0.0))
        assert (err.error_code, err.error_kind) == ("INVALID_ARGUMENT", INVALID_CODE_KIND)
        bad = err.bad_request()
        assert bad is not None and [v.field for v in bad.field_violations] == ["code"], f"bad_request={bad!r}"

    def test_oversized_details_are_dropped_whole(self, protocol_target: ProtocolTarget) -> None:
        """Over 4 KiB, the array is omitted entirely -- never trimmed.

        ``fail_oversized`` puts a small ``RetryInfo`` first.  A server that drops
        only the element that does not fit keeps it, and the error then reads as
        retryable: a partial list is indistinguishable from a complete one.
        """
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(secondary.fail_oversized)
        assert (err.error_code, err.error_kind) == ("RESOURCE_EXHAUSTED", OVERSIZED_KIND), (
            f"code and kind must survive the dropped details; got {err.error_code!r} / {err.error_kind!r}"
        )
        assert err.error_details == [], (
            f"details over {MAX_ERROR_DETAILS_BYTES} bytes reached the client: {err.error_details!r}. "
            f"The whole array must be dropped."
        )
        assert not err.is_retryable(), "with its RetryInfo dropped, RESOURCE_EXHAUSTED is final"


class TestTracebackPolicy:
    """Tracebacks are included by default, on every transport (WIRE_PROTOCOL.md §8).

    An earlier draft omitted them on HTTP and TCP.  The DuckDB extension puts
    the remote traceback into the user-visible error, so omitting it hid
    chained causes from users.  A port that cannot produce a stack sends a
    synthesized trace -- at minimum ``<ErrorType>: <message>`` and the
    ``<protocol>/<method>`` that raised it -- so only non-emptiness is asserted.
    The operator's omit setting is per server and cannot be flipped from here;
    the reference tests it locally.
    """

    def test_traceback_is_present_by_default(self, protocol_target: ProtocolTarget) -> None:
        """Every transport, default configuration: a non-empty traceback."""
        with protocol_target.connect(Secondary) as secondary:
            err = _raised(lambda: secondary.fail(code="INTERNAL", kind="", retry_delay_seconds=0.0))
        assert err.remote_traceback, (
            f"over {protocol_target.transport} the server sent no traceback. Tracebacks are included by "
            f"default on every transport; a stackless port sends a synthesized one."
        )
        assert err.error_code == "INTERNAL", "the traceback must not touch the error model"


# ---------------------------------------------------------------------------
# The server's bytes, read directly
# ---------------------------------------------------------------------------


def _request_body(protocol: type, method: str, kwargs: dict[str, Any]) -> bytes:
    """Serialize one unary request for *protocol*'s *method*."""
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._wire import _write_request

    info = rpc_methods(protocol)[method]
    buf = BytesIO()
    name = str(vars(protocol).get("protocol_name") or protocol.__name__)
    version = vars(protocol).get("protocol_version")
    _write_request(
        buf,
        method,
        info.params_schema,
        kwargs,
        protocol=name,
        protocol_version=version if isinstance(version, str) else None,
    )
    return buf.getvalue()


def _post(port: int, protocol: str, method: str, body: bytes) -> httpx2.Response:
    """POST to whichever route prefix this worker serves."""
    import httpx2

    response = None
    for prefix in ("", "/vgi"):
        response = httpx2.post(
            f"http://127.0.0.1:{port}{prefix}/{protocol}/{method}",
            content=body,
            headers={"content-type": _ARROW_CONTENT_TYPE},
            timeout=10.0,
        )
        if response.status_code != 404:
            return response
    assert response is not None
    return response


@dataclass(frozen=True)
class WireError:
    """One EXCEPTION batch's metadata, decoded but not interpreted."""

    top: dict[str, str]
    extra: dict[str, Any]


def _exception_batch(body: bytes) -> WireError:
    """Return the first EXCEPTION batch in an HTTP response body."""
    reader = ipc.open_stream(BytesIO(body))
    while True:
        try:
            _, metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        meta = {k.decode(): v.decode() for k, v in metadata.to_dict().items()} if metadata else {}
        if meta.get(LOG_LEVEL_KEY.decode()) == "EXCEPTION":
            return WireError(top=meta, extra=json.loads(meta.get(LOG_EXTRA_KEY.decode(), "{}")))
    raise AssertionError(f"no EXCEPTION batch in the response body ({len(body)} bytes)")


class TestErrorModelOnTheWire:
    """The three layers as the server writes them, over HTTP.

    Read off the bytes rather than through a client so that a server which
    truncates the details array -- producing invalid JSON that a lenient
    decoder reads as "no details" -- cannot pass by accident.
    """

    _CODE = ERROR_CODE_KEY.decode()
    _KIND = ERROR_KIND_KEY.decode()
    _DETAILS = ERROR_DETAILS_KEY.decode()

    def test_top_level_keys_and_log_extra_agree(self, conformance_http_port: int) -> None:
        """Code, kind and details ride top-level and are mirrored in ``log_extra``."""
        body = _request_body(
            Secondary, "fail", {"code": "UNAVAILABLE", "kind": "backend_down", "retry_delay_seconds": 7.0}
        )
        error = _exception_batch(_post(conformance_http_port, SECONDARY_PROTOCOL_NAME, "fail", body).content)
        assert error.top.get(self._CODE) == "UNAVAILABLE", f"top-level {self._CODE}={error.top.get(self._CODE)!r}"
        assert error.top.get(self._KIND) == "backend_down", f"top-level {self._KIND}={error.top.get(self._KIND)!r}"
        raw = error.top.get(self._DETAILS)
        assert raw is not None, f"no top-level {self._DETAILS}"
        assert len(raw.encode()) <= MAX_ERROR_DETAILS_BYTES
        details = json.loads(raw)
        assert _same_details(details, expected_fail_details(7.0)), f"{self._DETAILS}={details!r}"
        assert error.extra.get("error_code") == "UNAVAILABLE", f"log_extra.error_code={error.extra.get('error_code')!r}"
        assert error.extra.get("error_kind") == "backend_down"
        mirrored = error.extra.get("error_details")
        assert isinstance(mirrored, list) and _same_details(mirrored, details), (
            f"log_extra.error_details must mirror the top-level array as a JSON array; got {mirrored!r}"
        )

    def test_oversized_details_are_absent_from_both(self, conformance_http_port: int) -> None:
        """Dropped whole: neither the top-level key nor the mirror is present."""
        body = _request_body(Secondary, "fail_oversized", {})
        error = _exception_batch(_post(conformance_http_port, SECONDARY_PROTOCOL_NAME, "fail_oversized", body).content)
        assert self._DETAILS not in error.top, (
            f"{self._DETAILS} is present ({len(error.top[self._DETAILS])} bytes) for details over the cap. "
            f"Omit the key; a trimmed or truncated array is the gRPC-proxy failure this cap exists to prevent."
        )
        assert "error_details" not in error.extra, "log_extra.error_details must be dropped with the top-level key"
        assert error.top.get(self._CODE) == "RESOURCE_EXHAUSTED"
        assert error.top.get(self._KIND) == OVERSIZED_KIND

    def test_http_carries_the_traceback_by_default(self, conformance_http_port: int) -> None:
        """``log_extra.traceback`` is a non-empty string over HTTP too."""
        body = _request_body(Secondary, "fail", {"code": "INTERNAL", "kind": "", "retry_delay_seconds": 0.0})
        error = _exception_batch(_post(conformance_http_port, SECONDARY_PROTOCOL_NAME, "fail", body).content)
        traceback = error.extra.get("traceback")
        assert isinstance(traceback, str) and traceback, (
            f"an HTTP error carried log_extra.traceback={traceback!r}; it is included by default on every transport"
        )
        assert error.top.get(self._CODE) == "INTERNAL"
        assert self._KIND not in error.top, "an empty kind is absent, not an empty string"


class TestUnclassifiedErrorsAreUnknown:
    """The code is on every EXCEPTION batch, classified or not."""

    def test_every_exception_batch_carries_a_code(self, conformance_http_port: int) -> None:
        """An error with no classification is ``UNKNOWN``, never code-less.

        ``raise_value_error`` raises a plain error with no kind; the code is
        still required on every EXCEPTION batch.
        """
        from vgi_rpc.conformance import ConformanceService

        body = _request_body(ConformanceService, "raise_value_error", {"message": "boom"})
        error = _exception_batch(_post(conformance_http_port, PRIMARY_PROTOCOL_NAME, "raise_value_error", body).content)
        code = error.top.get(ERROR_CODE_KEY.decode())
        assert code == "UNKNOWN", f"{ERROR_CODE_KEY.decode()}={code!r}"


__all__ = [
    "CANONICAL_CODES",
    "CONNECTOR_FIXTURE",
    "PRIMARY_PROTOCOL_NAME",
    "PrimaryEcho",
    "ProtocolTarget",
    "SecondaryProbe",
    "StalePrimary",
    "TestErrorModelOnTheWire",
    "TestErrorModelRoundTrip",
    "TestRoutingByPair",
    "TestSecondaryDescribes",
    "TestSecondaryIsHosted",
    "TestSecondaryRouting",
    "TestTracebackPolicy",
    "TestUnclassifiedErrorsAreUnknown",
    "TestVersionMismatchCode",
    "UnhostedProtocol",
    "application_protocols",
    "list_protocols",
    "protocol_target",
]
