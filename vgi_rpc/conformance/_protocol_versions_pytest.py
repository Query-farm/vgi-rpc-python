# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Cross-language conformance for protocol version negotiation (``PROTOCOL_VERSIONS_V1.md`` §9).

**Server side** -- valid against any worker, conformance or hosted:

- ``TestVersionsHeader``: ``VGI-Protocol-Versions`` is present and parseable
  on a 200, a 400 refusal, a 404, ``HEAD /health`` and ``OPTIONS /health``,
  and names exactly the protocols the worker gates (those reflection lists with
  a ``protocol_version``), each window containing the declared version.
- ``TestVersionsHeaderOnRejections`` / ``TestVersionsHeaderOn5xx`` /
  ``TestVersionsHeaderIsCorsExposed``: the same header on a 401, a 413, a 5xx,
  and in ``Access-Control-Expose-Headers`` on a preflight and a real response.
- ``TestDescribeAdvertisesVersions``: the reflection result batch carries
  ``vgi_rpc.protocol_versions`` on every transport, and reflection answers a
  request whose ``vgi_rpc.protocol_version`` is out of every range.
- ``TestOutOfRangeRefusal``: an out-of-range request gets
  ``protocol_version_unsupported`` / ``FAILED_PRECONDITION`` with both windows
  in the message, an ``ErrorInfo`` detail carrying ``vgi_rpc.protocol_versions``,
  the same key on the error batch, and (HTTP) status 400 plus the header.

**Client side** -- ``TestClientNegotiation`` drives the client under test
against a scripted reference server this module starts in-process.

Runner hooks
------------

=========================================== =====================================================
``conformance_http_port``                   HTTP worker under test (existing).
``protocol_target`` (via
``conformance_protocol_connector``)         Any transport (existing).  Byte-stream proxies must
                                            expose ``_transport`` (``reader``/``writer``) for the
                                            raw-metadata checks -- the Python client's do.
``conformance_http_prefix`` *(optional)*    Route prefix of the HTTP worker; default ``""``
                                            (``"/vgi"`` is also tried).
``conformance_http_auth_port`` *(opt.)*     A worker that answers an unauthenticated call 401.
``conformance_http_small_request_cap_port`` A worker with a 4 KiB request cap (413).
``conformance_http_serve_start_fail_once_port``
                                            A worker whose first call fails 500 (existing hook).
``conformance_http_cors_port`` *(opt.)*     A CORS-enabled worker allowing ``https://conformance.example``.
``conformance_negotiating_client`` *(opt.)* ``factory(base_url, protocol, version, version_range)``
                                            -> context manager yielding an object with ``ping()``
                                            (a unary call on *protocol*, no arguments, returning
                                            utf8).  The client is bound to *protocol*, declares
                                            *version* (``"3.5.2"``) as its newest, supports
                                            *version_range* (``"3.4-3.5"``), and negotiates per
                                            §6.  :func:`negotiated_protocol` builds the Python
                                            reference's Protocol class for it.
=========================================== =====================================================

A missing optional hook skips its group loudly, naming the fixture.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from io import BytesIO
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.conformance._secondary_pytest import ProtocolTarget, list_protocols
from vgi_rpc.metadata import PROTOCOL_KEY, PROTOCOL_VERSION_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY
from vgi_rpc.protocol_versions import (
    CLIENT_PROTOCOL_VERSIONS_HEADER,
    CLIENT_PROTOCOL_VERSIONS_KEY,
    PROTOCOL_VERSION_UNSUPPORTED,
    PROTOCOL_VERSIONS_HEADER,
    PROTOCOL_VERSIONS_KEY,
    VersionRange,
    parse_versions,
)
from vgi_rpc.rpc import RpcError, _dispatch_log_or_error, _drain_stream
from vgi_rpc.rpc._reflection import ProtocolList, Reflection, ServiceDescription
from vgi_rpc.utils import IpcValidation, ValidatedReader

if TYPE_CHECKING:
    import httpx2

pytestmark = pytest.mark.timeout(30)

_ARROW = "application/vnd.apache.arrow.stream"
_REFLECTION = "vgi_rpc.Reflection.v1"
_CORS_ORIGIN = "https://conformance.example"
#: A version no server gates: majors far beyond anything shipped.
_FAR_FUTURE = "99.0.0"
_FAR_FUTURE_CLIENT_RANGE = "98.0-99.0"
NEGOTIATING_CLIENT_FIXTURE = "conformance_negotiating_client"


# ---------------------------------------------------------------------------
# What the worker should advertise, derived from what it says it hosts
# ---------------------------------------------------------------------------


def gated_protocols(listing: ProtocolList) -> dict[str, tuple[int, int]]:
    """Protocols reflection lists with a ``protocol_version``: exactly the ones a server gates.

    Args:
        listing: The worker's ``list_protocols`` answer.

    Returns:
        Wire name to declared ``(major, minor)``.

    """
    out: dict[str, tuple[int, int]] = {}
    for summary in listing.protocols:
        if summary.protocol_version:
            major, minor, *_ = (int(p) for p in summary.protocol_version.split("."))
            out[summary.protocol] = (major, minor)
    return out


def assert_advertisement(value: str | bytes | None, expected: dict[str, tuple[int, int]], where: str) -> None:
    """Require *value* to parse and to name exactly the gated protocols, each window containing its version.

    Args:
        value: The header or metadata value.
        expected: :func:`gated_protocols` of the same worker.
        where: What carried it, for the message.

    """
    assert value is not None, (
        f"{where} carries no {PROTOCOL_VERSIONS_HEADER} / vgi_rpc.protocol_versions "
        f"(PROTOCOL_VERSIONS_V1.md §4.1: every response, every transport)"
    )
    text = value.decode() if isinstance(value, bytes) else value
    parsed = parse_versions(text)
    entries = [e for e in text.split(",") if e.strip()]
    assert len(parsed) == len(entries), f"{where}: unparseable entries in {text!r} (syntax: name=MAJ.MIN-MAJ.MIN)"
    assert set(parsed) == set(expected), (
        f"{where} advertises {sorted(parsed)}, but the worker gates {sorted(expected)} -- the advertisement lists "
        f"every protocol with a protocol_version and nothing else (reflection is exempt, so absent)"
    )
    for name, version in expected.items():
        assert version in parsed[name], (
            f"{where}: {name} is declared {version[0]}.{version[1]} but advertised {parsed[name]}"
        )


# ---------------------------------------------------------------------------
# Raw requests
# ---------------------------------------------------------------------------


def _zero(t: pa.DataType) -> object:
    """Return a non-null placeholder of type *t* (the gate runs before parameters are read)."""
    if pa.types.is_integer(t):
        return 0
    if pa.types.is_floating(t):
        return 0.0
    if pa.types.is_boolean(t):
        return False
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return ""
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return b""
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return []
    if pa.types.is_struct(t):
        return {t.field(i).name: _zero(t.field(i).type) for i in range(t.num_fields)}
    return None


def request_body(
    protocol: str,
    method: str,
    schema: pa.Schema,
    *,
    version: str | None,
    client_versions: str | None = None,
    values: dict[str, object] | None = None,
) -> bytes:
    """One unary request as an Arrow IPC stream, parameters filled with placeholders.

    Args:
        protocol: Routing key.
        method: Method name.
        schema: The method's parameter schema.
        version: ``vgi_rpc.protocol_version`` to send, or ``None``.
        client_versions: ``vgi_rpc.protocol_versions_client`` to send, or ``None``.
        values: Parameter values to use instead of placeholders.

    Returns:
        The request bytes.

    """
    arrays = []
    for f in schema:
        placeholder = None if f.nullable else _zero(f.type)
        value = values.get(f.name, placeholder) if values is not None else placeholder
        arrays.append(pa.array([value], type=f.type))
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    md: dict[bytes, bytes] = {
        RPC_METHOD_KEY: method.encode(),
        REQUEST_VERSION_KEY: REQUEST_VERSION,
        PROTOCOL_KEY: protocol.encode(),
    }
    if version is not None:
        md[PROTOCOL_VERSION_KEY] = version.encode()
    if client_versions is not None:
        md[CLIENT_PROTOCOL_VERSIONS_KEY] = client_versions.encode()
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        writer.write_batch(batch, custom_metadata=pa.KeyValueMetadata(md))
    return buf.getvalue()


@dataclass
class RawReply:
    """What came back for one raw request.

    Attributes:
        metadata: Custom metadata of the first non-log batch (result or error).
        error: The decoded error, when the reply was one.
        status: HTTP status, or ``0`` on a byte-stream transport.
        headers: HTTP response headers (empty on byte streams).

    """

    metadata: dict[bytes, bytes]
    error: RpcError | None
    status: int = 0
    headers: dict[str, str] = field(default_factory=dict)


def _read_reply(source: Any) -> tuple[dict[bytes, bytes], RpcError | None]:
    """Read one reply stream: the first non-log batch's metadata, and its error if it is one."""
    reader = ValidatedReader(ipc.open_stream(source), IpcValidation.NONE)
    while True:
        try:
            batch, md = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            raise AssertionError("the reply stream ended without a result or error batch") from None
        raw = dict(md.to_dict()) if md is not None else {}
        try:
            if _dispatch_log_or_error(batch, md):
                continue
        except RpcError as exc:
            _drain_stream(reader)
            return raw, exc
        _drain_stream(reader)
        return raw, None


def _http_prefix(request: pytest.FixtureRequest) -> list[str]:
    """Route prefixes to try for the HTTP worker under test."""
    try:
        prefix = str(request.getfixturevalue("conformance_http_prefix"))
    except pytest.FixtureLookupError:
        prefix = ""
    return [prefix] if prefix else ["", "/vgi"]


def _http_post(
    request: pytest.FixtureRequest, port: int, path: str, body: bytes, headers: dict[str, str] | None = None
) -> httpx2.Response:
    """POST *body* to *path* under the worker's prefix (trying the alternatives on 404)."""
    import httpx2

    resp = None
    for prefix in _http_prefix(request):
        resp = httpx2.post(
            f"http://127.0.0.1:{port}{prefix}{path}",
            content=body,
            headers={"Content-Type": _ARROW, **(headers or {})},
            timeout=10.0,
        )
        if resp.status_code != 404:
            return resp
    assert resp is not None
    return resp


def raw_call(
    request: pytest.FixtureRequest,
    target: ProtocolTarget,
    protocol: str,
    method: str,
    schema: pa.Schema,
    *,
    version: str | None,
    client_versions: str | None = None,
    values: dict[str, object] | None = None,
) -> RawReply:
    """Send one hand-built request on *target*'s transport and read the reply's metadata.

    HTTP goes straight to ``conformance_http_port``; a byte-stream transport is
    written through the connected proxy's ``_transport``.  A client-role proxy
    has neither, and the test is skipped: these assert server bytes.

    Args:
        request: The test's fixture request.
        target: The transport under test.
        protocol: Routing key.
        method: Method name.
        schema: Parameter schema.
        version: ``vgi_rpc.protocol_version``, or ``None``.
        client_versions: ``vgi_rpc.protocol_versions_client``, or ``None``.
        values: Parameter values to use instead of placeholders.

    Returns:
        The reply.

    """
    body = request_body(protocol, method, schema, version=version, client_versions=client_versions, values=values)
    if target.transport.startswith("http"):
        port = int(request.getfixturevalue("conformance_http_port"))
        resp = _http_post(request, port, f"/{protocol}/{method}", body)
        md, err = _read_reply(BytesIO(resp.content))
        return RawReply(md, err, resp.status_code, {k.lower(): v for k, v in resp.headers.items()})
    with target.connect(Reflection) as proxy:
        transport = getattr(proxy, "_transport", None)
        if transport is None or not hasattr(transport, "reader") or not hasattr(transport, "writer"):
            pytest.skip(f"{target.transport}: the connector's proxy exposes no raw transport to write bytes on")
        transport.writer.write(body)
        transport.writer.flush()
        md, err = _read_reply(transport.reader)
        return RawReply(md, err)


def _empty_schema() -> pa.Schema:
    return pa.schema([])


def _versioned_method(target: ProtocolTarget) -> tuple[str, str, pa.Schema, tuple[int, int]]:
    """Return the first gated protocol and its first unary method: what a refusal is provoked on."""
    listing = list_protocols(target)
    gated = gated_protocols(listing)
    if not gated:
        pytest.skip("the worker gates no protocol (none declares a protocol_version)")
    with target.connect(Reflection) as proxy:
        for name, version in gated.items():
            description = proxy.describe(protocol=name)
            assert isinstance(description, ServiceDescription)
            for method in description.methods:
                if method.method_type == "unary":
                    schema = ipc.read_schema(pa.py_buffer(method.params_schema_ipc))
                    return name, method.name, schema, version
    pytest.skip("no gated protocol has a unary method to provoke a refusal on")


# ---------------------------------------------------------------------------
# Server side: the header
# ---------------------------------------------------------------------------


def _http_target(request: pytest.FixtureRequest) -> tuple[int, dict[str, tuple[int, int]]]:
    """Return the HTTP worker's port and what it should advertise (read through reflection over HTTP)."""
    port = int(request.getfixturevalue("conformance_http_port"))
    body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None)
    resp = _http_post(request, port, f"/{_REFLECTION}/list_protocols", body)
    assert resp.status_code == 200, f"list_protocols over HTTP answered {resp.status_code}"
    reader = ipc.open_stream(BytesIO(resp.content))
    raw: bytes | None = None
    for batch in reader:
        if batch.num_rows:
            raw = batch.column("result")[0].as_py()
    assert raw is not None, "list_protocols returned no result row"
    listing = ProtocolList.deserialize_from_batch(ipc.open_stream(BytesIO(raw)).read_next_batch())
    return port, gated_protocols(listing)


def _header(resp: Any) -> str | None:
    value = resp.headers.get(PROTOCOL_VERSIONS_HEADER)
    return None if value is None else str(value)


class TestVersionsHeader:
    """``VGI-Protocol-Versions`` on every response the worker under test gives."""

    def test_on_a_200(self, request: pytest.FixtureRequest) -> None:
        """Reflection succeeds and carries the header."""
        port, expected = _http_target(request)
        body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None)
        resp = _http_post(request, port, f"/{_REFLECTION}/list_protocols", body)
        assert resp.status_code == 200
        assert_advertisement(_header(resp), expected, "a 200")

    def test_on_a_404(self, request: pytest.FixtureRequest) -> None:
        """An unhosted protocol is a 404, and still advertises."""
        port, expected = _http_target(request)
        body = request_body("conformance.Unhosted.v1", "echo", _empty_schema(), version=None)
        resp = _http_post(request, port, "/conformance.Unhosted.v1/echo", body)
        assert resp.status_code == 404, f"an unhosted protocol answered {resp.status_code}"
        assert_advertisement(_header(resp), expected, "a 404")

    def test_on_head_health(self, request: pytest.FixtureRequest) -> None:
        """``HEAD /health`` -- the cheapest probe a client or load balancer makes."""
        import httpx2

        port, expected = _http_target(request)
        prefix = _http_prefix(request)[0]
        resp = httpx2.head(f"http://127.0.0.1:{port}{prefix}/health", timeout=5.0)
        assert_advertisement(_header(resp), expected, f"HEAD /health ({resp.status_code})")

    def test_on_options_health(self, request: pytest.FixtureRequest) -> None:
        """``OPTIONS /health`` -- the capability discovery target."""
        import httpx2

        port, expected = _http_target(request)
        prefix = _http_prefix(request)[0]
        resp = httpx2.options(f"http://127.0.0.1:{port}{prefix}/health", timeout=5.0)
        assert_advertisement(_header(resp), expected, f"OPTIONS /health ({resp.status_code})")

    def test_the_client_header_is_never_a_reason_to_refuse(self, request: pytest.FixtureRequest) -> None:
        """``VGI-Client-Protocol-Versions`` is informational (§4.2): nonsense in it changes nothing."""
        port, expected = _http_target(request)
        body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None, client_versions="!!garbage")
        resp = _http_post(
            request, port, f"/{_REFLECTION}/list_protocols", body, {CLIENT_PROTOCOL_VERSIONS_HEADER: "x=1.0-0.1, ="}
        )
        assert resp.status_code == 200, f"a malformed client advertisement was refused: {resp.status_code}"
        assert_advertisement(_header(resp), expected, "a 200 with a malformed client advertisement")


class TestVersionsHeaderOnRejections:
    """401 and 413 are answered before dispatch, by middleware -- and still advertise."""

    def test_on_a_401(self, request: pytest.FixtureRequest) -> None:
        """An unauthenticated call to an auth-requiring worker."""
        try:
            port = int(request.getfixturevalue("conformance_http_auth_port"))
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_auth_port")
        body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None)
        resp = _http_post(request, port, f"/{_REFLECTION}/list_protocols", body)
        assert resp.status_code == 401, f"expected 401 from the auth worker, got {resp.status_code}"
        value = _header(resp)
        assert value is not None, "a 401 carries no VGI-Protocol-Versions (hosted-mode 401s must, §4.1)"
        assert len(parse_versions(value)) == len([e for e in value.split(",") if e.strip()]), value

    def test_on_a_413(self, request: pytest.FixtureRequest) -> None:
        """A body over the advertised request cap."""
        try:
            port = int(request.getfixturevalue("conformance_http_small_request_cap_port"))
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_small_request_cap_port")
        resp = _http_post(request, port, "/ConformanceService/echo_string", b"\0" * 16384)
        assert resp.status_code == 413, f"expected 413 over the 4 KiB cap, got {resp.status_code}"
        value = _header(resp)
        assert value is not None, "a 413 carries no VGI-Protocol-Versions"
        assert "ConformanceService" in parse_versions(value), value


class TestVersionsHeaderOn5xx:
    """A server-side failure still advertises: the client may be reading it to diagnose that very failure."""

    def test_on_a_500(self, request: pytest.FixtureRequest) -> None:
        """The first call to a worker whose ``on_serve_start`` fails once."""
        try:
            port = int(request.getfixturevalue("conformance_http_serve_start_fail_once_port"))
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_serve_start_fail_once_port")
        import httpx2

        body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None)
        resp = httpx2.post(
            f"http://127.0.0.1:{port}/{_REFLECTION}/list_protocols",
            content=body,
            headers={"Content-Type": _ARROW},
            timeout=10.0,
        )
        assert resp.status_code >= 500, f"expected the injected 5xx, got {resp.status_code}"
        value = _header(resp)
        assert value is not None, f"a {resp.status_code} carries no VGI-Protocol-Versions"
        assert "ConformanceService" in parse_versions(value), value


class TestVersionsHeaderIsCorsExposed:
    """A browser client can read the advertisement only if it is exposed."""

    @staticmethod
    def _port(request: pytest.FixtureRequest) -> int:
        try:
            return int(request.getfixturevalue("conformance_http_cors_port"))
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no conformance_http_cors_port")

    @staticmethod
    def _exposed(resp: Any) -> set[str]:
        raw = resp.headers.get("access-control-expose-headers", "")
        return {n.strip().lower() for n in raw.split(",") if n.strip()}

    def test_on_a_preflight(self, request: pytest.FixtureRequest) -> None:
        """The preflight carries the header and exposes it."""
        import httpx2

        resp = httpx2.options(
            f"http://127.0.0.1:{self._port(request)}/ConformanceService/echo_string",
            headers={
                "Origin": _CORS_ORIGIN,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": f"content-type, {CLIENT_PROTOCOL_VERSIONS_HEADER.lower()}",
            },
            timeout=5.0,
        )
        assert _header(resp) is not None, "a CORS preflight carries no VGI-Protocol-Versions"
        exposed = self._exposed(resp)
        assert "*" in exposed or PROTOCOL_VERSIONS_HEADER.lower() in exposed, (
            f"{PROTOCOL_VERSIONS_HEADER} is not in Access-Control-Expose-Headers ({sorted(exposed)})"
        )
        allowed = {h.strip().lower() for h in resp.headers.get("access-control-allow-headers", "").split(",")}
        assert "*" in allowed or CLIENT_PROTOCOL_VERSIONS_HEADER.lower() in allowed, (
            f"a browser may not send {CLIENT_PROTOCOL_VERSIONS_HEADER}; allowed: {sorted(allowed)}"
        )

    def test_on_a_real_response(self, request: pytest.FixtureRequest) -> None:
        """The cross-origin response itself exposes it."""
        body = request_body(_REFLECTION, "list_protocols", _empty_schema(), version=None)
        port = self._port(request)
        import httpx2

        resp = httpx2.post(
            f"http://127.0.0.1:{port}/{_REFLECTION}/list_protocols",
            content=body,
            headers={"Content-Type": _ARROW, "Origin": _CORS_ORIGIN},
            timeout=10.0,
        )
        assert resp.status_code == 200
        assert _header(resp) is not None
        exposed = self._exposed(resp)
        assert "*" in exposed or PROTOCOL_VERSIONS_HEADER.lower() in exposed, sorted(exposed)


# ---------------------------------------------------------------------------
# Server side: describe and refusals, every transport
# ---------------------------------------------------------------------------


class TestDescribeAdvertisesVersions:
    """Reflection is the version-independent way in, on every transport (§4.1)."""

    def test_list_protocols_carries_the_windows(
        self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget
    ) -> None:
        """The result batch's metadata names every gated protocol's window."""
        expected = gated_protocols(list_protocols(protocol_target))
        reply = raw_call(request, protocol_target, _REFLECTION, "list_protocols", _empty_schema(), version=None)
        assert reply.error is None, f"list_protocols failed: {reply.error}"
        assert_advertisement(reply.metadata.get(PROTOCOL_VERSIONS_KEY), expected, "the list_protocols result batch")

    def test_describe_carries_the_windows(
        self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget
    ) -> None:
        """``describe`` too: either call may be the first a client makes."""
        listing = list_protocols(protocol_target)
        expected = gated_protocols(listing)
        schema = pa.schema([pa.field("protocol", pa.utf8(), nullable=False)])
        described = next(iter(expected), listing.protocols[0].protocol)
        reply = raw_call(
            request, protocol_target, _REFLECTION, "describe", schema, version=None, values={"protocol": described}
        )
        assert reply.error is None, f"describe({described!r}) failed: {reply.error}"
        assert_advertisement(reply.metadata.get(PROTOCOL_VERSIONS_KEY), expected, "the describe result batch")

    def test_reflection_answers_any_version(
        self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget
    ) -> None:
        """A request declaring a version outside every window still gets reflection's answer."""
        expected = gated_protocols(list_protocols(protocol_target))
        reply = raw_call(
            request,
            protocol_target,
            _REFLECTION,
            "list_protocols",
            _empty_schema(),
            version=_FAR_FUTURE,
            client_versions=f"{_REFLECTION}={_FAR_FUTURE_CLIENT_RANGE}",
        )
        assert reply.error is None, (
            f"reflection refused a {_FAR_FUTURE} client ({reply.error}); describe must be answerable by a client of "
            f"ANY version (§4.1) -- exempt it from the gate"
        )
        assert_advertisement(reply.metadata.get(PROTOCOL_VERSIONS_KEY), expected, "list_protocols at 99.0.0")


class TestOutOfRangeRefusal:
    """``protocol_version_unsupported``: both windows, machine-readable, on every transport (§4.3)."""

    def test_a_too_new_client_is_refused(self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget) -> None:
        """A 99.0 client against the worker's first gated protocol."""
        expected = gated_protocols(list_protocols(protocol_target))
        name, method, schema, _ = _versioned_method(protocol_target)
        reply = raw_call(
            request,
            protocol_target,
            name,
            method,
            schema,
            version=_FAR_FUTURE,
            client_versions=f"{name}={_FAR_FUTURE_CLIENT_RANGE}",
        )
        err = reply.error
        assert err is not None, f"{name}.{method} accepted protocol_version {_FAR_FUTURE}"
        assert (err.error_kind, err.error_code) == (PROTOCOL_VERSION_UNSUPPORTED, "FAILED_PRECONDITION"), (
            f"kind={err.error_kind!r} code={err.error_code!r}; a refusal is protocol_version_unsupported / "
            f"FAILED_PRECONDITION (the pre-negotiation kind protocol_version_mismatch is retired)"
        )
        message = err.error_message
        server_window = parse_versions(reply.metadata.get(PROTOCOL_VERSIONS_KEY) or b"").get(name)
        assert server_window is not None, (
            f"the error batch carries no vgi_rpc.protocol_versions naming {name}: {reply.metadata!r}"
        )
        assert f"{name} {server_window}" in message, f"the message must name the server's window: {message!r}"
        assert _FAR_FUTURE_CLIENT_RANGE in message and "99.0" in message, (
            f"the message must name the client's version and advertised window: {message!r}"
        )
        assert "too old" in message or "too new" in message, f"keep 'too old'/'too new' for log grep: {message!r}"
        info = err.error_info()
        assert info is not None, f"no vgi_rpc.ErrorInfo detail in {err.error_details!r}"
        assert_advertisement(info.metadata.get("vgi_rpc.protocol_versions"), expected, "the refusal's ErrorInfo")
        failure = err.precondition_failure()
        assert failure is not None and ("protocol_version", name) in [(v.type, v.subject) for v in failure.violations]
        if protocol_target.transport.startswith("http"):
            assert reply.status == 400, f"an HTTP refusal is a 400, got {reply.status}"
            assert_advertisement(reply.headers.get(PROTOCOL_VERSIONS_HEADER.lower()), expected, "the 400 refusal")

    def test_a_too_old_client_is_refused(self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget) -> None:
        """One major below the worker's window: the other direction."""
        name, method, schema, version = _versioned_method(protocol_target)
        if version[0] == 0:
            pytest.skip(f"{name} is at major 0; there is nothing below it")
        reply = raw_call(request, protocol_target, name, method, schema, version=f"{version[0] - 1}.0.0")
        err = reply.error
        assert err is not None and err.error_kind == PROTOCOL_VERSION_UNSUPPORTED, err
        assert "too old" in err.error_message, err.error_message

    def test_a_missing_version_is_refused(
        self, request: pytest.FixtureRequest, protocol_target: ProtocolTarget
    ) -> None:
        """Stripping the version does not bypass the gate."""
        name, method, schema, _ = _versioned_method(protocol_target)
        reply = raw_call(request, protocol_target, name, method, schema, version=None)
        err = reply.error
        assert err is not None and err.error_kind == PROTOCOL_VERSION_UNSUPPORTED, err
        assert reply.metadata.get(PROTOCOL_VERSIONS_KEY) is not None, reply.metadata


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------

#: The protocol the scripted server hosts for client tests.
NEGOTIATED_PROTOCOL = "conformance.Negotiated.v1"


def negotiated_protocol(version: str, version_range: str) -> type:
    """Return the Python reference's Protocol for :data:`NEGOTIATED_PROTOCOL` at *version* supporting *version_range*.

    Args:
        version: Newest version the client speaks (``"3.5.2"``).
        version_range: Window the client supports (``"3.4-3.5"``).

    Returns:
        A Protocol class with one method, ``ping() -> str``.

    """

    class Negotiated(Protocol):
        protocol_name: ClassVar[str] = NEGOTIATED_PROTOCOL
        protocol_version: ClassVar[str] = version
        protocol_version_range: ClassVar[str] = version_range

        def ping(self) -> str:
            """Return the ``vgi_rpc.protocol_version`` the server received."""
            ...

    return Negotiated


@dataclass
class ScriptedServer:
    """A reference HTTP server for client tests, recording every version it is sent.

    Attributes:
        base_url: Where it listens.
        received: ``vgi_rpc.protocol_version`` of every request that reached the gate, in order.

    """

    base_url: str
    received: list[str]


@contextlib.contextmanager
def scripted_server(window: str, *, advertise: bool = True, legacy: bool = False) -> Generator[ScriptedServer]:
    """Serve :data:`NEGOTIATED_PROTOCOL` over HTTP on loopback, accepting *window*.

    Args:
        window: The ``major.minor`` window it accepts (``"3.4-3.4"``).
        advertise: ``False`` strips ``VGI-Protocol-Versions`` from every response
            and the windows from refusals -- a server released before this spec.
        legacy: Refuse the way a 3.4.0 server did (``protocol_version_mismatch``,
            ``Server: X.Y.0``, no machine-readable windows).  Implies ``advertise=False``.

    Yields:
        The running server.

    """
    from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

    from vgi_rpc.http import make_wsgi_app
    from vgi_rpc.rpc import RpcServer
    from vgi_rpc.rpc._common import ProtocolVersionError, _current_request_metadata

    rng = VersionRange.parse(window)
    received: list[str] = []
    proto = negotiated_protocol(f"{rng.max[0]}.{rng.max[1]}.0", str(rng))

    class _LegacyRefusal(ProtocolVersionError):
        error_kind: ClassVar[str] = "protocol_version_mismatch"

    class _Recording(RpcServer):
        __slots__ = ()

        def gate_version(self, info: Any) -> None:
            md = _current_request_metadata.get()
            raw = md.get(PROTOCOL_VERSION_KEY) if md is not None else None
            if info.protocol_name == NEGOTIATED_PROTOCOL:
                received.append(raw.decode() if raw is not None else "")
            if legacy and info.protocol_name == NEGOTIATED_PROTOCOL:
                try:
                    super().gate_version(info)
                except ProtocolVersionError as exc:
                    server_version = f"{rng.max[0]}.{rng.max[1]}.0"
                    raise _LegacyRefusal(
                        f"VGI client/worker protocol_version mismatch for protocol {NEGOTIATED_PROTOCOL!r}.\n"
                        f"  Client: {exc.client_version}\n  Server: {server_version}\n"
                        f"  Direction: server is too old.",
                        protocol=NEGOTIATED_PROTOCOL,
                        client_version=exc.client_version,
                        server_version=server_version,
                    ) from None
                return
            super().gate_version(info)

    class _Impl:
        def ping(self) -> str:
            md = _current_request_metadata.get()
            raw = md.get(PROTOCOL_VERSION_KEY) if md is not None else None
            return raw.decode() if raw is not None else ""

    server = _Recording(proto, _Impl(), enable_describe=True, grant_keys=None)
    app = make_wsgi_app(server, token_key=b"k" * 32, enable_landing_page=False)
    strip = not advertise or legacy

    def wsgi(environ: dict[str, Any], start_response: Callable[..., Any]) -> Any:
        def _start(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
            if strip:
                headers = [(k, v) for k, v in headers if k.lower() != PROTOCOL_VERSIONS_HEADER.lower()]
            return start_response(status, headers, exc_info)

        return app(environ, _start)

    class _Quiet(WSGIRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    httpd: WSGIServer = make_server("127.0.0.1", 0, wsgi, handler_class=_Quiet)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield ScriptedServer(base_url=f"http://127.0.0.1:{httpd.server_port}", received=received)
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _client(request: pytest.FixtureRequest) -> Callable[..., contextlib.AbstractContextManager[Any]]:
    try:
        factory = request.getfixturevalue(NEGOTIATING_CLIENT_FIXTURE)
    except pytest.FixtureLookupError:
        pytest.skip(
            f"runner provides no {NEGOTIATING_CLIENT_FIXTURE!r}: client-side negotiation (PROTOCOL_VERSIONS_V1.md "
            f"§6, §9) is asserted through it. A skip here is an open deliverable, not a pass."
        )
    return cast("Callable[..., contextlib.AbstractContextManager[Any]]", factory)


def _call(proxy: Any) -> tuple[str | None, BaseException | None]:
    try:
        return str(proxy.ping()), None
    except Exception as exc:  # the client's own error type, whatever it is
        return None, exc


def _kind(exc: BaseException | None) -> str:
    return str(getattr(exc, "error_kind", "") or "")


class TestClientNegotiation:
    """The client under test against a scripted reference server (§6, §9)."""

    def test_sends_its_newest_version_before_any_response(self, request: pytest.FixtureRequest) -> None:
        """Nothing learned yet: the client speaks its max, with its own patch."""
        with (
            scripted_server("3.4-3.5") as srv,
            _client(request)(srv.base_url, NEGOTIATED_PROTOCOL, "3.5.2", "3.4-3.5") as c,
        ):
            got, err = _call(c)
        assert err is None, f"a call within both windows failed: {err}"
        assert srv.received[:1] == ["3.5.2"], f"the first request declared {srv.received[:1]}, expected ['3.5.2']"
        assert got == "3.5.2"

    def test_speaks_the_common_version_after_the_first_response(self, request: pytest.FixtureRequest) -> None:
        """Server ``3.4-3.4``, client ``3.4-3.5``: 3.4 once the client has seen a response.

        The first call may be refused and surfaced (a call not declared
        idempotent is never retried), or retried once at 3.4 -- either is
        conformant.  The second call must go out at 3.4 and succeed, and no call
        may cost more than two requests.
        """
        with (
            scripted_server("3.4-3.4") as srv,
            _client(request)(srv.base_url, NEGOTIATED_PROTOCOL, "3.5.2", "3.4-3.5") as c,
        ):
            _first, first_err = _call(c)
            sent_by_first = len(srv.received)
            second, second_err = _call(c)
        assert sent_by_first in (1, 2), f"the first call sent {sent_by_first} requests: {srv.received}"
        assert srv.received[0] == "3.5.2", f"before any response the client must send its max: {srv.received}"
        if first_err is not None:
            assert _kind(first_err) == PROTOCOL_VERSION_UNSUPPORTED, first_err
        assert second_err is None, f"after a response advertising 3.4-3.4 the client still failed: {second_err}"
        assert second == "3.4.2", f"the second call declared {second!r}; expected the common 3.4 plus patch: 3.4.2"
        assert srv.received[-1] == "3.4.2", srv.received

    def test_a_disjoint_window_fails_naming_both(self, request: pytest.FixtureRequest) -> None:
        """Server ``3.4-3.4``, client ``3.5-3.6``: a ``protocol_version_unsupported`` failure naming both windows.

        Once a response has shown there is no common version the client fails
        without sending: there is nothing it could send that would be accepted.
        """
        with (
            scripted_server("3.4-3.4") as srv,
            _client(request)(srv.base_url, NEGOTIATED_PROTOCOL, "3.6.0", "3.5-3.6") as c,
        ):
            _, first_err = _call(c)
            sent = len(srv.received)
            _, second_err = _call(c)
        assert first_err is not None and second_err is not None, "a disjoint pairing succeeded"
        for err in (first_err, second_err):
            text = str(getattr(err, "error_message", "") or err)
            assert "3.4-3.4" in text and "3.5-3.6" in text, f"the failure must name both windows: {text!r}"
            assert "too old" in text or "too new" in text, text
        assert _kind(second_err) == PROTOCOL_VERSION_UNSUPPORTED, second_err
        assert len(srv.received) == sent, (
            f"after learning there is no common version the client sent again: {srv.received[sent:]}"
        )

    def test_a_missing_header_falls_back_to_its_own_version(self, request: pytest.FixtureRequest) -> None:
        """A server released before this spec: the client keeps speaking its own version (§6)."""
        with (
            scripted_server("3.5-3.5", advertise=False) as srv,
            _client(request)(srv.base_url, NEGOTIATED_PROTOCOL, "3.5.1", "3.4-3.5") as c,
        ):
            first, first_err = _call(c)
            second, second_err = _call(c)
        assert first_err is None and second_err is None, (first_err, second_err)
        assert (first, second) == ("3.5.1", "3.5.1"), srv.received

    def test_a_legacy_refusal_names_the_version_to_speak(self, request: pytest.FixtureRequest) -> None:
        """A 3.4.0-era refusal (no header, ``Server: 3.4.0``): the client then speaks exactly that version."""
        with (
            scripted_server("3.4-3.4", legacy=True) as srv,
            _client(request)(srv.base_url, NEGOTIATED_PROTOCOL, "3.5.2", "3.4-3.5") as c,
        ):
            _call(c)
            second, second_err = _call(c)
        assert second_err is None, f"after a legacy refusal naming 3.4.0 the client still failed: {second_err}"
        assert second == "3.4.2", srv.received


__all__ = [
    "NEGOTIATED_PROTOCOL",
    "NEGOTIATING_CLIENT_FIXTURE",
    "ScriptedServer",
    "TestClientNegotiation",
    "TestDescribeAdvertisesVersions",
    "TestOutOfRangeRefusal",
    "TestVersionsHeader",
    "TestVersionsHeaderIsCorsExposed",
    "TestVersionsHeaderOn5xx",
    "TestVersionsHeaderOnRejections",
    "assert_advertisement",
    "gated_protocols",
    "negotiated_protocol",
    "request_body",
    "scripted_server",
]
