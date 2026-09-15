# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Cross-language conformance for ``vgi_rpc.Identity.v1``.

The protocol is implemented in every port and, until this module, was
exercised by the shared suite **not at all**.  Its wire shape was pinned only
by each port asserting three digests against constants copied out of a spec
document -- which proves the copy was made, not that the server answers that
way -- and its behaviour only by port-local translations of one Python test
file.  Five of six ports shipped the same hole (a whitespace-padded JWS
credential reaching the resolver) and no port's own suite caught it, because
every port had translated the same test that could not catch it.

What this group asserts is the part that is only visible *between*
implementations: the guard order, the uniform rejection, the enumerated trim
floor, the byte-measured cap, the freshness rule, the five ``error_kind``
strings, and that narrowing the hosted method set narrows the protocol hash.

Two things make these assertions non-vacuous, and both matter more than they
look.

**The conformance resolver resolves almost anything.**  Rejections are
deliberately uniform, so an over-long credential is *also* an unknown one and
a test that probes the cap with an unknown credential stays green when the cap
is deleted.  The reference had exactly that bug and two ports found it
independently.  With a resolver that answers for whatever it is handed, a
rejection can only have come from a guard, and a broken guard produces a
*success* -- which no amount of uniformity can disguise.

**The wire shape is read back, not asserted against a literal.**  The three
pinned digests are checked against what reflection reports from a running
server, and the parameter schemas are decoded from the IPC that same server
sent.  A port that computes the right digest from the wrong schema, or hosts a
method its description omits, fails here and cannot fail in a local unit test.

Skips are deliberately loud.  A port that hosts no identity fixture is not
non-conformant -- absence is legitimate, and a worker configuring no hook must
host no protocol at all -- but a skip that does not name the missing fixture is
how a gap stays invisible.  See ``IDENTITY_CONFORMANCE_FIXTURE.md`` for what a
port must provide.
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from typing import TYPE_CHECKING, Any, ClassVar

import pytest
from pyarrow import ipc

from vgi_rpc.conformance.identity_fixture import (
    AUTH_TIME_HEADER,
    GRANT_EXPIRES_AT,
    GRANT_ID,
    GRANT_TOKEN_PREFIX,
    IDENTITY_BOTH_METHODS_HASH,
    IDENTITY_INTROSPECT_ONLY_HASH,
    IDENTITY_PROTOCOL_NAME,
    INTROSPECTOR_PRINCIPAL,
    MAX_AUTH_AGE,
    MINIMAL_PURPOSE,
    MINTER_PRINCIPAL,
    OTHER_MINTER_PRINCIPAL,
    OUTSIDER_PRINCIPAL,
    PRINCIPAL_HEADER,
    REFUSED_PURPOSE,
    SCOPE_SEPARATOR,
    SUBJECT_PRINCIPAL,
    SUBJECT_TOKEN,
    SUBJECT_TOKEN_NAME,
    SUBJECT_TTL,
    TOKEN_JWS_TRAP,
    TOKEN_MINIMAL,
    TOKEN_PADDED_PROBE,
    TOKEN_PADDED_PROBE_NAME,
    TOKEN_UNAVAILABLE,
    TOKEN_UNKNOWN,
    TOKEN_ZERO_TTL,
)
from vgi_rpc.metadata import ERROR_KIND_KEY, LOG_EXTRA_KEY, LOG_LEVEL_KEY
from vgi_rpc.rpc._reflection import ProtocolList, Reflection, ServiceDescription
from vgi_rpc.rpc._token_identity import Identity, IssuedGrant, TokenIdentity

if TYPE_CHECKING:
    import httpx2

#: Worker spawn plus several dozen loopback round trips.  The suite's own
#: 5s default is for single calls against an already-running server.
pytestmark = pytest.mark.timeout(30)

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

#: Names of the fixtures a runner must provide, quoted verbatim in every skip
#: so the message names the thing to go and build.
_IDENTITY_FIXTURE = "conformance_http_identity_port"
_NARROWED_FIXTURE = "conformance_http_identity_introspect_only_port"

#: The closed set of ``error_kind`` strings ``IDENTITY_V1_SPEC.md`` §3 puts on
#: the wire.  Spelled out rather than derived from the exception classes, so a
#: port silently renaming one in Python would still fail here.
_ERROR_KINDS = frozenset(
    {
        "introspection_refused",
        "token_unresolved",
        "stale_auth",
        "grant_refused",
        "identity_unavailable",
    }
)

#: The eight codepoints every port MUST trim before the JWS shape test
#: (``IDENTITY_V1_SPEC.md`` §4).  A port trimming a narrower set routes a
#: padded JWS that another port refuses, which is the same hole one level
#: down.  ``U+0085`` is excluded by JavaScript's and Java's idea of
#: whitespace, ``U+00A0`` by an ASCII-only hand-rolled matcher -- those two
#: are where the ports actually split, so they are not decoration.
_TRIM_FLOOR = (0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0)
_TRIM_FLOOR_IDS = ("tab", "lf", "vt", "ff", "cr", "space", "nel", "nbsp")

#: A credential of 5000 ASCII characters: over the 4096 cap in every unit any
#: port might have reached for.
_OVERSIZE_ASCII = "a" * 5000

#: 2100 U+00E9 -- 2100 codepoints, 2100 UTF-16 code units, **4200 UTF-8
#: bytes**.  Under the cap in codepoints and in UTF-16 units, over it in
#: bytes, which is the unit the spec pins.  The resolver would resolve it, so
#: a port measuring the other way answers 200 here and fails.
_OVERSIZE_MULTIBYTE = "é" * 2100

#: One character and nine thousand spaces.  Trimmed it is one byte long and
#: not JWS-shaped, so nothing but the cap can refuse it -- and the cap only
#: does if it measures the credential that arrived rather than the trimmed
#: form the shape test looks at.
_OVERSIZE_PADDED = "x" + " " * 9000


class _Rejected(Exception):
    """A call answered with a typed error rather than a result."""

    def __init__(self, kind: str | None, error_type: str, message: str) -> None:
        self.kind = kind
        self.error_type = error_type
        self.message = message
        super().__init__(f"{error_type}[{kind}]: {message}")


# ---------------------------------------------------------------------------
# Driving the two protocols over HTTP
# ---------------------------------------------------------------------------


def _request_body(protocol: type, method: str, kwargs: dict[str, Any]) -> bytes:
    """Serialize one unary request for *protocol*'s *method*."""
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._wire import _write_request

    info = rpc_methods(protocol)[method]
    buf = BytesIO()
    name = str(vars(protocol).get("protocol_name") or protocol.__name__)
    _write_request(buf, method, info.params_schema, kwargs, protocol=name)
    return buf.getvalue()


def _post(port: int, protocol: str, method: str, body: bytes, headers: dict[str, str]) -> httpx2.Response:
    """POST to whichever route prefix this worker serves."""
    import httpx2

    sent = {"content-type": _ARROW_CONTENT_TYPE, **headers}
    response = None
    for prefix in ("", "/vgi"):
        response = httpx2.post(
            f"http://127.0.0.1:{port}{prefix}/{protocol}/{method}",
            content=body,
            headers=sent,
            timeout=10.0,
        )
        if response.status_code != 404:
            return response
    assert response is not None
    return response


def _read_result(response: httpx2.Response, cls: type[Any]) -> Any:
    """Decode the single result payload, or raise :class:`_Rejected`.

    Raises:
        _Rejected: The response carried a typed error instead of a result.
        AssertionError: The response was neither.

    """
    assert response.status_code == 200, (
        f"a {IDENTITY_PROTOCOL_NAME} call answered HTTP {response.status_code}. Both a result and a "
        f"refusal ride the body as Arrow; the status line carries neither. Body: {response.content[:200]!r}"
    )
    reader = ipc.open_stream(BytesIO(response.content))
    payload: bytes | None = None
    while True:
        try:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        meta = {k.decode(): v for k, v in metadata.to_dict().items()} if metadata else {}
        if meta.get(LOG_LEVEL_KEY.decode(), b"").decode(errors="replace") == "EXCEPTION":
            extra = json.loads(meta.get(LOG_EXTRA_KEY.decode(), b"{}").decode())
            kind_raw = meta.get(ERROR_KIND_KEY.decode())
            raise _Rejected(
                kind_raw.decode() if kind_raw is not None else None,
                str(extra.get("exception_type", "")),
                str(extra.get("exception_message", "")),
            )
        if batch.num_rows and batch.schema.names == ["result"]:
            payload = batch.column(0)[0].as_py()
    assert payload is not None, "a successful call carried no result payload"
    return cls.deserialize_from_bytes(payload)


def _introspect(port: int, token: str, *, principal: str | None = INTROSPECTOR_PRINCIPAL) -> TokenIdentity:
    """Call ``introspect_token`` as *principal*, returning the identity.

    Raises:
        _Rejected: The worker refused.

    """
    headers = {} if principal is None else {PRINCIPAL_HEADER: principal}
    body = _request_body(Identity, "introspect_token", {"token": token})
    got = _read_result(_post(port, IDENTITY_PROTOCOL_NAME, "introspect_token", body, headers), TokenIdentity)
    assert isinstance(got, TokenIdentity)
    return got


def _issue(
    port: int,
    *,
    principal: str | None = MINTER_PRINCIPAL,
    auth_time: str | None = "fresh",
    purpose: str = "conformance",
    scopes: list[str] | None = None,
    ttl_seconds: int = 60,
) -> IssuedGrant:
    """Call ``issue_grant``, returning the grant.

    ``auth_time="fresh"`` sends a timestamp well inside the ceiling; ``None``
    omits the header entirely, which is the "credential carries no auth_time"
    case rather than a malformed one.

    Raises:
        _Rejected: The worker refused.

    """
    headers: dict[str, str] = {}
    if principal is not None:
        headers[PRINCIPAL_HEADER] = principal
    if auth_time == "fresh":
        headers[AUTH_TIME_HEADER] = str(time.time() - 60.0)
    elif auth_time is not None:
        headers[AUTH_TIME_HEADER] = auth_time
    body = _request_body(
        Identity,
        "issue_grant",
        {"purpose": purpose, "scopes": [] if scopes is None else scopes, "ttl_seconds": ttl_seconds},
    )
    got = _read_result(_post(port, IDENTITY_PROTOCOL_NAME, "issue_grant", body, headers), IssuedGrant)
    assert isinstance(got, IssuedGrant)
    return got


def _refusal(call: Any, *args: Any, **kwargs: Any) -> _Rejected:
    """Run *call*, requiring it to be refused.

    The failure message is the point: a guard that did not fire produces a
    *success*, and saying so beats ``DID NOT RAISE``.
    """
    try:
        result = call(*args, **kwargs)
    except _Rejected as exc:
        return exc
    raise AssertionError(
        f"the call succeeded and returned {result!r}. The conformance resolver answers for "
        f"anything it is handed, so reaching it at all means a guard did not fire."
    )


def _list_protocols(port: int) -> ProtocolList:
    """Read what a worker hosts, through ordinary reflection."""
    body = _request_body(Reflection, "list_protocols", {})
    response = _post(port, Reflection.protocol_name, "list_protocols", body, {})
    if response.status_code != 200:
        pytest.skip(f"worker does not host {Reflection.protocol_name} over HTTP ({response.status_code})")
    return _read_result(response, ProtocolList)  # type: ignore[no-any-return]


def _describe(port: int, protocol: str) -> ServiceDescription:
    """Read one protocol's full description, through ordinary reflection."""
    body = _request_body(Reflection, "describe", {"protocol": protocol})
    response = _post(port, Reflection.protocol_name, "describe", body, {})
    if response.status_code != 200:
        pytest.skip(f"worker does not host {Reflection.protocol_name} over HTTP ({response.status_code})")
    return _read_result(response, ServiceDescription)  # type: ignore[no-any-return]


def _identity_port(request: pytest.FixtureRequest) -> int:
    """Return the both-methods identity worker's port, or skip loudly."""
    try:
        return int(request.getfixturevalue(_IDENTITY_FIXTURE))
    except pytest.FixtureLookupError:
        pytest.skip(
            f"runner provides no {_IDENTITY_FIXTURE!r}. {IDENTITY_PROTOCOL_NAME} is nearly all guards "
            f"and every guard reads deployment policy, so it cannot be tested against a worker whose "
            f"allowlist, resolver and minter are unknown. Supply an HTTP worker configured exactly as "
            f"IDENTITY_CONFORMANCE_FIXTURE.md specifies."
        )


def _narrowed_port(request: pytest.FixtureRequest) -> int:
    """Return the introspect-only identity worker's port, or skip loudly."""
    try:
        return int(request.getfixturevalue(_NARROWED_FIXTURE))
    except pytest.FixtureLookupError:
        pytest.skip(
            f"runner provides no {_NARROWED_FIXTURE!r}. Method-level narrowing -- that an unconfigured "
            f"hook makes its method absent rather than hosted-and-refusing, and shrinks the "
            f"protocol_hash with it -- is only observable against a *second* worker configured with "
            f"one hook. It is the same binary with the mint hook left out; see "
            f"IDENTITY_CONFORMANCE_FIXTURE.md."
        )


# ---------------------------------------------------------------------------
# The wire shape, read back off a running server
# ---------------------------------------------------------------------------


class TestIdentityWireShape:
    """The three pinned digests, checked against a server rather than a literal.

    Every port asserts these digests in a unit test that computes one locally
    and compares it to a constant transcribed from the spec.  That proves the
    transcription happened.  It does not prove the server hosts the protocol
    that hashes to it, nor that the description it serves matches the methods
    it dispatches -- and those are the two things a client actually depends on.
    """

    def test_the_protocol_is_hosted_under_its_reserved_name(self, request: pytest.FixtureRequest) -> None:
        """Reflection lists it, so a client discovers it without guessing."""
        hosted = {p.protocol for p in _list_protocols(_identity_port(request)).protocols}
        assert IDENTITY_PROTOCOL_NAME in hosted, (
            f"a worker configured with identity hooks does not list {IDENTITY_PROTOCOL_NAME} in "
            f"reflection (hosts {sorted(hosted)}). Registration must come *after* reflection so the "
            f"protocol appears in its own server's output; a client that cannot discover it has to "
            f"call it to find out whether it exists."
        )

    def test_hosting_both_methods_produces_the_pinned_digest(self, request: pytest.FixtureRequest) -> None:
        """The both-methods hash, as the server reports it.

        Field declaration order, ``scopes``'s item nullability and the two
        result columns all feed this digest.  A port that gets any of them
        wrong differs here and nowhere else a client can see.
        """
        summaries = {p.protocol: p for p in _list_protocols(_identity_port(request)).protocols}
        summary = summaries.get(IDENTITY_PROTOCOL_NAME)
        assert summary is not None, f"{IDENTITY_PROTOCOL_NAME} is not hosted; see the previous case"
        assert summary.protocol_hash == IDENTITY_BOTH_METHODS_HASH, (
            f"a worker hosting both identity methods reports protocol_hash="
            f"{summary.protocol_hash!r}, not {IDENTITY_BOTH_METHODS_HASH!r}. The canonical preimage "
            f"is in IDENTITY_V1_SPEC.md §1 -- diff against it. The likeliest cause is scopes' list "
            f"item declared non-nullable (it is nullable), then field declaration order."
        )

    def test_describe_lists_exactly_the_two_methods(self, request: pytest.FixtureRequest) -> None:
        """Both methods are unary, both return, neither has a header."""
        description = _describe(_identity_port(request), IDENTITY_PROTOCOL_NAME)
        by_name = {m.name: m for m in description.methods}
        assert sorted(by_name) == ["introspect_token", "issue_grant"], (
            f"a both-methods identity worker describes {sorted(by_name)}. The description is what a "
            f"client routes on; a method it omits is one no client will call, and a method it invents "
            f"is a 404 a client will retry."
        )
        for name, method in by_name.items():
            assert method.method_type == "unary", f"{name} is described as {method.method_type!r}, not unary"
            assert method.has_return, f"{name} must declare has_return"
            assert not method.has_header, f"{name} must not declare a header"

    def test_issue_grant_takes_no_subject_parameter(self, request: pytest.FixtureRequest) -> None:
        """Cross-subject minting is closed by construction, not by a check.

        The subject of a grant is always the caller's authenticated principal.
        A parameter naming somebody else would make that a *check*, which is
        the kind of thing that is forgotten in one of six ports -- and the one
        port that forgot it would mint credentials for arbitrary users.

        Asserted against the parameter schema the server itself published,
        because a port could add the parameter to its handler without adding
        it to a hand-written description, or vice versa.
        """
        description = _describe(_identity_port(request), IDENTITY_PROTOCOL_NAME)
        method = next((m for m in description.methods if m.name == "issue_grant"), None)
        if method is None:
            pytest.skip("worker does not host issue_grant")
        schema = ipc.open_stream(BytesIO(method.params_schema_ipc)).schema
        assert schema.names == ["purpose", "scopes", "ttl_seconds"], (
            f"issue_grant publishes parameters {schema.names}. There is no subject parameter and must "
            f"not be one: the subject is the caller's authenticated principal, so cross-subject "
            f"minting is impossible rather than merely refused. Declaration order is part of the hash."
        )
        assert schema.field("scopes").type.field(0).nullable, (
            "scopes is list<item?: utf8> -- the list *item* is nullable. TypeScript shipped this "
            "wrong across every list type once; it changes the protocol_hash and nothing else."
        )

    def test_introspect_token_takes_one_opaque_credential(self, request: pytest.FixtureRequest) -> None:
        """One non-null utf8 parameter, named ``token``."""
        description = _describe(_identity_port(request), IDENTITY_PROTOCOL_NAME)
        method = next((m for m in description.methods if m.name == "introspect_token"), None)
        if method is None:
            pytest.skip("worker does not host introspect_token")
        schema = ipc.open_stream(BytesIO(method.params_schema_ipc)).schema
        assert schema.names == ["token"], f"introspect_token publishes parameters {schema.names}"
        assert not schema.field("token").nullable, "token is declared non-null"


class TestIdentityNarrowing:
    """A method whose hook is unconfigured is absent, and the hash narrows.

    Absent beats routed-and-refusing: it is what keeps a dependency upgrade
    from growing a credential-to-identity oracle on every existing worker.
    The two single-method digests in the spec exist to prove the narrowing
    actually narrows, and cannot be checked against a worker hosting both.
    """

    def test_the_narrowed_worker_reports_the_narrowed_digest(self, request: pytest.FixtureRequest) -> None:
        """One hook configured, one method hosted, the one-method digest."""
        summaries = {p.protocol: p for p in _list_protocols(_narrowed_port(request)).protocols}
        summary = summaries.get(IDENTITY_PROTOCOL_NAME)
        assert summary is not None, (
            f"the introspect-only worker does not host {IDENTITY_PROTOCOL_NAME} at all. Leaving the "
            f"mint hook unconfigured must drop one *method*, not the protocol."
        )
        assert summary.protocol_hash == IDENTITY_INTROSPECT_ONLY_HASH, (
            f"a worker hosting only introspect_token reports protocol_hash={summary.protocol_hash!r}, "
            f"not {IDENTITY_INTROSPECT_ONLY_HASH!r}. If it reports "
            f"{IDENTITY_BOTH_METHODS_HASH!r} the binding was built from the whole protocol rather "
            f"than from the configured methods, and the hash is describing a method the server will "
            f"not dispatch."
        )

    def test_the_unconfigured_method_is_absent_from_the_description(self, request: pytest.FixtureRequest) -> None:
        """A client learns from reflection, not by calling and reading an error."""
        description = _describe(_narrowed_port(request), IDENTITY_PROTOCOL_NAME)
        names = sorted(m.name for m in description.methods)
        assert names == ["introspect_token"], (
            f"the introspect-only worker describes {names}. A method whose hook the deployment did "
            f"not configure is not hosted at all -- describing it and then refusing every call makes "
            f"a deployment choice look like a runtime failure."
        )

    def test_the_unconfigured_method_does_not_mint(self, request: pytest.FixtureRequest) -> None:
        """Calling it cannot succeed, whatever shape the refusal takes.

        The transport-level shape is deliberately *not* pinned: an unrouted
        path is a 404 on one stack and a typed ``method_not_implemented`` on
        another, and both are honest. What is pinned is that no grant comes
        back from a worker that configured no minter.
        """
        port = _narrowed_port(request)
        body = _request_body(Identity, "issue_grant", {"purpose": "conformance", "scopes": [], "ttl_seconds": 60})
        headers = {PRINCIPAL_HEADER: MINTER_PRINCIPAL, AUTH_TIME_HEADER: str(time.time() - 60.0)}
        response = _post(port, IDENTITY_PROTOCOL_NAME, "issue_grant", body, headers)
        if response.status_code == 200:
            with pytest.raises(_Rejected):
                _read_result(response, IssuedGrant)
        else:
            assert response.status_code == 404, (
                f"calling an unhosted method answered HTTP {response.status_code}; expected the route "
                f"to be absent (404) or the call to be typed-refused (200 with an error batch)"
            )

    def test_narrowing_changes_the_hash(self, request: pytest.FixtureRequest) -> None:
        """The two workers must not describe themselves identically.

        Stated separately from the two digest assertions because it is the
        *property*: a port could pin both digests to the same wrong constant
        and pass them individually, and a port whose binding ignores the
        configured method set reports one hash for two different servers.
        """
        both = {p.protocol: p for p in _list_protocols(_identity_port(request)).protocols}
        narrowed = {p.protocol: p for p in _list_protocols(_narrowed_port(request)).protocols}
        assert both[IDENTITY_PROTOCOL_NAME].protocol_hash != narrowed[IDENTITY_PROTOCOL_NAME].protocol_hash, (
            "a worker hosting both identity methods and one hosting only introspect_token report the "
            "same protocol_hash. The hash is what a client compares to decide whether its cached "
            "description is still valid, so two different method sets sharing one hash means a client "
            "keeps calling a method that is no longer there."
        )


class TestIdentityAbsentByDefault:
    """A worker configuring no hook hosts no identity protocol at all.

    This is the property that lets the protocol exist in the framework without
    every deployment inheriting a credential-to-identity oracle at its next
    dependency upgrade.  It is asserted against the *plain* conformance worker,
    which configures nothing -- so unlike the rest of this module it needs no
    fixture and cannot be skipped.
    """

    def test_a_worker_with_no_hooks_does_not_host_identity(self, conformance_http_port: int) -> None:
        """Not hosted, not hosted-and-refusing."""
        hosted = {p.protocol for p in _list_protocols(conformance_http_port).protocols}
        assert IDENTITY_PROTOCOL_NAME not in hosted, (
            f"a worker that configured neither identity hook hosts {IDENTITY_PROTOCOL_NAME} "
            f"(hosts {sorted(hosted)}). Hosting it unconditionally means every worker in a fleet "
            f"grows a credential oracle the moment it upgrades the framework, whether or not anyone "
            f"asked for one. Registering a protocol that refuses every call is not a smaller version "
            f"of this -- the refusal is policy the deployment never expressed."
        )


# ---------------------------------------------------------------------------
# introspect_token
# ---------------------------------------------------------------------------


class TestIntrospectionHappyPath:
    """What an allowlisted reverse proxy gets back, field by field."""

    def test_an_allowlisted_caller_resolves_a_credential(self, request: pytest.FixtureRequest) -> None:
        """The whole identity travels, not just the principal."""
        got = _introspect(_identity_port(request), SUBJECT_TOKEN)
        assert got.principal == SUBJECT_PRINCIPAL, (
            f"resolved principal {got.principal!r}, expected {SUBJECT_PRINCIPAL!r}. The asker "
            f"authorizes with this, using credentials the worker does not hold."
        )
        assert got.token_name == SUBJECT_TOKEN_NAME, f"token_name {got.token_name!r}"
        assert got.ttl_seconds == SUBJECT_TTL, f"ttl_seconds {got.ttl_seconds!r}"

    def test_a_resolver_ttl_of_zero_is_not_coerced_to_the_default(self, request: pytest.FixtureRequest) -> None:
        """Zero means *do not cache this*, and must survive the wire as zero.

        Several ports have a zero value where the reference has an omitted
        column -- a Go struct literal, a Rust ``Default``, a C# ``default(T)``
        -- and the tempting fix is to normalise ``ttl_seconds <= 0`` up to the
        300 default.  ``ttl_seconds`` is how long the caller may cache the
        answer, which on any path the asker serves without re-presenting the
        credential is an authorization window and therefore the revocation
        lag.  Coercing zero converts a resolver saying "do not cache this"
        into five minutes of continued access after revocation, silently.
        """
        got = _introspect(_identity_port(request), TOKEN_ZERO_TTL)
        assert got.ttl_seconds == 0, (
            f"a resolver that returned ttl_seconds=0 was reported as {got.ttl_seconds!r}. A default is "
            f"for an omitted field, never a coercion applied to a value a hook actually set."
        )

    def test_omitted_fields_land_on_their_documented_defaults(self, request: pytest.FixtureRequest) -> None:
        """The other half of the same rule: an omitted field *does* default."""
        got = _introspect(_identity_port(request), TOKEN_MINIMAL)
        assert got.principal == SUBJECT_PRINCIPAL
        assert got.token_name == "", f"token_name defaults to the empty string, got {got.token_name!r}"
        assert got.ttl_seconds == 300, f"ttl_seconds defaults to 300, got {got.ttl_seconds!r}"

    def test_the_resolver_receives_the_credential_untrimmed(self, request: pytest.FixtureRequest) -> None:
        """Trimming is for the shape test only.

        The shape test runs on the trimmed credential while the resolver gets
        what the caller actually sent; rewriting a credential before resolving
        it would make the worker answer about a string nobody presented.  A
        port that trims once, up front, and resolves the result passes every
        other case in this module -- the padded credential still resolves,
        just to a different rule -- so the fixture gives the untrimmed form a
        distinguishable ``token_name`` and nothing else can see the difference.
        """
        got = _introspect(_identity_port(request), TOKEN_PADDED_PROBE)
        assert got.token_name == TOKEN_PADDED_PROBE_NAME, (
            f"a credential with surrounding whitespace resolved to token_name={got.token_name!r}, the "
            f"answer for its *trimmed* form. Trim for the shape test; hand the resolver the original."
        )


class TestIntrospectionAuthorization:
    """Authentication is not introspection, and the order of the two matters."""

    def test_an_unauthenticated_caller_is_refused(self, request: pytest.FixtureRequest) -> None:
        """No principal, no answer -- which is also what makes raw transports fail closed."""
        rejected = _refusal(_introspect, _identity_port(request), SUBJECT_TOKEN, principal=None)
        assert rejected.kind == "introspection_refused", (
            f"an unauthenticated caller was refused with error_kind={rejected.kind!r}. "
            f"Transports that carry no authenticated principal at all -- stdio, unix -- rely on this "
            f"to fail closed for free."
        )

    def test_an_authenticated_caller_off_the_allowlist_is_refused(self, request: pytest.FixtureRequest) -> None:
        """There is no permissive default, and holding *a* credential is not enough.

        "Any authenticated caller may introspect" lets any user resolve any
        other user's credential to its owner, and test guesses of it at
        whatever rate they like.
        """
        rejected = _refusal(_introspect, _identity_port(request), SUBJECT_TOKEN, principal=OUTSIDER_PRINCIPAL)
        assert rejected.kind == "introspection_refused", (
            f"an authenticated caller who is not on the introspector allowlist was refused with "
            f"error_kind={rejected.kind!r}, expected 'introspection_refused'. If it resolved, the "
            f"allowlist is not being consulted and every authenticated user is an introspector."
        )

    @pytest.mark.parametrize(
        ("label", "token"),
        [
            ("unknown", TOKEN_UNKNOWN),
            ("jws_shaped", TOKEN_JWS_TRAP),
            ("padded_jws", TOKEN_JWS_TRAP + "\n"),
            ("oversize", _OVERSIZE_ASCII),
            ("blank", "   "),
        ],
    )
    def test_authorization_precedes_every_credential_guard(
        self, request: pytest.FixtureRequest, label: str, token: str
    ) -> None:
        """An unauthorized caller learns nothing about the subject credential.

        Including how long looking at it took.  Steps 2 and 3 of the guard
        order come before step 4 for that reason and must not be reordered for
        tidiness.

        The test carries its own control.  C#'s equivalent asserted only the
        allowlist refusal, which fires first whatever the credential guards
        do, so despite its name it never touched them -- a test that passes
        for a reason its name disclaims, which reads as coverage in review.
        Here each credential is first shown to produce ``token_unresolved``
        for an *allowed* caller, so the second half is a statement about
        ordering rather than about the allowlist alone.
        """
        port = _identity_port(request)
        allowed = _refusal(_introspect, port, token, principal=INTROSPECTOR_PRINCIPAL)
        assert allowed.kind == "token_unresolved", (
            f"control failed: for an allowlisted caller the {label} credential was refused with "
            f"error_kind={allowed.kind!r}, not 'token_unresolved'. Without that this case cannot "
            f"distinguish an ordering property from the allowlist refusing everything."
        )
        refused = _refusal(_introspect, port, token, principal=OUTSIDER_PRINCIPAL)
        assert refused.kind == "introspection_refused", (
            f"an unauthorized caller presenting a {label} credential got error_kind={refused.kind!r}. "
            f"'token_unresolved' here means the credential guards ran before the authorization check: "
            f"the worker told a caller it had already decided not to serve something about somebody "
            f"else's credential."
        )


class TestTheJwsTrap:
    """A three-segment credential must never reach a resolver.

    Such a credential is validated locally against a key set.  Routing it
    onward hands a third party a bearer token the asker may itself have
    rejected -- expired, wrong audience -- to somebody who might accept it.

    Whitespace is how it gets past.  Five of six ports had a hole here and
    none of their own suites caught it, because the shape test was spelled
    with anchors and seven regex dialects disagree about what an anchor
    matches next to a newline.  The rule that survives translation is to trim
    first and then match, because trimming means the same thing everywhere --
    provided every port trims the same set, which is what the enumerated floor
    below is for.
    """

    def test_a_bare_jws_is_refused(self, request: pytest.FixtureRequest) -> None:
        """The unpadded case, which every port already had right."""
        rejected = _refusal(_introspect, _identity_port(request), TOKEN_JWS_TRAP)
        assert rejected.kind == "token_unresolved", f"error_kind={rejected.kind!r}"

    @pytest.mark.parametrize("codepoint", _TRIM_FLOOR, ids=_TRIM_FLOOR_IDS)
    @pytest.mark.parametrize("placement", ["leading", "trailing", "both"])
    def test_padding_does_not_smuggle_a_jws_past_the_guard(
        self, request: pytest.FixtureRequest, codepoint: int, placement: str
    ) -> None:
        """Every codepoint on the enumerated trim floor, on every side.

        The floor is enumerated rather than delegated to the language because
        "whitespace" is itself a divergence one layer down: JavaScript's and
        Java's ``isWhitespace`` exclude ``U+0085``, and an ASCII-only
        hand-rolled matcher excludes ``U+00A0`` as well.  A port trimming a
        narrower set routes a padded JWS that another port refuses, which is
        the same hole one level down.  Trimming *wider* is safe -- it can only
        add refusals -- so this pins a floor, not an exact set.
        """
        pad = chr(codepoint)
        token = {"leading": pad + TOKEN_JWS_TRAP, "trailing": TOKEN_JWS_TRAP + pad, "both": pad + TOKEN_JWS_TRAP + pad}[
            placement
        ]
        rejected = _refusal(_introspect, _identity_port(request), token)
        assert rejected.kind == "token_unresolved", (
            f"a JWS-shaped credential with U+{codepoint:04X} padding ({placement}) resolved or was "
            f"refused as {rejected.kind!r}. The conformance resolver answers for anything, so this "
            f"credential reaching it means the shape test ran against the untrimmed string, or the "
            f"trim set omits U+{codepoint:04X}."
        )

    def test_repeated_padding_is_also_refused(self, request: pytest.FixtureRequest) -> None:
        """Two newlines, because the reference once refused one and admitted two.

        That asymmetry was an artifact of Python's ``$`` matching before a
        single trailing newline -- an accident, not a rule, and one that other
        ports could not reproduce even if they wanted to.
        """
        rejected = _refusal(_introspect, _identity_port(request), TOKEN_JWS_TRAP + "\n\n")
        assert rejected.kind == "token_unresolved", f"error_kind={rejected.kind!r}"

    @pytest.mark.parametrize("token", ["opaque-token", "two.segments", "a.b.c.d", "sk_live_abc123"])
    def test_an_ordinary_credential_still_reaches_the_resolver(
        self, request: pytest.FixtureRequest, token: str
    ) -> None:
        """The guard must discriminate.

        Trimming first tightens the shape test, and a guard that refuses
        everything is indistinguishable from one that works until the day a
        real credential shows up.  Two and four segments are both *not* a JWS.
        """
        got = _introspect(_identity_port(request), token)
        assert got.principal == SUBJECT_PRINCIPAL, (
            f"the opaque credential {token!r} did not reach the resolver. Two segments and four "
            f"segments are not JWS shapes; a guard that refuses them refuses ordinary API keys."
        )


class TestTheCredentialSizeCap:
    """4096 UTF-8 bytes, measured on the credential that arrived.

    This is one of exactly two guards whose refusal collapses into the uniform
    ``token_unresolved`` answer, so it is one of exactly two that can be tested
    vacuously -- an over-long credential is also an unknown one.  Every case
    here uses a credential the conformance resolver *would* resolve, so
    deleting the cap turns these red rather than leaving them green.
    """

    def test_an_oversized_credential_never_reaches_the_resolver(self, request: pytest.FixtureRequest) -> None:
        """Refusing early keeps a resolver from being handed megabytes."""
        rejected = _refusal(_introspect, _identity_port(request), _OVERSIZE_ASCII)
        assert rejected.kind == "token_unresolved", (
            f"a {len(_OVERSIZE_ASCII)}-character credential resolved or was refused as "
            f"{rejected.kind!r}. The conformance resolver answers for anything, so a success here "
            f"means the cap is not on the dispatch path."
        )

    def test_the_cap_is_measured_in_utf8_bytes(self, request: pytest.FixtureRequest) -> None:
        """The unit is bytes, not codepoints and not UTF-16 code units.

        The ports reached for all three.  They agree for an ASCII credential,
        which every real bearer token is, so the divergence is invisible until
        somebody sends a multibyte one -- and then the fleet disagrees about
        what it will even look at.  Bytes is the unit the purpose implies
        (what a resolver would have to handle) and the most conservative of the
        three, so standardising on it can only refuse earlier.

        This credential is 2100 codepoints and 2100 UTF-16 code units -- both
        under the cap -- and 4200 UTF-8 bytes, which is over it.
        """
        assert len(_OVERSIZE_MULTIBYTE) < 4096 < len(_OVERSIZE_MULTIBYTE.encode("utf-8"))
        rejected = _refusal(_introspect, _identity_port(request), _OVERSIZE_MULTIBYTE)
        assert rejected.kind == "token_unresolved", (
            f"a credential of {len(_OVERSIZE_MULTIBYTE)} codepoints / "
            f"{len(_OVERSIZE_MULTIBYTE.encode('utf-8'))} UTF-8 bytes was accepted. It is under the "
            f"4096 cap in codepoints and in UTF-16 code units and over it in bytes, and bytes is the "
            f"pinned unit."
        )

    def test_the_cap_applies_to_the_untrimmed_credential(self, request: pytest.FixtureRequest) -> None:
        """Padding must not be talked down into the allowance.

        Splitting "trim for the shape test" from "measure what arrived"
        creates a new way to get this wrong, and nothing else would catch it:
        this credential trims to a single character, so only the cap can
        refuse it, and only if it measures the original.
        """
        assert len(_OVERSIZE_PADDED.strip()) < 4096 < len(_OVERSIZE_PADDED.encode("utf-8"))
        rejected = _refusal(_introspect, _identity_port(request), _OVERSIZE_PADDED)
        assert rejected.kind == "token_unresolved", (
            f"a credential that is {len(_OVERSIZE_PADDED)} bytes long but trims to "
            f"{len(_OVERSIZE_PADDED.strip())} was accepted. The cap applies to what arrived -- that is "
            f"what a resolver would have to handle -- not to the trimmed form the shape test looks at."
        )

    @pytest.mark.parametrize("token", ["", " ", "   ", "\n", "\t\r\n"], ids=["empty", "space", "spaces", "lf", "mixed"])
    def test_a_blank_credential_is_not_a_credential(self, request: pytest.FixtureRequest, token: str) -> None:
        """Whitespace-only never reaches a resolver either."""
        rejected = _refusal(_introspect, _identity_port(request), token)
        assert rejected.kind == "token_unresolved", f"blank credential {token!r} gave {rejected.kind!r}"


class TestRejectionsAreUniform:
    """Unknown, malformed, over-long and blank are one answer.

    Reporting which would confirm that a guessed credential exists, which is
    the whole reason an introspection endpoint is dangerous at all.  Uniformity
    is what makes the guard tests above need their resolvable-probe shape, so
    it is worth asserting rather than assumed.
    """

    _CAUSES: ClassVar[dict[str, str]] = {
        "unknown": TOKEN_UNKNOWN,
        "jws_shaped": TOKEN_JWS_TRAP,
        "padded_jws": TOKEN_JWS_TRAP + "\n",
        "oversize": _OVERSIZE_ASCII,
        "blank": "   ",
    }

    def test_every_cause_produces_the_same_answer(self, request: pytest.FixtureRequest) -> None:
        """Same kind and same error type, whatever the reason."""
        port = _identity_port(request)
        seen = {label: _refusal(_introspect, port, token) for label, token in self._CAUSES.items()}
        kinds = {label: r.kind for label, r in seen.items()}
        assert set(kinds.values()) == {"token_unresolved"}, (
            f"rejection causes answered with different error_kind values: {kinds}. Unknown, expired "
            f"and malformed are one answer; distinguishing them lets a caller confirm that a guessed "
            f"credential exists."
        )
        types = {label: r.error_type for label, r in seen.items()}
        assert len(set(types.values())) == 1, (
            f"rejection causes answered with different error types: {types}. A client that catches on "
            f"type sees a difference the error_kind was carefully hiding."
        )
        messages = {label: r.message for label, r in seen.items()}
        assert len(set(messages.values())) == 1, (
            f"rejection causes answered with different messages: {messages}. The message is on the "
            f"wire too, so a cause named there is a cause the caller can read: 'credential too long' "
            f"against 'unresolved' tells an attacker probing a stolen credential which of its guesses "
            f"were even the right shape. Checking only error_kind lets this through -- it is the same "
            f"assertion-narrower-than-its-name failure this group exists to catch. One consequence is "
            f"deliberate: nothing credential-derived may appear in a refusal message, digests "
            f"included. Diagnostics are what the digest is for."
        )

    @pytest.mark.parametrize(
        ("label", "token", "principal"),
        [
            # JWS-shaped, so the framework guard refuses it before the resolver
            # runs: the message under test is the guard's own.
            ("guard", "conformance-must-not-echo-84c1f0.padding.signature", INTROSPECTOR_PRINCIPAL),
            # Refused by the resolver, so the message under test is the one the
            # dispatch path writes around a hook's "unknown" answer.
            ("resolver", TOKEN_UNKNOWN, INTROSPECTOR_PRINCIPAL),
            # Refused by the authorization check, which never looks at the
            # credential at all -- and so must not report it either.
            ("authorization", "conformance-must-not-echo-5b90ac", OUTSIDER_PRINCIPAL),
        ],
    )
    def test_a_rejection_never_echoes_the_credential(
        self, request: pytest.FixtureRequest, label: str, token: str, principal: str
    ) -> None:
        """The credential must not reach a log, a span, or an error message.

        An error message travels further than a log does -- to the caller, and
        from there into the caller's own logs, which may be a different trust
        domain entirely.  A digest is what diagnostics get.

        Parametrised over the three paths that can refuse, because they write
        their messages in three different places.  An earlier version of this
        probed only the authorization path, which refuses without ever looking
        at the credential -- so it could not have echoed one, and the test
        passed against a build that echoed the credential from every other
        path.  A mutation run is what surfaced that; the name had read as
        coverage for as long as it existed.
        """
        rejected = _refusal(_introspect, _identity_port(request), token, principal=principal)
        assert token not in rejected.message, (
            f"the {label} refusal message contains the credential verbatim: {rejected.message!r}. Use "
            f"a SHA-256 digest for diagnostics -- stable enough to correlate one credential's "
            f"failures, without being the credential."
        )


class TestUnavailableIsTransient:
    """A store outage is not a flavour of an unknown credential."""

    def test_an_unknowable_answer_has_its_own_kind(self, request: pytest.FixtureRequest) -> None:
        """A caller that negative-caches a rejection must not cache an outage.

        These used to be an HTTP route whose callers classified
        definitive-versus-transient on the status code (404 against 503).  As
        protocol methods every handler exception surfaces the same way, so
        ``error_kind`` is the *only* signal left.  A caller that
        negative-caches a transient failure locks out valid users for as long
        as the cache holds; one that retries a definitive rejection hammers
        the worker.
        """
        port = _identity_port(request)
        transient = _refusal(_introspect, port, TOKEN_UNAVAILABLE)
        assert transient.kind == "identity_unavailable", (
            f"a resolver that could not reach its backing store surfaced as "
            f"error_kind={transient.kind!r}. If that is 'token_unresolved', every caller that "
            f"negative-caches an unknown credential has just cached a thirty-second outage."
        )
        definitive = _refusal(_introspect, port, TOKEN_UNKNOWN)
        assert transient.kind != definitive.kind, (
            "a store outage and an unknown credential report the same error_kind, so no caller can tell them apart"
        )
        assert transient.error_type != definitive.error_type, (
            f"a store outage is reported as {transient.error_type!r}, the same type as a definitive "
            f"rejection. It must not be catchable as the port's 'invalid argument' or 'not my "
            f"credential' type: an authenticate chain that advances on that type reads an outage as "
            f"'try the next authenticator' and turns a blip into a fleet-wide re-login."
        )


# ---------------------------------------------------------------------------
# issue_grant
# ---------------------------------------------------------------------------


class TestGrantIssuance:
    """Minting is about the caller, so it is guarded differently on purpose.

    No allowlist and no rate limit, because it is not an oracle about anybody
    else -- and rejections that are deliberately *actionable*, because a
    console that cannot tell "your login is too old" from "no" cannot know to
    re-prompt.
    """

    def test_a_freshly_authenticated_caller_mints_a_grant(self, request: pytest.FixtureRequest) -> None:
        """Every field of the grant travels."""
        got = _issue(_identity_port(request), scopes=["read", "write"])
        assert got.token == f"{GRANT_TOKEN_PREFIX}{MINTER_PRINCIPAL}{SCOPE_SEPARATOR}read,write", (
            f"minted token {got.token!r}. The fixture builds it from the principal the framework "
            f"passed the hook and the scopes it received."
        )
        assert got.expires_at == GRANT_EXPIRES_AT, (
            f"expires_at {got.expires_at!r}, expected {GRANT_EXPIRES_AT!r} exactly -- a float64 that "
            f"does not round-trip is a lifetime nobody agreed to"
        )
        assert got.grant_id == GRANT_ID, f"grant_id {got.grant_id!r}"

    def test_the_subject_is_the_caller(self, request: pytest.FixtureRequest) -> None:
        """Two callers making identical requests get two different grants.

        The complement of the schema assertion: the description says there is
        no subject parameter, and this says the framework passes the *caller's*
        authenticated principal to the hook rather than a constant, the
        primary's principal, or whatever it last saw.
        """
        port = _identity_port(request)
        mine = _issue(port, principal=MINTER_PRINCIPAL)
        theirs = _issue(port, principal=OTHER_MINTER_PRINCIPAL)
        assert mine.token != theirs.token, (
            f"two different callers minted the same grant ({mine.token!r}). The framework must pass "
            f"the caller's own authenticated principal to the mint hook."
        )
        assert mine.token.startswith(f"{GRANT_TOKEN_PREFIX}{MINTER_PRINCIPAL}"), f"minted {mine.token!r}"
        assert theirs.token.startswith(f"{GRANT_TOKEN_PREFIX}{OTHER_MINTER_PRINCIPAL}"), f"minted {theirs.token!r}"

    @pytest.mark.parametrize("scopes", [[], ["read"], ["read", "write", "admin"]], ids=["empty", "one", "three"])
    def test_scopes_round_trip(self, request: pytest.FixtureRequest, scopes: list[str]) -> None:
        """The list reaches the hook intact, empty included.

        An empty list and a null list are different values that a port with
        loose list handling will render identically, and an off-by-one in list
        offsets shows up at zero elements before it shows up anywhere else.
        """
        got = _issue(_identity_port(request), scopes=scopes)
        _, _, echoed = got.token.partition(SCOPE_SEPARATOR)
        assert echoed == ",".join(scopes), f"sent scopes={scopes!r}, the hook saw {echoed!r}"

    def test_grant_id_defaults_to_empty(self, request: pytest.FixtureRequest) -> None:
        """A correlation handle is optional; a lifetime is not."""
        got = _issue(_identity_port(request), purpose=MINIMAL_PURPOSE)
        assert got.grant_id == "", f"grant_id defaults to the empty string, got {got.grant_id!r}"
        assert got.expires_at == GRANT_EXPIRES_AT, "expires_at has no default -- a minter must state one"

    def test_the_worker_may_refuse_to_mint(self, request: pytest.FixtureRequest) -> None:
        """The worker holds the policy; the framework only asked."""
        rejected = _refusal(_issue, _identity_port(request), purpose=REFUSED_PURPOSE)
        assert rejected.kind == "grant_refused", (
            f"a minter that refused surfaced as error_kind={rejected.kind!r}. 'grant_refused' is "
            f"definitive and distinct from the freshness refusals, which are the caller's to fix."
        )


class TestGrantFreshness:
    """Only a recently authenticated *human* may mint.

    Requiring ``auth_time`` is what stops a grant minting another grant: a
    grant is not IdP-issued, so it carries no ``auth_time``, so the lineage
    cannot escape the identity provider.  It also makes transports with no
    authenticated principal fail closed for free, and refuses a static bearer
    -- which proves a machine holds a secret, never that a human just
    authenticated.
    """

    def test_a_caller_with_no_auth_time_is_refused(self, request: pytest.FixtureRequest) -> None:
        """Authenticated is not the same as recently authenticated."""
        rejected = _refusal(_issue, _identity_port(request), auth_time=None)
        assert rejected.kind == "stale_auth", (
            f"an authenticated caller whose credential carries no auth_time minted with "
            f"error_kind={rejected.kind!r}. This is the check that keeps a minted grant from being "
            f"presented back to mint another one, escaping the IdP permanently."
        )

    def test_an_unparseable_auth_time_is_refused(self, request: pytest.FixtureRequest) -> None:
        """A claim that is present but not a number is not a freshness proof.

        Requires the fixture to pass the header through verbatim: one that
        parses and drops what will not parse turns this into the no-claim case
        above, and the property goes untested while the test stays green.
        """
        rejected = _refusal(_issue, _identity_port(request), auth_time="not-a-timestamp")
        assert rejected.kind == "stale_auth", f"error_kind={rejected.kind!r}"

    def test_a_stale_auth_time_is_refused(self, request: pytest.FixtureRequest) -> None:
        """Older than the configured ceiling, so re-authenticate."""
        stale = str(time.time() - (MAX_AUTH_AGE * 96))
        rejected = _refusal(_issue, _identity_port(request), auth_time=stale)
        assert rejected.kind == "stale_auth", (
            f"a caller who last authenticated a day ago minted a grant (error_kind={rejected.kind!r}) "
            f"against a {MAX_AUTH_AGE:.0f}s ceiling"
        )

    def test_a_fresh_auth_time_is_accepted(self, request: pytest.FixtureRequest) -> None:
        """The control.

        Without it every case above passes against a worker that refuses every
        mint, which is the failure mode a freshness test is most likely to
        have: refusing is the safe direction, so nothing else complains.
        """
        got = _issue(_identity_port(request), auth_time=str(time.time() - 60.0))
        assert got.token.startswith(GRANT_TOKEN_PREFIX), f"minted {got.token!r}"

    def test_an_unauthenticated_caller_cannot_mint(self, request: pytest.FixtureRequest) -> None:
        """No principal, nothing to mint for."""
        rejected = _refusal(_issue, _identity_port(request), principal=None, auth_time=None)
        assert rejected.kind == "stale_auth", f"error_kind={rejected.kind!r}"


class TestErrorKindsReachTheWire:
    """All five, from one worker, in one place.

    ``error_kind`` is the only signal a caller has for classifying a failure,
    and the classification decides whether the caller caches the answer or
    retries it.  Each kind is asserted at its own site above; this drives one
    probe per kind and checks the *set*, which is the assertion that fails
    when a port surfaces four of the five and silently folds the fifth into a
    generic error.
    """

    def test_all_five_kinds_are_observable(self, request: pytest.FixtureRequest) -> None:
        """One call per kind, then compare the whole set."""
        port = _identity_port(request)
        observed = {
            "introspection_refused": _refusal(_introspect, port, SUBJECT_TOKEN, principal=OUTSIDER_PRINCIPAL).kind,
            "token_unresolved": _refusal(_introspect, port, TOKEN_UNKNOWN).kind,
            "identity_unavailable": _refusal(_introspect, port, TOKEN_UNAVAILABLE).kind,
            "stale_auth": _refusal(_issue, port, auth_time=None).kind,
            "grant_refused": _refusal(_issue, port, purpose=REFUSED_PURPOSE).kind,
        }
        assert set(observed.values()) == _ERROR_KINDS, (
            f"the five error kinds did not all reach the wire: {observed}. Each key is the kind the "
            f"spec's error table requires for that probe; each value is what the worker actually "
            f"sent. A missing or renamed kind leaves a caller unable to tell a definitive rejection "
            f"it may cache from a transient one it must retry."
        )
        for expected, got in observed.items():
            assert got == expected, f"expected error_kind={expected!r}, got {got!r}"
