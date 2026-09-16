# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""External-location conformance over the persistent byte-stream transports.

``TestExternalInputRoutes`` and its neighbours in
:mod:`vgi_rpc.conformance._external_pytest` drive externalisation over HTTP,
because they need a raw HTTP driver to place a pointer batch on an inbound
route.  Nothing there touches pipe, subprocess, Unix-socket or TCP framing —
yet those transports resolve pointers too, through the very same
``resolve_external_location`` call that HTTP uses.

That asymmetry has already cost two ports the same two defects:

* the continuation cursor read off the **outer pointer batch** rather than the
  **inner data batch**, which works right up until a stream needs a second
  turn; and
* log batches silently dropped from a multi-batch externalised payload,
  because the reader stopped at the first data batch in the fetched stream.

An externalised cycle is a whole IPC *stream* — this turn's log batches
followed by its single data batch — and the pointer that replaces it on the
wire carries only ``vgi_rpc.location`` and ``vgi_rpc.location.sha256``.
Everything else a reader needs is *inside* the fetched object.  This group
pins that shape on a byte-stream transport, where nothing pinned it before.

Runners supply a ``conformance_bytestream_external_target`` fixture returning
a :class:`ByteStreamExternalTarget`.  A runner that implements no external
locations at all (no ``conformance_fake_storage`` either) skips the group; a
runner that has a pointer resolver and withholds the fixture *fails* it,
because withholding is how this half stayed untested in the first place.  See
:func:`_target`.

.. note:: **Not skipped on Windows**, unlike the ``http_externalize_always``
   transport variant.  That skip is for a waitress/httpx2 socket race on the
   *RPC* response: waitress closes the response socket before httpx2 drains
   the body, and the upload-URL bootstrap multiplies the request cycles that
   make it fire.  Nothing here serves RPC over HTTP — the transport is a byte
   stream, and the only HTTP traffic is storage upload/download, which the
   existing ``TestExternalInputRoutes`` group already runs unskipped on
   Windows.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, fields
from types import TracebackType
from typing import Protocol, Self

import pytest

from vgi_rpc.conformance._types import RichHeader, Status, build_dynamic_schema, build_rich_header
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import LOCATION_KEY, LOCATION_SHA256_KEY, LOCATION_SOURCE_KEY
from vgi_rpc.rpc import AnnotatedBatch, RpcError

from ._types import ANNOTATED_EMIT_LABEL

pytestmark = pytest.mark.timeout(30)

#: Storage objects one call is expected to produce, at minimum.  Deliberately
#: ``1`` rather than an exact count: a port may bundle a turn's logs with its
#: data batch into one object (the reference does) or upload more than one.
#: What must never happen is *zero* — that is the trivially-passing variant
#: this group exists to rule out.
_MIN_UPLOADS_PER_CALL = 1


class ExternalExchangeSession(Protocol):
    """Minimum exchange-stream surface this group drives."""

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...


class ExternalHeaderStream(Protocol):
    """Minimum header-carrying producer-stream surface this group drives."""

    @property
    def header(self) -> object: ...

    def __iter__(self) -> Iterator[AnnotatedBatch]: ...


class ByteStreamExternalConnection(Protocol):
    """The conformance methods this group calls, and nothing more.

    Spelled out rather than typed as ``Any`` so a runner wiring the fixture
    up can see exactly which slice of ``ConformanceService`` must work over
    an externalising byte-stream connection.
    """

    def echo_bytes(self, *, data: bytes) -> bytes: ...

    def echo_int(self, *, value: int) -> int: ...

    def echo_dict_encoded_string(self, *, value: str) -> str: ...

    def echo_enum(self, *, status: Status) -> Status: ...

    def echo_status_list(self, *, statuses: list[Status]) -> list[Status]: ...

    def oversized_unary(self, *, target_bytes: int) -> bytes: ...

    def produce_n(self, *, count: int) -> Iterable[AnnotatedBatch]: ...

    def produce_with_logs(self, *, count: int) -> Iterable[AnnotatedBatch]: ...

    def produce_annotated_batches(self, *, count: int, rows_per_batch: int) -> Iterable[AnnotatedBatch]: ...

    def produce_error_mid_stream(self, *, emit_before_error: int) -> Iterable[AnnotatedBatch]: ...

    def produce_large_batches(self, *, rows_per_batch: int, batch_count: int) -> Iterable[AnnotatedBatch]: ...

    def produce_with_rich_header(self, *, seed: int, count: int) -> ExternalHeaderStream: ...

    def produce_dynamic_schema(
        self,
        *,
        seed: int,
        count: int,
        include_strings: bool,
        include_floats: bool,
    ) -> ExternalHeaderStream: ...

    def exchange_scale(self, *, factor: float) -> ExternalExchangeSession: ...

    def exchange_with_logs(self) -> ExternalExchangeSession: ...


@dataclass(frozen=True, slots=True)
class ByteStreamExternalTarget:
    """A byte-stream connection whose responses are externalised.

    Attributes:
        name: Human-readable identifier for the configuration, used in
            assertion messages (e.g. ``"python-pipe"``).
        connect: Returns a fresh context-managed conformance proxy.  Called
            with an optional ``on_log`` keyword, exactly like the
            ``conformance_conn`` factory.  Both ends of the connection must
            be wired to the same external storage, over one of the
            persistent byte-stream transports (pipe, subprocess, Unix
            socket, or TCP) — **not** HTTP, which
            :mod:`vgi_rpc.conformance._external_pytest` already covers.
        uploaded_objects: Returns how many objects the backing storage has
            accepted so far.  Monotonically non-decreasing.  This is what
            makes "it really externalised" checkable rather than assumed.

    The server's externalisation threshold must be low enough that every
    data-bearing batch in this suite goes through storage — one byte is the
    reference setting.  Every test below asserts an upload happened, so a
    threshold that quietly leaves batches inline fails the group loudly
    instead of passing it vacuously.

    """

    name: str
    connect: Callable[..., AbstractContextManager[ByteStreamExternalConnection]]
    uploaded_objects: Callable[[], int]


def _target(request: pytest.FixtureRequest) -> ByteStreamExternalTarget:
    """Return the runner's target, or decide between skip and fail.

    External locations are optional: a port that implements none of the
    protocol skips this group, as it skips the HTTP external groups.  But a
    port that *does* implement pointer resolution has exactly one resolver,
    and its byte-stream client calls it — so withholding this fixture hides
    the half that has already broken twice.  That is an error, not a skip,
    on the same reasoning ``docs/sticky-sessions-spec.md`` §9.1 applies to
    the sticky failure paths.

    ``conformance_fake_storage`` is the discriminator because supplying it is
    what makes any of the external-location groups runnable at all.
    """
    try:
        target = request.getfixturevalue("conformance_bytestream_external_target")
    except pytest.FixtureLookupError:
        pass
    else:
        assert isinstance(target, ByteStreamExternalTarget)
        return target
    try:
        request.getfixturevalue("conformance_fake_storage")
    except pytest.FixtureLookupError:
        pytest.skip("runner implements no external locations — byte-stream externalization N/A")
    pytest.fail(
        "runner supplies external storage (conformance_fake_storage) but no "
        "'conformance_bytestream_external_target' fixture, so its pointer resolver is "
        "only ever exercised over HTTP. Supply a byte-stream connection with "
        "externalization wired on both ends — see "
        "docs/cross-language-conformance.md, 'Byte-stream externalization contract'."
    )


def _assert_uploaded(target: ByteStreamExternalTarget, before: int, what: str) -> None:
    """Require that the call under test actually pushed bytes through storage."""
    after = target.uploaded_objects()
    assert after - before >= _MIN_UPLOADS_PER_CALL, (
        f"{target.name}: {what} produced {after - before} storage uploads; "
        f"this group is meaningless unless responses are externalised — "
        f"lower the fixture's externalize threshold"
    )


def _assert_resolved_metadata(metadata: object, what: str) -> None:
    """Require resolved metadata to be the inner payload's, not the pointer's.

    ``vgi_rpc.location`` naming a batch that has already been fetched means
    the reader handed the caller the *pointer's* metadata — the defect that
    loses a stream's continuation cursor the moment a second turn needs it.
    ``vgi_rpc.location.source`` is the provenance key
    ``docs/WIRE_PROTOCOL.md`` §12 requires on every resolved batch, so its
    absence means the reader dropped the inner metadata wholesale.
    """
    assert metadata is not None, f"{what}: resolved batch carried no custom metadata"
    get = getattr(metadata, "get", None)
    assert callable(get), f"{what}: custom metadata is not a key-value mapping"
    assert get(LOCATION_KEY) is None, (
        f"{what}: resolved batch still carries {LOCATION_KEY.decode()} — "
        f"the reader returned the pointer batch's metadata instead of the "
        f"fetched payload's, which drops any continuation cursor with it"
    )
    assert get(LOCATION_SHA256_KEY) is None, (
        f"{what}: resolved batch still carries {LOCATION_SHA256_KEY.decode()} — pointer-only metadata leaked through"
    )
    assert get(LOCATION_SOURCE_KEY) is not None, (
        f"{what}: resolved batch is missing {LOCATION_SOURCE_KEY.decode()} provenance"
    )


def _assert_rich_header(actual: RichHeader, seed: int) -> None:
    """Assert every ``RichHeader`` field survived the round trip.

    Spelled as a field walk rather than the suite's explicit list because
    the question here is narrower: whether externalising the header batch
    lost anything, not whether the seed mapping is right (which
    ``TestDynamicRichHeader`` already pins on every transport).
    """
    expected = build_rich_header(seed)
    for field in fields(expected):
        got = getattr(actual, field.name)
        want = getattr(expected, field.name)
        if isinstance(want, float):
            assert got == pytest.approx(want), field.name
        else:
            assert got == want, field.name


class TestExternalByteStream:
    """Externalised payloads survive a persistent byte-stream transport."""

    def test_unary_result_really_goes_through_storage(self, request: pytest.FixtureRequest) -> None:
        """A unary result round-trips, and storage saw the object."""
        target = _target(request)
        payload = b"\xc3\xa9" * 4096
        with target.connect() as proxy:
            before = target.uploaded_objects()
            assert proxy.echo_bytes(data=payload) == payload
            _assert_uploaded(target, before, "echo_bytes")

    def test_resolved_metadata_is_the_payloads_not_the_pointers(self, request: pytest.FixtureRequest) -> None:
        """Every resolved batch carries the inner payload's metadata.

        This is the cursor defect in its transport-independent form: a
        reader that hands back the pointer's metadata has thrown away
        whatever the writer attached to the data batch.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            batches = list(proxy.produce_n(count=3))
            _assert_uploaded(target, before, "produce_n")
            assert len(batches) == 3
            for index, annotated in enumerate(batches):
                _assert_resolved_metadata(annotated.custom_metadata, f"produce_n batch {index}")

    def test_log_batches_survive_a_multi_batch_payload(self, request: pytest.FixtureRequest) -> None:
        """Logs bundled into an externalised cycle still reach ``on_log``.

        The uploaded object holds this turn's log batches *and* its data
        batch.  A reader that stops at the first data batch never dispatches
        the logs, and the loss is invisible to every assertion that only
        looks at data.
        """
        target = _target(request)
        logs: list[Message] = []
        with target.connect(on_log=logs.append) as proxy:
            before = target.uploaded_objects()
            batches = list(proxy.produce_with_logs(count=3))
            _assert_uploaded(target, before, "produce_with_logs")
        assert len(batches) == 3
        assert len(logs) == 3, f"{target.name}: externalised cycle dropped log batches"
        for index, log in enumerate(logs):
            assert log.level == Level.INFO
            assert str(index) in log.message

    def test_exchange_log_batches_survive_a_multi_batch_payload(self, request: pytest.FixtureRequest) -> None:
        """The same holds for an exchange turn's logs."""
        target = _target(request)
        logs: list[Message] = []
        with target.connect(on_log=logs.append) as proxy, proxy.exchange_with_logs() as session:
            before = target.uploaded_objects()
            result = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0, 3.0]}))
            _assert_uploaded(target, before, "exchange_with_logs")
            assert result.batch.num_rows == 3
        assert logs, f"{target.name}: externalised exchange cycle dropped log batches"

    def test_producer_stream_values_round_trip(self, request: pytest.FixtureRequest) -> None:
        """A multi-turn producer stream survives, turn after turn.

        Repeated turns are the point: a reader that loses per-batch state on
        the first externalised turn is still correct on turn one.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            batches = list(proxy.produce_n(count=5))
            _assert_uploaded(target, before, "produce_n")
        assert [ab.batch.column("value")[0].as_py() for ab in batches] == [0, 10, 20, 30, 40]
        assert [ab.batch.column("index")[0].as_py() for ab in batches] == [0, 1, 2, 3, 4]

    def test_exchange_stream_round_trips_across_turns(self, request: pytest.FixtureRequest) -> None:
        """Successive exchange turns each resolve their own payload."""
        target = _target(request)
        with target.connect() as proxy, proxy.exchange_scale(factor=3.0) as session:
            before = target.uploaded_objects()
            first = session.exchange(AnnotatedBatch.from_pydict({"value": [1.0, 2.0]}))
            assert first.batch.column("value").to_pylist() == pytest.approx([3.0, 6.0])
            second = session.exchange(AnnotatedBatch.from_pydict({"value": [4.0]}))
            assert second.batch.column("value").to_pylist() == pytest.approx([12.0])
            _assert_uploaded(target, before, "exchange_scale")
            _assert_resolved_metadata(second.custom_metadata, "exchange_scale turn 2")

    def test_per_batch_metadata_survives_externalization(self, request: pytest.FixtureRequest) -> None:
        """Per-emit custom metadata and externalization must compose.

        This is the one place the two meet, and two ports shipped opposite
        defects there -- one refusing to externalize any batch that carries
        metadata, the other externalizing and then replacing the result's
        metadata so ``vgi_rpc.location`` was erased. Each variant trips a
        different assertion below, which is what makes a failure legible
        rather than merely red:

        * no upload at all -> the writer treated "has metadata" as "is a
          control batch" and skipped externalization;
        * uploaded but zero rows -> the pointer lost its own pointer, so a
          resolver saw a bare zero-row data batch;
        * rows present but metadata absent -> the metadata went onto the
          pointer instead of into the payload, or the reader returned the
          pointer's metadata instead of the inner batch's.

        The last case is caught in *both* directions by this one test, because
        the two conformance roles put a different implementation on each side
        of the pointer: a writer defect fails it in the server role and a
        reader defect fails it in the client role.

        ``rows_per_batch=2000`` is 16 KB of int64, unambiguously over both the
        reference's 4 KiB default threshold and this fixture's one byte.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            batches = list(proxy.produce_annotated_batches(count=3, rows_per_batch=2000))
            _assert_uploaded(target, before, "produce_annotated_batches")
        assert len(batches) == 3
        for index, annotated in enumerate(batches):
            what = f"produce_annotated_batches batch {index}"
            assert annotated.batch.num_rows == 2000, (
                f"{what}: arrived with {annotated.batch.num_rows} rows -- a pointer "
                f"that lost vgi_rpc.location reads as a zero-row data batch"
            )
            metadata = annotated.custom_metadata
            assert metadata is not None, f"{what}: resolved batch carried no custom metadata"
            assert metadata.get(b"conformance.batch_index") == str(index).encode(), (
                f"{what}: per-emit metadata is missing or is another batch's -- "
                f"a reader that caches the first turn's metadata passes a constant "
                f"label and fails here"
            )
            assert metadata.get(b"conformance.batch_total") == b"3"
            assert metadata.get(b"conformance.emit_label") == ANNOTATED_EMIT_LABEL.encode()
            _assert_resolved_metadata(metadata, what)

    def test_dictionary_encoded_payloads_round_trip(self, request: pytest.FixtureRequest) -> None:
        """Dictionary-typed columns survive the pointer/payload split.

        A pointer batch is zero-row but keeps the payload's schema, so a
        dictionary column forces a dictionary message onto the wire with no
        values behind it, and a second one inside the fetched object.  Ports
        that mishandle that lose the whole batch to a ``dictionary with ID 0``
        error rather than to a wrong value.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            assert proxy.echo_dict_encoded_string(value="dictionary-encoded") == "dictionary-encoded"
            _assert_uploaded(target, before, "echo_dict_encoded_string")
            assert proxy.echo_dict_encoded_string(value="🌍" * 100) == "🌍" * 100
            assert proxy.echo_enum(status=Status.PENDING) == Status.PENDING
            assert proxy.echo_status_list(statuses=[Status.ACTIVE, Status.PENDING]) == [
                Status.ACTIVE,
                Status.PENDING,
            ]

    def test_stream_header_round_trips(self, request: pytest.FixtureRequest) -> None:
        """A rich stream header survives being externalised on its own.

        The header is written as its own IPC stream, ahead of the first data
        turn, and is externalised by the single-batch path rather than the
        collector path — a second call site, with its own chance to go wrong.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            session = proxy.produce_with_rich_header(seed=42, count=3)
            header = session.header
            assert isinstance(header, RichHeader)
            _assert_rich_header(header, 42)
            batches = list(session)
            _assert_uploaded(target, before, "produce_with_rich_header")
        assert len(batches) == 3

    def test_dynamic_schema_round_trips(self, request: pytest.FixtureRequest) -> None:
        """A per-stream output schema survives the pointer's schema echo."""
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            session = proxy.produce_dynamic_schema(seed=7, count=2, include_strings=True, include_floats=True)
            assert isinstance(session.header, RichHeader)
            batches = list(session)
            _assert_uploaded(target, before, "produce_dynamic_schema")
        expected = build_dynamic_schema(include_strings=True, include_floats=True)
        assert len(batches) == 2
        for annotated in batches:
            assert annotated.batch.schema.equals(expected)

    def test_error_after_externalised_batches_still_raises(self, request: pytest.FixtureRequest) -> None:
        """An error batch is zero-row, so it stays inline among pointers.

        A reader that treats every zero-row batch as a pointer, or every
        pointer as data, gets this one wrong in opposite directions.
        """
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            with pytest.raises(RpcError, match="intentional error"):
                for _ in proxy.produce_error_mid_stream(emit_before_error=2):
                    pass
            _assert_uploaded(target, before, "produce_error_mid_stream")
            assert proxy.echo_int(value=7) == 7

    def test_large_payload_round_trips(self, request: pytest.FixtureRequest) -> None:
        """A payload well past any inline budget survives intact."""
        target = _target(request)
        with target.connect() as proxy:
            before = target.uploaded_objects()
            assert len(proxy.oversized_unary(target_bytes=256_000)) == 256_000
            _assert_uploaded(target, before, "oversized_unary")
            batches = list(proxy.produce_large_batches(rows_per_batch=2_000, batch_count=2))
        assert [ab.batch.num_rows for ab in batches] == [2_000, 2_000]


__all__ = [
    "ByteStreamExternalConnection",
    "ByteStreamExternalTarget",
    "ExternalExchangeSession",
    "ExternalHeaderStream",
    "TestExternalByteStream",
]
