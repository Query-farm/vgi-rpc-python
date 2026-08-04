# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the cross-language access log specification.

These tests pin the Python reference implementation to ``access_log.schema.json``
and exercise the language-agnostic validator in ``vgi_rpc.access_log_conformance``.
"""

from __future__ import annotations

import io
import json
import logging
import queue
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import jsonschema
import pyarrow as pa
import pytest

from vgi_rpc import OutputCollector, Stream, StreamState
from vgi_rpc.access_log_conformance import (
    Violation,
    _load_schema,
    validate_access_logs,
)
from vgi_rpc.access_log_conformance import (
    main as conformance_main,
)
from vgi_rpc.logging_utils import (
    REDACTED,
    AccessLogSampler,
    DroppingQueueHandler,
    VgiJsonFormatter,
    apply_claim_redaction,
    no_redaction,
    redact_claims,
    set_claim_redactor,
)
from vgi_rpc.rpc import CallContext, RpcError, RpcServer, serve_pipe
from vgi_rpc.rpc._server import _current_trace_context

# ---------------------------------------------------------------------------
# Service used to produce a representative access log
# ---------------------------------------------------------------------------


class _Svc(Protocol):
    """Minimal protocol exercising unary, error, stream-init, stream continuations."""

    def greet(self, name: str) -> str:
        """Return a greeting."""
        ...

    def boom(self, message: str) -> str:
        """Raise."""
        ...

    def count(self, n: int) -> Stream[_CountState]:
        """Stream a count down from n."""
        ...


_COUNT_SCHEMA = pa.schema([pa.field("v", pa.int64())])


@dataclass
class _CountState(StreamState):
    """Emit one batch per remaining count."""

    remaining: int

    def process(
        self,
        input_batch: Any,
        out: OutputCollector,
        ctx: Any,
    ) -> None:
        """Emit one row, decrement, finish at zero."""
        if self.remaining <= 0:
            out.finish()
            return
        out.emit(pa.record_batch([pa.array([self.remaining])], schema=_COUNT_SCHEMA))
        self.remaining -= 1
        if self.remaining <= 0:
            out.finish()


class _StickySvc(Protocol):
    """Minimal sticky-session protocol: open, resume, close."""

    def open_thing(self, initial: int) -> int:
        """Open a session holding a counter."""
        ...

    def touch_thing(self, by: int) -> int:
        """Mutate the session-bound counter."""
        ...

    def close_thing(self) -> int:
        """Close the session, returning the counter's final value."""
        ...


@dataclass
class _Thing:
    """Session state for :class:`_StickySvc`."""

    value: int

    def close(self) -> None:
        """Cleanup hook the registry invokes on eviction."""


class _StickyImpl:
    """Reference sticky impl driving the access log's session fields."""

    def open_thing(self, initial: int, ctx: CallContext) -> int:
        """Register a counter in a new session."""
        ctx.open_session(_Thing(value=initial))
        return initial

    def touch_thing(self, by: int, ctx: CallContext) -> int:
        """Increment the session's counter."""
        thing = ctx.session
        assert isinstance(thing, _Thing)
        thing.value += by
        return thing.value

    def close_thing(self, ctx: CallContext) -> int:
        """Close the session and report the final value."""
        thing = ctx.session
        assert isinstance(thing, _Thing)
        final = thing.value
        ctx.close_session()
        return final


class _Impl:
    """Reference impl."""

    def greet(self, name: str) -> str:
        """Greet."""
        return f"Hello, {name}!"

    def boom(self, message: str) -> str:
        """Raise on demand."""
        raise ValueError(message)

    def count(self, n: int) -> Stream[_CountState]:
        """Stream a count down from n."""
        return Stream(output_schema=_COUNT_SCHEMA, state=_CountState(remaining=n))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_record(record: logging.LogRecord) -> dict[str, Any]:
    """Format a captured record through VgiJsonFormatter and parse back to dict."""
    formatter = VgiJsonFormatter()
    raw = formatter.format(record)
    parsed: dict[str, Any] = json.loads(raw)
    return parsed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSchema:
    """Sanity checks on the schema itself."""

    def test_schema_is_valid_draft_2020_12(self) -> None:
        """The shipped schema must itself be valid JSON Schema 2020-12."""
        schema = _load_schema()
        jsonschema.Draft202012Validator.check_schema(schema)

    def test_schema_requires_core_fields(self) -> None:
        """Empty object must fail with required-field violations."""
        schema = _load_schema()
        validator = jsonschema.Draft202012Validator(schema)
        errors = list(validator.iter_errors({}))
        missing = {e.message for e in errors if e.validator == "required"}
        assert any("server_id" in m for m in missing)
        assert any("method" in m for m in missing)
        assert any("status" in m for m in missing)


class TestValidator:
    """Tests for validate_access_logs against synthetic records."""

    def _good_unary_record(self) -> dict[str, Any]:
        return {
            "timestamp": "2026-01-01T00:00:00.000Z",
            "level": "INFO",
            "logger": "vgi_rpc.access",
            "message": "Svc.greet ok",
            "server_id": "abc123",
            "protocol": "Svc",
            "protocol_hash": "0" * 64,
            "method": "greet",
            "method_type": "unary",
            "principal": "",
            "auth_domain": "",
            "authenticated": False,
            "remote_addr": "",
            "duration_ms": 1.23,
            "status": "ok",
            "error_type": "",
            "request_data": "QQ==",
        }

    def test_minimal_unary_record_passes(self) -> None:
        """A correctly shaped unary record validates clean."""
        violations = validate_access_logs([self._good_unary_record()])
        assert violations == []

    def test_error_status_requires_message(self) -> None:
        """status=error without error_message is a violation."""
        rec = self._good_unary_record()
        rec["status"] = "error"
        rec["error_type"] = "ValueError"
        violations = validate_access_logs([rec])
        assert any("error_message" in v.message for v in violations)

    def test_ok_status_forbids_nonempty_error_type(self) -> None:
        """status=ok with a populated error_type is a violation."""
        rec = self._good_unary_record()
        rec["error_type"] = "Something"
        violations = validate_access_logs([rec])
        assert violations
        assert any(v.path == "error_type" for v in violations)

    def test_stream_method_requires_stream_id(self) -> None:
        """method_type=stream without stream_id is a violation."""
        rec = self._good_unary_record()
        rec["method_type"] = "stream"
        rec.pop("request_data", None)
        violations = validate_access_logs([rec])
        assert any("stream_id" in v.message for v in violations)

    def test_unary_record_requires_request_data(self) -> None:
        """method_type=unary without request_data is a violation."""
        rec = self._good_unary_record()
        rec.pop("request_data", None)
        violations = validate_access_logs([rec])
        assert any("request_data" in v.message for v in violations)

    def test_partial_call_statistics_rejected(self) -> None:
        """If any of the six stats fields is present they must all be present."""
        rec = self._good_unary_record()
        rec["input_batches"] = 1
        violations = validate_access_logs([rec])
        assert violations, "partial stats must be rejected"

    def test_full_call_statistics_accepted(self) -> None:
        """All six stats fields together is fine."""
        rec = self._good_unary_record()
        for k in ("input_batches", "output_batches", "input_rows", "output_rows", "input_bytes", "output_bytes"):
            rec[k] = 0
        assert validate_access_logs([rec]) == []

    def test_invalid_stream_id_format_rejected(self) -> None:
        """stream_id must be 32 lowercase hex chars."""
        rec = self._good_unary_record()
        rec["method_type"] = "stream"
        rec["stream_id"] = "not-a-uuid-hex"
        rec.pop("request_data", None)
        violations = validate_access_logs([rec])
        assert any(v.path == "stream_id" for v in violations)


class TestLiveCapture:
    """Drive real RPC calls and validate the captured records against the schema."""

    def _capture(self, callable_under_test: Any) -> list[dict[str, Any]]:
        records: list[logging.LogRecord] = []

        class _Sink(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        access_logger = logging.getLogger("vgi_rpc.access")
        prev_level = access_logger.level
        sink = _Sink(level=logging.INFO)
        access_logger.addHandler(sink)
        access_logger.setLevel(logging.INFO)
        try:
            callable_under_test()
        finally:
            access_logger.removeHandler(sink)
            access_logger.setLevel(prev_level)
        return [_format_record(r) for r in records]

    def test_unary_success_passes_schema(self) -> None:
        """A real unary call produces a schema-conformant record."""

        def run() -> None:
            with serve_pipe(_Svc, _Impl()) as proxy:
                proxy.greet(name="World")

        entries = self._capture(run)
        assert entries
        violations = validate_access_logs(entries)
        assert violations == [], f"violations: {violations}"
        assert any(e["method"] == "greet" and e["status"] == "ok" for e in entries)

    def test_unary_error_passes_schema(self) -> None:
        """A unary call that raises produces a schema-conformant error record."""

        def run() -> None:
            with serve_pipe(_Svc, _Impl()) as proxy, pytest.raises(RpcError):
                proxy.boom(message="bang")

        entries = self._capture(run)
        violations = validate_access_logs(entries)
        assert violations == [], f"violations: {violations}"
        err = next(e for e in entries if e["method"] == "boom")
        assert err["status"] == "error"
        assert err["error_type"] == "ValueError"
        assert err["error_message"] == "bang"

    def test_sticky_session_records_pass_schema(self) -> None:
        """Open / resume / close records carry schema-conformant sticky fields.

        ``session_id`` and ``session_action`` are part of the cross-language
        access-log contract, so a real sticky lifecycle has to validate
        against ``access_log.schema.json`` — not just against hand-written
        record dicts. Sticky and access logging are both HTTP-only, so this
        runs over the in-process WSGI client.
        """
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import make_sync_client

        def run() -> None:
            server = RpcServer(_StickySvc, _StickyImpl())
            client = make_sync_client(
                server,
                token_key=b"access-log-sticky-key-32-bytes!!",
                enable_sticky=True,
                sticky_default_ttl=60.0,
            )
            try:
                with (
                    http_connect(_StickySvc, client=client) as proxy,
                    cast("Any", proxy).with_session_token() as sess,
                ):
                    sess.open_thing(initial=4)
                    sess.touch_thing(by=3)
                    sess.close_thing()
            finally:
                client.close()

        entries = self._capture(run)
        violations = validate_access_logs(entries)
        assert violations == [], f"violations: {violations}"

        lifecycle = [e for e in entries if e["method"] in ("open_thing", "touch_thing", "close_thing")]
        assert [e["session_action"] for e in lifecycle] == ["open", "resume", "close"]
        session_ids = {e["session_id"] for e in lifecycle}
        assert len(session_ids) == 1, f"one session must produce one id; got {session_ids}"
        assert re.fullmatch(r"[0-9a-f]{24}", session_ids.pop()), "session_id must be 24 lowercase hex chars"

    def test_stream_records_pass_schema(self) -> None:
        """Stream init + continuations all conform; stream_id stable across them."""

        def run() -> None:
            with serve_pipe(_Svc, _Impl()) as proxy:
                list(proxy.count(n=3))

        entries = self._capture(run)
        violations = validate_access_logs(entries)
        assert violations == [], f"violations: {violations}"
        stream_entries = [e for e in entries if e.get("method_type") == "stream"]
        assert stream_entries, "expected at least one stream record"
        ids = {e["stream_id"] for e in stream_entries}
        assert len(ids) == 1, f"stream_id must be stable across continuations, got {ids}"


class TestCli:
    """Smoke-test the CLI entry point."""

    def test_passes_clean_input(self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
        """A clean record on stdin yields exit 0."""
        rec = TestValidator()._good_unary_record()
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(rec) + "\n"))
        rc = conformance_main(["-"])
        assert rc == 0
        assert "PASS" in capsys.readouterr().out

    def test_fails_bad_input(self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
        """A record missing required fields yields exit 1."""
        rec = TestValidator()._good_unary_record()
        del rec["server_id"]
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(rec) + "\n"))
        rc = conformance_main(["-"])
        assert rc == 1
        assert "FAIL" in capsys.readouterr().out


class TestVgiRpcTestCli:
    """End-to-end: drive the reference Python worker via vgi-rpc-test --access-log."""

    def test_passes_against_reference_worker(self, tmp_path: Path) -> None:
        """vgi-rpc-test --access-log validates the Python reference worker clean."""
        log_path = tmp_path / "access.log"
        cmd = f"{sys.executable} -m tests.serve_conformance_pipe --access-log {log_path}"
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "vgi_rpc.conformance._test_cli",
                "--cmd",
                cmd,
                "--access-log",
                str(log_path),
                "--filter",
                "scalar*,void*",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        # Suite + access-log validation must both succeed.
        assert proc.returncode == 0, f"stderr:\n{proc.stderr}\nstdout:\n{proc.stdout}"
        assert "--access-log: PASS" in proc.stderr
        # Confirm the worker actually wrote records.
        assert log_path.exists()
        assert log_path.read_text().count("\n") > 0


def test_violation_dataclass_shape() -> None:
    """Violation has the documented public fields."""
    v = Violation(entry_index=0, method="m", path="p", message="msg")
    assert (v.entry_index, v.method, v.path, v.message) == (0, "m", "p", "msg")


#: A schema-valid unary record, used as the base for field-level checks.
_MINIMAL_RECORD: dict[str, Any] = {
    "timestamp": "2026-01-01T00:00:00.000Z",
    "level": "INFO",
    "logger": "vgi_rpc.access",
    "message": "Svc.greet ok",
    "server_id": "abc123",
    "protocol": "Svc",
    "protocol_hash": "0" * 64,
    "method": "greet",
    "method_type": "unary",
    "principal": "",
    "auth_domain": "",
    "authenticated": False,
    "remote_addr": "",
    "duration_ms": 1.23,
    "status": "ok",
    "error_type": "",
    "request_data": "QQ==",
}


def _record(**extra: object) -> logging.LogRecord:
    """Build an access-log record carrying *extra* as attributes."""
    rec = logging.LogRecord("vgi_rpc.access", logging.INFO, __file__, 1, "m", None, None)
    for key, value in extra.items():
        setattr(rec, key, value)
    return rec


class TestAccessLogSampler:
    """Sampling must not cost you the records you keep logs for."""

    def test_full_rate_keeps_everything(self) -> None:
        """The default rate is a pass-through."""
        s = AccessLogSampler(1.0)
        assert all(s.filter(_record(request_id=f"r{i}", status="ok")) for i in range(200))

    def test_zero_rate_still_keeps_errors(self) -> None:
        """Errors are never sampled away, even at rate 0.

        A rate below 1 exists because successful calls repeat, which is
        exactly what failures do not. Dropping one error in ten leaves a
        consumer unable to say whether an error count fell because a fix
        landed or because the dice went the other way.
        """
        s = AccessLogSampler(0.0)
        assert not s.filter(_record(request_id="r1", status="ok"))
        assert s.filter(_record(request_id="r1", status="error"))

    def test_decision_is_stable_for_one_stream(self) -> None:
        """Every record of a stream shares its init's fate.

        Random per-record sampling shreds a multi-record call into
        fragments that read as data loss downstream — and the calls most
        likely to be split are the long streams worth studying.
        """
        s = AccessLogSampler(0.5)
        for stream in (f"s{i}" for i in range(40)):
            verdicts = {s.filter(_record(stream_id=stream, request_id=f"r{n}", status="ok")) for n in range(6)}
            assert len(verdicts) == 1, f"stream {stream} was split across the sample boundary"

    def test_stream_id_wins_over_request_id(self) -> None:
        """Keying prefers stream_id, so continuations group by call not request."""
        s = AccessLogSampler(0.5)
        a = s.filter(_record(stream_id="same", request_id="differs-1", status="ok"))
        b = s.filter(_record(stream_id="same", request_id="differs-2", status="ok"))
        assert a == b

    def test_rate_rides_on_kept_records(self) -> None:
        """A consumer scaling counts needs the divisor in-band."""
        s = AccessLogSampler(0.5)
        kept = [r for r in (_record(request_id=f"r{i}", status="ok") for i in range(200)) if s.filter(r)]
        assert kept, "rate 0.5 kept nothing across 200 records"
        assert all(getattr(r, "sample_rate", None) == 0.5 for r in kept)

    def test_rate_is_roughly_honoured(self) -> None:
        """The hash is a sampler, not just a filter that passes everything."""
        s = AccessLogSampler(0.25)
        kept = sum(1 for i in range(4000) if s.filter(_record(request_id=f"r{i}", status="ok")))
        assert 800 < kept < 1200, f"kept {kept}/4000, expected ~1000"

    @pytest.mark.parametrize("bad", [-0.1, 1.1, 2.0])
    def test_rejects_out_of_range(self, bad: float) -> None:
        """A rate of 100 meaning '100%' must fail loudly, not log everything."""
        with pytest.raises(ValueError, match=r"between 0\.0 and 1\.0"):
            AccessLogSampler(bad)


class TestClaimRedaction:
    """Claims reach an access log that outlives the token by years."""

    def test_credentials_are_redacted(self) -> None:
        """The credential list matches what vgi_rpc.sentry redacts from kwargs."""
        out = redact_claims({"access_token": "abc", "api_key": "k", "password": "p"})
        assert set(out.values()) == {REDACTED}

    def test_standard_oidc_pii_is_redacted(self) -> None:
        """`email`/`phone`/`name` are the claims an OIDC provider actually sends."""
        out = redact_claims({"email": "a@b.com", "phone_number": "+1", "given_name": "Ada"})
        assert set(out.values()) == {REDACTED}

    def test_keys_survive_redaction(self) -> None:
        """Which claims the token carried is auditable; their values are not.

        Dropping the key answers neither question. Keeping it answers the
        one an audit log exists for.
        """
        out = redact_claims({"email": "a@b.com"})
        assert list(out) == ["email"]
        assert out["email"] == REDACTED

    def test_non_sensitive_claims_pass_through(self) -> None:
        """Redaction must not gut the record — `iss`/`aud`/`scope` stay."""
        claims = {"iss": "https://idp", "aud": "svc", "scope": "read", "exp": 123}
        assert redact_claims(claims) == claims

    def test_value_matching_is_not_attempted(self) -> None:
        """Key-based, like sentry's: free text holding PII is not caught.

        Documented rather than fixed — matching on content means guessing,
        and a redactor that sometimes catches things is worse than one whose
        boundary is stated.
        """
        out = redact_claims({"context": "contact alice@example.com"})
        assert out["context"] == "contact alice@example.com"

    def test_a_raising_redactor_fails_closed(self) -> None:
        """A broken redactor drops claims rather than emitting them raw."""

        def boom(_claims: Mapping[str, object]) -> dict[str, object]:
            raise RuntimeError("nope")

        set_claim_redactor(boom)
        try:
            assert apply_claim_redaction({"email": "a@b.com"}) == {}
        finally:
            set_claim_redactor(redact_claims)

    def test_redactor_is_replaceable(self) -> None:
        """An internal service can opt out deliberately."""
        set_claim_redactor(no_redaction)
        try:
            assert apply_claim_redaction({"email": "a@b.com"}) == {"email": "a@b.com"}
        finally:
            set_claim_redactor(redact_claims)


class TestDroppingQueueHandler:
    """Async emission must not lose records silently."""

    def test_records_pass_through(self) -> None:
        """The ordinary path enqueues."""
        q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10)
        h = DroppingQueueHandler(q)
        h.emit(_record(request_id="r1"))
        assert q.qsize() == 1

    def test_full_queue_drops_instead_of_blocking(self) -> None:
        """A stalled writer must not become request latency."""
        q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=1)
        h = DroppingQueueHandler(q)
        h.emit(_record(request_id="r1"))
        h.emit(_record(request_id="r2"))  # would block on a plain QueueHandler
        assert h.dropped == 1

    def test_next_record_reports_the_loss(self) -> None:
        """The count reaches the same file the lost records would have.

        A log that loses records without saying so is worse than a slow
        one — the consumer cannot tell a quiet period from a dropped one.
        """
        q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=1)
        h = DroppingQueueHandler(q)
        h.emit(_record(request_id="r1"))
        h.emit(_record(request_id="r2"))
        h.emit(_record(request_id="r3"))
        assert h.dropped == 2
        q.get()  # drain, making room
        h.emit(_record(request_id="r4"))
        survivors = [q.get() for _ in range(q.qsize())]
        assert any(getattr(r, "dropped_records", None) == 2 for r in survivors)
        assert h.dropped == 0, "counter must reset once the loss is reported"


class TestTruncationMarker:
    """`truncated` distinguishes real loss from a configured omission."""

    def test_schema_accepts_payload_omitted(self) -> None:
        """The new value validates."""
        rec = {**_MINIMAL_RECORD, "truncated": "payload_omitted"}
        rec.pop("request_data")
        rec["original_request_bytes"] = 4096
        jsonschema.validate(rec, _load_schema())

    def test_schema_still_accepts_the_size_driven_values(self) -> None:
        """The two pre-existing meanings are unchanged."""
        schema = _load_schema()
        for value in (True, "record_too_large"):
            rec = {**_MINIMAL_RECORD, "truncated": value}
            rec.pop("request_data")
            jsonschema.validate(rec, schema)

    def test_schema_rejects_an_unknown_marker(self) -> None:
        """The set stays closed so a consumer can switch on it."""
        rec = {**_MINIMAL_RECORD, "truncated": "sort_of"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(rec, _load_schema())

    def test_omission_is_distinguishable_from_size_loss(self) -> None:
        """The common case is now separable, which is the whole point.

        Before, a normally-configured server set `truncated: true` on
        essentially every unary record, so a consumer scanning for real
        data loss had nothing to filter on.
        """
        schema_values = {opt["const"] for opt in _load_schema()["properties"]["truncated"]["oneOf"]}
        assert "payload_omitted" in schema_values
        assert schema_values >= {True, "record_too_large", "payload_omitted"}


class TestTraceCorrelation:
    """Access-log records carry the trace join key when one exists."""

    def test_absent_without_a_span(self) -> None:
        """No active trace means no fields — not empty strings."""
        trace_id, span_id = _current_trace_context()
        assert (trace_id, span_id) == ("", "")

    def test_hex_format_when_tracing(self) -> None:
        """IDs are W3C-shaped, which is what the schema pattern enforces."""
        pytest.importorskip("opentelemetry.sdk")
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        with provider.get_tracer("test").start_as_current_span("s"):
            trace_id, span_id = _current_trace_context()
        assert re.fullmatch(r"[0-9a-f]{32}", trace_id), trace_id
        assert re.fullmatch(r"[0-9a-f]{16}", span_id), span_id

    def test_schema_accepts_the_trace_fields(self) -> None:
        """The schema patterns match what the implementation emits."""
        schema = _load_schema()
        props = schema["properties"]
        assert props["trace_id"]["pattern"] == "^[0-9a-f]{32}$"
        assert props["span_id"]["pattern"] == "^[0-9a-f]{16}$"
        jsonschema.validate({**_MINIMAL_RECORD, "trace_id": "a" * 32, "span_id": "b" * 16}, schema)

    def test_schema_rejects_malformed_trace_id(self) -> None:
        """A port emitting a dashed UUID must fail, not pass silently."""
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**_MINIMAL_RECORD, "trace_id": "not-a-trace-id"}, _load_schema())


#: The four JSON envelope fields, which the porting guide counts separately
#: from the structured ones. Named here so the split is explicit rather than
#: an arithmetic constant.
_ENVELOPE_FIELDS = frozenset({"timestamp", "level", "logger", "message"})


class TestPortingGuideMatchesSchema:
    """The porting guide's required-field list must track the schema.

    The guide restates the schema's ``required`` array in prose, which is
    the right call for a document someone reads before writing any code --
    but a hand-maintained copy of a machine-readable list drifts, and this
    one did: ``protocol_hash`` was added to the schema and never to the
    guide, so a porter following it built records that failed validation on
    a field the guide never mentioned.
    """

    @staticmethod
    def _guide() -> str:
        return (Path(__file__).parent.parent / "docs" / "porting-guide.md").read_text()

    def test_every_required_field_is_documented(self) -> None:
        """A required field the guide never names is one a porter will omit."""
        required = set(_load_schema()["required"])
        guide = self._guide()
        missing = sorted(f for f in required if f"`{f}`" not in guide)
        assert not missing, (
            f"required by access_log.schema.json but absent from the porting guide: {missing} — "
            f"a porter following the guide would emit records failing validation"
        )

    def test_stated_count_matches_the_schema(self) -> None:
        """The stated count is what a porter checks their work against."""
        structured = set(_load_schema()["required"]) - _ENVELOPE_FIELDS
        guide = self._guide()
        stated = {int(n) for n in re.findall(r"(\d+) always-required fields", guide)}
        assert stated, "porting guide no longer states an always-required field count"
        assert stated == {len(structured)}, (
            f"porting guide says {sorted(stated)} always-required fields, schema has {len(structured)}"
        )
