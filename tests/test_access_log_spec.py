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


#: A spec-valid request shape: what replaced ``request_data`` on the record.
_REQUEST_SHAPE: dict[str, Any] = {"request_fields": [{"name": "v", "type": "string"}], "request_rows": 1}


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
            **_REQUEST_SHAPE,
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

    @pytest.mark.parametrize("field", ["request_data", "request_state", "response_state"])
    def test_payload_fields_are_rejected(self, field: str) -> None:
        """A record carrying a request/response payload fails, at any level.

        ``request_data`` used to be *required* at DEBUG, which made every
        server's DEBUG access log a credential store: VGI's
        ``catalog_attach`` carries API keys in its options, and the framework
        cannot tell a secret parameter from any other.
        """
        rec = self._good_unary_record()
        rec[field] = "QQ=="
        violations = validate_access_logs([rec])
        assert any(v.path == field and "must not appear" in v.message for v in violations), violations

    def test_unary_record_without_shape_passes(self) -> None:
        """The shape fields are optional -- absence is not a leak."""
        rec = self._good_unary_record()
        rec.pop("request_fields")
        rec.pop("request_rows")
        assert validate_access_logs([rec]) == []

    def test_request_fields_without_rows_rejected(self) -> None:
        """request_fields and request_rows travel together."""
        rec = self._good_unary_record()
        rec.pop("request_rows")
        assert validate_access_logs([rec])

    def test_request_fields_items_carry_no_value(self) -> None:
        """An entry is exactly {name, type}; a smuggled ``value`` is rejected."""
        rec = self._good_unary_record()
        rec["request_fields"] = [{"name": "api_key", "type": "string", "value": "hunter2"}]
        assert any("request_fields" in v.path for v in validate_access_logs([rec]))

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
        # The canonical code rides on every error record (WIRE_PROTOCOL.md §8);
        # an unclassified ValueError is UNKNOWN.
        assert err["error_code"] == "UNKNOWN"
        assert all("error_code" not in e for e in entries if e["status"] == "ok")

    def test_ok_record_with_error_code_is_rejected(self) -> None:
        """``error_code`` belongs to failures; a success carrying one is malformed."""
        rec = TestValidator()._good_unary_record()
        rec["error_code"] = "UNKNOWN"
        assert validate_access_logs([rec])

    def test_error_code_outside_the_closed_set_is_rejected(self) -> None:
        """The code is a closed set, so the schema enumerates it."""
        rec = TestValidator()._good_unary_record()
        rec.update(status="error", error_type="ValueError", error_message="x", error_code="NOT_A_CODE")
        assert validate_access_logs([rec])

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
    **_REQUEST_SHAPE,
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


class TestReferenceWorkerValidatesItself:
    """The reference worker must pass the reference validator, end to end.

    It could not before: ``vgi-rpc-conformance`` had no ``--access-log``, so
    the one implementation that defines correct behaviour was the one never
    run through the rules. That is how a ``num_rows != 1`` check shipped that
    rejects Python's own zero-parameter methods — ``void_noop`` sends a batch
    with an empty schema and no row, which ``_wire.py`` explicitly allows and
    the validator did not. Other ports were failed for matching the reference.
    """

    def test_zero_parameter_method_validates(self) -> None:
        """A zero-parameter call's record is conformant: no fields, no row.

        A method with no arguments has nothing to put in a row, so its shape
        is ``request_fields == []`` and ``request_rows == 0``.
        """
        records: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
        from vgi_rpc.http import http_connect
        from vgi_rpc.http._testing import make_sync_client

        logger = logging.getLogger("vgi_rpc.access")
        handler = _Capture()
        previous = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            client = make_sync_client(RpcServer(ConformanceService, ConformanceServiceImpl()), token_key=b"k" * 32)
            try:
                with http_connect(ConformanceService, "http://x", client=client) as proxy:
                    proxy.void_noop()
            finally:
                client.close()
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous)

        emitted = [r for r in records if getattr(r, "method", None) == "void_noop"]
        assert emitted, "no access-log record for void_noop"
        assert not hasattr(emitted[0], "request_data"), "no payload at DEBUG either"
        assert getattr(emitted[0], "request_fields", None) == [], "void_noop takes no parameters"
        assert getattr(emitted[0], "request_rows", None) == 0, "an empty schema carries no row"

        record = _format_record(emitted[0])
        violations = validate_access_logs([record])
        assert violations == [], f"the reference worker fails its own validator: {violations}"

    def test_shipped_reference_worker_validates_end_to_end(self, tmp_path: Path) -> None:
        """Drive the *shipped* reference worker through the reference validator.

        At DEBUG -- the level that used to emit ``request_data`` -- so the
        record that would leak is the one checked. Covers ``void_*``, where
        the zero-parameter shape applies.
        """
        log_path = tmp_path / "reference.log"
        worker = f"{sys.executable} -m vgi_rpc.conformance._cli --access-log {log_path} --access-log-debug"
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "vgi_rpc.conformance._test_cli",
                "--cmd",
                worker,
                "--access-log",
                str(log_path),
                "--filter",
                "scalar*,void*",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=180,
        )
        assert proc.returncode == 0, f"stderr:\n{proc.stderr}\nstdout:\n{proc.stdout}"
        assert "--access-log: PASS" in proc.stderr

        records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        unary = [r for r in records if r.get("method_type") == "unary"]
        assert unary, "no unary records emitted"
        assert not [r for r in records if {"request_data", "request_state", "response_state"} & r.keys()]
        assert all("request_fields" in r and "request_rows" in r for r in unary), "DEBUG must describe every request"
        assert any(str(r.get("method", "")).startswith("void") for r in unary), (
            "a zero-parameter method must be exercised"
        )

    def test_retired_require_request_data_flag_fails_loudly(self, tmp_path: Path) -> None:
        """``--require-request-data`` now fails with an explanation.

        Every port's CI passes it. A silent no-op would keep those runs green
        while asserting nothing; failing tells each port the rule inverted.
        """
        log_path = tmp_path / "info.log"
        worker = f"{sys.executable} -m vgi_rpc.conformance._cli --access-log {log_path}"
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "vgi_rpc.conformance._test_cli",
                "--cmd",
                worker,
                "--access-log",
                str(log_path),
                "--require-request-data",
                "--filter",
                "scalar*",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=180,
        )
        assert proc.returncode != 0
        assert "--require-request-data: removed" in proc.stderr

    def test_conformance_cli_can_emit_an_access_log(self) -> None:
        """The reference worker exposes the flag that makes the above testable.

        Without it there is no way to capture the reference's records at all,
        which is the gap that let the rule ship.
        """
        import subprocess

        out = subprocess.run(
            [sys.executable, "-m", "vgi_rpc.conformance._cli", "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        ).stdout
        assert "--access-log" in out
        assert "--access-log-debug" in out


_SECRET = "sk-live-SENTINEL-7f3a9c2e-do-not-log"


class _SecretSvc(Protocol):
    """A protocol whose parameters carry a credential, as VGI's catalog_attach does."""

    def attach(self, api_key: str, host: str) -> str:
        """Unary call carrying a secret."""
        ...

    def tail(self, api_key: str) -> Stream[_SecretState]:
        """Exchange stream holding the secret in serialized state."""
        ...


@dataclass
class _SecretState(StreamState):
    """Stream state that keeps the credential, so state tokens carry it."""

    api_key: str
    seen: int = 0

    def process(self, input_batch: Any, out: OutputCollector, ctx: Any) -> None:
        """Echo the input row count."""
        self.seen += 1
        out.emit(pa.record_batch([pa.array([self.seen])], schema=_COUNT_SCHEMA))


class _SecretImpl:
    """Implementation of :class:`_SecretSvc`."""

    def attach(self, api_key: str, host: str) -> str:
        """Return something that is not the key."""
        return f"attached to {host}"

    def tail(self, api_key: str) -> Stream[_SecretState]:
        """Open an exchange stream holding the key in state."""
        return Stream(output_schema=_COUNT_SCHEMA, state=_SecretState(api_key=api_key), input_schema=_COUNT_SCHEMA)


def _drive_secret_calls(proxy: Any) -> None:
    """One unary call and two exchange turns, all carrying the secret."""
    from vgi_rpc import AnnotatedBatch

    proxy.attach(api_key=_SECRET, host="db.example")
    with proxy.tail(api_key=_SECRET) as session:
        for _ in range(2):
            session.exchange(AnnotatedBatch(batch=pa.record_batch([pa.array([1])], schema=_COUNT_SCHEMA)))


class TestNoPayloadInLogs:
    """No request/response payload value reaches any log, at DEBUG, on any transport.

    The access log used to carry ``request_data`` (the full request as base64
    Arrow IPC) and the HTTP state tokens at DEBUG, and the
    ``vgi_rpc.wire.request`` DEBUG lines rendered every kwarg's ``repr``. A
    VGI ``catalog_attach`` carries API keys in its options, so turning on
    DEBUG wrote them to disk in plaintext. These tests run calls whose
    arguments carry a sentinel secret with *every* ``vgi_rpc`` logger at
    DEBUG and assert the sentinel appears nowhere in the formatted output --
    base64 forms included, since that is how ``request_data`` carried it.
    """

    @staticmethod
    def _run_at_debug(use_http: bool) -> tuple[str, list[dict[str, Any]]]:
        """Drive unary + stream calls at DEBUG; return all log text and access records."""
        stream = io.StringIO()
        text_handler = logging.StreamHandler(stream)
        text_handler.setFormatter(VgiJsonFormatter())
        access: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.name == "vgi_rpc.access":
                    access.append(record)

        capture = _Capture()
        root = logging.getLogger("vgi_rpc")
        access_logger = logging.getLogger("vgi_rpc.access")
        saved = (root.level, access_logger.level, access_logger.propagate)
        root.addHandler(text_handler)
        root.addHandler(capture)
        root.setLevel(logging.DEBUG)
        access_logger.setLevel(logging.DEBUG)
        access_logger.propagate = True
        try:
            if use_http:
                from vgi_rpc.http import http_connect
                from vgi_rpc.http._testing import make_sync_client

                # Exchange turns carry the state token in both directions.
                client = make_sync_client(RpcServer(_SecretSvc, _SecretImpl()), token_key=b"k" * 32)
                try:
                    with http_connect(_SecretSvc, "http://x", client=client) as proxy:
                        _drive_secret_calls(proxy)
                finally:
                    client.close()
            else:
                with serve_pipe(_SecretSvc, _SecretImpl()) as proxy:
                    _drive_secret_calls(proxy)
        finally:
            root.removeHandler(text_handler)
            root.removeHandler(capture)
            root.setLevel(saved[0])
            access_logger.setLevel(saved[1])
            access_logger.propagate = saved[2]
        return stream.getvalue(), [_format_record(r) for r in access]

    @staticmethod
    def _secret_forms() -> list[str]:
        """Return the sentinel plus its base64 encodings at each of the three alignments."""
        import base64

        raw = _SECRET.encode()
        forms = [_SECRET]
        for pad in range(3):
            enc = base64.b64encode(b"\0" * pad + raw).decode()
            # Drop the chars that straddle the alignment boundary on each end.
            forms.append(enc[4:-4])
        return forms

    @pytest.mark.parametrize("use_http", [True, False], ids=["http", "pipe"])
    def test_sentinel_absent_from_all_debug_output(self, use_http: bool) -> None:
        """Neither the secret nor any base64 of it is written by any vgi_rpc logger."""
        text, records = self._run_at_debug(use_http)
        assert records, "no access records captured -- the test would pass vacuously"
        assert "Parsed request" in text, "wire DEBUG lines missing -- the test would pass vacuously"
        for form in self._secret_forms():
            assert form not in text, f"secret leaked into DEBUG log output as {form!r}"

    @pytest.mark.parametrize("use_http", [True, False], ids=["http", "pipe"])
    def test_access_records_carry_shape_not_payload(self, use_http: bool) -> None:
        """Records describe the request (names, types, rows), and validate."""
        _text, records = self._run_at_debug(use_http)
        for r in records:
            assert not {"request_data", "request_state", "response_state"} & r.keys(), r
        attach = next(r for r in records if r["method"] == "attach")
        assert attach["request_fields"] == [
            {"name": "api_key", "type": "string"},
            {"name": "host", "type": "string"},
        ]
        assert attach["request_rows"] == 1
        assert "truncated" not in attach, "nothing is omitted any more, so nothing is marked"
        assert validate_access_logs(records) == []

    def test_http_state_tokens_logged_as_sizes(self) -> None:
        """State tokens are reported by size only."""
        _text, records = self._run_at_debug(use_http=True)
        sized = [r for r in records if "response_state_bytes" in r or "request_state_bytes" in r]
        assert sized, "expected exchange turns carrying state tokens"
        assert any("request_state_bytes" in r for r in sized), "an exchange turn sends a token back"
        assert all(
            isinstance(r.get("response_state_bytes", 0), int) and isinstance(r.get("request_state_bytes", 0), int)
            for r in sized
        )


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
        """The legacy value still validates: ports emitting it at INFO leaked nothing."""
        rec = {**_MINIMAL_RECORD, "truncated": "payload_omitted"}
        rec["original_request_bytes"] = 4096
        jsonschema.validate(rec, _load_schema())

    def test_schema_still_accepts_the_size_driven_values(self) -> None:
        """The two pre-existing meanings are unchanged."""
        schema = _load_schema()
        for value in (True, "record_too_large"):
            rec = {**_MINIMAL_RECORD, "truncated": value}
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


class TestPerBindingIdentity:
    """A record must name the protocol that owns the method -- and its hash.

    ``access-log-spec.md`` makes ``protocol`` the owning protocol's wire name
    and ``protocol_hash`` "the registry key when decoding archived records".
    The name was already per-binding; the hash was the server's primary at
    every emit site, so a reflection call produced a record naming one protocol
    and carrying another's digest.

    That is the failure the plan ranks highest, because nothing about it looks
    wrong: the record is well-formed, passes the schema, and decodes against
    the wrong description.  Only a call to a *secondary* protocol can catch it,
    which is why the conformance validator -- asserting
    ``protocol == "ConformanceService"`` -- never did.
    """

    def _capture(self, fn: Any) -> list[dict[str, Any]]:
        return TestLiveCapture()._capture(fn)

    def test_the_two_bindings_do_not_share_a_digest(self) -> None:
        """The precondition that makes the rest of this class meaningful.

        If reflection and the application hashed the same, logging the primary
        everywhere would be invisible *and* harmless, and none of these tests
        would prove anything.
        """
        from vgi_rpc.rpc._reflection import Reflection

        srv = RpcServer(_Svc, _Impl(), enable_describe=True)
        assert srv.bindings[Reflection.protocol_name].protocol_hash != srv.protocol_hash

    def test_the_hash_accessor_returns_the_owning_bindings_digest(self) -> None:
        """The unit-level property, which holds on every transport.

        Asserted directly as well as end-to-end because the emit sites are
        spread across both HTTP dispatchers, the stream resource and three
        raw-transport paths, and a future site added in one of them would
        otherwise reintroduce the bug with the suite still green.
        """
        from vgi_rpc.rpc._reflection import Reflection

        srv = RpcServer(_Svc, _Impl(), enable_describe=True)
        reflection_methods = srv.bindings[Reflection.protocol_name].methods
        info = next(iter(reflection_methods.values()))
        assert srv.protocol_hash_for(info) == srv.bindings[Reflection.protocol_name].protocol_hash
        assert srv.protocol_hash_for(info) != srv.protocol_hash

    def test_an_unowned_framework_endpoint_falls_back_to_the_primary(self) -> None:
        """``__transport_options__`` and ``__upload_url__`` belong to no protocol.

        The spec prescribes the server's primary for those, so the fallback is
        the specified behaviour rather than a gap in it.
        """
        srv = RpcServer(_Svc, _Impl(), enable_describe=True)
        assert srv.protocol_hash_for(None) == srv.protocol_hash
