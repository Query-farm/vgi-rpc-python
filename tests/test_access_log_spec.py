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
from dataclasses import dataclass
from typing import Any, Protocol

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
from vgi_rpc.logging_utils import VgiJsonFormatter
from vgi_rpc.rpc import RpcError, serve_pipe

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
            "timestamp": "2026-01-01T00:00:00Z",
            "level": "INFO",
            "logger": "vgi_rpc.access",
            "message": "Svc.greet ok",
            "server_id": "abc123",
            "protocol": "Svc",
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


def test_violation_dataclass_shape() -> None:
    """Violation has the documented public fields."""
    v = Violation(entry_index=0, method="m", path="p", message="msg")
    assert (v.entry_index, v.method, v.path, v.message) == (0, "m", "p", "msg")
