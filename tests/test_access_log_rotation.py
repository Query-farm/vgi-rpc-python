# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for access-log rotation, placeholder substitution, and record truncation."""

from __future__ import annotations

import json
import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path

import jsonschema
import pytest

from vgi_rpc.access_log_conformance import _load_schema
from vgi_rpc.logging_utils import VgiAccessLogFormatter
from vgi_rpc.rpc import _configure_access_log


def _make_record(**fields: object) -> logging.LogRecord:
    """Build a LogRecord with arbitrary structured extras."""
    record = logging.LogRecord(
        name="vgi_rpc.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="ConformanceService.greet ok",
        args=None,
        exc_info=None,
    )
    for key, value in fields.items():
        setattr(record, key, value)
    return record


def _required_envelope(**overrides: object) -> dict[str, object]:
    """Return a dict with the always-required access-log fields."""
    base: dict[str, object] = {
        "server_id": "abc123",
        "protocol": "ConformanceService",
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
    base.update(overrides)
    return base


def test_formatter_under_cap_round_trips() -> None:
    """Records below the cap pass through untouched."""
    fmt = VgiAccessLogFormatter(max_record_bytes=65536)
    record = _make_record(**_required_envelope())
    line = fmt.format(record)
    obj = json.loads(line)
    assert obj["method"] == "greet"
    assert obj["request_data"] == "QQ=="
    assert "truncated" not in obj


def test_formatter_drops_request_data_when_over_cap() -> None:
    """Oversize records have request_data dropped and original size recorded."""
    fmt = VgiAccessLogFormatter(max_record_bytes=512)
    big = "A" * 4096
    record = _make_record(**_required_envelope(request_data=big))
    line = fmt.format(record)
    obj = json.loads(line)
    assert "request_data" not in obj
    assert obj["truncated"] is True
    assert obj["original_request_bytes"] == len(big)
    assert len(line.encode("utf-8")) <= 512


def test_formatter_falls_back_to_sentinel() -> None:
    """When even shed records exceed the cap, the sentinel form is used and error_message is preserved."""
    fmt = VgiAccessLogFormatter(max_record_bytes=320)
    record = _make_record(
        **_required_envelope(
            status="error",
            error_type="ValueError",
            error_message="A" * 2048,
            request_data="B" * 2048,
        )
    )
    line = fmt.format(record)
    obj = json.loads(line)
    assert obj["truncated"] == "record_too_large"
    assert obj["status"] == "error"
    assert obj["error_message"] == "A" * 2048


def test_formatter_handles_non_json_serializable_extras() -> None:
    """Non-JSON-serializable extras are coerced via default=str without raising."""
    fmt = VgiAccessLogFormatter(max_record_bytes=65536)
    record = _make_record(**_required_envelope(weird={"x": {1, 2, 3}}))
    line = fmt.format(record)
    json.loads(line)


def test_truncated_record_validates_against_schema() -> None:
    """A truncated unary record (no request_data) still validates."""
    schema = _load_schema()
    fmt = VgiAccessLogFormatter(max_record_bytes=400)
    record = _make_record(**_required_envelope(request_data="X" * 4096))
    obj = json.loads(fmt.format(record))
    obj.setdefault("timestamp", "2026-04-26T15:30:45.123Z")
    obj.setdefault("level", "INFO")
    obj.setdefault("logger", "vgi_rpc.access")
    obj.setdefault("message", "msg")
    jsonschema.validate(obj, schema)


def test_sentinel_record_validates_against_schema() -> None:
    """The record_too_large sentinel form validates against the schema."""
    schema = _load_schema()
    fmt = VgiAccessLogFormatter(max_record_bytes=320)
    record = _make_record(
        **_required_envelope(
            status="error",
            error_type="ValueError",
            error_message="A" * 2048,
            request_data="B" * 2048,
        )
    )
    obj = json.loads(fmt.format(record))
    obj.setdefault("timestamp", "2026-04-26T15:30:45.123Z")
    obj.setdefault("level", "INFO")
    obj.setdefault("logger", "vgi_rpc.access")
    obj.setdefault("message", "record_too_large")
    jsonschema.validate(obj, schema)


def test_formatter_rejects_zero_max_bytes() -> None:
    """max_record_bytes must be positive."""
    with pytest.raises(ValueError):
        VgiAccessLogFormatter(max_record_bytes=0)


def _drain_handlers() -> None:
    """Detach and close every handler currently attached to vgi_rpc.access."""
    access = logging.getLogger("vgi_rpc.access")
    for h in list(access.handlers):
        access.removeHandler(h)
        h.close()


def test_configure_access_log_plain(tmp_path: Path) -> None:
    """Without rotation flags a plain FileHandler is attached."""
    _drain_handlers()
    target = tmp_path / "access.jsonl"
    _configure_access_log(
        path=str(target),
        max_bytes=0,
        backup_count=5,
        when=None,
        max_record_bytes=65536,
        server_id="serverA",
    )
    handlers = logging.getLogger("vgi_rpc.access").handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.FileHandler)
    assert not isinstance(handlers[0], (RotatingFileHandler, TimedRotatingFileHandler))
    _drain_handlers()


def test_configure_access_log_size_rotation(tmp_path: Path) -> None:
    """max_bytes > 0 selects RotatingFileHandler."""
    _drain_handlers()
    target = tmp_path / "access.jsonl"
    _configure_access_log(
        path=str(target),
        max_bytes=1024,
        backup_count=3,
        when=None,
        max_record_bytes=65536,
        server_id="serverA",
    )
    handlers = logging.getLogger("vgi_rpc.access").handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], RotatingFileHandler)
    _drain_handlers()


def test_configure_access_log_time_rotation(tmp_path: Path) -> None:
    """When set, this selects TimedRotatingFileHandler."""
    _drain_handlers()
    target = tmp_path / "access.jsonl"
    _configure_access_log(
        path=str(target),
        max_bytes=0,
        backup_count=3,
        when="H",
        max_record_bytes=65536,
        server_id="serverA",
    )
    handlers = logging.getLogger("vgi_rpc.access").handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], TimedRotatingFileHandler)
    _drain_handlers()


def test_configure_access_log_substitutes_placeholders(tmp_path: Path) -> None:
    """{pid} and {server_id} placeholders in the path are expanded."""
    _drain_handlers()
    target_template = str(tmp_path / "access-{pid}-{server_id}.jsonl")
    _configure_access_log(
        path=target_template,
        max_bytes=0,
        backup_count=5,
        when=None,
        max_record_bytes=65536,
        server_id="abc123def456",
    )
    handler = logging.getLogger("vgi_rpc.access").handlers[0]
    assert isinstance(handler, logging.FileHandler)
    expected = str(tmp_path / f"access-{os.getpid()}-abc123def456.jsonl")
    assert handler.baseFilename == expected
    _drain_handlers()


def test_configure_access_log_creates_parent_dir(tmp_path: Path) -> None:
    """Missing parent directories are created automatically."""
    _drain_handlers()
    target = tmp_path / "nested" / "subdir" / "access.jsonl"
    _configure_access_log(
        path=str(target),
        max_bytes=0,
        backup_count=5,
        when=None,
        max_record_bytes=65536,
        server_id="x",
    )
    assert target.parent.is_dir()
    _drain_handlers()


def test_size_rotation_actually_rotates(tmp_path: Path) -> None:
    """Writing past max_bytes creates a .1 backup file."""
    _drain_handlers()
    target = tmp_path / "access.jsonl"
    _configure_access_log(
        path=str(target),
        max_bytes=512,
        backup_count=3,
        when=None,
        max_record_bytes=65536,
        server_id="serverA",
    )
    access = logging.getLogger("vgi_rpc.access")
    payload = _required_envelope(request_data="A" * 256)
    for _ in range(20):
        access.info("ConformanceService.greet ok", extra=payload)
    rotated = sorted(p.name for p in tmp_path.iterdir())
    assert any(name.endswith(".jsonl.1") for name in rotated)
    _drain_handlers()
