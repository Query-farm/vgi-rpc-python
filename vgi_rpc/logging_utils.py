# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""JSON formatter for structured logging output.

Provides :class:`VgiJsonFormatter`, a :class:`logging.Formatter` subclass
that serializes log records as single-line JSON objects.  All ``extra``
fields attached to a record (via ``LoggerAdapter`` or per-call ``extra``)
are automatically included — no allowlist to maintain.

Also provides :class:`VgiAccessLogFormatter`, a subclass that enforces a
per-record byte cap so downstream log shippers (Vector, Fluent Bit, …)
do not silently drop oversize lines.

This module is **not** auto-imported by ``vgi_rpc``; import it explicitly::

    from vgi_rpc.logging_utils import VgiJsonFormatter, VgiAccessLogFormatter
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

__all__ = ["VgiAccessLogFormatter", "VgiJsonFormatter"]

# Build the set of attribute names that every LogRecord has by default.
# Anything *not* in this set was injected via ``extra``.
_DEFAULT_RECORD_ATTRS: frozenset[str] = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()) | {
    "message",
    "asctime",
}

_RESERVED_KEYS: frozenset[str] = frozenset({"timestamp", "level", "logger", "message", "exception", "stack_info"})


class VgiJsonFormatter(logging.Formatter):
    """JSON formatter that emits all structured extra fields.

    Standard fields (``timestamp``, ``level``, ``logger``, ``message``) are
    always present and cannot be overwritten by extra fields with the same
    name.  Every other non-default attribute on the ``LogRecord`` is emitted
    as an additional key — this is future-proof because new fields added via
    ``extra`` appear automatically without updating the formatter.

    Exception information is included under the ``"exception"`` key when
    present.

    Non-serializable values are coerced to strings via ``default=str``.
    """

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """Format the record timestamp as RFC 3339 UTC with millisecond precision.

        Per ``docs/access-log-spec.md`` every conformant emitter produces
        timestamps in this exact shape so cross-language logs collate cleanly.
        """
        dt = datetime.fromtimestamp(record.created, tz=UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    def _build_payload(self, record: logging.LogRecord) -> dict[str, object]:
        """Build the JSON-serializable payload dict for *record*."""
        record.message = record.getMessage()
        obj: dict[str, object] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
            **{k: v for k, v in record.__dict__.items() if k not in _DEFAULT_RECORD_ATTRS and k not in _RESERVED_KEYS},
        }
        if record.exc_info and record.exc_info[1]:
            obj["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            obj["stack_info"] = self.formatStack(record.stack_info)
        return obj

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a single-line JSON string."""
        return json.dumps(self._build_payload(record), default=str)


class VgiAccessLogFormatter(VgiJsonFormatter):
    """JSON formatter that caps each emitted record at *max_record_bytes*.

    Vector's ``file`` source defaults to ``max_line_bytes = 102400`` (100 KiB)
    and Fluent Bit's ``tail`` input has a similar per-line ceiling — lines
    exceeding the limit are silently dropped by those shippers.  This
    formatter enforces a configurable cap by progressively shedding the
    largest fields:

    1. Drop ``request_data`` (the usual offender — base64'd Arrow IPC bytes)
       and set ``truncated: true`` plus ``original_request_bytes``.
    2. Drop ``error_message`` and ``claims``.
    3. Fall back to a minimal sentinel record carrying only the
       always-required envelope fields plus ``truncated: "record_too_large"``.

    The default cap (1 MiB) is large enough for almost any realistic record
    while still keeping a hard upper bound on per-line size for shippers.
    Pair it with shipper configs that raise their per-line limits to match
    (the reference configs in ``docs/log-shipping/`` set Vector's
    ``max_line_bytes`` and Fluent Bit's ``Buffer_Max_Size`` to 1 MiB).
    """

    def __init__(self, *, max_record_bytes: int = 1_048_576) -> None:
        """Initialise the formatter.

        Args:
            max_record_bytes: Maximum size in bytes (UTF-8) of one emitted
                JSON line, *excluding* the trailing newline added by the
                logging Handler.  Must be positive.

        """
        if max_record_bytes <= 0:
            raise ValueError(f"max_record_bytes must be positive, got {max_record_bytes}")
        super().__init__()
        self.max_record_bytes = max_record_bytes

    def _encoded_len(self, obj: dict[str, object]) -> int:
        return len(json.dumps(obj, default=str).encode("utf-8"))

    def format(self, record: logging.LogRecord) -> str:
        """Format *record*, truncating fields if it would exceed the cap.

        ``error_message`` is never truncated: the spec explicitly forbids a
        length cap on it (see ``docs/access-log-spec.md`` § 4.1).  If a
        record carrying a huge error message exceeds the cap, the sentinel
        path preserves the full message even though everything else is shed.
        """
        obj = self._build_payload(record)
        if self._encoded_len(obj) <= self.max_record_bytes:
            return json.dumps(obj, default=str)

        request_data = obj.get("request_data")
        if isinstance(request_data, str):
            obj["original_request_bytes"] = len(request_data)
            del obj["request_data"]
            obj["truncated"] = True
            if self._encoded_len(obj) <= self.max_record_bytes:
                return json.dumps(obj, default=str)

        claims = obj.get("claims")
        if claims is not None:
            obj["claims"] = {}
            obj["truncated"] = True
            if self._encoded_len(obj) <= self.max_record_bytes:
                return json.dumps(obj, default=str)

        sentinel: dict[str, object] = {
            "timestamp": obj.get("timestamp"),
            "level": "INFO",
            "logger": "vgi_rpc.access",
            "message": "record_too_large",
            "server_id": obj.get("server_id", ""),
            "protocol": obj.get("protocol", ""),
            "protocol_hash": obj.get("protocol_hash", ""),
            "method": obj.get("method", ""),
            "method_type": obj.get("method_type", "unary"),
            "principal": obj.get("principal", ""),
            "auth_domain": obj.get("auth_domain", ""),
            "authenticated": obj.get("authenticated", False),
            "remote_addr": obj.get("remote_addr", ""),
            "duration_ms": obj.get("duration_ms", 0),
            "status": obj.get("status", "ok"),
            "error_type": obj.get("error_type", ""),
            "truncated": "record_too_large",
        }
        if sentinel["status"] == "error":
            err = obj.get("error_message")
            sentinel["error_message"] = err if isinstance(err, str) and err else "record_too_large"
        return json.dumps(sentinel, default=str)
