# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""JSON formatter for structured logging output.

Provides :class:`VgiJsonFormatter`, a :class:`logging.Formatter` subclass
that serializes log records as single-line JSON objects.  All ``extra``
fields attached to a record (via ``LoggerAdapter`` or per-call ``extra``)
are automatically included — no allowlist to maintain.

This module is **not** auto-imported by ``vgi_rpc``; import it explicitly::

    from vgi_rpc.logging_utils import VgiJsonFormatter
"""

from __future__ import annotations

import json
import logging

__all__ = ["VgiJsonFormatter"]

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

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a single-line JSON string."""
        record.message = record.getMessage()
        obj: dict[str, object] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
            # Emit extra fields, excluding default record attrs and reserved output keys
            **{k: v for k, v in record.__dict__.items() if k not in _DEFAULT_RECORD_ATTRS and k not in _RESERVED_KEYS},
        }
        if record.exc_info and record.exc_info[1]:
            obj["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            obj["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(obj, default=str)
