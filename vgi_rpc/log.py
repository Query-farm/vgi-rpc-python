"""Logging utilities for VGI functions.

This module provides Level and Message for emitting diagnostic information
during RPC method processing. Log messages are transmitted to the client as
zero-row batches with log metadata, interleaved with data batches in the
IPC stream.

CONVENIENCE CONSTRUCTORS
------------------------
Message provides factory methods for each level:

    Message.exception("Error occurred", traceback="...")
    Message.error("Something went wrong")
    Message.warn("Deprecated usage")
    Message.info("Processing started")
    Message.debug("Variable value", x=42)
    Message.trace("Detailed trace")

EXCEPTION HANDLING
------------------
When an exception occurs, use Message.from_exception() to capture
the full traceback:

    try:
        risky_operation()
    except Exception as e:
        emit_client_log(Message.from_exception(e))  # Includes traceback

KEY CLASSES
-----------
Level : Enum with EXCEPTION, ERROR, WARN, INFO, DEBUG, TRACE
Message : Log message with level, message text, and optional extras

"""

from __future__ import annotations

import json
import traceback
from enum import Enum
from typing import ClassVar

from vgi_rpc.metadata import LOG_EXTRA_KEY, LOG_LEVEL_KEY, LOG_MESSAGE_KEY

__all__ = [
    "Level",
    "Message",
]


class Level(Enum):
    """Severity levels for log messages emitted during function processing.

    Levels are ordered from most to least severe. Use the appropriate level
    to indicate the nature of the message:

    Attributes:
        EXCEPTION: Unrecoverable error that terminated processing.
        ERROR: Significant error that may affect results but didn't terminate.
        WARN: Potential issue that should be reviewed but isn't necessarily wrong.
        INFO: General informational message about processing status.
        DEBUG: Detailed information useful for debugging.
        TRACE: Fine-grained tracing information for detailed diagnostics.

    """

    EXCEPTION = "EXCEPTION"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


class Message:
    """Log message emitted during RPC method processing.

    Messages are emitted via the ``emit_client_log`` callback or ``OutputCollector.client_log()``
    and transmitted to the client as zero-row batches with log metadata.

    Attributes:
        level: Severity level indicating the nature of the message.
        message: Human-readable log message text.
        extra: Additional arbitrary key-value pairs to include in the JSON output.

    """

    __slots__ = ("extra", "level", "message")
    __hash__ = None  # type: ignore[assignment]  # Unhashable since we define __eq__

    _MAX_TRACEBACK_CHARS: ClassVar[int] = 16_000
    _MAX_TRACEBACK_FRAMES: ClassVar[int] = 5

    def __init__(self, level: Level, message: str, **kwargs: object) -> None:
        """Create a log message with level, message text, and optional extras."""
        self.level = level
        self.message = message
        self.extra: dict[str, object] | None = kwargs if kwargs else None

    def __eq__(self, other: object) -> bool:
        """Compare log messages by level, message, and extra fields."""
        if not isinstance(other, Message):
            return NotImplemented
        return self.level == other.level and self.message == other.message and self.extra == other.extra

    def __repr__(self) -> str:
        """Return a string representation suitable for debugging."""
        if self.extra:
            return f"Message({self.level!r}, {self.message!r}, **{self.extra!r})"
        return f"Message({self.level!r}, {self.message!r})"

    @classmethod
    def exception(cls, message: str, **kwargs: object) -> Message:
        """Create an EXCEPTION level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.EXCEPTION, message, **kwargs)

    @classmethod
    def error(cls, message: str, **kwargs: object) -> Message:
        """Create an ERROR level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.ERROR, message, **kwargs)

    @classmethod
    def warn(cls, message: str, **kwargs: object) -> Message:
        """Create a WARN level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.WARN, message, **kwargs)

    @classmethod
    def info(cls, message: str, **kwargs: object) -> Message:
        """Create an INFO level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.INFO, message, **kwargs)

    @classmethod
    def debug(cls, message: str, **kwargs: object) -> Message:
        """Create a DEBUG level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.DEBUG, message, **kwargs)

    @classmethod
    def trace(cls, message: str, **kwargs: object) -> Message:
        """Create a TRACE level log message.

        Additional kwargs are stored in the extra field.
        """
        return cls(Level.TRACE, message, **kwargs)

    def add_to_metadata(
        self,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Add log message fields to an existing metadata dictionary.

        Creates a new dictionary with log-related keys added. Does not mutate
        the input dictionary.

        Args:
            metadata: Existing metadata dict to augment, or None to create new.

        Returns:
            New dict containing original entries plus:
            - vgi_rpc.log_level: The Level value (e.g., "INFO", "EXCEPTION")
            - vgi_rpc.log_message: The human-readable message text
            - vgi_rpc.log_extra: JSON string with extra kwargs (omitted when empty)

        """
        result = dict(metadata) if metadata else {}
        level_key: str = LOG_LEVEL_KEY.decode()
        message_key: str = LOG_MESSAGE_KEY.decode()
        result[level_key] = self.level.value
        result[message_key] = self.message
        if self.extra:
            extra_key: str = LOG_EXTRA_KEY.decode()
            result[extra_key] = json.dumps(self.extra)
        return result

    @classmethod
    def from_exception(cls, exc: BaseException) -> Message:
        """Produce a Message from an exception."""
        tb_exc = traceback.TracebackException.from_exception(
            exc,
            capture_locals=False,
        )

        formatted_tb = "".join(tb_exc.format())
        if len(formatted_tb) > cls._MAX_TRACEBACK_CHARS:
            formatted_tb = formatted_tb[: cls._MAX_TRACEBACK_CHARS] + "\n… <traceback truncated>"

        # Short, semantic summary (LLM anchor)
        summary = f"{type(exc).__name__}: {exc}"

        extra: dict[str, object] = {
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "traceback": formatted_tb,
        }

        if tb_exc.__cause__:
            cause_str = "".join(tb_exc.__cause__.format())
            if len(cause_str) > cls._MAX_TRACEBACK_CHARS:
                cause_str = cause_str[: cls._MAX_TRACEBACK_CHARS] + "\n… <traceback truncated>"
            extra["cause"] = cause_str

        if tb_exc.__context__ and not tb_exc.__suppress_context__:
            context_str = "".join(tb_exc.__context__.format())
            if len(context_str) > cls._MAX_TRACEBACK_CHARS:
                context_str = context_str[: cls._MAX_TRACEBACK_CHARS] + "\n… <traceback truncated>"
            extra["context"] = context_str

        extra["frames"] = [
            {
                "file": f.filename,
                "line": f.lineno,
                "function": f.name,
                "code": f.line,
            }
            for f in tb_exc.stack[-cls._MAX_TRACEBACK_FRAMES :]
        ]

        return cls(
            Level.EXCEPTION,
            summary,
            **extra,
        )
