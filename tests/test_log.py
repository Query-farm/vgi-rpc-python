"""Tests for vgi_rpc.log — Level enum and Message class."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from vgi_rpc.log import Level, Message

# ---------------------------------------------------------------------------
# Message.__eq__
# ---------------------------------------------------------------------------


class TestMessageEq:
    """Tests for Message equality and inequality."""

    @pytest.mark.parametrize(
        ("a", "b"),
        [
            (Message(Level.INFO, "hello"), Message(Level.INFO, "hello")),
            (Message(Level.DEBUG, "x", key="val"), Message(Level.DEBUG, "x", key="val")),
        ],
        ids=["same", "with_extra"],
    )
    def test_eq(self, a: Message, b: Message) -> None:
        """Two identical messages are equal."""
        assert a == b

    def test_neq_different_level(self) -> None:
        """Messages with different levels are not equal."""
        a = Message(Level.INFO, "hello")
        b = Message(Level.ERROR, "hello")
        assert a != b

    def test_neq_different_message(self) -> None:
        """Messages with different text are not equal."""
        a = Message(Level.INFO, "hello")
        b = Message(Level.INFO, "world")
        assert a != b

    def test_neq_different_extra(self) -> None:
        """Messages with different extras are not equal."""
        a = Message(Level.INFO, "hello", key="a")
        b = Message(Level.INFO, "hello", key="b")
        assert a != b

    def test_eq_non_message_returns_not_implemented(self) -> None:
        """Comparing with a non-Message returns NotImplemented."""
        m = Message(Level.INFO, "hello")
        assert m.__eq__("not a message") is NotImplemented
        assert m.__eq__(42) is NotImplemented
        assert m.__eq__(None) is NotImplemented


# ---------------------------------------------------------------------------
# Message.__repr__
# ---------------------------------------------------------------------------


class TestMessageRepr:
    """Tests for Message repr."""

    def test_repr_without_extra(self) -> None:
        """Repr without extra has no kwargs."""
        m = Message(Level.INFO, "hello")
        r = repr(m)
        assert "Level.INFO" in r
        assert "'hello'" in r
        assert "**" not in r

    def test_repr_with_extra(self) -> None:
        """Repr with extra includes the kwargs dict."""
        m = Message(Level.DEBUG, "test", key="value")
        r = repr(m)
        assert "Level.DEBUG" in r
        assert "'test'" in r
        assert "**" in r
        assert "'key'" in r


# ---------------------------------------------------------------------------
# Factory class methods
# ---------------------------------------------------------------------------


class TestMessageFactories:
    """Tests for convenience factory methods."""

    @pytest.mark.parametrize(
        ("factory", "level"),
        [
            (Message.exception, Level.EXCEPTION),
            (Message.error, Level.ERROR),
            (Message.warn, Level.WARN),
            (Message.info, Level.INFO),
            (Message.debug, Level.DEBUG),
            (Message.trace, Level.TRACE),
        ],
    )
    def test_factory_creates_correct_level(self, factory: Callable[..., Message], level: Level) -> None:
        """Each factory creates the correct level with the given message."""
        m = factory("msg")
        assert m.level == level
        assert m.message == "msg"

    def test_factory_with_extras(self) -> None:
        """Factory methods pass through extra kwargs."""
        m = Message.exception("boom", detail="info")
        assert m.extra == {"detail": "info"}


# ---------------------------------------------------------------------------
# should_terminate
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# add_to_metadata
# ---------------------------------------------------------------------------


class TestAddToMetadata:
    """Tests for add_to_metadata()."""

    def test_creates_new_dict(self) -> None:
        """None metadata creates a fresh dict."""
        m = Message.info("hello")
        result = m.add_to_metadata(None)
        assert result["vgi_rpc.log_level"] == "INFO"
        assert result["vgi_rpc.log_message"] == "hello"
        # No extra kwargs → log_extra key is omitted
        assert "vgi_rpc.log_extra" not in result

    def test_augments_existing(self) -> None:
        """Existing metadata is preserved."""
        m = Message.warn("caution")
        result = m.add_to_metadata({"existing": "key"})
        assert result["existing"] == "key"
        assert result["vgi_rpc.log_level"] == "WARN"

    def test_extra_included(self) -> None:
        """Extra kwargs are in the log_extra JSON."""
        import json

        m = Message.debug("dbg", foo="bar")
        result = m.add_to_metadata()
        extra = json.loads(result["vgi_rpc.log_extra"])
        assert extra["foo"] == "bar"
        assert "pid" not in extra


# ---------------------------------------------------------------------------
# from_exception
# ---------------------------------------------------------------------------


class TestFromException:
    """Tests for from_exception()."""

    def test_basic_exception(self) -> None:
        """Basic exception captures type and message."""
        try:
            raise ValueError("test error")
        except ValueError as e:
            m = Message.from_exception(e)

        assert m.level == Level.EXCEPTION
        assert "ValueError" in m.message
        assert "test error" in m.message
        assert m.extra is not None
        assert m.extra["exception_type"] == "ValueError"
        assert m.extra["exception_message"] == "test error"
        assert "traceback" in m.extra
        assert "frames" in m.extra

    def test_long_traceback_truncated(self) -> None:
        """Traceback longer than _MAX_TRACEBACK_CHARS is truncated."""

        # Create a deeply nested call to produce a long traceback
        def recursive(n: int) -> None:
            if n <= 0:
                raise RuntimeError("deep " + "x" * 20000)
            recursive(n - 1)

        try:
            recursive(50)
        except RuntimeError as e:
            m = Message.from_exception(e)

        assert m.extra is not None
        tb = str(m.extra["traceback"])
        assert len(tb) <= Message._MAX_TRACEBACK_CHARS + 50  # small margin for suffix

    def test_chained_cause(self) -> None:
        """Exception with __cause__ captures the cause chain."""
        try:
            try:
                raise ValueError("root cause")
            except ValueError as inner:
                raise RuntimeError("wrapper") from inner
        except RuntimeError as e:
            m = Message.from_exception(e)

        assert m.extra is not None
        assert "cause" in m.extra
        assert "ValueError" in str(m.extra["cause"])

    def test_implicit_context(self) -> None:
        """Exception with __context__ (no explicit from) captures context."""
        try:
            try:
                raise ValueError("original")
            except ValueError:
                raise RuntimeError("during handling")  # noqa: B904
        except RuntimeError as e:
            m = Message.from_exception(e)

        assert m.extra is not None
        assert "context" in m.extra
        assert "ValueError" in str(m.extra["context"])

    def test_suppressed_context_not_included(self) -> None:
        """Exception with suppress_context=True does not include context."""
        try:
            try:
                raise ValueError("original")
            except ValueError:
                exc = RuntimeError("suppressed")
                exc.__suppress_context__ = True
                raise exc  # noqa: B904
        except RuntimeError as e:
            m = Message.from_exception(e)

        assert m.extra is not None
        assert "context" not in m.extra
