"""Tests for vgi_rpc.log — Level enum and Message class."""

from __future__ import annotations

from vgi_rpc.log import Level, Message

# ---------------------------------------------------------------------------
# Message.__eq__
# ---------------------------------------------------------------------------


class TestMessageEq:
    """Tests for Message equality and inequality."""

    def test_eq_same(self) -> None:
        """Two identical messages are equal."""
        a = Message(Level.INFO, "hello")
        b = Message(Level.INFO, "hello")
        assert a == b

    def test_eq_with_extra(self) -> None:
        """Two messages with same extra are equal."""
        a = Message(Level.DEBUG, "x", key="val")
        b = Message(Level.DEBUG, "x", key="val")
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

    def test_exception(self) -> None:
        """Message.exception() creates EXCEPTION level."""
        m = Message.exception("boom", detail="info")
        assert m.level == Level.EXCEPTION
        assert m.message == "boom"
        assert m.extra == {"detail": "info"}

    def test_error(self) -> None:
        """Message.error() creates ERROR level."""
        m = Message.error("fail")
        assert m.level == Level.ERROR
        assert m.message == "fail"
        assert m.extra is None

    def test_warn(self) -> None:
        """Message.warn() creates WARN level."""
        m = Message.warn("careful")
        assert m.level == Level.WARN

    def test_info(self) -> None:
        """Message.info() creates INFO level."""
        m = Message.info("ok")
        assert m.level == Level.INFO

    def test_debug(self) -> None:
        """Message.debug() creates DEBUG level."""
        m = Message.debug("dbg", x=1)
        assert m.level == Level.DEBUG
        assert m.extra == {"x": 1}

    def test_trace(self) -> None:
        """Message.trace() creates TRACE level."""
        m = Message.trace("fine")
        assert m.level == Level.TRACE


# ---------------------------------------------------------------------------
# should_terminate
# ---------------------------------------------------------------------------


class TestShouldTerminate:
    """Tests for should_terminate()."""

    def test_exception_terminates(self) -> None:
        """EXCEPTION level should terminate."""
        assert Message(Level.EXCEPTION, "x").should_terminate() is True

    def test_non_exception_does_not_terminate(self) -> None:
        """Non-EXCEPTION levels should not terminate."""
        for level in (Level.ERROR, Level.WARN, Level.INFO, Level.DEBUG, Level.TRACE):
            assert Message(level, "x").should_terminate() is False


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
        assert "vgi_rpc.log_extra" in result

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
        assert "pid" in extra


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
