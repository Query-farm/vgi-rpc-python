# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The server log level of a method error follows its canonical code.

Client-error codes log at INFO and transient codes at WARNING, both as one
line without a traceback; ``UNKNOWN`` / ``INTERNAL`` / ``DATA_LOSS`` keep the
ERROR-with-traceback line.  None of this changes the wire or the access log.
"""

from __future__ import annotations

import logging
from typing import Protocol

import pytest

from vgi_rpc.errors import CLIENT_ERROR_CODES, TRANSIENT_ERROR_CODES, Code, StatusError
from vgi_rpc.rpc import RpcError, _log_method_error, serve_pipe

_SERVER_LOGGER = "vgi_rpc.rpc"


def _server_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == _SERVER_LOGGER and r.getMessage().startswith("Error in ")]


def _raise_and_log(exc: BaseException) -> None:
    try:
        raise exc
    except BaseException as caught:
        _log_method_error("Svc", "m", "srv-1", caught)


class TestLevelByCode:
    """Unit tests of ``_log_method_error``."""

    @pytest.mark.parametrize("code", sorted(CLIENT_ERROR_CODES))
    def test_client_error_codes_log_info_without_traceback(self, code: Code, caplog: pytest.LogCaptureFixture) -> None:
        """A client error is one INFO line with code, kind, type and message."""
        with caplog.at_level(logging.DEBUG, logger=_SERVER_LOGGER):
            _raise_and_log(StatusError("no such table t", code=code, kind="table_missing"))
        (record,) = _server_records(caplog)
        assert record.levelno == logging.INFO
        assert record.exc_info is None
        assert record.getMessage() == f"Error in Svc.m: {code.value} (table_missing) StatusError: no such table t"
        assert getattr(record, "error_code") == code.value  # noqa: B009
        assert getattr(record, "error_kind") == "table_missing"  # noqa: B009

    @pytest.mark.parametrize("code", sorted(TRANSIENT_ERROR_CODES))
    def test_transient_codes_log_warning_without_traceback(self, code: Code, caplog: pytest.LogCaptureFixture) -> None:
        """A transient error is one WARNING line; no kind means no parenthetical."""
        with caplog.at_level(logging.DEBUG, logger=_SERVER_LOGGER):
            _raise_and_log(StatusError("busy", code=code))
        (record,) = _server_records(caplog)
        assert record.levelno == logging.WARNING
        assert record.exc_info is None
        assert record.getMessage() == f"Error in Svc.m: {code.value} StatusError: busy"
        assert not hasattr(record, "error_kind")

    @pytest.mark.parametrize("code", [Code.UNKNOWN, Code.INTERNAL, Code.DATA_LOSS])
    def test_server_fault_codes_log_error_with_traceback(self, code: Code, caplog: pytest.LogCaptureFixture) -> None:
        """Server faults keep the ERROR line with the traceback."""
        with caplog.at_level(logging.DEBUG, logger=_SERVER_LOGGER):
            _raise_and_log(StatusError("boom", code=code))
        (record,) = _server_records(caplog)
        assert record.levelno == logging.ERROR
        assert record.exc_info is not None
        assert record.exc_info[0] is StatusError
        assert getattr(record, "error_code") == code.value  # noqa: B009

    def test_unclassified_exception_is_error_with_traceback(self, caplog: pytest.LogCaptureFixture) -> None:
        """An exception with no ``error_code`` is UNKNOWN: ERROR with traceback."""
        with caplog.at_level(logging.DEBUG, logger=_SERVER_LOGGER):
            _raise_and_log(ValueError("bad"))
        (record,) = _server_records(caplog)
        assert record.levelno == logging.ERROR
        assert record.exc_info is not None
        assert record.getMessage() == "Error in Svc.m: bad"

    def test_every_code_is_classified_once(self) -> None:
        """The client and transient sets are disjoint and exclude server faults."""
        assert not CLIENT_ERROR_CODES & TRANSIENT_ERROR_CODES
        faults = set(Code) - CLIENT_ERROR_CODES - TRANSIENT_ERROR_CODES
        assert faults == {Code.UNKNOWN, Code.INTERNAL, Code.DATA_LOSS}


class _Svc(Protocol):
    def missing(self) -> None: ...

    def overloaded(self) -> None: ...

    def broken(self) -> None: ...


class _SvcImpl:
    def missing(self) -> None:
        raise StatusError("no such report", code=Code.NOT_FOUND, kind="report_missing")

    def overloaded(self) -> None:
        raise StatusError("try later", code=Code.UNAVAILABLE)

    def broken(self) -> None:
        raise RuntimeError("bug")


class TestEndToEnd:
    """Through a real server: log level changes, wire and access log do not."""

    @pytest.mark.parametrize(
        ("method", "code", "level", "has_tb"),
        [
            ("missing", Code.NOT_FOUND, logging.INFO, False),
            ("overloaded", Code.UNAVAILABLE, logging.WARNING, False),
            ("broken", Code.UNKNOWN, logging.ERROR, True),
        ],
    )
    def test_level_and_wire(
        self,
        method: str,
        code: Code,
        level: int,
        has_tb: bool,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The log level follows the code; the client still gets code and remote traceback."""
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc"),
            serve_pipe(_Svc, _SvcImpl()) as proxy,
            pytest.raises(RpcError) as exc_info,
        ):
            getattr(proxy, method)()

        err = exc_info.value
        assert err.error_code == code.value
        # The wire traceback is still governed by the server's traceback
        # setting (on by default), not by the log level.
        assert err.remote_traceback

        (record,) = _server_records(caplog)
        assert record.levelno == level
        assert (record.exc_info is not None) is has_tb

        access = [r for r in caplog.records if r.name == "vgi_rpc.access"]
        assert access
        assert getattr(access[0], "error_code") == code.value  # noqa: B009
