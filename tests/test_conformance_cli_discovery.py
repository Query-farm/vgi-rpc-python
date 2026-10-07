# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The conformance CLI announces a socket only once it can be connected to.

Every port's client tests spawn ``vgi-rpc-conformance --unix PATH`` (or
``--http``), read the discovery line, and connect.  ``_serve_unix`` used to
print ``UNIX:<path>`` *before* ``serve_unix`` bound the socket, so a reader
that connected straight away raced the bind and sometimes got
``ENOENT``/``ECONNREFUSED``.  The C# port hit it and worked around it with a
connect-retry loop -- which hides the bug rather than fixing it.  The TCP
line was already printed from ``on_bound``; these tests hold UNIX and HTTP
to the same rule: connect once, immediately, with no retry.
"""

from __future__ import annotations

import socket
import subprocess
import sys
from collections.abc import Callable

import pytest

from vgi_rpc.conformance import _cli

from .conftest import _short_unix_path, _spawn_ready_process

_ATTEMPTS = 15

_needs_unix = pytest.mark.skipif(
    sys.platform == "win32" or not hasattr(socket, "AF_UNIX"),
    reason="Unix domain sockets are not served on Windows",
)


def _connect_unix_once(path: str) -> None:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(path)
    finally:
        sock.close()


@_needs_unix
def test_unix_line_is_printed_from_on_bound(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Nothing is printed before the transport reports the socket bound.

    Deterministic counterpart to the subprocess loop below: a fake
    ``serve_unix`` checks stdout is still empty at the moment it would bind,
    then fires ``on_bound`` and checks the line appeared only then.
    """
    seen: dict[str, str] = {}

    def fake_serve_unix(
        server: object,
        path: str,
        *,
        threaded: bool = False,
        max_connections: int | None = None,
        on_bound: Callable[[str], None] | None = None,
    ) -> None:
        seen["before_bind"] = capsys.readouterr().out
        assert on_bound is not None, "the discovery line must come from on_bound"
        on_bound(path)
        seen["after_bind"] = capsys.readouterr().out

    import vgi_rpc.rpc

    monkeypatch.setattr(vgi_rpc.rpc, "serve_unix", fake_serve_unix)
    _cli._serve_unix(object(), "/tmp/vgi-discovery.sock")  # type: ignore[arg-type]
    assert seen["before_bind"] == ""
    assert seen["after_bind"] == "UNIX:/tmp/vgi-discovery.sock\n"


@_needs_unix
@pytest.mark.parametrize("attempt", range(_ATTEMPTS))
def test_unix_connect_immediately_after_discovery_line(attempt: int) -> None:
    """A single connect right after reading ``UNIX:<path>`` succeeds."""
    path = _short_unix_path("disc")
    cmd = [sys.executable, "-m", "vgi_rpc.conformance._cli", "--unix", path]
    with _spawn_ready_process(cmd) as (_, line):
        assert line == f"UNIX:{path}", f"attempt {attempt}: got {line!r}"
        _connect_unix_once(path)


@pytest.mark.parametrize("attempt", range(_ATTEMPTS))
def test_http_connect_immediately_after_discovery_line(attempt: int) -> None:
    """A single connect right after reading ``PORT:<n>`` succeeds."""
    pytest.importorskip("waitress")
    pytest.importorskip("vgi_rpc.http")
    cmd = [sys.executable, "-m", "vgi_rpc.conformance._cli", "--http"]
    with _spawn_ready_process(cmd) as (_, line):
        assert line.startswith("PORT:"), f"attempt {attempt}: got {line!r}"
        port = int(line.split(":", 1)[1])
        with socket.create_connection(("127.0.0.1", port), timeout=5):
            pass


def test_cli_module_is_runnable() -> None:
    """``python -m vgi_rpc.conformance._cli`` is how the tests above spawn it."""
    proc = subprocess.run(
        [sys.executable, "-m", "vgi_rpc.conformance._cli", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--unix" in proc.stdout
