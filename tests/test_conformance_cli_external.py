# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The conformance CLI externalizes over byte-stream transports.

Every port's ``conformance_bytestream_external_target`` fixture spawns
``vgi-rpc-conformance --pipe --fake-storage URL --externalize-threshold 1``
to get a *reference peer* whose externalised bytes their client must be able
to read.  That makes this CLI a cross-language contract artefact, not a test
helper: if it stops externalising, six ports' byte-stream lanes go green
while testing nothing, which is the precise failure mode the
``TestExternalByteStream`` group exists to end.

The reference's own fixture serves the group in-process via ``serve_pipe``,
so it does **not** cover this path -- the CLI can break with that group fully
green.  Hence a separate test that spawns the real subprocess.
"""

from __future__ import annotations

import json
import sys
import urllib.request

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.conformance.fake_storage import serve_in_thread
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.rpc import connect


def _object_count(base_url: str) -> int:
    with urllib.request.urlopen(base_url.rstrip("/") + "/_stats") as response:
        return int(json.loads(response.read()).get("object_count", 0))


def test_cli_externalizes_over_a_subprocess_pipe() -> None:
    """``--fake-storage`` on ``--pipe`` really pushes bytes through storage."""
    base_url, shutdown = serve_in_thread()
    try:
        argv = [
            sys.executable,
            "-m",
            "vgi_rpc.conformance._cli",
            "--pipe",
            "--fake-storage",
            base_url,
            "--externalize-threshold",
            "1",
        ]
        before = _object_count(base_url)
        client_config = ExternalLocationConfig(storage=None, url_validator=None)
        with connect(ConformanceService, argv, external_location=client_config) as proxy:
            assert proxy.echo_string(value="hello-over-a-pipe") == "hello-over-a-pipe"
            assert len(list(proxy.produce_n(count=3))) == 3
        after = _object_count(base_url)
    finally:
        shutdown()

    # Both a unary result and a producer stream's batches must have gone
    # through storage -- an assertion that the threshold is actually in
    # force, not merely accepted on the command line.
    assert after > before, f"CLI accepted --fake-storage but uploaded nothing ({before} -> {after})"


def test_loopback_validator_refuses_a_non_loopback_url() -> None:
    """The CLI's validator is narrower than ``url_validator=None``.

    A port copying this configuration must not inherit "validate nothing" --
    the fixture needs loopback fake storage, not arbitrary hosts.
    """
    import pytest

    from vgi_rpc.conformance._cli import _loopback_only_validator

    _loopback_only_validator("http://127.0.0.1:8080/blob/1")
    _loopback_only_validator("http://localhost:8080/blob/1")
    with pytest.raises(ValueError, match="non-loopback"):
        _loopback_only_validator("http://evil.example.com/blob/1")
    with pytest.raises(ValueError, match="non-HTTP"):
        _loopback_only_validator("file:///etc/passwd")
