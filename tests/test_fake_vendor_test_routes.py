# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The fake vendor's test routes: ``/__stats__``, download faults, the CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator

import httpx2
import pyarrow as pa
import pytest

from vgi_rpc.conformance.fake_vendor import (
    STATS_KEYS,
    TEST_ROUTE_BODY_BYTES,
    FakeVendorServer,
    serve_fake_vendor,
)
from vgi_rpc.vendor import VendorBlobClient, VendorError, VendorStorage

pytest.importorskip("aiohttp", reason="blob downloads need vgi-rpc[external]")

_TOKEN = "test-token"
_SCHEMA = pa.schema([pa.field("x", pa.int64())])


@pytest.fixture
def fake() -> Iterator[FakeVendorServer]:
    """Run a fake vendor with test routes."""
    with serve_fake_vendor({_TOKEN: "tenant-a"}, test_routes=True) as server:
        yield server


@pytest.fixture
def blobs(fake: FakeVendorServer) -> Iterator[VendorBlobClient]:
    """Return a purpose ``fs`` blob client, no retries."""
    with VendorBlobClient(fake.url, _TOKEN, backoff_seconds=0.0, max_attempts=1) as client:
        yield client


def _get(fake: FakeVendorServer, path: str) -> dict[str, object]:
    resp = httpx2.get(fake.url + path, timeout=10.0)
    resp.raise_for_status()
    body: dict[str, object] = json.loads(resp.content)
    return body


def test_routes_off_by_default() -> None:
    """Without ``test_routes`` the routes do not exist."""
    with serve_fake_vendor() as server:
        assert httpx2.get(server.url + "/__stats__", timeout=10.0).status_code == 404
        assert httpx2.get(server.url + "/__admin__/reset", timeout=10.0).status_code == 404


def test_stats_keys_and_counting(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """``/__stats__`` has exactly the documented keys and counts calls and bytes."""
    before = _get(fake, "/__stats__")
    assert tuple(before) == STATS_KEYS
    assert all(v == 0 for v in before.values())
    ref = blobs.put_bytes(b"x" * 1000)
    assert blobs.get_bytes(ref) == b"x" * 1000
    mid = _get(fake, "/__stats__")
    assert mid["puts"] == 1 and mid["gets"] == 1 and mid["storage_puts"] == 1
    assert mid["objects"] == 1 and mid["bytes"] == 1000
    assert mid["bytes_uploaded"] == 1000
    # The client probes with a 1-byte ranged GET, then fetches the body.
    assert 1000 <= mid["bytes_downloaded"] <= 1001  # type: ignore[operator]
    blobs.delete([ref])
    after = _get(fake, "/__stats__")
    assert after["deletes"] == 1 and after["objects"] == 0 and after["bytes"] == 0
    assert after["bytes_uploaded"] == 1000
    assert fake.vendor.test_stats() == after


def test_bodies_fixed_length_head_and_range(fake: FakeVendorServer) -> None:
    """Bodies are padded to a fixed size; HEAD agrees with GET; ranges work; no caching."""
    head = httpx2.head(fake.url + "/__stats__", timeout=10.0)
    get = httpx2.get(fake.url + "/__stats__", timeout=10.0)
    assert head.status_code == 200 and get.status_code == 200
    assert int(head.headers["content-length"]) == len(get.content) == TEST_ROUTE_BODY_BYTES
    assert get.headers["cache-control"] == "no-store"
    assert get.headers["content-type"] == "application/json"
    part = httpx2.get(fake.url + "/__stats__", headers={"Range": "bytes=0-9"}, timeout=10.0)
    assert part.status_code == 206 and part.content == get.content[:10]


def test_fault_corrupt_is_sticky_until_reset(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """A corrupt fault fails the integrity check on every read of that object until reset."""
    ref = blobs.put_bytes(b"payload" * 100)
    assert _get(fake, "/__admin__/fault?purpose=fs&action=corrupt&count=1")["ok"] is True
    for _ in range(2):
        with pytest.raises(VendorError) as exc:
            blobs.get_bytes(ref)
        assert exc.value.code == "sha256_mismatch"
    corrupted = next(iter(fake.vendor._faulted))
    assert corrupted == ref.object_id
    _get(fake, "/__admin__/reset")
    assert blobs.get_bytes(ref) == b"payload" * 100


def test_fault_corrupt_same_length_other_bytes(fake: FakeVendorServer) -> None:
    """Corrupted downloads (purpose rpc here) keep the length and change every byte."""
    with VendorStorage(fake.url, _TOKEN, backoff_seconds=0.0) as storage:
        url = storage.upload(b"\x00\x01abc", _SCHEMA)
    _get(fake, "/__admin__/fault?purpose=rpc&action=corrupt&count=1")
    assert httpx2.head(url, timeout=10.0).status_code == 200  # HEAD: no effect
    assert not fake.vendor._faulted
    body = httpx2.get(url, timeout=10.0).content
    assert body == bytes(b ^ 0xFF for b in b"\x00\x01abc")
    ranged = httpx2.get(url, headers={"Range": "bytes=2-4"}, timeout=10.0)
    assert ranged.status_code == 206 and ranged.content == body[2:5]


def test_fault_missing(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """A missing fault answers 404 to the download."""
    ref = blobs.put_bytes(b"gone" * 50)
    _get(fake, "/__admin__/fault?purpose=fs&action=missing&count=1")
    with pytest.raises(VendorError) as exc:
        blobs.get_bytes(ref)
    assert exc.value.status == 404


def test_fault_counts_objects_and_purpose(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """``count`` objects of the faulted purpose are hit; others and later ones are not."""
    refs = [blobs.put_bytes(bytes([i]) * 64) for i in range(3)]
    with VendorStorage(fake.url, _TOKEN, backoff_seconds=0.0) as storage:
        rpc_url = storage.upload(b"rpc-bytes", _SCHEMA)
    _get(fake, "/__admin__/fault?purpose=fs&action=corrupt&count=2")
    assert httpx2.get(rpc_url, timeout=10.0).content == b"rpc-bytes"
    failures = 0
    for ref in refs:
        try:
            blobs.get_bytes(ref)
        except VendorError:
            failures += 1
    assert failures == 2


def test_fault_replaces_and_disarms(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """Arming again replaces the fault for that purpose; count=0 disarms; HEAD never arms."""
    ref = blobs.put_bytes(b"z" * 100)
    _get(fake, "/__admin__/fault?purpose=fs&action=corrupt&count=1")
    _get(fake, "/__admin__/fault?purpose=fs&action=corrupt&count=1")  # repeat: still 1
    assert fake.vendor._armed == {"fs": ("corrupt", 1)}
    _get(fake, "/__admin__/fault?purpose=fs&action=corrupt&count=0")
    assert blobs.get_bytes(ref) == b"z" * 100
    httpx2.head(fake.url + "/__admin__/fault?purpose=fs&action=missing&count=1", timeout=10.0)
    assert blobs.get_bytes(ref) == b"z" * 100


@pytest.mark.parametrize(
    "query",
    [
        "purpose=xx&action=corrupt",
        "purpose=fs&action=burn",
        "purpose=fs&action=corrupt&count=-1",
        "purpose=fs&action=corrupt&count=a",
        "purpose=upload&action=missing",
        "purpose=upload&action=corrupt&count=-1",
    ],
)
def test_fault_rejects_bad_parameters(fake: FakeVendorServer, query: str) -> None:
    """Unknown purpose/action or a bad count is a 400."""
    assert httpx2.get(f"{fake.url}/__admin__/fault?{query}", timeout=10.0).status_code == 400


def test_upload_fault_corrupts_next_puts_in_transit(fake: FakeVendorServer) -> None:
    """``purpose=upload&action=corrupt&count=N`` damages N signed PUTs; the store refuses them (BadDigest)."""
    assert _get(fake, "/__admin__/fault?purpose=upload&action=corrupt&count=1")["ok"] is True
    with VendorStorage(fake.url, _TOKEN, backoff_seconds=0.0) as storage:
        with pytest.raises(VendorError) as exc:
            storage.upload(b"corrupt me", _SCHEMA)
        assert exc.value.code == "BadDigest"
        stats = _get(fake, "/__stats__")
        assert stats["uploads_corrupted"] == 1
        assert stats["upload_bad_digest"] == 1 and stats["upload_refusals"] == 1
        assert stats["objects"] == 0
        # Spent after one PUT.
        url = storage.upload(b"intact", _SCHEMA)
        assert httpx2.get(url, timeout=10.0).content == b"intact"


def test_reset_clears_upload_fault(fake: FakeVendorServer) -> None:
    """``/__admin__/reset`` disarms an armed upload fault; HEAD never arms one."""
    httpx2.head(fake.url + "/__admin__/fault?purpose=upload&action=corrupt&count=1", timeout=10.0)
    _get(fake, "/__admin__/fault?purpose=upload&action=corrupt&count=3")
    _get(fake, "/__admin__/reset")
    with VendorStorage(fake.url, _TOKEN, backoff_seconds=0.0) as storage:
        url = storage.upload(b"after reset", _SCHEMA)
    assert httpx2.get(url, timeout=10.0).content == b"after reset"
    assert _get(fake, "/__stats__")["uploads_corrupted"] == 0


def test_reset_clears_fail_next(fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
    """``/__admin__/reset`` also drops pending ``fail_next`` faults."""
    fake.vendor.fail_next("/v1/blobs/put", count=5)
    _get(fake, "/__admin__/reset")
    assert blobs.put_bytes(b"ok").size == 2


def test_cli_prints_port_and_url() -> None:
    """``vgi-rpc-fake-vendor`` prints PORT and URL and honours --token and --test-routes."""
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from vgi_rpc.conformance.fake_vendor import main; main()",
            "--token",
            "T=ten",
            "--token",
            "U",
            "--test-routes",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        port_line = proc.stdout.readline().strip()
        url_line = proc.stdout.readline().strip()
        assert port_line.startswith("PORT:")
        assert url_line == f"URL:http://127.0.0.1:{port_line[5:]}"
        base = url_line[4:]
        assert httpx2.get(base + "/__stats__", timeout=10.0).json()["puts"] == 0
        info = httpx2.get(base + "/v1/info", headers={"Authorization": "Bearer T"}, timeout=10.0)
        assert info.json()["tenant"] == "ten"
        info = httpx2.get(base + "/v1/info", headers={"Authorization": "Bearer U"}, timeout=10.0)
        assert info.json()["tenant"] == "tenant-a"
    finally:
        proc.terminate()
        proc.wait(timeout=10)
