# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Query Farm CLI workflow tests."""

from __future__ import annotations

import io
import json
import os
import stat
import time
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest
from typer.testing import CliRunner

from vgi_rpc import qf
from vgi_rpc.stockgate import (
    StockgateAddress,
    StockgateDeviceCredential,
    StockgateEndpoint,
    StockgateError,
    StockgateHeartbeat,
    StockgateRegistrationIntent,
    StockgateService,
    stockgate_identity,
)


class _FakeClient:
    """Capture the CLI's high-level registration request."""

    calls: ClassVar[list[tuple[object, ...]]] = []

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def register_service(self, *args: object, **kwargs: object) -> tuple[StockgateService, StockgateEndpoint]:
        """Return deterministic registered state."""
        self.calls.append((self.base_url, *args, kwargs))
        return (
            StockgateService("svc_test", "echo", True),
            StockgateEndpoint(
                endpoint_id="endpoint-test",
                endpoint_hex="00" * 32,
                service_id="svc_test",
                credential="ec_worker.secret",
                credential_expires_in=3600,
                signed_packet=b"signed",
                tags=(),
                ephemeral=False,
            ),
        )


class _RejectedCacheClient(_FakeClient):
    """Reject a cached credential once, then accept device authorization."""

    attempts: ClassVar[int] = 0

    def authenticate_device(self, **_kwargs: object) -> StockgateDeviceCredential:
        """Return a replacement human credential."""
        return StockgateDeviceCredential("dc_replacement.secret", 3600)

    def register_service(self, *args: object, **kwargs: object) -> tuple[StockgateService, StockgateEndpoint]:
        """Reject the stale cache before delegating to the successful fake."""
        type(self).attempts += 1
        if type(self).attempts == 1:
            raise StockgateError(401, "invalid_credential", "Credential was rejected.")
        return super().register_service(*args, **kwargs)


class _FakeProcess:
    """Small subprocess stand-in with controllable liveness."""

    def __init__(self, status: int | None) -> None:
        self.status = status
        self.terminated = False

    def poll(self) -> int | None:
        """Return the configured process status."""
        return self.status

    def terminate(self) -> None:
        """Record graceful termination."""
        self.terminated = True
        self.status = 0

    def kill(self) -> None:
        """Record forced termination."""
        self.terminate()

    def wait(self, timeout: float | None = None) -> int:
        """Return the terminal status."""
        del timeout
        return self.status or 0


class _RunClient:
    """Capture an ephemeral run lifecycle."""

    deleted: ClassVar[list[StockgateEndpoint]] = []
    registered_secret: ClassVar[bytes | None] = None

    def __init__(self, _base_url: str) -> None:
        pass

    def register_service(
        self,
        _credential: str,
        secret_key: bytes,
        _address: StockgateAddress,
        intent: StockgateRegistrationIntent,
        *,
        grant_expires_in: int,
    ) -> tuple[StockgateService, StockgateEndpoint]:
        """Require an ephemeral intent and return in-memory state."""
        assert intent.ephemeral is True
        assert grant_expires_in == 600
        type(self).registered_secret = secret_key
        identity = stockgate_identity(secret_key)
        return (
            StockgateService("svc_ephemeral", intent.service, False),
            StockgateEndpoint(
                endpoint_id=identity.endpoint_id,
                endpoint_hex=identity.endpoint_hex,
                service_id="svc_ephemeral",
                credential="ec_initial.secret",
                credential_expires_in=3600,
                signed_packet=b"signed",
                tags=(),
                ephemeral=True,
            ),
        )

    def heartbeat(self, endpoint: StockgateEndpoint) -> StockgateHeartbeat:
        """Rotate the in-memory endpoint credential."""
        rotated = StockgateEndpoint(
            endpoint_id=endpoint.endpoint_id,
            endpoint_hex=endpoint.endpoint_hex,
            service_id=endpoint.service_id,
            credential="ec_rotated.secret",
            credential_expires_in=endpoint.credential_expires_in,
            signed_packet=endpoint.signed_packet,
            tags=endpoint.tags,
            ephemeral=True,
        )
        return StockgateHeartbeat(rotated, 45, None, None, None)

    def delete_endpoint(self, endpoint: StockgateEndpoint) -> None:
        """Record cleanup with the latest credential."""
        type(self).deleted.append(endpoint)


def test_service_register_reuses_human_credential_and_saves_worker_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One command performs registration without exposing either credential."""
    monkeypatch.setattr(qf, "StockgateClient", _FakeClient)
    key_file = tmp_path / "iroh.key"
    state_file = tmp_path / "worker.json"
    device_file = tmp_path / "device.json"
    qf._write_private_json(
        device_file,
        {
            "access_token": "dc_human.secret",
            "expires_at": int(time.time()) + 3600,
            "issuer": "https://stockgate.example",
        },
    )

    result = CliRunner().invoke(
        qf.app,
        [
            "service",
            "register",
            "o/test-org/echo",
            "--stockgate-url",
            "https://stockgate.example",
            "--direct-address",
            "127.0.0.1:9400",
            "--key-file",
            str(key_file),
            "--state-file",
            str(state_file),
            "--device-credential-file",
            str(device_file),
            "--no-open-browser",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "stockgate://o/test-org/echo" in result.output
    assert "dc_human.secret" not in result.output
    assert "ec_worker.secret" not in result.output
    assert _FakeClient.calls[-1][1] == "dc_human.secret"
    saved = json.loads(state_file.read_text())
    assert saved["credential"] == "ec_worker.secret"
    assert saved["service_id"] == "svc_test"
    if os.name != "nt":
        assert stat.S_IMODE(state_file.stat().st_mode) == 0o600


def test_service_register_reauthenticates_once_when_cached_credential_is_revoked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A revoked cached device credential does not strand registration."""
    _RejectedCacheClient.attempts = 0
    monkeypatch.setattr(qf, "StockgateClient", _RejectedCacheClient)
    device_file = tmp_path / "device.json"
    qf._write_private_json(
        device_file,
        {
            "access_token": "dc_revoked.secret",
            "expires_at": int(time.time()) + 3600,
            "issuer": "https://stockgate.example",
        },
    )

    result = CliRunner().invoke(
        qf.app,
        [
            "service",
            "register",
            "o/test-org/echo",
            "--stockgate-url",
            "https://stockgate.example",
            "--direct-address",
            "127.0.0.1:9400",
            "--key-file",
            str(tmp_path / "iroh.key"),
            "--state-file",
            str(tmp_path / "worker.json"),
            "--device-credential-file",
            str(device_file),
            "--no-open-browser",
        ],
    )

    assert result.exit_code == 0, result.output
    assert _RejectedCacheClient.attempts == 2
    assert json.loads(device_file.read_text())["access_token"] == "dc_replacement.secret"


def test_ephemeral_bridge_receives_secret_only_through_pipe(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bridge key is absent from paths, environment, and argv."""
    identity = stockgate_identity(bytes(range(32)))
    written: list[str] = []
    arguments: list[str] = []
    environment: dict[str, str] = {}

    class SecretInput(io.StringIO):
        """Capture the secret before the supervisor closes the pipe."""

        def close(self) -> None:
            written.append(self.getvalue())
            super().close()

    class BridgeProcess(_FakeProcess):
        """Expose bridge discovery streams."""

        def __init__(self) -> None:
            super().__init__(None)
            self.stdin = SecretInput()
            self.stdout = io.StringIO(
                json.dumps(
                    {
                        "endpoint_id": identity.endpoint_hex,
                        "relay_urls": [],
                        "direct_addresses": ["127.0.0.1:9400"],
                    }
                )
                + "\n"
            )

    process = BridgeProcess()

    def popen(argv: list[str], **kwargs: object) -> BridgeProcess:
        arguments.extend(argv)
        environment.update(cast("dict[str, str]", kwargs["env"]))
        return process

    monkeypatch.setenv("VGI_IROH_SECRET_KEY", "must-not-be-inherited")
    monkeypatch.setattr(vars(qf)["shutil"], "which", lambda _name: "/usr/bin/vgi-iroh-bridge")
    monkeypatch.setattr(vars(qf)["subprocess"], "Popen", popen)
    running = qf._start_ephemeral_bridge(
        "vgi-iroh-bridge",
        identity.secret_key,
        identity.endpoint_hex,
        raw_upstream="tcp://127.0.0.1:9400",
        http_upstream=None,
        relay_urls=(),
        no_relay=True,
    )

    assert running.address == StockgateAddress(direct_addresses=("127.0.0.1:9400",))
    assert "--secret-key-stdin" in arguments
    assert identity.secret_key.hex() not in arguments
    assert written == [f"{identity.secret_key.hex()}\n"]
    assert "VGI_IROH_SECRET_KEY" not in environment


def test_service_run_keeps_identity_in_memory_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run command supervises, heartbeats, and deletes an ephemeral endpoint."""
    _RunClient.deleted = []
    _RunClient.registered_secret = None
    worker = _FakeProcess(0)
    bridge = _FakeProcess(None)
    device_file = tmp_path / "device.json"
    qf._write_private_json(
        device_file,
        {
            "access_token": "dc_human.secret",
            "expires_at": int(time.time()) + 3600,
            "issuer": "https://stockgate.example",
        },
    )
    monkeypatch.setattr(qf, "StockgateClient", _RunClient)
    monkeypatch.setattr(vars(qf)["subprocess"], "Popen", lambda *_args, **_kwargs: worker)
    monkeypatch.setattr(
        qf,
        "_start_ephemeral_bridge",
        lambda *_args, **_kwargs: qf._RunningBridge(
            cast("Any", bridge),
            StockgateAddress(direct_addresses=("127.0.0.1:9400",)),
        ),
    )

    result = CliRunner().invoke(
        qf.app,
        [
            "service",
            "run",
            "o/test-org/echo",
            "--stockgate-url",
            "https://stockgate.example",
            "--raw-upstream",
            "tcp://127.0.0.1:9400",
            "--device-credential-file",
            str(device_file),
            "--no-open-browser",
            "--",
            "python",
            "worker.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Ephemeral EndpointId:" in result.output
    assert _RunClient.registered_secret is not None
    assert _RunClient.deleted[-1].credential == "ec_rotated.secret"
    assert bridge.terminated is True
    assert sorted(path.name for path in tmp_path.iterdir()) == ["device.json"]
