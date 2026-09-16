# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Query Farm CLI workflows built on vgi-rpc and Stockgate."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import queue
import re
import secrets
import shutil
import subprocess
import threading
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, cast

import typer

from vgi_rpc.stockgate import (
    StockgateAddress,
    StockgateClient,
    StockgateDeviceCredential,
    StockgateEndpoint,
    StockgateError,
    StockgateRegistrationIntent,
    StockgateService,
    load_or_create_stockgate_identity,
    stockgate_identity,
)

_PRODUCTION_STOCKGATE = "https://stockgate.query-farm.services"

app = typer.Typer(name="qf", help="Query Farm service operations.", add_completion=False, no_args_is_help=True)
service_app = typer.Typer(help="Create and register VGI services.", no_args_is_help=True)
app.add_typer(service_app, name="service")


@dataclass(frozen=True, slots=True)
class _RunningBridge:
    process: subprocess.Popen[str]
    address: StockgateAddress


@dataclass(slots=True)
class _EndpointRuntime:
    endpoint: StockgateEndpoint


def _config_root() -> Path:
    configured = os.environ.get("XDG_CONFIG_HOME")
    return Path(configured) / "qf" if configured else Path.home() / ".config" / "qf"


def _safe_segment(value: str) -> str:
    segment = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.removeprefix("o/")).strip("-.")
    return segment or "service"


def _parse_service_ref(value: str) -> tuple[str, str]:
    parts = value.removeprefix("stockgate://").split("/")
    if len(parts) != 3 or parts[0] != "o" or not parts[1] or not parts[2]:
        raise ValueError("Service must be written as o/<organization>/<service>")
    return parts[1], parts[2]


def _read_cached_device_credential(path: Path, issuer: str) -> str | None:
    try:
        if os.name != "nt" and path.stat().st_mode & 0o077:
            return None
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    record = cast("dict[str, object]", value)
    credential = record.get("access_token")
    expires_at = record.get("expires_at")
    if record.get("issuer") != issuer or not isinstance(credential, str) or not credential.startswith("dc_"):
        return None
    if not isinstance(expires_at, int) or isinstance(expires_at, bool) or expires_at <= int(time.time()) + 60:
        return None
    return credential


def _write_private_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _authorize(
    client: StockgateClient,
    *,
    issuer: str,
    cache_path: Path,
    open_browser: bool,
    force: bool = False,
) -> tuple[str, bool]:
    cached = _read_cached_device_credential(cache_path, issuer)
    if cached is not None and not force:
        return cached, True

    def show_authorization(url: str, code: str) -> None:
        typer.echo(f"Approve this device at: {url}")
        typer.echo(f"Device code: {code}")
        if open_browser and not webbrowser.open(url):
            typer.echo("The browser could not be opened automatically; use the URL above.", err=True)

    credential: StockgateDeviceCredential = client.authenticate_device(on_authorization=show_authorization)
    _write_private_json(
        cache_path,
        {
            "access_token": credential.access_token,
            "expires_at": int(time.time()) + credential.expires_in,
            "issuer": issuer,
        },
    )
    return credential.access_token, False


def _write_endpoint_state(
    path: Path,
    issuer: str,
    org: str,
    service: StockgateService,
    endpoint: StockgateEndpoint,
) -> None:
    _write_private_json(
        path,
        {
            "config_version": endpoint.config_version,
            "credential": endpoint.credential,
            "credential_expires_at": int(time.time()) + endpoint.credential_expires_in,
            "endpoint_hex": endpoint.endpoint_hex,
            "endpoint_id": endpoint.endpoint_id,
            "ephemeral": endpoint.ephemeral,
            "issuer": issuer,
            "org": org if org.startswith("o/") else f"o/{org}",
            "service": service.name,
            "service_id": endpoint.service_id,
            "signed_packet": base64.b64encode(endpoint.signed_packet).decode("ascii"),
            "tags": list(endpoint.tags),
        },
    )


def _read_process_line(process: subprocess.Popen[str], timeout: float) -> str:
    if process.stdout is None:
        raise RuntimeError("Iroh bridge discovery output is unavailable")
    output = process.stdout
    result: queue.Queue[str | BaseException] = queue.Queue(maxsize=1)

    def read() -> None:
        try:
            result.put(output.readline())
        except BaseException as error:
            result.put(error)

    threading.Thread(target=read, daemon=True).start()
    try:
        value = result.get(timeout=timeout)
    except queue.Empty as error:
        raise TimeoutError("Iroh bridge did not become ready within the startup deadline") from error
    if isinstance(value, BaseException):
        raise RuntimeError("Could not read Iroh bridge discovery output") from value
    if not value:
        raise RuntimeError(f"Iroh bridge exited before becoming ready (status {process.poll()})")
    return value


def _stop_process(process: subprocess.Popen[str], timeout: float = 10.0) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def _start_ephemeral_bridge(
    executable: str,
    secret_key: bytes,
    endpoint_hex: str,
    *,
    raw_upstream: str | None,
    http_upstream: str | None,
    relay_urls: tuple[str, ...],
    no_relay: bool,
    startup_timeout: float = 30.0,
) -> _RunningBridge:
    resolved = shutil.which(executable)
    if resolved is None:
        candidate = Path(executable)
        if not candidate.is_file():
            raise ValueError(f"Iroh bridge executable was not found: {executable}")
        resolved = str(candidate)
    arguments = [resolved, "--secret-key-stdin", "--discovery-json"]
    if raw_upstream:
        arguments.extend(("--raw-upstream", raw_upstream))
    if http_upstream:
        arguments.extend(("--http-upstream", http_upstream))
    if no_relay:
        arguments.append("--no-relay")
    for relay_url in relay_urls:
        arguments.extend(("--relay-url", relay_url))
    bridge_environment = os.environ.copy()
    bridge_environment.pop("VGI_IROH_SECRET_KEY", None)
    process = subprocess.Popen(
        arguments,
        env=bridge_environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=os.name != "nt",
    )
    try:
        if process.stdin is None:
            raise RuntimeError("Iroh bridge secret input is unavailable")
        process.stdin.write(f"{secret_key.hex()}\n")
        process.stdin.close()
        raw_discovery = _read_process_line(process, startup_timeout)
        decoded: object = json.loads(raw_discovery)
        if not isinstance(decoded, dict):
            raise ValueError("Iroh bridge returned invalid discovery data")
        discovery = cast("dict[str, object]", decoded)
        if discovery.get("endpoint_id") != endpoint_hex:
            raise ValueError("Iroh bridge started with a different EndpointId")
        raw_relays = discovery.get("relay_urls")
        raw_direct = discovery.get("direct_addresses")
        if not isinstance(raw_relays, list) or any(not isinstance(item, str) for item in raw_relays):
            raise ValueError("Iroh bridge returned invalid relay discovery data")
        if not isinstance(raw_direct, list) or any(not isinstance(item, str) for item in raw_direct):
            raise ValueError("Iroh bridge returned invalid direct-address discovery data")
        typed_relays = cast("list[str]", raw_relays)
        typed_direct = cast("list[str]", raw_direct)
        discovered_relay = typed_relays[0] if typed_relays else None
        return _RunningBridge(process, StockgateAddress(discovered_relay, tuple(typed_direct)))
    except BaseException:
        _stop_process(process)
        raise


def _supervise_ephemeral(
    client: StockgateClient,
    state: _EndpointRuntime,
    worker: subprocess.Popen[str],
    bridge: subprocess.Popen[str],
) -> int:
    heartbeat = client.heartbeat(state.endpoint)
    state.endpoint = heartbeat.endpoint
    next_heartbeat = time.monotonic() + heartbeat.next_heartbeat_s
    while True:
        worker_status = worker.poll()
        bridge_status = bridge.poll()
        if worker_status is not None:
            return worker_status
        if bridge_status is not None:
            raise RuntimeError(f"Iroh bridge exited unexpectedly with status {bridge_status}")
        now = time.monotonic()
        if now >= next_heartbeat:
            heartbeat = client.heartbeat(state.endpoint)
            state.endpoint = heartbeat.endpoint
            next_heartbeat = now + heartbeat.next_heartbeat_s
        time.sleep(min(0.2, max(0.0, next_heartbeat - now)))


@service_app.command("register")
def register_service(
    service_ref: Annotated[str, typer.Argument(help="Service name as o/<organization>/<service>.")],
    relay_url: Annotated[str | None, typer.Option("--relay-url", help="Published HTTPS Iroh relay URL.")] = None,
    direct_address: Annotated[
        list[str] | None,
        typer.Option("--direct-address", help="Published direct Iroh address; may be repeated."),
    ] = None,
    stockgate_url: Annotated[
        str,
        typer.Option("--stockgate-url", envvar="STOCKGATE_URL", help="Stockgate issuer origin."),
    ] = _PRODUCTION_STOCKGATE,
    key_file: Annotated[Path | None, typer.Option("--key-file", help="Persistent Iroh secret-key file.")] = None,
    state_file: Annotated[
        Path | None,
        typer.Option("--state-file", help="Private endpoint credential and registration state file."),
    ] = None,
    device_credential_file: Annotated[
        Path | None,
        typer.Option("--device-credential-file", help="Private cached human device credential file."),
    ] = None,
    label: Annotated[str | None, typer.Option("--label", help="Optional endpoint label.")] = None,
    ephemeral: Annotated[bool, typer.Option("--ephemeral", help="Delete registration state when revoked.")] = False,
    grant_lifetime: Annotated[
        int,
        typer.Option("--grant-lifetime", min=60, max=3600, help="Single-use grant lifetime in seconds."),
    ] = 600,
    open_browser: Annotated[
        bool,
        typer.Option("--open-browser/--no-open-browser", help="Open the device approval page automatically."),
    ] = True,
) -> None:
    """Authenticate a human and immediately register one service endpoint."""
    try:
        org, service = _parse_service_ref(service_ref)
        config_root = _config_root()
        worker_root = config_root / "workers" / _safe_segment(org) / _safe_segment(service)
        resolved_key_file = key_file or worker_root / "iroh.key"
        resolved_state_file = state_file or worker_root / "stockgate.json"
        resolved_device_file = device_credential_file or config_root / "stockgate-device.json"
        address = StockgateAddress(relay_url, tuple(direct_address or ()))
        identity = load_or_create_stockgate_identity(resolved_key_file)
        client = StockgateClient(stockgate_url)
        human_credential, cached_credential = _authorize(
            client,
            issuer=stockgate_url.rstrip("/"),
            cache_path=resolved_device_file,
            open_browser=open_browser,
        )
        intent = StockgateRegistrationIntent(org=org, service=service, label=label, ephemeral=ephemeral)
        try:
            selected, endpoint = client.register_service(
                human_credential,
                identity.secret_key,
                address,
                intent,
                grant_expires_in=grant_lifetime,
            )
        except StockgateError as error:
            if error.status != 401 or not cached_credential:
                raise
            with contextlib.suppress(FileNotFoundError):
                resolved_device_file.unlink()
            human_credential, _ = _authorize(
                client,
                issuer=stockgate_url.rstrip("/"),
                cache_path=resolved_device_file,
                open_browser=open_browser,
                force=True,
            )
            selected, endpoint = client.register_service(
                human_credential,
                identity.secret_key,
                address,
                intent,
                grant_expires_in=grant_lifetime,
            )
        _write_endpoint_state(resolved_state_file, stockgate_url.rstrip("/"), org, selected, endpoint)
    except (OSError, ValueError, StockgateError) as error:
        typer.echo(f"Registration failed: {error}", err=True)
        raise typer.Exit(1) from error
    action = "Created and registered" if selected.created else "Registered"
    typer.echo(f"{action} stockgate://o/{org}/{service}")
    typer.echo(f"EndpointId: {endpoint.endpoint_id}")
    typer.echo(f"Worker state: {resolved_state_file}")


@service_app.command("run")
def run_service(
    service_ref: Annotated[str, typer.Argument(help="Service name as o/<organization>/<service>.")],
    command: Annotated[
        list[str] | None,
        typer.Argument(help="Worker command and arguments, placed after --."),
    ] = None,
    raw_upstream: Annotated[
        str | None,
        typer.Option("--raw-upstream", help="Bridge destination such as tcp://127.0.0.1:9400."),
    ] = None,
    http_upstream: Annotated[
        str | None,
        typer.Option("--http-upstream", help="Fixed HTTP(S) worker origin and base path."),
    ] = None,
    bridge_executable: Annotated[
        str,
        typer.Option("--bridge", envvar="VGI_IROH_BRIDGE", help="vgi-iroh-bridge executable."),
    ] = "vgi-iroh-bridge",
    relay_url: Annotated[
        list[str] | None,
        typer.Option("--relay-url", help="Replace the bridge relay set; may be repeated."),
    ] = None,
    no_relay: Annotated[bool, typer.Option("--no-relay", help="Use direct Iroh paths only.")] = False,
    stockgate_url: Annotated[
        str,
        typer.Option("--stockgate-url", envvar="STOCKGATE_URL", help="Stockgate issuer origin."),
    ] = _PRODUCTION_STOCKGATE,
    device_credential_file: Annotated[
        Path | None,
        typer.Option("--device-credential-file", help="Private cached human device credential file."),
    ] = None,
    label: Annotated[str | None, typer.Option("--label", help="Optional endpoint label.")] = None,
    grant_lifetime: Annotated[
        int,
        typer.Option("--grant-lifetime", min=60, max=3600, help="Single-use grant lifetime in seconds."),
    ] = 600,
    bridge_startup_timeout: Annotated[
        float,
        typer.Option("--bridge-startup-timeout", min=1.0, help="Bridge startup deadline in seconds."),
    ] = 30.0,
    open_browser: Annotated[
        bool,
        typer.Option("--open-browser/--no-open-browser", help="Open the device approval page automatically."),
    ] = True,
) -> None:
    """Run a worker with a process-lifetime Iroh identity and registration."""
    command = list(command or ())
    worker: subprocess.Popen[str] | None = None
    bridge: _RunningBridge | None = None
    client: StockgateClient | None = None
    state: _EndpointRuntime | None = None
    exit_code = 1
    try:
        org, service = _parse_service_ref(service_ref)
        if not command:
            raise ValueError("A worker command is required after --")
        if raw_upstream is None and http_upstream is None:
            raise ValueError("Configure --raw-upstream or --http-upstream")
        relays = tuple(relay_url or ())
        if no_relay and relays:
            raise ValueError("--no-relay conflicts with --relay-url")
        client = StockgateClient(stockgate_url)
        device_file = device_credential_file or _config_root() / "stockgate-device.json"
        human_credential, cached_credential = _authorize(
            client,
            issuer=stockgate_url.rstrip("/"),
            cache_path=device_file,
            open_browser=open_browser,
        )
        identity = stockgate_identity(secrets.token_bytes(32))
        worker = subprocess.Popen(command, text=True, start_new_session=os.name != "nt")
        bridge = _start_ephemeral_bridge(
            bridge_executable,
            identity.secret_key,
            identity.endpoint_hex,
            raw_upstream=raw_upstream,
            http_upstream=http_upstream,
            relay_urls=relays,
            no_relay=no_relay,
            startup_timeout=bridge_startup_timeout,
        )
        intent = StockgateRegistrationIntent(org=org, service=service, label=label, ephemeral=True)
        try:
            selected, endpoint = client.register_service(
                human_credential,
                identity.secret_key,
                bridge.address,
                intent,
                grant_expires_in=grant_lifetime,
            )
        except StockgateError as error:
            if error.status != 401 or not cached_credential:
                raise
            with contextlib.suppress(FileNotFoundError):
                device_file.unlink()
            human_credential, _ = _authorize(
                client,
                issuer=stockgate_url.rstrip("/"),
                cache_path=device_file,
                open_browser=open_browser,
                force=True,
            )
            selected, endpoint = client.register_service(
                human_credential,
                identity.secret_key,
                bridge.address,
                intent,
                grant_expires_in=grant_lifetime,
            )
        state = _EndpointRuntime(endpoint)
        action = "Created and running" if selected.created else "Running"
        typer.echo(f"{action} stockgate://o/{org}/{service}")
        typer.echo(f"Ephemeral EndpointId: {endpoint.endpoint_id}")
        exit_code = _supervise_ephemeral(client, state, worker, bridge.process)
    except KeyboardInterrupt:
        exit_code = 130
    except (OSError, ValueError, RuntimeError, StockgateError) as error:
        typer.echo(f"Run failed: {error}", err=True)
        exit_code = 1
    finally:
        if client is not None and state is not None:
            try:
                client.delete_endpoint(state.endpoint)
            except StockgateError as error:
                typer.echo(f"Warning: ephemeral endpoint cleanup failed: {error}", err=True)
        if bridge is not None:
            _stop_process(bridge.process)
        if worker is not None:
            _stop_process(worker)
    if exit_code:
        raise typer.Exit(exit_code)


__all__ = ["app"]
