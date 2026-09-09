# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Exercise a real VGI worker through a deployed Stockgate service."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import psutil

from tests._fixture_service import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc import iroh_connect
from vgi_rpc.rpc import RpcServer, serve_tcp
from vgi_rpc.stockgate import (
    StockgateAddress,
    StockgateClient,
    StockgateEndpoint,
    StockgateRegistrationIntent,
    load_or_create_iroh_secret_key,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="https://stockgate-dev.query-farm.services")
    parser.add_argument("--org", required=True, help="Organization handle, with or without o/")
    parser.add_argument("--service", required=True, help="Unused service name for the fixture")
    parser.add_argument("--bridge", default=os.environ.get("VGI_IROH_BRIDGE", "vgi-iroh-bridge"))
    parser.add_argument("--keep-endpoint", action="store_true", help="Skip explicit endpoint cleanup")
    return parser.parse_args()


def _start_worker() -> tuple[int, threading.Thread]:
    ready = threading.Event()
    bound: dict[str, Any] = {}

    def on_bound(_host: str, port: int) -> None:
        bound["port"] = port
        ready.set()

    thread = threading.Thread(
        target=lambda: serve_tcp(
            RpcServer(RpcFixtureService, RpcFixtureServiceImpl()),
            "127.0.0.1",
            0,
            threaded=True,
            on_bound=on_bound,
            proxy_protocol="required",
            trusted_proxy_addresses=("127.0.0.1",),
            iroh_proxy_issuer="stockgate-e2e",
        ),
        daemon=True,
    )
    thread.start()
    if not ready.wait(10):
        raise TimeoutError("VGI fixture worker did not bind within 10 seconds")
    return int(bound["port"]), thread


def _direct_address(process_id: int) -> str:
    for _ in range(50):
        connections = psutil.Process(process_id).net_connections(kind="udp")
        ipv4 = [connection for connection in connections if connection.laddr and "." in connection.laddr.ip]
        if ipv4:
            return f"127.0.0.1:{ipv4[0].laddr.port}"
        time.sleep(0.1)
    raise TimeoutError("Iroh bridge did not expose a direct UDP socket within 5 seconds")


def main() -> None:
    """Run the staging lifecycle without printing either credential."""
    args = _arguments()
    registration_token = os.environ.get("STOCKGATE_REGISTRATION_TOKEN")
    if not registration_token:
        raise SystemExit("Set STOCKGATE_REGISTRATION_TOKEN in the environment (it is never printed).")
    bridge_binary = shutil.which(args.bridge)
    if bridge_binary is None:
        raise SystemExit(f"Iroh bridge executable was not found: {args.bridge}")
    org = args.org.removeprefix("o/")
    worker_port, _worker_thread = _start_worker()
    endpoint: StockgateEndpoint | None = None
    client = StockgateClient(args.base_url)

    with tempfile.TemporaryDirectory(prefix="stockgate-e2e-") as directory:
        key_path = Path(directory) / "iroh.key"
        secret_key = load_or_create_iroh_secret_key(key_path)
        bridge = subprocess.Popen(
            [
                bridge_binary,
                "--secret-key-file",
                str(key_path),
                "--raw-upstream",
                f"tcp://127.0.0.1:{worker_port}",
                "--no-relay",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            if bridge.stdout is None:
                raise RuntimeError("Iroh bridge stdout is unavailable")
            bridge_id = bridge.stdout.readline().strip()
            if len(bridge_id) != 64:
                stderr = bridge.stderr.read(4096) if bridge.stderr else ""
                raise RuntimeError(f"Iroh bridge did not publish an EndpointId: {stderr}")
            direct_address = _direct_address(bridge.pid)
            endpoint = client.register(
                registration_token,
                secret_key,
                StockgateAddress(direct_addresses=(direct_address,)),
                StockgateRegistrationIntent(
                    org=f"o/{org}",
                    service=args.service,
                    label="automated staging E2E",
                ),
            )
            if endpoint.endpoint_hex != bridge_id:
                raise RuntimeError("Registered identity differs from the running Iroh bridge")
            print("PASS registration")

            heartbeat = client.heartbeat(endpoint)
            endpoint = heartbeat.endpoint
            print("PASS heartbeat")

            resolved = client.resolve(org, args.service, endpoint.credential)
            if resolved.endpoint_id != endpoint.endpoint_id or resolved.address.direct_addresses != (direct_address,):
                raise RuntimeError("Resolution differs from the registered endpoint")
            print("PASS resolver JWS and endpoint Pkarr verification")

            with iroh_connect(
                RpcFixtureService,
                f"iroh://{resolved.endpoint_hex}",
                no_relay=True,
                direct_addresses=resolved.address.direct_addresses,
                connect_timeout=10,
                io_timeout=10,
            ) as worker:
                if worker.add(a=20.0, b=22.0) != 42.0:
                    raise RuntimeError("VGI fixture returned the wrong result")
            print("PASS Iroh VGI RPC (20 + 22 = 42)")
        finally:
            if endpoint is not None and not args.keep_endpoint:
                try:
                    client.delete_endpoint(endpoint)
                    print("PASS endpoint cleanup")
                except Exception as error:
                    print(f"WARN endpoint cleanup failed: {type(error).__name__}")
            bridge.terminate()
            try:
                bridge.wait(timeout=10)
            except subprocess.TimeoutExpired:
                bridge.kill()
                bridge.wait(timeout=5)


if __name__ == "__main__":
    main()
