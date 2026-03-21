#!/usr/bin/env python3
"""Compare __describe__ output across all three vgi-rpc implementations.

Spawns each conformance worker, calls __describe__ via the vgi-rpc introspect
API, and compares method sets, describe versions, and schema field counts.

Requires: pip install vgi-rpc
"""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import threading

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.introspect import ServiceDescription, introspect
from vgi_rpc.rpc import PipeTransport, RpcServer
from vgi_rpc.rpc._transport import make_pipe_pair

REPOS_DIR = Path.home() / "Development"

SUBPROCESS_WORKERS: dict[str, dict[str, str | list[str] | Path]] = {
    "go": {
        "cmd": ["./conformance-worker"],
        "cwd": REPOS_DIR / "vgi-rpc-go",
        "build": "make build",
    },
    "typescript": {
        "cmd": ["bun", "run", "examples/conformance.ts"],
        "cwd": REPOS_DIR / "vgi-rpc-typescript",
    },
}


def get_python_description() -> ServiceDescription | str:
    """Get __describe__ from the Python implementation in-process via pipe pair."""
    try:
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            return introspect(client_transport)
        finally:
            client_transport.close()
    except Exception as e:
        return f"failed: {e}"


def get_subprocess_description(name: str, config: dict[str, str | list[str] | Path]) -> ServiceDescription | str:
    """Spawn a worker and call __describe__. Returns ServiceDescription or error string."""
    cwd = Path(str(config["cwd"]))
    if not cwd.is_dir():
        return f"repo not found: {cwd}"

    # Build if needed (Go worker)
    if "build" in config:
        result = subprocess.run(
            str(config["build"]),
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return f"build failed: {result.stderr.strip()}"

    cmd = [str(c) for c in config["cmd"]]  # type: ignore[union-attr]
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd=cwd,
            bufsize=0,
        )
        assert proc.stdout is not None
        assert proc.stdin is not None
        reader = io.BufferedReader(io.FileIO(proc.stdout.fileno(), closefd=False))
        writer = proc.stdin
        transport = PipeTransport(reader, writer)  # type: ignore[arg-type]
    except Exception as e:
        return f"failed to spawn: {e}"

    try:
        return introspect(transport)
    except Exception as e:
        return f"introspect failed: {e}"
    finally:
        transport.close()
        proc.terminate()
        proc.wait(timeout=5)


def main() -> None:
    descriptions: dict[str, ServiceDescription | str] = {}

    print("Connecting to python...", file=sys.stderr)
    descriptions["python"] = get_python_description()

    for name, config in SUBPROCESS_WORKERS.items():
        print(f"Connecting to {name}...", file=sys.stderr)
        descriptions[name] = get_subprocess_description(name, config)

    print()

    # Show summary for each
    for name, desc in descriptions.items():
        if isinstance(desc, str):
            print(f"{name:12s}  ERROR: {desc}")
        else:
            print(f"{name:12s}  {len(desc.methods)} methods, describe v{desc.describe_version}")

    print()

    # Compare only successful ones
    successful = {k: v for k, v in descriptions.items() if isinstance(v, ServiceDescription)}
    if len(successful) < 2:
        print("Need at least 2 successful connections to compare.")
        sys.exit(1)

    # Describe version comparison
    versions = {k: v.describe_version for k, v in successful.items()}
    unique_versions = set(versions.values())
    if len(unique_versions) > 1:
        parts = ", ".join(f"{k}={v}" for k, v in versions.items())
        print(f"Describe version drift: {parts}")

        # Show which describe fields are missing in lower-version implementations.
        # v3 added: is_exchange, param_docs_json
        # v2 added: has_header, header_schema_ipc (but all are at v2+)
        v3_fields = ["is_exchange", "param_docs_json"]
        for name, ver in versions.items():
            if ver < "3":
                print(f"  {name} (v{ver}) missing fields: {', '.join(v3_fields)}")
        print()
    else:
        print(f"Describe version: all at v{unique_versions.pop()}")
        print()

    # Method set comparison
    method_sets = {k: set(v.methods.keys()) for k, v in successful.items()}
    all_methods = set().union(*method_sets.values())

    missing_report: list[str] = []
    for name, methods in method_sets.items():
        missing = all_methods - methods
        if missing:
            missing_report.append(f"  {name} missing: {', '.join(sorted(missing))}")

    if missing_report:
        print("Method parity issues:")
        for line in missing_report:
            print(line)
    else:
        print(f"Method parity: all {len(all_methods)} methods present in all implementations.")

    # Method type comparison (unary vs stream)
    type_mismatches: list[str] = []
    names = list(successful.keys())
    for method in sorted(all_methods):
        types = {}
        for name in names:
            if method in successful[name].methods:
                types[name] = successful[name].methods[method].method_type.value
        unique = set(types.values())
        if len(unique) > 1:
            parts = ", ".join(f"{k}={v}" for k, v in types.items())
            type_mismatches.append(f"  {method}: {parts}")

    if type_mismatches:
        print("\nMethod type mismatches:")
        for line in type_mismatches:
            print(line)


if __name__ == "__main__":
    main()
