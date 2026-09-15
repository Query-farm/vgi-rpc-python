#!/usr/bin/env python3
"""Compare the conformance protocol's description across every vgi-rpc port.

Spawns each port's conformance worker, asks it for a description over
``vgi_rpc.Reflection.v1``, and compares.

The headline check is **protocol_hash equality**.  The hash is defined over
canonical JSON of the *decoded* description rather than over encoder bytes
(WIRE_PROTOCOL §14) precisely so it can be compared across ports -- which makes
this the one mechanical check that a port has not drifted.  Before that
definition existed there was nothing to compare but method names, which is how
drift went unnoticed.

A hash mismatch is not a mystery: every port ships the canonical preimage
beside its digest, so the fix is to diff two JSON documents and look at the
method, field or type token they spell differently.

Run: ``uv run python describe_diff.py``
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

#: Every port, and how to get a conformance worker speaking on stdio from it.
#: A port whose repo is absent, or whose worker does not build, is reported as
#: such rather than skipped silently -- "not checked" and "checked and agrees"
#: must not look the same in this output, since the entire value of the tool is
#: telling them apart.
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
    "rust": {
        "cmd": ["./target/release/vgi-rpc-conformance-rust"],
        "cwd": REPOS_DIR / "vgi-rpc-rust",
        "build": "cargo build --release --bin vgi-rpc-conformance-rust",
    },
    "java": {
        # The installed distribution, not `gradlew run`: Gradle writes its own
        # progress to stdout, which is the same stream the worker speaks Arrow
        # IPC on.
        "cmd": ["./conformance-worker/build/install/conformance-worker/bin/conformance-worker"],
        "cwd": REPOS_DIR / "vgi-rpc-java",
        "build": "./gradlew -q --console=plain :conformance-worker:installDist",
    },
    "csharp": {
        # The published binary, not `dotnet run`: the SDK writes build progress
        # to stdout, which is the same stream the worker speaks Arrow IPC on.
        "cmd": ["./artifacts/conformance-worker/QueryFarm.VgiRpc.ConformanceWorker"],
        "cwd": REPOS_DIR / "vgi-rpc-csharp",
        "build": (
            "dotnet publish conformance/QueryFarm.VgiRpc.ConformanceWorker "
            "-c Release -o artifacts/conformance-worker"
        ),
    },
    "cpp": {
        "cmd": ["./build/conformance/conformance_worker"],
        "cwd": REPOS_DIR / "vgi-rpc-c++",
        "build": "cmake --build build --target conformance_worker",
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
        # A worker still on the old protocol answers by listing every method it
        # *does* have, which is ~90 names and buries the one fact that matters.
        detail = str(e)
        if "list_protocols" in detail:
            return "does not host vgi_rpc.Reflection.v1 (still on the pre-2.0 __describe__ protocol)"
        return f"introspect failed: {detail.splitlines()[0][:160]}"
    finally:
        transport.close()
        proc.terminate()
        proc.wait(timeout=5)


def main() -> None:
    """Describe every port, then compare -- hash first."""
    descriptions: dict[str, ServiceDescription | str] = {}

    print("Connecting to python...", file=sys.stderr)
    descriptions["python"] = get_python_description()

    for name, config in SUBPROCESS_WORKERS.items():
        print(f"Connecting to {name}...", file=sys.stderr)
        descriptions[name] = get_subprocess_description(name, config)

    print()
    for name, desc in descriptions.items():
        if isinstance(desc, str):
            print(f"{name:12s}  UNAVAILABLE: {desc}")
        else:
            print(f"{name:12s}  {len(desc.methods):3d} methods  {desc.protocol_hash}")

    successful = {k: v for k, v in descriptions.items() if isinstance(v, ServiceDescription)}
    unavailable = sorted(k for k, v in descriptions.items() if isinstance(v, str))

    print()
    if len(successful) < 2:
        print("Need at least two reachable ports to compare.")
        sys.exit(1)

    failed = False

    # --- The headline check ------------------------------------------------
    hashes: dict[str, list[str]] = {}
    for name, desc in successful.items():
        hashes.setdefault(desc.protocol_hash, []).append(name)

    if len(hashes) == 1:
        print(f"protocol_hash: AGREED across {len(successful)} ports")
    else:
        failed = True
        print(f"protocol_hash: DRIFT across {len(successful)} ports")
        for digest, names in sorted(hashes.items(), key=lambda kv: -len(kv[1])):
            print(f"  {digest}  {', '.join(sorted(names))}")
        print()
        print("  Each port ships the canonical preimage beside its digest, so this is")
        print("  a JSON diff rather than a guess. In Python:")
        print("    cat vgi-rpc-python/tests/golden/protocol_hash_vector.json")

    # --- What actually differs, to make a hash mismatch actionable ---------
    reference = successful.get("python") or next(iter(successful.values()))
    ref_name = "python" if "python" in successful else next(iter(successful))

    for name, desc in successful.items():
        if name == ref_name:
            continue
        only_ref = sorted(set(reference.methods) - set(desc.methods))
        only_port = sorted(set(desc.methods) - set(reference.methods))
        if only_ref or only_port:
            failed = True
            print()
            print(f"{name} vs {ref_name}: method sets differ")
            if only_ref:
                print(f"  missing from {name}: {', '.join(only_ref)}")
            if only_port:
                print(f"  extra in {name}:   {', '.join(only_port)}")

        shape: list[str] = []
        for method in sorted(set(reference.methods) & set(desc.methods)):
            # Named in the same order as the heading above: this port first,
            # then the reference. Getting that backwards sends a reader to fix
            # the wrong side.
            port, ref = desc.methods[method], reference.methods[method]
            if port.method_type != ref.method_type:
                shape.append(f"{method}: method_type {port.method_type.value} vs {ref.method_type.value}")
            elif port.has_return != ref.has_return:
                shape.append(f"{method}: has_return {port.has_return} vs {ref.has_return}")
            elif port.params_schema != ref.params_schema:
                shape.append(f"{method}: params {port.params_schema} vs {ref.params_schema}")
            elif port.has_return and port.result_schema != ref.result_schema:
                shape.append(f"{method}: result {port.result_schema} vs {ref.result_schema}")
            elif port.has_header != ref.has_header:
                shape.append(f"{method}: has_header {port.has_header} vs {ref.has_header}")
            elif port.has_header and port.header_schema != ref.header_schema:
                # Header schemas are in the hash preimage, so a tool that does
                # not compare them reports "shapes agree" beside a hash mismatch
                # and sends the reader looking in the wrong place.
                shape.append(f"{method}: header {port.header_schema} vs {ref.header_schema}")
        if shape:
            failed = True
            print()
            print(f"{name} vs {ref_name}: {len(shape)} method(s) differ in shape")
            for line in shape[:15]:
                print(f"  {line}")
            if len(shape) > 15:
                print(f"  ... and {len(shape) - 15} more")

    # --- Versions ----------------------------------------------------------
    versions = {k: v.protocol_version for k, v in successful.items()}
    if len(set(versions.values())) > 1:
        failed = True
        print()
        print("protocol_version drift: " + ", ".join(f"{k}={v or '(none)'}" for k, v in sorted(versions.items())))

    if unavailable:
        print()
        print(f"Not checked ({len(unavailable)}): {', '.join(unavailable)}")
        print("  A port that could not be reached is not a port that agrees.")

    print()
    if failed:
        print("RESULT: drift")
        sys.exit(1)
    if unavailable:
        print(f"RESULT: the {len(successful)} reachable ports agree; {len(unavailable)} not checked")
        sys.exit(2)
    print(f"RESULT: all {len(successful)} ports agree")


if __name__ == "__main__":
    main()
