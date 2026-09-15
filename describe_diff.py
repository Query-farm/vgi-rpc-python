#!/usr/bin/env python3
"""Compare *every hosted protocol's* description across every vgi-rpc port.

Spawns each port's conformance worker, asks it over ``vgi_rpc.Reflection.v1``
for the list of protocols it hosts, describes **each** of them, and compares
port to port keyed by protocol name.

The headline check is **protocol_hash equality**.  The hash is defined over
canonical JSON of the *decoded* description rather than over encoder bytes
(WIRE_PROTOCOL §14) precisely so it can be compared across ports -- which makes
this the one mechanical check that a port has not drifted.  Before that
definition existed there was nothing to compare but method names, which is how
drift went unnoticed.

Comparing every hosted protocol, rather than just the application one, closes
two gaps.  ``vgi_rpc.Reflection.v1`` is the protocol this tool *speaks* in
order to compare, and until now it was the one protocol never compared -- a
port could describe everything else identically while disagreeing about
description itself.  ``vgi_rpc.Identity.v1``, where hosted, rested on each port
asserting digests against constants in a spec document, which establishes
agreement with the document but never compares the ports to each other.

**Absence is a finding, not a failure.**  A protocol is hosted only where a
deployment wires it up -- identity hooks in particular.  A port that does not
host one is reported as an asymmetry; only *disagreement among the ports that
do host it* is drift.  Collapsing those two into one verdict would mean either
failing on a legitimate deployment choice or, worse, treating "nobody hosts it"
as "everybody agrees".

A hash mismatch is not a mystery: every port ships the canonical preimage
beside its digest, so the fix is to diff two JSON documents and look at the
method, field or type token they spell differently.

Run: ``uv run python describe_diff.py``
"""

from __future__ import annotations

import dataclasses
import io
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.introspect import _reflection_call
from vgi_rpc.rpc import _EMPTY_SCHEMA, PipeTransport, RpcServer
from vgi_rpc.rpc._reflection import MethodInfo, ProtocolList, ServiceDescription
from vgi_rpc.rpc._transport import make_pipe_pair
from vgi_rpc.utils import IpcValidation

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

#: Schema for the ``describe`` request.  Built once; it is the same for every
#: call to every port.
_DESCRIBE_PARAMS = pa.schema([pa.field("protocol", pa.utf8(), nullable=False)])


@dataclass(frozen=True)
class PortReport:
    """Everything one port said about itself.

    ``protocols`` maps wire name to either the reflection description or, when
    ``describe`` failed for that one protocol, the error text.  A port that
    lists a protocol it then cannot describe is a finding in its own right, so
    it is carried rather than dropped.
    """

    listing: ProtocolList
    protocols: dict[str, ServiceDescription | str]


# ---------------------------------------------------------------------------
# Talking to a port
# ---------------------------------------------------------------------------


def describe_everything(transport: object) -> PortReport:
    """List what a server hosts, then describe every one of them.

    ``vgi_rpc.introspect.introspect()`` deliberately describes only the primary
    application protocol -- it picks the first non-``vgi_rpc.`` binding and
    discards the rest of the listing.  That is the right default for a client
    that wants a service; it is exactly the wrong one for a drift guard, whose
    subject is the whole hosted surface.  So the two reflection calls are made
    directly here.
    """
    listing = ProtocolList.deserialize_from_bytes(
        _reflection_call(transport, "list_protocols", _EMPTY_SCHEMA, {}, IpcValidation.FULL)
    )

    described: dict[str, ServiceDescription | str] = {}
    for summary in listing.protocols:
        if summary.protocol in described:
            continue
        try:
            described[summary.protocol] = ServiceDescription.deserialize_from_bytes(
                _reflection_call(
                    transport,
                    "describe",
                    _DESCRIBE_PARAMS,
                    {"protocol": summary.protocol},
                    IpcValidation.FULL,
                )
            )
        except Exception as e:  # noqa: BLE001 - reported, not raised
            described[summary.protocol] = f"describe failed: {str(e).splitlines()[0][:160]}"

    return PortReport(listing=listing, protocols=described)


def get_python_report() -> PortReport | str:
    """Describe the Python implementation in-process via a pipe pair."""
    try:
        client_transport, server_transport = make_pipe_pair()
        server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            return describe_everything(client_transport)
        finally:
            client_transport.close()
    except Exception as e:  # noqa: BLE001 - reported, not raised
        return f"failed: {e}"


def get_subprocess_report(name: str, config: dict[str, str | list[str] | Path]) -> PortReport | str:
    """Spawn a worker and describe everything it hosts."""
    cwd = Path(str(config["cwd"]))
    if not cwd.is_dir():
        return f"repo not found: {cwd}"

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
    except Exception as e:  # noqa: BLE001 - reported, not raised
        return f"failed to spawn: {e}"

    try:
        return describe_everything(transport)
    except Exception as e:  # noqa: BLE001 - reported, not raised
        # A worker still on the old protocol answers by listing every method it
        # *does* have, which is ~90 names and buries the one fact that matters.
        detail = str(e)
        if "list_protocols" in detail:
            return "does not host vgi_rpc.Reflection.v1 (still on the pre-2.0 __describe__ protocol)"
        return f"reflection failed: {detail.splitlines()[0][:160]}"
    finally:
        transport.close()
        proc.terminate()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Comparing
# ---------------------------------------------------------------------------


def _render(value: object) -> str:
    """One-line rendering, so a multi-field schema does not swamp the report."""
    text = str(value).replace("\n", " ")
    return text if len(text) <= 120 else text[:117] + "..."


def _read_schema(ipc_bytes: bytes) -> pa.Schema:
    """Read a schema from IPC bytes, treating empty as the empty schema."""
    if not ipc_bytes:
        return _EMPTY_SCHEMA
    return ipc.read_schema(pa.py_buffer(ipc_bytes))


def _comparable(field_name: str, value: object) -> object:
    """Normalize one field value so equality means what a reader expects.

    Schemas travel as serialized Arrow IPC, and reflection's own docstring is
    explicit that those bytes need not match between ports -- the hash is taken
    over the decoded structure for exactly that reason.  Comparing the bytes
    would therefore manufacture differences beside an agreeing hash, which is
    the same failure mode in the opposite direction as the one this comparison
    exists to avoid.  So decode, then compare.

    The rule is on the field *name suffix*, not a list of the three fields that
    have it today, so a fourth schema added to ``MethodInfo`` is decoded the day
    it arrives rather than the day someone notices.
    """
    if field_name.endswith("_schema_ipc"):
        return _read_schema(value)  # type: ignore[arg-type]
    return value


def _diff_dataclass(port_obj: object, ref_obj: object, skip: frozenset[str] = frozenset()) -> list[tuple[str, str]]:
    """Diff two instances of the same dataclass, by reflection over its fields.

    Every field, obtained from ``dataclasses.fields()`` rather than a
    hand-written list.

    This comparison has been wrong three times, each time the same way: it
    checked the fields someone remembered to add, reported "shapes agree"
    beside a hash mismatch, and sent the reader looking in the wrong place.
    Header schemas, protocol names and stream kind were each missed that way.
    Enumerating the fields removes the class of mistake rather than its latest
    instance -- a new field is compared the day it is added.

    That property is why the comparison is done against the *reflection wire*
    dataclasses (``ServiceDescription`` / ``MethodInfo``) rather than the
    client-side view in ``vgi_rpc.introspect``.  The client-side view is a
    convenience shape that drops fields the wire carries -- ``idempotency``,
    ``deprecated``, ``features`` -- and every field it drops is a field inside
    the hash preimage.  Reflecting over a lossy view reintroduces exactly the
    bug reflection was adopted to kill: a hash that moves for a reason the
    shape diff cannot name.

    Returns a list of ``(field_name, rendered difference)`` pairs.  Values are
    rendered *port first, then reference*, matching the heading each caller
    prints above them; getting that backwards sends a reader to fix the wrong
    side.
    """
    out: list[tuple[str, str]] = []
    for f in dataclasses.fields(ref_obj):  # type: ignore[arg-type]
        if f.name in skip:
            continue
        pv = _comparable(f.name, getattr(port_obj, f.name))
        rv = _comparable(f.name, getattr(ref_obj, f.name))
        if pv == rv:
            continue
        out.append((f.name, f"{_render(pv)} vs {_render(rv)}"))
    return out


def _suppressed(field_name: str, port: MethodInfo, ref: MethodInfo) -> bool:
    """Whether a differing field is noise governed by a flag reported already.

    A result or header schema is meaningless when the flag that governs it is
    false, and the flag itself is always reported, so do not also report the
    schema nobody looks at.  Note the asymmetry with ``_diff_dataclass``: a
    suppression that someone forgets to add produces noise, whereas a
    *comparison* that someone forgets to add produces a false "agrees".  Only
    the second is worth designing against, which is why this list is allowed to
    be explicit while that one is not.
    """
    if field_name == "result_schema_ipc":
        return not (port.has_return and ref.has_return)
    if field_name == "header_schema_ipc":
        return not (port.has_header and ref.has_header)
    return False


def _diff_description(port: ServiceDescription, ref: ServiceDescription) -> list[str]:
    """Every way two descriptions of the same protocol differ, as flat lines.

    ``protocol_hash`` is excluded because the caller has already grouped ports
    by it and printed the groups; repeating it once per port turns a two-line
    fact into a screenful.
    """
    lines: list[str] = []

    for field_name, detail in _diff_dataclass(port, ref, skip=frozenset({"methods", "protocol_hash"})):
        lines.append(f"{field_name} {detail}")

    port_methods = {m.name: m for m in port.methods}
    ref_methods = {m.name: m for m in ref.methods}

    only_ref = sorted(set(ref_methods) - set(port_methods))
    only_port = sorted(set(port_methods) - set(ref_methods))
    if only_ref:
        lines.append(f"missing: {', '.join(only_ref)}")
    if only_port:
        lines.append(f"extra:   {', '.join(only_port)}")

    shape: list[str] = []
    for method in sorted(set(ref_methods) & set(port_methods)):
        pm, rm = port_methods[method], ref_methods[method]
        for field_name, detail in _diff_dataclass(pm, rm):
            if _suppressed(field_name, pm, rm):
                continue
            shape.append(f"{method}: {field_name} {detail}")

    lines.extend(shape[:15])
    if len(shape) > 15:
        lines.append(f"... and {len(shape) - 15} more method field difference(s)")
    return lines


def compare_protocol(hosts: dict[str, ServiceDescription]) -> list[str]:
    """Compare one protocol across the ports that host it.

    Ports are grouped by ``protocol_hash`` first, and the structural diff is
    then taken *once per group* rather than once per port.  Six ports agreeing
    with each other and disagreeing with the seventh is one difference, and
    printing it six times buries it.

    The grouping is only sound if equal hashes imply equal descriptions -- the
    hash is defined over the description, so they must -- and that implication
    is checked rather than assumed: members of a group are diffed against each
    other, and a difference inside a group means the hash is not covering
    something it claims to.  Skipping that check would make the readability
    win a place for drift to hide, which is the opposite of the point.

    Returns the detail lines to print; empty means agreement.
    """
    groups: dict[str, list[str]] = {}
    for name, desc in hosts.items():
        groups.setdefault(desc.protocol_hash, []).append(name)
    for names in groups.values():
        names.sort()

    ref_hash = next((h for h, names in groups.items() if "python" in names), None)
    if ref_hash is None:
        ref_hash = max(groups, key=lambda h: (len(groups[h]), h))
    ref_name = "python" if "python" in groups[ref_hash] else groups[ref_hash][0]
    reference = hosts[ref_name]

    lines: list[str] = []

    # Equal hash must mean equal description.
    for digest, names in sorted(groups.items()):
        base = hosts[names[0]]
        for other in names[1:]:
            within = _diff_description(hosts[other], base)
            if within:
                lines.append(
                    f"  {other} and {names[0]} share hash {digest[:12]}... but describe differently"
                    " -- the hash is not covering something:"
                )
                lines.extend(f"    {line}" for line in within)

    # Then one block per group that disagrees with the reference group.
    for digest, names in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        if digest == ref_hash:
            continue
        rep = names[0]
        label = rep if len(names) == 1 else f"{rep} (and {', '.join(names[1:])})"
        lines.append(f"  {label} vs {ref_name}:")
        detail = _diff_description(hosts[rep], reference)
        if detail:
            lines.extend(f"    {line}" for line in detail)
        else:
            lines.append("    no structural difference found, yet the hashes differ -- compare the")
            lines.append("    canonical preimages; something in them is outside this dataclass view.")

    return lines


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def main() -> None:
    """Describe every port, then compare every protocol -- hash first."""
    reports: dict[str, PortReport | str] = {}

    print("Connecting to python...", file=sys.stderr)
    reports["python"] = get_python_report()

    for name, config in SUBPROCESS_WORKERS.items():
        print(f"Connecting to {name}...", file=sys.stderr)
        reports[name] = get_subprocess_report(name, config)

    reachable = {k: v for k, v in reports.items() if isinstance(v, PortReport)}
    unavailable = sorted(k for k, v in reports.items() if isinstance(v, str))

    # --- What each port hosts ----------------------------------------------
    print()
    for name, report in reports.items():
        if isinstance(report, str):
            print(f"{name:12s}  UNAVAILABLE: {report}")
            continue
        hosted = ", ".join(
            f"{p}({len(d.methods)})" if isinstance(d, ServiceDescription) else f"{p}(!)"
            for p, d in sorted(report.protocols.items())
        )
        print(f"{name:12s}  {hosted}")

    if len(reachable) < 2:
        print()
        print("Need at least two reachable ports to compare.")
        sys.exit(1)

    failed = False

    # --- Per protocol, across the ports that host it -----------------------
    hosting: dict[str, dict[str, ServiceDescription]] = {}
    # Listed-but-not-describable is its own state.  Folding it into "absent"
    # would report a port that hosts a protocol as one that does not, which
    # turns a broken server into a legitimate deployment choice.
    broken: dict[str, dict[str, str]] = {}
    for name, report in reachable.items():
        for protocol, described in report.protocols.items():
            hosting.setdefault(protocol, {})
            if isinstance(described, ServiceDescription):
                hosting[protocol][name] = described
            else:
                broken.setdefault(protocol, {})[name] = described

    details: list[str] = []
    asymmetries: list[str] = []
    undescribable: list[str] = []

    print()
    for protocol in sorted(hosting):
        hosts = hosting[protocol]
        unusable = broken.get(protocol, {})
        absent = sorted(set(reachable) - set(hosts) - set(unusable))

        hashes: dict[str, list[str]] = {}
        for name, desc in hosts.items():
            hashes.setdefault(desc.protocol_hash, []).append(name)

        span = f"{len(hosts)}/{len(reachable)} ports"
        if not hosts:
            verdict = "NOT DESCRIBABLE"
            digest = ""
        elif len(hosts) == 1:
            verdict = "ONE PORT ONLY"
            digest = next(iter(hosts.values())).protocol_hash
        elif len(hashes) == 1:
            verdict = "AGREED"
            digest = next(iter(hashes))
        else:
            verdict = "DRIFT"
            digest = ""
            failed = True

        print(f"{protocol:24s}  {span:14s}  {verdict:15s} {digest}")

        # Run the same comparison whatever the verdict.  Under DRIFT it says
        # what differs; under AGREED it is the check that the hash is not
        # quietly agreeing over descriptions that do not.
        detail = compare_protocol(hosts) if len(hosts) > 1 else []
        if verdict == "DRIFT":
            details.append(f"{protocol}: {len(hashes)} distinct hashes")
            for d, names in sorted(hashes.items(), key=lambda kv: (-len(kv[1]), kv[0])):
                marker = "  <- reference" if "python" in names else ""
                details.append(f"  {d}  {', '.join(sorted(names))}{marker}")
            details.extend(detail)
        elif detail:
            failed = True
            details.append(f"{protocol}: hashes agree but descriptions do not")
            details.extend(detail)

        if absent:
            asymmetries.append(f"{protocol}: not hosted by {', '.join(absent)}")
        for port_name, why in sorted(unusable.items()):
            failed = True
            undescribable.append(f"{protocol}: {port_name} lists it but {why}")

    # --- Versions ----------------------------------------------------------
    versions = {k: v.listing.request_version for k, v in reachable.items()}
    if len(set(versions.values())) > 1:
        failed = True
        spread = ", ".join(f"{k}={v or '(none)'}" for k, v in sorted(versions.items()))
        details.append(f"request_version drift: {spread}")

    if details:
        print()
        for line in details:
            print(line)
        print()
        print("Each port ships the canonical preimage beside its digest, so a hash")
        print("mismatch is a JSON diff rather than a guess. In Python:")
        print("  cat vgi-rpc-python/tests/golden/protocol_hash_vector.json")

    # --- Asymmetries are findings, not failures ----------------------------
    if asymmetries:
        print()
        print("Asymmetries (reported, not failures -- a protocol is hosted only where wired up):")
        for line in asymmetries:
            print(f"  {line}")

    if undescribable:
        print()
        print("Listed but not describable (failures -- a server must describe what it hosts):")
        for line in undescribable:
            print(f"  {line}")

    if unavailable:
        print()
        print(f"Not checked ({len(unavailable)}): {', '.join(unavailable)}")
        print("  A port that could not be reached is not a port that agrees.")

    print()
    if failed:
        print("RESULT: drift")
        sys.exit(1)
    summary = f"RESULT: all {len(hosting)} protocol(s) agree across the {len(reachable)} reachable ports"
    if asymmetries:
        summary += f" ({len(asymmetries)} hosted by some but not all)"
    print(summary)
    if unavailable:
        print(f"        {len(unavailable)} port(s) not checked")
        sys.exit(2)


if __name__ == "__main__":
    main()
