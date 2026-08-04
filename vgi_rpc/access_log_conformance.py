"""Access log conformance validator for vgi-rpc servers.

Reads JSON log lines from any source (file or stdin) and validates each
``vgi_rpc.access`` entry against ``access_log.schema.json``.  The schema
is the source of truth; see ``docs/access-log-spec.md`` for the prose.

This validator is language-agnostic: any vgi-rpc server implementation
that emits records on the ``vgi_rpc.access`` channel can pipe its log
through this tool to check conformance.

Usage::

    python -m vgi_rpc.access_log_conformance /tmp/vgi-http-test-server.log
    cat server.log | python -m vgi_rpc.access_log_conformance

Exit code 0 if all entries pass, 1 if any violations are found.
"""

from __future__ import annotations

import base64
import binascii
import json
import sys
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any

import jsonschema


@dataclass(frozen=True)
class Violation:
    """A single conformance violation."""

    entry_index: int
    method: str
    path: str
    message: str


@lru_cache(maxsize=1)
def _load_schema() -> dict[str, Any]:
    """Load the access log JSON Schema from the package."""
    text = resources.files("vgi_rpc").joinpath("access_log.schema.json").read_text(encoding="utf-8")
    schema: dict[str, Any] = json.loads(text)
    jsonschema.Draft202012Validator.check_schema(schema)
    return schema


def _check_request_data(index: int, method: str, entry: dict[str, object]) -> list[Violation]:
    """Check that ``request_data`` round-trips as an Arrow IPC stream.

    Schema validation cannot reach this: to JSON Schema the field is just a
    string. But ``docs/access-log-spec.md`` §4.3 calls round-trip
    equivalence *the* conformance test for it, and until this existed
    nothing checked — the Python reference shipped
    ``RecordBatch.serialize()``, a bare encapsulated message with no schema
    ahead of it, which fails ``open_stream`` outright. Every port could have
    picked a different wrong answer and all of them would have passed.

    Deliberately checks round-trip, not bytes: a port is free to use
    whatever encoding its Arrow library produces, so long as a reader gets
    the batch back.

    Args:
        index: Position of the entry in the log.
        method: The record's method name, for the violation message.
        entry: The parsed record.

    Returns:
        Violations found (empty when the field is absent or valid).

    """
    raw = entry.get("request_data")
    if raw is None:
        return []
    if not isinstance(raw, str):
        return [Violation(index, method, "request_data", f"must be a base64 string, got {type(raw).__name__}")]
    try:
        # validate=True rejects non-alphabet characters rather than skipping
        # them, and strict padding is required by the spec.
        decoded = base64.b64decode(raw, validate=True)
    except (ValueError, binascii.Error) as exc:
        return [Violation(index, method, "request_data", f"not valid base64 (RFC 4648, padding required): {exc}")]
    if not decoded:
        return [Violation(index, method, "request_data", "decoded to zero bytes")]

    try:
        import pyarrow as pa
        from pyarrow import ipc
    except ImportError:  # pragma: no cover - pyarrow is a hard dependency
        return []

    try:
        reader = ipc.open_stream(pa.BufferReader(decoded))
    except Exception as exc:
        return [
            Violation(
                index,
                method,
                "request_data",
                f"does not decode as a self-contained Arrow IPC stream "
                f"(schema message then record batch message): {type(exc).__name__}: {exc}. "
                f"A single encapsulated message -- what RecordBatch.serialize() produces -- is not a stream.",
            )
        ]
    try:
        batch = reader.read_next_batch()
    except StopIteration:
        return [Violation(index, method, "request_data", "stream decoded but contained no record batch")]
    except Exception as exc:
        return [
            Violation(index, method, "request_data", f"record batch could not be read: {type(exc).__name__}: {exc}")
        ]
    if batch.num_rows != 1:
        return [
            Violation(
                index,
                method,
                "request_data",
                f"request batch must carry exactly one row of parameters, got {batch.num_rows}",
            )
        ]
    return []


def validate_access_logs(entries: list[dict[str, object]]) -> list[Violation]:
    """Validate parsed access log entries against the JSON Schema.

    Args:
        entries: Parsed JSON dicts from ``vgi_rpc.access`` log lines.

    Returns:
        List of violations (empty if all entries conform).

    """
    validator = jsonschema.Draft202012Validator(_load_schema())
    violations: list[Violation] = []
    for i, entry in enumerate(entries):
        method = str(entry.get("method", ""))
        for err in validator.iter_errors(entry):
            path = "/".join(str(p) for p in err.absolute_path) or "<root>"
            violations.append(Violation(i, method, path, err.message))
        violations.extend(_check_request_data(i, method, entry))
    return violations


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_json_log_lines(source: list[str]) -> list[dict[str, object]]:
    """Parse JSON log lines, skipping non-JSON lines."""
    entries: list[dict[str, object]] = []
    for raw in source:
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            entries.append(obj)
    return entries


def _filter_access_logs(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Keep only vgi_rpc.access entries."""
    return [e for e in entries if e.get("logger") == "vgi_rpc.access"]


def main(argv: list[str] | None = None) -> int:
    """Run the conformance validator from the command line."""
    args = argv if argv is not None else sys.argv[1:]

    if args and args[0] not in ("-", "--help", "-h"):
        path = Path(args[0])
        if not path.exists():
            print(f"ERROR: File not found: {path}", file=sys.stderr)
            return 1
        lines = path.read_text().splitlines()
    else:
        if args and args[0] in ("--help", "-h"):
            print(__doc__ or "")
            return 0
        lines = sys.stdin.read().splitlines()

    all_entries = _parse_json_log_lines(lines)
    access_logs = _filter_access_logs(all_entries)

    if not access_logs:
        print("WARNING: No vgi_rpc.access entries found in input.", file=sys.stderr)
        return 1

    violations = validate_access_logs(access_logs)

    if violations:
        print(f"FAIL: {len(access_logs)} entries validated, {len(violations)} violations")
        for v in violations:
            print(f"  entry {v.entry_index} (method={v.method}, path={v.path}): {v.message}")
        return 1

    print(f"PASS: {len(access_logs)} entries validated, 0 violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
