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
