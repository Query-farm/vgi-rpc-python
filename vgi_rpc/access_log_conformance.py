"""Access log conformance validator for VGI servers.

Reads JSON log lines from ``vgi_rpc.access``, applies per-method-type
rules, and reports violations.  Language-agnostic — any VGI server
implementation dumps its access logs, this tool checks them.

Usage::

    python -m vgi_rpc.access_log_conformance /tmp/vgi-http-test-server.log
    cat server.log | python -m vgi_rpc.access_log_conformance

Exit code 0 if all entries pass, 1 if any violations are found.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Violation:
    """A single conformance violation."""

    entry_index: int
    method: str
    field: str
    rule: str


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

# Fields required on every access log entry regardless of method.
_ALWAYS_REQUIRED = ["server_version", "authenticated"]

# Fields required on entries that carry request parameters.
# These methods go through _read_request() which stashes the raw batch.
_REQUEST_DATA_METHODS = {"bind", "init", "table_function_cardinality"}

# Stream methods that must have a stream_id.
_STREAM_METHODS = {"init"}


def _is_catalog_method(method: str) -> bool:
    return method.startswith("catalog_")


def validate_access_logs(entries: list[dict[str, object]]) -> list[Violation]:
    """Validate a list of parsed access log entries against conformance rules.

    Args:
        entries: Parsed JSON dicts from ``vgi_rpc.access`` log lines.

    Returns:
        List of violations (empty if all entries conform).

    """
    violations: list[Violation] = []

    for i, entry in enumerate(entries):
        method = str(entry.get("method", ""))
        status = str(entry.get("status", ""))

        # --- Always required ---
        violations.extend(
            Violation(i, method, field, "required on all entries") for field in _ALWAYS_REQUIRED if field not in entry
        )

        # --- request_data on dispatch methods ---
        if (method in _REQUEST_DATA_METHODS or _is_catalog_method(method)) and not entry.get("request_data"):
            violations.append(Violation(i, method, "request_data", "required on dispatch methods"))

        # --- stream_id ---
        if method in _STREAM_METHODS and not entry.get("stream_id"):
            violations.append(Violation(i, method, "stream_id", "required on stream methods"))

        # --- HTTP exchange/produce continuations ---
        # These are stream method entries that lack request_data (not the init).
        # They should have stream_id from the state token.
        method_type = str(entry.get("method_type", ""))
        if (
            method_type == "stream"
            and method not in _STREAM_METHODS
            and not _is_catalog_method(method)
            and not entry.get("stream_id")
        ):
            violations.append(Violation(i, method, "stream_id", "required on stream continuations (from state token)"))

        # --- error_message on errors ---
        if status == "error" and not entry.get("error_message"):
            violations.append(Violation(i, method, "error_message", "required on error entries"))

    return violations


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_json_log_lines(source: list[str]) -> list[dict[str, object]]:
    """Parse JSON log lines, skipping non-JSON lines."""
    entries: list[dict[str, object]] = []
    for line in source:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                entries.append(obj)
        except json.JSONDecodeError:
            continue
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
            status = ""
            # Find the entry to show status if error
            if v.entry_index < len(access_logs):
                entry = access_logs[v.entry_index]
                if entry.get("status") == "error":
                    status = ", status=error"
            print(f"  entry {v.entry_index} (method={v.method}{status}): {v.rule} — missing '{v.field}'")
        return 1

    print(f"PASS: {len(access_logs)} entries validated, 0 violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
