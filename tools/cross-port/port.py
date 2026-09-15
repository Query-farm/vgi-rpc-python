#!/usr/bin/env python3
"""Generate a Claude prompt for porting a vgi-rpc feature between languages."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPOS_DIR = Path.home() / "Development"

REPOS = {
    "python": REPOS_DIR / "vgi-rpc",
    "go": REPOS_DIR / "vgi-rpc-go",
    "typescript": REPOS_DIR / "vgi-rpc-typescript",
}

TARGET_ALIASES = {"ts": "typescript"}


def find_python_source(feature: str) -> list[Path]:
    """Find Python source files for a feature using naming conventions."""
    repo = REPOS["python"]
    candidates = [
        repo / "vgi_rpc" / f"{feature}.py",
        repo / "vgi_rpc" / feature,  # directory (e.g., http/, rpc/)
        repo / "vgi_rpc" / "http" / f"_{feature}.py",  # http submodule
        repo / "vgi_rpc" / "http" / f"{feature}.py",
        repo / "vgi_rpc" / "rpc" / f"_{feature}.py",  # rpc submodule
        repo / "vgi_rpc" / "rpc" / f"{feature}.py",
    ]
    found = []
    for c in candidates:
        if c.is_file():
            found.append(c)
        elif c.is_dir() and any(c.glob("*.py")):
            found.extend(sorted(f for f in c.glob("*.py") if f.name != "__init__.py"))
    return found


def find_python_tests(feature: str) -> list[Path]:
    """Find test files for a feature."""
    repo = REPOS["python"]
    candidates = [
        repo / "tests" / f"test_{feature}.py",
    ]
    return [c for c in candidates if c.is_file()]


def generate_prompt(feature: str, target: str, source_override: str | None) -> str:
    """Generate the porting prompt."""
    target_repo = REPOS[target]
    target_claude = target_repo / "CLAUDE.md"

    # Find source files
    sources = [Path(source_override).expanduser().resolve()] if source_override else find_python_source(feature)

    tests = find_python_tests(feature)

    if not sources:
        tried = [
            f"  vgi_rpc/{feature}.py",
            f"  vgi_rpc/{feature}/",
            f"  vgi_rpc/http/_{feature}.py",
            f"  vgi_rpc/rpc/_{feature}.py",
        ]
        print(
            f"Could not find Python source for '{feature}'. Tried:\n"
            + "\n".join(tried)
            + "\n\nUse --source to specify the path explicitly.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build prompt
    lines = [
        f"Port the `{feature}` feature from the Python vgi-rpc implementation to {target.title()}.",
        "",
        "## Python Source (reference implementation)",
    ]
    for s in sources:
        lines.append(f"File: {s}")
    lines.append("")

    if tests:
        lines.append("## Python Tests")
        for t in tests:
            lines.append(f"File: {t}")
        lines.append("")

    lines.extend(
        [
            "## Target Repository",
            str(target_repo),
            "",
        ]
    )

    if target_claude.is_file():
        lines.extend(
            [
                "## Target Conventions",
                target_claude.read_text().strip(),
                "",
            ]
        )

    lines.extend(
        [
            "## Instructions",
            "- Read the Python source and tests to understand the feature",
            f"- Implement the equivalent in {target.title()} following the patterns in the target repo",
            "- The implementation should pass the same logical test cases as the Python version",
            "- Follow the coding conventions in the target repo's CLAUDE.md",
            "- Write tests for the new implementation",
            "",
        ]
    )

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Claude prompt for porting a vgi-rpc feature.",
        epilog="Example: ./port.py external_storage --to go | claude",
    )
    parser.add_argument("feature", help="Feature name (e.g., external_storage, bearer, otel)")
    parser.add_argument("--to", required=True, dest="target", help="Target language: go or typescript (or ts)")
    parser.add_argument("--source", help="Override Python source file path")
    args = parser.parse_args()

    target = TARGET_ALIASES.get(args.target, args.target)
    if target not in ("go", "typescript"):
        print(f"Unknown target '{args.target}'. Use 'go' or 'typescript' (or 'ts').", file=sys.stderr)
        sys.exit(1)

    if not REPOS[target].is_dir():
        print(f"Target repo not found: {REPOS[target]}", file=sys.stderr)
        sys.exit(1)

    print(generate_prompt(args.feature, target, args.source))


if __name__ == "__main__":
    main()
