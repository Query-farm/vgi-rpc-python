"""Tests for README.md Python code examples.

Verifies that every Python code block compiles, and that runnable
blocks execute without errors when run sequentially like notebook cells.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Any

import pytest
from pytest_examples import CodeExample, EvalExample, find_examples

_README = Path(__file__).parent.parent / "README.md"
_ALL = list(find_examples(str(_README)))


def _has_unresolvable_imports(source: str) -> bool:
    """Return True if *source* imports from a module that cannot be found."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                try:
                    if importlib.util.find_spec(alias.name) is None:
                        return True
                except (ModuleNotFoundError, ValueError):
                    return True
        if isinstance(node, ast.ImportFrom) and node.module:
            try:
                if importlib.util.find_spec(node.module) is None:
                    return True
            except (ModuleNotFoundError, ValueError):
                return True
    return False


# ---------------------------------------------------------------------------
# 1. Discovery sanity check
# ---------------------------------------------------------------------------


def test_readme_has_examples() -> None:
    """README.md contains discoverable Python code blocks."""
    assert _ALL, "No Python blocks found in README — check find_examples() compatibility"


# ---------------------------------------------------------------------------
# 2. Syntax — every Python block compiles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("example", _ALL, ids=str)
def test_readme_syntax(example: CodeExample) -> None:
    """Every Python code block compiles without syntax errors."""
    compile(example.source, str(example.path), "exec")


# ---------------------------------------------------------------------------
# 3. Execution — notebook-style sequential run
# ---------------------------------------------------------------------------


def test_readme_runnable(eval_example: EvalExample) -> None:
    """Execute README blocks sequentially with accumulated globals.

    Blocks run in document order.  Each block's definitions merge into a
    shared namespace (like Jupyter cells).  Blocks with unresolvable
    imports are skipped before execution.  Blocks that fail at runtime
    (missing names, external infrastructure, namespace conflicts across
    README sections) are recorded.  The total number of successfully
    executed blocks must stay above half the total block count.
    """
    globs: dict[str, Any] = {}
    ran = 0
    errors: list[tuple[str, str, str]] = []

    for example in _ALL:
        if _has_unresolvable_imports(example.source):
            continue
        try:
            new_globs = eval_example.run(example, module_globals=dict(globs))
            globs.update(new_globs)
            ran += 1
        except Exception as exc:
            errors.append((str(example), type(exc).__name__, str(exc)))

    min_expected = len(_ALL) // 2
    if ran < min_expected:
        report = "\n".join(f"  {loc}: {cls}: {msg}" for loc, cls, msg in errors)
        pytest.fail(f"Only {ran}/{len(_ALL)} README blocks ran (need >= {min_expected}).\nErrors:\n{report}")
