# Contributing to vgi-rpc

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Dev Setup

```bash
git clone https://github.com/Query-farm/vgi-rpc-python.git
cd vgi-rpc-python
uv sync --all-extras
```

This installs the package in editable mode with all optional dependencies (HTTP, S3, GCS, CLI, external storage) plus dev tools (pytest, mypy, ruff, ty, mutmut).

If you change entry points or add new modules, reinstall:

```bash
uv sync --all-extras --reinstall-package vgi-rpc
```

## Running Tests

```bash
# Full suite (includes mypy type checking and ruff linting via pytest plugins)
uv run pytest

# Single test
uv run pytest tests/test_rpc.py::test_name -v

# Skip mypy/ruff checks for faster iteration
uv run pytest tests/test_rpc.py -v -o "addopts="

# Coverage report (80% minimum, branch coverage)
uv run pytest --cov=vgi_rpc
```

Tests must complete in **50 seconds or less**.

## Pre-Commit Checklist

Run all four checks before committing:

```bash
# 1. Static type checking (strict mode)
uv run mypy vgi_rpc/

# 2. ty type checking
uv run ty check

# 3. Tests (includes mypy + ruff via pytest plugins)
uv run pytest

# 4. Format
uv run ruff format vgi_rpc/ tests/
```

All four must pass. Do not skip any.

## Code Style

| Rule | Value |
|---|---|
| Line length | 120 |
| Quote style | Double quotes |
| Python target | 3.12+ |
| Type checking | mypy strict + ty |
| Linter | ruff (E, F, I, UP, B, SIM, D, RUF, PERF) |
| Docstrings | Google style with Args/Returns/Raises |

Avoid `Any` types. Strict mypy is enforced and must stay clean.

### Lint and format manually

```bash
uv run ruff check vgi_rpc/ tests/
uv run ruff format vgi_rpc/ tests/
```

## Pull Request Expectations

- **One concern per PR.** Keep changes focused: a bug fix, a feature, or a refactor -- not all three.
- **Tests required.** New features need tests. Bug fixes need a regression test.
- **Type-safe.** All new code must pass `mypy --strict` and `ty check` cleanly.
- **Docstrings.** Public APIs need Google-style docstrings.
- **No unrelated changes.** Don't re-format files you didn't change or add drive-by refactors.
- **PR description.** Explain *what* changed and *why*. Link related issues.
