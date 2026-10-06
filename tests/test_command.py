"""Tests for the cross-platform worker command splitter."""

from __future__ import annotations

from vgi_rpc._command import split_command


def test_posix_split_unquotes() -> None:
    """POSIX splitting is plain ``shlex``."""
    assert split_command('"/opt/my python/bin/python" -m mod --flag', platform="linux") == [
        "/opt/my python/bin/python",
        "-m",
        "mod",
        "--flag",
    ]


def test_windows_quoted_executable_loses_its_quotes() -> None:
    """A quoted Windows path keeps its backslashes and loses its quotes.

    ``shlex.split(posix=False)`` alone returns the token with the quote
    characters attached, which ``CreateProcess`` cannot find.
    """
    cmd = r'"D:\a\repo\.venv\Scripts\python.exe" -m vgi_rpc.conformance._cli --pipe --describe'
    assert split_command(cmd, platform="win32") == [
        r"D:\a\repo\.venv\Scripts\python.exe",
        "-m",
        "vgi_rpc.conformance._cli",
        "--pipe",
        "--describe",
    ]


def test_windows_path_with_spaces() -> None:
    """Spaces inside the quotes stay in one token."""
    assert split_command(r'"C:\Program Files\Python\python.exe" -V', platform="win32") == [
        r"C:\Program Files\Python\python.exe",
        "-V",
    ]


def test_windows_unquoted_tokens_are_untouched() -> None:
    """Unquoted tokens pass through, backslashes intact."""
    assert split_command(r"C:\tools\worker.exe --pipe", platform="win32") == [r"C:\tools\worker.exe", "--pipe"]
