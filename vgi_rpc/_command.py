"""Split a worker command line into argv on every platform."""

from __future__ import annotations

import shlex
import sys

__all__ = ["split_command"]


def split_command(cmd: str, *, platform: str | None = None) -> list[str]:
    r"""Split *cmd* into an argv list for ``subprocess``.

    POSIX uses ``shlex`` as-is. On Windows, ``shlex.split(posix=False)`` keeps
    the quotes around a token, so a quoted executable path such as
    ``"C:\Program Files\python.exe"`` would reach ``CreateProcess`` with the
    quote characters still in it and fail with ``FileNotFoundError``. Strip one
    matching pair of surrounding quotes from each token; backslashes survive
    because non-POSIX mode never treats them as escapes.

    Args:
        cmd: The command line, as a user would type it in their shell.
        platform: Overrides ``sys.platform``; for tests.

    Returns:
        The argv list.

    """
    if (platform or sys.platform) != "win32":
        return shlex.split(cmd)
    return [
        token[1:-1] if len(token) >= 2 and token[0] == token[-1] and token[0] in "\"'" else token
        for token in shlex.split(cmd, posix=False)
    ]
