# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Synchronous adapter around :mod:`aiointercept` for the aiohttp test suites.

The fetch/external tests are synchronous (``def test_...``) and drive
:func:`vgi_rpc.external_fetch.fetch_url`, which runs aiohttp on its own daemon
thread and event loop. :mod:`aiointercept` is an *async* context manager that
boots a real localhost test server and patches aiohttp's DNS resolver at the
class level so requests to any registered host are transparently intercepted
(``mock_external_urls=True``) — exactly what is needed to mock the presigned
URLs the fetcher downloads.

:func:`mock_aiohttp` bridges the two: it drives ``aiointercept``'s async
``start``/``stop`` from synchronous test code on a transient event loop. The
class-level patches it installs are global, so they intercept the fetcher's
daemon-thread requests too. Registered callbacks are synchronous here, so the
transient loop used for startup never needs to outlive ``start()``.

This replaces ``aioresponses`` (which broke on aiohttp >=3.14); the registration
surface (``get``/``head``/``post``/...; ``CallbackResult``; ``callback(url, *,
headers, query, json, data)``) is API-compatible, so test bodies are unchanged.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
from typing import Any

from aiointercept import CallbackResult, aiointercept

__all__ = ["CallbackResult", "aiointercept", "mock_aiohttp"]


def _run_sync(coro: Coroutine[Any, Any, None]) -> None:
    """Run *coro* to completion on a throwaway loop in a dedicated thread.

    A dedicated thread keeps this safe whether or not the *calling* thread
    already has a running event loop — some tests drive ``fetch_url`` from
    inside their own loop (e.g. the nested-event-loop case), where a plain
    :func:`asyncio.run` would raise ``RuntimeError``. ``aiointercept`` runs its
    test server on its own daemon thread, so this loop only needs to live long
    enough to execute ``start``/``stop``; the registered callbacks here are
    synchronous and never reach back to it.

    Args:
        coro: The ``start()`` or ``stop()`` coroutine to drive.

    """
    error: list[BaseException] = []

    def _target() -> None:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        except BaseException as exc:  # surface failures to the caller thread
            error.append(exc)
        finally:
            loop.close()

    thread = threading.Thread(target=_target, name="aiomock-lifecycle")
    thread.start()
    thread.join()
    if error:
        raise error[0]


@contextmanager
def mock_aiohttp() -> Iterator[aiointercept]:
    """Yield an :class:`aiointercept.aiointercept` mock, driven synchronously.

    Intercepts requests to any registered host (``mock_external_urls=True``) so
    the daemon-thread fetcher's HTTPS requests to presigned URLs are mocked.

    Yields:
        The started ``aiointercept`` instance; register handlers on it with
        ``.get()`` / ``.head()`` / ``.post()`` and friends.

    """
    mock = aiointercept(mock_external_urls=True)
    _run_sync(mock.start())
    try:
        yield mock
    finally:
        _run_sync(mock.stop())
