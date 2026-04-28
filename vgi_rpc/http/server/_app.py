# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The internal ``_HttpRpcApp`` — state holder for HTTP dispatch.

This class holds the configuration shared between the unary and stream
request paths (the ``RpcServer``, signing key, per-method state-type
index, size/time limits, upload-url provider, token TTL).  The heavy
per-request logic lives in :mod:`_app_unary` and :mod:`_app_stream`,
which take the app as their first argument; the methods on this class
are thin delegating wrappers so that :mod:`_resources` can keep calling
``app._unary_sync(...)`` etc. without knowing about the split.
"""

from __future__ import annotations

from http import HTTPStatus
from io import BytesIO, IOBase

import falcon

from vgi_rpc.external import UploadUrlProvider
from vgi_rpc.rpc import RpcMethodInfo, RpcServer

from .._common import _RpcHttpError
from ._responses import _check_content_type
from ._state_token import _resolve_state_types


class _HttpRpcApp:
    """Internal helper that wraps an RpcServer and manages stream state."""

    __slots__ = (
        "_max_request_bytes",
        "_max_stream_response_bytes",
        "_max_upload_bytes",
        "_server",
        "_signing_key",
        "_state_types",
        "_token_ttl",
        "_upload_url_provider",
    )

    def __init__(
        self,
        server: RpcServer,
        signing_key: bytes,
        max_stream_response_bytes: int | None = None,
        max_request_bytes: int | None = None,
        upload_url_provider: UploadUrlProvider | None = None,
        max_upload_bytes: int | None = None,
        token_ttl: int = 3600,
    ) -> None:
        self._server = server
        self._signing_key = signing_key
        self._state_types = _resolve_state_types(server)
        self._max_stream_response_bytes = max_stream_response_bytes
        self._max_request_bytes = max_request_bytes
        self._upload_url_provider = upload_url_provider
        self._max_upload_bytes = max_upload_bytes
        self._token_ttl = token_ttl

    def _resolve_method(self, req: falcon.Request, method: str) -> RpcMethodInfo:
        """Validate content type and resolve method info.

        Raises:
            _RpcHttpError: If content type is wrong or method is unknown.

        """
        _check_content_type(req)
        info = self._server.methods.get(method)
        if info is None:
            available = sorted(self._server.methods.keys())
            raise _RpcHttpError(
                AttributeError(f"Unknown method: '{method}'. Available methods: {available}"),
                status_code=HTTPStatus.NOT_FOUND,
            )
        return info

    def _unary_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> tuple[BytesIO, HTTPStatus]:
        """Delegate to :func:`_app_unary._run_unary_sync`."""
        from ._app_unary import _run_unary_sync

        return _run_unary_sync(self, method_name, info, stream)

    def _stream_init_sync(self, method_name: str, info: RpcMethodInfo, stream: IOBase) -> BytesIO:
        """Delegate to :func:`_app_stream._run_stream_init_sync`."""
        from ._app_stream import _run_stream_init_sync

        return _run_stream_init_sync(self, method_name, info, stream)

    def _stream_exchange_sync(self, method_name: str, stream: IOBase) -> BytesIO:
        """Delegate to :func:`_app_stream._run_stream_exchange_sync`."""
        from ._app_stream import _run_stream_exchange_sync

        return _run_stream_exchange_sync(self, method_name, stream)
