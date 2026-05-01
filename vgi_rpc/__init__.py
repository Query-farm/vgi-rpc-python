# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport-agnostic RPC framework built on Apache Arrow IPC serialization."""

import importlib
import importlib.util
import logging
from typing import Any

from vgi_rpc.external import (
    ClientExternalConfig,
    Compression,
    ExternalLocationConfig,
    ExternalStorage,
    ServerExternalConfig,
    UploadUrl,
    UploadUrlProvider,
    https_only_validator,
)
from vgi_rpc.external_fetch import FetchConfig
from vgi_rpc.introspect import (
    DESCRIBE_METHOD_NAME,
    MethodDescription,
    ServiceDescription,
    introspect,
)
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import REQUEST_VERSION
from vgi_rpc.pool import PoolMetrics, WorkerPool
from vgi_rpc.rpc import (
    AnnotatedBatch,
    AuthContext,
    CallContext,
    CallStatistics,
    ClientLog,
    ExchangeState,
    MethodType,
    OutputCollector,
    PipeTransport,
    ProducerState,
    RpcConnection,
    RpcError,
    RpcMethodInfo,
    RpcServer,
    RpcTransport,
    ServeStartHook,
    ShmPipeTransport,
    StderrMode,
    Stream,
    StreamSession,
    StreamState,
    SubprocessTransport,
    TransportKind,
    VersionError,
    connect,
    describe_rpc,
    make_pipe_pair,
    rpc_methods,
    run_server,
    serve_pipe,
    serve_stdio,
)
from vgi_rpc.utils import (
    ArrowSerializableDataclass,
    ArrowType,
    IPCError,
    IpcValidation,
    Transient,
    ValidatedReader,
    validate_batch,
)

# Optional integrations are loaded on first attribute access via __getattr__.
# Submodules listed here pull in heavy deps (Falcon, httpx, sentry-sdk, boto3,
# google-cloud-storage) that subprocess workers and CLI users don't need at
# import time.
_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    # vgi_rpc.http (Falcon + httpx)
    "MAX_REQUEST_BYTES_HEADER": ("vgi_rpc.http", "MAX_REQUEST_BYTES_HEADER"),
    "MAX_UPLOAD_BYTES_HEADER": ("vgi_rpc.http", "MAX_UPLOAD_BYTES_HEADER"),
    "UPLOAD_URL_HEADER": ("vgi_rpc.http", "UPLOAD_URL_HEADER"),
    "HttpRetryConfig": ("vgi_rpc.http", "HttpRetryConfig"),
    "HttpServerCapabilities": ("vgi_rpc.http", "HttpServerCapabilities"),
    "HttpStreamSession": ("vgi_rpc.http", "HttpStreamSession"),
    "HttpTransientError": ("vgi_rpc.http", "HttpTransientError"),
    "OAuthResourceMetadata": ("vgi_rpc.http", "OAuthResourceMetadata"),
    "OAuthResourceMetadataResponse": ("vgi_rpc.http", "OAuthResourceMetadataResponse"),
    "fetch_oauth_metadata": ("vgi_rpc.http", "fetch_oauth_metadata"),
    "http_capabilities": ("vgi_rpc.http", "http_capabilities"),
    "http_connect": ("vgi_rpc.http", "http_connect"),
    "http_introspect": ("vgi_rpc.http", "http_introspect"),
    "http_oauth_metadata": ("vgi_rpc.http", "http_oauth_metadata"),
    "make_sync_client": ("vgi_rpc.http", "make_sync_client"),
    "make_wsgi_app": ("vgi_rpc.http", "make_wsgi_app"),
    "parse_resource_metadata_url": ("vgi_rpc.http", "parse_resource_metadata_url"),
    "request_upload_urls": ("vgi_rpc.http", "request_upload_urls"),
    # vgi_rpc.http._oauth_jwt
    "jwt_authenticate": ("vgi_rpc.http._oauth_jwt", "jwt_authenticate"),
    # vgi_rpc.otel
    "OtelConfig": ("vgi_rpc.otel", "OtelConfig"),
    "instrument_server": ("vgi_rpc.otel", "instrument_server"),
    # vgi_rpc.sentry
    "SentryConfig": ("vgi_rpc.sentry", "SentryConfig"),
    "instrument_server_sentry": ("vgi_rpc.sentry", "instrument_server_sentry"),
    # vgi_rpc.s3
    "S3Storage": ("vgi_rpc.s3", "S3Storage"),
    # vgi_rpc.gcs
    "GCSStorage": ("vgi_rpc.gcs", "GCSStorage"),
}


def __getattr__(name: str) -> Any:
    """Resolve optional-integration attributes on first access."""
    target = _LAZY_ATTRS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise AttributeError(
            f"module {__name__!r} attribute {name!r} requires the optional dependency "
            f"that provides {module_name!r}: {exc}"
        ) from exc
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def _optional_module_available(module_name: str) -> bool:
    """Return True if *module_name* is importable without actually importing it.

    For our own ``vgi_rpc.<sub>`` packages, ``find_spec`` would import the
    parent ``vgi_rpc.http`` to locate a submodule, which defeats the lazy
    point. Caller passes a *leaf* third-party dep name (``falcon``,
    ``sentry_sdk``, ...) instead.
    """
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False


__all__ = [
    # Core
    "RpcServer",
    "RpcConnection",
    "RpcTransport",
    "RpcError",
    "RpcMethodInfo",
    "VersionError",
    "IPCError",
    # Convenience
    "run_server",
    "connect",
    "serve_pipe",
    # Transports
    "PipeTransport",
    "ShmPipeTransport",
    "SubprocessTransport",
    "StderrMode",
    "TransportKind",
    "make_pipe_pair",
    "serve_stdio",
    # Streaming
    "Stream",
    "StreamState",
    "ProducerState",
    "ExchangeState",
    "StreamSession",
    "OutputCollector",
    "AnnotatedBatch",
    # Introspection
    "DESCRIBE_METHOD_NAME",
    "MethodDescription",
    "ServiceDescription",
    "introspect",
    "rpc_methods",
    "describe_rpc",
    "MethodType",
    # Auth & Context
    "AuthContext",
    "CallContext",
    "CallStatistics",
    "ServeStartHook",
    # Logging
    "ClientLog",
    "Level",
    "Message",
    # Serialization
    "ArrowSerializableDataclass",
    "ArrowType",
    "Transient",
    # Validation
    "IpcValidation",
    "ValidatedReader",
    "validate_batch",
    # Protocol version
    "REQUEST_VERSION",
    # Pool
    "PoolMetrics",
    "WorkerPool",
    # ExternalLocation
    "ClientExternalConfig",
    "Compression",
    "ExternalLocationConfig",  # alias for ServerExternalConfig
    "ExternalStorage",
    "FetchConfig",
    "ServerExternalConfig",
    "https_only_validator",
    # Upload URLs
    "UploadUrl",
    "UploadUrlProvider",
]

# Optional names — included in __all__ only when the underlying third-party
# dep is importable, so `from vgi_rpc import *` doesn't trigger AttributeError
# for extras the user didn't install. We probe the *leaf* dep (falcon,
# sentry_sdk, ...) rather than `vgi_rpc.http`, because find_spec on a
# vgi_rpc submodule would import its parent and defeat lazy loading.
if _optional_module_available("falcon"):
    __all__ += [
        "HttpRetryConfig",
        "HttpServerCapabilities",
        "HttpStreamSession",
        "HttpTransientError",
        "MAX_REQUEST_BYTES_HEADER",
        "MAX_UPLOAD_BYTES_HEADER",
        "OAuthResourceMetadata",
        "OAuthResourceMetadataResponse",
        "UPLOAD_URL_HEADER",
        "fetch_oauth_metadata",
        "http_capabilities",
        "http_connect",
        "http_introspect",
        "http_oauth_metadata",
        "make_sync_client",
        "make_wsgi_app",
        "parse_resource_metadata_url",
        "request_upload_urls",
    ]
    if _optional_module_available("joserfc"):
        __all__.append("jwt_authenticate")
if _optional_module_available("opentelemetry"):
    __all__ += ["OtelConfig", "instrument_server"]
if _optional_module_available("sentry_sdk"):
    __all__ += ["SentryConfig", "instrument_server_sentry"]
if _optional_module_available("boto3"):
    __all__.append("S3Storage")
if _optional_module_available("google.cloud.storage"):
    __all__.append("GCSStorage")

# Attach NullHandler to the root logger so library users don't get
# "No handler found" warnings.
logging.getLogger("vgi_rpc").addHandler(logging.NullHandler())
