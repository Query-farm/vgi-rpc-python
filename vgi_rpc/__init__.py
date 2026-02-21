# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport-agnostic RPC framework built on Apache Arrow IPC serialization."""

import contextlib
import logging

from vgi_rpc.external import (
    Compression,
    ExternalLocationConfig,
    ExternalStorage,
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
    ShmPipeTransport,
    StderrMode,
    Stream,
    StreamSession,
    StreamState,
    SubprocessTransport,
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
    ValidatedReader,
    validate_batch,
)

# HTTP (optional — requires `pip install vgi-rpc[http]`)
with contextlib.suppress(ImportError):
    from vgi_rpc.http import (
        MAX_REQUEST_BYTES_HEADER,
        MAX_UPLOAD_BYTES_HEADER,
        UPLOAD_URL_HEADER,
        HttpRetryConfig,
        HttpServerCapabilities,
        HttpStreamSession,
        HttpTransientError,
        http_capabilities,
        http_connect,
        http_introspect,
        make_sync_client,
        make_wsgi_app,
        request_upload_urls,
    )

# OpenTelemetry instrumentation (optional — requires `pip install vgi-rpc[otel]`)
with contextlib.suppress(ImportError):
    from vgi_rpc.otel import OtelConfig, instrument_server

# S3 storage backend (optional — requires `pip install vgi-rpc[s3]`)
with contextlib.suppress(ImportError):
    from vgi_rpc.s3 import S3Storage  # noqa: F401

# GCS storage backend (optional — requires `pip install vgi-rpc[gcs]`)
with contextlib.suppress(ImportError):
    from vgi_rpc.gcs import GCSStorage  # noqa: F401

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
    # Logging
    "ClientLog",
    "Level",
    "Message",
    # Serialization
    "ArrowSerializableDataclass",
    "ArrowType",
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
    "Compression",
    "ExternalLocationConfig",
    "ExternalStorage",
    "FetchConfig",
    "https_only_validator",
    # Upload URLs
    "UploadUrl",
    "UploadUrlProvider",
]

# Conditionally include optional names only when actually imported
if "OtelConfig" in dir():
    __all__ += ["OtelConfig", "instrument_server"]
if "S3Storage" in dir():
    __all__.append("S3Storage")
if "GCSStorage" in dir():
    __all__.append("GCSStorage")
if "HttpStreamSession" in dir():
    __all__ += [
        "HttpRetryConfig",
        "HttpServerCapabilities",
        "HttpStreamSession",
        "HttpTransientError",
        "MAX_REQUEST_BYTES_HEADER",
        "MAX_UPLOAD_BYTES_HEADER",
        "UPLOAD_URL_HEADER",
        "http_capabilities",
        "http_connect",
        "http_introspect",
        "make_wsgi_app",
        "make_sync_client",
        "request_upload_urls",
    ]

# Attach NullHandler to the root logger so library users don't get
# "No handler found" warnings.  Must come after all imports so the
# logger hierarchy is fully populated.
logging.getLogger("vgi_rpc").addHandler(logging.NullHandler())
