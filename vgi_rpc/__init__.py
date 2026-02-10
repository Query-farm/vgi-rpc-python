"""Transport-agnostic RPC framework built on Apache Arrow IPC serialization."""

import contextlib

from vgi_rpc.external import Compression, ExternalLocationConfig, ExternalStorage
from vgi_rpc.external_fetch import FetchConfig
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import REQUEST_VERSION
from vgi_rpc.rpc import (
    AnnotatedBatch,
    BidiSession,
    BidiStream,
    BidiStreamState,
    EmitLog,
    MethodType,
    OutputCollector,
    PipeTransport,
    RpcConnection,
    RpcError,
    RpcMethodInfo,
    RpcServer,
    RpcTransport,
    ServerStream,
    ServerStreamState,
    StreamSession,
    SubprocessTransport,
    connect,
    describe_rpc,
    make_pipe_pair,
    rpc_methods,
    run_server,
    serve_pipe,
    serve_stdio,
)
from vgi_rpc.utils import ArrowSerializableDataclass, ArrowType

# HTTP (optional — requires `pip install vgi-rpc[http]`)
with contextlib.suppress(ImportError):
    from vgi_rpc.http import HttpBidiSession, HttpStreamSession, http_connect, make_sync_client, make_wsgi_app

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
    # Convenience
    "run_server",
    "connect",
    "serve_pipe",
    # Transports
    "PipeTransport",
    "SubprocessTransport",
    "make_pipe_pair",
    "serve_stdio",
    # Streaming
    "ServerStream",
    "ServerStreamState",
    "BidiStream",
    "BidiStreamState",
    "StreamSession",
    "BidiSession",
    "OutputCollector",
    "AnnotatedBatch",
    # Introspection
    "rpc_methods",
    "describe_rpc",
    "MethodType",
    # Logging
    "EmitLog",
    "Level",
    "Message",
    # Serialization
    "ArrowSerializableDataclass",
    "ArrowType",
    # Protocol version
    "REQUEST_VERSION",
    # ExternalLocation
    "Compression",
    "ExternalLocationConfig",
    "ExternalStorage",
    "FetchConfig",
]

# Conditionally include optional backend names only when actually imported
if "S3Storage" in dir():
    __all__.append("S3Storage")
if "GCSStorage" in dir():
    __all__.append("GCSStorage")
if "HttpBidiSession" in dir():
    __all__ += ["HttpBidiSession", "HttpStreamSession", "http_connect", "make_wsgi_app", "make_sync_client"]
