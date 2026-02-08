"""Transport-agnostic RPC framework built on Apache Arrow IPC serialization."""

import contextlib

from vgi_rpc.log import Level, Message
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
    from vgi_rpc.http import HttpBidiSession, http_connect, make_asgi_app, make_sync_client

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
    # HTTP (optional)
    "HttpBidiSession",
    "http_connect",
    "make_asgi_app",
    "make_sync_client",
]
