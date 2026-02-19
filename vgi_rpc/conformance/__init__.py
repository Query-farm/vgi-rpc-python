"""Conformance test server for vgi-rpc wire protocol validation.

Provides a comprehensive RPC service exercising all framework capabilities.
Other language implementations can validate against this reference server.

Usage::

    from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
    from vgi_rpc.rpc import run_server

    run_server(ConformanceService, ConformanceServiceImpl())

"""

from vgi_rpc.conformance._impl import ConformanceServiceImpl
from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.conformance._types import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    Point,
    Status,
)

__all__ = [
    "AllTypes",
    "BoundingBox",
    "ConformanceHeader",
    "ConformanceService",
    "ConformanceServiceImpl",
    "Point",
    "Status",
]
