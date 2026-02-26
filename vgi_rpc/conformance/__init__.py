# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

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
from vgi_rpc.conformance._runner import (
    DEFAULT_TEST_TIMEOUT,
    ConformanceResult,
    ConformanceSuite,
    LogCollector,
    list_conformance_tests,
    list_describe_conformance_tests,
    run_conformance,
    run_describe_conformance,
)
from vgi_rpc.conformance._types import (
    AllTypes,
    BoundingBox,
    ConformanceHeader,
    Point,
    RichHeader,
    Status,
    build_dynamic_schema,
    build_rich_header,
)

__all__ = [
    "AllTypes",
    "BoundingBox",
    "ConformanceHeader",
    "ConformanceResult",
    "ConformanceService",
    "ConformanceServiceImpl",
    "ConformanceSuite",
    "DEFAULT_TEST_TIMEOUT",
    "LogCollector",
    "Point",
    "RichHeader",
    "Status",
    "build_dynamic_schema",
    "build_rich_header",
    "list_conformance_tests",
    "list_describe_conformance_tests",
    "run_conformance",
    "run_describe_conformance",
]
