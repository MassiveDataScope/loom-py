"""Loom REST layer — transport-agnostic declarations and adapters.

Provides the declarative REST interface model, route compiler, and HTTP
adapters.  These components are framework-agnostic and do not depend on
FastAPI directly.

For FastAPI-specific bindings (response class, route registration, app
factory) see :mod:`loom.rest.fastapi`.
"""

from loom.rest.adapter import PydanticAdapter
from loom.rest.checks import AUTHENTICATION_CHECK, SCHEMA_CHECK
from loom.rest.compiler import (
    CompiledRoute,
    InterfaceCompilationError,
    RestInterfaceCompiler,
    RouteSources,
)
from loom.rest.config import RestInterfaceConfigError
from loom.rest.errors import HttpErrorMapper
from loom.rest.middleware import TraceIdMiddleware
from loom.rest.model import (
    ALL,
    PaginationMode,
    RestApiDefaults,
    RestInterface,
    RestRoute,
)
from loom.rest.rest_adapter import LoomRestAdapter

__all__ = [
    "ALL",
    "AUTHENTICATION_CHECK",
    "SCHEMA_CHECK",
    "CompiledRoute",
    "HttpErrorMapper",
    "InterfaceCompilationError",
    "LoomRestAdapter",
    "PaginationMode",
    "PydanticAdapter",
    "RestApiDefaults",
    "RestInterface",
    "RestInterfaceCompiler",
    "RestInterfaceConfigError",
    "RestRoute",
    "RouteSources",
    "TraceIdMiddleware",
]
