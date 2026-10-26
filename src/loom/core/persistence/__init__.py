"""Persistence backend plugins: contract, registry and the ``none`` backend.

This package imports no concrete backend; SQLAlchemy, DynamoDB and the rest
live under ``loom.core.repository`` and are reached only through their entry
points.
"""

from loom.core.persistence.abc import PersistenceBackend, PersistenceWiring
from loom.core.persistence.database import (
    DatabaseConfig,
    PoolConfig,
    SchemaConfig,
    decode_database_config,
    load_database_config,
)
from loom.core.persistence.none import NoneBackend
from loom.core.persistence.profile import (
    SERVER_PROFILE,
    STREAMING_PROFILE,
    WORKER_PROFILE,
    BackendRuntime,
    PoolDefaults,
    PoolMetricsSink,
    RuntimeProfile,
)
from loom.core.persistence.registry import BACKEND_ENTRY_POINT_GROUP, resolve_backend

__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "SERVER_PROFILE",
    "STREAMING_PROFILE",
    "WORKER_PROFILE",
    "BackendRuntime",
    "DatabaseConfig",
    "NoneBackend",
    "PersistenceBackend",
    "PersistenceWiring",
    "PoolConfig",
    "PoolDefaults",
    "PoolMetricsSink",
    "RuntimeProfile",
    "SchemaConfig",
    "decode_database_config",
    "load_database_config",
    "resolve_backend",
]
