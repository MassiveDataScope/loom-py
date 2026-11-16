"""Store ports: the backend contract, its registry, the catalog and the ``none`` backend.

This package imports no concrete backend; SQLAlchemy, DynamoDB and the rest
live under ``loom.stores`` and are reached only through their entry
points.
"""

from loom.core.stores.backend import PersistenceBackend, PersistenceWiring
from loom.core.stores.database import (
    DatabaseConfig,
    PoolConfig,
    SchemaConfig,
    decode_database_config,
    load_database_config,
)
from loom.core.stores.none import NoneBackend
from loom.core.stores.profile import (
    LAMBDA_PROFILE,
    SERVER_PROFILE,
    STREAMING_PROFILE,
    WORKER_PROFILE,
    BackendRuntime,
    PoolDefaults,
    PoolMetricsSink,
    RuntimeProfile,
)
from loom.core.stores.registry import BACKEND_ENTRY_POINT_GROUP, resolve_backend

__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "LAMBDA_PROFILE",
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
