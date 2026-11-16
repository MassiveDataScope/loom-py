"""Store ports: backends, their registry, database configuration and runtime profiles.

The ``none`` backend is exported here too. The store catalog and its specs
live in ``loom.core.stores.catalog``.

This package imports no concrete backend; SQLAlchemy, DynamoDB and the rest
live under ``loom.stores`` and are reached only through their entry
points.
"""

from loom.core.stores.backend import StoreBackend, StoreWiring
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
    PoolDefaults,
    PoolMetricsSink,
    RuntimeProfile,
    StoreRuntime,
)
from loom.core.stores.registry import BACKEND_ENTRY_POINT_GROUP, resolve_store_backend

__all__ = [
    "BACKEND_ENTRY_POINT_GROUP",
    "LAMBDA_PROFILE",
    "SERVER_PROFILE",
    "STREAMING_PROFILE",
    "WORKER_PROFILE",
    "StoreRuntime",
    "DatabaseConfig",
    "NoneBackend",
    "StoreBackend",
    "StoreWiring",
    "PoolConfig",
    "PoolDefaults",
    "PoolMetricsSink",
    "RuntimeProfile",
    "SchemaConfig",
    "decode_database_config",
    "load_database_config",
    "resolve_store_backend",
]
