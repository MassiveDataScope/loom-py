"""Relational connection settings: the one ``database`` contract, re-exported.

API, worker, streaming sinks, migrations and the row-level-security tooling
all read :class:`~loom.core.persistence.database.DatabaseConfig`; it is
defined there, next to the pool defaults it resolves against, and only named
here so every engine is found in :mod:`loom.core.connections`. Its client
builder is :func:`loom.core.repository.sqlalchemy.session_manager.build_session_manager`,
the SQLAlchemy island.
"""

from __future__ import annotations

from loom.core.persistence.database import (
    DatabaseConfig,
    PoolConfig,
    SchemaConfig,
    decode_database_config,
    load_database_config,
)

__all__ = [
    "DatabaseConfig",
    "PoolConfig",
    "SchemaConfig",
    "decode_database_config",
    "load_database_config",
]
