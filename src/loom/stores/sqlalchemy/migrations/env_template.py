"""Alembic environment for row-scoped schemas; copy it as ``env.py`` into each tree.

The same file serves ``alembic/`` (structural tree) and ``alembic/data`` (data
tree): ``alembic_config`` records which tree a script location is. The
application comes from ``config.attributes["application"]`` or, failing that,
from the default config directory (``LOOM_CONFIG_DIR`` or ``./config``) through
``load_store_schema``. The engine dials the URL ``alembic_config`` recorded with
the store section's ``connect()`` arguments (``migration_dial``), as the runtime
engine does. Offline mode is refused because the guard needs a connection.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.pool import NullPool

from loom.stores.sqlalchemy.alembic import load_store_schema
from loom.stores.sqlalchemy.migrations.runners import (
    DATA_TREE,
    migration_dial,
    run_data_migrations,
    run_migrations,
)

config = context.config
application = config.attributes.get("application") or load_store_schema(
    config.attributes.get("config_path")
)
runner = run_data_migrations if config.attributes.get("tree") == DATA_TREE else run_migrations


async def _run() -> None:
    engine = migration_dial(config, application).create_async_engine(poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(runner, application)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    raise SystemExit("offline mode is not supported: the guard needs a live connection")

asyncio.run(_run())
