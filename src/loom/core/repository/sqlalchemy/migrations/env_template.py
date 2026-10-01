"""Alembic environment for row-scoped schemas; copy it as ``env.py`` into each tree.

The same file serves ``alembic/`` (structural tree) and ``alembic/data`` (data
tree): ``alembic_config`` records which tree a script location is. The
application comes from ``config.attributes["application"]`` or, failing that,
from ``LOOM_CONFIG`` through the locator. Offline mode is refused because the
guard needs a connection.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import load_application
from loom.core.repository.sqlalchemy.migrations.runners import (
    DATA_TREE,
    run_data_migrations,
    run_migrations,
)

config = context.config
application = config.attributes.get("application") or load_application(
    config.attributes.get("config_path")
)
runner = run_data_migrations if config.attributes.get("tree") == DATA_TREE else run_migrations


async def _run() -> None:
    url = str(config.get_main_option("sqlalchemy.url")).replace("%%", "%")
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(runner, application)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    raise SystemExit("offline mode is not supported: the guard needs a live connection")

asyncio.run(_run())
