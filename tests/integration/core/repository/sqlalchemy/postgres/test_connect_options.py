"""A store's ``connect_args`` reach asyncpg through the runtime engine.

The migrations environment dials with the same arguments; its test lives with
the other migration runs, in ``tests/integration/rls/test_migrations.py``.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from loom.core.model.redaction import Dsn, RedactedMap
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.database import DatabaseConfig
from loom.core.stores.profile import SERVER_PROFILE
from loom.stores.sqlalchemy.session_manager import build_session_manager

pytestmark = pytest.mark.integration

_APPLICATION_NAME = "loom-runtime-engine"


class TestConnectOptions:
    async def test_server_settings_reach_the_runtime_engine(self, pg_admin_uri: str) -> None:
        config = DatabaseConfig(
            url=Dsn(pg_admin_uri),
            connect_args=RedactedMap({"server_settings": {"application_name": _APPLICATION_NAME}}),
        )
        manager = build_session_manager(config, SERVER_PROFILE, database=DEFAULT_STORE)
        try:
            async with manager.engine.connect() as connection:
                name = await connection.scalar(text("SHOW application_name"))
        finally:
            await manager.close()

        assert name == _APPLICATION_NAME
