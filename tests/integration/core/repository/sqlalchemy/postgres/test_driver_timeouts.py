"""``database.read_timeout`` reaches asyncpg: a statement running longer fails at the bound.

The section's standard timeouts become asyncpg's ``command_timeout`` and
``timeout`` through ``DatabaseConfig.client_options()``; a ``pg_sleep`` longer
than the bound must fail close to it instead of running to its end.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import text

from loom.core.persistence.database import DatabaseConfig
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.sqlalchemy.session_manager import build_session_manager

_READ_TIMEOUT = 1
_SLEEP_SECONDS = 4


async def test_a_statement_longer_than_read_timeout_fails_at_the_bound(pg_admin_uri: str) -> None:
    config = DatabaseConfig(url=pg_admin_uri, connect_timeout=5, read_timeout=_READ_TIMEOUT)
    manager = build_session_manager(config, SERVER_PROFILE)
    started = time.perf_counter()
    try:
        with pytest.raises(Exception) as raised:
            async with manager.engine.connect() as connection:
                await connection.execute(text("SELECT pg_sleep(:s)"), {"s": _SLEEP_SECONDS})
    finally:
        await manager.dispose()
    elapsed = time.perf_counter() - started

    assert "timeout" in f"{type(raised.value).__name__} {raised.value}".lower()
    assert elapsed < _SLEEP_SECONDS - 1
