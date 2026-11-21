"""Shared fixtures for the ``loom.core.stores.sql`` unit tests."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.core.stores.sql.config import SqlConnectionConfig
from loom.core.stores.sql.service import SqlQueryService
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_sql_config

ServiceFactory = Callable[[FakeSqlExecutor, SqlConnectionConfig], SqlQueryService]


@pytest.fixture
def fake_executor() -> FakeSqlExecutor:
    """Fresh call-capturing fake executor per test."""
    return FakeSqlExecutor()


@pytest.fixture
def make_service() -> ServiceFactory:
    """Factory building a ``SqlQueryService`` over a fake 'analytics' connection."""

    def _make(executor: FakeSqlExecutor, connection: SqlConnectionConfig) -> SqlQueryService:
        return SqlQueryService(
            executors={"analytics": executor},
            config=make_sql_config(analytics=connection),
        )

    return _make
