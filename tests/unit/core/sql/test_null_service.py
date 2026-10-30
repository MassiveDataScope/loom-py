"""M5 null service tests: no ``sql`` section never yields an opaque 500 (spec §3/§7.6)."""

from __future__ import annotations

import pytest

from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.sql.service import NullSqlQueryService, SqlQueryService


async def test_null_service_raises_actionable_config_error_on_first_use() -> None:
    """The null service fails with a ConfigError that mentions the missing ``sql`` section."""
    service = NullSqlQueryService()
    with pytest.raises(ConfigError, match="sql"):
        await service.execute("SELECT 1", connection="analytics")


def test_container_resolves_the_null_service_without_resolution_error() -> None:
    """Registered always (``configure_job_service`` pattern), resolving never fails."""
    container = LoomContainer()
    container.register(SqlQueryService, NullSqlQueryService, scope=Scope.APPLICATION)
    assert isinstance(container.resolve(SqlQueryService), NullSqlQueryService)


async def test_first_use_from_the_container_raises_config_error_not_resolution_error() -> None:
    """A use case resolving and using the service gets a ConfigError, not an opaque 500."""
    container = LoomContainer()
    container.register(SqlQueryService, NullSqlQueryService, scope=Scope.APPLICATION)
    service = container.resolve(SqlQueryService)
    with pytest.raises(ConfigError):
        await service.execute("SELECT 1", connection="analytics")
