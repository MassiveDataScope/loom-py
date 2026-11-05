"""Two applications composed in one process keep their own scope sources (US7 PR 8).

Each application binds its ``request.<name>`` sources when it is composed, so
registering or clearing a source for a second application never changes what
the first one's sessions set.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import text

from loom.composition import (
    AppConfig,
    Application,
    CompositionSource,
    Recipe,
    compose_application,
)
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.sqlalchemy.rls import register_scope_source
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.helpers.composition import composition_source

pytestmark = pytest.mark.integration

DESK = text("SELECT coalesce(current_setting('loom.scope.desk', true), '')")


class _Ticket(BaseModel, RowScoped):
    __tablename__ = "two_applications_tickets"

    desk: str = ScopedField(String(36), primary_key=True, scope="desk")
    id: int = ColumnField(Integer, primary_key=True)


class _Tickets:
    """Composes the ticket model on the SQLAlchemy backend."""

    def recipe(self, source: CompositionSource) -> Recipe:
        del source
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(models=(_Ticket,), use_cases=(), interfaces=()),
            default_backend="sqlalchemy",
        )

    def bind(self, application: Application) -> Application:
        return application


@pytest.fixture(autouse=True)
def _no_sources() -> Iterator[None]:
    clear_scope_sources()
    yield
    clear_scope_sources()


def _compose(url: str, name: str) -> Application:
    ctx = ConfigContext.from_dict(
        {
            "stores": {
                "default": {
                    "backend": "sqlalchemy",
                    "url": url,
                    "schema": {"mode": "external", "scopes": {"desk": "request.desk"}},
                }
            },
            "persistence": {"cursors": {"signing": "ephemeral"}},
        }
    )
    return compose_application(composition_source(ctx, app=AppConfig(name=name)), _Tickets())


async def _desk(application: Application) -> str:
    """The ``desk`` value a session of *application* sets on its connection."""
    manager = application.kernel.container.resolve(SessionManager)
    try:
        async with manager.session() as session:
            return str(await session.scalar(DESK))
    finally:
        await manager.dispose()


async def test_each_application_keeps_the_sources_registered_when_it_was_composed(
    module_database_uri: str,
) -> None:
    register_scope_source("desk", lambda: "north")
    first = _compose(module_database_uri, "first")
    clear_scope_sources()
    register_scope_source("desk", lambda: "south")
    second = _compose(module_database_uri, "second")

    assert [await _desk(first), await _desk(second)] == ["north", "south"]
