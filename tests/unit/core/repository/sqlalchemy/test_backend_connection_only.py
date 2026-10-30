"""``SQLAlchemyBackend`` built with ``BackendRuntime(connection_only=True)``.

A host with no model (a worker running raw-SQL jobs) gets a unit of work and
nothing more: no cursor codec, no repository, and the compiled registry left
as the caller compiled it.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled, reset_registry
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import WORKER_PROFILE, BackendRuntime
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.session_manager import SessionManager

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


class _CallerCompiled(BaseModel):
    __tablename__ = "caller_compiled_connection_only"

    id: int = ColumnField(primary_key=True, autoincrement=True)


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    reset_registry()
    yield
    reset_registry()


def _wiring(database: dict[str, Any]) -> PersistenceWiring:
    # No persistence.cursors at all: a codec built here would fail.
    ctx = ConfigContext.from_dict({"database": database})
    return SQLAlchemyBackend().build(ctx, (), BackendRuntime(WORKER_PROFILE, connection_only=True))


def _registered(wiring: PersistenceWiring) -> LoomContainer:
    container = LoomContainer()
    wiring.repo_registration_module(container)
    return container


def test_no_cursor_codec_is_built_or_registered() -> None:
    container = _registered(_wiring({"url": "sqlite+aiosqlite:///"}))

    assert not container.is_registered(CursorCodec)


def test_the_session_manager_and_the_unit_of_work_are_still_served() -> None:
    wiring = _wiring({"url": "sqlite+aiosqlite:///"})

    assert wiring.uow_factory is not None
    assert isinstance(_registered(wiring).resolve(SessionManager), SessionManager)


def test_preparing_leaves_the_compiled_registry_alone() -> None:
    compile_all(_CallerCompiled)

    _wiring({"url": "sqlite+aiosqlite:///"}).prepare_models(())

    assert get_compiled(_CallerCompiled) is not None


def test_row_level_security_stays_unbound_and_fails_closed() -> None:
    """Postgres with an external schema: no session may run without the scoped settings."""
    database = {"url": _POSTGRES, "schema": {"mode": "external", "scopes": {"owner": "x.y"}}}
    wiring = _wiring(database)
    wiring.prepare_models(())
    manager = _registered(wiring).resolve(SessionManager)

    assert manager.has_session_settings
    with pytest.raises(RuntimeError, match="before startup bound them"):
        manager.current_settings()
