"""``SQLAlchemyBackend`` serves the store the host hands it, never ``stores.default`` by fiat.

ADR 0008: each store has its own section, metadata, metrics label and DI
keys. The default store keeps the bare ``SessionManager`` and
``ElevationSink`` keys every consumer resolves; another store registers
under :func:`~loom.core.persistence.stores.store_binding`, so building it
next to the default store never replaces the default store's bindings.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.authz.elevation import ElevationSink
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey, load_catalog, store_binding
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.rls.elevate import SQLAlchemyElevationSink
from loom.core.repository.sqlalchemy.session_manager import SessionManager

_AUDIT = StoreKey("audit")
_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"
_EXTERNAL = {"mode": "external", "scopes": {"holder": "identity.subject"}}


class _Entry(BaseModel, RowScoped):
    __tablename__ = "store_audit_entries"

    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


class _Labels:
    """A pool-metrics sink recording the label each engine is reported under."""

    def __init__(self) -> None:
        self.labels: list[str] = []

    def register_engine(self, engine: Any, database: str) -> None:
        self.labels.append(database)

    def observe_checkout(self, database: str, seconds: float) -> None: ...

    def count_checkout_timeout(self, database: str) -> None: ...


def _context(**stores: dict[str, Any]) -> ConfigContext:
    return ConfigContext.from_dict(
        {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )


def _build(
    ctx: ConfigContext,
    store: StoreKey,
    *models: type[BaseModel],
    pool_metrics: _Labels | None = None,
    connection_only: bool = False,
) -> PersistenceWiring:
    runtime = BackendRuntime(
        SERVER_PROFILE,
        pool_metrics=pool_metrics,
        connection_only=connection_only,
        catalog=load_catalog(ctx),
        store=store,
    )
    return SQLAlchemyBackend().build(ctx, models, runtime)


def _registered(*wirings: PersistenceWiring) -> LoomContainer:
    container = LoomContainer()
    for wiring in wirings:
        wiring.repo_registration_module(container)
    return container


def test_it_reads_the_section_of_the_store_it_serves() -> None:
    ctx = _context(
        default={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
        audit={
            "backend": "sqlalchemy",
            "url": "sqlite+aiosqlite:///",
            "schema": {"naming_convention": {"primary": "pk_%(table_name)s"}},
        },
    )

    _build(ctx, DEFAULT_STORE)
    with pytest.raises(ConfigError, match=r"stores\.audit\.schema\.naming_convention"):
        _build(ctx, _AUDIT)


def test_a_store_that_is_not_declared_is_named() -> None:
    ctx = _context(default={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"})

    with pytest.raises(ConfigError, match=r"needs stores\.audit, which is not declared"):
        _build(ctx, _AUDIT)


def test_its_pool_is_measured_under_the_store_key() -> None:
    labels = _Labels()
    ctx = _context(
        default={"backend": "sqlalchemy", "url": _POSTGRES},
        audit={"backend": "sqlalchemy", "url": _POSTGRES},
    )

    _build(ctx, DEFAULT_STORE, pool_metrics=labels)
    _build(ctx, _AUDIT, pool_metrics=labels)

    assert labels.labels == ["default", "audit"]


def test_another_store_registers_its_own_keys_and_leaves_the_default_ones_alone() -> None:
    ctx = _context(
        default={"backend": "sqlalchemy", "url": _POSTGRES},
        audit={"backend": "sqlalchemy", "url": _POSTGRES, "schema": _EXTERNAL},
    )
    audit = _build(ctx, _AUDIT, _Entry)
    # Connection-only: two stores serving repositories would each register a
    # cursor codec, and the process has one (the invocation scope's delivery).
    default = _build(ctx, DEFAULT_STORE, connection_only=True)

    alone = _registered(audit)
    together = _registered(default, audit)

    assert not alone.is_registered(SessionManager)
    assert not alone.is_registered(ElevationSink)
    assert isinstance(alone.resolve(store_binding(ElevationSink, _AUDIT)), SQLAlchemyElevationSink)
    audit_manager = together.resolve(store_binding(SessionManager, _AUDIT))
    assert audit_manager is alone.resolve(store_binding(SessionManager, _AUDIT))
    assert together.resolve(SessionManager) is not audit_manager


async def test_startup_checks_name_the_section_of_the_store_they_check() -> None:
    ctx = _context(audit={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"})
    wiring = _build(ctx, _AUDIT, _Entry)
    wiring.prepare_models((_Entry,))

    with pytest.raises(ConfigError, match=r"set stores\.audit\.schema\.allow_unprotected_dialect"):
        await wiring.startup_checks()
