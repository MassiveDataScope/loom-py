"""``SQLAlchemyBackend`` serves the store the host hands it, never ``stores.default`` by fiat.

ADR 0008: each store has its own section, metadata and metrics label, and
every message about its schema names its own section.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer
from loom.core.stores import StoreWiring
from loom.core.stores.catalog import DEFAULT_STORE, StoreKey, load_catalog
from loom.core.stores.profile import SERVER_PROFILE, StoreRuntime
from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.plugin import SQLAlchemyBackend

_AUDIT = StoreKey("audit")
_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


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
        {
            "stores": {**stores, "settings": {"cursors": {"signing": "ephemeral"}}},
        }
    )


def _build(
    ctx: ConfigContext,
    store: StoreKey,
    *models: type[BaseModel],
    pool_metrics: _Labels | None = None,
) -> StoreWiring:
    runtime = StoreRuntime(
        SERVER_PROFILE,
        pool_metrics=pool_metrics,
        catalog=load_catalog(ctx),
        store=store,
        cursor_codec=CursorCodec.from_context(ctx),
        aws=AwsClients(),
    )
    return SQLAlchemyBackend().build(ctx, models, runtime)


class TestBackendStores:
    def test_it_reads_the_section_of_the_store_it_serves(self) -> None:
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

    def test_its_pool_is_measured_under_the_store_key(self) -> None:
        labels = _Labels()
        ctx = _context(
            default={"backend": "sqlalchemy", "url": _POSTGRES},
            audit={"backend": "sqlalchemy", "url": _POSTGRES},
        )

        _build(ctx, DEFAULT_STORE, pool_metrics=labels)
        _build(ctx, _AUDIT, pool_metrics=labels)

        assert labels.labels == ["default", "audit"]

    async def test_startup_checks_name_the_section_of_the_store_they_check(self) -> None:
        ctx = _context(audit={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"})
        wiring = _build(ctx, _AUDIT, _Entry)
        wiring.prepare_models((_Entry,))

        with pytest.raises(
            ConfigError, match=r"set stores\.audit\.schema\.allow_unprotected_dialect"
        ):
            await wiring.startup_checks()

    def test_row_level_security_bindings_are_named_under_the_store_they_belong_to(self) -> None:
        schema = {"mode": "external", "scopes": {"tenant": "identity.subject"}}
        ctx = _context(audit={"backend": "sqlalchemy", "url": _POSTGRES, "schema": schema})
        wiring = _build(ctx, _AUDIT, _Entry)

        with pytest.raises(ConfigError, match=r"stores\.audit\.schema\.scopes is missing .*holder"):
            wiring.prepare_models((_Entry,))

    def test_every_store_of_an_application_registers_the_one_codec_the_host_built(self) -> None:
        """Two codecs in one container would each refuse the other's cursors: the second is
        an error.
        """
        ctx = _context(
            default={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
            audit={"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
        )
        codec = CursorCodec.from_context(ctx)
        container = LoomContainer()
        container.register_instance(Environment, Environment.TEST)

        for store in (DEFAULT_STORE, _AUDIT):
            runtime = StoreRuntime(
                SERVER_PROFILE,
                catalog=load_catalog(ctx),
                store=store,
                cursor_codec=codec,
                aws=AwsClients(),
            )
            SQLAlchemyBackend().build(ctx, (), runtime).repo_registration_module(container)

        assert container.resolve(CursorCodec) is codec
