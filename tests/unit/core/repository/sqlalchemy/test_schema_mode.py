from __future__ import annotations

import logging
from datetime import time

import msgspec
import pytest
from sqlalchemy import MetaData

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.kernel import create_kernel
from loom.core.config import ConfigContext, ConfigError
from loom.core.config.convert import convert_config
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField, ColumnType, RowScoped, ScopedField
from loom.core.model.types import Integer, Text
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.database import DatabaseConfig, SchemaConfig
from loom.core.stores.profile import SERVER_PROFILE
from loom.stores.sqlalchemy.event import SqlalchemyEvent, SqlalchemyKey
from loom.stores.sqlalchemy.plugin import (
    SQLAlchemyStorePlugin,
    check_time_zone_columns,
    startup_checks,
)
from loom.stores.sqlalchemy.rls import SQLAlchemyStoreElevation
from loom.stores.sqlalchemy.schema import compile_all, scoped_tables
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.helpers.elevation import sink_for
from tests.helpers.persistence import build_backend


class Plain(BaseModel):
    __tablename__ = "plain_items"
    id: int = ColumnField(Integer, primary_key=True)
    name: str = ColumnField(Text)


class Scoped(BaseModel, RowScoped):
    __tablename__ = "scoped_items"
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


class Zoned(BaseModel):
    __tablename__ = "zoned_items"
    id: int = ColumnField(Integer, primary_key=True)
    closes_at: time = ColumnField(ColumnType("Time", kwargs=(("timezone", True),)))


def _zoned() -> MetaData:
    metadata = MetaData()
    compile_all(Zoned, metadata=metadata)
    return metadata


def _scoped() -> dict:
    metadata = MetaData()
    compile_all(Scoped, metadata=metadata)
    return dict(scoped_tables(metadata))


def _wiring(url: str, *models: type, **schema: object):
    config = {
        "app": {"name": "demo"},
        "stores": {
            "default": {"backend": "sqlalchemy", "url": url, "schema": schema},
            "settings": {"cursors": {"signing": "ephemeral"}},
        },
    }
    return build_backend(
        SQLAlchemyStorePlugin(), ConfigContext.from_dict(config), models, SERVER_PROFILE
    )


class TestSchemaMode:
    def test_schema_config_defaults_and_is_frozen(self) -> None:
        config = convert_config({"url": "sqlite+aiosqlite://"}, DatabaseConfig, strict=True)

        assert config.schema.mode == "create_all"
        assert config.schema.allow_unprotected_dialect is False
        with pytest.raises(AttributeError):
            config.schema.mode = "external"  # type: ignore[misc]

    def test_schema_config_reads_mode_and_the_dialect_opt_out(self) -> None:
        raw = {
            "url": "sqlite+aiosqlite://",
            "schema": {"mode": "external", "allow_unprotected_dialect": True},
        }

        config = convert_config(raw, DatabaseConfig, strict=True)

        assert config.schema == SchemaConfig(mode="external", allow_unprotected_dialect=True)

    def test_an_unknown_mode_is_rejected(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_config({"url": "x", "schema": {"mode": "drop"}}, DatabaseConfig, strict=True)

    def test_has_session_settings_is_false_without_a_provider_and_true_with_one(self) -> None:
        url = "postgresql+asyncpg://u:p@localhost/db"
        plain = SessionManager(url, store=DEFAULT_STORE)
        scoped = SessionManager(url, session_settings=lambda: None, store=DEFAULT_STORE)

        assert plain.has_session_settings is False
        assert scoped.has_session_settings is True

    def test_startup_without_scoped_models_changes_nothing(self) -> None:
        assert (
            startup_checks(
                SchemaConfig(), "sqlite", {}, has_session_settings=False, path="stores.default"
            )
            == ()
        )

    def test_startup_refuses_create_all_with_scoped_models_on_postgres(self) -> None:
        schema_config = SchemaConfig()
        scoped_models = _scoped()

        with pytest.raises(ConfigError, match=r"create_schema\(migrator_url"):
            startup_checks(
                schema_config,
                "postgresql",
                scoped_models,
                has_session_settings=False,
                path="stores.default",
            )

    def test_startup_refuses_scoped_models_on_another_dialect_unless_allowed(self) -> None:
        schema_config = SchemaConfig()
        scoped_models = _scoped()
        allowed = SchemaConfig(allow_unprotected_dialect=True)

        with pytest.raises(ConfigError, match=r"sqlite.*scoped_items"):
            startup_checks(
                schema_config,
                "sqlite",
                scoped_models,
                has_session_settings=False,
                path="stores.default",
            )

        assert startup_checks(
            allowed, "sqlite", scoped_models, has_session_settings=False, path="stores.default"
        ) == ("scoped_items",)

    def test_startup_never_creates_the_schema_with_the_application_session(self) -> None:
        schema_config = SchemaConfig()
        external = SchemaConfig(mode="external")

        with pytest.raises(ConfigError, match="session settings"):
            startup_checks(
                schema_config, "sqlite", {}, has_session_settings=True, path="stores.default"
            )

        assert (
            startup_checks(external, "sqlite", {}, has_session_settings=True, path="stores.default")
            == ()
        )

    async def test_create_all_mode_creates_unscoped_tables_as_before(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Plain)
        wiring.prepare_models((Plain,))

        await wiring.startup_checks()
        async with wiring.lifespan_init():
            assert await wiring.readiness()

    async def test_create_all_mode_with_the_dialect_opt_out_warns_and_creates_plain_tables(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Scoped, allow_unprotected_dialect=True)
        wiring.prepare_models((Scoped,))

        with caplog.at_level(logging.WARNING):
            await wiring.startup_checks()

        async with wiring.lifespan_init():
            pass

        assert [
            record.__dict__[SqlalchemyKey.TABLES]
            for record in caplog.records
            if record.msg == SqlalchemyEvent.SCOPED_TABLES_UNPROTECTED
        ] == [["scoped_items"]]

    async def test_external_mode_fails_at_startup_when_a_table_is_missing(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Plain, mode="external")
        wiring.prepare_models((Plain,))

        async with wiring.lifespan_init():
            pass
        with pytest.raises(ConfigError, match="plain_items"):
            await wiring.startup_checks()

    def test_external_mode_refuses_scoped_models_on_another_dialect_unless_allowed(self) -> None:
        external = SchemaConfig(mode="external")
        scoped_models = _scoped()
        allowed = SchemaConfig(mode="external", allow_unprotected_dialect=True)

        with pytest.raises(ConfigError, match="scoped_items"):
            startup_checks(
                external, "sqlite", scoped_models, has_session_settings=True, path="stores.default"
            )

        assert startup_checks(
            allowed, "sqlite", scoped_models, has_session_settings=True, path="stores.default"
        ) == ("scoped_items",)

    def test_external_mode_wires_the_scoped_provider_and_the_store_s_elevation(self) -> None:
        external = _wiring("postgresql+asyncpg://app:secret@localhost/app", Plain, mode="external")
        create_all = _wiring("sqlite+aiosqlite://", Plain)

        assert isinstance(external.elevation, SQLAlchemyStoreElevation)
        assert external.uow_factory._session_manager.has_session_settings
        assert create_all.elevation is None
        assert not create_all.uow_factory._session_manager.has_session_settings

    async def test_external_mode_validates_scope_bindings_at_startup(self) -> None:
        wiring = _wiring(
            "postgresql+asyncpg://app:secret@localhost/app",
            Scoped,
            mode="external",
            scopes={"holder": "request.unregistered_source"},
        )
        with pytest.raises(ConfigError, match="unregistered_source"):
            wiring.prepare_models((Scoped,))

    def test_create_kernel_keeps_the_elevation_sink_a_module_registered(self) -> None:
        config = convert_config({"url": "sqlite+aiosqlite://"}, DatabaseConfig, strict=True)
        manager = SessionManager(
            "sqlite+aiosqlite://",
            pool_size=None,
            max_overflow=None,
            pool_timeout=None,
            pool_recycle=None,
            store=DEFAULT_STORE,
        )
        sink = sink_for(manager, "editor")

        def register(container: LoomContainer) -> None:
            container.register_instance(ElevationSink, sink)

        with_sink = create_kernel(config=config, use_cases=[], modules=[register])
        without_sink = create_kernel(config=config, use_cases=[], modules=[])

        assert with_sink.container.resolve(ElevationSink) is sink
        assert without_sink.container.resolve_or_none(ElevationSink) is None

    def test_a_time_with_time_zone_column_is_refused_on_a_dialect_that_drops_its_offset(
        self,
    ) -> None:
        with pytest.raises(ConfigError, match=r"zoned_items\.closes_at.*offset"):
            check_time_zone_columns("sqlite", _zoned())

    def test_a_time_with_time_zone_column_is_accepted_on_postgres(self) -> None:
        check_time_zone_columns("postgresql", _zoned())

    async def test_startup_refuses_a_time_with_time_zone_column_on_another_dialect(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Zoned)
        wiring.prepare_models((Zoned,))

        with pytest.raises(ConfigError, match=r"zoned_items\.closes_at"):
            await wiring.startup_checks()
