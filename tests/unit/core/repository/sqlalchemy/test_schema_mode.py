from __future__ import annotations

import logging

import msgspec
import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, RowScoped
from loom.core.model.types import Integer, Text
from loom.core.repository.sqlalchemy.backend import (
    SQLAlchemyBackend,
    _DatabaseConfig,
    _SchemaConfig,
    startup_checks,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class Plain(BaseModel):
    __tablename__ = "plain_items"
    id: int = ColumnField(Integer, primary_key=True)
    name: str = ColumnField(Text)


class Scoped(BaseModel, RowScoped):
    __tablename__ = "scoped_items"
    holder: int = ColumnField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


def _scoped() -> dict:
    metadata = MetaData()
    compile_all(Scoped, metadata=metadata)
    return dict(scoped_tables(metadata))


def test_schema_config_defaults_and_is_frozen() -> None:
    config = msgspec.convert({"url": "sqlite+aiosqlite://"}, _DatabaseConfig)

    assert config.schema.mode == "create_all"
    assert config.schema.allow_unprotected_dialect is False
    with pytest.raises(AttributeError):
        config.schema.mode = "external"  # type: ignore[misc]


def test_schema_config_reads_mode_and_the_dialect_opt_out() -> None:
    raw = {
        "url": "sqlite+aiosqlite://",
        "schema": {"mode": "external", "allow_unprotected_dialect": True},
    }

    config = msgspec.convert(raw, _DatabaseConfig)

    assert config.schema == _SchemaConfig(mode="external", allow_unprotected_dialect=True)


def test_an_unknown_mode_is_rejected() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"url": "x", "schema": {"mode": "drop"}}, _DatabaseConfig)


def test_has_session_settings_is_false_without_a_provider_and_true_with_one() -> None:
    url = "postgresql+asyncpg://u:p@localhost/db"
    plain = SessionManager(url)
    scoped = SessionManager(url, session_settings=lambda: None)

    assert plain.has_session_settings is False
    assert scoped.has_session_settings is True


def test_startup_without_scoped_models_changes_nothing() -> None:
    assert startup_checks(_SchemaConfig(), "sqlite", {}, has_session_settings=False) == ()


def test_startup_refuses_create_all_with_scoped_models_on_postgres() -> None:
    with pytest.raises(ConfigError, match=r"create_schema\(migrator_url"):
        startup_checks(_SchemaConfig(), "postgresql", _scoped(), has_session_settings=False)


def test_startup_refuses_scoped_models_on_another_dialect_unless_allowed() -> None:
    with pytest.raises(ConfigError, match=r"sqlite.*scoped_items"):
        startup_checks(_SchemaConfig(), "sqlite", _scoped(), has_session_settings=False)

    allowed = _SchemaConfig(allow_unprotected_dialect=True)
    assert startup_checks(allowed, "sqlite", _scoped(), has_session_settings=False) == (
        "scoped_items",
    )


def test_startup_never_creates_the_schema_with_the_application_session() -> None:
    with pytest.raises(ConfigError, match="session settings"):
        startup_checks(_SchemaConfig(), "sqlite", {}, has_session_settings=True)

    external = _SchemaConfig(mode="external")
    assert startup_checks(external, "sqlite", {}, has_session_settings=True) == ()


def _wiring(url: str, *models: type, **schema: object):
    config = {"app": {"name": "demo"}, "database": {"url": url, "schema": schema}}
    return SQLAlchemyBackend().build(ConfigContext.from_dict(config), models)


async def test_create_all_mode_creates_unscoped_tables_as_before() -> None:
    wiring = _wiring("sqlite+aiosqlite://", Plain)
    wiring.prepare_models((Plain,))

    async with wiring.lifespan_init():
        assert await wiring.readiness()


async def test_create_all_mode_with_the_dialect_opt_out_warns_and_creates_plain_tables(
    caplog: pytest.LogCaptureFixture,
) -> None:
    wiring = _wiring("sqlite+aiosqlite://", Scoped, allow_unprotected_dialect=True)
    wiring.prepare_models((Scoped,))

    with caplog.at_level(logging.WARNING):
        async with wiring.lifespan_init():
            pass

    assert any("scoped_items" in record.getMessage() for record in caplog.records)


async def test_external_mode_fails_at_startup_when_a_table_is_missing() -> None:
    wiring = _wiring("sqlite+aiosqlite://", Plain, mode="external")
    wiring.prepare_models((Plain,))

    with pytest.raises(ConfigError, match="plain_items"):
        async with wiring.lifespan_init():
            pass
