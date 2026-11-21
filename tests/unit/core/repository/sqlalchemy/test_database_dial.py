"""``DatabaseDial``: the URL and ``connect()`` arguments every engine of a store dials."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import MetaData
from sqlalchemy.pool import NullPool

from loom.core.model.redaction import Dsn, RedactedMap, Secret
from loom.core.stores.database import DatabaseConfig
from loom.stores.sqlalchemy.alembic import StoreSchema
from loom.stores.sqlalchemy.migrations import alembic_config, migration_dial
from loom.stores.sqlalchemy.session_manager import DatabaseDial

_ADDRESS = "postgresql+asyncpg://app:url-pass@db.invalid:5432/app"
_SERVER_SETTINGS = {"server_settings": {"application_name": "loom-dial"}}


def _config(**fields: object) -> DatabaseConfig:
    return DatabaseConfig(url=Dsn(_ADDRESS), **fields)  # type: ignore[arg-type]


@pytest.fixture
def config() -> DatabaseConfig:
    return _config(connect_args=RedactedMap({**_SERVER_SETTINGS, "password": "arg-pass"}))


@pytest.fixture
def dial(config: DatabaseConfig) -> DatabaseDial:
    return DatabaseDial.from_config(config)


class TestFromConfig:
    @pytest.mark.parametrize(
        ("credentials", "expected"),
        [
            ({}, _ADDRESS),
            ({"username": "ops"}, "postgresql+asyncpg://ops:url-pass@db.invalid:5432/app"),
            (
                {"password": Secret("own-pass")},
                "postgresql+asyncpg://app:own-pass@db.invalid:5432/app",
            ),
            (
                {"username": "ops", "password": Secret("own-pass")},
                "postgresql+asyncpg://ops:own-pass@db.invalid:5432/app",
            ),
        ],
        ids=["url-credentials", "username-wins", "password-wins", "both-win"],
    )
    def test_explicit_credentials_win_over_the_url(
        self, credentials: dict[str, object], expected: str
    ) -> None:
        dial = DatabaseDial.from_config(_config(**credentials))

        assert dial.url.reveal() == expected

    def test_the_client_options_are_connect_args_plus_the_standard_timeouts(self) -> None:
        config = _config(
            connect_args=RedactedMap(_SERVER_SETTINGS), connect_timeout=3, read_timeout=30
        )

        dial = DatabaseDial.from_config(config)

        assert dial.client_options.reveal() == {
            **_SERVER_SETTINGS,
            "timeout": 3,
            "command_timeout": 30,
        }


class TestDisplay:
    def test_the_repr_hides_the_url_password_and_secret_arguments(self, dial: DatabaseDial) -> None:
        shown = repr(dial)

        assert "url-pass" not in shown
        assert "arg-pass" not in shown
        assert "loom-dial" in shown


class TestWithUrl:
    def test_another_address_keeps_the_store_arguments(self, dial: DatabaseDial) -> None:
        moved = dial.with_url("postgresql+asyncpg://migrator:m@db.invalid/app")

        assert moved.url.reveal() == "postgresql+asyncpg://migrator:m@db.invalid/app"
        assert moved.client_options == dial.client_options


class TestCreateAsyncEngine:
    async def test_the_engine_dials_the_url_with_the_callers_pool(self, dial: DatabaseDial) -> None:
        engine = dial.create_async_engine(poolclass=NullPool)
        try:
            assert engine.url.render_as_string(hide_password=False) == dial.url.reveal()
            assert type(engine.sync_engine.pool) is NullPool
        finally:
            await engine.dispose()

    def test_the_caller_cannot_pass_connect_args_again(self, dial: DatabaseDial) -> None:
        with pytest.raises(TypeError, match="connect_args"):
            dial.create_async_engine(connect_args={})


class TestMigrationDial:
    def test_a_migration_dials_the_trees_url_with_the_stores_arguments(
        self, config: DatabaseConfig, dial: DatabaseDial, tmp_path: Path
    ) -> None:
        migrator = "postgresql+asyncpg://migrator:p%40ss@db.invalid/app"
        schema = StoreSchema(
            models=(),
            metadata=MetaData(),
            database=config,
            bootstrap=None,
            scoped={},
            scope_sources={},
        )

        found = migration_dial(alembic_config(str(tmp_path / "alembic"), migrator), schema)

        assert found == dial.with_url(migrator)
