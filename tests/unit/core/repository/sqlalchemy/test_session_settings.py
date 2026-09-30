from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.session_settings import (
    SessionSettings,
    install_session_settings,
    settings_statement,
)


def test_keys_and_values_are_bound_parameters_and_never_inlined_in_the_sql() -> None:
    result = settings_statement({"app.tenant_id": "acme", "app.subject": "u-1"})

    assert result is not None
    clause, params = result
    assert params == {"k0": "app.tenant_id", "v0": "acme", "k1": "app.subject", "v1": "u-1"}
    assert str(clause) == "SELECT set_config(:k0, :v0, true), set_config(:k1, :v1, true)"


@pytest.mark.parametrize("values", [None, {}])
def test_none_or_empty_values_give_no_statement(values: Mapping[str, str] | None) -> None:
    assert settings_statement(values) is None


@pytest.mark.parametrize(
    "key",
    [
        "tenant_id",
        "app.tenant_id.extra",
        "app.",
        ".tenant_id",
        "app.tenant-id",
        "app.tenant id",
        "1app.tenant_id",
        "app.1tenant",
        "app.tenant_id; DROP TABLE t",
        "",
    ],
)
def test_an_invalid_key_raises_value_error(key: str) -> None:
    with pytest.raises(ValueError, match="session setting key"):
        settings_statement({key: "acme"})


def test_a_non_str_key_raises_value_error() -> None:
    with pytest.raises(ValueError, match="session setting key"):
        settings_statement({1: "acme"})  # type: ignore[dict-item]


@pytest.mark.parametrize("value", [1, None, b"acme", ["acme"]])
def test_a_non_str_value_raises_type_error(value: object) -> None:
    with pytest.raises(TypeError, match="session setting value"):
        settings_statement({"app.tenant_id": value})  # type: ignore[dict-item]


def test_session_settings_on_a_non_postgres_url_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="postgresql"):
        SessionManager(
            "sqlite+aiosqlite:///:memory:", session_settings=lambda: {"app.tenant_id": "acme"}
        )


def test_from_config_rejects_session_settings_on_a_non_postgres_url() -> None:
    with pytest.raises(ValueError, match="postgresql"):
        SessionManager.from_config(
            {"url": "sqlite+aiosqlite:///:memory:"}, session_settings=lambda: None
        )


@pytest.mark.parametrize(
    "engine_kwargs",
    [
        {"isolation_level": "AUTOCOMMIT"},
        {"isolation_level": "autocommit"},
        {"execution_options": {"isolation_level": "AUTOCOMMIT"}},
    ],
)
def test_session_settings_with_autocommit_is_rejected_at_construction(
    engine_kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="AUTOCOMMIT"):
        SessionManager(
            "postgresql+asyncpg://user:secret@localhost/db",
            session_settings=lambda: None,
            **engine_kwargs,
        )


async def test_without_session_settings_sessions_use_the_plain_sqlalchemy_session_class() -> None:
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
    )
    try:
        async with manager.session() as session:
            assert type(session.sync_session) is Session
    finally:
        await manager.dispose()


def _sqlite_session_class(provider: SessionSettings) -> tuple[Engine, type[Session]]:
    engine = create_engine("sqlite://")
    session_class = type("SettingsSession", (Session,), {})
    install_session_settings(session_class, provider)
    return engine, session_class


def test_a_provider_returning_none_lets_the_transaction_run_untouched() -> None:
    engine, session_class = _sqlite_session_class(lambda: None)
    with session_class(engine) as session:
        assert session.execute(text("SELECT 1")).scalar_one() == 1
        assert not session.connection().invalidated


def test_a_savepoint_does_not_call_the_provider_again() -> None:
    calls: list[int] = []

    def counting() -> Mapping[str, str] | None:
        calls.append(1)
        return None

    engine, session_class = _sqlite_session_class(counting)
    with session_class(engine) as session:
        session.execute(text("SELECT 1"))
        with session.begin_nested():
            session.execute(text("SELECT 1"))
    assert len(calls) == 1


def test_a_raising_provider_invalidates_the_connection() -> None:
    class ProviderError(RuntimeError):
        pass

    def raising() -> Mapping[str, str] | None:
        raise ProviderError("no context")

    engine, session_class = _sqlite_session_class(raising)
    with session_class(engine) as session:
        with pytest.raises(ProviderError):
            session.execute(text("SELECT 1"))
        assert session.connection().invalidated


def test_a_failing_settings_statement_invalidates_the_connection() -> None:
    engine, session_class = _sqlite_session_class(lambda: {"app.tenant_id": "acme"})
    with session_class(engine) as session:
        with pytest.raises(OperationalError):
            session.execute(text("SELECT 1"))
        assert session.connection().invalidated


async def test_a_provider_on_a_postgres_url_installs_a_dedicated_session_class() -> None:
    manager = SessionManager(
        "postgresql+asyncpg://user:secret@localhost/db", session_settings=lambda: None
    )
    try:
        assert manager.session_factory.kw["sync_session_class"].__name__ == "SettingsSession"
    finally:
        await manager.dispose()


async def test_from_config_installs_the_provider_on_a_postgres_url() -> None:
    manager = SessionManager.from_config(
        {"url": "postgresql+asyncpg://user:secret@localhost/db"}, session_settings=lambda: None
    )
    try:
        assert manager.session_factory.kw["sync_session_class"].__name__ == "SettingsSession"
    finally:
        await manager.dispose()
