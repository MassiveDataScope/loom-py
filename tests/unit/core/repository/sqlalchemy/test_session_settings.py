from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from sqlalchemy.orm import Session

from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.session_settings import settings_statement


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
