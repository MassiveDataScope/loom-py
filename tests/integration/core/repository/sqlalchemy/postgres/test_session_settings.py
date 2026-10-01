from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextvars import ContextVar
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError, PendingRollbackError

from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.session_settings import SessionSettings

from .conftest import ROWS_TABLE, TENANT_SETTING

pytestmark = pytest.mark.integration

current_tenant: ContextVar[str | None] = ContextVar("current_tenant", default=None)

_ROWS = f"SELECT payload FROM {ROWS_TABLE} ORDER BY payload"
_COUNTS = f"SELECT tenant_id, count(*) FROM {ROWS_TABLE} GROUP BY tenant_id ORDER BY tenant_id"
_CURRENT = f"SELECT current_setting('{TENANT_SETTING}', true)"
_INSERT = f"INSERT INTO {ROWS_TABLE} (tenant_id, payload) VALUES (:tenant, :payload)"


class ProviderError(RuntimeError):
    pass


def tenant_settings() -> Mapping[str, str] | None:
    value = current_tenant.get()
    return None if value is None else {TENANT_SETTING: value}


def raising_provider() -> Mapping[str, str] | None:
    raise ProviderError("no context available")


def _manager(
    uri: str, session_settings: SessionSettings | None, pool_size: int = 4
) -> SessionManager:
    return SessionManager(
        uri,
        session_settings=session_settings,
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=False,
    )


def _record_statements(manager: SessionManager, containing: str = "") -> list[str]:
    statements: list[str] = []

    @event.listens_for(manager.engine.sync_engine, "before_cursor_execute")
    def _record(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        if containing in statement:
            statements.append(statement)

    return statements


def _is_unset(value: object) -> bool:
    return value in ("", None)


async def _payloads(manager: SessionManager) -> list[str]:
    async with manager.session() as session:
        return list((await session.execute(text(_ROWS))).scalars())


@pytest.fixture
async def app_sessions(pg_app_uri: str) -> AsyncIterator[SessionManager]:
    manager = _manager(pg_app_uri, tenant_settings)
    try:
        yield manager
    finally:
        await manager.dispose()


@pytest.fixture
async def platform_sessions(pg_platform_uri: str) -> AsyncIterator[SessionManager]:
    manager = _manager(pg_platform_uri, None)
    try:
        yield manager
    finally:
        await manager.dispose()


async def test_a_transaction_runs_with_its_tenant_set(app_sessions: SessionManager) -> None:
    current_tenant.set("a")
    async with app_sessions.session() as session:
        current = (await session.execute(text(_CURRENT))).scalar_one()
        payloads = list((await session.execute(text(_ROWS))).scalars())
    assert current == "a"
    assert payloads == ["a1", "a2"]


async def test_a_query_without_the_tenant_filter_returns_only_the_tenant_rows(
    app_sessions: SessionManager,
) -> None:
    current_tenant.set("b")
    assert await _payloads(app_sessions) == ["b1"]


async def test_concurrent_tenants_on_a_shared_pool_never_leak(app_sessions: SessionManager) -> None:
    async def run_transaction(value: str | None) -> tuple[str | None, tuple[tuple[str, int], ...]]:
        current_tenant.set(value)
        async with app_sessions.session() as session:
            rows = (await session.execute(text(_COUNTS))).all()
            await session.commit()
        return value, tuple((tenant_id, int(count)) for tenant_id, count in rows)

    outcomes = await asyncio.gather(*(run_transaction(("a", "b", None)[i % 3]) for i in range(60)))

    assert set(outcomes) == {("a", (("a", 2),)), ("b", (("b", 1),)), (None, ())}


async def test_no_context_sets_nothing_and_sees_zero_rows(app_sessions: SessionManager) -> None:
    current_tenant.set(None)
    async with app_sessions.session() as session:
        current = (await session.execute(text(_CURRENT))).scalar_one_or_none()
        payloads = list((await session.execute(text(_ROWS))).scalars())
    assert _is_unset(current)
    assert payloads == []


async def test_a_write_outside_the_tenant_is_refused_by_the_database(
    app_sessions: SessionManager,
) -> None:
    current_tenant.set("a")
    insert = text(_INSERT)
    foreign_row = {"tenant": "b", "payload": "b2"}
    async with app_sessions.session() as session:
        await session.execute(insert, {"tenant": "a", "payload": "a3"})
        with pytest.raises(DBAPIError):
            await session.execute(insert, foreign_row)
        await session.rollback()


async def test_a_committed_transaction_leaves_the_pooled_connection_clean(pg_app_uri: str) -> None:
    manager = _manager(pg_app_uri, tenant_settings, pool_size=1)
    try:
        current_tenant.set("a")
        async with manager.session() as session:
            assert (await session.execute(text(_CURRENT))).scalar_one() == "a"
            await session.commit()
        async with manager.engine.connect() as conn:
            assert _is_unset((await conn.execute(text(_CURRENT))).scalar_one_or_none())
        current_tenant.set(None)
        async with manager.session() as session:
            assert _is_unset((await session.execute(text(_CURRENT))).scalar_one_or_none())
    finally:
        await manager.dispose()


async def test_a_savepoint_does_not_rerun_the_provider(pg_app_uri: str) -> None:
    calls = 0

    def counting_provider() -> Mapping[str, str] | None:
        nonlocal calls
        calls += 1
        return {TENANT_SETTING: "a"}

    manager = _manager(pg_app_uri, counting_provider)
    set_config_statements = _record_statements(manager, "set_config")
    try:
        async with manager.session() as session:
            await session.execute(text(_ROWS))
            async with session.begin_nested():
                await session.execute(text(_ROWS))
            await session.execute(text(_ROWS))
            await session.commit()
    finally:
        await manager.dispose()
    assert calls == 1
    assert len(set_config_statements) == 1


@pytest.mark.parametrize(
    ("session_settings", "error"),
    [
        pytest.param(raising_provider, ProviderError, id="raises"),
        pytest.param(lambda: {TENANT_SETTING: 7}, TypeError, id="non-str-value"),
        pytest.param(lambda: {"tenant_id": "a"}, ValueError, id="unprefixed-key"),
    ],
)
async def test_a_bad_provider_fails_before_any_product_statement(
    pg_app_uri: str, session_settings: SessionSettings, error: type[Exception]
) -> None:
    manager = _manager(pg_app_uri, session_settings)
    statements = _record_statements(manager)
    rows = text(_ROWS)
    try:
        async with manager.session() as session:
            with pytest.raises(error):
                await session.execute(rows)
    finally:
        await manager.dispose()
    assert statements == []


async def test_after_a_provider_failure_the_session_is_unusable_until_rolled_back(
    pg_app_uri: str,
) -> None:
    attempts = 0

    def failing_once() -> Mapping[str, str] | None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ProviderError("no context yet")
        return {TENANT_SETTING: "a"}

    manager = _manager(pg_app_uri, failing_once)
    product_statements = _record_statements(manager, ROWS_TABLE)
    rows = text(_ROWS)
    try:
        async with manager.session() as session:
            with pytest.raises(ProviderError):
                await session.execute(rows)
            with pytest.raises(PendingRollbackError):
                await session.execute(rows)
            assert product_statements == []
            await session.rollback()
            payloads = list((await session.execute(rows)).scalars())
    finally:
        await manager.dispose()
    assert attempts == 2
    assert payloads == ["a1", "a2"]
    assert len(product_statements) == 1


async def test_platform_and_app_managers_in_one_process_see_different_rows(
    app_sessions: SessionManager, platform_sessions: SessionManager
) -> None:
    current_tenant.set("a")
    assert await _payloads(app_sessions) == ["a1", "a2"]
    assert await _payloads(platform_sessions) == ["a1", "a2", "b1"]
