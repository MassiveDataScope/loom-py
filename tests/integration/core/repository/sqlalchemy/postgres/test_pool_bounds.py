"""A declared pool bounds the connections one process opens against Postgres.

Twelve callers each hold a connection for longer than the pool's checkout
timeout. At most ``size + max_overflow`` are ever open on the server — counted
in ``pg_stat_activity`` by a unique ``application_name`` — and every other
caller fails with a checkout timeout close to the declared one instead of
hanging.
"""

from __future__ import annotations

import asyncio
import time
import uuid

from sqlalchemy import text
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.persistence.database import DatabaseConfig, PoolConfig
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.sqlalchemy.session_manager import SessionManager, build_session_manager

_CALLERS = 12
_HOLD_SECONDS = 1.5
_TIMEOUT_SECONDS = 1


async def _hold(manager: SessionManager, outcomes: list[str], durations: list[float]) -> None:
    started = time.perf_counter()
    try:
        async with manager.engine.connect() as connection:
            await connection.execute(text("SELECT pg_sleep(:s)"), {"s": _HOLD_SECONDS})
        outcomes.append("held")
    except PoolTimeoutError:
        outcomes.append("timed_out")
        durations.append(time.perf_counter() - started)


async def _peak_connections(admin_uri: str, application: str, stop: asyncio.Event) -> int:
    engine = create_async_engine(admin_uri, poolclass=NullPool)
    peak = 0
    try:
        async with engine.connect() as connection:
            while not stop.is_set():
                result = await connection.execute(
                    text("SELECT count(*) FROM pg_stat_activity WHERE application_name = :app"),
                    {"app": application},
                )
                peak = max(peak, int(result.scalar_one()))
                await asyncio.sleep(0.05)
    finally:
        await engine.dispose()
    return peak


async def test_the_pool_never_opens_more_than_size_plus_overflow(pg_admin_uri: str) -> None:
    application = f"loom-it-pool-{uuid.uuid4().hex[:8]}"
    config = DatabaseConfig(
        url=pg_admin_uri,
        pool=PoolConfig(size=2, max_overflow=1, timeout=_TIMEOUT_SECONDS),
        connect_args={"server_settings": {"application_name": application}},
    )
    manager = build_session_manager(config, SERVER_PROFILE)
    outcomes: list[str] = []
    durations: list[float] = []
    stop = asyncio.Event()
    try:
        monitor = asyncio.create_task(_peak_connections(pg_admin_uri, application, stop))
        await asyncio.gather(*(_hold(manager, outcomes, durations) for _ in range(_CALLERS)))
        stop.set()
        peak = await monitor
    finally:
        await manager.dispose()

    assert peak <= 3
    assert outcomes.count("held") == 3
    assert outcomes.count("timed_out") == _CALLERS - 3
    assert all(_TIMEOUT_SECONDS * 0.8 <= d < _TIMEOUT_SECONDS + 1 for d in durations)
