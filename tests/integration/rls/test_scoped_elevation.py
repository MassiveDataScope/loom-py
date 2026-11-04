"""The composition's one elevation sink against real Postgres (US7 PR 5).

Through ``build_application`` on a pool of one connection, so the next
checkout is the connection that held the flag. Flags set and cleared by a
nested execution are pinned in ``test_elevation.py``; here:

- the composition wires one ``ScopedElevationSink``;
- ``elevate`` from a ``gather`` child is refused and elevates nothing;
- a failed clearing, or a timeout once the flag was sent, commits nothing,
  even when the parent catches the error, and leaves no flag in the pool.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.composition import build_application
from loom.core.authz import Decision, Grant, Scope
from loom.core.authz.elevation import elevated_scopes
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.persistence.elevation import ScopedElevationSink
from loom.core.repository.sqlalchemy.rls import SQLAlchemyStoreElevation, create_schema, elevate
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.scope import ScopeOwnershipError
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import (
    FLAG,
    U1,
    BootstrapFactory,
    application_for,
    scalar,
    seed_notes,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("notes_wiring")]

DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
_INSERT = f"INSERT INTO notes (owner_id, editor, body) VALUES ('{U1}', 'ana', 'outer')"
_SEEDED = "a,b,z"
_flags: list[str] = []
"""The flag each elevating execution saw, kept even when its invocation fails."""


def _session(session_manager: SessionManager) -> AsyncSession:
    session = session_manager.active_session
    assert session is not None
    return session


class EditBobElevated(UseCase[notes.Note, None]):
    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> None:
        await elevate("editor", DECISION, at=Scope.of(U1))
        session = _session(self._session_manager)
        _flags.append((await session.execute(text(FLAG))).scalar_one())
        await session.execute(text("UPDATE notes SET body = 'edited' WHERE editor = 'bob'"))


class WriteAndNest(UseCase[notes.Note, None]):
    """Write, run :class:`EditBobElevated` nested, maybe catching its error, write again."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self, catch: bool, timeout_s: float | None) -> None:
        await _session(self._session_manager).execute(text(_INSERT))
        try:
            async with asyncio.timeout(timeout_s):
                await self._app.invoke(EditBobElevated)
        except Exception:
            if not catch:
                raise
        await _session(self._session_manager).execute(text(_INSERT))


class ElevateFromGatherChild(UseCase[notes.Note, tuple[object, frozenset[str], str]]):
    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> tuple[object, frozenset[str], str]:
        elevation = elevate("editor", DECISION, at=Scope.of(U1))
        (refused,) = await asyncio.gather(elevation, return_exceptions=True)
        flag = (await _session(self._session_manager).execute(text(FLAG))).scalar_one()
        return refused, elevated_scopes(), flag


@dataclass(frozen=True)
class Composed:
    app: AppInvoker
    manager: SessionManager
    sink: object
    bodies_sql: str
    bypass: str

    async def outcome(self) -> tuple[str, str]:
        """Every body, read through the bypass user, and the next checkout's raw flag."""
        bodies = await scalar(self.bypass, self.bodies_sql)
        async with self.manager.engine.connect() as connection:
            return bodies, (await connection.execute(text(FLAG))).scalar_one()


@pytest.fixture
async def composed(scoped_database: BootstrapFactory, tmp_path: Path) -> AsyncIterator[Composed]:
    schema = f"notes_sink_{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    await seed_notes(database.bypass)
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{schema}.yaml").read_text())
    document["app"]["discovery"]["modules"]["include"].append(__name__)
    document["stores"]["default"]["pool"] = {"size": 1, "max_overflow": 0}
    document["persistence"] = {"cursors": {"signing": "ephemeral"}}
    (tmp_path / "app.yaml").write_text(yaml.safe_dump(document))
    built = build_application(
        str(tmp_path / "app.yaml"), adapter=FastAPIAdapter(), environment="test"
    )
    _flags.clear()
    identity = set_identity(Identity(subject=U1))
    try:
        async with built.running():
            yield Composed(
                app=cast(AppInvoker, built.kernel.app),
                manager=built.kernel.container.resolve(SessionManager),
                sink=built.kernel.executor._elevation_sink,
                bodies_sql=f"SELECT string_agg(body, ',' ORDER BY body) FROM {schema}.notes",
                bypass=database.bypass,
            )
    finally:
        reset_identity(identity)


def test_the_composition_wires_one_scoped_sink(composed: Composed) -> None:
    assert isinstance(composed.sink, ScopedElevationSink)


async def test_elevate_from_a_gather_child_is_refused_and_elevates_nothing(
    composed: Composed,
) -> None:
    refused, elevated, flag = await composed.app.invoke(ElevateFromGatherChild)

    assert isinstance(refused, ScopeOwnershipError)
    assert (elevated, flag) == (frozenset(), "")


def _refused(message: str) -> Awaitable[object]:
    async def refuse() -> None:
        raise RuntimeError(message)

    return refuse()


@pytest.mark.parametrize("catch", [False, True], ids=["uncaught", "caught"])
@pytest.mark.parametrize("invalidation_fails", [False, True], ids=["invalidated", "kept"])
async def test_a_failed_clearing_commits_nothing_and_leaves_no_flag(
    composed: Composed, monkeypatch: pytest.MonkeyPatch, catch: bool, invalidation_fails: bool
) -> None:
    monkeypatch.setattr(SQLAlchemyStoreElevation, "clear_flags", lambda *_: _refused("clearing"))
    if invalidation_fails:
        monkeypatch.setattr(AsyncSession, "invalidate", lambda _: _refused("invalidation"))
    raised = "invalidation" if invalidation_fails and not catch else "clearing"

    with pytest.raises(RuntimeError, match=raised):
        await composed.app.invoke(WriteAndNest, params={"catch": catch, "timeout_s": None})

    assert _flags == ["on"]
    assert await composed.outcome() == (_SEEDED, "")


async def test_a_timeout_once_the_flag_was_sent_commits_nothing_even_when_caught(
    composed: Composed, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent = SQLAlchemyStoreElevation.set_flag
    seen: list[str] = []

    def set_then_linger(store: SQLAlchemyStoreElevation, scope: str) -> Awaitable[object]:
        statement, session = sent(store, scope), store._open_session()

        async def linger() -> None:
            await statement  # set_config has run on the server
            seen.append((await session.execute(text(FLAG))).scalar_one())
            await asyncio.sleep(0.3)

        return linger()

    monkeypatch.setattr(SQLAlchemyStoreElevation, "set_flag", set_then_linger)

    with pytest.raises(RuntimeError, match="elevating scope 'editor' was interrupted"):
        await composed.app.invoke(WriteAndNest, params={"catch": True, "timeout_s": 0.05})

    assert seen == ["on"]
    assert await composed.outcome() == (_SEEDED, "")
