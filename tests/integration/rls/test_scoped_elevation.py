"""The composition's one elevation sink against real Postgres (US7 PR 5, S3).

- A nested execution that elevates clears its flag on the connection the
  outer execution keeps, and both executions' writes commit together.
- ``elevate`` from a ``gather`` child is refused by the invocation scope's
  checkpoint and leaves nothing elevated.
- When clearing the flag fails, whether invalidating the session then
  succeeds or fails, the invocation commits nothing, even when a parent use
  case catches the error, and the connection that held the flag returns to
  the pool without it.

The use cases run through the real composition (``build_application``) with
``schema.mode: external``, on a pool of one connection, so the next checkout
is the connection that held the flag.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

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
from tests.integration.rls.conftest import FLAG, U1, BootstrapFactory, application_for, seed_notes

pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("notes_wiring"),
    pytest.mark.timeout(120),
]

DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
_PID = "SELECT pg_backend_pid()"
_EDIT_BOB = "UPDATE notes SET body = 'edited' WHERE editor = 'bob'"
_INSERT_OUTER = f"INSERT INTO notes (owner_id, editor, body) VALUES ('{U1}', 'ana', 'outer')"
_BODIES = "SELECT body FROM {schema}.notes ORDER BY body"
_SEEDED = ("a", "b", "z")


@dataclass(frozen=True)
class OnConnection:
    """A connection's backend pid and the ``editor`` flag it carried."""

    pid: int
    flag: str


_held: list[OnConnection] = []
"""What :class:`EditBobElevated` saw, kept even when its invocation fails afterwards."""


def _active(session_manager: SessionManager) -> Any:
    session = session_manager.active_session
    if session is None:
        raise RuntimeError("no unit-of-work session")
    return session


async def _on_connection(session: Any) -> OnConnection:
    return OnConnection(
        pid=(await session.execute(text(_PID))).scalar_one(),
        flag=(await session.execute(text(FLAG))).scalar_one(),
    )


class EditBobElevated(UseCase[notes.Note, int]):
    """Elevate ``editor`` and edit bob's note; record the connection and its flag."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> int:
        await elevate("editor", DECISION, at=Scope.of(U1))
        session = _active(self._session_manager)
        edited = cast(CursorResult[Any], await session.execute(text(_EDIT_BOB))).rowcount
        _held.append(await _on_connection(session))
        return edited


class WriteThenNestEditor(UseCase[notes.Note, tuple[int, OnConnection]]):
    """Write, run :class:`EditBobElevated` nested, then report the connection and its flag."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self) -> tuple[int, OnConnection]:
        session = _active(self._session_manager)
        await session.execute(text(_INSERT_OUTER))
        edited = cast(int, await self._app.invoke(EditBobElevated))
        return edited, await _on_connection(session)


@dataclass(frozen=True)
class GatherElevation:
    """What the parent saw after a ``gather`` child tried to elevate ``editor``."""

    refused: ScopeOwnershipError | None
    elevated: frozenset[str]
    flag: str
    bob_rows_writable: int


class ElevateFromGatherChild(UseCase[notes.Note, GatherElevation]):
    """Elevate ``editor`` from a ``gather`` child, before the transaction begins."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> GatherElevation:
        refused: ScopeOwnershipError | None = None
        try:
            await asyncio.gather(elevate("editor", DECISION, at=Scope.of(U1)))
        except ScopeOwnershipError as exc:
            refused = exc
        session = _active(self._session_manager)
        touched = cast(
            CursorResult[Any],
            await session.execute(text("UPDATE notes SET body = body WHERE editor = 'bob'")),
        ).rowcount
        return GatherElevation(
            refused=refused,
            elevated=elevated_scopes(),
            flag=(await session.execute(text(FLAG))).scalar_one(),
            bob_rows_writable=touched,
        )


@dataclass(frozen=True)
class Composed:
    app: AppInvoker
    session_manager: SessionManager
    sink: object
    bypass: str
    schema: str

    async def bodies(self) -> tuple[str, ...]:
        """Every note's body, read through the bypass user."""
        engine = create_async_engine(self.bypass, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                query = text(_BODIES.format(schema=self.schema))
                return tuple((await connection.execute(query)).scalars())
        finally:
            await engine.dispose()

    async def next_checkout(self) -> OnConnection:
        """The pool's next connection, checked out without the session-settings provider."""
        async with self.session_manager.engine.connect() as connection:
            return await _on_connection(connection)


def _config(tmp_path: Path, schema: str) -> str:
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{schema}.yaml").read_text())
    document["app"]["discovery"]["modules"]["include"].append(__name__)
    document["stores"]["default"]["pool"] = {"size": 1, "max_overflow": 0}
    document["persistence"] = {"cursors": {"signing": "ephemeral"}}
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump(document))
    return str(path)


@pytest.fixture
def held() -> Iterator[list[OnConnection]]:
    _held.clear()
    yield _held
    _held.clear()


@pytest.fixture
async def composed(scoped_database: BootstrapFactory, tmp_path: Path) -> AsyncIterator[Composed]:
    """The composed application, as ``U1``, against a fresh seeded schema."""
    schema = f"notes_sink_{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    await seed_notes(database.bypass)
    built = build_application(
        _config(tmp_path, schema), adapter=FastAPIAdapter(), environment="test"
    )
    identity = set_identity(Identity(subject=U1))
    try:
        async with built.running():
            container = built.kernel.container
            yield Composed(
                app=cast(AppInvoker, built.kernel.app),
                session_manager=container.resolve(SessionManager),
                sink=built.kernel.executor._elevation_sink,
                bypass=database.bypass,
                schema=schema,
            )
    finally:
        reset_identity(identity)


def test_the_composition_wires_one_scoped_sink(composed: Composed) -> None:
    assert isinstance(composed.sink, ScopedElevationSink)


async def test_a_nested_elevation_is_cleared_on_the_connection_the_outer_execution_keeps(
    composed: Composed, held: list[OnConnection]
) -> None:
    edited, after = await composed.app.invoke(WriteThenNestEditor)

    assert (edited, held, after) == (
        1,
        [OnConnection(after.pid, "on")],
        OnConnection(after.pid, ""),
    )
    assert await composed.bodies() == ("a", "edited", "outer", "z")


async def test_elevate_from_a_gather_child_is_refused_and_elevates_nothing(
    composed: Composed,
) -> None:
    seen: GatherElevation = await composed.app.invoke(ElevateFromGatherChild)

    assert isinstance(seen.refused, ScopeOwnershipError)
    assert (seen.elevated, seen.flag, seen.bob_rows_writable) == (frozenset(), "", 0)


def _refused(message: str) -> Awaitable[object]:
    async def refuse() -> None:
        raise RuntimeError(message)

    return refuse()


class WriteThenNestEditorCatching(UseCase[notes.Note, str]):
    """Write, run :class:`EditBobElevated` nested, catch its failure and write again."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self) -> str:
        session = _active(self._session_manager)
        await session.execute(text(_INSERT_OUTER))
        try:
            await self._app.invoke(EditBobElevated)
        except RuntimeError as exc:
            caught = str(exc)
        else:
            caught = ""
        await _active(self._session_manager).execute(text(_INSERT_OUTER))
        return caught


@dataclass(frozen=True)
class CleanupFailure:
    """How a failed cleanup ends.

    Attributes:
        raised: What the outer invocation raises when nothing catches it.
        connection_kept: Whether the connection that held the flag goes back
            to the pool; an invalidated one is discarded.
    """

    raised: str
    connection_kept: bool


@pytest.fixture(params=["invalidation-succeeds", "invalidation-fails"])
def cleanup_fails(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> CleanupFailure:
    """Clearing the flags fails; invalidating the session after it succeeds, or fails too."""
    monkeypatch.setattr(
        SQLAlchemyStoreElevation, "clear_flags", lambda _, __: _refused("clearing refused")
    )
    if request.param == "invalidation-succeeds":
        return CleanupFailure("clearing refused", connection_kept=False)
    monkeypatch.setattr(AsyncSession, "invalidate", lambda _: _refused("invalidation refused"))
    return CleanupFailure("invalidation refused", connection_kept=True)


async def _no_flag_reaches_the_pool(
    composed: Composed, held: list[OnConnection], failure: CleanupFailure
) -> None:
    """The flag was on; nothing is persisted; the next checkout carries no flag."""
    (flagged,) = held
    assert flagged.flag == "on"
    assert await composed.bodies() == _SEEDED
    following = await composed.next_checkout()
    assert following.flag == ""
    assert (following.pid == flagged.pid) is failure.connection_kept


async def test_a_failed_cleanup_rolls_back_and_the_flag_never_reaches_the_next_checkout(
    composed: Composed, held: list[OnConnection], cleanup_fails: CleanupFailure
) -> None:
    with pytest.raises(RuntimeError, match=cleanup_fails.raised):
        await composed.app.invoke(WriteThenNestEditor)

    await _no_flag_reaches_the_pool(composed, held, cleanup_fails)


async def test_a_parent_catching_a_failed_cleanup_still_commits_nothing(
    composed: Composed, held: list[OnConnection], cleanup_fails: CleanupFailure
) -> None:
    with pytest.raises(RuntimeError, match="clearing refused"):
        await composed.app.invoke(WriteThenNestEditorCatching)

    await _no_flag_reaches_the_pool(composed, held, cleanup_fails)
