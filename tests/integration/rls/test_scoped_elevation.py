"""The composition's one elevation sink against real Postgres (US7 PR 5).

Through ``Project.compose`` on a pool of one connection, so the next
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

from loom.composition import Project
from loom.core.authz.elevation import ElevationSink
from loom.core.errors import ScopeOwnershipError
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.stores.elevation import ScopedElevationSink
from loom.core.use_case.invoker import AppInvoker
from loom.rest.fastapi.runtime import FastAPIAdapter
from loom.stores.sqlalchemy.rls import SQLAlchemyStoreElevation, create_schema
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import notes
from tests.integration.rls.apps.scoped_elevation import (
    FLAGS,
    ElevateFromGatherChild,
    WriteAndNest,
)
from tests.integration.rls.conftest import (
    FLAG,
    U1,
    BootstrapFactory,
    application_for,
    scalar,
    seed_notes,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("notes_wiring")]

_SEEDED = "a,b,z"


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
    document["app"]["packages"].append("tests.integration.rls.apps.scoped_elevation")
    document["stores"]["default"]["pool"] = {"size": 1, "max_overflow": 0}
    document["stores"]["settings"] = {"cursors": {"signing": "ephemeral"}}
    (tmp_path / "app.yaml").write_text(yaml.safe_dump(document))
    built = Project.load(str(tmp_path / "app.yaml"), environment="test").compose(FastAPIAdapter())
    FLAGS.clear()
    identity = set_identity(Identity(subject=U1))
    try:
        async with built.running():
            yield Composed(
                app=cast(AppInvoker, built.kernel.invoker),
                manager=built.kernel.container.resolve(SessionManager),
                sink=built.kernel.container.resolve(ElevationSink),
                bodies_sql=f"SELECT string_agg(body, ',' ORDER BY body) FROM {schema}.notes",
                bypass=database.bypass,
            )
    finally:
        reset_identity(identity)


def _refused(message: str) -> Awaitable[object]:
    async def refuse() -> None:
        raise RuntimeError(message)

    return refuse()


class TestScopedElevation:
    def test_the_composition_wires_one_scoped_sink(self, composed: Composed) -> None:
        assert isinstance(composed.sink, ScopedElevationSink)

    async def test_elevate_from_a_gather_child_is_refused_and_elevates_nothing(
        self,
        composed: Composed,
    ) -> None:
        refused, elevated, flag = await composed.app.invoke(ElevateFromGatherChild)

        assert isinstance(refused, ScopeOwnershipError)
        assert (elevated, flag) == (frozenset(), "")

    @pytest.mark.parametrize("catch", [False, True], ids=["uncaught", "caught"])
    @pytest.mark.parametrize("invalidation_fails", [False, True], ids=["invalidated", "kept"])
    async def test_a_failed_clearing_commits_nothing_and_leaves_no_flag(
        self,
        composed: Composed,
        monkeypatch: pytest.MonkeyPatch,
        catch: bool,
        invalidation_fails: bool,
    ) -> None:
        monkeypatch.setattr(
            SQLAlchemyStoreElevation, "clear_flags", lambda *_: _refused("clearing")
        )
        if invalidation_fails:
            monkeypatch.setattr(AsyncSession, "invalidate", lambda _: _refused("invalidation"))
        raised = "invalidation" if invalidation_fails and not catch else "clearing"

        with pytest.raises(RuntimeError, match=raised):
            await composed.app.invoke(WriteAndNest, params={"catch": catch, "timeout_s": None})

        assert FLAGS == ["on"]
        assert await composed.outcome() == (_SEEDED, "")

    async def test_a_timeout_once_the_flag_was_sent_commits_nothing_even_when_caught(
        self, composed: Composed, monkeypatch: pytest.MonkeyPatch
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
