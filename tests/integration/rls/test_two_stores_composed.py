"""Two Postgres stores with row-level security, composed as one application (T066, ADR 0008).

``default`` serves ``notes`` on server A and ``audit`` serves ``memos`` on
server B, each from a schema of its own: B's is created and verified through
``load_application(store="audit")``. Use cases run through the composed
application as a caller, so every session they touch, in either store, must
carry that caller's identity and its own store's settings.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.composition import AppConfig, Application, CompositionSource, Recipe, compose_application
from loom.core.authz import Decision, Grant, Scope
from loom.core.command import Command
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope as DiScope
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.post_commit import store_channel
from loom.core.identity import Identity
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import RepoFor
from loom.core.repository.sqlalchemy.rls import create_schema, elevate
from loom.core.repository.sqlalchemy.rls.verify import verify
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.coordinator import Committed, RolledBack
from loom.core.uow.scope import ScopeOwnershipError, StoresNotCommitted
from loom.core.use_case import Input, Joined, UseCase
from loom.core.use_case.invoker import ApplicationInvoker
from tests.helpers.composition import composition_source
from tests.integration.agnosticism import memos, notes
from tests.integration.rls.conftest import (
    U1,
    U2,
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]

AUDIT = StoreKey("audit")
NOTES = {U1: ("a1", "a2"), U2: ("a3",)}
MEMOS = {U1: ("b1",), U2: ("b2", "b3")}
OWNER = text("SELECT coalesce(current_setting('loom.scope.owner', true), '')")
EDITOR_FLAG = text("SELECT coalesce(current_setting('loom.scope.editor.any', true), '')")
DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
REFUSED = "fail-at-commit"


class _NewNote(Command, frozen=True):
    owner_id: str
    editor: str
    body: str


class _NewMemo(Command, frozen=True):
    owner_id: str
    body: str


@dataclass
class _Sessions:
    """Both stores' session managers, and the stores whose post-commit actions ran."""

    default: SessionManager
    audit: SessionManager
    ran: list[StoreKey] = field(default_factory=list)

    def of(self, key: StoreKey) -> AsyncSession:
        manager = self.default if key == DEFAULT_STORE else self.audit
        session = manager.active_session
        assert session is not None, f"store {key!r} is not enlisted"
        return session

    async def owners(self) -> dict[StoreKey, str]:
        return {key: str((await self.of(key).execute(OWNER)).scalar_one()) for key in _STORES}


_STORES = (DEFAULT_STORE, AUDIT)


class _ReadBoth(UseCase[notes.Note, tuple[int, int]]):
    read_only = True

    def __init__(self, notes_repo: RepoFor[notes.Note], memos_repo: RepoFor[memos.Memo]) -> None:
        self.notes = notes_repo
        self.memos = memos_repo

    async def execute(self) -> tuple[int, int]:
        return await self.notes.count(), await self.memos.count()


class _GatherBoth(_ReadBoth):
    async def execute(self) -> tuple[int, int]:
        counted = await asyncio.gather(self.notes.count(), self.memos.count())
        return counted[0], counted[1]


class _GatherWhileWriting(UseCase[notes.Note, None]):
    def __init__(
        self, notes_repo: RepoFor[notes.Note], memos_repo: Joined[RepoFor[memos.Memo]]
    ) -> None:
        self.notes = notes_repo
        self.memos = memos_repo

    async def execute(self) -> None:
        await self.notes.create(_NewNote(owner_id=U1, editor="ana", body="gathered"))
        await asyncio.gather(self.notes.count(), self.memos.count())


class _RecordMemo(UseCase[memos.Memo, str]):
    """Primary store ``audit``: writes a memo and reports the owner setting of its session."""

    def __init__(self, memos_repo: RepoFor[memos.Memo], sessions: _Sessions) -> None:
        self.memos = memos_repo
        self.sessions = sessions

    async def execute(self, cmd: _NewMemo = Input()) -> str:
        await self.memos.create(cmd)
        return str((await self.sessions.of(AUDIT).execute(OWNER)).scalar_one())


class _NoteThenNestedMemo(UseCase[notes.Note, tuple[str, str]]):
    def __init__(
        self,
        notes_repo: RepoFor[notes.Note],
        sessions: _Sessions,
        invoker: ApplicationInvoker,
    ) -> None:
        self.notes = notes_repo
        self.sessions = sessions
        self.invoker = invoker

    async def execute(self) -> tuple[str, str]:
        await self.notes.create(_NewNote(owner_id=U1, editor="ana", body="outer"))
        nested = await self.invoker.invoke(_RecordMemo, payload={"owner_id": U1, "body": "nested"})
        own = str((await self.sessions.of(DEFAULT_STORE).execute(OWNER)).scalar_one())
        return own, nested


class _ElevateEditor(UseCase[notes.Note, dict[StoreKey, str]]):
    """Begins both stores' transactions, elevates ``editor`` and reads each store's flag."""

    def __init__(self, memos_repo: Joined[RepoFor[memos.Memo]], sessions: _Sessions) -> None:
        self.memos = memos_repo
        self.sessions = sessions

    async def execute(self) -> dict[StoreKey, str]:
        await self.sessions.owners()
        await elevate("editor", DECISION, at=Scope.of(U1))
        return {
            key: str((await self.sessions.of(key).execute(EDITOR_FLAG)).scalar_one())
            for key in _STORES
        }


class _SplitCommit(UseCase[notes.Note, None]):
    """Writes ``default`` first, then a memo ``audit`` refuses when it commits."""

    def __init__(
        self,
        notes_repo: RepoFor[notes.Note],
        memos_repo: Joined[RepoFor[memos.Memo]],
        sessions: _Sessions,
    ) -> None:
        self.notes = notes_repo
        self.memos = memos_repo
        self.sessions = sessions

    async def execute(self) -> None:
        await self.notes.create(_NewNote(owner_id=U1, editor="ana", body="split"))
        await self.memos.create(_NewMemo(owner_id=U1, body=REFUSED))
        for key in _STORES:
            channel = store_channel(key)
            assert channel is not None
            channel.enqueue(lambda key=key: self.sessions.ran.append(key))


_USE_CASES = (
    _ReadBoth,
    _GatherBoth,
    _GatherWhileWriting,
    _RecordMemo,
    _NoteThenNestedMemo,
    _ElevateEditor,
    _SplitCommit,
)


class _Serving:
    def __init__(self) -> None:
        self.discovered = DiscoveryResult(
            models=(notes.Note, memos.Memo), use_cases=cast(Any, _USE_CASES), interfaces=()
        )

    def recipe(self, source: CompositionSource) -> Recipe:
        del source
        return Recipe(profile=SERVER_PROFILE, discovered=self.discovered)

    def bind(self, application: Application) -> Application:
        return application


def _sessions_module(container: LoomContainer) -> None:
    def build() -> _Sessions:
        audit = container.resolve((SessionManager, AUDIT))
        return _Sessions(container.resolve(SessionManager), audit)

    container.register(_Sessions, build, scope=DiScope.APPLICATION)


@dataclass(frozen=True)
class TwoStores:
    """Each store's bootstrapped, created and seeded schema, on its own server."""

    default: ScopedDatabase
    audit: ScopedDatabase

    def compose(self) -> Application:
        stores = {
            DEFAULT_STORE: _section(self.default, notes.SCOPE_BINDINGS),
            AUDIT: _section(self.audit, memos.SCOPE_BINDINGS),
        }
        ctx = ConfigContext.from_dict(
            {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
        )
        source = composition_source(ctx, app=AppConfig(name="two_stores_composed"))
        return compose_application(source, _Serving(), modules=(_sessions_module,))

    async def rows(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Every note body on A and memo body on B, read through the bypass users."""
        return (
            await _bodies(self.default, "notes"),
            await _bodies(self.audit, "memos"),
        )


def _section(database: ScopedDatabase, scopes: dict[str, str]) -> dict[str, Any]:
    return {
        "backend": "sqlalchemy",
        "url": database.write,
        "schema": {
            "mode": "external",
            "scopes": dict(scopes),
            "guard": f"loom_guard_{database.schema}",
        },
    }


async def _bodies(database: ScopedDatabase, table: str) -> tuple[str, ...]:
    rows = await scalar(
        database.bypass,
        f"SELECT coalesce(string_agg(body, ',' ORDER BY body), '') FROM {database.schema}.{table}",
    )
    return tuple(rows.split(",")) if rows else ()


async def _seed(database: ScopedDatabase, table: str, rows: str) -> None:
    await execute(database.bypass, f"INSERT INTO {database.schema}.{table} {rows}")


@pytest.fixture
async def two_stores(
    notes_wiring: None,
    scoped_database: BootstrapFactory,
    scoped_database_b: BootstrapFactory,
    tmp_path: Path,
) -> TwoStores:
    suffix = secrets.token_hex(3)
    default = await scoped_database(f"cn_{suffix}")
    await create_schema(
        default.migrator, application_for(notes, default, tmp_path, schema=default.schema)
    )
    audit = await scoped_database_b(f"cm_{suffix}")
    audit_app = application_for(memos, audit, tmp_path, schema=audit.schema, store=AUDIT)
    await create_schema(audit.migrator, audit_app)
    assert (await verify(audit.superuser, audit_app)).ok
    await _seed(
        default,
        "notes",
        "(owner_id, editor, body) VALUES "
        + ", ".join(f"('{o}', 'ana', '{b}')" for o, bodies in NOTES.items() for b in bodies),
    )
    await _seed(
        audit,
        "memos",
        "(owner_id, body) VALUES "
        + ", ".join(f"('{o}', '{b}')" for o, bodies in MEMOS.items() for b in bodies),
    )
    return TwoStores(default, audit)


@pytest.fixture
async def application(two_stores: TwoStores) -> AsyncIterator[Application]:
    application = two_stores.compose()
    async with application.running():
        yield application


def _as(application: Application, subject: str) -> Any:
    return application.kernel.app.for_identity(Identity(subject=subject))


@pytest.mark.parametrize("use_case", [_ReadBoth, _GatherBoth], ids=["sequential", "gather"])
@pytest.mark.parametrize("subject", [U1, U2])
async def test_a_read_only_execution_reads_each_store_as_its_caller(
    application: Application, use_case: type[_ReadBoth], subject: str
) -> None:
    counts = await _as(application, subject).invoke(use_case)

    assert counts == (len(NOTES[subject]), len(MEMOS[subject]))


async def test_a_gather_child_may_not_touch_the_stores_of_a_writing_execution(
    application: Application, two_stores: TwoStores
) -> None:
    caller = _as(application, U1)

    with pytest.raises(ScopeOwnershipError):
        await caller.invoke(_GatherWhileWriting)

    assert await two_stores.rows() == (("a1", "a2", "a3"), ("b1", "b2", "b3"))


async def test_a_nested_execution_on_the_other_store_keeps_the_caller_and_commits_on_b_only(
    application: Application, two_stores: TwoStores
) -> None:
    owners = await _as(application, U1).invoke(_NoteThenNestedMemo)
    notes_on_a, memos_on_b = await two_stores.rows()
    memos_on_a = await scalar(
        two_stores.default.bypass, f"SELECT to_regclass('{two_stores.default.schema}.memos')"
    )

    assert owners == (U1, U1)
    assert (notes_on_a, memos_on_b) == (("a1", "a2", "a3", "outer"), ("b1", "b2", "b3", "nested"))
    assert memos_on_a is None


async def test_an_elevation_flags_only_the_store_that_declares_the_scope(
    application: Application,
) -> None:
    flags = await _as(application, U1).invoke(_ElevateEditor)

    assert flags == {DEFAULT_STORE: "on", AUDIT: ""}


@pytest.fixture
async def audit_refuses_at_commit(two_stores: TwoStores) -> None:
    """A deferred constraint on B's memos that fails the commit of a ``fail-at-commit`` memo."""
    schema = two_stores.audit.schema
    await execute(
        two_stores.audit.superuser,
        f"ALTER EVENT TRIGGER loom_guard_{schema}_ddl DISABLE",
        "CREATE OR REPLACE FUNCTION public.loom_it_refuse_at_commit() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        f"IF NEW.body = '{REFUSED}' THEN "
        "RAISE EXCEPTION 'refused at commit' USING ERRCODE = '23514'; END IF; "
        "RETURN NULL; END $$",
        f"CREATE CONSTRAINT TRIGGER loom_it_refuse_at_commit AFTER INSERT ON {schema}.memos "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        "EXECUTE FUNCTION public.loom_it_refuse_at_commit()",
        f"ALTER EVENT TRIGGER loom_guard_{schema}_ddl ENABLE ALWAYS",
    )


async def test_a_split_commit_raises_and_runs_only_the_committed_store_s_actions(
    audit_refuses_at_commit: None, application: Application, two_stores: TwoStores
) -> None:
    sessions = application.kernel.container.resolve(_Sessions)
    caller = _as(application, U1)

    with pytest.raises(StoresNotCommitted) as raised:
        await caller.invoke(_SplitCommit)

    outcome = raised.value.outcome
    assert outcome is not None
    assert [type(store) for store in outcome.stores] == [Committed, RolledBack]
    assert [store.key for store in outcome.stores] == [DEFAULT_STORE, AUDIT]
    assert await two_stores.rows() == (("a1", "a2", "a3", "split"), ("b1", "b2", "b3"))
    assert sessions.ran == [DEFAULT_STORE]
