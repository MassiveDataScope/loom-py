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
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.composition import AppConfig, Application, compose_application
from loom.core.authz import Scope
from loom.core.command import Command
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope as DiScope
from loom.core.engine.post_commit import store_channel
from loom.core.errors import ScopeOwnershipError, StoresNotCommitted, UndeclaredStoreWrite
from loom.core.identity import Identity
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey, store_binding
from loom.core.repository.abc import RepoFor
from loom.core.repository.sqlalchemy.rls import elevate
from loom.core.repository.sqlalchemy.rls.verify import verify
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.coordinator import Committed, RolledBack
from loom.core.use_case import Input, Joined, UseCase
from loom.core.use_case.invoker import ApplicationInvoker
from tests.helpers.composition import Serving, composition_source
from tests.integration.agnosticism import memos, notes
from tests.integration.rls.conftest import (
    AUDIT,
    DECISION,
    FLAG,
    OWNER_SETTING,
    U1,
    U2,
    ScopedDatabase,
    StoreFactory,
    application_for,
    execute,
    scalar,
    store_section,
)

pytestmark = [pytest.mark.integration, pytest.mark.timeout(120)]

T1 = "t1000000-0000-0000-0000-000000000001"
T2 = "t2000000-0000-0000-0000-000000000002"
TENANT = {U1: T1, U2: T2}
"""Each caller's ``tenant``, which memos are scoped by, unlike notes (``subject``)."""
NOTES = {U1: ("a1", "a2"), U2: ("a3",)}
MEMOS = {T1: ("b1",), T2: ("b2", "b3")}
OWNER = text(OWNER_SETTING)
EDITOR_FLAG = text(FLAG)
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

    managers: Mapping[StoreKey, SessionManager]
    ran: list[StoreKey] = field(default_factory=list)

    def of(self, key: StoreKey) -> AsyncSession:
        session = self.managers[key].active_session
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
        nested = await self.invoker.invoke(_RecordMemo, payload={"owner_id": T1, "body": "nested"})
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
        await self.memos.create(_NewMemo(owner_id=T1, body=REFUSED))
        for key in _STORES:
            channel = store_channel(key)
            assert channel is not None
            channel.enqueue(lambda key=key: self.sessions.ran.append(key))


class _WriteInRead(UseCase[notes.Note, None]):
    """Read-only, yet writes a note on A or a memo on B."""

    read_only = True

    def __init__(
        self, notes_repo: RepoFor[notes.Note], memos_repo: Joined[RepoFor[memos.Memo]]
    ) -> None:
        self.notes = notes_repo
        self.memos = memos_repo

    async def execute(self, store: str) -> None:
        if store == DEFAULT_STORE:
            await self.notes.create(_NewNote(owner_id=U1, editor="ana", body="read"))
        else:
            await self.memos.create(_NewMemo(owner_id=T1, body="read"))


_USE_CASES = (
    _ReadBoth,
    _GatherBoth,
    _GatherWhileWriting,
    _RecordMemo,
    _NoteThenNestedMemo,
    _ElevateEditor,
    _SplitCommit,
    _WriteInRead,
)


def _sessions_module(container: LoomContainer) -> None:
    def build() -> _Sessions:
        return _Sessions({k: container.resolve(store_binding(SessionManager, k)) for k in _STORES})

    container.register(_Sessions, build, scope=DiScope.APPLICATION)


@dataclass(frozen=True)
class TwoStores:
    """Each store's bootstrapped, created and seeded schema, on its own server."""

    default: ScopedDatabase
    audit: ScopedDatabase

    def compose(self) -> Application:
        stores = {
            DEFAULT_STORE: store_section(self.default, notes.SCOPE_BINDINGS),
            AUDIT: store_section(self.audit, memos.SCOPE_BINDINGS),
        }
        ctx = ConfigContext.from_dict(
            {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
        )
        source = composition_source(ctx, app=AppConfig(name="two_stores_composed"))
        adapter = Serving(notes.Note, memos.Memo, use_cases=_USE_CASES)
        return compose_application(source, adapter, modules=(_sessions_module,))

    async def rows(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Every note body on A and memo body on B, read through the bypass users."""
        return (
            await _bodies(self.default, "notes"),
            await _bodies(self.audit, "memos"),
        )


async def _bodies(database: ScopedDatabase, table: str) -> tuple[str, ...]:
    rows = await scalar(
        database.bypass,
        f"SELECT coalesce(string_agg(body, ',' ORDER BY body), '') FROM {database.schema}.{table}",
    )
    return tuple(rows.split(",")) if rows else ()


@pytest.fixture
async def two_stores(notes_wiring: None, create_store: StoreFactory, tmp_path: Path) -> TwoStores:
    suffix = secrets.token_hex(3)
    default = await create_store(DEFAULT_STORE, notes, f"cn_{suffix}")
    audit = await create_store(AUDIT, memos, f"cm_{suffix}")
    audit_app = application_for(memos, audit, tmp_path, schema=audit.schema, store=AUDIT)
    assert (await verify(audit.superuser, audit_app)).ok
    note_rows = ", ".join(f"('{o}', 'ana', '{b}')" for o, bs in NOTES.items() for b in bs)
    memo_rows = ", ".join(f"('{o}', '{b}')" for o, bs in MEMOS.items() for b in bs)
    await execute(
        default.bypass,
        f"INSERT INTO {default.schema}.notes (owner_id, editor, body) VALUES {note_rows}",
    )
    await execute(
        audit.bypass, f"INSERT INTO {audit.schema}.memos (owner_id, body) VALUES {memo_rows}"
    )
    return TwoStores(default, audit)


@pytest.fixture
async def application(two_stores: TwoStores) -> AsyncIterator[Application]:
    application = two_stores.compose()
    async with application.running():
        yield application


def _as(application: Application, subject: str) -> Any:
    identity = Identity(subject=subject, attributes={"tenant": TENANT[subject]})
    return application.kernel.app.for_identity(identity)


@pytest.mark.parametrize("use_case", [_ReadBoth, _GatherBoth], ids=["sequential", "gather"])
@pytest.mark.parametrize("subject", [U1, U2])
async def test_a_read_only_execution_reads_each_store_as_its_caller(
    application: Application, use_case: type[_ReadBoth], subject: str
) -> None:
    counts = await _as(application, subject).invoke(use_case)

    assert counts == (len(NOTES[subject]), len(MEMOS[TENANT[subject]]))


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

    assert owners == (U1, T1)
    assert (notes_on_a, memos_on_b) == (("a1", "a2", "a3", "outer"), ("b1", "b2", "b3", "nested"))
    assert memos_on_a is None


@pytest.mark.parametrize("store", [DEFAULT_STORE, AUDIT])
async def test_a_write_in_a_read_only_execution_is_refused_on_either_store(
    application: Application, two_stores: TwoStores, store: StoreKey
) -> None:
    caller = _as(application, U1)

    with pytest.raises(UndeclaredStoreWrite, match="is read-only"):
        await caller.invoke(_WriteInRead, params={"store": store})

    assert await two_stores.rows() == (("a1", "a2", "a3"), ("b1", "b2", "b3"))


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
