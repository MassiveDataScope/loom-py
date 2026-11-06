"""Two Postgres stores on two servers keep row-level security apart (T043, ADR 0008).

Each store is built directly through the SQLAlchemy backend from one catalog,
below the composition (``test_two_stores_composed.py`` goes through it): ``default`` on
server A and ``audit`` on server B, each serving the synthetic ``notes``
product from an external schema of its own. Every server holds different
rows, so a row read back names the server it came from.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import QueuePool

from loom.core.authz import Scope
from loom.core.authz.elevation import ElevationSink, elevated_scopes, elevation_scope
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.errors import UndeclaredStoreWrite
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.identity.identity import ANONYMOUS
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.elevation import (
    ElevableScopeInSeveralStores,
    ScopedElevationSink,
    StoreElevation,
)
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey, store_binding
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.rls import elevate
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, current_scope, invocation
from tests.helpers.persistence import build_backend
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import (
    AUDIT,
    DECISION,
    FLAG,
    OWNER_SETTING,
    U1,
    U2,
    ScopedDatabase,
    StoreFactory,
    execute,
    scalar,
    store_section,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("notes_wiring")]

T = TypeVar("T")

STORES = (DEFAULT_STORE, AUDIT)
OTHER = {DEFAULT_STORE: AUDIT, AUDIT: DEFAULT_STORE}
SEEDS: Mapping[StoreKey, Mapping[str, tuple[str, ...]]] = {
    DEFAULT_STORE: {U1: ("a1", "a2"), U2: ("a3",)},
    AUDIT: {U1: ("b1",), U2: ("b2", "b3")},
}
OWNER = text(OWNER_SETTING)
EDITOR_FLAG = text(FLAG)
BODIES = text("SELECT body FROM notes ORDER BY body")
BACKEND_PID = text("SELECT pg_backend_pid()")
PROBE = text("SELECT coalesce(current_setting('loom.probe', true), '')")


@dataclass(frozen=True)
class TwoServers:
    """The bootstrapped, created and seeded schema of each store, on its own server."""

    databases: Mapping[StoreKey, ScopedDatabase]

    def context(
        self,
        *,
        urls: Mapping[StoreKey, str] | None = None,
        unscoped: frozenset[StoreKey] = frozenset(),
    ) -> ConfigContext:
        """The configuration declaring both stores, each on its write user by default.

        Args:
            urls: A connection URL replacing a store's write user.
            unscoped: Stores declared without their external schema section.
        """
        urls = urls or {}
        return ConfigContext.from_dict(
            {
                "app": {"name": "two_stores"},
                "stores": {
                    key: _section(database, urls.get(key), external=key not in unscoped)
                    for key, database in self.databases.items()
                },
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        )

    async def row_counts(self) -> dict[StoreKey, int]:
        """Every store's row count, read through its bypass user."""
        return {
            key: await scalar(database.bypass, f"SELECT count(*) FROM {database.schema}.notes")
            for key, database in self.databases.items()
        }


def _section(database: ScopedDatabase, url: str | None, *, external: bool) -> dict[str, Any]:
    section = store_section(database, notes.SCOPE_BINDINGS, url=url)
    if not external:
        del section["schema"]
    return section


@pytest.fixture
async def two_servers(create_store: StoreFactory) -> TwoServers:
    suffix = secrets.token_hex(3)
    databases: dict[StoreKey, ScopedDatabase] = {}
    for key in STORES:
        schema = f"two_{key}_{suffix}"
        database = databases[key] = await create_store(key, notes, schema)
        rows = ", ".join(
            f"('{owner}', 'ana', '{body}')"
            for owner, bodies in SEEDS[key].items()
            for body in bodies
        )
        await execute(
            database.bypass, f"INSERT INTO {schema}.notes (owner_id, editor, body) VALUES {rows}"
        )
    return TwoServers(databases)


@dataclass(frozen=True)
class Store:
    """One store as its backend wires it: the wiring, its session manager and a repository."""

    key: StoreKey
    wiring: PersistenceWiring
    manager: SessionManager
    notes: RepositorySQLAlchemy
    elevation: StoreElevation | None

    def unit_of_work(self) -> UnitOfWork:
        factory = self.wiring.uow_factory
        if factory is None:
            raise RuntimeError(f"store {self.key!r} serves no unit of work")
        return factory.create()

    @property
    def sink(self) -> ElevationSink:
        """The composition's sink over this store alone, from the elevation its wiring exposes."""
        if self.elevation is None:
            raise RuntimeError(f"store {self.key!r} exposes no elevation")
        return ScopedElevationSink({self.key: self.elevation})

    def checked_out(self) -> int:
        pool = self.manager.engine.sync_engine.pool
        if not isinstance(pool, QueuePool):
            raise TypeError(f"store {self.key!r} has no queued pool")
        return pool.checkedout()


def _build(ctx: ConfigContext, key: StoreKey) -> Store:
    wiring = build_backend(SQLAlchemyBackend(), ctx, notes.MODELS, store=key)
    wiring.prepare_models(notes.MODELS)
    container = LoomContainer()
    wiring.repo_registration_module(container)
    manager = container.resolve(store_binding(SessionManager, key))
    repository = RepositorySQLAlchemy(
        manager, notes.Note, cursor_codec=container.resolve(CursorCodec)
    )
    return Store(
        key=key,
        wiring=wiring,
        manager=manager,
        notes=repository,
        elevation=wiring.elevation,
    )


@asynccontextmanager
async def _wired(ctx: ConfigContext) -> AsyncIterator[dict[StoreKey, Store]]:
    """Both stores built from one catalog, not started; their engines disposed after."""
    stores = {key: _build(ctx, key) for key in STORES}
    try:
        yield stores
    finally:
        for store in stores.values():
            await store.manager.dispose()


@asynccontextmanager
async def _started(ctx: ConfigContext) -> AsyncIterator[dict[StoreKey, Store]]:
    """Both stores past their startup checks and inside their lifespans."""
    async with _wired(ctx) as stores, AsyncExitStack() as lifespans:
        for store in stores.values():
            await store.wiring.startup_checks()
            await lifespans.enter_async_context(store.wiring.lifespan_init())
        yield stores


async def _as(subject: str | None, call: Callable[[], Awaitable[T]]) -> T:
    """Run *call* as *subject*, anonymous when ``None``."""
    if subject is None:
        return await call()
    token = set_identity(Identity(subject=subject))
    try:
        return await call()
    finally:
        reset_identity(token)


def _active(store: Store) -> AsyncSession:
    session = store.manager.active_session
    if session is None:
        raise RuntimeError("no unit-of-work session")
    return session


async def _bodies(session: AsyncSession) -> tuple[str, ...]:
    return tuple((await session.execute(BODIES)).scalars())


async def _visible(store: Store) -> tuple[int, tuple[str, ...]]:
    """The rows the caller sees in one unit of work: the repository's count, the session's rows."""
    async with store.unit_of_work():
        return await store.notes.count(), await _bodies(_active(store))


async def _seen_across(opened: Store, other: Store) -> tuple[str, str]:
    """The scope setting of *opened*'s unit of work, and of *other*'s connection meanwhile."""
    async with opened.unit_of_work():
        own = (await _active(opened).execute(OWNER)).scalar_one()
        async with other.manager.engine.connect() as raw:
            there = (await raw.execute(OWNER)).scalar_one()
    return own, there


async def _failing(store: Store) -> None:
    async with store.unit_of_work():
        await _active(store).execute(OWNER)
        await _active(store).execute(text("SELECT 1 / 0"))


async def _cancelled(store: Store) -> bool:
    """Cancel a unit of work in the current task, so its context variables are this task's."""
    try:
        async with asyncio.timeout(0.05), store.unit_of_work():
            await _active(store).execute(OWNER)
            await asyncio.sleep(10)
    except TimeoutError:
        return True
    return False


async def _residue(store: Store) -> str:
    async with store.manager.engine.connect() as raw:
        return str((await raw.execute(OWNER)).scalar_one())


async def _write_both(outer: Store, inner: Store) -> None:
    """Write through *outer*'s repository, then *inner*'s, inside *outer*'s unit of work."""
    async with outer.unit_of_work():
        await outer.notes.create(notes.Note(owner_id=U1, editor="ana", body="lost"))
        await inner.notes.create(notes.Note(owner_id=U1, editor="ana", body="lost"))


async def _force_row_security(database: ScopedDatabase, *, force: bool) -> None:
    """Force or unforce row-level security on the notes table, past the guard's event trigger."""
    schema = database.schema
    await execute(
        database.superuser,
        f"ALTER EVENT TRIGGER loom_guard_{schema}_ddl DISABLE",
        f"ALTER TABLE {schema}.notes {'' if force else 'NO '}FORCE ROW LEVEL SECURITY",
        f"ALTER EVENT TRIGGER loom_guard_{schema}_ddl ENABLE ALWAYS",
    )


def _expected(subject: str | None, key: StoreKey) -> tuple[int, tuple[str, ...]]:
    bodies = () if subject is None else SEEDS[key][subject]
    return len(bodies), bodies


async def test_no_tenant_sees_another_tenant_s_rows_through_either_store(
    two_servers: TwoServers,
) -> None:
    async with _started(two_servers.context()) as stores:
        seen = {
            (key, subject): await _as(subject, lambda store=store: _visible(store))
            for key, store in stores.items()
            for subject in (U1, U2, None)
        }

    assert seen == {
        (key, subject): _expected(subject, key) for key in STORES for subject in (U1, U2, None)
    }


@pytest.mark.parametrize("privileged", STORES)
async def test_a_bypass_role_is_refused_on_its_store_while_the_other_store_starts(
    two_servers: TwoServers, privileged: StoreKey
) -> None:
    healthy = OTHER[privileged]
    ctx = two_servers.context(urls={privileged: two_servers.databases[privileged].bypass})
    async with _wired(ctx) as stores:
        await stores[healthy].wiring.startup_checks()
        seen = await _as(U1, lambda: _visible(stores[healthy]))
        with pytest.raises(
            ConfigError, match=rf"stores\.{privileged}\.url connects as a superuser"
        ):
            await stores[privileged].wiring.startup_checks()

    assert seen == _expected(U1, healthy)


@pytest.mark.parametrize("broken", STORES)
async def test_the_guard_check_refuses_only_the_store_whose_table_lost_its_protection(
    two_servers: TwoServers, broken: StoreKey
) -> None:
    database = two_servers.databases[broken]
    schema = database.schema
    await _force_row_security(database, force=False)
    try:
        async with _wired(two_servers.context()) as stores:
            await stores[OTHER[broken]].wiring.startup_checks()
            with pytest.raises(
                ConfigError,
                match=rf"loom_guard_{schema} or the scoped tables differ"
                r".*table\.rls notes: not forced",
            ):
                await stores[broken].wiring.startup_checks()
    finally:
        # A table left unforced blocks every later bootstrap of the module's database.
        await _force_row_security(database, force=True)


@pytest.mark.parametrize("unscoped", STORES)
async def test_a_store_without_scope_settings_fails_closed_on_its_own(
    two_servers: TwoServers, unscoped: StoreKey
) -> None:
    async with _wired(two_servers.context(unscoped=frozenset({unscoped}))) as stores:
        await stores[OTHER[unscoped]].wiring.startup_checks()
        with pytest.raises(
            ConfigError, match=rf"cannot protect scoped tables .*stores\.{unscoped}\.schema\.mode"
        ):
            await stores[unscoped].wiring.startup_checks()
        bare = stores[unscoped].manager

        async def read() -> tuple[str, ...]:
            async with bare.session() as session:
                return await _bodies(session)

        seen = await _as(U1, read)

    assert bare.has_session_settings is False
    assert seen == ()


async def test_settings_pools_and_context_never_cross_the_two_servers(
    two_servers: TwoServers,
) -> None:
    async with _started(two_servers.context()) as stores:
        names = {key: store.manager.engine.url.database for key, store in stores.items()}
        foreign = {
            key: await scalar(
                two_servers.databases[key].write,
                "SELECT count(*) FROM pg_database WHERE datname = :name",
                name=names[OTHER[key]],
            )
            for key in STORES
        }
        across = {
            key: await _as(U1, lambda store=store: _seen_across(store, stores[OTHER[store.key]]))
            for key, store in stores.items()
        }
        for store in stores.values():
            await _as(U2, lambda store=store: _visible(store))
            with pytest.raises(DBAPIError, match="division by zero"):
                await _as(U2, lambda store=store: _failing(store))
        cancelled = {
            key: await _as(U1, lambda store=store: _cancelled(store))
            for key, store in stores.items()
        }
        checked_out = {key: store.checked_out() for key, store in stores.items()}
        residue = {key: await _residue(store) for key, store in stores.items()}
        context = (current_scope(), current_identity())

    assert names[DEFAULT_STORE] != names[AUDIT]
    assert foreign == {DEFAULT_STORE: 0, AUDIT: 0}
    assert across == {DEFAULT_STORE: (U1, ""), AUDIT: (U1, "")}
    assert cancelled == {DEFAULT_STORE: True, AUDIT: True}
    assert checked_out == {DEFAULT_STORE: 0, AUDIT: 0}
    assert residue == {DEFAULT_STORE: "", AUDIT: ""}
    assert context == (None, ANONYMOUS)
    assert elevated_scopes() == frozenset()


async def test_concurrent_invocations_with_different_identities_share_no_settings_or_sessions(
    two_servers: TwoServers,
) -> None:
    subjects = (U1, U2, U1, U2)
    barrier = asyncio.Barrier(len(subjects))

    async with _started(two_servers.context()) as stores:

        async def snapshot(session: AsyncSession) -> tuple[str, tuple[str, ...], int]:
            owner = (await session.execute(OWNER)).scalar_one()
            pid = (await session.execute(BACKEND_PID)).scalar_one()
            return owner, await _bodies(session), pid

        async def invocation() -> tuple[dict[StoreKey, Any], dict[StoreKey, Any]]:
            async with (
                stores[DEFAULT_STORE].manager.session() as a,
                stores[AUDIT].manager.session() as b,
            ):
                sessions = {DEFAULT_STORE: a, AUDIT: b}
                first = {key: await snapshot(session) for key, session in sessions.items()}
                await barrier.wait()
                second = {key: await snapshot(session) for key, session in sessions.items()}
            return first, second

        results = await asyncio.gather(*(_as(subject, invocation) for subject in subjects))

    observed = [
        {key: (owner, bodies) for key, (owner, bodies, _) in second.items()}
        for _, second in results
    ]
    pids = {key: {first[key][2] for first, _ in results} for key in STORES}
    assert observed == [
        {key: (subject, SEEDS[key][subject]) for key in STORES} for subject in subjects
    ]
    assert [first for first, _ in results] == [second for _, second in results]
    assert {key: len(found) for key, found in pids.items()} == {
        DEFAULT_STORE: len(subjects),
        AUDIT: len(subjects),
    }


@pytest.mark.parametrize(("outer", "inner"), [(DEFAULT_STORE, AUDIT), (AUDIT, DEFAULT_STORE)])
async def test_a_repository_of_one_store_refuses_the_other_store_s_unit_of_work(
    two_servers: TwoServers, outer: StoreKey, inner: StoreKey
) -> None:
    before = await two_servers.row_counts()
    async with _started(two_servers.context()) as stores:
        with pytest.raises(
            UndeclaredStoreWrite,
            match=rf"a write to store '{inner}' .* declares only store '{outer}'; ",
        ):
            await _as(U1, lambda: _write_both(stores[outer], stores[inner]))

    assert await two_servers.row_counts() == before
    assert before == {key: sum(map(len, SEEDS[key].values())) for key in STORES}


def _both(stores: Mapping[StoreKey, Store]) -> OrderedCommits:
    """One coordinator over both stores, as the composition builds it."""
    factories = {key: store.wiring.uow_factory for key, store in stores.items()}
    return OrderedCommits({key: f for key, f in factories.items() if f is not None})


async def _flags_after_elevating(stores: Mapping[StoreKey, Store], elevated: StoreKey) -> Any:
    """Elevate ``editor`` with *elevated*'s wired sink once both stores' transactions began.

    Both transactions are open before ``elevate()``, so the flag can reach
    them only through the sink, never through the session-settings provider.
    """
    async with (
        invocation(_both(stores)) as scope,
        scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
    ):
        for store in stores.values():
            await _active(store).execute(OWNER)
        async with elevation_scope(owns_transaction=True, sink=stores[elevated].sink):
            await elevate("editor", DECISION, at=Scope.of(U1))
            return {
                key: (await _active(store).execute(EDITOR_FLAG)).scalar_one()
                for key, store in stores.items()
            }


@pytest.mark.parametrize("elevated", STORES)
async def test_an_elevation_flag_is_set_on_its_own_store_s_session_only(
    two_servers: TwoServers, elevated: StoreKey
) -> None:
    async with _started(two_servers.context()) as stores:
        flags = await _as(U1, lambda: _flags_after_elevating(stores, elevated))
        residue = {key: await _residue(store) for key, store in stores.items()}

    assert flags == {elevated: "on", OTHER[elevated]: ""}
    assert residue == {DEFAULT_STORE: "", AUDIT: ""}


async def _provider_flag_after_elevating(
    stores: Mapping[StoreKey, Store], elevated: StoreKey
) -> str:
    """Elevate ``editor`` before *elevated*'s transaction begins; read its flag.

    No transaction is open at ``elevate()``, so the sink sets nothing and the
    flag can only come from the session-settings provider when it begins.
    """
    async with (
        invocation(_both(stores)) as scope,
        scope.frame(Frame(elevated)),
        elevation_scope(owns_transaction=True, sink=stores[elevated].sink),
    ):
        await elevate("editor", DECISION, at=Scope.of(U1))
        return str((await _active(stores[elevated]).execute(EDITOR_FLAG)).scalar_one())


@pytest.mark.parametrize("elevated", STORES)
async def test_an_elevation_before_the_transaction_arrives_through_the_provider(
    two_servers: TwoServers, elevated: StoreKey
) -> None:
    async with _started(two_servers.context()) as stores:
        flag = await _as(U1, lambda: _provider_flag_after_elevating(stores, elevated))

    assert flag == "on"


async def test_the_same_elevable_scope_in_both_stores_is_refused_at_startup(
    two_servers: TwoServers,
) -> None:
    async with _wired(two_servers.context()) as stores:
        elevations = {k: s.elevation for k, s in stores.items() if s.elevation is not None}
        assert elevations.keys() == set(STORES)
        with pytest.raises(ElevableScopeInSeveralStores, match="stores audit, default"):
            ScopedElevationSink(elevations)


async def _session_setting_left_behind(store: Store) -> tuple[int, str]:
    """Leave a session-level setting on the unit of work's connection; its pid and the value."""
    await store.manager.engine.dispose()
    async with store.unit_of_work():
        session = _active(store)
        await session.execute(text("SET loom.probe = 'left'"))
        pid = (await session.execute(BACKEND_PID)).scalar_one()
        value = (await session.execute(PROBE)).scalar_one()
    return pid, value


async def _back_from_the_pool(store: Store) -> tuple[int, str]:
    """The pid and the probe setting of the connection the pool hands out next."""
    async with store.manager.engine.connect() as raw:
        pid = (await raw.execute(BACKEND_PID)).scalar_one()
        return pid, str((await raw.execute(PROBE)).scalar_one())


@pytest.fixture
async def pool_round_trip(two_servers: TwoServers) -> dict[StoreKey, tuple[tuple[int, str], ...]]:
    """Per store: the unit of work's (pid, probe), then the next pooled connection's."""
    async with _started(two_servers.context()) as stores:
        trips: dict[StoreKey, tuple[tuple[int, str], ...]] = {}
        for key, store in stores.items():
            left = await _as(U1, lambda store=store: _session_setting_left_behind(store))
            trips[key] = (left, await _back_from_the_pool(store))
    return trips


@pytest.mark.parametrize("key", STORES)
def test_the_pool_hands_the_same_connection_back(
    pool_round_trip: dict[StoreKey, tuple[tuple[int, str], ...]], key: StoreKey
) -> None:
    (left_pid, _), (back_pid, _) = pool_round_trip[key]

    assert back_pid == left_pid


@pytest.mark.parametrize("key", STORES)
def test_the_setting_was_set_on_that_connection(
    pool_round_trip: dict[StoreKey, tuple[tuple[int, str], ...]], key: StoreKey
) -> None:
    (_, left), _ = pool_round_trip[key]

    assert left == "left"


@pytest.mark.parametrize("key", STORES)
def test_a_session_level_setting_is_reset_when_the_connection_returns(
    pool_round_trip: dict[StoreKey, tuple[tuple[int, str], ...]], key: StoreKey
) -> None:
    _, (_, back) = pool_round_trip[key]

    assert back == ""
