"""Regressions for how identity and elevation reach row-level security.

- H1: a nested execution started after its parent elevated ``editor`` is
  refused, so it never runs with the elevation flag on the shared transaction.
- H2: the row-level security settings follow the identity the executor
  received (``Caller()``), not the ambient ``current_identity()``; a nested
  execution that would join the parent's transaction as another identity is
  refused.
- H3: an inline job run or dispatched by an execution bound to identity X
  runs as X, not as the ambient (anonymous) identity.

The use cases run through the real composition (``build_application``) against
real Postgres with ``schema.mode: external``.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.composition import build_application
from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.product import clear_authz_product, register_authz_product
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.repository.sqlalchemy.rls import create_schema, elevate, register_scope_source
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources, resolve_binding
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.scope import NestedElevation, NestedIdentityMismatch
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
ANA = Identity(subject=U1)
ZOE = Identity(subject=U2)
MANAGE = Permission("members.manage")
CATALOG = RoleCatalog((MANAGE,), (Role("admin", (MANAGE,)),))
DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))

_FLAG = "SELECT coalesce(current_setting('loom.scope.editor.any', true), '')"
_OWNER = "SELECT coalesce(current_setting('loom.scope.owner', true), '')"
_BODIES = "SELECT body FROM notes ORDER BY body"
_TOUCH_BOB = "UPDATE notes SET body = body WHERE editor = 'bob'"


@dataclass
class Product:
    catalog: RoleCatalog = CATALOG
    delegate: Permission = MANAGE
    elevations: dict[str, Permission] = field(default_factory=lambda: {"editor": MANAGE})


@dataclass(frozen=True)
class Seen:
    """What one execution saw on its transaction."""

    caller: str | None
    owner_setting: str
    editor_any: str
    bodies: tuple[str, ...]
    bob_rows_writable: int


async def _observe(session_manager: SessionManager, caller: Identity | None) -> Seen:
    session = session_manager.active_session
    if session is None:
        raise RuntimeError("no unit-of-work session")
    touched = cast(CursorResult[Any], await session.execute(text(_TOUCH_BOB))).rowcount
    return Seen(
        caller=None if caller is None else caller.subject,
        owner_setting=(await session.execute(text(_OWNER))).scalar_one(),
        editor_any=(await session.execute(text(_FLAG))).scalar_one(),
        bodies=tuple((await session.execute(text(_BODIES))).scalars()),
        bob_rows_writable=touched,
    )


class ProbeRls(UseCase[notes.Note, Seen]):
    """Report the RLS settings and rows visible to this execution; declares no caller."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        return await _observe(self._session_manager, None)


class ProbeRlsAsCaller(UseCase[notes.Note, Seen]):
    """Report what ``Caller()`` received next to what RLS applied."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self, caller: Identity = Caller()) -> Seen:
        return await _observe(self._session_manager, caller)


async def _open_if(session_manager: SessionManager, open_first: bool) -> None:
    if open_first:
        session = session_manager.active_session
        assert session is not None
        await session.execute(text("SELECT 1"))


class ElevateThenNest(UseCase[notes.Note, Seen]):
    """Elevate ``editor``, then invoke :class:`ProbeRls` as a nested execution."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self, open_first: bool) -> Seen:
        await _open_if(self._session_manager, open_first)
        await elevate("editor", DECISION, at=Scope.of(U1))
        return cast(Seen, await self._app.invoke(ProbeRls))


class NestAsZoe(UseCase[notes.Note, tuple[NestedIdentityMismatch | None, str]]):
    """Invoke :class:`ProbeRlsAsCaller` as ``ZOE``; report the refusal and the owner afterwards."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = cast(AppInvoker, app)

    async def execute(self, open_first: bool) -> tuple[NestedIdentityMismatch | None, str]:
        await _open_if(self._session_manager, open_first)
        as_zoe = self._app.for_identity(ZOE)
        refused: NestedIdentityMismatch | None = None
        try:
            await as_zoe.invoke(ProbeRlsAsCaller)
        except NestedIdentityMismatch as exc:
            refused = exc
        return refused, (await _observe(self._session_manager, None)).owner_setting


class NestAsSelf(UseCase[notes.Note, Seen]):
    """Invoke :class:`ProbeRlsAsCaller` as the caller this execution received."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = cast(AppInvoker, app)

    async def execute(self, caller: Identity = Caller()) -> Seen:
        return cast(Seen, await self._app.for_identity(caller).invoke(ProbeRlsAsCaller))


class ReadThenWriteAsZoe(UseCase[notes.Note, Seen]):
    """A read-only parent: its writing child opens a unit of work of its own, as ``ZOE``."""

    read_only = True

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = cast(AppInvoker, app)

    async def execute(self) -> Seen:
        return cast(Seen, await self._app.for_identity(ZOE).invoke(ProbeRlsAsCaller))


class Fail(UseCase[notes.Note, None]):
    async def execute(self, cancel: bool) -> None:
        if cancel:
            raise asyncio.CancelledError
        raise RuntimeError("business failure")


class OwnersSeenByGather(UseCase[notes.Note, list[object]]):
    """The owner binding RLS would apply, read from two ``gather`` children."""

    async def execute(self) -> list[object]:
        async def owner() -> object:
            return resolve_binding("identity.subject")

        return list(await asyncio.gather(owner(), owner()))


class ElevatingChild(UseCase[notes.Note, Seen]):
    """Elevate ``editor`` inside the nested execution itself."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        await elevate("editor", DECISION, at=Scope.of(U1))
        return await _observe(self._session_manager, None)


class NestElevatingChild(UseCase[notes.Note, tuple[Seen, str]]):
    """Invoke :class:`ElevatingChild`, then report the flag on the parent's transaction."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self) -> tuple[Seen, str]:
        child = cast(Seen, await self._app.invoke(ElevatingChild))
        return child, (await _observe(self._session_manager, None)).editor_any


class WhoAmIJob(Job[str]):
    """Return the subject the job's ``Caller()`` received."""

    async def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class ProbeJob(Job[Seen]):
    """Report the RLS settings the job's own unit of work runs with."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        return await _observe(self._session_manager, None)


class ElevateThenDispatch(UseCase[notes.Note, Any]):
    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self) -> Any:
        await elevate("editor", DECISION, at=Scope.of(U1))
        return self._jobs.dispatch(ProbeJob)


class RunJob(UseCase[notes.Note, str]):
    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self) -> str:
        return await self._jobs.run(WhoAmIJob)


class DispatchJob(UseCase[notes.Note, Any]):
    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self) -> Any:
        return self._jobs.dispatch(WhoAmIJob)


@pytest.fixture(autouse=True)
def _wiring() -> Iterator[None]:
    clear_scope_sources()
    clear_authz_product()
    register_scope_source("editor", lambda: "ana")
    register_authz_product(Product())
    yield
    clear_scope_sources()
    clear_authz_product()


async def _seed(bypass_url: str) -> None:
    engine = create_async_engine(bypass_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO notes (owner_id, editor, body) VALUES "
                    f"('{U1}', 'ana', 'a'), ('{U1}', 'bob', 'b'), ('{U2}', 'zoe', 'z')"
                )
            )
    finally:
        await engine.dispose()


def _config(tmp_path: Path, schema: str) -> str:
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{schema}.yaml").read_text())
    document["app"]["discovery"]["modules"]["include"].append(__name__)
    document["persistence"] = {"cursors": {"signing": "ephemeral"}}
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump(document))
    return str(path)


@pytest.fixture
async def app(scoped_database: BootstrapFactory, tmp_path: Path) -> AsyncIterator[AppInvoker]:
    """The composed application's invoker, running against a fresh seeded schema."""
    schema = f"notes_ident_{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    await _seed(database.bypass)
    composed = build_application(
        _config(tmp_path, schema), adapter=FastAPIAdapter(), environment="test"
    )
    async with composed.running():
        yield cast(AppInvoker, composed.kernel.app)


@pytest.fixture
def ambient(request: pytest.FixtureRequest) -> Iterator[None]:
    """Install the ambient identity the way the REST middleware does; ``None`` installs none."""
    subject: str | None = request.param
    if subject is None:
        yield
        return
    token = set_identity(Identity(subject=subject))
    try:
        yield
    finally:
        reset_identity(token)


# --------------------------------------------------------------------- H1


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_control_a_top_level_probe_is_not_elevated(app: AppInvoker) -> None:
    seen: Seen = await app.invoke(ProbeRls)

    assert (seen.editor_any, seen.bob_rows_writable, seen.bodies) == ("", 0, ("a", "b"))


OPEN_FIRST = pytest.mark.parametrize("open_first", [True, False], ids=["tx-open", "tx-lazy"])


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@OPEN_FIRST
@pytest.mark.usefixtures("ambient")
async def test_h1_a_nested_execution_after_an_elevation_is_refused(
    app: AppInvoker, open_first: bool
) -> None:
    params = {"open_first": open_first}

    with pytest.raises(NestedElevation, match="ProbeRls cannot run inside ElevateThenNest"):
        await app.invoke(ElevateThenNest, params=params)


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h1_the_next_execution_after_a_refused_nesting_is_not_elevated(
    app: AppInvoker,
) -> None:
    params = {"open_first": True}
    with pytest.raises(NestedElevation):
        await app.invoke(ElevateThenNest, params=params)

    seen: Seen = await app.invoke(ProbeRls)

    assert (seen.editor_any, seen.bob_rows_writable) == ("", 0)


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h1_a_nested_execution_may_elevate_itself_and_clears_the_flag(
    app: AppInvoker,
) -> None:
    child, parent_flag = await app.invoke(NestElevatingChild)

    assert (child.editor_any, child.bob_rows_writable, parent_flag) == ("on", 1, "")


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h1_a_job_dispatched_after_an_elevation_runs_without_the_flag(
    app: AppInvoker,
) -> None:
    handle = await app.invoke(ElevateThenDispatch)

    seen: Seen = handle.wait()
    assert (seen.editor_any, seen.bob_rows_writable, seen.owner_setting) == ("", 0, U1)


# --------------------------------------------------------------------- H2


@pytest.mark.parametrize("ambient", [U2, None], ids=["ambient-U2", "ambient-none"], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h2_rls_follows_the_identity_the_executor_received(app: AppInvoker) -> None:
    seen: Seen = await app.for_identity(ANA).invoke(ProbeRlsAsCaller)

    assert (seen.caller, seen.owner_setting, seen.bodies) == (U1, U1, ("a", "b"))


@pytest.mark.parametrize("ambient", [None], indirect=True)
@OPEN_FIRST
@pytest.mark.usefixtures("ambient")
async def test_h2_joining_the_parents_unit_of_work_as_another_identity_is_refused(
    app: AppInvoker, open_first: bool
) -> None:
    refused, owner = await app.for_identity(ANA).invoke(
        NestAsZoe, params={"open_first": open_first}
    )

    assert isinstance(refused, NestedIdentityMismatch)
    assert "ProbeRlsAsCaller would join the unit of work of NestAsZoe" in refused.message
    assert owner == U1


@pytest.mark.parametrize("ambient", [None], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h2_joining_the_parents_unit_of_work_as_the_same_identity_is_allowed(
    app: AppInvoker,
) -> None:
    seen: Seen = await app.for_identity(ANA).invoke(NestAsSelf)

    assert (seen.caller, seen.owner_setting, seen.bodies) == (U1, U1, ("a", "b"))


@pytest.mark.parametrize("ambient", [None], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h2_a_nested_unit_of_work_of_its_own_follows_the_childs_identity(
    app: AppInvoker,
) -> None:
    seen: Seen = await app.for_identity(ANA).invoke(ReadThenWriteAsZoe)

    assert (seen.caller, seen.owner_setting, seen.bodies) == (U2, U2, ("z",))


@pytest.mark.parametrize("ambient", [U1], indirect=True)
@pytest.mark.parametrize(
    ("cancel", "raised"),
    [(False, RuntimeError), (True, asyncio.CancelledError)],
    ids=["exception", "cancellation"],
)
@pytest.mark.usefixtures("ambient")
async def test_h2_the_ambient_identity_is_restored_after_a_failed_execution(
    app: AppInvoker, cancel: bool, raised: type[BaseException]
) -> None:
    as_zoe = app.for_identity(ZOE)
    params = {"cancel": cancel}
    with pytest.raises(raised):
        await as_zoe.invoke(Fail, params=params)

    seen: Seen = await app.invoke(ProbeRls)

    assert (current_identity(), seen.owner_setting, seen.bodies) == (ANA, U1, ("a", "b"))


@pytest.mark.parametrize("ambient", [U2], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h2_gather_children_see_the_bound_identity(app: AppInvoker) -> None:
    assert await app.for_identity(ANA).invoke(OwnersSeenByGather) == [U1, U1]


# --------------------------------------------------------------------- H3


@pytest.mark.parametrize("ambient", [None], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h3_an_inline_job_run_runs_as_the_invokers_identity(app: AppInvoker) -> None:
    subject = await app.for_identity(ANA).invoke(RunJob)

    assert subject == U1


@pytest.mark.parametrize("ambient", [None], indirect=True)
@pytest.mark.usefixtures("ambient")
async def test_h3_an_inline_job_dispatch_runs_as_the_invokers_identity(app: AppInvoker) -> None:
    handle = await app.for_identity(ANA).invoke(DispatchJob)

    assert handle.wait() == U1
