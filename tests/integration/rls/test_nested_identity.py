"""Regressions for how identity and elevation reach row-level security.

- H1: a nested execution started after its parent elevated ``editor`` is
  refused, so it never runs with the elevation flag on the shared transaction.
- H2: the row-level security settings follow the identity the executor
  received (``Caller()``), not the ambient ``current_identity()``; a nested
  execution that would join the parent's transaction as another identity is
  refused.
- H3: an inline job run or dispatched by an execution bound to identity X
  runs as X, not as the ambient (anonymous) identity.

The use cases run through the real composition (``Project.compose``) against
real Postgres with ``schema.mode: external``.
"""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from loom.composition import Project
from loom.core.errors import NestedElevation, NestedIdentityMismatch, UndeclaredStoreWrite
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.use_case.invoker import AppInvoker
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.stores.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import notes
from tests.integration.rls.apps.nested_identity import (
    ANA,
    ElevateThenDispatch,
    ElevateThenNest,
    NestAsSelf,
    NestAsZoe,
    NestElevatingChild,
    ProbeRls,
    ProbeRlsAsCaller,
    ReadThenWriteAsZoe,
    RunOrDispatchJob,
    Seen,
)
from tests.integration.rls.conftest import (
    U1,
    U2,
    BootstrapFactory,
    application_for,
    seed_notes,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("notes_wiring")]


def _config(tmp_path: Path, schema: str) -> str:
    document: dict[str, Any] = yaml.safe_load((tmp_path / f"{schema}.yaml").read_text())
    document["app"]["packages"].append("tests.integration.rls.apps.nested_identity")
    document["stores"]["settings"] = {"cursors": {"signing": "ephemeral"}}
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
    await seed_notes(database.bypass)
    composed = Project.load(_config(tmp_path, schema), environment="test").compose(FastAPIAdapter())
    async with composed.running():
        yield cast(AppInvoker, composed.kernel.invoker)


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


class TestNestedIdentity:
    @pytest.mark.parametrize("ambient", [U1], indirect=True)
    @pytest.mark.usefixtures("ambient")
    async def test_h1_a_nested_execution_after_an_elevation_is_refused(
        self, app: AppInvoker
    ) -> None:
        with pytest.raises(NestedElevation, match="ProbeRls cannot run inside an execution"):
            await app.invoke(ElevateThenNest)

    @pytest.mark.parametrize("ambient", [U1], indirect=True)
    @pytest.mark.usefixtures("ambient")
    async def test_h1_the_next_execution_after_a_refused_nesting_is_not_elevated(
        self,
        app: AppInvoker,
    ) -> None:
        with pytest.raises(NestedElevation):
            await app.invoke(ElevateThenNest)

        seen: Seen = await app.invoke(ProbeRls)

        assert (seen.editor_any, seen.bob_rows_writable, seen.bodies) == ("", 0, ("a", "b"))

    @pytest.mark.parametrize("ambient", [U1], indirect=True)
    @pytest.mark.usefixtures("ambient")
    async def test_h1_a_nested_execution_may_elevate_itself_and_clears_the_flag(
        self,
        app: AppInvoker,
    ) -> None:
        child, parent_flag = await app.invoke(NestElevatingChild)

        assert (child.editor_any, child.bob_rows_writable, parent_flag) == ("on", 1, "")

    @pytest.mark.parametrize("ambient", [U1], indirect=True)
    @pytest.mark.usefixtures("ambient")
    async def test_h1_a_job_dispatched_after_an_elevation_runs_without_the_flag(
        self,
        app: AppInvoker,
    ) -> None:
        handle = await app.invoke(ElevateThenDispatch)

        seen: Seen = handle.wait()
        assert (seen.editor_any, seen.bob_rows_writable, seen.owner_setting) == ("", 0, U1)

    # --------------------------------------------------------------------- H2

    @pytest.mark.parametrize(
        "ambient", [U2, None], ids=["ambient-U2", "ambient-none"], indirect=True
    )
    @pytest.mark.usefixtures("ambient")
    async def test_h2_rls_follows_the_identity_the_executor_received(self, app: AppInvoker) -> None:
        seen: Seen = await app.for_identity(ANA).invoke(ProbeRlsAsCaller)

        assert (seen.caller, seen.owner_setting, seen.bodies) == (U1, U1, ("a", "b"))

    async def test_h2_joining_the_parents_unit_of_work_as_another_identity_is_refused(
        self,
        app: AppInvoker,
    ) -> None:
        refused, owner = await app.for_identity(ANA).invoke(NestAsZoe)

        assert isinstance(refused, NestedIdentityMismatch)
        assert "ProbeRlsAsCaller would join the open unit of work" in refused.message
        assert owner == U1

    async def test_h2_joining_the_parents_unit_of_work_as_the_same_identity_is_allowed(
        self,
        app: AppInvoker,
    ) -> None:
        seen: Seen = await app.for_identity(ANA).invoke(NestAsSelf)

        assert (seen.caller, seen.owner_setting, seen.bodies) == (U1, U1, ("a", "b"))

    async def test_h2_a_writing_child_of_a_read_only_parent_is_refused_as_any_identity(
        self,
        app: AppInvoker,
    ) -> None:
        caller = app.for_identity(ANA)

        with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
            await caller.invoke(ReadThenWriteAsZoe)

    # --------------------------------------------------------------------- H3

    @pytest.mark.parametrize("dispatch", [False, True], ids=["run", "dispatch"])
    async def test_h3_an_inline_job_runs_as_the_invokers_identity(
        self, app: AppInvoker, dispatch: bool
    ) -> None:
        result = await app.for_identity(ANA).invoke(RunOrDispatchJob, params={"dispatch": dispatch})

        assert (result.wait() if dispatch else result) == U1
