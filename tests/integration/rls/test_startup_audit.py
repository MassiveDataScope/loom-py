"""Startup refuses an audited or reading application the database cannot serve.

FR-018, SC-002; spec 009 US1 #3, FR-012, reviews M9 and N3 (T12).
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.audit.entry import audit_entry_for
from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.rls import create_schema, guard_manifest
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_GUARD_REVISION
from loom.core.repository.sqlalchemy.rls.integrity import Problem, startup_problems
from tests.helpers.persistence import build_backend
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls import test_audit_read_guard as mixed
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, application_for
from tests.integration.rls.test_audit_verify import (
    READER,
    TENANT,
    drifted,
    leak_view,
    reader_body_drift,
)

pytestmark = pytest.mark.integration


async def _found(
    database: ScopedDatabase,
    *,
    audited: bool,
    recording: tuple[str, ...] = (),
    reading: bool = False,
) -> list[Problem]:
    engine = create_async_engine(database.write, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await startup_problems(
                connection,
                f"loom_guard_{database.schema}",
                {},
                audited=audited,
                recording=recording,
                reading=reading,
            )
    finally:
        await engine.dispose()


async def _problems(database: ScopedDatabase, **options: Any) -> set[str]:
    """The checks of :func:`_found`'s problems, called with the same *options*."""
    return {problem.check for problem in await _found(database, **options)}


async def _reading(factory: BootstrapFactory, tmp_path: Path, schema: str) -> ScopedDatabase:
    """Bootstrap *schema* with reading on and deploy it, so the readers group may read."""
    database = await factory(schema, audit_reading=True, audit_boundary=TENANT)
    application = application_for(
        bookkeeping, database, tmp_path, schema=schema, audit={"reading": {"enabled": True}}
    )
    await create_schema(database.migrator, application)
    return database


async def _start(
    database: ScopedDatabase,
    models: Sequence[type[BaseModel]],
    *,
    reading: bool,
    scopes: dict[str, str] | None = None,
) -> Any:
    """Build the standard backend over *models* as the write user and run its startup checks."""
    schema = database.schema
    config = {
        "app": {"name": schema},
        "stores": {
            "default": {
                "backend": "sqlalchemy",
                "url": database.write,
                "schema": {
                    "mode": "external",
                    "name": schema,
                    "guard": f"loom_guard_{schema}",
                    "scopes": scopes or dict(bookkeeping.SCOPE_BINDINGS),
                    "audit": {"reading": {"enabled": reading}},
                },
            }
        },
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    wiring = build_backend(SQLAlchemyBackend(), ConfigContext.from_dict(config), models)
    wiring.prepare_models(models)
    await wiring.startup_checks()
    return wiring


async def test_an_audited_schema_starts_clean(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_start")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )

    assert await _problems(database, audited=True) == set()


async def test_a_marked_table_without_its_trigger_stops_startup(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_unmigrated")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )

    protected = await _problems(database, audited=True, recording=("accounts", "memos"))
    unmigrated = await _problems(database, audited=True, recording=("memos", "drafts"))

    assert protected == set()
    assert unmigrated == {"audit.trigger"}


async def test_a_changed_log_column_stops_startup_even_without_marked_tables(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_shape")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )
    async with drifted(
        database,
        "ALTER TABLE books_shape.audit_log ADD COLUMN note text",
        "ALTER TABLE books_shape.audit_log DROP COLUMN note",
    ):
        assert "audit.shape" in await _problems(database, audited=False)


async def test_an_audited_application_on_revision_two_does_not_start(
    scoped_database: BootstrapFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", guard_manifest.GUARD_REVISIONS[:2])
    database = await scoped_database("books_old")
    monkeypatch.undo()

    assert await _problems(database, audited=True) == {"audit.guard_revision"}


async def test_a_reading_schema_starts_clean(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _reading(scoped_database, tmp_path, "rs_clean")

    assert await _problems(database, audited=True, reading=True) == set()


async def test_reading_on_revision_three_names_the_reader(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """US1 #3, review N3: revision 3 has no reader, and the problem says revision 4 installs it."""
    monkeypatch.setattr(
        guard_manifest, "GUARD_REVISIONS", guard_manifest.GUARD_REVISIONS[:AUDIT_GUARD_REVISION]
    )
    database = await scoped_database("rs_rev3")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema="rs_rev3")
    )
    monkeypatch.undo()

    [problem] = await _found(database, audited=True, reading=True)

    assert problem.check == "audit.reading_revision"
    assert "read_audit_log" in f"{problem.subject}: {problem.actual}"
    assert await _problems(database, audited=True, reading=False) == set()


async def test_reading_without_the_reader_names_the_missing_function(
    scoped_database: BootstrapFactory,
) -> None:
    """US1 #3, N13: revision 4 without a log installs no reader; startup names both."""
    database = await scoped_database("rs_missing", audit_reading=True, audit_boundary=TENANT)

    problems = await _found(database, audited=False, reading=True)

    assert {(problem.check, problem.subject, problem.actual) for problem in problems} == {
        ("audit.shape", "audit_log", "missing"),
        ("audit.reader", "read_audit_log", "missing"),
    }


async def test_a_grant_implies_the_flag(scoped_database: BootstrapFactory, tmp_path: Path) -> None:
    """The application user may execute the reader, yet its store does not read: refused."""
    database = await _reading(scoped_database, tmp_path, "rs_flag")

    assert await _problems(database, audited=True, reading=False) == {"audit.reader_grant"}


async def test_a_reader_anyone_may_execute_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _reading(scoped_database, tmp_path, "rs_public")
    reader = f"rs_public.{READER}"
    async with drifted(
        database,
        f"GRANT EXECUTE ON FUNCTION {reader} TO PUBLIC",
        f"REVOKE EXECUTE ON FUNCTION {reader} FROM PUBLIC",
    ):
        problems = await _problems(database, audited=True, reading=True)

    assert problems == {"audit.reader_shape"}


async def test_a_store_holding_only_the_audit_model_starts_and_is_checked(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Option C, review M9: no table is created or required; guard and reading are checked."""
    database = await _reading(scoped_database, tmp_path, "rs_only")

    wiring = await _start(database, (audit_entry_for("default"),), reading=True)

    assert not wiring.uow_factory._session_manager.metadata.tables


async def test_a_reading_store_without_tables_still_fails_without_the_reader(
    scoped_database: BootstrapFactory,
) -> None:
    """Review M9: the widened gate runs the reading checks with no scoped or audited table."""
    database = await scoped_database("rs_gate", audit_reading=True, audit_boundary=TENANT)

    with pytest.raises(ConfigError, match="read_audit_log"):
        await _start(database, (audit_entry_for("default"),), reading=True)


async def test_a_reading_store_without_the_log_does_not_start(
    scoped_database: BootstrapFactory,
) -> None:
    """N13: a reading store holding only the audit model needs the log, not only the reader."""
    database = await scoped_database("rs_nolog", audit_reading=True, audit_boundary=TENANT)

    with pytest.raises(ConfigError, match="audit.shape audit_log: missing"):
        await _start(database, (audit_entry_for("default"),), reading=True)


async def test_a_reading_store_with_two_boundaries_does_not_start(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-012 at startup, through ``derive_audit_boundary``; with reading off it starts."""
    database = await scoped_database("rs_mixed")
    await create_schema(
        database.migrator, application_for(mixed.MIXED, database, tmp_path, schema="rs_mixed")
    )
    scopes = dict(mixed.SCOPE_BINDINGS)

    await _start(database, mixed.MODELS, reading=False, scopes=scopes)
    with pytest.raises(ConfigError, match="audit_read_guard_sites"):
        await _start(database, mixed.MODELS, reading=True, scopes=scopes)


@pytest.mark.parametrize(
    ("grant", "undo"),
    [
        (
            "GRANT EXECUTE ON FUNCTION {reader} TO {schema}_ops",
            "REVOKE EXECUTE ON FUNCTION {reader} FROM {schema}_ops",
        ),
        (
            "GRANT EXECUTE ON FUNCTION {reader} TO {schema}_readers WITH GRANT OPTION",
            "REVOKE GRANT OPTION FOR EXECUTE ON FUNCTION {reader} FROM {schema}_readers",
        ),
    ],
    ids=["extra-grantee", "grantable"],
)
async def test_a_reader_grant_beyond_the_readers_execute_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path, grant: str, undo: str
) -> None:
    """Review F1: one grantee besides the owner, without grant option, while reading."""
    schema = "rs_grantee"
    database = await _reading(scoped_database, tmp_path, schema)
    names = {"reader": f"{schema}.{READER}", "schema": schema}
    async with drifted(database, grant.format(**names), undo.format(**names)):
        problems = await _problems(database, audited=True, reading=True)

    assert problems == {"audit.reader_grant"}


async def test_a_changed_reader_body_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Review F1: startup compares the body with the one the guard releases."""
    database = await _reading(scoped_database, tmp_path, "rs_body")
    async with drifted(database, *reader_body_drift("rs_body")):
        problems = await _problems(database, audited=True, reading=True)

    assert problems == {"audit.reader_source"}


async def test_a_volatile_reader_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _reading(scoped_database, tmp_path, "rs_volatile")
    reader = f"rs_volatile.{READER}"
    async with drifted(
        database, f"ALTER FUNCTION {reader} VOLATILE", f"ALTER FUNCTION {reader} STABLE"
    ):
        problems = await _problems(database, audited=True, reading=True)

    assert problems == {"audit.reader_shape"}


async def test_a_grant_the_user_does_not_hold_still_implies_the_flag(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Review F2: reading off, EXECUTE granted to a role the application user cannot use."""
    database = await scoped_database("rs_acl")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema="rs_acl")
    )
    reader = f"rs_acl.{READER}"
    async with drifted(
        database,
        f"GRANT EXECUTE ON FUNCTION {reader} TO rs_acl_ops",
        f"REVOKE EXECUTE ON FUNCTION {reader} FROM rs_acl_ops",
    ):
        problems = await _problems(database, audited=True, reading=False)

    assert problems == {"audit.reader_grant"}


async def test_a_view_over_the_log_stops_startup(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """T12b: a view made before the guard's revision, or behind its back, is read as any role."""
    database = await scoped_database("av_start")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )
    async with drifted(database, *leak_view("av_start")):
        assert await _problems(database, audited=True) == {"audit.view"}


async def test_a_column_grant_on_the_log_stops_startup(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """T12c fix round F3: a column grant reads the log too, though the table is not readable."""
    database = await scoped_database("ac_start")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )
    async with drifted(
        database,
        "GRANT SELECT (subject, changed) ON ac_start.audit_log TO ac_start_writers",
        "REVOKE SELECT (subject, changed) ON ac_start.audit_log FROM ac_start_writers",
    ):
        problems = await _problems(database, audited=True)

    assert problems == {"audit.read_columns"}
