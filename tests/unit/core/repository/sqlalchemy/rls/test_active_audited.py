"""Which audited tables record rows, and which guard revision a store needs."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from loom.core.backend.scoped_ddl import ASSERT_SCHEMA
from loom.core.locator import Application
from loom.core.model.audited import AuditedTable
from loom.core.model.redaction import Dsn
from loom.core.persistence.database import AuditSchemaConfig, DatabaseConfig, SchemaConfig
from loom.core.repository.sqlalchemy.rls.audit import (
    APPLY_AUDIT_READING,
    READING_SUPPORTED,
    reconcile_audited,
)
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, DatabaseRoles
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    AUDIT_GUARD_REVISION,
    READING_GUARD_REVISION,
    required_guard_revision,
)
from loom.core.repository.sqlalchemy.rls.integrity import AUDIT_SUPPORTED
from loom.core.schema_names import SchemaNames

_LEDGER = AuditedTable(schema=None, name="ledger", key_columns=("id",), excluded=frozenset())


def _application(*, enabled: bool) -> Application:
    from sqlalchemy import MetaData

    schema = SchemaConfig(audit=AuditSchemaConfig(enabled=enabled))
    return Application(
        models=(),
        metadata=MetaData(),
        database=DatabaseConfig(url=Dsn("postgresql+asyncpg://u:p@localhost/db"), schema=schema),
        bootstrap=None,
        scoped={},
        scope_sources={},
        audited={(None, "ledger"): _LEDGER},
    )


def test_marked_tables_record_while_the_switch_is_on() -> None:
    assert _application(enabled=True).active_audited == {(None, "ledger"): _LEDGER}


def test_no_table_records_while_the_switch_is_off() -> None:
    application = _application(enabled=False)

    assert application.active_audited == {}
    assert application.audited  # the marks stay; only recording stops


def test_an_auditing_store_needs_the_audit_revision() -> None:
    assert required_guard_revision(True) == AUDIT_GUARD_REVISION


def test_a_store_without_audit_takes_any_revision() -> None:
    assert required_guard_revision(False) is None


# Spec 009, review N18: both callers ask for the reader's revision while reading is on.


class _Refused(Exception):
    """The guard revision check stopped the deploy."""


class _Connection:
    """An async connection that records ``run_sync``: any DDL would go through it."""

    def __init__(self) -> None:
        self.synced: list[object] = []

    async def execute(self, *_: object) -> None:
        return None

    async def run_sync(self, action: object) -> None:
        self.synced.append(action)


def _reading_application() -> Application:
    from sqlalchemy import MetaData

    bootstrap = BootstrapConfig(
        schema="s1",
        roles=DatabaseRoles(owner="s1_owner", migrator="s1_migrator"),
        database_users={},
        names=SchemaNames.derived("s1"),
        audit_reading=True,
    )
    return Application(
        models=(),
        metadata=MetaData(),
        database=DatabaseConfig(url=Dsn("postgresql+asyncpg://u:p@localhost/db")),
        bootstrap=bootstrap,
        scoped={},
        scope_sources={},
        audited={},
    )


def test_a_reading_store_needs_the_reading_revision() -> None:
    assert required_guard_revision(False, reading=True) == READING_GUARD_REVISION
    assert required_guard_revision(True, reading=True) == READING_GUARD_REVISION


async def test_create_schema_refuses_an_older_guard_before_any_ddl_while_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.core.repository.sqlalchemy.rls import schema as module

    minimums: list[int | None] = []

    async def landed(*_: object) -> None:
        return None

    async def refuse(_connection: object, _guard: str, *, minimum: int | None) -> None:
        minimums.append(minimum)
        raise _Refused

    monkeypatch.setattr(module, "_require_guard", landed)
    monkeypatch.setattr(module, "require_guard_revision", refuse)
    application = _reading_application()
    assert application.bootstrap is not None
    recorder = _Connection()
    connection: Any = recorder  # a recording stand-in for the AsyncConnection

    with pytest.raises(_Refused):
        await module._create_protected(connection, application, application.bootstrap, "5s")

    assert minimums == [READING_GUARD_REVISION]
    assert recorder.synced == []


def test_migrations_refuse_an_older_guard_before_any_ddl_while_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from alembic import context

    from loom.core.repository.sqlalchemy.migrations import runners

    minimums: list[int | None] = []

    def refuse(_connection: object, _guard: str, *, minimum: int | None) -> None:
        minimums.append(minimum)
        raise _Refused

    monkeypatch.setattr(context, "config", SimpleNamespace(attributes={}), raising=False)
    monkeypatch.setattr(runners, "_reject_revisions", lambda *_, **__: None)
    monkeypatch.setattr(runners, "require_landing", lambda *_: None)
    monkeypatch.setattr(runners, "require_guard_revision_sync", refuse)
    connection: Any = SimpleNamespace(execute=_no_statement, commit=_no_statement)

    with pytest.raises(_Refused):
        runners.run_migrations(connection, _reading_application())

    assert minimums == [READING_GUARD_REVISION]


def _no_statement(*_: object) -> None:
    raise AssertionError("no statement may run before the guard revision check")


# Spec 009, review M5: the owner step runs on revision 4, also with nothing declared.


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar(self) -> object:
        return self.value

    def __iter__(self) -> Iterator[object]:
        return iter(())


class _Catalogue:
    """A sync connection on a revision 3 or later guard with no audited table registered.

    It records every statement; the reading catalogue query answers *reading*.
    """

    def __init__(self, *, reading: bool) -> None:
        self.answers = {AUDIT_SUPPORTED: True, READING_SUPPORTED: reading}
        self.statements: list[str] = []

    def execute(self, statement: object, _params: object = None) -> _Result:
        sql = str(statement)
        self.statements.append(sql)
        return _Result(self.answers.get(sql, False))


class _AsyncCatalogue:
    """The async side ``create_schema`` drives; ``run_sync`` hands over the sync catalogue.

    Its own statements are recorded in the same list, so the order is visible.
    """

    def __init__(self, sync: _Catalogue) -> None:
        self.sync = sync
        self.info: dict[str, object] = {}

    async def execute(self, statement: object, _params: object = None) -> None:
        self.sync.statements.append(str(statement))

    async def get_raw_connection(self) -> _AsyncCatalogue:
        return self

    async def run_sync(self, action: Callable[[Any], object]) -> None:
        if getattr(action, "__name__", "") != "create_all":
            action(self.sync)


@pytest.mark.parametrize(("reading", "applied"), [(True, [APPLY_AUDIT_READING]), (False, [])])
def test_reconcile_runs_the_owner_step_only_on_a_guard_that_has_it(
    reading: bool, applied: list[str]
) -> None:
    connection = _Catalogue(reading=reading)
    sync: Any = connection

    reconcile_audited(sync, schema="s1", guard="g1", declared={})

    assert [sql for sql in connection.statements if sql == APPLY_AUDIT_READING] == applied
    assert connection.statements[-1] == (APPLY_AUDIT_READING if reading else READING_SUPPORTED)


def test_the_migration_path_runs_the_owner_step_with_nothing_declared() -> None:
    from loom.core.repository.sqlalchemy.migrations import runners

    connection = _Catalogue(reading=True)
    sync: Any = connection

    runners._reconcile_present(sync, "s1", "g1", {})

    assert connection.statements[-1] == APPLY_AUDIT_READING


async def test_the_create_all_path_runs_the_owner_step_before_the_assertion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.core.repository.sqlalchemy.rls import schema as module

    async def passes(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_guard", passes)
    monkeypatch.setattr(module, "require_guard_revision", passes)
    application = _reading_application()
    assert application.bootstrap is not None
    catalogue = _Catalogue(reading=True)
    connection: Any = _AsyncCatalogue(catalogue)

    await module._create_protected(connection, application, application.bootstrap, "5s")

    assert catalogue.statements[-2:] == [APPLY_AUDIT_READING, ASSERT_SCHEMA]
