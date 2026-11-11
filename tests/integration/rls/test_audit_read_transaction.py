"""The audit reader's own transaction is read-only and never commits (spec 009, T13 fix round).

The product's subjects select runs inside the reader's statement, with the
application user's privileges. These tests give it one that would write, a
row lock and a value its bind processing refuses, through the real reader
against a store deployed with reading on (the seed of
``test_audit_read_visibility.py``), and check that nothing is written and
that the 500 carries no SQL.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Column, MetaData, Select, Table, Text, bindparam, insert, select
from sqlalchemy.types import TypeDecorator

from loom.core.audit import AuditReadFailed
from loom.core.persistence.stores import StoreKey
from loom.core.repository.abc import CursorCodec, PaginationMode, QuerySpec, SortSpec
from loom.core.repository.sqlalchemy.audit.reader import SQLAlchemyAuditLogReader
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, execute, scalar
from tests.integration.rls.test_audit_read_visibility import _seeded

pytestmark = pytest.mark.integration

_PRODUCT = "t13prod"
_MARKS = Table("marks", MetaData(), Column("member", Text), schema=_PRODUCT)
_QUERY = QuerySpec(sort=(SortSpec("at", "DESC"),), pagination=PaginationMode.CURSOR, limit=10)


class _Refused(TypeDecorator[str]):
    """A type whose bind processing fails, as a product's custom type may."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> str:
        raise ValueError(f"cannot bind {value}")


@pytest.fixture
async def deployed(scoped_database: BootstrapFactory, tmp_path: Path) -> ScopedDatabase:
    database = await _seeded(scoped_database, tmp_path)
    await execute(
        database.superuser,
        f"CREATE SCHEMA {_PRODUCT}",
        f"CREATE TABLE {_PRODUCT}.marks (member text)",
        f"INSERT INTO {_PRODUCT}.marks VALUES ('carla')",
        f"GRANT USAGE ON SCHEMA {_PRODUCT} TO {database.schema}_rw",
        f"GRANT SELECT, INSERT, UPDATE ON {_PRODUCT}.marks TO {database.schema}_rw",
    )
    return database


@pytest.fixture
async def reader(deployed: ScopedDatabase) -> AsyncIterator[SQLAlchemyAuditLogReader]:
    manager = SessionManager(
        deployed.write,
        session_settings=lambda: {"loom.scope.tenant": "acme", "loom.scope.region": "north"},
        store=StoreKey("default"),
    )
    try:
        yield SQLAlchemyAuditLogReader(
            manager,
            CursorCodec(signing="ephemeral"),
            schema=deployed.schema,
            store=StoreKey("default"),
            max_subjects=1000,
        )
    finally:
        await manager.dispose()


def _writing() -> Select[Any]:
    written = insert(_MARKS).values(member="dave").returning(_MARKS.c.member).cte("written")
    return select(written.c.member)


def _locking() -> Select[Any]:
    return select(_MARKS.c.member).with_for_update()


@pytest.mark.parametrize("subjects", [_writing, _locking], ids=["dml-cte", "for-update"])
async def test_a_subjects_select_that_writes_or_locks_is_refused_and_writes_nothing(
    subjects: Any,
    reader: SQLAlchemyAuditLogReader,
    deployed: ScopedDatabase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING), pytest.raises(AuditReadFailed):
        await reader.read(_QUERY, subjects())

    assert "SQLSTATE 25006" in caplog.text
    members = await scalar(
        deployed.superuser, f"SELECT string_agg(member, ',') FROM {_PRODUCT}.marks"
    )
    assert members == "carla"


async def test_a_bind_the_driver_never_sees_is_a_fixed_500_without_sql(
    reader: SQLAlchemyAuditLogReader, caplog: pytest.LogCaptureFixture
) -> None:
    refused = select(_MARKS.c.member).where(
        _MARKS.c.member != bindparam(None, "secret-member", type_=_Refused())
    )

    with caplog.at_level(logging.DEBUG, logger="loom"), pytest.raises(AuditReadFailed) as raised:
        await reader.read(_QUERY, refused)

    error = raised.value
    assert error.__cause__ is None and error.__context__ is None
    assert "StatementError" in caplog.text
    for leaked in ("SELECT", "secret-member"):
        assert leaked not in str(error) + repr(error.args)
        assert leaked not in caplog.text
