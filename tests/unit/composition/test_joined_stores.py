"""Composition refuses a store a use case or job writes to but could not commit (ADR 0008, T046).

``Joined`` on a store that cannot roll back, and a primary or joined store
with no unit of work, because no discovered model belongs to it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, Application, compose_application
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import RepoFor
from loom.core.use_case import Joined, UseCase
from loom.core.use_case.errors import (
    JoinedAutocommitStore,
    UncomposedWriteStore,
    UseCaseErrorCode,
)
from tests.helpers.composition import Serving, composition_source

pytestmark = pytest.mark.timeout(30)


class _Invoice(BaseModel):
    __tablename__ = "joined_stores_invoices"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _AuditEntry(BaseModel):
    __tablename__ = "joined_stores_audit_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _Audited(UseCase[_Invoice, None]):
    def __init__(self, entries: Joined[RepoFor[_AuditEntry]]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


class _Audit(UseCase[_AuditEntry, None]):
    """A use case whose primary store is ``audit``."""

    async def execute(self) -> None:
        return None


class _AuditJob(Job[None]):
    def __init__(self, entries: Joined[RepoFor[_AuditEntry]]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


def _compose(
    tmp_path: Path, audit: dict[str, Any] | None, component: type[object] = _Audited
) -> Application:
    stores: dict[str, Any] = {
        "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}
    }
    if audit is not None:
        stores["audit"] = audit
    source = composition_source(
        ConfigContext.from_dict(
            {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
        ),
        app=AppConfig(name="api"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    if issubclass(component, Job):
        return compose_application(source, Serving(_Invoice, jobs=(component,)))
    return compose_application(source, Serving(_Invoice, use_cases=(component,)))


@pytest.mark.parametrize("component", [_Audited, _AuditJob], ids=["use-case", "job"])
@pytest.mark.parametrize(
    "audit",
    [
        pytest.param({"backend": "mongo", "url": "mongodb://audit"}, id="mongo-no-transactions"),
        pytest.param({"backend": "dynamodb"}, id="dynamodb"),
    ],
)
def test_joined_on_a_store_that_cannot_roll_back_fails_the_composition(
    tmp_path: Path, audit: dict[str, Any], component: type[object]
) -> None:
    with pytest.raises(JoinedAutocommitStore) as caught:
        _compose(tmp_path, audit, component)

    issue = caught.value.issue
    assert (issue.code, issue.component, issue.field) == (
        UseCaseErrorCode.JOINED_AUTOCOMMIT_STORE,
        component.__qualname__,
        "audit",
    )


@pytest.mark.parametrize("component", [_Audited, _Audit], ids=["joined", "primary"])
def test_a_written_store_without_a_unit_of_work_fails_the_composition(
    tmp_path: Path, component: type[object]
) -> None:
    audit = {"backend": "mongo", "url": "mongodb://audit", "transactions": True}

    with pytest.raises(UncomposedWriteStore) as caught:
        _compose(tmp_path, audit, component)

    issue = caught.value.issue
    assert (issue.code, issue.component, issue.field) == (
        UseCaseErrorCode.UNCOMPOSED_WRITE_STORE,
        component.__qualname__,
        "audit",
    )


def test_joined_on_an_undeclared_store_fails_the_composition(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"Joined in _Audited needs stores\.audit"):
        _compose(tmp_path, None)
