"""Composition refuses ``Joined`` on a store that cannot roll back (ADR 0008, T046).

Use cases and jobs alike. Until several stores compose (US7 PR 11) the joined
store's model is not served, so a use case that passes this check fails next
with :class:`~loom.core.use_case.errors.UnservedModelError`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application, CompositionSource, Recipe, compose_application
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence import SERVER_PROFILE
from loom.core.repository.abc import RepoFor
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.use_case import Joined, UnservedModelError, UseCase
from loom.core.use_case.errors import JoinedAutocommitStore, UseCaseErrorCode
from tests.helpers.composition import composition_source

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


class _AuditJob(Job[None]):
    def __init__(self, entries: Joined[RepoFor[_AuditEntry]]) -> None:
        self.entries = entries

    async def execute(self) -> None:
        return None


class _Serving:
    """Composes ``_Invoice`` and one use case or job."""

    def __init__(self, component: type[object]) -> None:
        self.component = component

    def recipe(self, source: CompositionSource) -> Recipe:
        del source
        is_job = issubclass(self.component, Job)
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(
                models=(_Invoice,),
                use_cases=() if is_job else cast(Any, (self.component,)),
                interfaces=(),
                jobs=cast(Any, (self.component,)) if is_job else (),
            ),
            backend=SQLAlchemyBackend(),
        )

    def bind(self, application: Application) -> Application:
        return application


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
    return compose_application(source, _Serving(component))


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


def test_joined_on_a_transactional_store_passes_the_check(tmp_path: Path) -> None:
    audit = {"backend": "mongo", "url": "mongodb://audit", "transactions": True}

    with pytest.raises(UnservedModelError, match="_AuditEntry"):
        _compose(tmp_path, audit)


def test_joined_on_an_undeclared_store_fails_the_composition(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"Joined in _Audited needs stores\.audit"):
        _compose(tmp_path, None)
