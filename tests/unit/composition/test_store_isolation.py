"""Two applications in one process, each with its own store and models, do not step on each other.

ADR 0008: each store has its own metadata, compiled models, startup checks
and readiness. Composing a second application must neither drop the models
the first one compiled nor make the first one create the second one's tables.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from loom.composition import (
    AppConfig,
    Application,
    CompositionSource,
    Recipe,
    ResourceKind,
    compose_application,
)
from loom.core.command import Command
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.abc import PageParams
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.transactional import ForeignSessionError
from tests.helpers.composition import composition_source


class _Invoice(BaseModel):
    __tablename__ = "isolation_invoices"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    number: str = ColumnField(length=20)


class _Shipment(BaseModel):
    __tablename__ = "isolation_shipments"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    carrier: str = ColumnField(length=20)


class _AuditEntry(BaseModel):
    __tablename__ = "isolation_audit_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _NewInvoice(Command, frozen=True):
    number: str


class _Models:
    """Composes exactly the models it is given."""

    def __init__(self, *models: type[BaseModel]) -> None:
        self.models = models

    def recipe(self, source: CompositionSource) -> Recipe:
        del source
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(models=self.models, use_cases=(), interfaces=()),
            backend=SQLAlchemyBackend(),
        )

    def bind(self, application: Application) -> Application:
        return application


def _compose(database: Path, *models: type[BaseModel]) -> Application:
    ctx = ConfigContext.from_dict(
        {
            "stores": {
                "default": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{database}"}
            },
            "persistence": {"cursors": {"signing": "ephemeral"}},
        }
    )
    source = composition_source(ctx, app=AppConfig(name=database.stem))
    return compose_application(source, _Models(*models))


def _tables(database: Path) -> set[str]:
    with sqlite3.connect(database) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {name for (name,) in rows}


async def test_a_second_application_neither_drops_nor_adds_to_the_first_ones_models(
    tmp_path: Path,
) -> None:
    invoices, shipments = tmp_path / "invoices.db", tmp_path / "shipments.db"
    first = _compose(invoices, _Invoice)
    second = _compose(shipments, _Shipment)

    async with first.running(), second.running():
        repository = first.kernel.container.resolve_repo(_Invoice)
        created = await repository.create(_NewInvoice(number="F-1"))
        found = await repository.get_by_id(created.id)

    assert found is not None
    assert found.number == "F-1"
    assert _tables(invoices) == {"isolation_invoices"}
    assert _tables(shipments) == {"isolation_shipments"}


async def test_one_model_composed_twice_serves_each_application_from_its_own_store(
    tmp_path: Path,
) -> None:
    left, right = tmp_path / "left.db", tmp_path / "right.db"
    first = _compose(left, _Invoice)
    second = _compose(right, _Invoice)

    async with first.running(), second.running():
        await first.kernel.container.resolve_repo(_Invoice).create(_NewInvoice(number="L-1"))
        page = await second.kernel.container.resolve_repo(_Invoice).list_paginated(PageParams())

    assert page.items == ()
    assert _tables(left) == _tables(right) == {"isolation_invoices"}


async def test_a_repository_never_writes_through_another_applications_unit_of_work(
    tmp_path: Path,
) -> None:
    """Fail closed: B's repository inside A's unit of work would use A's engine and settings."""
    invoices, shipments = tmp_path / "a.db", tmp_path / "b.db"
    first = _compose(invoices, _Invoice)
    second = _compose(shipments, _Invoice)

    async with first.running(), second.running():
        foreign = second.kernel.container.resolve_repo(_Invoice)
        assert first.persistence.uow_factory is not None
        invoice = _NewInvoice(number="B-1")
        async with first.persistence.uow_factory.create():
            with pytest.raises(
                ForeignSessionError, match=r"b\.db.*cannot join the active session.*a\.db"
            ):
                await foreign.create(invoice)

    with sqlite3.connect(invoices) as a, sqlite3.connect(shipments) as b:
        counts = [db.execute("SELECT COUNT(*) FROM isolation_invoices").fetchone() for db in (a, b)]
    assert counts == [(0,), (0,)]


async def test_the_store_resource_is_named_by_its_key_and_probed_under_its_backend(
    tmp_path: Path,
) -> None:
    application = _compose(tmp_path / "ready.db", _Invoice)

    stores = [r.name for r in application.resources if r.kind is ResourceKind.STORE]
    async with application.running():
        ready = {key: await probe() for key, probe in application.readiness_probes().items()}

    assert stores == ["default"]
    assert ready == {"sqlalchemy": True}


def test_a_model_of_another_store_is_refused_until_the_invocation_scope_exists(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match="_AuditEntry belongs to store 'audit'"):
        _compose(tmp_path / "refused.db", _Invoice, _AuditEntry)
