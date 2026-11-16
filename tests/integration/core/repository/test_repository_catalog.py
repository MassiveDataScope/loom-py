"""A job writes to a model chosen at call time through the catalog, over two SQLite stores."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application
from loom.core.command import Command
from loom.core.config import ConfigContext
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import BulkCreatable, OnConflict, RepositoryCatalog
from loom.core.use_case import Input
from tests.helpers.composition import Serving, composition_project

pytestmark = pytest.mark.timeout(30)


class AcmeDefaultInvoice(BaseModel):
    __tablename__ = "catalog_flow_invoices"

    id: int = ColumnField(primary_key=True)


class AcmeLedgerOrder(BaseModel):
    __tablename__ = "catalog_flow_orders"
    __store__ = "ledger"

    id: int = ColumnField(primary_key=True)


class AcmeLedgerRefund(BaseModel):
    __tablename__ = "catalog_flow_refunds"
    __store__ = "ledger"

    id: int = ColumnField(primary_key=True)


class _Rows(Command, frozen=True):
    table: str
    ids: tuple[int, ...]


class _NewRow(Command, frozen=True):
    id: int


class AcmeWriteRows(Job[int]):
    __store__ = "ledger"

    def __init__(self, catalog: RepositoryCatalog) -> None:
        self.catalog = catalog

    async def execute(self, rows: _Rows = Input()) -> int:
        repository = cast(BulkCreatable[Any], self.catalog.for_table(rows.table))
        inserted = await repository.create_many(
            [_NewRow(id=row_id) for row_id in rows.ids], on_conflict=OnConflict.IGNORE
        )
        return len(inserted)


def _row_ids(database: Path, table: str) -> list[int]:
    with sqlite3.connect(database) as connection:
        return [row_id for (row_id,) in connection.execute(f'SELECT id FROM "{table}" ORDER BY id')]


class TestRepositoryCatalogJob:
    @pytest.fixture
    def databases(self, tmp_path: Path) -> tuple[Path, Path]:
        return tmp_path / "default.db", tmp_path / "ledger.db"

    @pytest.fixture
    def application(self, databases: tuple[Path, Path]) -> Application:
        default, ledger = databases
        ctx = ConfigContext.from_dict(
            {
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{default}"},
                    "ledger": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{ledger}"},
                    "settings": {"cursors": {"signing": "ephemeral"}},
                }
            }
        )
        project = composition_project(ctx, app=AppConfig(name="ledger"))
        adapter = Serving(
            AcmeDefaultInvoice, AcmeLedgerOrder, AcmeLedgerRefund, jobs=(AcmeWriteRows,)
        )
        return project.compose(adapter)

    async def test_the_job_writes_to_the_table_named_at_call_time_and_ignores_existing_rows(
        self, application: Application, databases: tuple[Path, Path]
    ) -> None:
        async with application.running():
            first = await application.kernel.app.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_orders", "ids": [1, 2]}
            )
            second = await application.kernel.app.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_orders", "ids": [2, 3]}
            )
            other = await application.kernel.app.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_refunds", "ids": [9]}
            )

        _, ledger = databases
        assert (first, second, other) == (2, 1, 1)
        assert _row_ids(ledger, "catalog_flow_orders") == [1, 2, 3]
        assert _row_ids(ledger, "catalog_flow_refunds") == [9]
