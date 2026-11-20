"""A job writes to a model chosen at call time through the catalog, over two SQLite stores."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from loom.composition import AppConfig, Application
from loom.core.command import Command
from loom.core.config import ConfigContext
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import OnConflict, RepositoryCatalog
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


class AcmeRows(Command, frozen=True):
    table: str
    ids: tuple[int, ...]


class AcmeNewRow(Command, frozen=True):
    id: int


class AcmeWriteRows(Job[int]):
    __store__ = "ledger"

    def __init__(self, catalog: RepositoryCatalog) -> None:
        self.catalog = catalog

    async def execute(self, rows: AcmeRows = Input()) -> int:
        inserted = await self.catalog.for_table(rows.table).create_many(
            [AcmeNewRow(id=row_id) for row_id in rows.ids], on_conflict=OnConflict.IGNORE
        )
        return len(inserted)


class TestRepositoryCatalogJob:
    @staticmethod
    def row_ids(database: Path, table: str) -> list[int]:
        with sqlite3.connect(database) as connection:
            rows = connection.execute(f'SELECT id FROM "{table}" ORDER BY id')
            return [row_id for (row_id,) in rows]

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
            first = await application.kernel.invoker.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_orders", "ids": [1, 2]}
            )
            second = await application.kernel.invoker.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_orders", "ids": [2, 3]}
            )
            other = await application.kernel.invoker.invoke(
                AcmeWriteRows, payload={"table": "catalog_flow_refunds", "ids": [9]}
            )

        _, ledger = databases
        assert (first, second, other) == (2, 1, 1)
        assert self.row_ids(ledger, "catalog_flow_orders") == [1, 2, 3]
        assert self.row_ids(ledger, "catalog_flow_refunds") == [9]
