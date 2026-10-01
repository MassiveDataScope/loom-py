"""Agnosticism evidence: three synthetic products run on one loom (FR-037, SC-008).

Each product is loaded from its own configuration file into its own
``Application`` with its own ``MetaData``; none shares vocabulary with a real
product or with loom. Every scenario mirrors one line of the gate evidence, so
a regression here is a regression of the proof.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from tests.integration.agnosticism import ledger, notes, sites
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

PRODUCTS = (notes, sites, ledger)


@pytest.mark.parametrize("product", PRODUCTS, ids=lambda p: p.SCHEMA)
async def test_each_product_bootstraps_and_creates_its_schema_without_touching_loom(
    product: ModuleType, scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    from loom.core.repository.sqlalchemy.rls import create_schema, verify

    database = await scoped_database(product.SCHEMA)
    application = application_for(product, database, tmp_path)

    await create_schema(database.migrator, application)
    report = await verify(database.superuser, application)

    assert report.ok, report.findings
