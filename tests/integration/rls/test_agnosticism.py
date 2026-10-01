"""Agnosticism evidence: three synthetic products run on one loom (FR-037, SC-008).

Each product is its own ``Application`` with its own ``MetaData``; none shares
vocabulary with a real product or with loom. Every scenario below mirrors one
line of the gate evidence, so a regression here is a regression of the proof.
"""

from __future__ import annotations

import pytest

from tests.integration.agnosticism import ledger, notes, sites
from tests.integration.rls.conftest import BootstrapFactory

pytestmark = pytest.mark.integration

PRODUCTS = (notes, sites, ledger)


@pytest.mark.parametrize("product", PRODUCTS, ids=lambda p: p.SCHEMA)
async def test_each_product_bootstraps_and_creates_its_schema_without_touching_loom(
    product, scoped_database: BootstrapFactory
) -> None:
    from loom.core.locator import Application
    from loom.core.repository.sqlalchemy.rls import create_schema, verify

    database = await scoped_database(product.SCHEMA)
    application = Application.from_models(
        product.MODELS, schema=product.SCHEMA, scope_bindings=product.SCOPE_BINDINGS
    )

    await create_schema(database.migrator, application)
    report = await verify(database.superuser, application)

    assert report.ok, report.findings
