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
import yaml

from loom.core.locator import Application, load_application
from tests.integration.agnosticism import ledger, notes, sites
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase

pytestmark = pytest.mark.integration

PRODUCTS = (notes, sites, ledger)


def _application(product: ModuleType, database: ScopedDatabase, tmp_path: Path) -> Application:
    config = {
        "app": {
            "name": product.SCHEMA,
            "discovery": {"mode": "modules", "modules": {"include": [product.__name__]}},
        },
        "database": {
            "url": database.write,
            "schema": {
                "mode": "external",
                "name": product.SCHEMA,
                "roles": {
                    "owner": f"{product.SCHEMA}_owner",
                    "migrator": f"{product.SCHEMA}_migrator",
                },
                "database_users": {
                    user: {"login": True, "access": access}
                    for user, access in product.USERS.items()
                },
                "scopes": dict(product.SCOPE_BINDINGS),
            },
        },
    }
    path = tmp_path / f"{product.SCHEMA}.yaml"
    path.write_text(yaml.safe_dump(config))
    return load_application(str(path))


@pytest.mark.parametrize("product", PRODUCTS, ids=lambda p: p.SCHEMA)
async def test_each_product_bootstraps_and_creates_its_schema_without_touching_loom(
    product: ModuleType, scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    from loom.core.repository.sqlalchemy.rls import create_schema, verify

    database = await scoped_database(product.SCHEMA)
    application = _application(product, database, tmp_path)

    await create_schema(database.migrator, application)
    report = await verify(database.superuser, application)

    assert report.ok, report.findings
