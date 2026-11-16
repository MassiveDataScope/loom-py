from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevated_scopes, elevation_scope
from loom.core.authz.product import clear_authz_product, load_authz_product, register_authz_product
from loom.core.config import ConfigError
from loom.stores.sqlalchemy.rls import elevate
from tests.helpers.elevation import MANAGE, Product
from tests.unit.core.authz._elevation_doubles import FakeSink

WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE, MANAGE), (Role("editor", (WRITE,)), Role("admin", (WRITE, MANAGE))))
BOUNDARY = Scope.of("b1")


@pytest.fixture(autouse=True)
def _no_product() -> Iterator[None]:
    clear_authz_product()
    yield
    clear_authz_product()


def _decision(role: str) -> Decision:
    return Decision(allowed=True, grant=Grant(subject="ana", role=role, scope=BOUNDARY))


class TestElevate:
    def test_no_product_is_registered_by_default(self) -> None:
        assert load_authz_product() is None

    def test_a_registered_product_is_what_the_loader_returns(self) -> None:
        product = Product(CATALOG)
        register_authz_product(product)

        assert load_authz_product() is product

    async def test_elevate_without_a_product_is_a_configuration_error(self) -> None:
        admin_decision = _decision("admin")
        async with elevation_scope(owns_transaction=True, sink=FakeSink()):
            with pytest.raises(ConfigError, match="loom.authz"):
                await elevate("editor", admin_decision, at=BOUNDARY)

    async def test_elevate_refuses_a_scope_the_product_did_not_map(self) -> None:
        register_authz_product(Product(CATALOG))
        admin_decision = _decision("admin")
        async with elevation_scope(owns_transaction=True, sink=FakeSink()):
            with pytest.raises(ConfigError, match="holder"):
                await elevate("holder", admin_decision, at=BOUNDARY)

    async def test_elevate_requires_the_mapped_permission_on_the_grant_role(self) -> None:
        register_authz_product(Product(CATALOG))
        editor_decision = _decision("editor")
        admin_decision = _decision("admin")
        async with elevation_scope(owns_transaction=True, sink=FakeSink()):
            with pytest.raises(PermissionError, match="editor"):
                await elevate("editor", editor_decision, at=BOUNDARY)
            await elevate("editor", admin_decision, at=BOUNDARY)
            assert elevated_scopes() == frozenset({"editor"})

    async def test_elevate_outside_an_execution_is_a_programming_error(self) -> None:
        register_authz_product(Product(CATALOG))
        admin_decision = _decision("admin")
        with pytest.raises(RuntimeError, match="frame"):
            await elevate("editor", admin_decision, at=BOUNDARY)

    async def test_an_explicit_catalog_overrides_the_product_catalog(self) -> None:
        register_authz_product(Product(CATALOG))
        narrower = RoleCatalog((WRITE, MANAGE), (Role("admin", (WRITE,)),))
        admin_decision = _decision("admin")
        async with elevation_scope(owns_transaction=True, sink=FakeSink()):
            with pytest.raises(PermissionError):
                await elevate("editor", admin_decision, at=BOUNDARY, catalog=narrower)
