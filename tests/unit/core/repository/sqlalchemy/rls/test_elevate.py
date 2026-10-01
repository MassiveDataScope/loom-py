from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

import pytest
from sqlalchemy import MetaData

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevated_scopes, elevation_scope
from loom.core.authz.product import clear_authz_product, load_authz_product, register_authz_product
from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField, RowScoped
from loom.core.model.types import Integer, String, Text
from loom.core.repository.sqlalchemy.rls import elevate, validate_elevations
from tests.unit.core.authz._elevation_doubles import FakeSink

WRITE = Permission("rows.write")
MANAGE = Permission("members.manage")
CATALOG = RoleCatalog((WRITE, MANAGE), (Role("editor", (WRITE,)), Role("admin", (WRITE, MANAGE))))
BOUNDARY = Scope.of("b1")


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    key: str = ColumnField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ColumnField(Text, scope="editor", on="write", elevable=True)


@dataclass
class Product:
    catalog: RoleCatalog = CATALOG
    delegate: Permission = MANAGE
    elevations: dict[str, Permission] = field(default_factory=lambda: {"editor": MANAGE})


@pytest.fixture(autouse=True)
def _no_product() -> Iterator[None]:
    clear_authz_product()
    yield
    clear_authz_product()


def _scoped():
    metadata = MetaData()
    compile_all(Note, metadata=metadata)
    return scoped_tables(metadata)


def _decision(role: str) -> Decision:
    return Decision(allowed=True, grant=Grant(subject="ana", role=role, scope=BOUNDARY))


def test_no_product_is_registered_by_default() -> None:
    assert load_authz_product() is None


def test_a_registered_product_is_what_the_loader_returns() -> None:
    product = Product()
    register_authz_product(product)

    assert load_authz_product() is product


def test_validate_elevations_accepts_elevable_scopes_only() -> None:
    validate_elevations(Product(), _scoped())

    with pytest.raises(ConfigError, match=r"holder.*elevable"):
        validate_elevations(Product(elevations={"holder": MANAGE}), _scoped())
    with pytest.raises(ConfigError, match=r"nope"):
        validate_elevations(Product(elevations={"nope": MANAGE}), _scoped())


async def test_elevate_without_a_product_is_a_configuration_error() -> None:
    async with elevation_scope(owns_transaction=True, sink=FakeSink()):
        with pytest.raises(ConfigError, match="loom.authz"):
            await elevate("editor", _decision("admin"), at=BOUNDARY)


async def test_elevate_refuses_a_scope_the_product_did_not_map() -> None:
    register_authz_product(Product())
    async with elevation_scope(owns_transaction=True, sink=FakeSink()):
        with pytest.raises(ConfigError, match="holder"):
            await elevate("holder", _decision("admin"), at=BOUNDARY)


async def test_elevate_requires_the_mapped_permission_on_the_grant_role() -> None:
    register_authz_product(Product())
    async with elevation_scope(owns_transaction=True, sink=FakeSink()):
        with pytest.raises(PermissionError, match="editor"):
            await elevate("editor", _decision("editor"), at=BOUNDARY)
        await elevate("editor", _decision("admin"), at=BOUNDARY)
        assert elevated_scopes() == frozenset({"editor"})


async def test_elevate_outside_an_execution_is_a_programming_error() -> None:
    register_authz_product(Product())
    with pytest.raises(RuntimeError, match="frame"):
        await elevate("editor", _decision("admin"), at=BOUNDARY)


async def test_an_explicit_catalog_overrides_the_product_catalog() -> None:
    register_authz_product(Product())
    narrower = RoleCatalog((WRITE, MANAGE), (Role("admin", (WRITE,)),))
    async with elevation_scope(owns_transaction=True, sink=FakeSink()):
        with pytest.raises(PermissionError):
            await elevate("editor", _decision("admin"), at=BOUNDARY, catalog=narrower)
