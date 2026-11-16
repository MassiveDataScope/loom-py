"""Fixtures of the repository catalog tests: two stores, one table name served by both."""

from __future__ import annotations

from collections import Counter

import pytest

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.catalog import DEFAULT_STORE, StoreKey
from loom.core.stores.repository import RepositoryCatalog
from loom.core.stores.repository.registration import build_repository_registration_module
from loom.core.stores.repository.registry import RepositoryBuildContext

LEDGER = StoreKey("ledger")


class AcmeInvoice(BaseModel):
    __tablename__ = "catalog_invoices"

    id: int = ColumnField(primary_key=True)


class AcmeSharedDefault(BaseModel):
    __tablename__ = "catalog_shared"

    id: int = ColumnField(primary_key=True)


class AcmeOrder(BaseModel):
    __tablename__ = "catalog_orders"

    id: int = ColumnField(primary_key=True)


class AcmeSharedLedger(BaseModel):
    __tablename__ = "catalog_shared"

    id: int = ColumnField(primary_key=True)


class AcmeOrderCopy(BaseModel):
    __tablename__ = "catalog_orders"

    id: int = ColumnField(primary_key=True)


class AcmeUnserved(BaseModel):
    __tablename__ = "catalog_unserved"

    id: int = ColumnField(primary_key=True)


class CountingBuilder:
    """A default repository builder that records every repository it builds."""

    def __init__(self) -> None:
        self.builds: Counter[type] = Counter()

    def __call__(self, context: RepositoryBuildContext) -> object:
        self.builds[context.model] += 1
        return object()


@pytest.fixture
def served_models() -> dict[StoreKey, tuple[type[BaseModel], ...]]:
    return {
        DEFAULT_STORE: (AcmeInvoice, AcmeSharedDefault),
        LEDGER: (AcmeOrder, AcmeSharedLedger),
    }


@pytest.fixture
def builder() -> CountingBuilder:
    return CountingBuilder()


@pytest.fixture
def container(
    served_models: dict[StoreKey, tuple[type[BaseModel], ...]], builder: CountingBuilder
) -> LoomContainer:
    container = LoomContainer()
    for store, models in served_models.items():
        build_repository_registration_module(
            models=models,
            build_registered_repository=lambda context, registration: object(),
            default_builder=builder,
            store=store,
        )(container)
    return container


@pytest.fixture
def catalog(
    container: LoomContainer, served_models: dict[StoreKey, tuple[type[BaseModel], ...]]
) -> RepositoryCatalog:
    return RepositoryCatalog(container, served_models)
