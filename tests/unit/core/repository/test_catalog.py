"""The repository catalog resolves the bound repository by model or by table name."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.model.introspection import get_table_name
from loom.core.stores.catalog import DEFAULT_STORE, StoreKey
from loom.core.stores.repository import RepositoryCatalog
from loom.core.stores.repository.registry import RepositoryToken
from loom.testing.overrides import Override
from tests.unit.core.repository.conftest import (
    LEDGER,
    AcmeInvoice,
    AcmeOrder,
    AcmeOrderCopy,
    AcmeSharedDefault,
    AcmeSharedLedger,
    AcmeUnserved,
    CountingBuilder,
)


class TestRepositoryCatalogLookup:
    @pytest.mark.parametrize("model", [AcmeInvoice, AcmeOrder, AcmeSharedLedger])
    def test_a_served_model_resolves_the_repository_bound_for_it(
        self, catalog: RepositoryCatalog, container: LoomContainer, model: type[BaseModel]
    ) -> None:
        assert catalog[model] is container.resolve_repo(model)

    def test_an_override_double_is_returned(
        self, catalog: RepositoryCatalog, container: LoomContainer
    ) -> None:
        double = object()
        container.register_instance(RepositoryToken(AcmeOrder), double)

        assert catalog[AcmeOrder] is double

    def test_temporary_repository_override_reaches_catalog_and_restores(
        self, catalog: RepositoryCatalog, container: LoomContainer
    ) -> None:
        original = catalog[AcmeOrder]
        double = object()

        with Override.repository(AcmeOrder, double).applied(container):
            assert catalog[AcmeOrder] is double
            assert catalog.for_table("catalog_orders", store=LEDGER) is double

        assert catalog[AcmeOrder] is original

    def test_an_unserved_model_names_itself(self, catalog: RepositoryCatalog) -> None:
        with pytest.raises(KeyError, match="AcmeUnserved"):
            catalog[AcmeUnserved]

    def test_a_lookup_builds_each_repository_once(
        self, catalog: RepositoryCatalog, builder: CountingBuilder
    ) -> None:
        for _ in range(5):
            catalog[AcmeOrder]
            catalog.for_table("catalog_orders")

        assert builder.builds == {AcmeOrder: 1}


class TestRepositoryCatalogForTable:
    @pytest.mark.parametrize(
        ("table", "model"),
        [("catalog_invoices", AcmeInvoice), ("catalog_orders", AcmeOrder)],
    )
    def test_a_table_served_by_one_store_resolves_without_a_store(
        self, catalog: RepositoryCatalog, table: str, model: type[BaseModel]
    ) -> None:
        assert catalog.for_table(table) is catalog[model]

    @pytest.mark.parametrize(
        ("store", "model"), [(DEFAULT_STORE, AcmeSharedDefault), (LEDGER, AcmeSharedLedger)]
    )
    def test_a_store_picks_the_model_among_stores_serving_the_table(
        self, catalog: RepositoryCatalog, store: StoreKey, model: type[BaseModel]
    ) -> None:
        assert catalog.for_table("catalog_shared", store=store) is catalog[model]

    def test_a_table_served_by_two_stores_names_both_and_asks_for_a_store(
        self, catalog: RepositoryCatalog
    ) -> None:
        with pytest.raises(ValueError, match=f"{DEFAULT_STORE}, {LEDGER}.*store="):
            catalog.for_table("catalog_shared")

    def test_an_unknown_table_names_it_and_the_stores_searched(
        self, catalog: RepositoryCatalog
    ) -> None:
        with pytest.raises(KeyError, match=f"catalog_missing.*{DEFAULT_STORE}, {LEDGER}"):
            catalog.for_table("catalog_missing")

    def test_a_table_of_another_store_is_unknown_in_the_store_asked(
        self, catalog: RepositoryCatalog
    ) -> None:
        with pytest.raises(KeyError, match=f"catalog_orders.*{DEFAULT_STORE}"):
            catalog.for_table("catalog_orders", store=DEFAULT_STORE)


class TestRepositoryCatalogIndex:
    def test_no_table_name_is_read_after_construction(
        self,
        monkeypatch: pytest.MonkeyPatch,
        container: LoomContainer,
        served_models: dict[StoreKey, tuple[type[BaseModel], ...]],
    ) -> None:
        reads: list[type] = []

        def spy(model: type) -> str:
            reads.append(model)
            return get_table_name(model)

        monkeypatch.setattr("loom.core.stores.repository.catalog.get_table_name", spy)
        catalog = RepositoryCatalog(container, served_models)
        built = len(reads)

        for _ in range(3):
            catalog.for_table("catalog_orders")
            catalog.for_table("catalog_shared", store=LEDGER)

        assert (built, len(reads)) == (4, 4)

    def test_a_store_serving_two_models_of_one_table_is_refused(
        self, container: LoomContainer
    ) -> None:
        with pytest.raises(ValueError, match="ledger.*catalog_orders.*AcmeOrder.*AcmeOrderCopy"):
            RepositoryCatalog(container, {LEDGER: (AcmeOrder, AcmeOrderCopy)})


class TestFromRepositories:
    def test_indexes_models_by_their_declared_store_and_table(self) -> None:
        class AcmeLedgerRecord(BaseModel):
            __tablename__ = "ledger_records"
            __store__ = "ledger"
            id: int

        repository = object()
        catalog = RepositoryCatalog.from_repositories({AcmeLedgerRecord: repository})

        assert catalog[AcmeLedgerRecord] is repository
        assert catalog.for_table("ledger_records", store=LEDGER) is repository
        with pytest.raises(KeyError, match="ledger_records"):
            catalog.for_table("ledger_records", store=DEFAULT_STORE)
