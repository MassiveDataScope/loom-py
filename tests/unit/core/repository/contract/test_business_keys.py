from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import uuid4

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.errors import BoundaryValidationError, Conflict
from loom.core.stores.repository import BulkCreatable, Creatable, OnConflict, Readable, Updatable

from .conftest import (
    AcmeContractListing,
    AcmeContractSlot,
    BackendCase,
    CreateListing,
    CreateSlot,
    PartialListing,
    UpdateListing,
    require,
)

OpenRepository = Callable[..., AbstractAsyncContextManager[Any]]

_IDEALISTA = CreateListing(portal="idealista", external_id="123", title="flat")
_FOTOCASA = CreateListing(portal="fotocasa", external_id="123", title="house")
_DERIVED_ID = "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"


class TestContractDerivedKey:
    async def test_create_fills_the_id_the_business_key_derives(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create(_IDEALISTA)

            assert created.id == AcmeContractListing.key_for(_IDEALISTA) == _DERIVED_ID
            assert await listings.get_by_id(_DERIVED_ID) == created

    async def test_create_accepts_the_id_the_business_key_derives(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable)

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create(
                CreateListing(portal="idealista", external_id="123", id=_DERIVED_ID)
            )

        assert created.id == _DERIVED_ID

    async def test_create_refuses_an_id_the_business_key_does_not_derive(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            with pytest.raises(
                BoundaryValidationError, match=r"AcmeContractListing\.id is derived from"
            ):
                await listings.create(
                    CreateListing(portal="idealista", external_id="123", id="chosen")
                )

            assert await listings.get_by_id("chosen") is None
            assert await listings.get_by_id(_DERIVED_ID) is None

    async def test_creating_the_same_business_key_again_conflicts(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable)

        async with open_repository(AcmeContractListing) as listings:
            await listings.create(_IDEALISTA)

            with pytest.raises(Conflict):
                await listings.create(_IDEALISTA)

    async def test_update_refuses_a_business_key_field(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Updatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create(_IDEALISTA)

            with pytest.raises(BoundaryValidationError, match=r"\(portal\) cannot be updated"):
                await listings.update(created.id, UpdateListing(portal="fotocasa"))

            assert await listings.get_by_id(created.id) == created

    async def test_update_changes_the_fields_outside_the_business_key(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Updatable)

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create(_IDEALISTA)

            updated = await listings.update(created.id, UpdateListing(title="penthouse"))

        assert (updated.id, updated.title) == (created.id, "penthouse")

    async def test_create_many_derives_every_id(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable)

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create_many([_IDEALISTA, _FOTOCASA])

        assert [row.id for row in created] == [
            AcmeContractListing.key_for(_IDEALISTA),
            AcmeContractListing.key_for(_FOTOCASA),
        ]

    async def test_ignore_drops_existing_and_repeated_business_keys(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable, Creatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            existing = await listings.create(_IDEALISTA)

            created = await listings.create_many(
                [_IDEALISTA, _FOTOCASA, _FOTOCASA], on_conflict=OnConflict.IGNORE
            )

            assert [row.portal for row in created] == ["fotocasa"]
            assert await listings.get_by_id(existing.id) == existing


class TestContractCreateCheckpoint:
    async def test_create_stores_a_model_whose_id_it_derived_on_construction(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)
        listing = AcmeContractListing(portal="idealista", external_id="123")

        async with open_repository(AcmeContractListing) as listings:
            created = await listings.create(listing)

            assert created.id == listing.id == _DERIVED_ID
            assert await listings.get_by_id(_DERIVED_ID) == created

    async def test_create_refuses_a_stale_id_after_a_key_field_was_reassigned(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)
        listing = AcmeContractListing(portal="idealista", external_id="123")
        msgspec.structs.force_setattr(listing, "external_id", "999")

        async with open_repository(AcmeContractListing) as listings:
            with pytest.raises(
                BoundaryValidationError, match=r"AcmeContractListing\.id is derived from"
            ):
                await listings.create(listing)

            assert await listings.get_by_id(_DERIVED_ID) is None

    async def test_create_refuses_a_row_missing_a_business_key_field(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            with pytest.raises(BoundaryValidationError, match="'external_id' is missing"):
                await listings.create(PartialListing(portal="idealista"))

    async def test_create_many_refuses_a_row_missing_a_business_key_field_and_stores_nothing(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable, Readable)

        async with open_repository(AcmeContractListing) as listings:
            with pytest.raises(BoundaryValidationError, match="'external_id' is missing"):
                await listings.create_many([_IDEALISTA, PartialListing(portal="fotocasa")])

            assert await listings.get_by_id(_DERIVED_ID) is None


class TestContractUniqueBusinessKey:
    """A business key under a random id is unique by constraint where the store has one."""

    async def test_a_second_row_with_the_same_business_key_conflicts(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, Creatable)
        _require_unique(case)

        async with open_repository(AcmeContractSlot) as slots:
            await slots.create(CreateSlot(id=str(uuid4()), portal="idealista", slot=1))

            with pytest.raises(Conflict):
                await slots.create(CreateSlot(id=str(uuid4()), portal="idealista", slot=1))

    async def test_ignore_drops_the_rows_whose_business_key_exists(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        require(case, BulkCreatable, Creatable)
        _require_unique(case)

        async with open_repository(AcmeContractSlot) as slots:
            await slots.create(CreateSlot(id=str(uuid4()), portal="idealista", slot=1))

            created = await slots.create_many(
                [
                    CreateSlot(id=str(uuid4()), portal="idealista", slot=1),
                    CreateSlot(id=str(uuid4()), portal="idealista", slot=2),
                    CreateSlot(id=str(uuid4()), portal="idealista", slot=2),
                ],
                on_conflict=OnConflict.IGNORE,
            )

        assert [row.slot for row in created] == [2]

    async def test_a_store_that_cannot_enforce_the_key_refuses_the_model(
        self, case: BackendCase, open_repository: OpenRepository
    ) -> None:
        if not case.refuses_unhashed_business_key:
            pytest.skip(f"{case.name} does not refuse a business key without a hash")

        with pytest.raises(ConfigError, match="AcmeContractSlot declares a business key"):
            async with open_repository(AcmeContractSlot):
                pass


def _require_unique(case: BackendCase) -> None:
    if not case.enforces_unique:
        pytest.skip(f"{case.name} does not enforce a business key under a random id")
