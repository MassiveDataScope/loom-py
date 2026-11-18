from __future__ import annotations

from typing import Any
from uuid import UUID

import msgspec
import pytest

from loom.core.errors import Conflict, UnsupportedQuery
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import OnConflict
from loom.stores.dynamodb.repository import RepositoryDynamoDB
from tests.helpers.aws import moto_client
from tests.helpers.dynamodb import MotoTable

from .conftest import Product, ProductCreate, ProductUpdate

pytestmark = pytest.mark.asyncio


def _repo(client: Any) -> RepositoryDynamoDB[Product, int]:
    return RepositoryDynamoDB(client=client, table_name="products", model=Product)


class PlainProductCreate(msgspec.Struct, omit_defaults=True):
    id: int
    name: str = ""


class PriceUpdate(msgspec.Struct, omit_defaults=True):
    price: float = 0.0


class TestDynamoRepositoryRoundTrip:
    async def test_create_then_get_by_id_roundtrips(self, dynamodb: Any) -> None:
        repo = _repo(dynamodb)

        created = await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        assert created == Product(id=1, name="Widget", price=9.5)
        fetched = await repo.get_by_id(1)
        assert fetched == Product(id=1, name="Widget", price=9.5)

    async def test_create_encodes_floats_as_decimal(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        repo = _repo(dynamodb)

        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        # Floats cross the wire as low-level ``N`` AttributeValues (never native floats).
        assert products_table.items_by_id()["1"]["price"] == {"N": "9.5"}

    async def test_create_stores_a_field_left_at_its_default(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        await _repo(dynamodb).create(ProductCreate(id=1))

        assert products_table.items_by_id()["1"] == {
            "id": {"N": "1"},
            "name": {"S": ""},
            "price": {"N": "0.0"},
        }

    async def test_create_many_stores_a_field_left_at_its_default(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        await _repo(dynamodb).create_many([ProductCreate(id=1)])

        assert products_table.items_by_id()["1"]["name"] == {"S": ""}

    async def test_a_plain_struct_stores_its_defaults_on_create(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        await _repo(dynamodb).create(PlainProductCreate(id=1))

        assert products_table.items_by_id()["1"] == {"id": {"N": "1"}, "name": {"S": ""}}

    async def test_update_keeps_the_fields_the_caller_did_not_send(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        repo = _repo(dynamodb)
        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        await repo.update(1, PriceUpdate(price=1.5))

        assert products_table.items_by_id()["1"]["name"] == {"S": "Widget"}

    async def test_get_by_non_key_field_raises_unsupported_query(self, dynamodb: Any) -> None:
        repo = _repo(dynamodb)

        with pytest.raises(UnsupportedQuery, match="get_by\\('name'\\)") as exc_info:
            await repo.get_by("name", "Bolt")

        assert exc_info.value.code == "unsupported_query"
        assert exc_info.value.backend == "dynamodb"
        assert exc_info.value.model == "Product"

    async def test_exists_by_primary_key(self, dynamodb: Any) -> None:
        """The contract suite skips exists_by for DynamoDB (no Countable): pin the key path here."""
        repo = _repo(dynamodb)
        await repo.create(ProductCreate(id=3, name="Nut"))

        assert await repo.exists_by("id", 3) is True
        assert await repo.exists_by("id", 999) is False

    async def test_exists_by_non_key_field_raises_unsupported_query(self, dynamodb: Any) -> None:
        repo = _repo(dynamodb)

        with pytest.raises(UnsupportedQuery, match="exists_by\\('name'\\)"):
            await repo.exists_by("name", "Nut")

    async def test_update_conditional_failure_returns_none(
        self, dynamodb: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Item vanishes between the read and the conditional write (concurrent delete);
        # the anti-resurrection condition fails, which maps to "no longer exists" (None).
        repo = _repo(dynamodb)
        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        original_get = dynamodb.get_item

        def _get_then_delete(**kwargs: object) -> dict[str, object]:
            response = original_get(**kwargs)  # type: ignore[arg-type]
            dynamodb.delete_item(TableName="products", Key={"id": {"N": "1"}})
            return response

        monkeypatch.setattr(dynamodb, "get_item", _get_then_delete)

        assert await repo.update(1, ProductUpdate(name="Gadget")) is None

    async def test_entity_name_is_the_model_table_name(self, dynamodb: Any) -> None:
        # Single-table design: the DynamoDB table name is shared, the model name is not.
        repo = RepositoryDynamoDB(client=dynamodb, table_name="shared", model=Product)

        assert repo.entity_name == "products"
        assert repo.model is Product


class AcmeDynamoBadge(BaseModel):
    __tablename__ = "badges"
    id: UUID = ColumnField(primary_key=True)
    label: str = ""


class _BadgeUpdate(BaseModel):
    label: str = ""


_BADGE_ID = UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")


@pytest.fixture
def badges(mocked_aws: None) -> MotoTable:
    return MotoTable(moto_client("dynamodb"), "badges", key_type="S").create()


def _badge_repo(client: Any) -> RepositoryDynamoDB[AcmeDynamoBadge, UUID]:
    return RepositoryDynamoDB(client=client, table_name="badges", model=AcmeDynamoBadge)


class TestDynamoUuidPrimaryKey:
    async def test_a_uuid_primary_key_is_read_back_by_the_uuid(self, badges: MotoTable) -> None:
        repo = _badge_repo(badges.client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.get_by_id(_BADGE_ID) == AcmeDynamoBadge(id=_BADGE_ID, label="first")

    async def test_a_uuid_primary_key_answers_exists_by(self, badges: MotoTable) -> None:
        repo = _badge_repo(badges.client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.exists_by("id", _BADGE_ID) is True

    async def test_an_item_is_updated_by_its_uuid_primary_key(self, badges: MotoTable) -> None:
        repo = _badge_repo(badges.client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        updated = await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        assert updated == AcmeDynamoBadge(id=_BADGE_ID, label="second")
        assert await repo.get_by_id(_BADGE_ID) == AcmeDynamoBadge(id=_BADGE_ID, label="second")

    async def test_an_item_is_deleted_by_its_uuid_primary_key(self, badges: MotoTable) -> None:
        repo = _badge_repo(badges.client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.delete(_BADGE_ID) is True
        assert await repo.get_by_id(_BADGE_ID) is None

    async def test_an_update_keeps_the_key_in_its_stored_form(self, badges: MotoTable) -> None:
        client = badges.client
        repo = _badge_repo(client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        assert badges.items_by_id()[str(_BADGE_ID)]["id"] == {"S": str(_BADGE_ID)}

    async def test_an_update_keeps_attributes_the_model_does_not_declare(
        self, badges: MotoTable
    ) -> None:
        # Single-table design: another writer stores a binary and two sets on the item.
        client = badges.client
        repo = _badge_repo(client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))
        stored = badges.items_by_id()[str(_BADGE_ID)]
        client.put_item(
            TableName="badges",
            Item={
                **stored,
                "blob": {"B": b"\x00\x01"},
                "tags": {"SS": ["a", "b"]},
                "sizes": {"NS": ["1", "2.5"]},
            },
        )

        await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        after = badges.items_by_id()[str(_BADGE_ID)]
        assert after["blob"] == {"B": b"\x00\x01"}
        assert sorted(after["tags"]["SS"]) == ["a", "b"]
        assert sorted(after["sizes"]["NS"]) == ["1", "2.5"]


class TestDynamoCreateMany:
    async def test_error_writes_every_item_in_input_order_one_transaction_per_hundred(
        self, dynamodb: Any, transactions: list[int], products_table: MotoTable
    ) -> None:
        keys = list(range(250, 0, -1))

        created = await _repo(dynamodb).create_many([ProductCreate(id=key) for key in keys])

        assert [item.id for item in created] == keys
        assert transactions == [100, 100, 50]
        assert len(products_table.items_by_id()) == 250

    async def test_error_on_an_existing_key_raises_conflict_and_cancels_only_its_chunk(
        self, dynamodb: Any, transactions: list[int], products_table: MotoTable
    ) -> None:
        repo = _repo(dynamodb)
        await repo.create(ProductCreate(id=500))
        batch = [ProductCreate(id=key) for key in range(1, 101)] + [
            ProductCreate(id=101),
            ProductCreate(id=500),
        ]

        with pytest.raises(Conflict, match="Product with id=500 already exists"):
            await repo.create_many(batch)

        assert transactions == [100]
        assert "101" not in products_table.items_by_id()

    async def test_error_on_a_key_repeated_in_the_input_raises_conflict_and_writes_nothing(
        self, dynamodb: Any, products_table: MotoTable
    ) -> None:
        batch = [ProductCreate(id=1), ProductCreate(id=2), ProductCreate(id=1)]

        with pytest.raises(Conflict, match="Product with id=1 already exists"):
            await _repo(dynamodb).create_many(batch)

        assert products_table.items_by_id() == {}

    async def test_ignore_writes_one_put_per_item_and_no_transaction(
        self, dynamodb: Any, transactions: list[int]
    ) -> None:
        created = await _repo(dynamodb).create_many(
            [ProductCreate(id=key) for key in range(150)], on_conflict=OnConflict.IGNORE
        )

        assert len(created) == 150
        assert transactions == []
