from __future__ import annotations

from uuid import UUID

import msgspec
import pytest

from loom.core.errors import Conflict, UnsupportedQuery
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.stores.repository import OnConflict

from .conftest import FakeClient, Product, ProductCreate, ProductUpdate

pytestmark = pytest.mark.asyncio


def _repo(client: FakeClient) -> RepositoryDynamoDB[Product, int]:
    return RepositoryDynamoDB(client=client, table_name="products", model=Product)


class PlainProductCreate(msgspec.Struct, omit_defaults=True):
    id: int
    name: str = ""


class PriceUpdate(msgspec.Struct, omit_defaults=True):
    price: float = 0.0


class TestDynamoRepositoryRoundTrip:
    async def test_create_then_get_by_id_roundtrips(self, fake_client: FakeClient) -> None:
        repo = _repo(fake_client)

        created = await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        assert created == Product(id=1, name="Widget", price=9.5)
        fetched = await repo.get_by_id(1)
        assert fetched == Product(id=1, name="Widget", price=9.5)

    async def test_create_encodes_floats_as_decimal(self, fake_client: FakeClient) -> None:
        repo = _repo(fake_client)

        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        # Floats cross the wire as low-level ``N`` AttributeValues (never native floats).
        assert fake_client.items["products"]["1"]["price"] == {"N": "9.5"}

    async def test_create_stores_a_field_left_at_its_default(self, fake_client: FakeClient) -> None:
        await _repo(fake_client).create(ProductCreate(id=1))

        assert fake_client.items["products"]["1"] == {
            "id": {"N": "1"},
            "name": {"S": ""},
            "price": {"N": "0.0"},
        }

    async def test_create_many_stores_a_field_left_at_its_default(
        self, fake_client: FakeClient
    ) -> None:
        await _repo(fake_client).create_many([ProductCreate(id=1)])

        assert fake_client.items["products"]["1"]["name"] == {"S": ""}

    async def test_a_plain_struct_stores_its_defaults_on_create(
        self, fake_client: FakeClient
    ) -> None:
        await _repo(fake_client).create(PlainProductCreate(id=1))

        assert fake_client.items["products"]["1"] == {"id": {"N": "1"}, "name": {"S": ""}}

    async def test_update_keeps_the_fields_the_caller_did_not_send(
        self, fake_client: FakeClient
    ) -> None:
        repo = _repo(fake_client)
        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        await repo.update(1, PriceUpdate(price=1.5))

        assert fake_client.items["products"]["1"]["name"] == {"S": "Widget"}

    async def test_get_by_non_key_field_raises_unsupported_query(
        self, fake_client: FakeClient
    ) -> None:
        repo = _repo(fake_client)

        with pytest.raises(UnsupportedQuery, match="get_by\\('name'\\)") as exc_info:
            await repo.get_by("name", "Bolt")

        assert exc_info.value.code == "unsupported_query"
        assert exc_info.value.backend == "dynamodb"
        assert exc_info.value.model == "Product"

    async def test_exists_by_primary_key(self, fake_client: FakeClient) -> None:
        """The contract suite skips exists_by for DynamoDB (no Countable): pin the key path here."""
        repo = _repo(fake_client)
        await repo.create(ProductCreate(id=3, name="Nut"))

        assert await repo.exists_by("id", 3) is True
        assert await repo.exists_by("id", 999) is False

    async def test_exists_by_non_key_field_raises_unsupported_query(
        self, fake_client: FakeClient
    ) -> None:
        repo = _repo(fake_client)

        with pytest.raises(UnsupportedQuery, match="exists_by\\('name'\\)"):
            await repo.exists_by("name", "Nut")

    async def test_update_conditional_failure_returns_none(self, fake_client: FakeClient) -> None:
        # Item vanishes between the read and the conditional write (concurrent delete);
        # the anti-resurrection condition fails, which maps to "no longer exists" (None).
        repo = _repo(fake_client)
        await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

        original_get = fake_client.get_item

        def _get_then_delete(**kwargs: object) -> dict[str, object]:
            response = original_get(**kwargs)  # type: ignore[arg-type]
            fake_client.items["products"].pop("1", None)
            return response

        fake_client.get_item = _get_then_delete  # type: ignore[method-assign]

        assert await repo.update(1, ProductUpdate(name="Gadget")) is None

    async def test_entity_name_is_the_model_table_name(self, fake_client: FakeClient) -> None:
        # Single-table design: the DynamoDB table name is shared, the model name is not.
        repo = RepositoryDynamoDB(client=fake_client, table_name="shared", model=Product)

        assert repo.entity_name == "products"
        assert repo.model is Product


class AcmeDynamoBadge(BaseModel):
    __tablename__ = "badges"
    id: UUID = ColumnField(primary_key=True)
    label: str = ""


class _BadgeUpdate(BaseModel):
    label: str = ""


_BADGE_ID = UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")


def _badge_repo(client: FakeClient | None = None) -> RepositoryDynamoDB[AcmeDynamoBadge, UUID]:
    return RepositoryDynamoDB(
        client=client or FakeClient(key_name="id", table_names=("badges",)),
        table_name="badges",
        model=AcmeDynamoBadge,
    )


class TestDynamoUuidPrimaryKey:
    async def test_a_uuid_primary_key_is_read_back_by_the_uuid(self) -> None:
        repo = _badge_repo()
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.get_by_id(_BADGE_ID) == AcmeDynamoBadge(id=_BADGE_ID, label="first")

    async def test_a_uuid_primary_key_answers_exists_by(self) -> None:
        repo = _badge_repo()
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.exists_by("id", _BADGE_ID) is True

    async def test_an_item_is_updated_by_its_uuid_primary_key(self) -> None:
        repo = _badge_repo()
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        updated = await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        assert updated == AcmeDynamoBadge(id=_BADGE_ID, label="second")
        assert await repo.get_by_id(_BADGE_ID) == AcmeDynamoBadge(id=_BADGE_ID, label="second")

    async def test_an_item_is_deleted_by_its_uuid_primary_key(self) -> None:
        repo = _badge_repo()
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        assert await repo.delete(_BADGE_ID) is True
        assert await repo.get_by_id(_BADGE_ID) is None

    async def test_an_update_keeps_the_key_in_its_stored_form(self) -> None:
        client = FakeClient(key_name="id", table_names=("badges",))
        repo = _badge_repo(client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))

        await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        assert client.items["badges"][str(_BADGE_ID)]["id"] == {"S": str(_BADGE_ID)}

    async def test_an_update_keeps_attributes_the_model_does_not_declare(self) -> None:
        # Single-table design: another writer stores a binary and two sets on the item.
        client = FakeClient(key_name="id", table_names=("badges",))
        repo = _badge_repo(client)
        await repo.create(AcmeDynamoBadge(id=_BADGE_ID, label="first"))
        stored = client.items["badges"][str(_BADGE_ID)]
        stored["blob"] = {"B": b"\x00\x01"}
        stored["tags"] = {"SS": ["a", "b"]}
        stored["sizes"] = {"NS": ["1", "2.5"]}

        await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

        after = client.items["badges"][str(_BADGE_ID)]
        assert after["blob"] == {"B": b"\x00\x01"}
        assert sorted(after["tags"]["SS"]) == ["a", "b"]
        assert sorted(after["sizes"]["NS"]) == ["1", "2.5"]


class TestDynamoCreateMany:
    async def test_error_writes_every_item_in_input_order_one_transaction_per_hundred(
        self, fake_client: FakeClient
    ) -> None:
        keys = list(range(250, 0, -1))

        created = await _repo(fake_client).create_many([ProductCreate(id=key) for key in keys])

        assert [item.id for item in created] == keys
        assert fake_client.transactions == [100, 100, 50]
        assert len(fake_client.items["products"]) == 250

    async def test_error_on_an_existing_key_raises_conflict_and_cancels_only_its_chunk(
        self, fake_client: FakeClient
    ) -> None:
        repo = _repo(fake_client)
        await repo.create(ProductCreate(id=500))
        batch = [ProductCreate(id=key) for key in range(1, 101)] + [
            ProductCreate(id=101),
            ProductCreate(id=500),
        ]

        with pytest.raises(Conflict, match="Product with id=500 already exists"):
            await repo.create_many(batch)

        assert fake_client.transactions == [100]
        assert "101" not in fake_client.items["products"]

    async def test_error_on_a_key_repeated_in_the_input_raises_conflict_and_writes_nothing(
        self, fake_client: FakeClient
    ) -> None:
        batch = [ProductCreate(id=1), ProductCreate(id=2), ProductCreate(id=1)]

        with pytest.raises(Conflict, match="Product with id=1 already exists"):
            await _repo(fake_client).create_many(batch)

        assert fake_client.items["products"] == {}

    async def test_ignore_writes_one_put_per_item_and_no_transaction(
        self, fake_client: FakeClient
    ) -> None:
        created = await _repo(fake_client).create_many(
            [ProductCreate(id=key) for key in range(150)], on_conflict=OnConflict.IGNORE
        )

        assert len(created) == 150
        assert fake_client.transactions == []
