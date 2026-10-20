from __future__ import annotations

from uuid import UUID

import pytest

from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import UnsupportedQuery
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB

from .conftest import FakeClient, Product, ProductCreate, ProductUpdate

pytestmark = pytest.mark.asyncio


def _repo(client: FakeClient) -> RepositoryDynamoDB[Product, int]:
    return RepositoryDynamoDB(client=client, table_name="products", model=Product)


async def test_create_then_get_by_id_roundtrips(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    created = await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    assert created == Product(id=1, name="Widget", price=9.5)
    fetched = await repo.get_by_id(1)
    assert fetched == Product(id=1, name="Widget", price=9.5)


async def test_create_encodes_floats_as_decimal(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    await repo.create(ProductCreate(id=1, name="Widget", price=9.5))

    # Floats cross the wire as low-level ``N`` AttributeValues (never native floats).
    assert fake_client.items["products"]["1"]["price"] == {"N": "9.5"}


async def test_get_by_non_key_field_raises_unsupported_query(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    with pytest.raises(UnsupportedQuery, match="get_by\\('name'\\)") as exc_info:
        await repo.get_by("name", "Bolt")

    assert exc_info.value.code == "unsupported_query"
    assert exc_info.value.backend == "dynamodb"
    assert exc_info.value.model == "Product"


async def test_exists_by_primary_key(fake_client: FakeClient) -> None:
    """The contract suite skips exists_by for DynamoDB (no Countable): pin the key path here."""
    repo = _repo(fake_client)
    await repo.create(ProductCreate(id=3, name="Nut"))

    assert await repo.exists_by("id", 3) is True
    assert await repo.exists_by("id", 999) is False


async def test_exists_by_non_key_field_raises_unsupported_query(fake_client: FakeClient) -> None:
    repo = _repo(fake_client)

    with pytest.raises(UnsupportedQuery, match="exists_by\\('name'\\)"):
        await repo.exists_by("name", "Nut")


async def test_update_conditional_failure_returns_none(fake_client: FakeClient) -> None:
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


async def test_entity_name_is_the_model_table_name(fake_client: FakeClient) -> None:
    # Single-table design: the DynamoDB table name is shared, the model name is not.
    repo = RepositoryDynamoDB(client=fake_client, table_name="shared", model=Product)

    assert repo.entity_name == "products"
    assert repo.model is Product


class _Badge(BaseModel):
    __tablename__ = "badges"
    id: UUID = ColumnField(primary_key=True)
    label: str = ""


class _BadgeUpdate(BaseModel):
    label: str = ""


_BADGE_ID = UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")


def _badge_repo(client: FakeClient | None = None) -> RepositoryDynamoDB[_Badge, UUID]:
    return RepositoryDynamoDB(
        client=client or FakeClient(key_name="id", table_names=("badges",)),
        table_name="badges",
        model=_Badge,
    )


async def test_a_uuid_primary_key_is_read_back_by_the_uuid() -> None:
    repo = _badge_repo()
    await repo.create(_Badge(id=_BADGE_ID, label="first"))

    assert await repo.get_by_id(_BADGE_ID) == _Badge(id=_BADGE_ID, label="first")


async def test_a_uuid_primary_key_answers_exists_by() -> None:
    repo = _badge_repo()
    await repo.create(_Badge(id=_BADGE_ID, label="first"))

    assert await repo.exists_by("id", _BADGE_ID) is True


async def test_an_item_is_updated_by_its_uuid_primary_key() -> None:
    repo = _badge_repo()
    await repo.create(_Badge(id=_BADGE_ID, label="first"))

    updated = await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

    assert updated == _Badge(id=_BADGE_ID, label="second")
    assert await repo.get_by_id(_BADGE_ID) == _Badge(id=_BADGE_ID, label="second")


async def test_an_item_is_deleted_by_its_uuid_primary_key() -> None:
    repo = _badge_repo()
    await repo.create(_Badge(id=_BADGE_ID, label="first"))

    assert await repo.delete(_BADGE_ID) is True
    assert await repo.get_by_id(_BADGE_ID) is None


async def test_an_update_keeps_the_key_in_its_stored_form() -> None:
    client = FakeClient(key_name="id", table_names=("badges",))
    repo = _badge_repo(client)
    await repo.create(_Badge(id=_BADGE_ID, label="first"))

    await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

    assert client.items["badges"][str(_BADGE_ID)]["id"] == {"S": str(_BADGE_ID)}


async def test_an_update_keeps_attributes_the_model_does_not_declare() -> None:
    # Single-table design: another writer stores a binary and two sets on the item.
    client = FakeClient(key_name="id", table_names=("badges",))
    repo = _badge_repo(client)
    await repo.create(_Badge(id=_BADGE_ID, label="first"))
    stored = client.items["badges"][str(_BADGE_ID)]
    stored["blob"] = {"B": b"\x00\x01"}
    stored["tags"] = {"SS": ["a", "b"]}
    stored["sizes"] = {"NS": ["1", "2.5"]}

    await repo.update(_BADGE_ID, _BadgeUpdate(label="second"))

    after = client.items["badges"][str(_BADGE_ID)]
    assert after["blob"] == {"B": b"\x00\x01"}
    assert sorted(after["tags"]["SS"]) == ["a", "b"]
    assert sorted(after["sizes"]["NS"]) == ["1", "2.5"]
