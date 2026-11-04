"""A ``dynamodb`` store naming only its table reaches the emulator through ``aws:``."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Any

import pytest

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, Field, Integer, String
from loom.core.persistence import PersistenceWiring
from loom.core.repository.abc import Creatable, Readable
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from tests.helpers.persistence import build_backend
from tests.integration.aws.conftest import AwsClientFactory, ConfigWriter, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


class Gadget(BaseModel):
    __tablename__ = "gadgets"
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""


@pytest.fixture
def dynamodb(aws: AwsClientFactory) -> Any:
    return aws("dynamodb")


@pytest.fixture
def table(dynamodb: Any) -> Iterator[str]:
    name = unique("loom-it-gadgets")
    dynamodb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName=name)
    yield name
    dynamodb.delete_table(TableName=name)


@pytest.fixture
def wiring(write_config: ConfigWriter, table: str) -> PersistenceWiring:
    path = write_config(
        {
            "app": {"name": "loom-it-dynamodb"},
            "stores": {"default": {"backend": "dynamodb", "table": table}},
        }
    )
    return build_backend(DynamoDBBackend(), ConfigContext.from_yaml(str(path)), (Gadget,))


async def test_the_readiness_probe_finds_the_table(wiring: PersistenceWiring) -> None:
    assert wiring.readiness is not None
    assert await wiring.readiness() is True


async def test_a_repository_round_trips_through_the_table(
    wiring: PersistenceWiring, dynamodb: Any, table: str
) -> None:
    container = LoomContainer()
    wiring.repo_registration_module(container)

    created = await container.resolve(Creatable[Gadget]).create(Gadget(id=7, name="lamp"))

    assert created == Gadget(id=7, name="lamp")
    assert await container.resolve(Readable[Gadget]).get_by_id(7) == created
    item = dynamodb.get_item(TableName=table, Key={"id": {"N": "7"}})["Item"]
    assert item["name"] == {"S": "lamp"}
