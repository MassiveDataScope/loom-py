"""A DynamoDB repository refuses a model whose primary key is composite."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB

from .conftest import FakeClient


class _Line(BaseModel):
    __tablename__ = "lines"
    order_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)


def test_a_composite_primary_key_is_refused(fake_client: FakeClient) -> None:
    with pytest.raises(ConfigError, match=r"_Line has the composite primary key \(order_id, id\)"):
        RepositoryDynamoDB(client=fake_client, table_name="lines", model=_Line)
