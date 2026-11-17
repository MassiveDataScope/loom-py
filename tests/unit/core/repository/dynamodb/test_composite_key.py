"""A DynamoDB repository refuses a model whose primary key is composite."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.stores.dynamodb.repository import RepositoryDynamoDB


class AcmeDynamoLine(BaseModel):
    __tablename__ = "lines"
    order_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)


class TestDynamoCompositeKey:
    def test_a_composite_primary_key_is_refused(self, dynamodb: Any) -> None:
        with pytest.raises(
            ConfigError, match=r"AcmeDynamoLine has the composite primary key \(order_id, id\)"
        ):
            RepositoryDynamoDB(client=dynamodb, table_name="lines", model=AcmeDynamoLine)
