from __future__ import annotations

from datetime import datetime

import pytest

from loom.core.model import BaseModel, ColumnField, DateTime
from loom.stores.mongo.query_compiler import MongoQueryCompiler


class Article(BaseModel):
    """Model with a non-``_id`` primary key so the ``slug`` -> ``_id`` mapping is exercised."""

    __tablename__ = "articles"

    slug: str = ColumnField(primary_key=True, length=32)
    title: str = ColumnField(length=64)
    views: int = ColumnField()
    published_at: datetime | None = ColumnField(DateTime(tz=False), nullable=True, default=None)


@pytest.fixture
def compiler() -> MongoQueryCompiler:
    return MongoQueryCompiler(Article, "slug")
