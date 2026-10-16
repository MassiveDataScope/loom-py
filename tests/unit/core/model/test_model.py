from __future__ import annotations

from typing import Annotated

import msgspec
import pytest
from msgspec import UnsetType

from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    OnDelete,
    ProjectionField,
    RelationField,
    get_column_fields,
    get_id_attribute,
    get_id_attributes,
    get_projections,
    get_relations,
    get_table_name,
)


class _Product(BaseModel):
    __tablename__ = "products"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()
    active: bool = ColumnField(default=True)

    reviews: list[dict[str, object]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        on_delete=OnDelete.CASCADE,
        profiles=("with_details",),
        depends_on=("reviews:product_id",),
    )
    count_reviews: int = ProjectionField(
        loader=None,
        profiles=("with_details",),
        depends_on=("reviews:product_id",),
        default=0,
    )
    rating: Annotated[int, "stars"] = ProjectionField(loader=None, default=0)


class _Line(BaseModel):
    __tablename__ = "lines"
    order_id: int = ColumnField(primary_key=True)
    line_no: int = ColumnField(primary_key=True)


def test_annotated_projection_keeps_its_metadata() -> None:
    types = {field.name: field.type for field in msgspec.structs.fields(_Product)}
    assert types["rating"] == Annotated[int, "stars"] | UnsetType


class TestModelIntrospection:
    def test_column_fields(self) -> None:
        fields = get_column_fields(_Product)
        assert set(fields.keys()) == {"id", "name", "price", "active"}
        assert fields["id"].field.primary_key is True
        assert fields["name"].column_type.type_name == "String"
        assert fields["name"].column_type.args == (120,)
        assert fields["price"].column_type.type_name == "Float"

    def test_id_attribute_and_table_name(self) -> None:
        assert get_id_attribute(_Product) == "id"
        assert get_table_name(_Product) == "products"

    def test_id_attributes_list_every_key_column_in_declaration_order(self) -> None:
        assert get_id_attributes(_Product) == ("id",)
        assert get_id_attributes(_Line) == ("order_id", "line_no")

    def test_relations_and_projections(self) -> None:
        assert "reviews" in get_relations(_Product)
        assert get_relations(_Product)["reviews"].cardinality == Cardinality.ONE_TO_MANY
        assert "count_reviews" in get_projections(_Product)
        assert get_projections(_Product)["count_reviews"].default == 0


class TestStructBehavior:
    def test_frozen_and_omit_defaults(self) -> None:
        p = _Product(id=1, name="x", price=1.0, active=False)
        data = msgspec.json.decode(msgspec.json.encode(p))
        # Non-default column fields are present
        assert set(data.keys()) == {"id", "name", "price", "active"}
        # Frozen
        with pytest.raises(AttributeError):
            p.name = "y"

    def test_camel_rename_and_loaded_fields(self) -> None:
        p = _Product(id=1, name="x", price=1.0, active=True, count_reviews=5)
        data = msgspec.json.decode(msgspec.json.encode(p))
        assert data["countReviews"] == 5
