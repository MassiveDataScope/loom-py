from __future__ import annotations

import pickle

import pytest

from loom.core.errors import InvalidType
from loom.core.model import BaseModel, ColumnField, RelationField
from loom.core.model.enums import Cardinality
from loom.core.model.types import Integer, Text
from loom.testing import make


class Customer(BaseModel):
    __tablename__ = "customers"
    id: int = ColumnField(Integer, primary_key=True)
    full_name: str = ColumnField(Text)


class Order(BaseModel):
    __tablename__ = "orders"
    id: int = ColumnField(Integer, primary_key=True)
    customer_id: int = ColumnField(Integer)
    placed_on: str = ColumnField(Text)
    customer: Customer = RelationField(
        foreign_key="customer_id", cardinality=Cardinality.MANY_TO_ONE
    )


class PriorityOrder(Order):
    __tablename__ = "priority_orders"
    rush_fee: int = ColumnField(Integer)


class TestColumnNames:
    def test_names_are_the_field_names_without_relations(self) -> None:
        assert (Customer.c.id, Customer.c.full_name) == ("id", "full_name")
        assert dir(Customer.c) == ["full_name", "id"]

    def test_a_misspelt_name_names_the_model_and_the_field(self) -> None:
        with pytest.raises(AttributeError, match=r"Customer has no column 'fullname'"):
            _ = Customer.c.fullname

    def test_a_relation_is_not_a_column(self) -> None:
        with pytest.raises(AttributeError, match="customer"):
            _ = Order.c.customer

    def test_a_subclass_lists_its_own_fields(self) -> None:
        assert dir(PriorityOrder.c) == ["customer_id", "id", "placed_on", "rush_fee"]
        assert dir(Order.c) == ["customer_id", "id", "placed_on"]

    def test_a_field_named_c_is_refused_naming_the_model(self) -> None:
        with pytest.raises(InvalidType, match="Shadowed.c cannot be a field"):

            class Shadowed(BaseModel):
                __tablename__ = "shadowed"
                id: int = ColumnField(Integer, primary_key=True)
                c: int = ColumnField(Integer)

    def test_the_namespace_is_read_only(self) -> None:
        with pytest.raises(AttributeError, match="read-only"):
            Order.c.id = "other"  # type: ignore[misc]

    def test_a_field_named_like_a_namespace_method_is_a_column(self) -> None:
        class Odd(BaseModel):
            __tablename__ = "odd"
            id: int = ColumnField(Integer, primary_key=True)
            of: int = ColumnField(Integer)

        assert Odd.c.of == "of"
        with pytest.raises(AttributeError, match="no column 'of'"):
            _ = Order.c.of

    def test_instances_still_pickle(self) -> None:
        order = make(Order, id=7)

        assert pickle.loads(pickle.dumps(order)) == order
