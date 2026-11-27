from __future__ import annotations

import pickle

import pytest

from loom.core.errors import InvalidType
from loom.core.model import BaseModel, BusinessKey, ColumnField, ModelKeys, RelationField
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


class Plain(BaseModel, rename=None):
    __tablename__ = "plain"
    id: int = ColumnField(Integer, primary_key=True)
    full_name: str = ColumnField(Text)


class Keyed(BaseModel):
    __tablename__ = "keyed"
    __business_key__ = BusinessKey("customer_id", "placed_on")
    id: int = ColumnField(Integer, primary_key=True)
    customer_id: int = ColumnField(Integer)
    placed_on: str = ColumnField(Text)


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

    @pytest.mark.parametrize(
        ("model", "name"),
        [(Order, "customer_id"), (Plain, "full_name")],
        ids=["camel", "plain"],
    )
    def test_names_ignore_the_wire_encoding(self, model: type[BaseModel], name: str) -> None:
        assert getattr(model.c, name) == name

    def test_instances_still_pickle(self) -> None:
        order = make(Order, id=7)

        assert pickle.loads(pickle.dumps(order)) == order


class TestConflictKey:
    def test_it_is_the_business_key_when_declared(self) -> None:
        assert ModelKeys.of(Keyed).conflict == ("customer_id", "placed_on")

    def test_it_is_the_primary_key_otherwise(self) -> None:
        assert ModelKeys.of(Order).conflict == ("id",)
