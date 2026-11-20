"""``make`` and ``Factory`` build valid, deterministic loom instances."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import msgspec
import pytest
from polyfactory.fields import Use

from loom.core.model import BigInteger, LoomFrozenStruct
from loom.core.stores.repository.query import MAX_PAGE_LIMIT, MIN_PAGE_LIMIT, PageParams
from loom.testing import Factory, make

BUILDS = 200


@dataclass(frozen=True)
class Address:
    city: str
    zip_code: int


class Customer(LoomFrozenStruct, frozen=True, kw_only=True):
    name: str
    age: Annotated[int, msgspec.Meta(ge=18, le=65)]
    address: Address


@dataclass(frozen=True)
class Window:
    first: int
    last: int

    def __post_init__(self) -> None:
        if self.first > self.last:
            raise ValueError("first must not exceed last")


@dataclass(frozen=True)
class Report:
    title: str
    window: Window


class WindowFactory(Factory[Window]):
    __set_as_default_factory_for_type__ = True

    first = Use(lambda: 1)
    last = Use(lambda: 9)


class TestMake:
    def test_builds_a_dataclass(self):
        assert isinstance(make(Address), Address)

    def test_builds_a_msgspec_struct_with_a_nested_dataclass(self):
        customer = make(Customer)

        assert isinstance(customer, Customer)
        assert isinstance(customer.address, Address)

    def test_given_fields_win_over_generated_ones(self):
        customer = make(Customer, name="Ada", address=Address(city="Lyon", zip_code=69000))

        assert (customer.name, customer.address.city) == ("Ada", "Lyon")

    def test_same_call_builds_the_same_values(self):
        assert make(Customer) == make(Customer)

    def test_registered_factory_builds_a_nested_type(self):
        report = make(Report)

        assert (report.window.first, report.window.last) == (1, 9)

    def test_annotated_range_is_respected(self):
        ages = [
            Factory.create_factory(Customer, __random_seed__=seed).build().age
            for seed in range(BUILDS)
        ]

        assert all(18 <= age <= 65 for age in ages)

    def test_page_params_are_never_invalid(self):
        pages = [
            Factory.create_factory(PageParams, __random_seed__=seed).build()
            for seed in range(BUILDS)
        ]

        assert all(
            MIN_PAGE_LIMIT <= page.limit <= MAX_PAGE_LIMIT and page.page >= 1 for page in pages
        )

    def test_page_params_built_by_make_is_valid(self):
        assert make(PageParams).limit <= MAX_PAGE_LIMIT

    def test_invalid_given_value_still_fails_the_type_rule(self):
        with pytest.raises(ValueError, match="limit must be in"):
            make(PageParams, limit=MAX_PAGE_LIMIT + 1)


@dataclass(frozen=True)
class _Counted:
    total: Annotated[int, BigInteger]


@dataclass(frozen=True)
class _Registered:
    value: int


class _RegisteredFactory(Factory[_Registered]):
    __set_as_default_factory_for_type__ = True


class TestMakeRegressions:
    def test_a_field_with_column_metadata_builds(self) -> None:
        built = make(_Counted, total=7)

        assert built == _Counted(total=7)

    def test_a_registered_factory_builds_the_same_values_on_every_call(self) -> None:
        first = make(_Registered)

        assert [make(_Registered) for _ in range(3)] == [first, first, first]


class _Laddered(msgspec.Struct, frozen=True):
    name: str
    ladder: tuple[str, ...] = ()


class _LadderedFactory(Factory[_Laddered]):
    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True

    ladder = Use(lambda: ("plain",))


class _ChildLadderedFactory(_LadderedFactory):
    pass


class TestDeclaredFieldsWinOverDefaults:
    def test_a_declared_field_wins_over_the_model_default(self) -> None:
        assert make(_Laddered).ladder == ("plain",)

    def test_a_field_declared_on_a_parent_factory_still_wins(self) -> None:
        assert _ChildLadderedFactory.build().ladder == ("plain",)

    def test_a_given_value_wins_over_the_declared_field(self) -> None:
        assert make(_Laddered, ladder=("given",)).ladder == ("given",)
