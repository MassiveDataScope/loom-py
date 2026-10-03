from __future__ import annotations

import ipaddress
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import msgspec
import pytest

from loom.core.model.convert import to_struct


class _Tier(StrEnum):
    GOLD = "gold"
    SILVER = "silver"


class _Address(msgspec.Struct, rename="camel"):
    street_name: str
    zip_code: str


class _Customer(msgspec.Struct, rename="camel"):
    full_name: str
    tier: _Tier
    joined_at: datetime
    balance: Decimal
    address: _Address | None = None
    past_addresses: list[_Address] = []
    tags: list[dict[str, Any]] = []
    extra: Any = None
    by_label: dict[str, _Address] = {}


def test_snake_keys_map_to_the_camel_encoded_names() -> None:
    customer = to_struct(
        _Customer,
        {"full_name": "Ann", "tier": "gold", "joined_at": datetime(2026, 1, 3), "balance": "1"},
    )

    assert customer.full_name == "Ann"


def test_enum_datetime_and_decimal_come_back_typed() -> None:
    customer = to_struct(
        _Customer,
        {"full_name": "Ann", "tier": "gold", "joined_at": "2026-01-03T12:00:00", "balance": "9.5"},
    )

    assert customer.tier is _Tier.GOLD
    assert customer.joined_at == datetime(2026, 1, 3, 12, 0)
    assert customer.balance == Decimal("9.5")


def test_a_bad_value_raises_validation_error() -> None:
    payload = {
        "full_name": "Ann",
        "tier": "bronze",
        "joined_at": datetime(2026, 1, 3),
        "balance": "1",
    }

    with pytest.raises(msgspec.ValidationError):
        to_struct(_Customer, payload)


def test_nested_struct_values_are_renamed_too() -> None:
    address = {"street_name": "Main", "zip_code": "01234"}

    customer = to_struct(
        _Customer,
        {
            "full_name": "Ann",
            "tier": "silver",
            "joined_at": datetime(2026, 1, 3),
            "balance": "1",
            "address": address,
            "past_addresses": [address, {"street_name": "Old", "zip_code": "99999"}],
        },
    )

    assert customer.address == _Address(street_name="Main", zip_code="01234")
    assert [item.zip_code for item in customer.past_addresses] == ["01234", "99999"]


def test_already_typed_values_pass_through() -> None:
    address = _Address(street_name="Main", zip_code="01234")

    customer = to_struct(
        _Customer,
        {
            "full_name": "Ann",
            "tier": _Tier.GOLD,
            "joined_at": datetime(2026, 1, 3),
            "balance": Decimal("1"),
            "address": address,
        },
    )

    assert customer.address is address
    assert customer.tier is _Tier.GOLD


def test_struct_instances_in_a_dict_annotated_field_become_mappings() -> None:
    customer = to_struct(
        _Customer,
        {
            "full_name": "Ann",
            "tier": "gold",
            "joined_at": datetime(2026, 1, 3),
            "balance": "1",
            "tags": [_Address(street_name="Main", zip_code="01234")],
            "extra": _Address(street_name="Any", zip_code="00000"),
        },
    )

    assert customer.tags == [{"streetName": "Main", "zipCode": "01234"}]
    assert customer.extra == {"streetName": "Any", "zipCode": "00000"}


def test_a_mapping_of_structs_is_not_read_as_one_struct() -> None:
    home = _Address(street_name="Main", zip_code="01234")

    customer = to_struct(
        _Customer,
        {
            "full_name": "Ann",
            "tier": "gold",
            "joined_at": datetime(2026, 1, 3),
            "balance": "1",
            "by_label": {"home": home},
        },
    )

    assert customer.by_label == {"home": home}


def test_a_struct_of_another_type_is_retyped_to_the_annotation() -> None:
    class _AddressRow(msgspec.Struct):
        street_name: str
        zip_code: str

    customer = to_struct(
        _Customer,
        {
            "full_name": "Ann",
            "tier": "gold",
            "joined_at": datetime(2026, 1, 3),
            "balance": "1",
            "address": _AddressRow(street_name="Main", zip_code="01234"),
        },
    )

    assert customer.address == _Address(street_name="Main", zip_code="01234")


class _Host(msgspec.Struct):
    address: str
    network: str
    fingerprint: bytes


def test_network_addresses_read_from_the_driver_come_back_as_text() -> None:
    host = to_struct(
        _Host,
        {
            "address": ipaddress.ip_address("10.0.0.1"),
            "network": ipaddress.ip_interface("2001:db8::1/64"),
            "fingerprint": b"\x00\xff",
        },
    )

    assert host.address == "10.0.0.1"
    assert host.network == "2001:db8::1/64"
    assert host.fingerprint == b"\x00\xff"
