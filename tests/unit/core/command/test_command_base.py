from __future__ import annotations

from typing import Any, cast

import pytest

from loom.core.command import Command, Computed, Internal, Patch
from loom.core.errors import BoundaryValidationError


class CreateUser(Command, frozen=True):
    email: str
    name: str
    age: int | None = None


class CreateWithInternal(Command, frozen=True):
    email: str
    tenant_id: Internal[int] = 0
    is_adult: Computed[bool] = False


class UpdateUserCamel(Command, frozen=True, rename="camel"):
    full_name: Patch[str | None]


class TestCommandCreation:
    def test_create_basic(self) -> None:
        cmd = CreateUser(email="a@b.com", name="Alice")
        assert cmd.email == "a@b.com"
        assert cmd.name == "Alice"
        assert cmd.age is None

    def test_create_with_all_fields(self) -> None:
        cmd = CreateUser(email="a@b.com", name="Alice", age=30)
        assert cmd.age == 30

    def test_create_with_command_fields(self) -> None:
        cmd = CreateWithInternal(email="a@b.com")
        assert cmd.email == "a@b.com"
        assert cmd.tenant_id == 0
        assert cmd.is_adult is False

    def test_create_override_internal(self) -> None:
        cmd = CreateWithInternal(email="a@b.com", tenant_id=5)
        assert cmd.tenant_id == 5


class TestCommandFrozen:
    def test_cannot_mutate(self) -> None:
        cmd = CreateUser(email="a@b.com", name="Alice")
        with pytest.raises(AttributeError):
            cast(Any, cmd).email = "other@b.com"


class TestCommandFromPayload:
    def test_full_payload(self) -> None:
        cmd, fields_set = CreateUser.from_payload({"email": "a@b.com", "name": "Alice", "age": 30})
        assert cmd.email == "a@b.com"
        assert cmd.name == "Alice"
        assert cmd.age == 30
        assert fields_set == frozenset({"email", "name", "age"})

    def test_partial_payload_with_defaults(self) -> None:
        cmd, fields_set = CreateUser.from_payload({"email": "a@b.com", "name": "Alice"})
        assert cmd.age is None
        assert fields_set == frozenset({"email", "name"})

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(BoundaryValidationError):
            CreateUser.from_payload({"email": "a@b.com"})

    def test_fields_set_tracks_payload_keys(self) -> None:
        cmd, fields_set = CreateWithInternal.from_payload({"email": "a@b.com"})
        assert cmd.email == "a@b.com"
        assert cmd.tenant_id == 0
        assert "email" in fields_set
        assert "tenant_id" not in fields_set

    def test_extra_keys_ignored_in_fields_set(self) -> None:
        cmd, fields_set = CreateUser.from_payload(
            {"email": "a@b.com", "name": "Alice", "unknown": "x"}
        )
        assert "unknown" not in fields_set
        assert fields_set == frozenset({"email", "name"})

    def test_fields_set_maps_camelcase_keys_to_internal_names(self) -> None:
        cmd, fields_set = UpdateUserCamel.from_payload({"fullName": "Alice"})
        assert cmd.full_name == "Alice"
        assert fields_set == frozenset({"full_name"})
