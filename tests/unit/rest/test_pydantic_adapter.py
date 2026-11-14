from __future__ import annotations

import pydantic
import pytest

from loom.core.command import Command, Computed, Internal, Patch
from loom.rest.adapter import PydanticAdapter


class CreateUser(Command, frozen=True):
    email: str
    name: str
    slug: Computed[str] = ""
    tenant_id: Internal[str] = ""


class UpdateUser(Command, frozen=True):
    email: Patch[str | None]
    name: Patch[str | None]
    tenant_id: Internal[str] = ""


class TestCompileSchema:
    def test_returns_pydantic_base_model_subclass(self) -> None:
        adapter = PydanticAdapter()
        schema = adapter.compile_schema(CreateUser)

        assert issubclass(schema, pydantic.BaseModel)

    @pytest.mark.parametrize(
        "expected",
        [
            pytest.param("tenant_id", id="excludes_internal_fields"),
            pytest.param("slug", id="excludes_calculated_fields"),
        ],
    )
    def test_excludes_hidden_fields(self, expected: str) -> None:
        adapter = PydanticAdapter()
        schema = adapter.compile_schema(CreateUser)

        assert expected not in schema.model_fields

    def test_includes_input_fields(self) -> None:
        adapter = PydanticAdapter()
        schema = adapter.compile_schema(CreateUser)

        assert "email" in schema.model_fields
        assert "name" in schema.model_fields

    def test_schema_is_cached(self) -> None:
        adapter = PydanticAdapter()
        first = adapter.compile_schema(CreateUser)
        second = adapter.compile_schema(CreateUser)

        assert first is second


class TestRegister:
    def test_register_populates_cache(self) -> None:
        adapter = PydanticAdapter()
        adapter.register(CreateUser, UpdateUser)

        assert CreateUser in adapter._cache
        assert UpdateUser in adapter._cache

    def test_register_schemas_are_usable(self) -> None:
        adapter = PydanticAdapter()
        adapter.register(CreateUser)

        cmd, fields_set = adapter.parse(CreateUser, {"email": "a@b.com", "name": "Alice"})
        assert isinstance(cmd, CreateUser)
        assert cmd.email == "a@b.com"


class TestParse:
    def test_returns_correct_command_and_fields_set(self) -> None:
        adapter = PydanticAdapter()
        cmd, fields_set = adapter.parse(CreateUser, {"email": "a@b.com", "name": "Alice"})

        assert isinstance(cmd, CreateUser)
        assert cmd.email == "a@b.com"
        assert cmd.name == "Alice"
        assert fields_set == frozenset({"email", "name"})

    def test_patch_command_tracks_partial_fields(self) -> None:
        adapter = PydanticAdapter()
        cmd, fields_set = adapter.parse(UpdateUser, {"email": "new@b.com"})

        assert isinstance(cmd, UpdateUser)
        assert cmd.email == "new@b.com"
        assert cmd.name is None
        assert fields_set == frozenset({"email"})

    def test_validation_error_propagates(self) -> None:
        adapter = PydanticAdapter()

        with pytest.raises(pydantic.ValidationError):
            adapter.parse(CreateUser, {"email": "a@b.com"})

    def test_excluded_fields_get_defaults(self) -> None:
        adapter = PydanticAdapter()
        cmd, _ = adapter.parse(CreateUser, {"email": "a@b.com", "name": "Alice"})

        assert cmd.slug == ""
        assert cmd.tenant_id == ""
