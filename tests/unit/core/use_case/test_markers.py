from __future__ import annotations

import pytest

import loom.core.use_case as use_case
from loom.core.use_case.markers import (
    Agent,
    Exists,
    Input,
    Load,
    LoadById,
    Mcp,
    OnMissing,
    SourceKind,
)


class FakeEntity:
    pass


class TestInput:
    def test_has_no_extra_attributes(self) -> None:
        marker = Input()
        assert type(marker).__slots__ == ()


class TestAgent:
    def test_stores_the_agent_name(self) -> None:
        marker = Agent("incident-triage")

        assert marker.name == "incident-triage"

    def test_has_no_extra_attributes(self) -> None:
        marker = Agent("incident-triage")

        assert type(marker).__slots__ == ("name",)


class TestMcp:
    def test_stores_the_server_name_and_normalizes_include_to_a_tuple(self) -> None:
        marker = Mcp("docs-server", include=["search", "fetch"])

        assert marker.server == "docs-server"
        assert marker.include == ("search", "fetch")

    def test_has_no_extra_attributes(self) -> None:
        marker = Mcp("docs-server", include=["search"])

        assert type(marker).__slots__ == ("server", "include")

    def test_empty_include_raises(self) -> None:
        with pytest.raises(ValueError, match="include"):
            Mcp("docs-server", include=[])

    def test_a_bare_string_include_raises_instead_of_being_split_into_chars(self) -> None:
        with pytest.raises(ValueError, match="sequence"):
            Mcp("docs-server", include="search")

    def test_is_pinned_as_public(self) -> None:
        assert "Mcp" in use_case.__all__
        assert use_case.Mcp is Mcp


class TestLoad:
    def test_stores_entity_type_and_default_by(self) -> None:
        marker = LoadById(FakeEntity)

        assert marker.entity_type is FakeEntity
        assert marker.by == "id"

    def test_custom_by_parameter(self) -> None:
        marker = LoadById(FakeEntity, by="slug")

        assert marker.by == "slug"


class TestLoadByField:
    def test_from_param_configuration(self) -> None:
        marker = Load(FakeEntity, from_param="email", against="email")

        assert marker.from_kind is SourceKind.PARAM
        assert marker.from_name == "email"
        assert marker.against == "email"
        assert marker.on_missing is OnMissing.RAISE

    def test_from_command_configuration(self) -> None:
        marker = Load(FakeEntity, from_command="email", against="email")

        assert marker.from_kind is SourceKind.COMMAND
        assert marker.from_name == "email"

    def test_invalid_source_configuration_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            Load(FakeEntity, against="email")


class TestExists:
    def test_default_on_missing(self) -> None:
        marker = Exists(FakeEntity, from_param="email", against="email")
        assert marker.on_missing is OnMissing.RETURN_FALSE
