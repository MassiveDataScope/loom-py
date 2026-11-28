from __future__ import annotations

import pytest

import loom.core.use_case as use_case
from loom.core.errors import InvalidType, NotFound
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
        with pytest.raises(InvalidType, match="sequence"):
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


class TestOnMissing:
    @pytest.mark.parametrize(
        ("policy", "answer"),
        [
            pytest.param(OnMissing.RETURN_NONE, None, id="return-none"),
            pytest.param(OnMissing.RETURN_FALSE, False, id="return-false"),
        ],
    )
    def test_a_returning_policy_answers_its_value(
        self, policy: OnMissing, answer: bool | None
    ) -> None:
        assert policy.answer(FakeEntity, 7) is answer

    def test_raise_refuses_with_the_entity_and_the_value(self) -> None:
        with pytest.raises(NotFound) as caught:
            OnMissing.RAISE.answer(FakeEntity, 7)

        assert "FakeEntity" in str(caught.value)
        assert "7" in str(caught.value)
