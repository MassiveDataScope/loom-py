"""``IssueCode``: one compile-issue catalogue entry renders one issue."""

from __future__ import annotations

from enum import unique

import msgspec
import pytest

from loom.core.errors.codes import CompileIssue, IssueCode, Template


class _Step:
    pass


@unique
class _Code(IssueCode):
    PLAIN = "PLAIN", Template("{component}: '{name}' is unknown", field="name")
    LISTED = "LISTED", Template("{component}: known values: {known}")
    CONSTANT = "CONSTANT", Template("fork must be last", component="fork")
    DERIVED = (
        "DERIVED",
        Template("{step}: source '{alias}' is unknown", field="{alias}", component="{step}"),
    )


def test_issue_renders_message_component_and_field() -> None:
    issue = _Code.PLAIN.issue(component="triage", name="x")

    assert issue == CompileIssue(
        code=_Code.PLAIN, message="triage: 'x' is unknown", component="triage", field="name"
    )


def test_a_sequence_is_joined_with_comma_and_space() -> None:
    issue = _Code.LISTED.issue(component="ai", known=("a", "b", 3))

    assert issue.message == "ai: known values: a, b, 3"


def test_a_set_is_sorted_before_it_is_joined() -> None:
    issue = _Code.LISTED.issue(component="ai", known={"b", "c", "a"})

    assert issue.message == "ai: known values: a, b, c"


def test_an_empty_sequence_renders_none() -> None:
    issue = _Code.LISTED.issue(component="ai", known=[])

    assert issue.message == "ai: known values: none"


def test_a_type_renders_as_its_qualified_name() -> None:
    issue = _Code.DERIVED.issue(step=_Step, alias="orders")

    assert issue.message == "_Step: source 'orders' is unknown"
    assert issue.component == "_Step"


def test_a_generic_alias_renders_as_its_text_not_as_an_iterable() -> None:
    issue = _Code.LISTED.issue(component="ai", known=list[int])

    assert issue.message == "ai: known values: list[int]"


def test_the_field_and_the_component_are_templates_too() -> None:
    issue = _Code.DERIVED.issue(step="LoadOrders", alias="orders")

    assert issue.component == "LoadOrders"
    assert issue.field == "orders"


def test_a_constant_component_needs_no_parameter() -> None:
    issue = _Code.CONSTANT.issue()

    assert issue.component == "fork"
    assert issue.field is None


def test_a_missing_parameter_raises_type_error_naming_the_code() -> None:
    with pytest.raises(TypeError, match="PLAIN takes"):
        _Code.PLAIN.issue(component="triage")


def test_an_unknown_parameter_raises_type_error_naming_the_code() -> None:
    with pytest.raises(TypeError, match="PLAIN takes"):
        _Code.PLAIN.issue(component="triage", name="x", nmae="y")


def test_params_are_the_placeholders_of_every_template() -> None:
    assert _Code.DERIVED.params == frozenset({"step", "alias"})
    assert _Code.CONSTANT.params == frozenset()


def test_a_member_is_its_string_and_encodes_as_one() -> None:
    issue = _Code.CONSTANT.issue()

    assert _Code.CONSTANT == "CONSTANT"
    assert _Code("CONSTANT") is _Code.CONSTANT
    assert msgspec.json.decode(msgspec.json.encode(issue))["code"] == "CONSTANT"
