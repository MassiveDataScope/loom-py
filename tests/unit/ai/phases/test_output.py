"""Output phase failures (T050): schema validity and ``type_ref`` resolution."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, TypeRefOutput
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode


@pytest.mark.parametrize(
    "schema",
    [
        {"type": 42},
        {"type": "object", "properties": ["not", "a", "mapping"]},
    ],
    ids=["type_not_a_string", "properties_not_an_object"],
)
def test_reports_schema_invalid_when_output_schema_is_not_valid_json_schema(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
    schema: dict[str, Any],
) -> None:
    spec = spec_factory(output=JsonSchemaOutput(schema=schema))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.OUTPUT_SCHEMA_INVALID,
        "output.schema",
    )


def test_reports_type_ref_unresolvable_when_module_does_not_import(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(output=TypeRefOutput(ref="no_such_pkg_zz.domain:Missing"))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE,
        "output.ref",
    )


def test_reports_output_check_unresolvable_when_the_check_does_not_import(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    spec = spec_factory(output_check="no_such_pkg_zz.checks:verdict")
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.OUTPUT_CHECK_UNRESOLVABLE,
        "output_check",
    )
    assert issue.message.endswith(
        ": output check 'no_such_pkg_zz.checks:verdict' cannot be imported"
    )


@pytest.mark.parametrize(
    "ref",
    [
        "myapp.domain.unsupported:PlainModel",
        "myapp.domain.unsupported:NOT_A_TYPE",
        "myapp.domain.unsupported:LaxModel",
    ],
    ids=["plain_class", "dict_value", "pydantic_model_admitting_unknown_fields"],
)
def test_reports_type_ref_unsupported_when_symbol_is_not_a_strict_boundary_type(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
    ref: str,
) -> None:
    spec = spec_factory(output=TypeRefOutput(ref=ref))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED,
        "output.ref",
    )


def test_reports_type_ref_unsupported_when_struct_does_not_forbid_unknown_fields(
    spec_factory: Callable[..., AgentSpecV1],
    single_issue_for: Callable[..., AgentCompilationIssue],
) -> None:
    """Invariant 5: byte pass-through requires a strict decode at compile."""
    spec = spec_factory(output=TypeRefOutput(ref="myapp.domain.unsupported:LaxStruct"))
    issue = single_issue_for(spec)
    assert (issue.code, issue.field) == (
        AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED,
        "output.ref",
    )
