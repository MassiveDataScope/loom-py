"""Output-hook phase (002 T3): resolution, feedability proof and grant conflict."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Any

import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import (
    AgentSpecV1,
    JsonSchemaOutput,
    OutputHookSpec,
    TypeRefOutput,
    UsecaseCapability,
)
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.registry import UseCaseRegistry

from .conftest import ANSWER_SCHEMA

HOOK_KEY = "incidents.record_triage"
TRIAGE_REF = "myapp.domain.triage:TriageReport"

ACCEPTED_NAMES = frozenset(
    {"output", "interaction_id", "conversation_id", "agent", "model", "recorded_by"}
)


@pytest.fixture
def triage(fake_myapp_path: object) -> ModuleType:
    """The triage fixture module, importable only while ``myapp`` is on the path."""
    return importlib.import_module("myapp.domain.triage")


@pytest.fixture
def triage_registry(triage: ModuleType) -> UseCaseRegistry:
    """Registry of the triage use cases, compiled as the bootstrap compiles them."""
    compiler = UseCaseCompiler()
    use_cases = [
        triage.RecordTriage,
        triage.RecordReviewedTriage,
        triage.RecordTriageById,
        triage.CountTriages,
        triage.RecordTurn,
    ]
    for use_case in use_cases:
        compiler.compile(use_case)
    return UseCaseRegistry.build(use_cases)


def _hooked(spec_factory: Callable[..., AgentSpecV1], key: str, **overrides: Any) -> AgentSpecV1:
    return spec_factory(on_output=OutputHookSpec(usecase=key), **overrides)


class TestHookCompilation:
    def test_reports_usecase_unknown_when_the_hooks_key_is_not_registered(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        triage_registry: UseCaseRegistry,
    ) -> None:
        spec = _hooked(spec_factory, "incidents.no_such_key")
        issue = single_issue_for(spec, registry=triage_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN,
            "on_output.usecase",
        )

    @pytest.mark.parametrize(
        "output",
        [TypeRefOutput(ref=TRIAGE_REF), JsonSchemaOutput(schema=ANSWER_SCHEMA)],
        ids=["type_ref", "json_schema"],
    )
    def test_compiles_the_hook_when_the_input_only_asks_for_output_and_context(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        triage: ModuleType,
        triage_registry: UseCaseRegistry,
        output: TypeRefOutput | JsonSchemaOutput,
    ) -> None:
        spec = _hooked(spec_factory, HOOK_KEY, output=output)
        plan = plan_for(spec, registry=triage_registry)
        assert plan.on_output is not None
        assert plan.on_output.usecase == HOOK_KEY
        assert plan.on_output.use_case is triage.RecordTriage
        assert plan.on_output.accepted == ACCEPTED_NAMES

    def test_leaves_on_output_as_none_when_the_artifact_declares_no_hook(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        triage_registry: UseCaseRegistry,
    ) -> None:
        plan = plan_for(spec_factory(), registry=triage_registry)
        assert plan.on_output is None

    @pytest.mark.parametrize(
        "output",
        [TypeRefOutput(ref=TRIAGE_REF), JsonSchemaOutput(schema=ANSWER_SCHEMA)],
        ids=["type_ref", "json_schema"],
    )
    @pytest.mark.parametrize(
        ("key", "reason_fragment"),
        [
            ("incidents.record_reviewed_triage", "reviewer_email"),
            ("incidents.record_triage_by_id", "triage_id"),
            ("incidents.count_triages", "no Input()"),
        ],
        ids=["required_name_not_offered", "primitive_parameter", "no_input"],
    )
    def test_reports_input_unsatisfied_when_the_run_cannot_feed_the_input(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        triage_registry: UseCaseRegistry,
        output: TypeRefOutput | JsonSchemaOutput,
        key: str,
        reason_fragment: str,
    ) -> None:
        spec = _hooked(spec_factory, key, output=output)
        issue = single_issue_for(spec, registry=triage_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED,
            "on_output.usecase",
        )
        assert reason_fragment in issue.message

    def test_reports_input_unsatisfied_when_the_use_case_is_not_compiled(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        triage: ModuleType,
    ) -> None:
        # A fresh subclass: never compiled, whatever earlier tests did to the module class.
        uncompiled = type(
            "UncompiledRecordTriage", (triage.RecordTriage,), {"__execution_plan__": None}
        )
        registry = UseCaseRegistry.build([uncompiled])
        assert uncompiled.__execution_plan__ is None
        issue = single_issue_for(_hooked(spec_factory, HOOK_KEY), registry=registry)
        assert issue.code == AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED
        assert "not compiled" in issue.message

    def test_reports_also_granted_when_the_hook_is_also_a_capability(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        triage_registry: UseCaseRegistry,
    ) -> None:
        spec = _hooked(
            spec_factory,
            HOOK_KEY,
            capabilities=(UsecaseCapability(keys=(HOOK_KEY,)),),
        )
        issue = single_issue_for(spec, registry=triage_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED,
            "on_output.usecase",
        )

    def test_reports_also_granted_when_the_grant_fails_for_another_key(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        triage_registry: UseCaseRegistry,
    ) -> None:
        spec = _hooked(
            spec_factory,
            HOOK_KEY,
            capabilities=(UsecaseCapability(keys=(HOOK_KEY, "incidents.no_such_key")),),
        )
        codes = [issue.code for issue in issues_for(spec, registry=triage_registry)]
        assert codes.count(AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED) == 1
        assert AgentErrorCode.USECASE_KEY_UNKNOWN in codes

    def test_does_not_touch_capabilities_when_declaring_a_hook(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        triage: ModuleType,
        triage_registry: UseCaseRegistry,
    ) -> None:
        granted = UsecaseCapability(keys=("incidents.count_triages",))
        plain = plan_for(spec_factory(capabilities=(granted,)), registry=triage_registry)
        hooked = plan_for(
            _hooked(spec_factory, HOOK_KEY, capabilities=(granted,)),
            registry=triage_registry,
        )
        assert hooked.capabilities == plain.capabilities
        assert hooked.capabilities[0].use_cases == (triage.CountTriages,)

    def test_compiles_the_hook_when_the_input_also_asks_for_messages(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        triage: ModuleType,
        triage_registry: UseCaseRegistry,
    ) -> None:
        granted = UsecaseCapability(keys=("incidents.count_triages",))
        plain = plan_for(spec_factory(capabilities=(granted,)), registry=triage_registry)
        hooked = plan_for(
            _hooked(spec_factory, "incidents.record_turn", capabilities=(granted,)),
            registry=triage_registry,
        )
        assert hooked.on_output is not None
        assert hooked.on_output.use_case is triage.RecordTurn
        assert hooked.on_output.accepted == frozenset({"output", "interaction_id", "messages"})
        assert hooked.capabilities == plain.capabilities
