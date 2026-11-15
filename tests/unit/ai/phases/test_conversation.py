"""Conversation phase (006 T3): resolution, feedability proof and grant conflict."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Any

import pytest

from loom.ai.declarative import (
    AgentSpecV1,
    ConversationSpec,
    JsonSchemaOutput,
    TypeRefOutput,
    UsecaseCapability,
)
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.registry import UseCaseRegistry

from .conftest import ANSWER_SCHEMA

LOADER_KEY = "conversations.load"
TRIAGE_REF = "myapp.domain.triage:TriageReport"
CONVERSATION_FIELD = "conversation.usecase"

ACCEPTED_NAMES = frozenset({"conversation_id", "subject", "loaded_by"})

OUTPUTS = [TypeRefOutput(ref=TRIAGE_REF), JsonSchemaOutput(schema=ANSWER_SCHEMA)]
OUTPUT_IDS = ["type_ref", "json_schema"]


@pytest.fixture
def conversations(fake_myapp_path: object) -> ModuleType:
    """The conversations fixture module, importable only while ``myapp`` is on the path."""
    return importlib.import_module("myapp.domain.conversations")


@pytest.fixture
def loader_registry(conversations: ModuleType) -> UseCaseRegistry:
    """Registry of the loader use cases, compiled as the bootstrap compiles them."""
    compiler = UseCaseCompiler()
    use_cases = [
        conversations.LoadConversation,
        conversations.LoadRecent,
        conversations.LoadTenantThread,
        conversations.LoadWithOutput,
        conversations.LoadByToken,
        conversations.CountThreads,
    ]
    for use_case in use_cases:
        compiler.compile(use_case)
    return UseCaseRegistry.build(use_cases)


def _with_loader(
    spec_factory: Callable[..., AgentSpecV1], key: str, **overrides: Any
) -> AgentSpecV1:
    return spec_factory(conversation=ConversationSpec(usecase=key), **overrides)


class TestConversationCompilation:
    def test_reports_usecase_unknown_when_the_loaders_key_is_not_registered(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        loader_registry: UseCaseRegistry,
    ) -> None:
        spec = _with_loader(spec_factory, "conversations.no_such_key")
        issue = single_issue_for(spec, registry=loader_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.CONVERSATION_USECASE_UNKNOWN,
            CONVERSATION_FIELD,
        )

    @pytest.mark.parametrize("output", OUTPUTS, ids=OUTPUT_IDS)
    def test_compiles_the_loader_when_the_input_declares_conversation_id_and_context(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        conversations: ModuleType,
        loader_registry: UseCaseRegistry,
        output: TypeRefOutput | JsonSchemaOutput,
    ) -> None:
        spec = _with_loader(spec_factory, LOADER_KEY, output=output)
        plan = plan_for(spec, registry=loader_registry)
        assert plan.conversation is not None
        assert plan.conversation.usecase == LOADER_KEY
        assert plan.conversation.use_case is conversations.LoadConversation
        assert plan.conversation.accepted == ACCEPTED_NAMES

    def test_leaves_conversation_as_none_when_the_artifact_declares_no_loader(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        loader_registry: UseCaseRegistry,
    ) -> None:
        plan = plan_for(spec_factory(), registry=loader_registry)
        assert plan.conversation is None

    @pytest.mark.parametrize("output", OUTPUTS, ids=OUTPUT_IDS)
    @pytest.mark.parametrize(
        ("key", "reason_fragment"),
        [
            ("conversations.load_recent", "conversation_id"),
            ("conversations.load_tenant_thread", "tenant"),
            ("conversations.load_with_output", "output"),
            ("conversations.load_by_token", "token"),
            ("conversations.count_threads", "no Input()"),
        ],
        ids=[
            "no_conversation_id",
            "required_name_not_offered",
            "output_not_offered",
            "primitive_parameter",
            "no_input",
        ],
    )
    def test_reports_input_unsatisfied_when_the_run_cannot_feed_the_loader(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        loader_registry: UseCaseRegistry,
        output: TypeRefOutput | JsonSchemaOutput,
        key: str,
        reason_fragment: str,
    ) -> None:
        spec = _with_loader(spec_factory, key, output=output)
        issue = single_issue_for(spec, registry=loader_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED,
            CONVERSATION_FIELD,
        )
        assert reason_fragment in issue.message

    def test_reports_input_unsatisfied_when_the_use_case_is_not_compiled(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        conversations: ModuleType,
    ) -> None:
        # A fresh subclass: never compiled, whatever earlier tests did to the module class.
        uncompiled = type(
            "UncompiledLoadConversation",
            (conversations.LoadConversation,),
            {"__execution_plan__": None},
        )
        registry = UseCaseRegistry.build([uncompiled])
        assert uncompiled.__execution_plan__ is None
        issue = single_issue_for(_with_loader(spec_factory, LOADER_KEY), registry=registry)
        assert issue.code == AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED
        assert "not compiled" in issue.message

    def test_reports_also_granted_when_the_loader_is_also_a_capability(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        loader_registry: UseCaseRegistry,
    ) -> None:
        spec = _with_loader(
            spec_factory,
            LOADER_KEY,
            capabilities=(UsecaseCapability(keys=(LOADER_KEY,)),),
        )
        issue = single_issue_for(spec, registry=loader_registry)
        assert (issue.code, issue.field) == (
            AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED,
            CONVERSATION_FIELD,
        )

    def test_reports_also_granted_when_the_grant_fails_for_another_key(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
        loader_registry: UseCaseRegistry,
    ) -> None:
        spec = _with_loader(
            spec_factory,
            LOADER_KEY,
            capabilities=(UsecaseCapability(keys=(LOADER_KEY, "conversations.no_such_key")),),
        )
        codes = [issue.code for issue in issues_for(spec, registry=loader_registry)]
        assert codes.count(AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED) == 1
        assert AgentErrorCode.USECASE_KEY_UNKNOWN in codes

    def test_does_not_touch_capabilities_when_declaring_a_loader(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
        conversations: ModuleType,
        loader_registry: UseCaseRegistry,
    ) -> None:
        granted = UsecaseCapability(keys=("conversations.count_threads",))
        plain = plan_for(spec_factory(capabilities=(granted,)), registry=loader_registry)
        loaded = plan_for(
            _with_loader(spec_factory, LOADER_KEY, capabilities=(granted,)),
            registry=loader_registry,
        )
        assert loaded.capabilities == plain.capabilities
        assert loaded.capabilities[0].use_cases == (conversations.CountThreads,)
