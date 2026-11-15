"""Model-role phase failures (T050): the role must be bound in ``ai.models``."""

from __future__ import annotations

from collections.abc import Callable

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import AgentSpecV1
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode


class TestModelRoleBinding:
    def test_reports_model_role_unbound_when_role_is_absent_from_ai_models(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        issue = single_issue_for(spec_factory(model_role="unbound-role"))
        assert (issue.code, issue.field) == (
            AgentErrorCode.MODEL_ROLE_UNBOUND,
            "model_role",
        )

    def test_compiles_clean_when_declared_role_is_bound_in_ai_models(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        compiler_factory: Callable[..., object],
    ) -> None:
        plan = compiler_factory().compile(spec_factory(model_role="reasoning"))  # type: ignore[attr-defined]
        assert plan.name == "subject-agent"
