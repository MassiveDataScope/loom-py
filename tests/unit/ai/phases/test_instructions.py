"""Instructions phase (T202): the declared form compiles to one block sequence.

A bare string and a one-block sequence compile to the same plan shape (FR-003's
instruction-side counterpart), so nothing downstream branches on which of the
two forms an artifact declared.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loom.ai.compiler._plan import CompiledInstruction
from loom.ai.declarative import AgentSpecV1, InstructionBlock
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode


class TestInstructionCompilation:
    def test_a_string_artifact_and_a_one_block_artifact_compile_to_the_same_plan_shape(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        string_plan = plan_for(
            spec_factory(instructions="Answer using only the prompt. Say so when unsure.")
        )
        block_plan = plan_for(
            spec_factory(
                instructions=(
                    InstructionBlock(text="Answer using only the prompt. Say so when unsure."),
                )
            )
        )

        assert string_plan.instructions == block_plan.instructions
        assert string_plan.instructions == (
            CompiledInstruction(
                text="Answer using only the prompt. Say so when unsure.", name=None, template=None
            ),
        )

    def test_a_block_sequence_compiles_preserving_authored_order(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        spec = spec_factory(
            instructions=(
                InstructionBlock(text="First.", name="opening"),
                InstructionBlock(text="Second."),
                InstructionBlock(text="Third.", name="closing"),
            )
        )

        plan = plan_for(spec)

        assert [block.text for block in plan.instructions] == ["First.", "Second.", "Third."]
        assert [block.name for block in plan.instructions] == ["opening", None, "closing"]

    def test_two_blocks_named_the_same_report_one_issue_naming_both_positions(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            instructions=(
                InstructionBlock(text="First.", name="base"),
                InstructionBlock(text="Second."),
                InstructionBlock(text="Third.", name="base"),
            )
        )

        issue = single_issue_for(spec)

        assert issue.code == AgentErrorCode.INSTRUCTION_BLOCK_INVALID
        assert "base" in issue.message
        assert "0" in issue.message
        assert "2" in issue.message

    def test_a_template_block_with_no_declared_state_fails_naming_what_is_missing(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
    ) -> None:
        spec = spec_factory(
            instructions=(
                InstructionBlock(text="Hello {{name}}", name="greeting", template="handlebars"),
            )
        )

        issue = single_issue_for(spec)

        assert issue.code == AgentErrorCode.INSTRUCTION_BLOCK_INVALID
        assert "greeting" in issue.message
        assert "state" in issue.message

    def test_a_template_block_with_declared_state_compiles(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        plan_for: Callable[..., Any],
    ) -> None:
        spec = spec_factory(
            instructions=(
                InstructionBlock(text="Hello {{name}}", name="greeting", template="handlebars"),
            ),
            deps_type="dict",
        )

        plan = plan_for(spec)

        assert plan.instructions == (
            CompiledInstruction(text="Hello {{name}}", name="greeting", template="handlebars"),
        )
