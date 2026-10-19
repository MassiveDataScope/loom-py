"""Instructions phase: project the declared form onto one sequence of blocks.

:attr:`~loom.ai.declarative.AgentSpecV1.instructions` accepts a literal string
or a non-empty sequence of :class:`~loom.ai.declarative.InstructionBlock`.
:func:`compile_instructions` resolves both spellings to one
``tuple[CompiledInstruction, ...]``: a bare string becomes a single unnamed
literal block, so a string artifact and a one-block artifact compile to the
same plan shape and nothing downstream asks which of the two an artifact
declared.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from loom.ai.abc import StateShape
from loom.ai.compiler._plan import CompiledInstruction
from loom.ai.declarative import InstructionBlock
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_CompileResult = tuple[tuple[CompiledInstruction, ...], list[AgentCompilationIssue]]


def compile_instructions(
    declared: str | tuple[InstructionBlock, ...],
    state: StateShape | None,
    component: str,
) -> _CompileResult:
    """Compile the declared ``instructions`` into plan-ready blocks.

    Args:
        declared: Value decoded from the artifact's ``instructions``.
        state: State shape :func:`~loom.ai.compiler.phases._state.compile_state`
            resolved for the same artifact, or ``None`` when it declares no
            state. A templated block requires this to be set (FR-027).
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled blocks in authored order and the issues found. On
        failure the blocks are an empty tuple and the issues are non-empty.
    """
    if isinstance(declared, str):
        return (CompiledInstruction(text=declared, name=None, template=None),), []
    issues = _duplicate_name_issues(declared, component)
    issues.extend(_missing_state_issues(declared, state, component))
    if issues:
        return (), issues
    compiled = tuple(
        CompiledInstruction(text=block.text, name=block.name, template=block.template)
        for block in declared
    )
    return compiled, []


def _duplicate_name_issues(
    blocks: Sequence[InstructionBlock], component: str
) -> list[AgentCompilationIssue]:
    positions: dict[str, list[int]] = defaultdict(list)
    for index, block in enumerate(blocks):
        if block.name is not None:
            positions[block.name].append(index)
    return [
        AgentErrorCode.INSTRUCTION_BLOCK_INVALID.issue(
            component=component,
            reason=f"name '{name}' is declared twice, at positions "
            f"{', '.join(str(position) for position in dupes)}",
        )
        for name, dupes in positions.items()
        if len(dupes) > 1
    ]


def _missing_state_issues(
    blocks: Sequence[InstructionBlock], state: StateShape | None, component: str
) -> list[AgentCompilationIssue]:
    if state is not None:
        return []
    return [
        AgentErrorCode.INSTRUCTION_BLOCK_INVALID.issue(
            component=component,
            reason=f"block {_block_label(block, index)} declares template "
            f"'{block.template}' but the artifact declares no state "
            "(add deps_type or deps_schema)",
        )
        for index, block in enumerate(blocks)
        if block.template is not None
    ]


def _block_label(block: InstructionBlock, index: int) -> str:
    return f"'{block.name}'" if block.name is not None else f"at position {index}"
