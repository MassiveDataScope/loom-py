"""Conversation phase: resolve ``conversation.usecase`` and prove its Input is feedable.

The runtime offers the loader a fixed set of run-context names
(:data:`~loom.ai.compiler._plan.CONVERSATION_CONTEXT_FIELDS`) and the proof
is the one shared with the output hook
(:mod:`loom.ai.compiler.phases._feedable`), plus one rule of its own: the
Input must declare ``conversation_id``, or the loader cannot know which
conversation to load.  Nothing about the loader's return type is inspected;
the return value is checked at run time.

The model never sees the loader: it is not a tool and never reaches
``plan.capabilities``.  A key that is also a ``kind: usecase`` grant is
refused, because the model would then choose the ``conversation_id``.
"""

from __future__ import annotations

from loom.ai.compiler._plan import CONVERSATION_CONTEXT_FIELDS, CompiledConversation
from loom.ai.compiler.phases._feedable import feedable_input, is_granted
from loom.ai.declarative import AgentSpecV1
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.use_case.registry import UseCaseRegistry

_ConversationResult = tuple[CompiledConversation | None, list[AgentCompilationIssue]]

_OFFERED: frozenset[str] = frozenset(CONVERSATION_CONTEXT_FIELDS)

_CONVERSATION_ID: str = "conversation_id"

_MISSING_CONVERSATION_ID = (
    "Input does not declare conversation_id; the loader cannot know which conversation to load"
)


def compile_conversation(
    spec: AgentSpecV1,
    *,
    component: str,
    registry: UseCaseRegistry,
) -> _ConversationResult:
    """Resolve the artifact's conversation loader and prove the run can feed it.

    Args:
        spec: Decoded artifact; ``spec.conversation`` may be ``None``.
        component: Artifact provenance every issue points at.
        registry: Use-case registry the key resolves against.

    Returns:
        The compiled loader and no issues; ``None`` and no issues when the
        artifact declares no loader; ``None`` and the issues found otherwise.
    """
    if spec.conversation is None:
        return None, []
    key = spec.conversation.usecase
    issues: list[AgentCompilationIssue] = []
    if is_granted(spec.capabilities, key):
        issues.append(
            AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED.issue(component=component, key=key)
        )
    try:
        use_case = registry.resolve(key)
    except KeyError:
        issues.append(
            AgentErrorCode.CONVERSATION_USECASE_UNKNOWN.issue(component=component, key=key)
        )
        return None, issues
    accepted, reason = feedable_input(use_case, _OFFERED)
    if reason is not None:
        issues.append(
            AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED.issue(
                component=component, key=key, reason=reason
            )
        )
    elif _CONVERSATION_ID not in accepted:
        issues.append(
            AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED.issue(
                component=component, key=key, reason=_MISSING_CONVERSATION_ID
            )
        )
    if issues:
        return None, issues
    return CompiledConversation(usecase=key, use_case=use_case, accepted=accepted), []
