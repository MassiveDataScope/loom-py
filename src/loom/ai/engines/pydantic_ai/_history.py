"""The conversation a run continues, in pydantic-ai's own terms.

Loom carries a conversation as opaque bytes (:class:`~loom.ai.abc.Conversation`);
this module is the only place that knows the bytes are pydantic-ai's serialised
message list. The history is decoded once, before any provider call, so a
malformed one fails with nothing spent, and the same decoded list is re-sent on
every retried attempt. What the engine returns is only what this run
added: ``new_messages_json()`` excludes the history by construction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pydantic
from pydantic_ai import AgentRunResult
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

from loom.ai.abc import Conversation
from loom.ai.errors import CONVERSATION_LOAD_FAILED_MESSAGE, AgentRunError, AgentRunErrorCode
from loom.ai.event import AiEvent, AiKey
from loom.core.errors.summary import ErrorSummary

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RunConversation:
    """A conversation decoded for one run.

    Attributes:
        conversation_id: Loom's identifier, forwarded verbatim so pydantic-ai
            stamps every new message with it.
        history: Prior turns, or ``None`` on a first turn.
    """

    conversation_id: str
    history: list[ModelMessage] | None


def decode_conversation(conversation: Conversation | None) -> RunConversation | None:
    """Decode the history loom hands the engine.

    Args:
        conversation: The run's conversation, or ``None`` for a single shot.

    Returns:
        The decoded conversation, or ``None`` when the run carries none.

    Raises:
        AgentRunError: ``CONVERSATION_LOAD_FAILED`` when the history is not a
            valid message list; the detail is logged, never returned.
    """
    if conversation is None:
        return None
    if conversation.history is None:
        return RunConversation(conversation.conversation_id, None)
    try:
        history = ModelMessagesTypeAdapter.validate_json(conversation.history)
    except pydantic.ValidationError as error:
        _logger.error(AiEvent.HISTORY_INVALID, extra=ErrorSummary.of(error).fields())
        raise AgentRunError(
            AgentRunErrorCode.CONVERSATION_LOAD_FAILED, CONVERSATION_LOAD_FAILED_MESSAGE
        ) from error
    return RunConversation(conversation.conversation_id, history)


def run_kwargs(conversation: RunConversation | None) -> dict[str, Any]:
    """Return the keyword arguments that thread *conversation* into one attempt.

    Empty for a single shot, so that call stays byte for byte today's.
    """
    if conversation is None:
        return {}
    return {
        "message_history": conversation.history,
        "conversation_id": conversation.conversation_id,
    }


def new_messages(
    result: AgentRunResult[Any], *, agent: str, max_history_bytes: int
) -> bytes | None:
    """Return this run's messages, serialised, bounded by ``policies.max_history_bytes``.

    Args:
        result: A completed attempt's result.
        agent: Name of the plan this run served, for the over-bound log line.
        max_history_bytes: ``policies.max_history_bytes`` of the plan.

    Returns:
        This run's messages, whether or not it carried a conversation;
        ``None`` when they are longer than *max_history_bytes* — logged with
        *agent*, the measured size and the bound.
    """
    encoded = result.new_messages_json()
    if len(encoded) > max_history_bytes:
        _logger.warning(AiEvent.HISTORY_TOO_LARGE, extra={AiKey.AGENT: agent})
        return None
    return encoded
