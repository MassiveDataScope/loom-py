"""Run-time contract of ``HOOK_FAILED`` and the ``interaction_id`` of a run error.

A hook failure is neither the provider's fault nor the model's: it is the
application's own use case failing after the answer was produced.  Classifying
it as ``INFRASTRUCTURE`` would make the retry policy replay a run whose hook
already began recording, so its class is pinned here (U1, AC7).
"""

from __future__ import annotations

from loom.ai.errors import (
    CONVERSATION_LOAD_FAILED_MESSAGE,
    CONVERSATION_LOAD_TIMEOUT_MESSAGE,
    AgentRunError,
    AgentRunErrorClass,
    AgentRunErrorCode,
    is_retriable,
    run_error_class,
)


class TestHookFailed:
    def test_hook_failed_is_application_class_and_not_retriable(self) -> None:
        """The hook is not retried: its class is ``APPLICATION`` and never retriable."""
        code = AgentRunErrorCode.HOOK_FAILED

        assert run_error_class(code) is AgentRunErrorClass.APPLICATION
        assert is_retriable(code) is False

    def test_the_run_error_carries_no_interaction_id_when_none_is_given(self) -> None:
        """Pre-admission failures have no interaction to name."""
        error = AgentRunError(AgentRunErrorCode.HOOK_FAILED, "the output hook failed")

        assert error.interaction_id is None

    def test_the_run_error_keeps_the_interaction_id_when_given(self) -> None:
        """The keyword survives so a transport can echo the interaction to the caller."""
        error = AgentRunError(
            AgentRunErrorCode.HOOK_FAILED, "the output hook failed", interaction_id="int-1"
        )

        assert error.interaction_id == "int-1"
        assert error.code is AgentRunErrorCode.HOOK_FAILED
        assert str(error) == "the output hook failed"

    def test_hook_failed_has_an_explicit_http_status_mapping(self) -> None:
        from loom.ai.fastapi.endpoints import _STATUS_BY_CODE

        assert _STATUS_BY_CODE[AgentRunErrorCode.HOOK_FAILED] == 500


class TestConversationLoadFailed:
    def test_conversation_load_failed_is_application_class_and_not_retriable(self) -> None:
        """The loader is the application's own use case: never retried (U1, AC7)."""
        code = AgentRunErrorCode.CONVERSATION_LOAD_FAILED

        assert run_error_class(code) is AgentRunErrorClass.APPLICATION
        assert is_retriable(code) is False

    def test_conversation_load_failed_has_an_explicit_http_status_mapping(self) -> None:
        from loom.ai.fastapi.endpoints import _STATUS_BY_CODE

        assert _STATUS_BY_CODE[AgentRunErrorCode.CONVERSATION_LOAD_FAILED] == 500

    def test_conversation_load_failed_message_is_the_fixed_text(self) -> None:
        """The client text is fixed (D8): it never carries the loader's detail."""
        assert (
            CONVERSATION_LOAD_FAILED_MESSAGE
            == "the conversation could not be loaded; the detail is recorded server-side"
        )


class TestConversationLoadTimeout:
    def test_conversation_load_timeout_is_infrastructure_class_and_retriable(self) -> None:
        """A loader cut at its bound is a slow store, not a broken use case (FR-063)."""
        code = AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT

        assert run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE
        assert is_retriable(code) is True

    def test_conversation_load_timeout_has_an_explicit_http_status_mapping(self) -> None:
        from loom.ai.fastapi.endpoints import _STATUS_BY_CODE

        assert _STATUS_BY_CODE[AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT] == 504

    def test_conversation_load_timeout_message_is_the_fixed_text(self) -> None:
        """The client text is fixed: it never names the loader or its store."""
        assert (
            CONVERSATION_LOAD_TIMEOUT_MESSAGE == "the conversation loader exceeded its time limit"
        )
