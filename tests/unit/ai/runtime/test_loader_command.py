"""``loader_command``, ``_as_history`` and ``failure_error``: the loader side of a run (006 T5)."""

from __future__ import annotations

import logging

import msgspec
import pytest

from loom.ai.compiler._plan import CONVERSATION_CONTEXT_FIELDS
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._bounded import RunContext, failure_error
from loom.ai.runtime._conversation import _as_history, loader_command
from loom.core.command import Command
from loom.core.errors import Forbidden
from loom.core.identity import Identity
from tests.unit.ai.runtime.doubles import incident_triage_plan

_ALL_NAMES = frozenset(CONVERSATION_CONTEXT_FIELDS)
_DENIED_MESSAGE = "the caller is not allowed to perform this operation"


class _StrictLoaderCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    conversation_id: str


@pytest.fixture
def run() -> RunContext:
    """One admitted run of a verified caller continuing a conversation."""
    return RunContext(
        plan=incident_triage_plan(),
        identity=Identity(subject="user-1", roles=("analyst",), mechanism="test"),
        interaction_id="int-1",
        conversation_id="c-42",
    )


class TestLoaderCommand:
    def test_offers_the_five_context_values_when_the_input_accepts_them(
        self, run: RunContext
    ) -> None:
        """Every offered name carries the run's, the identity's or the plan's value."""
        command = loader_command(run, _ALL_NAMES)

        assert command == {
            "conversation_id": "c-42",
            "interaction_id": "int-1",
            "subject": "user-1",
            "mechanism": "test",
            "agent": "incident-triage",
        }

    def test_filters_to_the_accepted_names_when_the_input_declares_fewer(
        self, run: RunContext
    ) -> None:
        """Names the Input does not declare never reach it."""
        command = loader_command(run, frozenset({"conversation_id", "agent"}))

        assert command == {"conversation_id": "c-42", "agent": "incident-triage"}

    def test_decodes_a_strict_command_when_filtered_to_its_names(self, run: RunContext) -> None:
        """A ``forbid_unknown_fields`` Command declaring only ``conversation_id`` decodes."""
        accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictLoaderCommand))

        instance, seen = _StrictLoaderCommand.from_payload(loader_command(run, accepted))

        assert instance == _StrictLoaderCommand(conversation_id="c-42")
        assert seen == frozenset({"conversation_id"})

    def test_offers_exactly_the_names_the_compiler_promises(self, run: RunContext) -> None:
        """The run-time command and the compile-time offer are one contract."""
        command = loader_command(run, _ALL_NAMES)

        assert set(command) == set(CONVERSATION_CONTEXT_FIELDS)


class TestFailureError:
    def test_keeps_code_and_message_when_already_an_agent_run_error(self, run: RunContext) -> None:
        """An ``AgentRunError`` keeps both its code and its text and gains the run's id."""
        original = AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "no invoker bound")

        error = failure_error(
            original,
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message="fixed",
            what="conversation loader",
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == "no invoker bound"
        assert error.interaction_id == "int-1"
        assert error.usage is None

    def test_maps_a_denial_to_unauthorized_when_the_rules_reject(self, run: RunContext) -> None:
        """A denial keeps its meaning and answers the fixed denial text."""
        error = failure_error(
            Forbidden("thread c-42 belongs to another subject"),
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message="fixed",
            what="conversation loader",
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == _DENIED_MESSAGE
        assert error.interaction_id == "int-1"

    def test_uses_the_given_code_and_text_for_a_generic_exception(
        self, run: RunContext, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Anything else is logged server-side and answered with the given code and text."""
        with caplog.at_level(logging.ERROR, logger="loom.ai.runtime._bounded"):
            error = failure_error(
                ValueError("secret detail"),
                run,
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message="fixed",
                what="conversation loader",
            )

        assert error.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(error) == "fixed"
        assert error.interaction_id == "int-1"
        assert "secret detail" not in str(error)
        assert any(
            "conversation loader" in record.getMessage() and "int-1" in record.getMessage()
            for record in caplog.records
        )


class TestAsHistory:
    def test_returns_none_when_the_loader_returns_nothing(self) -> None:
        """``None`` is single-shot and is never measured."""
        assert _as_history(None, 1024) is None

    def test_returns_the_same_object_when_the_bytes_equal_the_limit(self) -> None:
        """Bytes exactly at the bound pass through untouched."""
        history = b"x" * 1024

        assert _as_history(history, 1024) is history

    def test_raises_value_error_with_both_sizes_when_the_bytes_exceed_the_limit(self) -> None:
        """One byte over the bound is refused and the message names both sizes."""
        with pytest.raises(ValueError, match=r"1025(?s:.*)1024"):
            _as_history(b"x" * 1025, 1024)

    def test_raises_type_error_when_the_loader_returns_a_str(self) -> None:
        """A ``str`` is never coerced to bytes."""
        with pytest.raises(TypeError):
            _as_history("not bytes", 1024)
