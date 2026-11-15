"""``TurnCallbacks``: the commands, the failure mapping and the history check of one run."""

from __future__ import annotations

import logging
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Any

import msgspec
import pytest

from loom.ai.abc import FinalEvent
from loom.ai.compiler._plan import (
    CONVERSATION_CONTEXT_FIELDS,
    HOOK_CONTEXT_FIELDS,
    HOOK_MESSAGES_FIELD,
    HOOK_OUTPUT_FIELD,
    CompiledConversation,
    CompiledOutput,
)
from loom.ai.declarative import PolicySpec
from loom.ai.errors import DENIED_MESSAGE, AgentRunError, AgentRunErrorCode
from loom.ai.runtime._turn import TurnCallbacks
from loom.core.command import Command
from loom.core.errors import Forbidden
from loom.core.identity import Identity
from loom.core.model import loom_type
from tests.helpers.ai import DEFAULT_USAGE
from tests.unit.ai.runtime.doubles import incident_triage_plan

_ALL_HOOK_NAMES = frozenset({HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, *HOOK_CONTEXT_FIELDS})
_ALL_LOADER_NAMES = frozenset(CONVERSATION_CONTEXT_FIELDS)
_LOADER = CompiledConversation(usecase="load", use_case=Command, accepted=frozenset())


class _Report(msgspec.Struct, frozen=True, kw_only=True):
    severity: str
    confidence: float


class _StrictHookCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    output: dict[str, Any]
    interaction_id: str


class _StrictLoaderCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    conversation_id: str


@pytest.fixture
def turn() -> TurnCallbacks:
    """One admitted run of a verified caller continuing a conversation."""
    return TurnCallbacks(
        plan=incident_triage_plan(),
        identity=Identity(subject="user-1", roles=("analyst",), mechanism="test"),
        interaction_id="int-1",
        conversation_id="c-42",
        invoker=None,
    )


def hook_command(
    output: object, turn: TurnCallbacks, accepted: frozenset[str], *, messages: bytes | None = None
) -> dict[str, Any]:
    """The command the hook of *turn* receives for a final event carrying *output*."""
    final = FinalEvent(output=output, usage=DEFAULT_USAGE, messages=messages)
    return turn.hook_command(final, accepted)


class TestHookCommand:
    def test_nests_the_output_when_it_is_a_dict(self, turn: TurnCallbacks) -> None:
        """A dict output is nested verbatim under ``output``."""
        command = hook_command({"answer": "42"}, turn, _ALL_HOOK_NAMES)

        assert command["output"] == {"answer": "42"}

    def test_converts_the_output_to_builtins_when_it_is_a_struct(self, turn: TurnCallbacks) -> None:
        """A struct output is offered as builtins so any Input type can convert it back."""
        command = hook_command(_Report(severity="high", confidence=0.7), turn, _ALL_HOOK_NAMES)

        assert command["output"] == {"severity": "high", "confidence": 0.7}

    def test_offers_the_runs_context_when_the_input_accepts_it(self, turn: TurnCallbacks) -> None:
        """Every context name carries the run's, the identity's or the plan's value."""
        command = hook_command({}, turn, _ALL_HOOK_NAMES)

        assert command == {
            "output": {},
            "messages": None,
            "interaction_id": "int-1",
            "conversation_id": "c-42",
            "subject": "user-1",
            "mechanism": "test",
            "agent": "incident-triage",
            "provider": "fake",
            "model": "fake-model",
        }

    def test_filters_to_the_accepted_names_when_the_input_declares_fewer(
        self, turn: TurnCallbacks
    ) -> None:
        """Names the Input does not declare never reach it."""
        command = hook_command({"answer": "42"}, turn, frozenset({"output", "agent"}))

        assert command == {"output": {"answer": "42"}, "agent": "incident-triage"}

    def test_does_not_let_the_output_shadow_the_context_when_names_collide(
        self, turn: TurnCallbacks
    ) -> None:
        """An output field named ``subject`` stays nested; the context wins."""
        command = hook_command({"subject": "spoofed"}, turn, _ALL_HOOK_NAMES)

        assert command["subject"] == "user-1"
        assert command["output"] == {"subject": "spoofed"}

    def test_decodes_a_strict_command_when_filtered_to_its_names(self, turn: TurnCallbacks) -> None:
        """A ``forbid_unknown_fields`` Command accepts the filtered dict."""
        accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictHookCommand))

        instance, seen = _StrictHookCommand.from_payload(
            hook_command({"answer": "42"}, turn, accepted)
        )

        assert instance == _StrictHookCommand(output={"answer": "42"}, interaction_id="int-1")
        assert seen == frozenset({"output", "interaction_id"})

    def test_offers_exactly_the_names_the_compiler_promises(self, turn: TurnCallbacks) -> None:
        """The run-time command and the compile-time offer are one contract, not two lists."""
        command = hook_command({}, turn, _ALL_HOOK_NAMES)

        assert set(command) == {HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, *HOOK_CONTEXT_FIELDS}

    def test_offers_the_messages_when_the_run_carries_them(self, turn: TurnCallbacks) -> None:
        """The run's serialised new messages are offered verbatim under ``messages``."""
        command = hook_command({}, turn, _ALL_HOOK_NAMES, messages=b"[]")

        assert command[HOOK_MESSAGES_FIELD] == b"[]"

    def test_filters_out_the_messages_when_the_input_does_not_declare_them(
        self, turn: TurnCallbacks
    ) -> None:
        """A Command not declaring ``messages`` never receives them."""
        command = hook_command({}, turn, frozenset({"output"}), messages=b"[]")

        assert command == {"output": {}}

    def test_offers_messages_none_when_not_given(self, turn: TurnCallbacks) -> None:
        """A single-shot run offers ``None``, so an optional field keeps its default."""
        command = hook_command({}, turn, _ALL_HOOK_NAMES)

        assert command[HOOK_MESSAGES_FIELD] is None

    def test_projects_a_pydantic_output_through_the_plans_loom_type(
        self, fake_myapp_path: Path, turn: TurnCallbacks
    ) -> None:
        """A pydantic answer is offered as ``model_dump(mode='json')``, not a ``TypeError``."""
        from myapp.domain.pydantic_invoices import InvoiceSummaryModel

        answer = InvoiceSummaryModel(issuer="Acme", total=42.5, due_date="2026-01-01")
        plan = msgspec.structs.replace(
            turn.plan,
            output=CompiledOutput(
                schema={"type": "object"}, loom_type=loom_type(InvoiceSummaryModel)
            ),
        )
        turn = dataclass_replace(turn, plan=plan)

        command = hook_command(answer, turn, _ALL_HOOK_NAMES)

        assert command["output"] == answer.model_dump(mode="json")

    def test_never_projects_the_output_when_the_hook_does_not_declare_it(
        self, fake_myapp_path: Path, turn: TurnCallbacks
    ) -> None:
        """A hook Input without ``output`` never touches the answer.

        The plan's compiled output type is for a different shape entirely (a
        pydantic model), while the actual answer is a plain ``str`` — the shape a
        ``run_text`` run produces.  Projecting it would raise; the guard means it
        is never attempted.
        """
        from myapp.domain.pydantic_invoices import InvoiceSummaryModel

        plan = msgspec.structs.replace(
            turn.plan,
            output=CompiledOutput(
                schema={"type": "object"}, loom_type=loom_type(InvoiceSummaryModel)
            ),
        )
        turn = dataclass_replace(turn, plan=plan)

        command = hook_command("plain text answer", turn, frozenset({"agent"}))

        assert command == {"agent": "incident-triage"}


class TestCommand:
    def test_offers_the_five_context_values_when_the_input_accepts_them(
        self, turn: TurnCallbacks
    ) -> None:
        """Every offered name carries the run's, the identity's or the plan's value."""
        command = turn.command(_ALL_LOADER_NAMES)

        assert command == {
            "conversation_id": "c-42",
            "interaction_id": "int-1",
            "subject": "user-1",
            "mechanism": "test",
            "agent": "incident-triage",
        }

    def test_filters_to_the_accepted_names_when_the_input_declares_fewer(
        self, turn: TurnCallbacks
    ) -> None:
        """Names the Input does not declare never reach it."""
        command = turn.command(frozenset({"conversation_id", "agent"}))

        assert command == {"conversation_id": "c-42", "agent": "incident-triage"}

    def test_decodes_a_strict_command_when_filtered_to_its_names(self, turn: TurnCallbacks) -> None:
        """A ``forbid_unknown_fields`` Command declaring only ``conversation_id`` decodes."""
        accepted = frozenset(info.name for info in msgspec.structs.fields(_StrictLoaderCommand))

        instance, seen = _StrictLoaderCommand.from_payload(turn.command(accepted))

        assert instance == _StrictLoaderCommand(conversation_id="c-42")
        assert seen == frozenset({"conversation_id"})

    def test_offers_exactly_the_names_the_compiler_promises(self, turn: TurnCallbacks) -> None:
        """The run-time command and the compile-time offer are one contract."""
        command = turn.command(_ALL_LOADER_NAMES)

        assert set(command) == set(CONVERSATION_CONTEXT_FIELDS)


class TestFailure:
    def test_keeps_code_and_message_when_already_an_agent_run_error(
        self, turn: TurnCallbacks
    ) -> None:
        """An ``AgentRunError`` keeps both its code and its text and gains the run's id."""
        original = AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "no invoker bound")

        error = turn.failure(
            original, _LOADER, code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED, message="fixed"
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == "no invoker bound"
        assert error.interaction_id == "int-1"
        assert error.usage is None

    def test_maps_a_denial_to_unauthorized_when_the_rules_reject(self, turn: TurnCallbacks) -> None:
        """A denial keeps its meaning and answers the fixed denial text."""
        error = turn.failure(
            Forbidden("thread c-42 belongs to another subject"),
            _LOADER,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message="fixed",
        )

        assert error.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(error) == DENIED_MESSAGE
        assert error.interaction_id == "int-1"

    def test_uses_the_given_code_and_text_for_a_generic_exception(
        self, turn: TurnCallbacks, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Anything else is logged server-side, naming the step, and answered with *message*."""
        with caplog.at_level(logging.ERROR, logger="loom.ai.runtime._turn"):
            error = turn.failure(
                ValueError("secret detail"),
                _LOADER,
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message="fixed",
            )

        assert error.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(error) == "fixed"
        assert error.interaction_id == "int-1"
        assert "secret detail" not in str(error)
        assert any(
            "conversation loader" in record.getMessage() and "int-1" in record.getMessage()
            for record in caplog.records
        )


class TestHistory:
    @pytest.fixture
    def bounded(self, turn: TurnCallbacks) -> TurnCallbacks:
        plan = msgspec.structs.replace(turn.plan, policies=PolicySpec(max_history_bytes=1024))
        return dataclass_replace(turn, plan=plan)

    def test_returns_none_when_the_loader_returns_nothing(self, bounded: TurnCallbacks) -> None:
        """``None`` is single-shot and is never measured."""
        assert bounded.history(_LOADER, None) is None

    def test_returns_the_same_object_when_the_bytes_equal_the_limit(
        self, bounded: TurnCallbacks
    ) -> None:
        """Bytes exactly at the bound pass through untouched."""
        history = b"x" * 1024

        assert bounded.history(_LOADER, history) is history

    def test_raises_value_error_with_both_sizes_when_the_bytes_exceed_the_limit(
        self, bounded: TurnCallbacks
    ) -> None:
        """One byte over the bound is refused and the message names both sizes."""
        with pytest.raises(ValueError, match=r"1025(?s:.*)1024"):
            bounded.history(_LOADER, b"x" * 1025)

    def test_raises_type_error_when_the_loader_returns_a_str(self, bounded: TurnCallbacks) -> None:
        """A ``str`` is never coerced to bytes."""
        with pytest.raises(TypeError, match="conversation loader"):
            bounded.history(_LOADER, "not bytes")
