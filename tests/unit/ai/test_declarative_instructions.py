"""Authored instruction blocks of spec version 1 (spec 016, T101).

``instructions`` accepts either the string form it has always accepted, or a
non-empty sequence of :class:`InstructionBlock`. These tests pin the artifact
half: decoding, ordering and the two rejected ``name`` values the engine also
rejects (FR-020 … FR-022).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1, InstructionBlock, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode


def _payload(instructions: Any) -> dict[str, Any]:
    """Build a minimal v1 artifact declaring the given ``instructions`` value."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": instructions,
        "output": {"kind": "json_schema", "schema": {"type": "object"}},
    }


def _decode(instructions: Any) -> AgentSpecV1:
    spec = decode_spec(json.dumps(_payload(instructions)).encode("utf-8")).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    return [issue.code for issue in error.issues]


class TestInstructionArtifacts:
    def test_a_string_artifact_still_decodes_unchanged(self) -> None:
        """A bare string keeps meaning exactly what it means today."""
        spec = _decode("Read the incident description and return a report.")

        assert spec.instructions == "Read the incident description and return a report."

    def test_a_sequence_artifact_decodes_into_blocks_in_order(self) -> None:
        """Every block decodes in the order it was authored."""
        spec = _decode(
            [
                {"text": "You are the triage assistant."},
                {"text": "Context: {{summary}}", "name": "context", "template": "handlebars"},
            ]
        )

        assert spec.instructions == (
            InstructionBlock(text="You are the triage assistant."),
            InstructionBlock(text="Context: {{summary}}", name="context", template="handlebars"),
        )

    def test_an_empty_sequence_is_rejected(self) -> None:
        """A sequence with no block carries nothing to author."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode([])

        assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]
        assert "instructions" in exc.value.issues[0].message

    def test_a_block_name_containing_a_colon_is_rejected_and_names_the_field(self) -> None:
        """The engine rejects a colon in an instruction name; loom rejects it at decode."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode([{"text": "hi", "name": "a:b"}])

        assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]
        assert exc.value.issues[0].field == "instructions[0].name"

    def test_a_block_named_agent_is_rejected_and_names_the_field(self) -> None:
        """The engine reserves the name 'agent'; loom rejects it at decode."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode([{"text": "hi", "name": "agent"}])

        assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]
        assert exc.value.issues[0].field == "instructions[0].name"

    def test_a_block_with_an_extra_key_is_rejected(self) -> None:
        """Options are not part of v1: an extra key is rejected, never dropped (FR-025)."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode([{"text": "hi", "dynamic": True}])

        assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]

    def test_a_block_with_no_text_is_rejected(self) -> None:
        """``text`` is the whole declaration of a literal block; it is required."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode([{"name": "context"}])

        assert _codes(exc.value) == [AgentErrorCode.SPEC_MALFORMED]

    def test_a_block_is_frozen(self) -> None:
        """An authored block cannot be mutated after decoding."""
        block = InstructionBlock(text="hi")

        with pytest.raises(AttributeError):
            block.text = "bye"  # type: ignore[misc]

    def test_a_block_is_keyword_only(self) -> None:
        """A block's fields cannot be supplied positionally."""
        with pytest.raises(TypeError):
            InstructionBlock("hi")  # type: ignore[call-arg]
