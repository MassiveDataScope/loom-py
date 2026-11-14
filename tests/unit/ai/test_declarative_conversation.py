"""Authored ``conversation`` loader of spec version 1 (``loom.ai.declarative``).

``conversation.usecase`` names the use case the runtime executes before a run that
carries a ``conversation_id`` (006/D1). These tests pin the artifact half of AC1: the
field decodes into :class:`ConversationSpec`, stays ``None`` when absent, rejects unknown
keys the same way every other struct does, and the published JSON Schema accepts the
same document the decoder accepts.

Assertions are made on error *codes*, never on messages: the codes are the public
contract, the wording is not.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from loom.ai.declarative import (
    AgentSpecV1,
    ConversationSpec,
    agent_spec_json_schema,
    decode_spec,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_LOADER_USECASE = "incidents.load_conversation"


def _payload_without_conversation() -> dict[str, Any]:
    """Build a minimal v1 artifact with a ``type_ref`` output and no loader."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description, across a conversation.",
        "instructions": "Classify the incident described by the user.",
        "output": {"kind": "type_ref", "ref": "myapp.domain.triage:TriageReport"},
    }


def _payload_with_conversation() -> dict[str, Any]:
    """Build the same artifact declaring ``conversation`` as the spec's deployment example."""
    payload = _payload_without_conversation()
    payload["conversation"] = {"usecase": _LOADER_USECASE}
    return payload


def _encode(payload: dict[str, Any]) -> bytes:
    """Render an artifact mapping as the JSON bytes that ``decode_spec`` consumes."""
    return json.dumps(payload).encode("utf-8")


def _decode(payload: dict[str, Any]) -> AgentSpecV1:
    """Decode an artifact and narrow it to the v1 struct."""
    spec = decode_spec(_encode(payload)).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


class TestDecodeConversation:
    def test_decode_spec_returns_conversation_spec_when_conversation_is_declared(self) -> None:
        """``conversation: {usecase: k}`` decodes to ``ConversationSpec(usecase="k")`` (AC1)."""
        spec = _decode(_payload_with_conversation())

        assert spec.conversation == ConversationSpec(usecase=_LOADER_USECASE)

    def test_decode_spec_leaves_conversation_as_none_when_not_declared(self) -> None:
        """The loader is optional and additive: an artifact without it keeps decoding (AC1)."""
        spec = _decode(_payload_without_conversation())

        assert spec.conversation is None

    def test_decode_spec_fails_with_spec_unknown_field_when_conversation_has_an_extra_key(
        self,
    ) -> None:
        """An unrecognised key inside ``conversation`` is rejected, never dropped (AC1)."""
        payload = _payload_with_conversation()
        payload["conversation"]["ttl"] = 3
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]

    def test_decode_spec_fails_when_conversation_names_no_usecase(self) -> None:
        """``usecase`` is the whole declaration; an empty loader object is malformed."""
        payload = _payload_with_conversation()
        payload["conversation"] = {}
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError):
            decode_spec(encoded)


class TestPublishedConversationSchema:
    def test_the_published_schema_accepts_the_artifact_declaring_conversation(self) -> None:
        """What the decoder accepts, the published schema accepts too (AC1)."""
        validator = Draft202012Validator(agent_spec_json_schema(1))

        assert list(validator.iter_errors(_payload_with_conversation())) == []

    def test_the_published_schema_rejects_conversation_with_an_extra_key(self) -> None:
        """The schema is as strict as the struct: unknown keys inside ``conversation`` fail."""
        payload = _payload_with_conversation()
        payload["conversation"]["ttl"] = 3
        validator = Draft202012Validator(agent_spec_json_schema(1))

        assert list(validator.iter_errors(payload)) != []
