"""Authored ``on_output`` hook of spec version 1 (``loom.ai.declarative``).

``on_output.usecase`` names the use case the runtime executes once per completed run
(002/D1). These tests pin the artifact half of AC1: the field decodes into
:class:`OutputHookSpec`, stays ``None`` when absent, rejects unknown keys the same way
every other struct does, and the published JSON Schema accepts the same document the
decoder accepts.

Assertions are made on error *codes*, never on messages: the codes are the public
contract, the wording is not.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from loom.ai.declarative import AgentSpecV1, OutputHookSpec, agent_spec_json_schema, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_HOOK_USECASE = "incidents.record_triage"


def _payload_without_hook() -> dict[str, Any]:
    """Build a minimal v1 artifact with a ``type_ref`` output and no hook."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Clasifica un incidente a partir de su descripcion.",
        "instructions": "Lee la descripcion del incidente y devuelve un informe.",
        "output": {"kind": "type_ref", "ref": "myapp.domain.incidents:IncidentReport"},
    }


def _payload_with_hook() -> dict[str, Any]:
    """Build the same artifact declaring ``on_output`` as the spec's deployment example."""
    payload = _payload_without_hook()
    payload["on_output"] = {"usecase": _HOOK_USECASE}
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


class TestDecodeOutputHook:
    def test_decode_spec_returns_output_hook_spec_when_on_output_is_declared(self) -> None:
        """``on_output: {usecase: k}`` decodes to ``OutputHookSpec(usecase="k")`` (AC1)."""
        spec = _decode(_payload_with_hook())

        assert spec.on_output == OutputHookSpec(usecase=_HOOK_USECASE)

    def test_decode_spec_leaves_on_output_as_none_when_not_declared(self) -> None:
        """The hook is optional and additive: an artifact without it keeps decoding (AC1)."""
        spec = _decode(_payload_without_hook())

        assert spec.on_output is None

    def test_decode_spec_fails_with_spec_unknown_field_when_on_output_has_an_extra_key(
        self,
    ) -> None:
        """An unrecognised key inside ``on_output`` is rejected, never dropped (AC1, FR-005)."""
        payload = _payload_with_hook()
        payload["on_output"]["retries"] = 3
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]

    def test_decode_spec_fails_when_on_output_names_no_usecase(self) -> None:
        """``usecase`` is the whole declaration; an empty hook object is malformed."""
        payload = _payload_with_hook()
        payload["on_output"] = {}
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError):
            decode_spec(encoded)


class TestPublishedOutputHookSchema:
    def test_the_published_schema_accepts_the_artifact_declaring_on_output(self) -> None:
        """What the decoder accepts, the published schema accepts too (AC1)."""
        validator = Draft202012Validator(agent_spec_json_schema(1))

        assert list(validator.iter_errors(_payload_with_hook())) == []

    def test_the_published_schema_rejects_on_output_with_an_extra_key(self) -> None:
        """The schema is as strict as the struct: unknown keys inside ``on_output`` fail."""
        payload = _payload_with_hook()
        payload["on_output"]["retries"] = 3
        validator = Draft202012Validator(agent_spec_json_schema(1))

        assert list(validator.iter_errors(payload)) != []
