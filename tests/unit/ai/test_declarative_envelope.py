"""Envelope decoding contract for Tier-1 agent artifacts (``loom.ai.declarative``).

The two-phase decoder is the only supported entry point for authored artifacts:
phase 1 reads the envelope (``spec_version``) and phase 2 decodes the payload with
the struct registered for that version. These tests pin the observable contract of
``decode_spec``: which failures are fatal, which are merely reported as issues, and
which defaults an artifact inherits when it stays silent.

Assertions are made on error *codes*, never on messages: the codes are the public
contract, the wording is not.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1, DecodedSpec, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_ARTIFACT_PATH = "ai/agents/support-triage/agent.yaml"

_OUTPUT_JSON_SCHEMA: dict[str, Any] = {
    "kind": "json_schema",
    "schema": {"type": "object", "properties": {"answer": {"type": "string"}}},
}


def _valid_payload() -> dict[str, Any]:
    """Build the minimal artifact accepted by spec version 1."""
    return {
        "spec_version": 1,
        "name": "support-triage",
        "description": "Clasifica tickets de soporte entrantes.",
        "instructions": "Lee el ticket y devuelve la categoria adecuada.",
        "output": dict(_OUTPUT_JSON_SCHEMA),
    }


def _encode(payload: dict[str, Any]) -> bytes:
    """Render an artifact mapping as the JSON bytes that ``decode_spec`` consumes."""
    return json.dumps(payload).encode("utf-8")


def _decode_valid() -> DecodedSpec:
    """Decode the minimal valid artifact through the public entry point."""
    return decode_spec(_encode(_valid_payload()))


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


class TestDecodeSpec:
    def test_decode_spec_returns_agent_spec_v1_for_a_valid_artifact(self) -> None:
        """A well-formed v1 artifact decodes into the v1 struct."""
        decoded = _decode_valid()

        assert isinstance(decoded.spec, AgentSpecV1)

    def test_decode_spec_keeps_the_declared_fields_for_a_valid_artifact(self) -> None:
        """Declared envelope fields survive decoding unchanged."""
        decoded = _decode_valid()

        assert (decoded.spec.spec_version, decoded.spec.name, decoded.spec.description) == (
            1,
            "support-triage",
            "Clasifica tickets de soporte entrantes.",
        )

    def test_decode_spec_reports_no_issues_for_a_valid_artifact(self) -> None:
        """A supported, current spec version produces no non-fatal issues."""
        decoded = _decode_valid()

        assert decoded.issues == ()

    def test_decode_spec_fails_with_spec_unknown_field_for_an_engine_key(self) -> None:
        """Deployment vocabulary in a Tier-1 artifact is rejected, never ignored."""
        payload = _valid_payload()
        payload["engine"] = "pydantic-ai"
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.SPEC_UNKNOWN_FIELD]

    def test_decode_spec_fails_with_spec_version_unsupported_for_version_two(self) -> None:
        """An envelope version outside the registry is fatal."""
        payload = _valid_payload()
        payload["spec_version"] = 2
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.SPEC_VERSION_UNSUPPORTED]

    def test_decode_spec_fails_with_spec_version_missing_when_the_version_is_absent(self) -> None:
        """An artifact without ``spec_version`` cannot be routed to any struct."""
        payload = _valid_payload()
        del payload["spec_version"]
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.SPEC_VERSION_MISSING]

    def test_decode_spec_fails_with_agent_name_invalid_for_a_name_off_pattern(self) -> None:
        """The agent name pattern is enforced during payload decoding."""
        payload = _valid_payload()
        payload["name"] = "Support Triage!"
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded)

        assert _codes(exc.value) == [AgentErrorCode.AGENT_NAME_INVALID]

    def test_decode_spec_returns_spec_version_deprecated_when_a_later_version_exists(self) -> None:
        """A supported but superseded version is accepted with a deprecation issue."""
        decoded = decode_spec(
            _encode(_valid_payload()),
            versions={1: AgentSpecV1, 2: AgentSpecV1},
        )

        assert [issue.code for issue in decoded.issues] == [AgentErrorCode.SPEC_VERSION_DEPRECATED]

    def test_decode_spec_decodes_the_artifact_when_the_version_is_deprecated(self) -> None:
        """Deprecation is an issue, not a failure: the spec is still returned."""
        decoded = decode_spec(
            _encode(_valid_payload()),
            versions={1: AgentSpecV1, 2: AgentSpecV1},
        )

        assert isinstance(decoded.spec, AgentSpecV1)

    def test_decode_spec_applies_the_default_model_role_when_not_declared(self) -> None:
        """An artifact that omits ``model_role`` binds to the default role."""
        decoded = _decode_valid()

        assert decoded.spec.model_role == "default"

    def test_decode_spec_applies_the_default_run_timeout_when_there_are_no_policies(self) -> None:
        """Omitted policies materialise with their documented defaults."""
        decoded = _decode_valid()

        assert decoded.spec.policies.run_timeout_ms == 120000

    def test_decode_spec_applies_the_default_max_history_bytes_when_there_are_no_policies(
        self,
    ) -> None:
        """The history ceiling defaults to 1 MiB when the artifact declares no policies."""
        decoded = _decode_valid()

        assert decoded.spec.policies.max_history_bytes == 1048576

    def test_decode_spec_keeps_max_history_bytes_when_the_artifact_declares_it(self) -> None:
        """A declared history ceiling survives decoding unchanged."""
        payload = _valid_payload()
        payload["policies"] = {"max_history_bytes": 2048}

        decoded = decode_spec(_encode(payload))

        assert decoded.spec.policies.max_history_bytes == 2048

    def test_decode_spec_leaves_capabilities_empty_when_not_declared(self) -> None:
        """An artifact without capabilities decodes to an empty tuple, not ``None``."""
        decoded = _decode_valid()

        assert decoded.spec.capabilities == ()

    def test_decode_spec_keeps_the_source_path_when_the_origin_is_a_file(self) -> None:
        """A ``./`` skill library resolves against this path, so it must survive decoding."""
        decoded = decode_spec(_encode(_valid_payload()), source=_ARTIFACT_PATH)

        assert decoded.source_path == _ARTIFACT_PATH

    def test_decode_spec_leaves_source_path_as_none_when_decoding_from_bytes(self) -> None:
        """Bytes have no file behind them, so no ``./`` library can be anchored to them."""
        decoded = _decode_valid()

        assert decoded.source_path is None

    def test_decode_spec_points_issues_at_the_origin_when_a_file_is_declared(self) -> None:
        """The declared origin is also what every issue points at as its component."""
        payload = _valid_payload()
        payload["engine"] = "pydantic-ai"
        encoded = _encode(payload)

        with pytest.raises(AgentCompilationError) as exc:
            decode_spec(encoded, source=_ARTIFACT_PATH)

        assert [issue.component for issue in exc.value.issues] == [_ARTIFACT_PATH]
