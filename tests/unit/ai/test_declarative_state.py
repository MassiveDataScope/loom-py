"""Authored state declaration of spec version 1 (spec 016, T102).

``deps_type`` and ``deps_schema`` are the artifact's three spellings of one
mechanism (FR-003): absent (no state), ``dict`` (open, unvalidated), a
``module:Symbol`` reference (sugar over a JSON Schema the compiler resolves), or a
JSON Schema object declared directly. These tests pin the artifact half:
decoding, and the pattern ``deps_type`` accepts. Resolving a symbol, deriving
its schema and refusing an artifact that declares both fields are compilation
concerns, pinned by the compiler's own tests.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from loom.ai.declarative import AgentSpecV1, decode_spec
from loom.ai.errors import AgentCompilationError, AgentErrorCode


def _payload(**overrides: Any) -> dict[str, Any]:
    """Build a minimal v1 artifact, optionally declaring state fields."""
    payload: dict[str, Any] = {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": "Read the incident description and return a report.",
        "output": {"kind": "json_schema", "schema": {"type": "object"}},
    }
    payload.update(overrides)
    return payload


def _decode(**overrides: Any) -> AgentSpecV1:
    spec = decode_spec(json.dumps(_payload(**overrides)).encode("utf-8")).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


class TestStateFieldDecoding:
    def test_no_state_declaration_decodes_to_neither_field(self) -> None:
        """An artifact declaring neither field has no state, as every artifact does today."""
        spec = _decode()

        assert spec.deps_type is None
        assert spec.deps_schema is None

    def test_deps_type_dict_decodes_as_the_open_form(self) -> None:
        """``dict`` is the open form: it decodes to the literal string."""
        spec = _decode(deps_type="dict")

        assert spec.deps_type == "dict"

    def test_deps_type_symbol_reference_decodes(self) -> None:
        """A ``module:Symbol`` reference decodes unchanged."""
        spec = _decode(deps_type="myapp.state:GeoState")

        assert spec.deps_type == "myapp.state:GeoState"

    def test_deps_schema_decodes_as_a_mapping(self) -> None:
        """A JSON Schema object declared directly decodes unchanged."""
        schema = {"type": "object", "properties": {"km": {"type": "integer"}}}

        spec = _decode(deps_schema=schema)

        assert spec.deps_schema == schema

    def test_deps_type_rejects_a_filesystem_path(self) -> None:
        """A filesystem path is not representable, exactly as for ``TypeRefOutput.ref``."""
        with pytest.raises(AgentCompilationError) as exc:
            _decode(deps_type="./x.py")

        assert [issue.code for issue in exc.value.issues] == [AgentErrorCode.SPEC_MALFORMED]
        assert exc.value.issues[0].field == "deps_type"

    def test_declaring_both_fields_decodes_successfully(self) -> None:
        """The conflict is not a decode-time failure: compilation reports it."""
        spec = _decode(deps_type="dict", deps_schema={"type": "object"})

        assert spec.deps_type == "dict"
        assert spec.deps_schema == {"type": "object"}
