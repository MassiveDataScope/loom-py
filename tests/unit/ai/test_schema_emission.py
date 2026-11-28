"""Contract between the emitted JSON Schema and the schema file that ships.

``loom/ai/declarative/schemas/agent-spec-v1.schema.json`` is the published artifact
contract (FR-009). It is *shipped inside the distribution* rather than referenced from
``specs/``: that directory is not distributed (it is git-ignored working state), so a
test anchored there proves nothing on a fresh checkout and the file it guards would
never reach a consumer. The shipped copy is what a generator downloads, so the shipped
copy is what the ratchet must guard.

The byte-for-byte test keeps it from drifting from the Tier-1 structs: the schema the
code emits and the file consumers validate against must be the same document, byte for
byte. Regenerate the file from ``_schema.py`` when the structs legitimately change —
never edit it by hand to make this test pass.

The contract and the emitter agree again on ``policies.run_timeout_ms`` and on the
mandatory ``sql`` result bounds; both used to be documented divergences and are pinned
below so a regression shows up as a named failure instead of a diff.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from loom.ai.declarative import agent_spec_json_schema, agent_spec_schema_path
from loom.ai.declarative._v1 import (
    DEPS_TYPE_PATTERN,
    INSTRUCTION_NAME_PATTERN,
    RETRIES_DESCRIPTION,
)

_CONTRACT_PATH: Path = agent_spec_schema_path(1)


def _emitted() -> dict[str, Any]:
    """Build the schema document Loom publishes for spec version 1."""
    return agent_spec_json_schema(1)


def _committed() -> dict[str, Any]:
    """Parse the committed schema contract file."""
    document: dict[str, Any] = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    return document


def _serialise(document: dict[str, Any]) -> str:
    """Render a schema document in the canonical published form."""
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def _capability_variant(document: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return one variant of the capability union of a schema document."""
    variants: list[dict[str, Any]] = document["$defs"]["capability"]["oneOf"]
    return next(variant for variant in variants if variant["properties"]["kind"]["const"] == kind)


def _sql_variant(document: dict[str, Any]) -> dict[str, Any]:
    """Return the ``sql`` variant of the capability union of a schema document."""
    return _capability_variant(document, "sql")


def _property_names(node: Any) -> set[str]:
    """Collect every property name declared anywhere in a schema document."""
    if isinstance(node, dict):
        names: set[str] = set(node.get("properties", {}))
        for value in node.values():
            names |= _property_names(value)
        return names
    if isinstance(node, list):
        names = set()
        for item in node:
            names |= _property_names(item)
        return names
    return set()


def _policy_properties(document: dict[str, Any]) -> dict[str, Any]:
    """Return the property map of the ``policies`` definition of a schema document."""
    properties: dict[str, Any] = document["$defs"]["policies"]["properties"]
    return properties


def _ordered(value: Any) -> Any:
    """Project a JSON document onto an order-sensitive structure.

    Mappings become lists of key/value pairs, so comparing two projections detects a
    reordered key, not only a changed value.
    """
    if isinstance(value, dict):
        return [(key, _ordered(item)) for key, item in value.items()]
    if isinstance(value, list):
        return [_ordered(item) for item in value]
    return value


class TestEmittedPolicies:
    def test_the_emitted_schema_is_identical_to_the_published_contract(self) -> None:
        """The emitted schema should be byte-for-byte the committed contract file."""
        assert _serialise(_emitted()) == _CONTRACT_PATH.read_text(encoding="utf-8")

    def test_emitted_policies_declare_run_timeout_ms(self) -> None:
        """The whole-run budget is part of the published policy vocabulary (FR-033a)."""
        assert _policy_properties(_emitted())["run_timeout_ms"] == {
            "type": "integer",
            "minimum": 1000,
            "maximum": 1800000,
            "default": 120000,
        }

    def test_emitted_policies_declare_max_history_bytes(self) -> None:
        """The history ceiling is part of the published policy vocabulary (FR-061)."""
        assert _policy_properties(_emitted())["max_history_bytes"] == {
            "type": "integer",
            "minimum": 1024,
            "maximum": 67108864,
            "default": 1048576,
        }

    def test_max_requests_publishes_fifty_as_its_default_when_emitted(self) -> None:
        """``max_requests`` publishes the engine's own always-in-force default (FR-041)."""
        assert _policy_properties(_emitted())["max_requests"]["default"] == 50
        assert _policy_properties(_emitted())["max_requests"]["minimum"] == 1

    def test_emitted_policies_declare_the_five_spend_caps(self) -> None:
        """Every spend cap of FR-040 is part of the published policy vocabulary."""
        properties = _policy_properties(_emitted())
        assert {
            "max_usd",
            "max_total_tokens",
            "max_input_tokens_per_request",
            "max_tool_calls",
            "max_requests",
        } <= set(properties)
        assert properties["max_usd"]["type"] == "number"
        assert "default" not in properties["max_usd"]


class TestPublishedSpendCaps:
    def test_the_published_schema_accepts_the_five_spend_caps_when_declared(self) -> None:
        """An artifact declaring every spend cap validates against the published schema."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["policies"] = {
            "max_usd": 2.0,
            "max_total_tokens": 50000,
            "max_input_tokens_per_request": 12000,
            "max_tool_calls": 30,
            "max_requests": 50,
        }

        assert _schema_errors(payload) == []

    def test_the_published_schema_rejects_a_spend_cap_out_of_range(self) -> None:
        """A cap outside its published range is rejected offline, before compilation."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["policies"] = {"max_requests": 0}

        assert _schema_errors(payload) != []


class TestEmittedCapabilityShape:
    def test_the_emitted_sql_capability_requires_result_bounds(self) -> None:
        """An unbounded query is not representable: both bounds are required (FR-046b)."""
        assert _sql_variant(_emitted())["required"] == [
            "kind",
            "connection",
            "max_rows",
            "max_result_bytes",
        ]

    def test_the_emitted_output_hook_requires_the_usecase(self) -> None:
        """``on_output`` names one use case and naming it is the whole declaration (002/AC1)."""
        assert _emitted()["$defs"]["on_output"]["required"] == ["usecase"]

    def test_the_emitted_conversation_loader_requires_the_usecase(self) -> None:
        """``conversation`` names one use case and naming it is the whole declaration (006/AC1)."""
        assert _emitted()["$defs"]["conversation"]["required"] == ["usecase"]

    @pytest.mark.parametrize(
        ("kind", "reference"),
        [("mcp", "server"), ("a2a", "agent"), ("skills", "library")],
    )
    def test_the_emitted_capability_requires_its_reference_by_name(
        self,
        kind: str,
        reference: str,
    ) -> None:
        """Every outward-pointing capability names its target, and naming it is mandatory."""
        variant = _capability_variant(_emitted(), kind)

        assert variant["required"] == ["kind", reference]

    def test_the_emitted_native_capability_closes_the_tool_vocabulary(self) -> None:
        """``native`` names one provider tool from a closed list, and nothing else (030/AC-1)."""
        variant = _capability_variant(_emitted(), "native")

        assert variant["required"] == ["kind", "tool"]
        assert variant["additionalProperties"] is False
        assert variant["properties"]["tool"]["enum"] == [
            "web_search",
            "web_fetch",
            "code_execution",
        ]

    @pytest.mark.parametrize("kind", ["mcp", "a2a", "skills"])
    def test_the_emitted_capability_declares_the_flat_filter(self, kind: str) -> None:
        """The three filtered kinds share one flat include/exclude vocabulary."""
        properties = _capability_variant(_emitted(), kind)["properties"]

        assert {"include", "exclude"} <= set(properties)


def _python_artifact(capability: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal v1 artifact declaring exactly one ``python`` capability."""
    return {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": "Read the incident description and return a report.",
        "output": {"kind": "json_schema", "schema": {"type": "object"}},
        "capabilities": [{"kind": "python", **capability}],
    }


def _schema_errors(payload: dict[str, Any]) -> list[str]:
    """Validate an artifact against the emitted schema and return the messages."""
    validator = Draft202012Validator(_emitted())
    return [error.message for error in validator.iter_errors(payload)]


class TestEmittedPythonCapability:
    def test_emitted_python_capability_accepts_params_as_an_object(self) -> None:
        """``params`` is an object with arbitrary keys nested under the capability."""
        payload = _python_artifact(
            {"factory": "myapp.tools.geo:build_geo_toolset", "params": {"max_results": 3}}
        )

        assert _schema_errors(payload) == []

    def test_emitted_python_capability_rejects_a_sibling_of_factory(self) -> None:
        """A parameter placed beside ``factory`` instead of under ``params`` is rejected."""
        payload = _python_artifact(
            {"factory": "myapp.tools.geo:build_geo_toolset", "max_results": 3}
        )

        assert _schema_errors(payload) != []

    @pytest.mark.parametrize("retired", ["tool_filter", "refs", "url", "headers_ref", "timeout_ms"])
    def test_the_emitted_schema_declares_no_retired_vocabulary(
        self,
        retired: str,
    ) -> None:
        """Addresses and credentials are deployment facts; they left the artifact."""
        assert retired not in _property_names(_emitted())


class TestSchemaLocation:
    def test_agent_spec_json_schema_fails_for_an_unpublished_version(self) -> None:
        """An unpublished spec version is a programming error, not an empty document."""
        with pytest.raises(ValueError, match="no JSON Schema is published"):
            agent_spec_json_schema(2)

    def test_the_published_schema_lives_inside_the_package(self) -> None:
        """The schema file must live under the installed package, not under 'specs/'.

        A consumer validates against a file it can extract from the wheel or the sdist;
        anchoring the contract in git-ignored working state would publish nothing (FR-009).
        """
        path = agent_spec_schema_path(1)

        assert path.is_file()
        assert path.parent.name == "schemas"
        assert path.parent.parent.name == "declarative"

    def test_locating_the_schema_fails_for_an_unpublished_version(self) -> None:
        """An unpublished spec version is a programming error, not a missing file."""
        with pytest.raises(ValueError, match="no JSON Schema is published"):
            agent_spec_schema_path(2)


class TestEmittedSchemaConstants:
    def test_the_emitted_required_list_is_unchanged(self) -> None:
        """T104b adds properties, not requirements: the v1 contract's mandatory set is stable."""
        assert _emitted()["required"] == [
            "spec_version",
            "name",
            "description",
            "instructions",
            "output",
        ]

    def test_emitted_deps_type_pattern_is_the_struct_constant_not_a_copy(self) -> None:
        """NFR-005: the pattern is derived from the struct constant, never re-typed (T102)."""
        assert _emitted()["properties"]["deps_type"]["pattern"] == DEPS_TYPE_PATTERN

    def test_emitted_instruction_block_name_pattern_is_the_struct_constant_not_a_copy(self) -> None:
        """NFR-005: the pattern is derived from the struct constant, never re-typed (T101)."""
        pattern = _emitted()["$defs"]["instruction_block"]["properties"]["name"]["pattern"]
        assert pattern == INSTRUCTION_NAME_PATTERN

    def test_emitted_retries_description_is_the_struct_constant_not_a_copy(self) -> None:
        """L20: the schema names both axes from the constant the docstring cites, never re-typed."""
        assert _policy_properties(_emitted())["retries"]["description"] == RETRIES_DESCRIPTION

    def test_emitted_deps_type_description_states_the_dict_waiver(self) -> None:
        """FR-006 must be readable in the schema text itself, not only in the struct docstring."""
        assert "waive" in _emitted()["properties"]["deps_type"]["description"]


class TestPublishedStateAndInstructions:
    @pytest.mark.parametrize(
        "deps_type",
        ["dict", "myapp.state:GeoState"],
    )
    def test_the_published_schema_accepts_every_deps_type_form(self, deps_type: str) -> None:
        """Both spellings of ``deps_type`` validate (FR-002)."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["deps_type"] = deps_type

        assert _schema_errors(payload) == []

    def test_the_published_schema_accepts_deps_schema_as_a_json_schema_object(self) -> None:
        """``deps_schema`` validates as a JSON Schema object (FR-003)."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["deps_schema"] = {"type": "object", "properties": {"km": {"type": "integer"}}}

        assert _schema_errors(payload) == []

    def test_the_published_schema_rejects_a_deps_type_filesystem_path(self) -> None:
        """A filesystem path is not representable, exactly as for ``TypeRefOutput.ref``."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["deps_type"] = "./x.py"

        assert _schema_errors(payload) != []

    def test_the_published_schema_accepts_a_string_instructions_form(self) -> None:
        """The string form of ``instructions`` still validates."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})

        assert _schema_errors(payload) == []

    def test_the_published_schema_accepts_a_block_sequence_instructions_form(self) -> None:
        """The array-of-blocks form of ``instructions`` validates (FR-020)."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["instructions"] = [
            {"text": "You are the triage assistant."},
            {"text": "Context: {{summary}}", "name": "context", "template": "handlebars"},
        ]

        assert _schema_errors(payload) == []

    def test_the_published_schema_rejects_an_empty_instructions_sequence(self) -> None:
        """An empty sequence carries nothing to author."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["instructions"] = []

        assert _schema_errors(payload) != []

    @pytest.mark.parametrize("name", ["a:b", "agent"])
    def test_the_published_schema_rejects_a_reserved_instruction_block_name(
        self, name: str
    ) -> None:
        """The two names the engine reserves are rejected offline, before compilation (FR-021)."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["instructions"] = [{"text": "hi", "name": name}]

        assert _schema_errors(payload) != []

    def test_the_published_schema_rejects_an_unknown_instruction_block_key(self) -> None:
        """``instruction_block``'s ``additionalProperties: false`` is fixed, not incidental."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["instructions"] = [{"text": "hi", "dynamic": True}]

        assert _schema_errors(payload) != []

    def test_the_published_schema_accepts_deps_type_and_deps_schema_together(self) -> None:
        """The offline schema must not pre-empt the conflict compilation reports."""
        payload = _python_artifact({"factory": "myapp.tools.geo:build_geo_toolset"})
        payload["deps_type"] = "dict"
        payload["deps_schema"] = {"type": "object"}

        assert _schema_errors(payload) == []
