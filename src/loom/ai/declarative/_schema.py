"""Published JSON Schema for authored agent artifacts.

The document is emitted from the Tier-1 structs rather than read from disk, and
every pattern, default, minimum and maximum comes from the constants in
:mod:`loom.ai.declarative._v1`, so the schema cannot drift from the structs it
describes.

The emitted document is byte-for-byte the schema file **shipped inside the
distribution** (``loom/ai/declarative/schemas/agent-spec-v1.schema.json``), and
a test asserts it: the contract is published to generators and editors, so a
drift between the schema they validate against and the structs loom decodes
with would only surface as a decoding failure at deploy time.

The file is shipped rather than only emitted because the consumer that needs it
most — a generator that writes ``.agent.yaml`` files — must be able to validate
its output without importing loom, and often without installing it at all: the
file can be extracted from the wheel or the sdist and handed to any JSON Schema
validator in any language.  :func:`agent_spec_schema_path` locates it for the
callers that *do* have loom installed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ._envelope import LATEST_SPEC_VERSION
from ._v1 import (
    AGENT_NAME_PATTERN,
    DEFAULT_MODEL_ROLE,
    DEPS_TYPE_PATTERN,
    INSTRUCTION_NAME_PATTERN,
    MAX_HISTORY_BYTES_DEFAULT,
    MAX_HISTORY_BYTES_MAX,
    MAX_HISTORY_BYTES_MIN,
    MAX_INPUT_TOKENS_PER_REQUEST_MAX,
    MAX_INPUT_TOKENS_PER_REQUEST_MIN,
    MAX_ITERATIONS_DEFAULT,
    MAX_ITERATIONS_MAX,
    MAX_ITERATIONS_MIN,
    MAX_REQUESTS_DEFAULT,
    MAX_REQUESTS_MAX,
    MAX_REQUESTS_MIN,
    MAX_TOOL_CALLS_MAX,
    MAX_TOOL_CALLS_MIN,
    MAX_TOTAL_TOKENS_MAX,
    MAX_TOTAL_TOKENS_MIN,
    MAX_USD_MAX,
    MAX_USD_MIN,
    MODEL_ROLE_PATTERN,
    NATIVE_TOOLS,
    ON_UNPRICED_SPEND_DEFAULT,
    ON_UNPRICED_SPEND_POLICIES,
    RESERVED_INSTRUCTION_NAME,
    RETRIES_DEFAULT,
    RETRIES_DESCRIPTION,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_DEFAULT,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    SKILLS_LIBRARY_PATTERN,
    SPEC_VERSION_V1,
    SYMBOL_REF_PATTERN,
    TEMPLATE_ENGINES,
    TOOL_TIMEOUT_MS_DEFAULT,
    TOOL_TIMEOUT_MS_MAX,
    TOOL_TIMEOUT_MS_MIN,
)


def _v1_properties() -> dict[str, Any]:
    return {
        "spec_version": {
            "const": SPEC_VERSION_V1,
            "description": "Format version. Read first, before any other field.",
        },
        "name": {
            "type": "string",
            "pattern": AGENT_NAME_PATTERN,
            "description": ("Unique agent name within the application. Published in the A2A card."),
        },
        "description": {
            "type": "string",
            "minLength": 1,
            "description": "What the agent does. PUBLIC: published in the A2A card.",
        },
        "deps_type": {
            "type": "string",
            "pattern": DEPS_TYPE_PATTERN,
            "description": (
                "State type the artifact declares: the literal 'dict', which waives marker "
                "validation for every templated instruction block, or a module:Symbol "
                "reference \u2014 sugar over deps_schema. Absent when the artifact declares "
                "no state."
            ),
        },
        "deps_schema": {
            "type": "object",
            "description": (
                "State declared directly as a JSON Schema object, the canonical form of the "
                "one mechanism deps_type is sugar over. Absent when the artifact declares no "
                "state, or declares it through deps_type."
            ),
        },
        "instructions": {
            "oneOf": [
                {
                    "type": "string",
                    "minLength": 1,
                    "description": "One literal instruction block, unnamed.",
                },
                {
                    "type": "array",
                    "minItems": 1,
                    "items": {"$ref": "#/$defs/instruction_block"},
                    "description": "Instruction blocks in authored order.",
                },
            ],
            "description": (
                "Instructions the agent follows. NEVER published. Must not encode authorization."
            ),
        },
        "model_role": {
            "type": "string",
            "pattern": MODEL_ROLE_PATTERN,
            "default": DEFAULT_MODEL_ROLE,
            "description": (
                "Logical model role, bound to a concrete provider and model by "
                "deployment configuration. Never a vendor name or model id."
            ),
        },
        "output": {"$ref": "#/$defs/output"},
        "output_check": {
            "type": "string",
            "pattern": SYMBOL_REF_PATTERN,
            "description": (
                "module:symbol reference to a pure predicate over the parsed answer, "
                "returning None to accept or the correction text to reject and retry. "
                "Absent when the artifact declares no check."
            ),
        },
        "on_output": {"$ref": "#/$defs/on_output"},
        "conversation": {"$ref": "#/$defs/conversation"},
        "capabilities": {
            "type": "array",
            "default": [],
            "items": {"$ref": "#/$defs/capability"},
        },
        "policies": {"$ref": "#/$defs/policies"},
        "metadata": {
            "type": "object",
            "additionalProperties": {"type": "string"},
            "default": {},
        },
    }


def _v1_output_def() -> dict[str, Any]:
    return {
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "schema"],
                "properties": {
                    "kind": {"const": "json_schema"},
                    "schema": {
                        "type": "object",
                        "description": (
                            "JSON Schema object describing the required answer. "
                            "Canonical form; what a generator emits."
                        ),
                    },
                },
            },
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "ref"],
                "properties": {
                    "kind": {"const": "type_ref"},
                    "ref": {
                        "type": "string",
                        "pattern": SYMBOL_REF_PATTERN,
                        "description": (
                            "Shortcut for hand-written applications: module:Symbol "
                            "resolved at compile time."
                        ),
                    },
                },
            },
        ]
    }


def _v1_usecase_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "keys"],
        "properties": {
            "kind": {"const": "usecase"},
            "keys": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
                "description": (
                    "Explicitly granted business operation keys. Never expanded automatically."
                ),
            },
        },
    }


def _v1_sql_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "connection", "max_rows", "max_result_bytes"],
        "properties": {
            "kind": {"const": "sql"},
            "connection": {
                "type": "string",
                "minLength": 1,
                "description": "Named connection. Compilation fails unless it is read-only.",
            },
            "max_rows": {"type": "integer", "minimum": 1},
            "max_result_bytes": {"type": "integer", "minimum": 1},
        },
    }


def _v1_name_filter() -> dict[str, Any]:
    """Flat include/exclude properties shared by mcp, skills and a2a."""
    return {
        "include": {
            "type": "array",
            "items": {"type": "string"},
            "default": [],
            "description": "Names or glob patterns to expose. Empty means all.",
        },
        "exclude": {
            "type": "array",
            "items": {"type": "string"},
            "default": [],
            "description": "Names or glob patterns to omit.",
        },
    }


def _v1_mcp_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "server"],
        "properties": {
            "kind": {
                "const": "mcp",
                "description": "Tools from a named remote tool server.",
            },
            "server": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Named remote tool server, resolved from ai.mcp_servers in "
                    "deployment configuration. The artifact names the server; the "
                    "deployment knows where it is and how to authenticate to it."
                ),
            },
            **_v1_name_filter(),
        },
    }


def _v1_skills_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "library"],
        "properties": {
            "kind": {
                "const": "skills",
                "description": "Packaged prompt material from a skill library.",
            },
            "library": {
                "type": "string",
                "minLength": 1,
                "pattern": SKILLS_LIBRARY_PATTERN,
                "description": (
                    "Skill library. './name' resolves beside this artifact and travels "
                    "with it; a bare name resolves against ai.skills_root. '..' is not "
                    "representable, so a library can never escape its own directory."
                ),
            },
            **_v1_name_filter(),
        },
    }


def _v1_python_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "factory"],
        "properties": {
            "kind": {"const": "python"},
            "factory": {
                "type": "string",
                "pattern": SYMBOL_REF_PATTERN,
                "description": (
                    "module:factory called once at build as factory(context, **params). "
                    "A factory, never a constructed object."
                ),
            },
            "params": {
                "type": "object",
                "description": (
                    "Keyword arguments the factory is called with. Names are "
                    "validated against the factory signature at compile time; "
                    "values are not. Settings, never secrets."
                ),
            },
        },
    }


def _v1_a2a_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "agent"],
        "properties": {
            "kind": {
                "const": "a2a",
                "description": "Delegation to a named remote agent.",
            },
            "agent": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Named remote agent, resolved from ai.a2a_agents in deployment configuration."
                ),
            },
            **_v1_name_filter(),
        },
    }


def _v1_native_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "tool"],
        "properties": {
            "kind": {
                "const": "native",
                "description": "Tool the model provider runs in its own infrastructure.",
            },
            "tool": {
                "type": "string",
                "enum": list(NATIVE_TOOLS),
                "description": (
                    "Provider tool by its stable name. Whether the model bound to "
                    "model_role admits it is checked at compile time."
                ),
            },
        },
    }


def _v1_output_hook_def() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["usecase"],
        "properties": {
            "usecase": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Use-case key of the registry executed once per completed run "
                    "with the validated output. Not a tool: the model never sees it, "
                    "and the same key must not also be granted as a usecase capability."
                ),
            },
        },
    }


def _v1_conversation_def() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["usecase"],
        "properties": {
            "usecase": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Use-case key of the registry executed before a run that carries a "
                    "conversation_id. It returns the prior messages as bytes in the "
                    "engine's serialised form, or None on the first turn. Not a tool: "
                    "the model never sees it, and the same key must not also be "
                    "granted as a usecase capability."
                ),
            },
        },
    }


def _policy_properties() -> dict[str, Any]:
    return {
        "retries": {
            "type": "integer",
            "minimum": RETRIES_MIN,
            "maximum": RETRIES_MAX,
            "default": RETRIES_DEFAULT,
            "description": RETRIES_DESCRIPTION,
        },
        "tool_timeout_ms": {
            "type": "integer",
            "minimum": TOOL_TIMEOUT_MS_MIN,
            "maximum": TOOL_TIMEOUT_MS_MAX,
            "default": TOOL_TIMEOUT_MS_DEFAULT,
        },
        "max_iterations": {
            "type": "integer",
            "minimum": MAX_ITERATIONS_MIN,
            "maximum": MAX_ITERATIONS_MAX,
            "default": MAX_ITERATIONS_DEFAULT,
        },
        "run_timeout_ms": {
            "type": "integer",
            "minimum": RUN_TIMEOUT_MS_MIN,
            "maximum": RUN_TIMEOUT_MS_MAX,
            "default": RUN_TIMEOUT_MS_DEFAULT,
        },
        "max_history_bytes": {
            "type": "integer",
            "minimum": MAX_HISTORY_BYTES_MIN,
            "maximum": MAX_HISTORY_BYTES_MAX,
            "default": MAX_HISTORY_BYTES_DEFAULT,
        },
        "max_usd": {
            "type": "number",
            "minimum": float(MAX_USD_MIN),
            "maximum": float(MAX_USD_MAX),
            "description": (
                "Cumulative spend ceiling in US dollars for the whole run, "
                "including every retried attempt. Absent disables the cap. "
                "See 'Spend caps' in docs/ai/artifacts.md for enforcement "
                "timing and the YAML/JSON precision difference."
            ),
        },
        "max_total_tokens": {
            "type": "integer",
            "minimum": MAX_TOTAL_TOKENS_MIN,
            "maximum": MAX_TOTAL_TOKENS_MAX,
            "description": (
                "Cumulative input-plus-output token ceiling for the whole "
                "run. Absent disables the cap. Enforced after the fact, the "
                "same way as max_usd; see 'Spend caps' in docs/ai/artifacts.md."
            ),
        },
        "max_input_tokens_per_request": {
            "type": "integer",
            "minimum": MAX_INPUT_TOKENS_PER_REQUEST_MIN,
            "maximum": MAX_INPUT_TOKENS_PER_REQUEST_MAX,
            "description": (
                "Ceiling on the input tokens of any one request in the run. "
                "Absent disables the cap. Enforced after the fact, the same "
                "way as max_usd; see 'Spend caps' in docs/ai/artifacts.md."
            ),
        },
        "max_tool_calls": {
            "type": "integer",
            "minimum": MAX_TOOL_CALLS_MIN,
            "maximum": MAX_TOOL_CALLS_MAX,
            "description": (
                "Cumulative successful tool-call ceiling for the whole run. "
                "Absent disables the cap. Preemptive, unlike the caps above; "
                "see 'Spend caps' in docs/ai/artifacts.md."
            ),
        },
        "max_requests": {
            "type": "integer",
            "minimum": MAX_REQUESTS_MIN,
            "maximum": MAX_REQUESTS_MAX,
            "default": MAX_REQUESTS_DEFAULT,
            "description": (
                "Cumulative model-request ceiling for the whole run, counted "
                "by the engine. Defaults to 50, the engine's own default "
                "substituted whenever no limits are passed; see 'Spend "
                "caps' in docs/ai/artifacts.md."
            ),
        },
        "on_unpriced_spend": {
            "type": "string",
            "enum": list(ON_UNPRICED_SPEND_POLICIES),
            "default": ON_UNPRICED_SPEND_DEFAULT,
            "description": (
                "What a run does when max_usd is declared and at least one "
                "of its model responses could not be priced. Inert when "
                "max_usd is absent; see 'Spend caps' in docs/ai/artifacts.md."
            ),
        },
    }


def _v1_instruction_block_def() -> dict[str, Any]:
    """Emit the authored instruction block.

    ``name`` publishes its reservation as ``not``/``const`` rather than the
    look-ahead the decoder uses, so a validator built on an RE2-family engine
    can compile the document.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["text"],
        "properties": {
            "text": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Instruction text. Literal unless 'template' names a template engine; "
                    "with no 'template', any '{{' it contains reaches the model unchanged."
                ),
            },
            "name": {
                "type": "string",
                "pattern": INSTRUCTION_NAME_PATTERN,
                "not": {"const": RESERVED_INSTRUCTION_NAME},
                "description": (
                    "Optional name identifying the block in compilation issues and start-up "
                    "diagnostics. Never becomes an addressable id on the engine's own side."
                ),
            },
            "template": {
                "type": "string",
                "enum": list(TEMPLATE_ENGINES),
                "description": (
                    "Compiles 'text' as a Handlebars template, checked against the artifact's "
                    "declared state and rendered against it at run time. Absent when 'text' is "
                    "literal."
                ),
            },
        },
    }


def _v1_defs() -> dict[str, Any]:
    return {
        "output": _v1_output_def(),
        "on_output": _v1_output_hook_def(),
        "conversation": _v1_conversation_def(),
        "instruction_block": _v1_instruction_block_def(),
        "capability": {
            "oneOf": [
                _v1_usecase_capability(),
                _v1_sql_capability(),
                _v1_mcp_capability(),
                _v1_skills_capability(),
                _v1_python_capability(),
                _v1_a2a_capability(),
                _v1_native_capability(),
            ]
        },
        "policies": {
            "type": "object",
            "additionalProperties": False,
            "properties": _policy_properties(),
        },
    }


def _v1_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://loom.dev/schemas/agent-spec/v1.json",
        "title": "Loom AgentSpec v1",
        "description": (
            "Declarative, engine-agnostic and vendor-agnostic agent definition. "
            "Published per spec version so generators and editors can "
            "validate without the platform runtime."
        ),
        "type": "object",
        "additionalProperties": False,
        "required": ["spec_version", "name", "description", "instructions", "output"],
        "properties": _v1_properties(),
        "$defs": _v1_defs(),
    }


_SCHEMA_BUILDERS: Final[Mapping[int, Callable[[], dict[str, Any]]]] = MappingProxyType(
    {1: _v1_schema}
)


def agent_spec_json_schema(spec_version: int = LATEST_SPEC_VERSION) -> dict[str, Any]:
    """Return the published JSON Schema document for a spec version.

    The document is rebuilt on every call, so callers may mutate the result
    freely without affecting anyone else.

    Args:
        spec_version: Spec version whose schema is wanted.

    Returns:
        The JSON Schema document, ready to serialise or hand to a validator.

    Raises:
        ValueError: If no schema is published for that version.

    Example:
        >>> agent_spec_json_schema(1)["title"]
        'Loom AgentSpec v1'
    """
    builder = _SCHEMA_BUILDERS.get(spec_version)
    if builder is None:
        known = ", ".join(str(version) for version in sorted(_SCHEMA_BUILDERS))
        raise ValueError(
            f"no JSON Schema published for spec version {spec_version}; known: {known}"
        )
    return builder()


SCHEMA_FILENAMES: Final[Mapping[int, str]] = MappingProxyType({1: "agent-spec-v1.schema.json"})
"""File name each published spec version is shipped under, inside ``schemas/``."""

_SCHEMA_DIRECTORY: Final[Path] = Path(__file__).parent / "schemas"


def agent_spec_schema_path(spec_version: int = LATEST_SPEC_VERSION) -> Path:
    """Locate the JSON Schema file shipped in the installed distribution.

    The file is the byte-for-byte twin of :func:`agent_spec_json_schema`, kept
    honest by a test.  Prefer this over the emitter when the schema must be
    handed to a tool that reads a *path* — an editor, a linter, a CI validation
    step in another language.

    Args:
        spec_version: Spec version whose schema file is wanted.

    Returns:
        Absolute path of the shipped schema file.

    Raises:
        ValueError: If no schema is published for that version.
        FileNotFoundError: If the distribution was built without its schema
            data files, which is a packaging defect rather than a usage error.

    Example:
        >>> agent_spec_schema_path(1).name
        'agent-spec-v1.schema.json'
    """
    filename = SCHEMA_FILENAMES.get(spec_version)
    if filename is None:
        known = ", ".join(str(version) for version in sorted(SCHEMA_FILENAMES))
        raise ValueError(
            f"no JSON Schema published for spec version {spec_version}; known: {known}"
        )
    path = _SCHEMA_DIRECTORY / filename
    if not path.is_file():
        raise FileNotFoundError(
            f"the installed distribution ships no schema file at {path}; "
            f"rebuild the package so 'schemas/*.json' is included"
        )
    return path
