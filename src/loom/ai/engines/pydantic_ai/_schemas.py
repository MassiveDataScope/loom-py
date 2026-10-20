"""Published tool names and the argument schemas they are declared with.

A tool name is derived from the granted handle alone (design R2), so the whole
plan can be checked at build time: two grants of *any* capability deriving one
name, or a name longer than a provider accepts, fails the build instead of
silently shadowing an operation at run time.

The argument schemas are derived once, at build, from the compiled execution
plan — never by reflection on the call path.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any

import msgspec

from loom.ai.compiler import (
    CompiledA2ACapability,
    CompiledCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.engine.plan import ExecutionPlan

_MAX_TOOL_NAME = 64
"""Longest tool name providers accept (``^[a-zA-Z0-9_-]{1,64}$``)."""

_NON_TOOL_NAME = re.compile(r"\W", re.ASCII)
"""Every character a tool name may not carry (design R2).

``re.ASCII`` keeps ``\\W`` restricted to non-``[A-Za-z0-9_]`` characters —
provider tool names are ASCII-only, so the concise class must not treat
Unicode letters as word characters.
"""


_JSON_TYPES: Mapping[type[Any], str] = MappingProxyType(
    {str: "string", int: "integer", float: "number", bool: "boolean"}
)
"""Primitive annotation → JSON Schema type; anything else stays unconstrained."""


def tool_name(prefix: str, granted: str) -> str:
    """Derive one published tool name from a granted handle (design R2)."""
    return f"{prefix}_{_NON_TOOL_NAME.sub('_', granted)}"


def published_names(capability: CompiledCapability) -> tuple[tuple[str, str], ...]:
    """Return the ``(tool name, granted handle)`` pairs loom itself publishes.

    ``mcp`` and ``python`` name their own tools, so their names are not derived
    here and cannot be validated at build; ``skills`` and ``native`` publish no tool at all.
    """
    pairs: list[tuple[str, str]]
    match capability:
        case CompiledUsecaseCapability():
            pairs = [(tool_name("usecase", key), key) for key in capability.keys]
        case CompiledSqlCapability():
            pairs = [(tool_name("sql", capability.connection), capability.connection)]
        case CompiledA2ACapability():
            pairs = [(tool_name("a2a", capability.agent), capability.agent)]
        case _:
            pairs = []
    return tuple(pairs)


def reject_unusable_names(capabilities: Sequence[CompiledCapability], agent: str) -> None:
    """Fail the build on a name two grants share or a provider would reject.

    Collision detection spans **every** capability of the plan: two ``usecase``
    grants of different capabilities deriving one name would otherwise shadow
    each other silently.
    """
    seen: dict[str, str] = {}
    for capability in capabilities:
        for name, granted in published_names(capability):
            _reject_long_name(name, granted, agent)
            clash = seen.get(name)
            if clash is not None:
                raise AgentCompilationError(
                    [
                        AgentErrorCode.TOOL_NAME_COLLISION.issue(
                            component=agent, first=clash, second=granted, name=name
                        )
                    ]
                )
            seen[name] = granted


def _reject_long_name(name: str, granted: str, agent: str) -> None:
    if len(name) <= _MAX_TOOL_NAME:
        return
    raise AgentCompilationError(
        [
            AgentErrorCode.TOOL_NAME_TOO_LONG.issue(
                component=agent,
                granted=granted,
                name=name,
                length=len(name),
                limit=_MAX_TOOL_NAME,
            )
        ]
    )


def usecase_schema(execution: ExecutionPlan) -> dict[str, Any]:
    """Publish the argument schema derived once from the execution plan."""
    properties: dict[str, Any] = {
        binding.name: _property_schema(binding.annotation) for binding in execution.param_bindings
    }
    required = [binding.name for binding in execution.param_bindings]
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }
    if execution.input_binding is not None:
        payload_schema, components = _command_schema(execution.input_binding.command_type)
        properties[execution.input_binding.name] = payload_schema
        required.append(execution.input_binding.name)
        if components:
            schema["$defs"] = components
    return schema


def _property_schema(annotation: type[Any]) -> dict[str, Any]:
    json_type = _JSON_TYPES.get(annotation)
    return {} if json_type is None else {"type": json_type}


def _command_schema(command_type: type[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Describe the command payload, degrading to a bare object when opaque."""
    try:
        schemas, components = msgspec.json.schema_components(
            [command_type], ref_template="#/$defs/{name}"
        )
    except TypeError:
        return {"type": "object"}, {}
    return dict(schemas[0]), dict(components)


def sql_schema() -> dict[str, Any]:
    """Build a fresh argument schema: the caller supplies the statement only."""
    return {
        "type": "object",
        "properties": {"sql": {"type": "string", "description": "Read-only SQL statement."}},
        "required": ["sql"],
        "additionalProperties": False,
    }


def a2a_schema() -> dict[str, Any]:
    """Build a fresh argument schema: the caller supplies the request only."""
    return {
        "type": "object",
        "properties": {
            "prompt": {
                "type": "string",
                "description": "What the remote agent is asked to do.",
            }
        },
        "required": ["prompt"],
        "additionalProperties": False,
    }
