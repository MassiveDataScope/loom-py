"""State phase: resolve ``deps_type``/``deps_schema`` into one :class:`StateShape`.

``deps_schema``, ``deps_type: <symbol>`` and ``deps_type: dict`` are three
authored spellings of one thing: an optional JSON Schema (FR-003).
:func:`compile_state` resolves all three to one small frozen value, built by
three branches of one factory function.

The symbol-resolution step and the hand-written-schema-to-annotation step are
not reimplemented here: both come from :mod:`loom.ai.compiler.phases._output`
(:func:`~loom.ai.compiler.phases._output._resolve_symbol`,
:func:`~loom.ai.compiler.phases._output._schema_to_annotation`), parametrised
by this module's own issue codes.

A resolved ``deps_type`` symbol that is a class is compiled the same way the
output side compiles a ``type_ref`` (D3): :func:`~loom.core.model.loom_type`
decides the library and checks strictness once. A symbol that is not a class
— a container or alias a hand-authored ``deps_type`` may still resolve to,
such as ``dict[str, int]`` — falls back to ``msgspec.json.schema()`` plus
:func:`~loom.core.model.msgspec_type`, exactly as before this train, so no
``deps_type`` that compiled before it stops compiling (FR-007). Either way
state forbids unknown fields exactly as the output side does (FR-017):
:func:`_schema_admits_unknown_fields` checks the derived schema's own
``additionalProperties`` verdict for the fallback path, and ``loom_type``
checks strictness directly for the class path.

This module does not import ``pydantic_ai`` or any other engine package: the
compiler is engine-agnostic.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import msgspec

from loom.ai.abc import StateShape
from loom.ai.compiler.phases._output import _resolve_symbol, _schema_to_annotation
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.model import UnsupportedBoundaryType, loom_type, msgspec_type

_CompileResult = tuple[StateShape | None, list[AgentCompilationIssue]]


def compile_state(
    deps_type: str | None,
    deps_schema: Mapping[str, Any] | None,
    component: str,
) -> _CompileResult:
    """Compile a ``deps_type``/``deps_schema`` declaration into a :class:`StateShape`.

    Args:
        deps_type: Declared ``deps_type`` value: ``"dict"``, a
            ``module:Symbol`` reference, or ``None``.
        deps_schema: Declared ``deps_schema`` value, or ``None``.
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled state shape (``None`` when neither field is declared, or
        on failure) and the issues found.
    """
    if deps_type is not None and deps_schema is not None:
        return None, [AgentErrorCode.STATE_DECLARATION_CONFLICT.issue(component=component)]
    if deps_type == "dict":
        return StateShape(schema=None, loom_type=None), []
    if deps_type is not None:
        return _compile_type_ref(deps_type, component)
    if deps_schema is not None:
        return _compile_schema(deps_schema, component)
    return None, []


def _compile_type_ref(ref: str, component: str) -> _CompileResult:
    symbol, issues = _resolve_symbol(ref, component, AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE)
    if symbol is None:
        return None, issues
    if isinstance(symbol, type):
        try:
            lt = loom_type(symbol)
        except UnsupportedBoundaryType as exc:
            return None, [
                AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
                    component=component, ref=ref, reason=str(exc)
                )
            ]
        return StateShape(schema=MappingProxyType(lt.schema()), loom_type=lt), []
    try:
        schema = msgspec.json.schema(symbol)
        lt = msgspec_type(symbol)
    except (TypeError, UnsupportedBoundaryType) as exc:
        return None, [
            AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
                component=component, ref=ref, reason=str(exc)
            )
        ]
    if _schema_admits_unknown_fields(schema):
        return None, [
            AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
                component=component,
                ref=ref,
                reason="the struct must declare forbid_unknown_fields=True: state decoded from a "
                "caller's request body must reject a field the artifact did not declare, "
                "exactly as the output side already requires (FR-017)",
            )
        ]
    return StateShape(schema=MappingProxyType(schema), loom_type=lt), []


def _schema_admits_unknown_fields(schema: Mapping[str, Any]) -> bool:
    """Return whether *schema* lacks ``additionalProperties: false`` on its root ``$defs`` entry.

    Reads msgspec's own verdict rather than inspecting the resolved symbol
    (FR-004): a struct schema built with ``forbid_unknown_fields=True``
    carries the key (measured); a schema msgspec did not wrap in ``$defs`` —
    a scalar, a list, a bare ``dict`` — carries no such notion and answers
    ``False``.
    """
    defs = schema.get("$defs")
    ref = schema.get("$ref")
    if not isinstance(defs, Mapping) or not isinstance(ref, str):
        return False
    node = defs.get(ref.removeprefix("#/$defs/"))
    if not isinstance(node, Mapping):
        return False
    return node.get("additionalProperties") is not False


def _compile_schema(schema: Mapping[str, Any], component: str) -> _CompileResult:
    compiled, issues = _schema_to_annotation(
        schema, "CompiledStateModel", component, AgentErrorCode.STATE_SCHEMA_INVALID
    )
    if compiled is None:
        return None, issues
    result_schema, lt = compiled
    return StateShape(schema=result_schema, loom_type=lt), []
