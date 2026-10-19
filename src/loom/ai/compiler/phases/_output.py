"""Output phase: schema validation, ``type_ref`` resolution, boundary type build.

A ``json_schema`` output compiles to a concrete type via
``msgspec.defstruct()`` and the plan stores a :class:`~loom.core.model.LoomType`
built once over it (:func:`~loom.core.model.msgspec_type`), so nothing
reflects per invocation (research R-004, invariant 5).  A ``type_ref`` output
accepts a strict ``msgspec.Struct`` or ``pydantic.BaseModel`` subclass,
resolved through :func:`~loom.core.model.loom_type`, which checks strictness
for both libraries and rejects anything else with
``UnsupportedBoundaryType``: both satisfy invariant 5 with a strict decode,
so both may own the pass-through of the validated bytes.

:func:`_resolve_symbol` and :func:`_schema_to_annotation` are shared with the
state phase (:mod:`loom.ai.compiler.phases._state`), which compiles the same
two authored shapes — a symbol reference and a hand-written JSON Schema —
into a schema and an annotation. Each caller supplies its own issue code
and, for the schema path, its own generated-struct name; nothing about the
issue codes, the messages, or the exceptions caught changes between the two
callers. What differs between the two ``type_ref``/``deps_type`` paths stays
local to each phase: the output side requires every resolved symbol to
compile through :func:`~loom.core.model.loom_type` (invariant 5); the state
side only requires that of a resolved symbol that is a class, and falls back
to ``msgspec.json.schema()`` for a container or alias, so a ``deps_type``
that never named a class keeps compiling.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast

import msgspec

from loom.ai.abc import OutputCheck
from loom.ai.compiler._plan import CompiledOutput
from loom.ai.declarative import JsonSchemaOutput, OutputSpec, TypeRefOutput
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode
from loom.core.model import LoomType, UnsupportedBoundaryType, loom_type, msgspec_type
from loom.core.symbols import import_symbol

_CompileResult = tuple[CompiledOutput | None, list[AgentCompilationIssue]]

# ``Any`` in the values: the mapped Python annotations are runtime objects
# (``str``, ``list[...]``, generated structs) that a static alias cannot name.
_SCALAR_TYPES: Mapping[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "null": type(None),
}


def compile_output(output: OutputSpec, component: str) -> _CompileResult:
    """Compile the output declaration into a :class:`CompiledOutput`.

    Args:
        output: Declared output variant.
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled output (or ``None`` on failure) and the issues found.
    """
    if isinstance(output, JsonSchemaOutput):
        return _compile_json_schema(output, component)
    return _compile_type_ref(output, component)


def compile_output_check(
    ref: str | None, component: str
) -> tuple[OutputCheck | None, list[AgentCompilationIssue]]:
    """Resolve the artifact's ``output_check`` reference, when it declares one.

    Args:
        ref: ``module:symbol`` reference from :attr:`~loom.ai.declarative.AgentSpecV1.output_check`,
            or ``None`` when the artifact declares no check.
        component: Artifact path or agent name the issue points at.

    Returns:
        The resolved :data:`~loom.ai.abc.OutputCheck`, or ``None`` when *ref*
        is ``None`` or does not resolve; paired with the issues found.
    """
    if ref is None:
        return None, []
    symbol, issues = _resolve_symbol(ref, component, AgentErrorCode.OUTPUT_CHECK_UNRESOLVABLE)
    if symbol is None:
        return None, issues
    return cast("OutputCheck", symbol), []


def _compile_json_schema(output: JsonSchemaOutput, component: str) -> _CompileResult:
    compiled, issues = _schema_to_annotation(
        output.schema, "CompiledOutputModel", component, AgentErrorCode.OUTPUT_SCHEMA_INVALID
    )
    if compiled is None:
        return None, issues
    schema, lt = compiled
    return CompiledOutput(schema=schema, loom_type=lt), []


def _compile_type_ref(output: TypeRefOutput, component: str) -> _CompileResult:
    symbol, issues = _resolve_symbol(
        output.ref, component, AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE
    )
    if symbol is None:
        return None, issues
    try:
        lt = loom_type(symbol)
    except UnsupportedBoundaryType as exc:
        return None, [
            AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED.issue(
                component=component, ref=output.ref, reason=str(exc)
            )
        ]
    return CompiledOutput(schema=MappingProxyType(lt.schema()), loom_type=lt), []


def _resolve_symbol(
    ref: str, component: str, unresolvable: AgentErrorCode
) -> tuple[Any | None, list[AgentCompilationIssue]]:
    """Resolve *ref*, or report it under the *unresolvable* code, which takes ``component, ref``.

    Shared with the state ``deps_type`` symbol path (:mod:`loom.ai.compiler.phases._state`).
    """
    try:
        return import_symbol(ref), []
    except (ImportError, AttributeError, ValueError):
        return None, [unresolvable.issue(component=component, ref=ref)]


def _schema_to_annotation(
    schema: Mapping[str, Any], model_name: str, component: str, invalid: AgentErrorCode
) -> tuple[tuple[Mapping[str, Any], LoomType] | None, list[AgentCompilationIssue]]:
    """Compile a hand-written JSON Schema object into a ``(schema, loom_type)`` pair.

    *model_name* names the generated struct type; the *invalid* code, which
    takes ``component, reason``, reports a structural fault, a failure to map
    the schema to a runtime annotation, or a failure of
    :func:`~loom.core.model.msgspec_type` to build a decoder over it — the
    annotation mapping and the decoder build both run inside the same
    guard, so either failure is a coded compilation issue and never an
    unhandled exception.
    """
    fault = _schema_fault(schema)
    if fault is not None:
        return None, [invalid.issue(component=component, reason=fault)]
    try:
        annotation = _annotation_for(schema, model_name)
        lt = msgspec_type(annotation)
    except (TypeError, ValueError, UnsupportedBoundaryType) as exc:
        return None, [invalid.issue(component=component, reason=str(exc))]
    return (MappingProxyType(dict(schema)), lt), []


def _schema_fault(schema: Mapping[str, Any]) -> str | None:
    """Return the first structural fault of a JSON Schema object, if any."""
    type_value = schema.get("type")
    if type_value is not None and not isinstance(type_value, str):
        return "'type' must be a string"
    required_fault = _required_fault(schema.get("required"))
    if required_fault is not None:
        return required_fault
    properties_fault = _properties_fault(schema.get("properties"))
    if properties_fault is not None:
        return properties_fault
    items = schema.get("items")
    if items is None:
        return None
    if not isinstance(items, Mapping):
        return "'items' must be a schema object"
    return _schema_fault(items)


def _required_fault(required: object) -> str | None:
    if required is None:
        return None
    if isinstance(required, str) or not isinstance(required, Sequence):
        return "'required' must be an array of strings"
    if not all(isinstance(name, str) for name in required):
        return "'required' must be an array of strings"
    return None


def _properties_fault(properties: object) -> str | None:
    if properties is None:
        return None
    if not isinstance(properties, Mapping):
        return "'properties' must be an object mapping names to schemas"
    for name, sub_schema in properties.items():
        if not isinstance(sub_schema, Mapping):
            return f"property '{name}' must be a schema object"
        fault = _schema_fault(sub_schema)
        if fault is not None:
            return fault
    return None


def _annotation_for(schema: Mapping[str, Any], name: str) -> Any:
    """Map a validated JSON Schema node to a runtime type annotation."""
    type_value = schema.get("type")
    if type_value == "object":
        return _object_annotation(schema, name)
    if type_value == "array":
        items = schema.get("items")
        item = _annotation_for(items, f"{name}Item") if items is not None else Any
        # Runtime subscription with a computed annotation. The explicit
        # __class_getitem__ call this used to make is correct but trips
        # Sonar S930, which miscounts cls on a builtin classmethod; the
        # subscript is the same object and reads better.
        # Subscript a value typed Any: mypy rejects ``list[item]`` in type
        # position with a computed argument, and the explicit
        # __class_getitem__ call trips Sonar S930, which miscounts cls on a
        # builtin classmethod. Going through a value satisfies both and is
        # the same object at runtime.
        list_type: Any = list
        return list_type[item]
    if isinstance(type_value, str):
        return _SCALAR_TYPES.get(type_value, Any)
    return Any


def _object_annotation(schema: Mapping[str, Any], name: str) -> Any:
    """Build a strict struct type for an object node with declared properties."""
    properties: Mapping[str, Mapping[str, Any]] = schema.get("properties") or {}
    if not properties:
        return dict[str, Any]
    required = frozenset(schema.get("required") or ())
    mandatory: list[tuple[str, Any] | tuple[str, Any, Any]] = []
    optional: list[tuple[str, Any] | tuple[str, Any, Any]] = []
    for prop, sub_schema in properties.items():
        annotation = _annotation_for(sub_schema, f"{name}_{prop}")
        if prop in required:
            mandatory.append((prop, annotation))
        else:
            optional.append((prop, annotation | None, None))
    return msgspec.defstruct(name, [*mandatory, *optional], frozen=True, forbid_unknown_fields=True)
