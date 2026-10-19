"""OpenAPI schema helpers for FastAPI route binding.

Builds request/response schema fragments from Loom UseCase contracts while
keeping runtime execution transport-agnostic.
"""

from __future__ import annotations

from http import HTTPStatus
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import msgspec

from loom.core.command.introspection import (
    get_calculated_fields,
    get_internal_fields,
    get_patch_fields,
)
from loom.core.engine.plan import ExecutionPlan
from loom.core.model import pydantic_type
from loom.core.model.json_schema import DEFS, DEFS_PREFIX, REF
from loom.core.repository.abc.query import PaginationMode, QuerySpec
from loom.rest.compiler import CompiledRoute, QueryField
from loom.rest.constants import LIMIT_DEFAULT, LOCATION_HEADER, QueryParam

JsonSchema = dict[str, Any]

_LOCATION_HEADER_SCHEMA: JsonSchema = {
    LOCATION_HEADER: {
        "description": "URI of the created resource, when the use case gives one.",
        "schema": {"type": "string", "format": "uri-reference"},
    }
}

_PARAM_IN_QUERY = "query"
_PARAM_NAME = "name"
_PARAM_IN = "in"
_PARAM_REQUIRED = "required"
_PARAM_SCHEMA = "schema"
_PARAM_DESCRIPTION = "description"

# Re-export for backwards compatibility — consumers may import from this module.
QUERY_PARAM_PAGE = QueryParam.PAGE
QUERY_PARAM_LIMIT = QueryParam.LIMIT
QUERY_PARAM_PAGINATION = QueryParam.PAGINATION
QUERY_PARAM_AFTER = QueryParam.AFTER
QUERY_PARAM_CURSOR = QueryParam.CURSOR
QUERY_PARAM_SORT = QueryParam.SORT
QUERY_PARAM_DIRECTION = QueryParam.DIRECTION
QUERY_PARAM_PROFILE = QueryParam.PROFILE

QUERY_SPEC_PARAMETER_NAMES: tuple[str, ...] = (
    QueryParam.PAGE,
    QueryParam.LIMIT,
    QueryParam.PAGINATION,
    QueryParam.AFTER,
    QueryParam.CURSOR,
    QueryParam.SORT,
    QueryParam.DIRECTION,
)

_COMPONENT_REF_PREFIX = "#/components/schemas/"


def build_request_body_schema(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    """Return OpenAPI ``requestBody`` schema for the route, if it has Input().

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas. When provided, nested ``$defs`` are extracted
            as ``#/components/schemas/`` entries.

    Returns:
        OpenAPI ``requestBody`` fragment, or ``None`` when the route has no
        ``Input()`` binding.
    """
    plan = _get_execution_plan(compiled_route)
    if plan is None or plan.input_binding is None:
        return None

    request_schema = _command_request_schema(plan.input_binding.command_type, component_registry)
    if request_schema is None:
        return None

    return {
        "required": True,
        "content": {
            "application/json": {
                "schema": request_schema,
            }
        },
    }


def build_success_response_schema(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    """Return OpenAPI response entry for the route success status, if resolvable.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas. When provided, nested ``$defs`` are extracted
            as ``#/components/schemas/`` entries.

    Returns:
        OpenAPI response schema fragment, or ``None`` when the return type
        cannot be resolved to a JSON Schema.
    """
    return _response_entry(compiled_route.result_type, component_registry)


def build_success_responses(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> dict[int | str, JsonSchema]:
    """Return the OpenAPI success responses of the route, keyed by status.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas.

    Returns:
        The route's ``status_code`` entry, plus a ``201`` entry when the use
        case may return ``Created``; a status whose type has no JSON Schema
        is left out.
    """
    responses: dict[int | str, JsonSchema] = {}
    success = build_success_response_schema(compiled_route, component_registry)
    if success is not None:
        responses[compiled_route.route.status_code] = success
    if compiled_route.created_type is not None:
        created = _response_entry(compiled_route.created_type, component_registry)
        if created is not None:
            responses[HTTPStatus.CREATED.value] = {**created, "headers": _LOCATION_HEADER_SCHEMA}
    return responses


def build_query_parameters_schema(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> list[JsonSchema]:
    """Return OpenAPI query parameters inferred from the use-case contract.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas, such as the enums of a declared query.

    Returns:
        The ``QuerySpec`` parameters or the declared ``Query()`` fields,
        followed by ``profile`` when the route exposes it.
    """
    use_case_type = compiled_route.route.use_case
    hints = get_type_hints(use_case_type.execute)
    params: list[JsonSchema] = []

    if _has_query_spec_parameter(hints):
        params.extend(
            _query_spec_openapi_parameters(
                compiled_route.effective_pagination_mode, compiled_route.effective_max_limit
            )
        )

    params.extend(
        _declared_query_parameter(field, component_registry)
        for field in compiled_route.query_fields
    )

    if _has_profile_parameter(hints) and compiled_route.effective_expose_profile:
        profile_param: JsonSchema = {
            _PARAM_NAME: QUERY_PARAM_PROFILE,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Projection profile used by repository mappings.",
        }
        allowed = compiled_route.effective_allowed_profiles
        if allowed:
            profile_param[_PARAM_SCHEMA]["enum"] = list(allowed)
        params.append(profile_param)

    return params


def _declared_query_parameter(
    field: QueryField, component_registry: dict[str, JsonSchema] | None
) -> JsonSchema:
    schema = {key: value for key, value in field.schema.items() if key != "title"}
    description = schema.pop(_PARAM_DESCRIPTION, None)
    parameter: JsonSchema = {
        _PARAM_NAME: field.name,
        _PARAM_IN: _PARAM_IN_QUERY,
        _PARAM_REQUIRED: field.required,
        _PARAM_SCHEMA: _with_component_refs(schema, component_registry),
    }
    if description is not None:
        parameter[_PARAM_DESCRIPTION] = description
    return parameter


def _get_execution_plan(compiled_route: CompiledRoute) -> ExecutionPlan | None:
    plan = getattr(compiled_route.route.use_case, "__execution_plan__", None)
    if isinstance(plan, ExecutionPlan):
        return plan
    return None


def _has_profile_parameter(type_hints: dict[str, Any]) -> bool:
    return "profile" in type_hints


def _has_query_spec_parameter(type_hints: dict[str, Any]) -> bool:
    for annotation in type_hints.values():
        if annotation is QuerySpec:
            return True
        origin = get_origin(annotation)
        args = get_args(annotation)
        if origin in (UnionType, Union) and QuerySpec in args:
            return True
    return False


def _query_spec_openapi_parameters(
    default_mode: PaginationMode, max_limit: int
) -> list[JsonSchema]:
    mode_values = [item.value for item in PaginationMode]
    return [
        {
            _PARAM_NAME: QUERY_PARAM_PAGE,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "integer", "minimum": 1, "default": 1},
            _PARAM_DESCRIPTION: "Offset page number.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_LIMIT,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {
                "type": "integer",
                "minimum": 1,
                "maximum": max_limit,
                "default": min(LIMIT_DEFAULT, max_limit),
            },
            _PARAM_DESCRIPTION: "Maximum rows per page.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_PAGINATION,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string", "enum": mode_values, "default": default_mode.value},
            _PARAM_DESCRIPTION: "Pagination strategy.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_AFTER,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Cursor token for cursor pagination.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_CURSOR,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Alias for 'after'.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_SORT,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Field used for ordering.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_DIRECTION,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string", "enum": ["ASC", "DESC"], "default": "ASC"},
            _PARAM_DESCRIPTION: "Sort direction.",
        },
    ]


def _response_entry(
    result_type: Any,
    component_registry: dict[str, JsonSchema] | None,
) -> JsonSchema | None:
    if result_type is Any:
        return None
    response_schema = _safe_schema(result_type, component_registry)
    if response_schema is None:
        return None
    return {"content": {"application/json": {"schema": response_schema}}}


def _command_request_schema(
    command_type: type[Any],
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    if not isinstance(command_type, type):
        return None
    if issubclass(command_type, msgspec.Struct):
        public_struct = _build_public_command_struct(command_type)
        return _safe_schema(public_struct, component_registry)

    # Fallback for user-defined plain types (dataclass/pydantic/typing).
    return _safe_schema(command_type, component_registry)


def _build_public_command_struct(command_type: type[Any]) -> type[Any]:
    struct_fields = {field.name: field for field in msgspec.structs.fields(command_type)}
    excluded = set(get_internal_fields(command_type)) | set(get_calculated_fields(command_type))
    patch_fields = set(get_patch_fields(command_type))

    definitions: list[tuple[Any, ...]] = []
    for name, sf in struct_fields.items():
        if name in excluded:
            continue

        annotation = _without_unset_type(sf.type)
        if name in patch_fields:
            definitions.append((name, _with_optional_none(annotation), None))
            continue

        if sf.default is msgspec.NODEFAULT:
            definitions.append((name, annotation))
            continue

        definitions.append((name, annotation, sf.default))

    use_camel = any(field.encode_name != field.name for field in struct_fields.values())
    return msgspec.defstruct(
        f"{command_type.__name__}Request",
        definitions,
        kw_only=True,
        rename="camel" if use_camel else None,
    )


def _safe_msgspec_schema(annotation: Any) -> JsonSchema | None:
    try:
        return msgspec.json.schema(annotation)
    except (TypeError, ValueError):
        return None


def _safe_pydantic_schema(annotation: Any) -> JsonSchema | None:
    try:
        return dict(pydantic_type(annotation).schema())
    except Exception:
        return None


def _rewrite_to_component_refs(node: Any) -> Any:
    """Rewrite ``#/$defs/X`` references to ``#/components/schemas/X`` recursively.

    Strips ``$defs`` keys and rewrites every ``$ref`` value that points to a
    local definition so the resulting fragment is valid at the OpenAPI document
    root where ``#/components/schemas/`` is resolvable.
    """
    if isinstance(node, list):
        return [_rewrite_to_component_refs(item) for item in node]
    if not isinstance(node, dict):
        return node
    result: dict[str, Any] = {}
    for k, v in node.items():
        if k == DEFS:
            continue
        if k == REF and isinstance(v, str) and v.startswith(DEFS_PREFIX):
            result[k] = _COMPONENT_REF_PREFIX + v.removeprefix(DEFS_PREFIX)
        else:
            result[k] = _rewrite_to_component_refs(v)
    return result


def _safe_schema(
    annotation: Any,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    origin = get_origin(annotation)
    if origin in (UnionType, Union):
        union_members = [
            _safe_schema(member, component_registry) for member in get_args(annotation)
        ]
        if any(member is None for member in union_members):
            return None
        members = [member for member in union_members if member is not None]
        if len(members) == 1:
            return members[0]
        return {"anyOf": members}

    raw = _safe_msgspec_schema(annotation) or _safe_pydantic_schema(annotation)
    if raw is None:
        return None
    return _with_component_refs(raw, component_registry)


def _with_component_refs(
    raw: JsonSchema, component_registry: dict[str, JsonSchema] | None
) -> JsonSchema:
    if component_registry is None:
        return raw

    defs = raw.get(DEFS)
    if isinstance(defs, dict):
        for name, def_schema in defs.items():
            component_registry[name] = _rewrite_to_component_refs(def_schema)
    rewritten: JsonSchema = _rewrite_to_component_refs(raw)
    return rewritten


def _without_unset_type(annotation: Any) -> Any:
    if annotation is msgspec.UnsetType:
        return Any

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    if origin in (UnionType, Union):
        args = tuple(arg for arg in get_args(annotation) if arg is not msgspec.UnsetType)
        if not args:
            return Any
        result = args[0]
        for arg in args[1:]:
            result = result | arg
        return result

    return annotation


def _with_optional_none(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in (UnionType, Union) and NoneType in get_args(annotation):
        return annotation
    if annotation is NoneType:
        return annotation
    return annotation | None
