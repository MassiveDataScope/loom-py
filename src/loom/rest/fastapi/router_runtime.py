"""Router runtime — binds CompiledRoute records to a FastAPI application.

Generates async handler functions at startup, one per
:class:`~loom.rest.compiler.CompiledRoute`.  Each handler:

1. Builds the :class:`~loom.core.use_case.use_case.UseCase` instance via the
   :class:`~loom.core.use_case.factory.UseCaseFactory`.
2. Extracts path parameters from ``request.path_params`` (populated by
   Starlette's routing layer from the URL).
3. Reads the raw request body and decodes it with ``msgspec.json.decode``,
   answering ``400`` when it is empty, not well-formed JSON or nested too
   deeply to decode, and ``422`` when it is not a JSON object.
4. Drives execution through :class:`~loom.core.engine.executor.RuntimeExecutor`.
5. Returns a :class:`~loom.rest.fastapi.response.MsgspecJSONResponse`.

Handler ``__signature__`` is manipulated so FastAPI validates and documents
path parameters correctly in OpenAPI while keeping the implementation generic.

No reflection occurs at request time — all structural decisions (path params,
status codes, tags) are taken from the ``CompiledRoute`` produced at startup.
"""

from __future__ import annotations

import inspect
import logging
import re
import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import msgspec
from fastapi import FastAPI, HTTPException
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import BODY_FIELD, Forbidden, LoomError
from loom.core.identity import Identity, current_identity
from loom.core.model import BoundaryValidationError
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc.query import (
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.tracing import get_trace_id
from loom.core.use_case.factory import UseCaseFactory
from loom.rest._body import MALFORMED_BODY_ERRORS, BodyTooLarge
from loom.rest.compiler import CompiledRoute
from loom.rest.constants import QueryParam
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi._errors import internal_error_response, malformed_body_error
from loom.rest.fastapi.openapi import (
    QUERY_SPEC_PARAMETER_NAMES,
    build_query_parameters_schema,
    build_request_body_schema,
    build_success_response_schema,
)
from loom.rest.fastapi.response import MsgspecJSONResponse

_logger = logging.getLogger(__name__)
_error_mapper = HttpErrorMapper()

_DEFAULT_PAGE = 1
_DEFAULT_LIMIT = 50
_NOT_AUTHORIZED_MESSAGE = "You are not authorized to access this route."
_NOT_AN_OBJECT_MESSAGE = "Request body must be a JSON object"


def _authorize_route(identity: Identity, required_roles: tuple[str, ...], route: str) -> None:
    """Refuse callers holding none of the roles the route declares.

    Holding **any** declared role grants access.  The refusal is a ``403`` for
    an anonymous caller too: whether authenticating would have helped is part
    of the route's policy, and the response must not become an oracle for it.
    The message stays generic; the audit trail is server-side.

    Args:
        identity: Verified caller of the request.
        required_roles: Roles resolved for the route at compile time.
        route: Full path, recorded in the audit log.

    Raises:
        Forbidden: When the caller holds none of *required_roles*.
    """
    if not required_roles or any(identity.has_role(role) for role in required_roles):
        return
    _logger.warning(
        "Route authorization denied: route=%s subject=%s mechanism=%s required_roles=%s",
        route,
        identity.subject,
        identity.mechanism,
        ",".join(required_roles),
    )
    raise Forbidden(_NOT_AUTHORIZED_MESSAGE)


def _extract_path_params(path: str) -> list[str]:
    """Return ordered path-parameter names from a FastAPI path template.

    Args:
        path: Path string, e.g. ``"/{user_id}/orders/{order_id}"``.

    Returns:
        List of parameter names in declaration order, e.g.
        ``["user_id", "order_id"]``.
    """
    return re.findall(r"\{(\w+)\}", path)


def _normalize_path_param_annotation(annotation: Any) -> Any:
    if annotation is inspect._empty or annotation is Any:
        return str

    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        args = typing.get_args(annotation)
        if not args:
            return str
        return _normalize_path_param_annotation(args[0])

    if origin in {typing.Union, types.UnionType}:
        args = tuple(arg for arg in typing.get_args(annotation) if arg is not type(None))
        if len(args) != 1:
            return str
        return _normalize_path_param_annotation(args[0])

    if isinstance(annotation, type):
        return annotation
    return str


_RESERVED_QUERY_KEYS = frozenset((*QUERY_SPEC_PARAMETER_NAMES, QueryParam.PROFILE))
_FILTER_OP_VALUES = frozenset(item.value for item in FilterOp)


def _camel_to_snake(value: str) -> str:
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", value)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    return s.lower()


def _normalize_field_name(value: str) -> str:
    if any(char.isupper() for char in value):
        return _camel_to_snake(value)
    return value


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_pagination_mode(
    raw: str | None,
    cursor: str | None,
    *,
    default_mode: PaginationMode,
    allow_override: bool,
) -> PaginationMode:
    if not allow_override:
        if raw is not None and raw.lower() != default_mode.value:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=(
                    "Query parameter 'pagination' cannot override this route's "
                    f"default mode ({default_mode.value!r})."
                ),
            )
        if cursor is not None and default_mode is PaginationMode.OFFSET:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=(
                    "Cursor parameters are not allowed when pagination mode is fixed to 'offset'."
                ),
            )
        return default_mode

    if raw is None:
        if cursor is not None:
            return PaginationMode.CURSOR
        return default_mode
    try:
        return PaginationMode(raw.lower())
    except ValueError as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Invalid pagination mode: {raw!r}.",
        ) from exc


def _parse_sort(sort_field: str | None, direction_raw: str) -> tuple[SortSpec, ...]:
    direction = direction_raw.upper()
    if direction not in {"ASC", "DESC"}:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="direction must be 'ASC' or 'DESC'."
        )
    requested_fields = (sort_field,) if sort_field else ()
    return tuple(
        SortSpec(field=_normalize_field_name(field), direction=typing.cast(Any, direction))
        for field in requested_fields
    )


def _parse_filter_specs(query_params: QueryParams) -> list[FilterSpec]:
    filters: list[FilterSpec] = []
    for key, raw_value in query_params.items():
        if key in _RESERVED_QUERY_KEYS:
            continue

        parts = [part for part in key.split("__") if part]
        if not parts:
            continue

        maybe_op = parts[-1].lower()
        if maybe_op in _FILTER_OP_VALUES:
            op = FilterOp(maybe_op)
            field_parts = parts[:-1]
        else:
            op = FilterOp.EQ
            field_parts = parts

        if not field_parts:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST, detail=f"Invalid filter field: {key!r}."
            )
        field = ".".join(_normalize_field_name(part) for part in field_parts)

        if op == FilterOp.IN:
            value = [_coerce_scalar(item) for item in raw_value.split(",") if item != ""]
        else:
            value = _coerce_scalar(raw_value)

        filters.append(FilterSpec(field=field, op=op, value=value))
    return filters


def _positive_int(query_params: QueryParams, name: str, default: int) -> int:
    """Read a positive integer query parameter, refusing anything else.

    A non-numeric value used to raise inside the handler and surface as a 500;
    it is a client error and must read as one.
    """
    raw = query_params.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Query parameter {name!r} must be an integer, got {raw!r}.",
        ) from exc
    if value < 1:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Query parameter {name!r} must be greater than or equal to 1.",
        )
    return value


def _build_query_spec(
    request: Request,
    *,
    default_pagination_mode: PaginationMode,
    allow_pagination_override: bool,
    max_limit: int,
) -> QuerySpec:
    query_params = request.query_params
    page = _positive_int(query_params, QueryParam.PAGE, _DEFAULT_PAGE)
    # Clamped rather than refused: a caller asking for more rows than the API
    # serves gets the maximum, but never turns one request into a full scan.
    limit = min(_positive_int(query_params, QueryParam.LIMIT, _DEFAULT_LIMIT), max_limit)
    cursor = query_params.get(QueryParam.AFTER) or query_params.get(QueryParam.CURSOR)
    pagination = _parse_pagination_mode(
        query_params.get(QueryParam.PAGINATION),
        cursor,
        default_mode=default_pagination_mode,
        allow_override=allow_pagination_override,
    )
    sort = _parse_sort(
        query_params.get(QueryParam.SORT),
        query_params.get(QueryParam.DIRECTION, "ASC"),
    )
    filters = _parse_filter_specs(query_params)
    filter_group = FilterGroup(filters=tuple(filters)) if filters else None
    return QuerySpec(
        filters=filter_group,
        sort=sort,
        pagination=pagination,
        limit=limit,
        page=page,
        cursor=cursor,
    )


def _resolve_query_param_name(uc_type: type[Any]) -> str | None:
    execute_sig = inspect.signature(uc_type.execute)
    hints = typing.get_type_hints(uc_type.execute)
    for name, param in execute_sig.parameters.items():
        if name == "self":
            continue
        annotation = hints.get(name, param.annotation)
        origin = typing.get_origin(annotation)
        args = typing.get_args(annotation)
        if annotation is QuerySpec:
            return name
        if origin in {typing.Union, types.UnionType} and QuerySpec in args:
            return name
    return None


def _route_docs(compiled_route: CompiledRoute) -> tuple[str | None, str | None]:
    """Resolve OpenAPI summary/description from route metadata or UseCase docs."""
    summary = compiled_route.route.summary.strip()
    description = compiled_route.route.description.strip()
    if summary or description:
        return summary or None, description or None

    uc_doc = inspect.getdoc(compiled_route.route.use_case) or ""
    cleaned_lines = tuple(line.strip() for line in uc_doc.splitlines() if line.strip())
    if not cleaned_lines:
        return None, None

    auto_summary, *rest = cleaned_lines
    auto_description = "\n".join(rest) if rest else None
    return auto_summary, auto_description


@dataclass(frozen=True, slots=True)
class _RouteRuntime:
    """Per-route facts resolved once at startup and read on every request."""

    compiled_route: CompiledRoute
    path_params: tuple[str, ...]
    accepts_profile_param: bool
    query_param_name: str | None
    has_input_binding: bool

    @property
    def use_case_type(self) -> type[Any]:
        return self.compiled_route.route.use_case


def _build_route_runtime(compiled_route: CompiledRoute) -> _RouteRuntime:
    uc_type = compiled_route.route.use_case
    plan = getattr(uc_type, "__execution_plan__", None)
    return _RouteRuntime(
        compiled_route=compiled_route,
        path_params=tuple(_extract_path_params(compiled_route.route.path)),
        accepts_profile_param="profile" in inspect.signature(uc_type.execute).parameters,
        query_param_name=_resolve_query_param_name(uc_type),
        has_input_binding=plan is not None and plan.input_binding is not None,
    )


def _resolve_profile(request: Request, compiled_route: CompiledRoute) -> str:
    """Return the profile the request runs under, refusing values the route bans."""
    requested = request.query_params.get(QueryParam.PROFILE)
    if requested is None:
        return compiled_route.effective_profile_default

    if not compiled_route.effective_expose_profile:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Query parameter 'profile' is not allowed for this route.",
        )

    allowed = compiled_route.effective_allowed_profiles
    if allowed and requested not in allowed:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=(f"Invalid profile {requested!r}. Allowed: {', '.join(allowed)}"),
        )
    return requested


def _build_execution_params(
    request: Request, path_kwargs: Mapping[str, Any], runtime: _RouteRuntime
) -> dict[str, Any]:
    compiled_route = runtime.compiled_route
    params: dict[str, Any] = {name: path_kwargs[name] for name in runtime.path_params}
    selected_profile = _resolve_profile(request, compiled_route)
    if runtime.accepts_profile_param:
        params["profile"] = selected_profile
    if runtime.query_param_name is not None:
        params[runtime.query_param_name] = _build_query_spec(
            request,
            default_pagination_mode=compiled_route.effective_pagination_mode,
            allow_pagination_override=compiled_route.effective_allow_pagination_override,
            max_limit=compiled_route.effective_max_limit,
        )
    return params


async def _decode_payload(request: Request, has_input_binding: bool) -> dict[str, Any] | None:
    """Return the decoded JSON object, or ``None`` when the route takes no input.

    The 400 and 422 messages are fixed, so the offending input is never echoed.

    Raises:
        HTTPException: ``400`` when the body is empty, not well-formed UTF-8
            JSON, or nested too deeply to decode.
        BoundaryValidationError: When the body is well-formed JSON but not an
            object.
    """
    if not has_input_binding:
        return None
    body = await request.body()
    try:
        payload = msgspec.json.decode(body)
    except MALFORMED_BODY_ERRORS as exc:
        raise malformed_body_error() from exc
    if not isinstance(payload, dict):
        raise BoundaryValidationError(
            _NOT_AN_OBJECT_MESSAGE, ((BODY_FIELD, _NOT_AN_OBJECT_MESSAGE),)
        )
    return payload


async def _execute_route(
    request: Request,
    path_kwargs: Mapping[str, Any],
    runtime: _RouteRuntime,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> Response:
    compiled_route = runtime.compiled_route
    status_code = compiled_route.route.status_code
    # The only ambient identity read of the REST layer: from here on
    # the caller travels as an explicit argument, never as a global.
    identity = current_identity()
    with observability_runtime.span(
        Scope.USE_CASE,
        runtime.use_case_type.__name__,
        trace_id=get_trace_id(),
        route=compiled_route.full_path,
        method=compiled_route.route.method.upper(),
        status_code=status_code,
        read_only=compiled_route.read_only,
    ):
        _authorize_route(
            identity, compiled_route.effective_requires_roles, compiled_route.full_path
        )
        result = await executor.execute(
            factory.build(runtime.use_case_type),
            params=_build_execution_params(request, path_kwargs, runtime),
            payload=await _decode_payload(request, runtime.has_input_binding),
            read_only=compiled_route.read_only,
            identity=identity,
        )
    return MsgspecJSONResponse(content=result, status_code=status_code)


async def _dispatch_route(
    request: Request,
    path_kwargs: Mapping[str, Any],
    runtime: _RouteRuntime,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> Response:
    """Run a route and translate any failure into the response the caller sees."""
    try:
        return await _execute_route(
            request, path_kwargs, runtime, factory, executor, observability_runtime
        )
    except HTTPException:
        raise
    except BodyTooLarge:
        # Answered as 413 by BodySizeLimitMiddleware: swallowing it here
        # would report a 500 for a perfectly diagnosable client error.
        raise
    except LoomError as exc:
        raise _error_mapper.to_http(exc) from exc
    except Exception:
        trace_id = get_trace_id() or ""
        # Without this the caller holds a trace id with no counterpart in
        # the logs: an untraceable 500, and an attacker who triggers one
        # leaves no record at all.
        _logger.exception(
            "Unhandled error in %s (route=%s trace_id=%s)",
            runtime.use_case_type.__name__,
            runtime.compiled_route.full_path,
            trace_id,
        )
        return internal_error_response()


def _handler_signature(
    path_params: Sequence[str], execute_param_types: Mapping[str, Any]
) -> inspect.Signature:
    """Expose ``request`` plus every path parameter so FastAPI validates them."""
    sig_params = [
        inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Request)
    ]
    sig_params.extend(
        inspect.Parameter(
            name,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=_normalize_path_param_annotation(execute_param_types.get(name, str)),
        )
        for name in path_params
    )
    return inspect.Signature(sig_params, return_annotation=Response)


def _make_handler(
    compiled_route: CompiledRoute,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> Any:
    """Build an async handler for the given compiled route.

    The returned callable has its ``__signature__`` overridden to expose
    path parameters so FastAPI injects and validates them correctly.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        factory: Factory used to construct the use-case instance per request.
        executor: Executor that drives the use-case pipeline.
        observability_runtime: Shared runtime used to emit a span event per request.

    Returns:
        Async callable suitable for ``FastAPI.add_api_route``.
    """
    runtime = _build_route_runtime(compiled_route)

    async def _handler(request: Request, **kwargs: Any) -> Response:
        return await _dispatch_route(
            request, kwargs, runtime, factory, executor, observability_runtime
        )

    _handler.__signature__ = _handler_signature(  # type: ignore[attr-defined]
        runtime.path_params, dict(compiled_route.execute_param_types)
    )
    _handler.__name__ = f"handle_{runtime.use_case_type.__name__}"

    return _handler


def bind_interfaces(
    app: FastAPI,
    compiled_routes: Sequence[CompiledRoute],
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> dict[str, Any]:
    """Register compiled routes on a FastAPI application.

    For each :class:`~loom.rest.compiler.CompiledRoute`, creates a dynamic
    async handler and registers it via ``app.add_api_route``.  Path parameters
    are inferred from the route path template and exposed in the handler
    signature so FastAPI validates and documents them correctly.

    Nested JSON Schema ``$defs`` produced by msgspec/pydantic are collected
    into a shared component registry and returned.  The caller is responsible
    for injecting these into ``components.schemas`` of the OpenAPI document.

    Args:
        app: FastAPI application instance to register routes on.
        compiled_routes: Ordered list of fully resolved routes produced by
            :class:`~loom.rest.compiler.RestInterfaceCompiler`.
        factory: Use-case factory for constructing instances per request.
        executor: Runtime executor that drives the use-case pipeline.
        observability_runtime: Shared runtime used to emit request lifecycle
            events around each handler execution.

    Returns:
        Mapping of schema name → JSON Schema fragment for all collected
        ``$defs`` that should appear under ``components.schemas``.

    Example::

        compiler = RestInterfaceCompiler(use_case_compiler)
        routes = compiler.compile(UserRestInterface)
        component_schemas = bind_interfaces(
            app,
            routes,
            factory,
            executor,
            observability_runtime=ObservabilityRuntime.noop(),
        )
    """
    component_registry: dict[str, Any] = {}

    for cr in compiled_routes:
        handler = _make_handler(cr, factory, executor, observability_runtime)
        summary, description = _route_docs(cr)
        request_body = build_request_body_schema(cr, component_registry)
        success_response = build_success_response_schema(cr, component_registry)
        responses: dict[int | str, dict[str, Any]] | None = None
        if success_response is not None:
            responses = {cr.route.status_code: success_response}

        openapi_extra: dict[str, Any] | None = None
        query_parameters = build_query_parameters_schema(cr)
        if request_body is not None or query_parameters:
            openapi_extra = {}
            if request_body is not None:
                openapi_extra["requestBody"] = request_body
            if query_parameters:
                openapi_extra["parameters"] = query_parameters

        app.add_api_route(
            path=cr.full_path,
            endpoint=handler,
            methods=[cr.route.method.upper()],
            summary=summary,
            description=description,
            status_code=cr.route.status_code,
            tags=list(cr.interface_tags) if cr.interface_tags else [],
            responses=responses,
            openapi_extra=openapi_extra,
        )

    return component_registry
