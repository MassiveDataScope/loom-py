"""Resolution of declarative config bindings inside streaming flows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeGuard

from loom.core.config import ConfigBinding, ConfigContext, ConfigError
from loom.streaming.compiler._errors import CompilationIssue, StreamingErrorCode
from loom.streaming.graph._flow import Process, ProcessNode, StreamFlow
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast, BroadcastRoute
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkKind, ForkRoute
from loom.streaming.nodes._router import Route, Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep, Step
from loom.streaming.nodes._with import With, WithAsync


def resolve_flow_bindings(
    flow: StreamFlow[Any, Any],
    ctx: ConfigContext,
) -> tuple[StreamFlow[Any, Any], list[CompilationIssue]]:
    """Resolve every config binding reachable from a streaming flow.

    The flow declaration remains declarative in user code. This helper
    materializes bindings into real runtime objects before compilation.

    Args:
        flow: User-declared streaming flow.
        ctx: Runtime config context used for binding resolution.

    Returns:
        A resolved flow plus a list of binding-resolution issues.
    """
    errors: list[CompilationIssue] = []
    resolved_process = _resolve_process(flow.process, ctx, errors)
    resolved_flow = StreamFlow(
        name=flow.name,
        source=flow.source,
        process=resolved_process,
        output=flow.output,
        errors=flow.errors,
    )
    return resolved_flow, errors


def _resolve_process(
    process: Process[Any, Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> Process[Any, Any]:
    nodes = tuple(_resolve_process_node(node, ctx, errors) for node in process.nodes)
    return Process(*nodes)


def _resolve_process_node(
    node: ProcessNode,
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> ProcessNode:
    if _is_step_class(node):
        try:
            return node()
        except TypeError as exc:
            errors.append(
                StreamingErrorCode.STEP_INSTANTIATION_FAILED.issue(component=node, exc=exc)
            )
            return node
    if isinstance(node, ConfigBinding):
        resolved = _resolve_binding(node, ctx, errors)
        if _is_process_node(resolved):
            return resolved
        binding_name = node.config_path or node.target.__name__
        errors.append(StreamingErrorCode.BINDING_NOT_PROCESS_NODE.issue(component=binding_name))
        return node
    if isinstance(node, With):
        return _resolve_with(node, ctx, errors)
    if isinstance(node, WithAsync):
        return _resolve_with_async(node, ctx, errors)
    if isinstance(node, Router):
        return _resolve_router(node, ctx, errors)
    if isinstance(node, Fork):
        return _resolve_fork(node, ctx, errors)
    if isinstance(node, Broadcast):
        return _resolve_broadcast(node, ctx, errors)
    if isinstance(node, ExpandRoutes):
        return _resolve_expand_routes(node, ctx, errors)
    return node


def _resolve_binding(
    binding: ConfigBinding,
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> object:
    try:
        return ctx.resolve(binding)
    except (ConfigError, TypeError, ValueError) as exc:
        binding_name = binding.config_path or binding.target.__name__
        errors.append(
            StreamingErrorCode.BINDING_RESOLUTION_FAILED.issue(component=binding_name, exc=exc)
        )
        return binding


def _resolve_with(
    node: With[Any, Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> With[Any, Any]:
    resolved_process = _resolve_process(node.process, ctx, errors)
    resolved_deps = _resolve_dependencies(node.sync_contexts, ctx, errors)
    resolved_deps.update(_resolve_dependencies(node.context_factories, ctx, errors))
    resolved_deps.update(_resolve_dependencies(node.plain_deps, ctx, errors))
    return With(scope=node.scope, process=resolved_process, **resolved_deps)


def _resolve_with_async(
    node: WithAsync[Any, Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> WithAsync[Any, Any]:
    resolved_process = _resolve_process(node.process, ctx, errors)
    resolved_deps = _resolve_dependencies(node.async_contexts, ctx, errors)
    resolved_deps.update(_resolve_dependencies(node.context_factories, ctx, errors))
    resolved_deps.update(_resolve_dependencies(node.plain_deps, ctx, errors))
    return WithAsync(
        scope=node.scope,
        max_concurrency=node.max_concurrency,
        task_timeout_ms=node.task_timeout_ms,
        process=resolved_process,
        **resolved_deps,
    )


def _resolve_router(
    node: Router[Any, Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> Router[Any, Any]:
    default = _resolve_process(node.default, ctx, errors) if node.default else None
    routes = {key: _resolve_process(process, ctx, errors) for key, process in node.routes.items()}
    predicate_routes = tuple(
        Route(
            when=route.when,
            process=_resolve_process(route.process, ctx, errors),
        )
        for route in node.predicate_routes
    )
    return Router(
        selector=node.selector, routes=routes, predicate_routes=predicate_routes, default=default
    )


def _resolve_fork(
    node: Fork[Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> Fork[Any]:
    default = _resolve_process(node.default, ctx, errors) if node.default else None
    if node.kind is ForkKind.KEYED:
        selector = node.selector
        if selector is None:
            errors.append(StreamingErrorCode.FORK_SELECTOR_MISSING.issue())
            return node
        routes = {
            key: _resolve_process(process, ctx, errors) for key, process in node.routes.items()
        }
        return Fork.by(selector, routes, default=default)
    predicate_routes = tuple(
        ForkRoute(
            when=route.when,
            process=_resolve_process(route.process, ctx, errors),
        )
        for route in node.predicate_routes
    )
    return Fork.when(predicate_routes, default=default)


def _resolve_broadcast(
    node: Broadcast[Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> Broadcast[Any]:
    routes = tuple(
        BroadcastRoute(
            process=_resolve_process(route.process, ctx, errors),
            output=route.output,
        )
        for route in node.routes
    )
    return Broadcast(*routes)


def _resolve_expand_routes(
    node: ExpandRoutes[Any],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> ExpandRoutes[Any]:
    routes = {
        output_type: _resolve_process(process, ctx, errors)
        for output_type, process in node.routes.items()
    }
    default = _resolve_process(node.default, ctx, errors) if node.default else None
    return ExpandRoutes(expander=node.expander, routes=routes, default=default)


def _resolve_dependencies(
    dependencies: Mapping[str, object],
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> dict[str, object]:
    return {name: _resolve_dependency(value, ctx, errors) for name, value in dependencies.items()}


def _resolve_dependency(
    value: object,
    ctx: ConfigContext,
    errors: list[CompilationIssue],
) -> object:
    if isinstance(value, ConfigBinding):
        return _resolve_binding(value, ctx, errors)
    return value


def _is_process_node(value: object) -> TypeGuard[ProcessNode]:
    return isinstance(
        value,
        (
            ConfigBinding,
            RecordStep,
            BatchStep,
            ExpandStep,
            BatchExpandStep,
            With,
            WithAsync,
            CollectBatch,
            ForEach,
            Drain,
            IntoTopic,
            Fork,
            Router,
            Broadcast,
            ExpandRoutes,
        ),
    )


def _is_step_class(value: object) -> TypeGuard[type[Step[Any, Any]]]:
    return isinstance(value, type) and issubclass(value, Step)
