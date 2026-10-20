"""Validation phase for streaming flow compilation.

Each validator returns a list of :class:`CompilationIssue` — structured
failures with machine-readable codes — accumulated by the compiler into a
single :class:`~loom.streaming.compiler.CompilationError`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.keys import ConfigKey
from loom.streaming.compiler._errors import CompilationIssue, StreamingErrorCode
from loom.streaming.core._typing import StreamPayload
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import ConsumerSettings, KafkaSettings
from loom.streaming.mongo import MongoConfig
from loom.streaming.nodes._boundary import FromMultiTypeTopic, FromTopic, IntoTopic
from loom.streaming.nodes._branches import (
    PATH_END_NODE_TYPES,
    branch_ends_path,
    is_branching_node,
    iter_branches,
    router_ends_every_path,
)
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._decompose import Explode
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._table import Backend, IntoTable
from loom.streaming.nodes._table.config import (
    resolve_clickhouse_table_config,
    resolve_delta_table_config,
    resolve_sqlalchemy_table_config,
)
from loom.streaming.nodes._with import ResourceScope, With, WithAsync


def validate_storage_sinks(
    flow: StreamFlow[Any, Any],
    ctx: ConfigContext,
) -> list[CompilationIssue]:
    """Validate that every named IntoSink node has a config section at streaming.sinks.<name>."""
    errors: list[CompilationIssue] = []
    for node in _iter_unique_storage_sinks(flow.process.nodes, errors):
        errors.extend(_validate_storage_sink_node(node, ctx))
    return errors


def validate_kafka(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate Kafka settings required by *flow*."""
    if not _uses_kafka(flow):
        return []
    try:
        ctx.section(ConfigKey.KAFKA, KafkaSettings)
        return []
    except ConfigError as exc:
        return [StreamingErrorCode.KAFKA_CONFIG_INVALID.issue(exc=exc)]


def validate_delivery(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate that explicit delivery semantics do not contradict legacy flags.

    Emits :data:`StreamingErrorCode.DELIVERY_CONFLICT` when a resolved consumer
    sets ``delivery`` and an explicit ``enable_auto_commit`` that contradicts
    it.  Missing Kafka config is reported by :func:`validate_kafka`, not here.
    """
    if not _uses_kafka(flow):
        return []
    if not isinstance(flow.source, (FromTopic, FromMultiTypeTopic)):
        return []
    consumer = _resolve_consumer_settings(flow.source, ctx)
    if consumer is None:
        return []
    issues = _delivery_conflict_issues(flow.source.name, consumer)
    if consumer.effective_delivery() == "at_least_once":
        issues.extend(_unrouted_fork_issues(flow))
    return issues


def validate_mongo(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate Mongo settings required by *flow*."""
    if not isinstance(flow.source, FromMongoCDC):
        return []
    try:
        mongo = ctx.section(ConfigKey.MONGO, MongoConfig)
        mongo.source_for(flow.source.logical_ref)
        return []
    except (ConfigError, KeyError) as exc:
        return [
            StreamingErrorCode.MONGO_CONFIG_INVALID.issue(source_name=flow.source.name, exc=exc)
        ]


def validate_resources(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate resource usage across the full process tree."""
    errors: list[CompilationIssue] = []
    for node in _walk_all_process_nodes(flow.process.nodes):
        if isinstance(node, (With, WithAsync)) and node.scope == ResourceScope.BATCH:
            direct_cms = list(node.sync_contexts.keys()) + list(node.async_contexts.keys())
            if direct_cms:
                errors.append(
                    StreamingErrorCode.BATCH_SCOPE_DIRECT_CONTEXT.issue(
                        component=type(node).__name__, direct_cms=direct_cms
                    )
                )
    return errors


def validate_shapes(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate shape transitions through the process tree."""
    errors, _ = _validate_shape_sequence(flow.process.nodes, flow.source.shape)
    errors.extend(_validate_window_strategies(flow.process.nodes))
    errors.extend(_validate_scoped_process_nodes(_walk_all_process_nodes(flow.process.nodes)))
    return errors


def validate_outputs(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate terminal outputs and branch terminality."""
    errors: list[CompilationIssue] = []
    has_terminal = flow.output is not None or _has_terminal_output(flow.process.nodes)

    if flow.output is not None and _contains_fork(flow.process.nodes):
        errors.append(StreamingErrorCode.OUTPUT_WITH_FORK.issue())

    if flow.output is not None and _contains_broadcast(flow.process.nodes):
        errors.append(StreamingErrorCode.OUTPUT_WITH_BROADCAST.issue())

    last = _path_end(flow.process.nodes)
    if flow.output is not None and last is not None and not isinstance(last, (Broadcast, Fork)):
        errors.append(StreamingErrorCode.OUTPUT_AFTER_PATH_END.issue(node=type(last).__name__))

    if not has_terminal:
        errors.append(StreamingErrorCode.MISSING_TERMINAL_OUTPUT.issue())

    return errors


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    if isinstance(flow.source, (FromTopic, FromMultiTypeTopic)):
        return True
    if flow.output is not None:
        return True
    return _has_kafka_topic_output(flow.process.nodes)


def _resolve_consumer_settings(
    source: FromTopic[Any] | FromMultiTypeTopic[Any],
    ctx: ConfigContext,
) -> ConsumerSettings | None:
    """Resolve the consumer settings for one Kafka source, or None if unresolvable."""
    try:
        kafka = ctx.section(ConfigKey.KAFKA, KafkaSettings)
        return kafka.consumer_for(source.logical_ref)
    except (ConfigError, KeyError):
        return None


def _unrouted_fork_issues(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Report forks whose unmatched messages would be committed without being written."""
    issues: list[CompilationIssue] = []
    for node in _walk_all_process_nodes(flow.process.nodes):
        if isinstance(node, Fork) and node.default is None:
            issues.append(StreamingErrorCode.FORK_UNMATCHED_UNROUTED.issue())
    return issues


def _delivery_conflict_issues(
    consumer_ref: str,
    settings: ConsumerSettings,
) -> list[CompilationIssue]:
    """Return a conflict issue when delivery and enable_auto_commit contradict."""
    if settings.delivery is None or settings.enable_auto_commit is None:
        return []
    expected_auto_commit = settings.delivery == "at_most_once"
    if settings.enable_auto_commit == expected_auto_commit:
        return []
    return [
        StreamingErrorCode.DELIVERY_CONFLICT.issue(
            consumer_ref=consumer_ref,
            delivery=settings.delivery,
            enable_auto_commit=settings.enable_auto_commit,
        )
    ]


def _is_scoped_node_with_process(node: object) -> bool:
    return isinstance(node, WithAsync) or (isinstance(node, With) and node.process is not None)


def _iter_child_node_groups(node: object) -> Iterable[Iterable[object]]:
    if is_branching_node(node):
        for branch in iter_branches(node):
            yield branch.nodes
    elif _is_scoped_node_with_process(node):
        yield cast(Any, node).process.nodes


def _walk_all_process_nodes(nodes: Iterable[object]) -> Iterable[object]:
    for node in nodes:
        yield node
        for child_nodes in _iter_child_node_groups(node):
            yield from _walk_all_process_nodes(child_nodes)


def walk_process_nodes(nodes: Iterable[object]) -> Iterable[object]:
    """Yield every DSL node reachable from *nodes*, recursing into branches.

    Public traversal helper over the process tree: recurses into Router,
    Fork, Broadcast, ExpandRoutes and scoped With/WithAsync processes.
    Used by the compiler phases and by runtime guards that must inspect
    nested nodes.
    """
    yield from _walk_all_process_nodes(nodes)


def _node_needs_async_bridge(node: object) -> bool:
    return isinstance(node, WithAsync) or (
        isinstance(node, IntoTable) and node.backend is Backend.SQLALCHEMY
    )


def _check_input_shape(
    node: object,
    current_shape: StreamShape,
    errors: list[CompilationIssue],
) -> None:
    expected = _node_input_shape(node)
    if expected is not None and current_shape != expected:
        errors.append(
            StreamingErrorCode.SHAPE_MISMATCH.issue(
                expected=expected.value, got=current_shape.value, component=type(node).__name__
            )
        )


def _must_be_last_errors(
    idx: int,
    node_list: tuple[object, ...],
    issue: CompilationIssue,
) -> list[CompilationIssue]:
    if idx != len(node_list) - 1:
        return [issue]
    return []


def _is_scoped_process(node: object) -> bool:
    return isinstance(node, (WithAsync, With))


def _validate_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        _check_input_shape(node, current_shape, errors)
        node_errors, next_shape, should_stop = _validate_shape_node(
            node,
            idx,
            node_list,
            current_shape,
        )
        errors.extend(node_errors)
        current_shape = next_shape
        if should_stop:
            break

    return errors, current_shape


def _iter_unique_storage_sinks(
    nodes: Iterable[object],
    errors: list[CompilationIssue],
) -> Iterable[IntoSink[Any]]:
    """Yield named storage sinks once while recording missing-name errors."""
    seen: set[str] = set()
    for node in _walk_all_process_nodes(nodes):
        if not isinstance(node, IntoSink):
            continue
        if not node.name:
            errors.append(StreamingErrorCode.SINK_MISSING_NAME.issue(component=type(node).__name__))
            continue
        if node.name in seen:
            continue
        seen.add(node.name)
        yield node


def _validate_storage_sink_node(node: IntoSink[Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate backend-specific config for one storage sink."""
    if not isinstance(node, IntoTable):
        return []
    if node.backend is Backend.SQLALCHEMY:
        return _validate_storage_sink_resolution(node, ctx, resolve_sqlalchemy_table_config)
    if node.backend is Backend.DELTA:
        return _validate_storage_sink_resolution(node, ctx, resolve_delta_table_config)
    if node.backend is Backend.CLICKHOUSE:
        return _validate_storage_sink_resolution(node, ctx, resolve_clickhouse_table_config)
    return []


def _validate_storage_sink_resolution(
    node: IntoTable[Any],
    ctx: ConfigContext,
    resolver: Any,
) -> list[CompilationIssue]:
    """Resolve one IntoTable config and return validation errors."""
    try:
        resolver(node, ctx)
    except ValueError as exc:
        component = node.name or type(node).__name__
        return [StreamingErrorCode.SINK_CONFIG_INVALID.issue(component=component, exc=exc)]
    return []


def _validate_shape_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate one node inside a process shape sequence.

    ``Broadcast`` is checked before the leaf terminals, which include it, so
    its branches are validated and its own not-last code is reported.
    """
    if isinstance(node, Broadcast):
        return _validate_broadcast_node(node, idx, node_list, current_shape)
    if _is_leaf_terminal(node):
        return _validate_leaf_terminal_node(node, idx, node_list, current_shape)
    if isinstance(node, Fork):
        return _validate_fork_node(node, idx, node_list, current_shape)
    if _is_scoped_process(node):
        return _validate_scoped_process_node(node, idx, node_list)
    if isinstance(node, Explode):
        return _validate_explode_node(node, idx, node_list, current_shape)
    if isinstance(node, ExpandRoutes):
        return _validate_expand_routes_node(node, idx, node_list)
    if isinstance(node, Router):
        router_errors, next_shape = _validate_router_shapes(node, current_shape)
        if router_ends_every_path(node):
            not_last = StreamingErrorCode.TERMINAL_NOT_LAST.issue(component=type(node).__name__)
            router_errors.extend(_must_be_last_errors(idx, node_list, not_last))
            return router_errors, StreamShape.NONE, True
        return router_errors, next_shape, False
    return [], _node_output_shape(node, current_shape), False


def _validate_leaf_terminal_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a terminal leaf node."""
    issue = StreamingErrorCode.TERMINAL_NOT_LAST.issue(component=type(node).__name__)
    errors = _must_be_last_errors(idx, node_list, issue)
    return errors, current_shape, True


def _validate_fork_node(
    node: Fork[StreamPayload],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a fork node and stop the enclosing process."""
    errors, next_shape = _validate_fork_shapes(node, current_shape)
    errors.extend(_must_be_last_errors(idx, node_list, StreamingErrorCode.FORK_NOT_LAST.issue()))
    return errors, next_shape, True


def _validate_broadcast_node(
    node: Broadcast[Any],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a broadcast node and stop the enclosing process."""
    errors, next_shape = _validate_broadcast_shapes(node, current_shape)
    errors.extend(
        _must_be_last_errors(idx, node_list, StreamingErrorCode.BROADCAST_NOT_LAST.issue())
    )
    return errors, next_shape, True


def _validate_scoped_process_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a scoped process node and stop the enclosing process."""
    issue = StreamingErrorCode.SCOPED_PROCESS_NOT_LAST.issue(component=type(node).__name__)
    errors = _must_be_last_errors(idx, node_list, issue)
    return errors, StreamShape.NONE, True


def _validate_explode_node(
    node: Explode[Any],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate that Explode is followed immediately by Router."""
    next_node = node_list[idx + 1] if idx + 1 < len(node_list) else None
    errors: list[CompilationIssue] = []
    if not isinstance(next_node, Router):
        got = type(next_node).__name__ if next_node is not None else "nothing"
        errors.append(StreamingErrorCode.EXPLODE_WITHOUT_ROUTER.issue(got=got))
    return errors, _node_output_shape(node, current_shape), False


def _prefixed(issue: CompilationIssue, prefix: str) -> CompilationIssue:
    """Scope *issue* under a branch: ``"{prefix}: {message}"``, component ``"{prefix} > ..."``."""
    component = f"{prefix} > {issue.component}" if issue.component else prefix
    return msgspec.structs.replace(issue, message=f"{prefix}: {issue.message}", component=component)


def _validate_expand_routes_node(
    node: ExpandRoutes[Any],
    idx: int,
    node_list: tuple[object, ...],
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Shape-check every route on the rows it receives, and keep ExpandRoutes last.

    Each route receives one message per expanded row, so it starts as a record
    stream. ExpandRoutes is terminal: the dispatcher hands the expanded
    payloads, not rows, to any node that follows it.
    """
    errors: list[CompilationIssue] = []
    for label, nodes in _labelled_branch_nodes(node):
        branch_errors, _ = _validate_shape_sequence(nodes, StreamShape.RECORD)
        errors.extend(_prefixed(issue, f"expand_routes branch {label}") for issue in branch_errors)
    not_last = StreamingErrorCode.TERMINAL_NOT_LAST.issue(component=type(node).__name__)
    errors.extend(_must_be_last_errors(idx, node_list, not_last))
    return errors, StreamShape.NONE, True


def _validate_fork_shapes(
    fork: Fork[StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []

    for label, nodes in _labelled_branch_nodes(fork):
        branch_errors, _ = _validate_shape_sequence(nodes, initial_shape)
        errors.extend(_prefixed(issue, f"fork branch {label}") for issue in branch_errors)
        if not _has_terminal_output(nodes):
            errors.append(StreamingErrorCode.FORK_BRANCH_NO_TERMINAL.issue(label=label))

    return errors, StreamShape.NONE


def _validate_broadcast_shapes(
    broadcast: Broadcast[Any],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    for branch in iter_branches(broadcast):
        branch_errors, _ = _validate_shape_sequence(branch.nodes, initial_shape)
        errors.extend(
            _prefixed(issue, f"broadcast branch {branch.label}") for issue in branch_errors
        )
        last = _path_end(branch.nodes)
        if branch.output is not None and last is not None:
            errors.append(
                StreamingErrorCode.BROADCAST_OUTPUT_AFTER_PATH_END.issue(
                    label=branch.label, node=type(last).__name__
                )
            )
    return errors, StreamShape.NONE


def _validate_router_branch_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        if isinstance(node, BatchStep):
            current_shape = StreamShape.RECORD
            continue
        expected = _node_input_shape(node)
        if expected is not None and current_shape != expected:
            errors.append(
                StreamingErrorCode.SHAPE_MISMATCH.issue(
                    expected=expected.value, got=current_shape.value, component=type(node).__name__
                )
            )
        if _is_leaf_terminal(node) and idx != len(node_list) - 1:
            errors.append(
                StreamingErrorCode.BROADCAST_NOT_LAST.issue()
                if isinstance(node, Broadcast)
                else StreamingErrorCode.TERMINAL_NOT_LAST.issue(component=type(node).__name__)
            )
            break
        current_shape = _node_output_shape(node, current_shape)

    return errors, current_shape


def _validate_router_shapes(
    router: Router[StreamPayload, StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    """Validate *router*'s branches and return the shape of the messages that continue.

    Those are the outputs of the branches that do not end their path and, without
    a default branch, the unmatched input; only they must agree on one shape.
    """
    errors: list[CompilationIssue] = []
    outputs: dict[StreamShape, None] = {} if router.default is not None else {initial_shape: None}

    for branch in iter_branches(router):
        label, nodes = branch.label, branch.nodes
        branch_errors, branch_output = _validate_router_branch_shape_sequence(nodes, initial_shape)
        errors.extend(_prefixed(issue, f"router branch {label}") for issue in branch_errors)
        for node in nodes:
            if not isinstance(node, RouterBranchSafe):
                errors.append(
                    StreamingErrorCode.ROUTER_BRANCH_UNSAFE_NODE.issue(
                        label=label, node=type(node).__name__
                    )
                )
            elif isinstance(node, (ExpandStep, BatchExpandStep)):
                errors.append(
                    StreamingErrorCode.ROUTER_BRANCH_FANOUT_UNSUPPORTED.issue(
                        label=label, node=type(node).__name__
                    )
                )
        if not branch_ends_path(branch):
            outputs[branch_output] = None

    if len(outputs) > 1:
        shapes = {shape.value for shape in outputs}
        errors.append(StreamingErrorCode.ROUTER_BRANCH_SHAPE_DIVERGENCE.issue(shapes=shapes))

    return errors, next(iter(outputs), StreamShape.NONE)


def _labelled_branch_nodes(node: object) -> Iterable[tuple[str, tuple[object, ...]]]:
    """Yield the label and nodes of every branch of *node*, fallback branch last."""
    for branch in iter_branches(node):
        yield branch.label, branch.nodes


def _is_leaf_terminal(node: object) -> bool:
    """Return True for nodes that end a process.

    ``Broadcast`` has branches, but they need not reach an output; the shape
    validators check ``Broadcast`` before this, to validate those branches.
    """
    return isinstance(node, PATH_END_NODE_TYPES)


def _path_end(nodes: tuple[object, ...]) -> object | None:
    """Return the last of *nodes* when it ends the path of every message it receives.

    Nothing runs after such a node, so an output declared after it is never written.
    A Fork ends the path only with a default branch; without one, the messages no
    branch claims go on. A Router ends it only with a default branch and when every
    branch ends in a path-ending node; otherwise some messages continue after it.
    """
    last = nodes[-1] if nodes else None
    if _is_leaf_terminal(last) or isinstance(last, ExpandRoutes):
        return last
    if isinstance(last, Fork) and last.default is not None:
        return last
    if isinstance(last, Router) and router_ends_every_path(last):
        return last
    return None


def _node_has_terminal_output(node: object) -> bool:
    if _is_leaf_terminal(node):
        return True
    if isinstance(node, Router):
        return _router_has_terminal_output(node)
    if isinstance(node, Fork):
        return _fork_has_terminal_output(node)
    if isinstance(node, ExpandRoutes):
        return _expand_routes_has_terminal_output(node)
    if isinstance(node, (WithAsync, With)):
        return _has_terminal_output(node.process.nodes)
    return False


def _has_terminal_output(nodes: Iterable[object]) -> bool:
    return any(_node_has_terminal_output(node) for node in nodes)


def _router_has_terminal_output(router: Router[StreamPayload, StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _labelled_branch_nodes(router))


def _fork_has_terminal_output(fork: Fork[StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _labelled_branch_nodes(fork))


def _expand_routes_has_terminal_output(node: ExpandRoutes[Any]) -> bool:
    return any(_has_terminal_output(branch.nodes) for branch in iter_branches(node))


def _node_has_kafka_topic_output(node: object) -> bool:
    if isinstance(node, IntoTopic):
        return True
    if isinstance(node, Router):
        return any(_has_kafka_topic_output(nodes) for _, nodes in _labelled_branch_nodes(node))
    if isinstance(node, Fork):
        return any(_has_kafka_topic_output(nodes) for _, nodes in _labelled_branch_nodes(node))
    if isinstance(node, Broadcast):
        return any(_has_kafka_topic_output(branch.nodes) for branch in iter_branches(node))
    if isinstance(node, (WithAsync, With)):
        return _has_kafka_topic_output(node.process.nodes)
    return False


def _has_kafka_topic_output(nodes: Iterable[object]) -> bool:
    return any(_node_has_kafka_topic_output(node) for node in nodes)


def _contains_fork(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Fork) for node in nodes)


def _contains_broadcast(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Broadcast) for node in nodes)


def _node_input_shape(node: object) -> StreamShape | None:
    if isinstance(node, RecordStep):
        return StreamShape.RECORD
    if isinstance(node, BatchStep):
        return StreamShape.BATCH
    if isinstance(node, ExpandStep):
        return StreamShape.RECORD
    if isinstance(node, BatchExpandStep):
        return StreamShape.BATCH
    if isinstance(node, Explode):
        return StreamShape.RECORD
    if isinstance(node, (With, WithAsync)):
        return None
    if isinstance(node, ForEach):
        return StreamShape.MANY
    if isinstance(node, Drain):
        return None
    if isinstance(node, Fork):
        return None
    if isinstance(node, Router):
        return StreamShape.RECORD
    return None


def _validate_window_strategies(nodes: Iterable[object]) -> list[CompilationIssue]:
    errors: list[CompilationIssue] = []
    for node in nodes:
        if isinstance(node, CollectBatch) and node.window is not WindowStrategy.COLLECT:
            errors.append(StreamingErrorCode.WINDOW_STRATEGY_UNSUPPORTED.issue(window=node.window))
    return errors


def _validate_scoped_process_nodes(nodes: Iterable[object]) -> list[CompilationIssue]:
    errors: list[CompilationIssue] = []
    for node in nodes:
        if not isinstance(node, (With, WithAsync)):
            continue
        inner_nodes = tuple(node.process.nodes)
        for idx, inner_node in enumerate(inner_nodes):
            if isinstance(inner_node, RecordStep):
                continue
            if isinstance(inner_node, IntoTopic):
                if idx != len(inner_nodes) - 1:
                    errors.append(
                        StreamingErrorCode.SCOPED_INTO_TOPIC_NOT_LAST.issue(
                            component=type(node).__name__,
                            following=type(inner_nodes[idx + 1]).__name__,
                        )
                    )
                continue
            errors.append(
                StreamingErrorCode.SCOPED_PROCESS_UNSUPPORTED_NODE.issue(
                    component=type(node).__name__, inner_node=type(inner_node).__name__
                )
            )
    return errors


_FIXED_OUTPUT_SHAPES: dict[type, StreamShape] = {
    CollectBatch: StreamShape.BATCH,
    Explode: StreamShape.RECORD,
    ForEach: StreamShape.RECORD,
    RecordStep: StreamShape.RECORD,
    BatchStep: StreamShape.BATCH,
    ExpandStep: StreamShape.RECORD,
    BatchExpandStep: StreamShape.RECORD,
    WithAsync: StreamShape.NONE,
    Drain: StreamShape.NONE,
    Fork: StreamShape.NONE,
    Broadcast: StreamShape.NONE,
}


def _node_output_shape(node: object, current: StreamShape) -> StreamShape:
    fixed = _FIXED_OUTPUT_SHAPES.get(type(node))
    if fixed is not None:
        return fixed
    if isinstance(node, IntoSink):
        return StreamShape.NONE
    if isinstance(node, IntoTopic):
        return node.shape
    if isinstance(node, With):
        return StreamShape.NONE if node.process is not None else StreamShape.MANY
    return current
