"""Plan-building phase for streaming flow compilation."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from types import MappingProxyType
from typing import Any, Literal, get_args, get_origin

from loom.core.config import ConfigContext
from loom.core.config.keys import ConfigKey
from loom.streaming.compiler._errors import StreamingErrorCode
from loom.streaming.compiler._plan import (
    CompilationError,
    CompiledMongoCDCSource,
    CompiledMultiSource,
    CompiledNode,
    CompiledPlan,
    CompiledSingleSource,
    CompiledSink,
    CompiledSource,
    CompiledStorageSink,
)
from loom.streaming.compiler.phases.validate import (
    _node_needs_async_bridge,
    _walk_all_process_nodes,
    node_output_shape,
)
from loom.streaming.core._errors import ErrorEnvelope
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import KafkaSettings
from loom.streaming.kafka._wire import DecodeError, DispatchTable
from loom.streaming.mongo import MongoConfig
from loom.streaming.nodes._boundary import FromMultiTypeTopic, FromTopic, IntoTopic
from loom.streaming.nodes._branches import is_branching_node, iter_branches
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._shape import CollectBatch, StreamShape
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._table import Backend, IntoTable
from loom.streaming.nodes._table.config import (
    resolve_clickhouse_table_config,
    resolve_delta_table_config,
    resolve_sqlalchemy_table_config,
)
from loom.streaming.nodes._with import With, WithAsync


def build_plan(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledPlan:
    """Build a compiled plan from a validated flow and runtime config context.

    Args:
        flow: Validated stream flow.
        ctx: Runtime config context providing Kafka settings and section access.

    Returns:
        Immutable compiled plan ready for adapter wiring.
    """
    source = _build_source(flow, ctx)
    nodes = _build_nodes(flow)
    output = _build_sink(flow.output, ctx) if flow.output else None
    terminal_sinks, terminal_storage_sinks = _build_terminal_sinks(flow.process.nodes, ctx)
    error_routes = {kind: _build_sink(topic, ctx) for kind, topic in flow.errors.items()}
    needs_async = any(
        _node_needs_async_bridge(node) for node in _walk_all_process_nodes(flow.process.nodes)
    )
    return CompiledPlan(
        name=flow.name,
        source=source,
        nodes=tuple(nodes),
        output=output,
        terminal_sinks=terminal_sinks,
        terminal_storage_sinks=terminal_storage_sinks,
        error_routes=error_routes,
        needs_async_bridge=needs_async,
    )


def _build_mongo_source(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledMongoCDCSource:
    source = flow.source
    if not isinstance(source, FromMongoCDC):
        raise TypeError(f"Expected FromMongoCDC source, got {type(source).__name__}.")
    mongo = ctx.section(ConfigKey.MONGO, MongoConfig)
    settings = mongo.source_for(source.logical_ref)
    watch_options = {**settings.watch_options, **source.watch_options}
    return CompiledMongoCDCSource(
        settings=settings,
        collections=source.collections,
        watch_options=watch_options,
    )


def _build_single_source(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledSingleSource:
    source: FromTopic[Any] = flow.source  # type: ignore[assignment]
    kafka = ctx.section(ConfigKey.KAFKA, KafkaSettings)
    consumer = kafka.consumer_for(source.logical_ref)
    decode_strategy: Literal["record", "batch"] = (
        "batch" if any(isinstance(n, CollectBatch) for n in flow.process.nodes) else "record"
    )
    return CompiledSingleSource(
        settings=consumer,
        topics=consumer.topics,
        payload_type=source.payload,
        decode_strategy=decode_strategy,
    )


def _build_multi_source(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledMultiSource:
    source: FromMultiTypeTopic[Any] = flow.source  # type: ignore[assignment]
    kafka = ctx.section(ConfigKey.KAFKA, KafkaSettings)
    consumer = kafka.consumer_for(source.logical_ref)
    decode_strategy: Literal["record", "batch"] = (
        "batch" if any(isinstance(n, CollectBatch) for n in flow.process.nodes) else "record"
    )
    dispatch = _build_dispatch_table(source.payloads)
    return CompiledMultiSource(
        settings=consumer,
        topics=consumer.topics,
        dispatch=dispatch,
        decode_strategy=decode_strategy,
    )


_SOURCE_BUILDERS: MappingProxyType[
    type, Callable[[StreamFlow[Any, Any], ConfigContext], CompiledSource]
] = MappingProxyType(
    {
        FromMongoCDC: _build_mongo_source,
        FromMultiTypeTopic: _build_multi_source,
    }
)


def _build_source(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> CompiledSource:
    builder = _SOURCE_BUILDERS.get(type(flow.source))
    if builder is not None:
        return builder(flow, ctx)
    return _build_single_source(flow, ctx)


def _build_dispatch_table(payloads: tuple[type[Any], ...]) -> DispatchTable:
    plain: dict[str, Any] = {}
    error: dict[str, Any] = {}
    wire: dict[str, Any] = {}
    for t in payloads:
        if isinstance(t, type) and issubclass(t, DecodeError):
            wire[t.loom_message_type()] = t
            continue
        if t is ErrorEnvelope:
            issue = StreamingErrorCode.ERROR_ENVELOPE_UNPARAMETERIZED.issue(payload=repr(t))
            raise CompilationError([issue])
        if get_origin(t) is ErrorEnvelope:
            key = _require_message_type(get_args(t)[0])
            error[key] = t
        else:
            key = _require_message_type(t)
            plain[key] = t
    return DispatchTable(plain=plain, error=error, wire=wire)


def _require_message_type(t: type[Any]) -> str:
    try:
        return str(t.loom_message_type())
    except AttributeError:
        issue = StreamingErrorCode.PAYLOAD_TYPE_INVALID.issue(payload=repr(t))
        raise CompilationError([issue]) from None


def _build_nodes(flow: StreamFlow[Any, Any]) -> list[CompiledNode]:
    nodes: list[CompiledNode] = []
    current_shape = StreamShape.RECORD
    for node in flow.process.nodes:
        input_shape = current_shape
        output_shape = node_output_shape(node, current_shape)
        nodes.append(
            CompiledNode(
                node=node,
                input_shape=input_shape,
                output_shape=output_shape,
                path=(len(nodes),),
            )
        )
        current_shape = output_shape
    return nodes


_TerminalSinks = tuple[
    dict[tuple[int, ...], CompiledSink], dict[tuple[int, ...], CompiledStorageSink]
]


def _build_terminal_sinks(
    nodes: Iterable[object],
    ctx: ConfigContext,
    *,
    path_prefix: tuple[int, ...] = (),
) -> _TerminalSinks:
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    storage_sinks: dict[tuple[int, ...], CompiledStorageSink] = {}
    for idx, node in enumerate(tuple(nodes)):
        path = path_prefix + (idx,)
        if isinstance(node, IntoTopic):
            sinks[path] = _build_sink(node, ctx)
            continue
        if isinstance(node, IntoSink):
            storage_sinks[path] = _build_storage_sink(node, ctx)
            continue
        if is_branching_node(node):
            sub_sinks, sub_storage = _build_branch_terminal_sinks(node, ctx, path_prefix=path)
            sinks.update(sub_sinks)
            storage_sinks.update(sub_storage)
            continue
        if isinstance(node, WithAsync) or (isinstance(node, With) and node.process is not None):
            sub_sinks, sub_storage = _build_terminal_sinks(
                node.process.nodes, ctx, path_prefix=path
            )
            sinks.update(sub_sinks)
            storage_sinks.update(sub_storage)
    return sinks, storage_sinks


def _build_branch_terminal_sinks(
    node: object,
    ctx: ConfigContext,
    *,
    path_prefix: tuple[int, ...],
) -> _TerminalSinks:
    """Build the sinks of every branch of one branching node, keyed by branch path."""
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    storage_sinks: dict[tuple[int, ...], CompiledStorageSink] = {}
    for branch in iter_branches(node):
        branch_path = path_prefix + (branch.index,)
        if branch.output is not None:
            sinks[branch_path] = _build_sink(branch.output, ctx)
        sub_sinks, sub_storage = _build_terminal_sinks(branch.nodes, ctx, path_prefix=branch_path)
        sinks.update(sub_sinks)
        storage_sinks.update(sub_storage)
    return sinks, storage_sinks


def _build_sink(topic: IntoTopic[Any], ctx: ConfigContext) -> CompiledSink:
    kafka = ctx.section(ConfigKey.KAFKA, KafkaSettings)
    producer = kafka.producer_for(topic.logical_ref)
    return CompiledSink(
        settings=producer,
        topic=producer.topic or str(topic.logical_ref),
        partition_policy=topic.partitioning,
        dlq_topic=topic.dlq,
    )


def _build_storage_sink(node: IntoSink[Any], ctx: ConfigContext) -> CompiledStorageSink:
    if isinstance(node, IntoTable):
        if node.backend is Backend.SQLALCHEMY:
            sql_resolved = resolve_sqlalchemy_table_config(node, ctx)
            return CompiledStorageSink(
                node=node,
                config=sql_resolved.sink,
                database_config=sql_resolved.database,
            )
        if node.backend is Backend.DELTA:
            delta_resolved = resolve_delta_table_config(node, ctx)
            return CompiledStorageSink(
                node=node,
                config=delta_resolved.sink,
                database_config=None,
            )
        if node.backend is Backend.CLICKHOUSE:
            ch_resolved = resolve_clickhouse_table_config(node, ctx)
            return CompiledStorageSink(
                node=node,
                config=ch_resolved.sink,
                database_config=None,
            )
    raise CompilationError(
        [StreamingErrorCode.STORAGE_SINK_UNSUPPORTED.issue(component=type(node).__name__)]
    )
