"""Streaming compilation error codes and structured issues.

``StreamingErrorCode``
    Machine-readable catalogue of every validation failure the streaming
    compiler can report. Each member carries the template of its issue, and
    ``StreamingErrorCode.X.issue(...)`` builds it.

``CompilationIssue``
    :class:`~loom.core.errors.codes.CompileIssue` over this catalogue: the
    ``code``, ``message``, ``component`` and ``field`` of one failure.

Unlike ETL — which raises one exception per failure — the streaming compiler
accumulates every issue found in a run, so the issue is data and
:class:`~loom.streaming.compiler.CompilationError` aggregates the collection.
"""

from __future__ import annotations

from enum import unique
from typing import Final, TypeAlias

from loom.core.errors.codes import CompileIssue, IssueCode, Template

_FORK: Final = "fork"
_OUTPUT: Final = "output"


@unique
class StreamingErrorCode(IssueCode):
    """Catalogue of every streaming compiler failure and the issue it renders."""

    # Binding resolution phase
    STEP_INSTANTIATION_FAILED = "STEP_INSTANTIATION_FAILED", Template("step {component}: {exc}")
    BINDING_RESOLUTION_FAILED = "BINDING_RESOLUTION_FAILED", Template("binding {component}: {exc}")
    BINDING_NOT_PROCESS_NODE = (
        "BINDING_NOT_PROCESS_NODE",
        Template("binding {component}: resolved object is not a process node"),
    )
    FORK_SELECTOR_MISSING = (
        "FORK_SELECTOR_MISSING",
        Template("fork selector missing for keyed fork", component=_FORK, field="selector"),
    )

    # Validation phase
    SINK_MISSING_NAME = (
        "SINK_MISSING_NAME",
        Template("storage sink '{component}': missing name", field="name"),
    )
    SINK_CONFIG_INVALID = (
        "SINK_CONFIG_INVALID",
        Template("storage sink '{component}': {exc}", field="streaming.sinks.{component}"),
    )
    KAFKA_CONFIG_INVALID = (
        "KAFKA_CONFIG_INVALID",
        Template("kafka: {exc}", component="kafka", field="kafka"),
    )
    MONGO_CONFIG_INVALID = (
        "MONGO_CONFIG_INVALID",
        Template(
            "mongo source '{source_name}': {exc}",
            component="mongo source '{source_name}'",
            field="mongo",
        ),
    )
    BATCH_SCOPE_DIRECT_CONTEXT = (
        "BATCH_SCOPE_DIRECT_CONTEXT",
        Template(
            "{component} with scope=BATCH cannot use direct context manager instances: "
            "{direct_cms}. Use ContextFactory for batch-scoped resources."
        ),
    )
    SHAPE_MISMATCH = (
        "SHAPE_MISMATCH",
        Template("shape mismatch: expected {expected} but got {got} before {component}"),
    )
    TERMINAL_NOT_LAST = (
        "TERMINAL_NOT_LAST",
        Template("{component} must be the last node in a process"),
    )
    FORK_NOT_LAST = (
        "FORK_NOT_LAST",
        Template("fork must be the last node in a process", component=_FORK),
    )
    BROADCAST_NOT_LAST = (
        "BROADCAST_NOT_LAST",
        Template("broadcast must be the last node in a process", component="broadcast"),
    )
    SCOPED_PROCESS_NOT_LAST = (
        "SCOPED_PROCESS_NOT_LAST",
        Template("{component}(process=...) must be the last node in a process"),
    )
    EXPLODE_WITHOUT_ROUTER = (
        "EXPLODE_WITHOUT_ROUTER",
        Template(
            "Explode must be immediately followed by a Router; got {got}", component="Explode"
        ),
    )
    FORK_BRANCH_NO_TERMINAL = (
        "FORK_BRANCH_NO_TERMINAL",
        Template("fork branch {label}: no terminal output found", component="fork branch {label}"),
    )
    ROUTER_BRANCH_UNSAFE_NODE = (
        "ROUTER_BRANCH_UNSAFE_NODE",
        Template(
            "router branch {label}: node {node} is not router-branch safe",
            component="router branch {label}",
        ),
    )
    ROUTER_BRANCH_FANOUT_UNSUPPORTED = (
        "ROUTER_BRANCH_FANOUT_UNSUPPORTED",
        Template(
            "router branch {label}: {node} is not supported in Router branches — Router is "
            "1-to-1; use Fork for fan-out.",
            component="router branch {label}",
        ),
    )
    ROUTER_BRANCH_SHAPE_DIVERGENCE = (
        "ROUTER_BRANCH_SHAPE_DIVERGENCE",
        Template("router branches produce different shapes: {shapes}", component="router"),
    )
    OUTPUT_WITH_FORK = (
        "OUTPUT_WITH_FORK",
        Template(
            "flow.output cannot be combined with Fork: branches must be terminal",
            component="flow.output",
            field=_OUTPUT,
        ),
    )
    OUTPUT_WITH_BROADCAST = (
        "OUTPUT_WITH_BROADCAST",
        Template(
            "flow.output cannot be combined with Broadcast: branches must be terminal",
            component="flow.output",
            field=_OUTPUT,
        ),
    )
    OUTPUT_AFTER_PATH_END = (
        "OUTPUT_AFTER_PATH_END",
        Template(
            "flow.output is never written: the process ends in {node}, which ends the path "
            "of every message it receives. Remove the output or the {node}.",
            component="flow.output",
            field=_OUTPUT,
        ),
    )
    BROADCAST_OUTPUT_AFTER_PATH_END = (
        "BROADCAST_OUTPUT_AFTER_PATH_END",
        Template(
            "broadcast branch {label}: its output is never written: the route's process "
            "ends in {node}, which ends the path of every message it receives. Remove the "
            "output or the {node}.",
            component="broadcast branch {label}",
            field=_OUTPUT,
        ),
    )
    MISSING_TERMINAL_OUTPUT = (
        "MISSING_TERMINAL_OUTPUT",
        Template(
            "no terminal output found: add IntoTopic, a storage sink node, or flow.output",
            component="flow",
            field=_OUTPUT,
        ),
    )
    WINDOW_STRATEGY_UNSUPPORTED = (
        "WINDOW_STRATEGY_UNSUPPORTED",
        Template(
            "CollectBatch.window={window} is not yet supported by the Bytewax adapter. "
            "Only WindowStrategy.COLLECT is available in this adapter version.",
            component="CollectBatch",
            field="window",
        ),
    )
    SCOPED_INTO_TOPIC_NOT_LAST = (
        "SCOPED_INTO_TOPIC_NOT_LAST",
        Template(
            "{component}(process=...) requires IntoTopic to be last; found {following} after it."
        ),
    )
    SCOPED_PROCESS_UNSUPPORTED_NODE = (
        "SCOPED_PROCESS_UNSUPPORTED_NODE",
        Template(
            "{component}(process=...) only supports RecordStep nodes and an optional terminal "
            "IntoTopic; found {inner_node}."
        ),
    )

    # Plan-building phase
    ERROR_ENVELOPE_UNPARAMETERIZED = (
        "ERROR_ENVELOPE_UNPARAMETERIZED",
        Template(
            "ErrorEnvelope in FromMultiTypeTopic must be parameterized, "
            "e.g. ErrorEnvelope[OrderEvent]. Got: {payload}",
            component="FromMultiTypeTopic",
            field="payloads",
        ),
    )
    STORAGE_SINK_UNSUPPORTED = (
        "STORAGE_SINK_UNSUPPORTED",
        Template("Unsupported storage sink: {component}"),
    )
    PAYLOAD_TYPE_INVALID = (
        "PAYLOAD_TYPE_INVALID",
        Template(
            "FromMultiTypeTopic payload {payload} does not define loom_message_type(); "
            "payloads must be Loom struct types",
            component="FromMultiTypeTopic",
            field="payloads",
        ),
    )

    # Delivery-semantics phase (partitioned source spec)
    DELIVERY_CONFLICT = (
        "DELIVERY_CONFLICT",
        Template(
            "kafka consumer '{consumer_ref}': delivery={delivery} conflicts with "
            "enable_auto_commit={enable_auto_commit}; remove the deprecated enable_auto_commit",
            component="kafka consumer '{consumer_ref}'",
            field="kafka.consumer.enable_auto_commit",
        ),
    )
    DELIVERY_KEYED_MULTIPROCESS = (
        "DELIVERY_KEYED_MULTIPROCESS",
        Template(
            "delivery=at_least_once with {component} is not supported on a multi-process "
            "Bytewax cluster: keyed operators route records to other processes, where "
            "completions cannot reach the source process's commit tracker. Scale with "
            "workers_per_process (threads) or use Bytewax recovery instead.",
            field="streaming.runtime.addresses",
        ),
    )
    SINK_CANNOT_TRACK_COMMITS = (
        "SINK_CANNOT_TRACK_COMMITS",
        Template(
            "delivery=at_least_once requires every runtime sink to accept the commit tracker, "
            "but {component} exposes no bind_commit_tracker method. Records written by it "
            "would never be completed, so its partitions would stop committing while the "
            "flow looks healthy. Add bind_commit_tracker(tracker) to the sink, or declare "
            "delivery=at_most_once.",
            field="streaming.delivery",
        ),
    )
    INLINE_SINK_CANNOT_SKIP_COMMITS = (
        "INLINE_SINK_CANNOT_SKIP_COMMITS",
        Template(
            "delivery=at_least_once: {component} is written inline by "
            "With/WithAsync(process=...) but exposes no build_inline_partition method, so "
            "its partitions would complete the offsets of messages that go on after the "
            "inline write. Add build_inline_partition() to the sink, or declare "
            "delivery=at_most_once.",
            field="streaming.delivery",
        ),
    )
    # Under at-least-once delivery no message is discarded unless the flow says so.
    TERMINAL_SINK_MISSING = (
        "TERMINAL_SINK_MISSING",
        Template(
            "no runtime sink for the IntoTopic compiled at path {path} (topic {topic}): a "
            "terminal_sinks override must cover every compiled IntoTopic path, or its "
            "messages would complete without being written.",
            component="IntoTopic",
        ),
    )
    FORK_UNMATCHED_UNROUTED = (
        "FORK_UNMATCHED_UNROUTED",
        Template(
            "fork has no default branch: under delivery=at_least_once a message no branch "
            "claims would be committed without being written anywhere. Add a default route "
            "that writes it, or default=Process(Drain()) to discard it explicitly.",
            component=_FORK,
            field="default",
        ),
    )

    # Issues built from bare strings
    UNSPECIFIED = "UNSPECIFIED", Template("{message}", component="")


CompilationIssue: TypeAlias = CompileIssue[StreamingErrorCode]
