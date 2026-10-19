"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, node dispatch, encode, and output routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

import bytewax.dataflow as _bytewax_dataflow
from bytewax.operators import branch
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map
from bytewax.operators import output as bw_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.core.async_bridge import AsyncBridge
from loom.core.logger import get_logger
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.tracing.context import generate_trace_id
from loom.streaming.bytewax._commit_tracker import CommitCompletionPort
from loom.streaming.bytewax._errors import RuntimeConfigurationError
from loom.streaming.bytewax._resource_manager import ResourceManager
from loom.streaming.bytewax._runtime_io import build_runtime_terminal_sinks, drop_item_to_commit
from loom.streaming.bytewax.handlers.dispatcher import (
    _NODE_HANDLERS,
    _wire_process,
)
from loom.streaming.compiler import CompiledPlan
from loom.streaming.compiler._errors import StreamingErrorCode
from loom.streaming.compiler._plan import CompiledMultiSource, CompiledSingleSource
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import (
    DecodeOk,
    DecodeResult,
    try_decode_multi_record,
    try_decode_record,
)
from loom.streaming.nodes._with import With, WithAsync

Stream: TypeAlias = Any
logger = get_logger(__name__)

__all__ = ["build_dataflow", "build_dataflow_with_shutdown", "_NODE_HANDLERS"]


@dataclass(frozen=True)
class _BuiltDataflow:
    """Bytewax dataflow plus adapter-owned shutdown callback."""

    dataflow: Any
    shutdown: Callable[[], None]


class _DropSinkPartition(StatelessSinkPartition[Any]):
    """Discard items routed to an unrouted error branch.

    Discarding still completes each record with the commit tracker: a dropped
    message that never completes would freeze its partition's watermark under
    at-least-once delivery.
    """

    def __init__(self, commit_tracker: CommitCompletionPort | None = None) -> None:
        self._commit_tracker = commit_tracker

    def write_batch(self, items: list[Any]) -> None:
        if self._commit_tracker is None:
            return
        for item in items:
            topic, partition, offset = drop_item_to_commit(item)
            if topic is not None and partition is not None and offset is not None:
                self._commit_tracker.complete(topic, partition, offset)


class _DropSink(DynamicSink[Any]):
    """Build a tracker-aware discard sink for unrouted error branches."""

    def __init__(self, commit_tracker: CommitCompletionPort | None = None) -> None:
        self._commit_tracker = commit_tracker

    def bind_commit_tracker(self, tracker: CommitCompletionPort) -> None:
        """Bind a commit tracker (adapter duck-typed hook)."""
        self._commit_tracker = tracker

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[Any]:
        del step_id, worker_index, worker_count
        return _DropSinkPartition(self._commit_tracker)


def build_dataflow(
    plan: CompiledPlan,
    *,
    observability_runtime: ObservabilityRuntime | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
) -> Any:
    """Build a Bytewax Dataflow from a compiled plan."""
    return build_dataflow_with_shutdown(
        plan,
        observability_runtime=observability_runtime,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
    ).dataflow


def build_dataflow_with_shutdown(
    plan: CompiledPlan,
    *,
    observability_runtime: ObservabilityRuntime | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
    bridge: AsyncBridge | None = None,
    commit_tracker: CommitCompletionPort | None = None,
) -> _BuiltDataflow:
    """Build a Bytewax Dataflow and expose its shutdown callback.

    Args:
        plan: Compiled flow plan.
        observability_runtime: Optional observability runtime for lifecycle events.
        source: Optional Bytewax source override (used in tests).
        sink: Optional Bytewax sink override (used in tests).
        terminal_sinks: Optional per-branch sink overrides.
        error_sinks: Optional per-kind error sink overrides.
        bridge: Pre-configured :class:`AsyncBridge`.  When ``None``, a default
            asyncio bridge is created if the plan requires async execution.
            Pass an explicit bridge to control backend and uvloop settings.
    """
    if terminal_sinks is None:
        terminal_sinks = build_runtime_terminal_sinks(plan.terminal_sinks, commit_tracker)
    resolved_runtime = observability_runtime or ObservabilityRuntime.noop()
    _bind_commit_tracker_object(source, commit_tracker)
    _bind_commit_tracker_object(sink, commit_tracker)
    _bind_commit_tracker_mapping(terminal_sinks, commit_tracker)
    _bind_commit_tracker_mapping(error_sinks, commit_tracker)
    resolved_bridge = bridge if bridge is not None else _maybe_create_bridge(plan)
    ctx = _BuildContext(
        plan=plan,
        bridge=resolved_bridge,
        flow_runtime=resolved_runtime,
        flow_run_id=generate_trace_id(),
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
        commit_tracker=commit_tracker,
    )
    return _BuiltDataflow(dataflow=_assemble_dataflow(plan, ctx), shutdown=ctx.shutdown_all)


@runtime_checkable
class _SupportsCommitBind(Protocol):
    """Runtime object that accepts a Kafka commit tracker."""

    def bind_commit_tracker(self, tracker: CommitCompletionPort) -> None:
        """Bind a commit tracker to this runtime object."""


@runtime_checkable
class _SupportsTerminalTracing(Protocol):
    """Runtime sink that can record the death of the messages it writes."""

    def bind_terminal_tracing(
        self, observer: ObservabilityRuntime, flow: str, flow_run_id: str
    ) -> None:
        """Make this sink emit a terminal span per message it writes."""

    def mark_inline_partition(self) -> None:
        """Record that an inline, non-terminal partition is built from this sink."""


def _mark_inline_partition(sink: object) -> None:
    """Record on one sink that an inline, non-terminal partition is built from it.

    Sinks that cannot carry terminal tracing — storage sinks, test doubles —
    have nothing to record and are left untouched.
    """
    if isinstance(sink, _SupportsTerminalTracing):
        sink.mark_inline_partition()


def _bind_commit_tracker_object(
    item: object | None, commit_tracker: CommitCompletionPort | None
) -> None:
    """Bind a commit tracker to one runtime object, or refuse to run.

    Under at-least-once delivery a sink that cannot receive the tracker never
    completes the records it writes, so its partitions stop committing and the
    consumer lag grows without bound while the flow looks healthy. Skipping the
    binding silently — the previous behaviour — made that the default outcome
    for any sink a user registered themselves.

    Raises:
        RuntimeConfigurationError: If a tracker is required and this runtime
            object cannot accept one.
    """
    if item is None or commit_tracker is None:
        return
    if not isinstance(item, _SupportsCommitBind):
        issue = StreamingErrorCode.SINK_CANNOT_TRACK_COMMITS.issue(component=type(item).__name__)
        raise RuntimeConfigurationError([issue])
    item.bind_commit_tracker(commit_tracker)


def _bind_commit_tracker_mapping(
    items: Mapping[Any, Any] | None,
    commit_tracker: CommitCompletionPort | None,
) -> None:
    """Bind a commit tracker to each runtime object in a mapping when supported."""
    if items is None or commit_tracker is None:
        return
    for item in items.values():
        _bind_commit_tracker_object(item, commit_tracker)


def _assemble_dataflow(plan: CompiledPlan, ctx: _BuildContext) -> Any:
    """Assemble a Bytewax Dataflow from a pre-built context."""
    flow = _bytewax_dataflow.Dataflow(plan.name)
    stream = _build_source_pipeline(flow, ctx)
    stream = _wire_process(stream, tuple(node.node for node in plan.nodes), ctx)

    _wire_output(stream, ctx)
    return flow


class _BuildContext:
    """Wiring-phase state shared across operator builders."""

    __slots__ = (
        "plan",
        "bridge",
        "commit_tracker",
        "flow_runtime",
        "flow_run_id",
        "source",
        "sink",
        "error_sinks",
        "terminal_sinks",
        "resource_manager",
        "_path",
    )

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_runtime: ObservabilityRuntime,
        flow_run_id: str,
        source: Any | None = None,
        sink: Any | None = None,
        terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
        error_sinks: Mapping[ErrorKind, Any] | None = None,
        commit_tracker: CommitCompletionPort | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.commit_tracker = commit_tracker
        self.flow_runtime = flow_runtime
        self.flow_run_id = flow_run_id
        self.source = source
        self.sink = sink
        self.terminal_sinks = terminal_sinks or {}
        self.error_sinks = error_sinks or {}
        self.resource_manager = ResourceManager(bridge)
        self._path: tuple[int, ...] = ()

    def wire_terminal(self, step_id: str, stream: Any) -> None:
        if self.sink is None:
            raise RuntimeError("Bytewax sink is required for terminal output wiring.")
        self._bind_terminal_tracing(self.sink)
        bw_output(step_id, stream, self.sink)

    def wire_branch_terminal(self, step_id: str, stream: Any, path: tuple[int, ...]) -> None:
        sink = self.terminal_sinks.get(path)
        if sink is not None:
            self._bind_terminal_tracing(sink)
            bw_output(_qualified_step_id(step_id, path), stream, sink)
            return
        # A branch without a terminal still owes its record a completion: the
        # broadcast that created it already forked the offset once per branch.
        # Discarding the stream silently left that fork outstanding forever and
        # froze the partition's watermark. Unrouted error branches already fall
        # back to a drop sink for exactly this reason; branches now do too.
        logger.warning(
            "unrouted_branch_drop_sink",
            flow=self.plan.name,
            step_id=step_id,
            path=path,
        )
        bw_output(
            _qualified_step_id(f"{step_id}_dropped", path),
            stream,
            _DropSink(self.commit_tracker),
        )

    def _bind_terminal_tracing(self, sink: Any) -> None:
        """Give one real terminal sink the runtime its death spans open on.

        Called only from the two wiring sites that run for a genuine terminal.
        ``_BuildContext.inline_sink_partition_for`` builds from the *same*
        ``terminal_sinks`` mapping for ``WithAsync(process=[..., IntoTopic])``,
        where the message explicitly does not die; binding any earlier would
        emit a death span for a message that goes on living.

        Sinks that cannot record a death — storage sinks, drop sinks, test
        doubles — are left untouched.
        """
        if isinstance(sink, _SupportsTerminalTracing):
            sink.bind_terminal_tracing(self.flow_runtime, self.plan.name, self.flow_run_id)

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Any) -> None:
        sink = self.error_sinks.get(kind)
        if sink is not None:
            bw_output(f"{step_id}_{kind.value}_errors", stream, sink)
            return
        logger.warning(
            "unrouted_error_drop_sink",
            flow=self.plan.name,
            kind=kind.value,
            step_id=step_id,
        )
        bw_output(f"{step_id}_{kind.value}_dropped", stream, _DropSink(self.commit_tracker))

    def wire_flow_output(self, stream: Any, plan: CompiledPlan) -> None:
        if self.sink is None and plan.output is not None:
            raise RuntimeError("Bytewax sink is required for terminal output wiring.")
        if self.sink is not None:
            self.wire_terminal("output", stream)
        for kind in plan.error_routes:
            if kind not in self.error_sinks:
                raise RuntimeError(f"Bytewax sink is required for error route {kind.value}.")

    def wire_decode_error(self, stream: Any, plan: CompiledPlan) -> None:
        del plan
        if self.error_sinks.get(ErrorKind.WIRE) is not None:
            self.wire_node_error(ErrorKind.WIRE, "decode", stream)
            return
        if ErrorKind.WIRE in self.plan.error_routes:
            raise RuntimeError("Bytewax sink is required for WIRE error routing.")

    def inline_sink_partition_for(
        self,
        path: tuple[int, ...],
    ) -> StatelessSinkPartition[Any] | None:
        """Build an inline sink partition for the given path.

        Delegates to the runtime-wired Bytewax ``Sink`` for the path so that
        test doubles (e.g. ``TestingSink``) are honoured instead of always
        creating real Kafka producers.

        Args:
            path: Compiled path identifying the terminal sink.

        Returns:
            A ready-to-write ``StatelessSinkPartition``, or ``None`` if no
            sink is registered for *path*.

        Raises:
            RuntimeError: If the sink for *path* already carries terminal
                tracing, meaning an inline and a terminal wiring path collided
                on one compiled sink.
        """
        sink = self.terminal_sinks.get(path)
        if sink is None:
            return None
        _mark_inline_partition(sink)
        step_id = "inline_" + "_".join(str(p) for p in path)
        return cast(StatelessSinkPartition[Any], sink.build(step_id, 0, 1))

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> Any:
        """Get or create a resource manager for *node* at position *idx*."""
        return self.resource_manager.manager_for(idx, node)

    def session_manager_for(self, config: Any) -> Any:
        """Get or create a shared SQLAlchemy session manager for one config."""
        return self.resource_manager.session_manager_for(config)

    @property
    def current_path(self) -> tuple[int, ...]:
        """Return the current wiring path inside the process tree."""
        return self._path

    @contextmanager
    def enter_path(self, path: tuple[int, ...]) -> Iterator[None]:
        """Temporarily set the current wiring path."""
        previous = self._path
        self._path = path
        try:
            yield
        finally:
            self._path = previous

    def wire_process(
        self,
        stream: Any,
        nodes: tuple[object, ...],
        *,
        path_prefix: tuple[int, ...] = (),
    ) -> Any:
        """Wire one nested process subtree."""
        return _wire_process(stream, nodes, self, path_prefix=path_prefix)

    def shutdown_all(self) -> None:
        """Shutdown all resource managers."""
        self.resource_manager.shutdown_all()


def _build_source_pipeline(flow: Any, ctx: _BuildContext) -> Stream:
    """Build the source-side pipeline up to the first decoded Message stream."""
    if ctx.source is None:
        raise RuntimeError("Bytewax source is required to build a runtime dataflow.")
    source = ctx.source
    if not ctx.plan.source.needs_decode:
        return bw_input("source", flow, source)
    codec: MsgspecCodec[Any] = MsgspecCodec()
    if not isinstance(ctx.plan.source, (CompiledSingleSource, CompiledMultiSource)):
        raise TypeError(f"Expected a Kafka compiled source, got {type(ctx.plan.source).__name__}.")
    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"

    stream: Stream = bw_input("source", flow, source)
    decoded = _decode_source_stream(stream, ctx, codec, step_id)
    decoded_branch = _split_decode_results(decoded, step_id)
    ctx.wire_decode_error(decoded_branch.falses, ctx.plan)
    return bw_map(f"{step_id}_message", decoded_branch.trues, _decode_ok_message)


def _decode_source_stream(
    stream: Stream,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
    step_id: str,
) -> Stream:
    """Map raw source items into decode results without raising wire errors."""
    return bw_map(step_id, stream, lambda item: _decode_source_record(item, ctx, codec))


def _split_decode_results(stream: Stream, step_id: str) -> Any:
    """Split decode results into successful messages and wire errors."""
    return branch(f"{step_id}_is_ok", stream, _is_decode_ok)


def _wire_output(stream: Any, ctx: _BuildContext) -> None:
    """Wire the output sink and error routes."""
    ctx.wire_flow_output(stream, ctx.plan)


def _qualified_step_id(step_id: str, path: tuple[int, ...]) -> str:
    if not path:
        return step_id
    return "_".join((step_id, *map(str, path)))


def _maybe_create_bridge(plan: CompiledPlan) -> AsyncBridge | None:
    """Create a default asyncio AsyncBridge if the plan requires async execution.

    Used as a fallback when no pre-configured bridge is supplied to
    :func:`build_dataflow_with_shutdown` — e.g. in test helpers or direct
    adapter use.  Production runners should pass an explicit bridge created
    via :func:`~loom.streaming.bytewax.runner._create_bridge` so that backend
    and uvloop settings from :class:`BytewaxRuntimeConfig` are applied.
    """
    if not plan.needs_async_bridge:
        return None
    return AsyncBridge()


def _decode_source_record(
    payload: Any,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
) -> DecodeResult[StreamPayload]:
    """Decode source records into DSL messages without raising decode errors."""
    if isinstance(payload, Message):
        return DecodeOk(message=cast(Message[StreamPayload], payload))
    if isinstance(payload, KafkaRecord):
        record = cast(KafkaRecord[bytes], payload)
        source = ctx.plan.source
        if not source.needs_decode:
            raise TypeError("Mongo CDC sources must emit Message values, not KafkaRecord items.")
        if isinstance(source, CompiledMultiSource):
            return try_decode_multi_record(record, source.dispatch, codec)
        if not isinstance(source, CompiledSingleSource):
            raise TypeError(f"Expected CompiledSingleSource, got {type(source).__name__}.")
        return try_decode_record(record, source.payload_type, codec)
    raise TypeError(f"Expected Message or KafkaRecord, got {type(payload).__name__}.")


def _is_decode_ok(result: DecodeResult[StreamPayload]) -> bool:
    """Return whether a source decode result can continue through the flow."""
    return isinstance(result, DecodeOk)


def _decode_ok_message(result: DecodeResult[StreamPayload]) -> Message[StreamPayload]:
    """Unwrap a successful source decode result."""
    if isinstance(result, DecodeOk):
        return result.message
    raise TypeError(f"Expected DecodeOk, got {type(result).__name__}.")
