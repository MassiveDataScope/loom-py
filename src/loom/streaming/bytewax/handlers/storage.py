"""Bytewax handler for IntoSink storage terminal nodes.

Bridges the loom SinkPartition protocol to the Bytewax DynamicSink API.
One _StorageDynamicSink per IntoSink node; one _StorageSinkPartition per
Bytewax worker, built via node.build_partition(config, worker_index, worker_count, bridge).

Observability
-------------
A storage partition buffers, so a message dies when the flush that holds it
returns, not when it is handed over. ``_StorageSinkPartition`` emits the N+1
shape of :mod:`loom.streaming.bytewax._batch_spans` for the messages each call
made durable: one ``terminal:sink_write`` span per message, in that message's
own trace, plus one ``Scope.WRITE`` span for the call that flushed them, in a
trace of its own, linking back to all of them.  The loom ``SinkPartition``
implementation stays free of framework dependencies — observability lives
entirely in the adapter layer.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from time import time_ns
from typing import Any

from bytewax.operators import output as bw_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from loom.core.logger import get_logger
from loom.core.observability.event import Scope, TerminalReason
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.streaming.bytewax._batch_spans import BatchSpan, BatchWindow, emit_batch_spans
from loom.streaming.bytewax._commit_tracker import CommitCompletionPort
from loom.streaming.bytewax.handlers._shared import _BuildContextProtocol, _step_id
from loom.streaming.compiler._plan import CompiledStorageSink
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.core._tracing import open_terminal_span
from loom.streaming.nodes._sink import IntoSink, SinkPartition
from loom.streaming.nodes._table import Backend, IntoTable

Stream = Any


class _StorageSinkPartition(StatelessSinkPartition[Message[Any]]):
    """Bytewax sink partition that delegates writes to a loom SinkPartition.

    Extracts the typed payload from each Loom ``Message`` envelope and forwards
    the batch to the underlying partition. The metadata of every message it has
    not yet reported durable waits here, oldest first; when the partition
    reports how many it made durable, exactly those messages get their terminal
    span and complete their source offset. Storage backends remain free of
    transport and observability concerns.

    Args:
        partition:  Loom SinkPartition built by ``IntoSink.build_partition``.
        node_name:  Human-readable sink identifier used in observability events.
        flow_name:  Name of the enclosing streaming flow.
        flow_run_id: Identifier of the flow run this partition belongs to.
        observer:   Observability runtime that receives WRITE lifecycle events.
        commit_tracker: Completion port under at-least-once delivery, else ``None``.
    """

    def __init__(
        self,
        partition: SinkPartition[Any],
        *,
        node_name: str,
        flow_name: str,
        flow_run_id: str,
        observer: ObservabilityRuntime,
        commit_tracker: CommitCompletionPort | None = None,
    ) -> None:
        self._partition = partition
        self._node_name = node_name
        self._flow_name = flow_name
        self._flow_run_id = flow_run_id
        self._observer = observer
        self._commit_tracker = commit_tracker
        self._pending: deque[MessageMeta] = deque()
        self._failed = False

    def write_batch(self, items: list[Message[Any]]) -> None:
        """Forward the payloads, then finish the messages the partition made durable.

        Empty epochs are forwarded too, so a partition can flush on age; a call
        that makes nothing durable emits no span.

        Args:
            items: Loom messages delivered by Bytewax for the current epoch.

        Raises:
            Exception: Whatever the underlying partition raises, after the
                failure has been recorded on the span of every message it held.
        """
        self._pending.extend(item.meta for item in items)
        started_ns = time_ns()
        try:
            durable = self._partition.write_batch([item.payload for item in items])
        except Exception as exc:
            self._failed = True
            self._emit_spans(list(self._pending), BatchWindow.since(started_ns), exc)
            raise
        self._finish(durable, BatchWindow.since(started_ns))

    def _finish(self, durable: object, window: BatchWindow) -> None:
        """Emit the terminal spans of, and complete, the *durable* oldest pending messages.

        After a call to the partition raised, nothing completes: a later count
        cannot say which of the pending messages it covers.

        Raises:
            TypeError: If the partition did not report a count of its pending items.
        """
        if self._failed:
            self._pending.clear()
            return
        if (
            isinstance(durable, bool)
            or not isinstance(durable, int)
            or not 0 <= durable <= len(self._pending)
        ):
            raise TypeError(
                f"{self._node_name}: SinkPartition.write_batch and close must return how "
                f"many of the {len(self._pending)} pending items they made durable; "
                f"got {durable!r}."
            )
        if not durable:
            return
        metas = [self._pending.popleft() for _ in range(durable)]
        self._emit_spans(metas, window, None)
        if self._commit_tracker is None:
            return
        for meta in metas:
            if meta.topic is not None and meta.partition is not None and meta.offset is not None:
                self._commit_tracker.complete(meta.topic, meta.partition, meta.offset)

    def _emit_spans(
        self,
        metas: list[MessageMeta],
        window: BatchWindow,
        error: BaseException | None,
    ) -> None:
        """Emit the per-message terminal spans and the batch write span."""
        emit_batch_spans(
            self._observer,
            metas,
            batch=BatchSpan(
                scope=Scope.WRITE,
                name=f"{self._flow_name}:{self._node_name}",
                attributes={
                    "flow": self._flow_name,
                    "sink": self._node_name,
                    "batch_size": len(metas),
                    "loom.flow_run_id": self._flow_run_id,
                },
            ),
            open_participation=self._open_terminal,
            window=window,
            error=error,
        )

    def _open_terminal(
        self,
        meta: MessageMeta,
        batch_attributes: Mapping[str, object],
        started_ns: int,
    ) -> LoomSpan:
        return open_terminal_span(
            self._observer,
            meta,
            TerminalReason.SINK_WRITE,
            start_time_ns=started_ns,
            attributes={
                "flow": self._flow_name,
                "sink": self._node_name,
                **batch_attributes,
            },
        )

    def close(self) -> None:
        """Close the underlying partition and finish what its final flush made durable."""
        started_ns = time_ns()
        self._finish(self._partition.close(), BatchWindow.since(started_ns))


class _StorageDynamicSink(DynamicSink[Message[Any]]):
    """Bytewax DynamicSink that builds one _StorageSinkPartition per worker.

    Args:
        compiled: Pre-resolved storage sink carrying the DSL node and config.
        ctx:      Adapter build context: flow name and run id, observability
                  runtime, async bridge, session managers and commit tracker
                  for each per-worker partition.
    """

    def __init__(
        self,
        compiled: CompiledStorageSink,
        ctx: _BuildContextProtocol,
    ) -> None:
        self._compiled = compiled
        self._ctx = ctx

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _StorageSinkPartition:
        """Build the per-worker partition by delegating to the DSL node.

        Args:
            step_id:      Bytewax step identifier (unused by the partition).
            worker_index: Zero-based index of the calling worker.
            worker_count: Total number of workers in this run.

        Returns:
            A ready-to-write ``_StorageSinkPartition`` with observability wired.
        """
        del step_id
        node = self._compiled.node
        node_name = node.name or type(node).__name__
        bound_logger = get_logger(__name__).bind(
            flow=self._ctx.plan.name,
            sink=node_name,
        )
        if isinstance(node, IntoTable):
            session_manager = None
            if node.backend is Backend.SQLALCHEMY:
                if self._ctx.bridge is None:
                    raise RuntimeError(
                        "IntoTable backend=sqlalchemy requires an AsyncBridge "
                        "from the Bytewax runtime."
                    )
                if self._compiled.database_config is None:
                    raise RuntimeError(
                        "IntoTable backend=sqlalchemy requires a resolved database config."
                    )
                session_manager = self._ctx.session_manager_for(self._compiled.database_config)
            partition = node.build_partition(
                self._compiled.config,
                worker_index,
                worker_count,
                bridge=self._ctx.bridge,
                session_manager=session_manager,
                logger=bound_logger,
            )
        else:
            partition = node.build_partition(
                self._compiled.config,
                worker_index,
                worker_count,
                bridge=self._ctx.bridge,
            )
        return _StorageSinkPartition(
            partition,
            node_name=node_name,
            flow_name=self._ctx.plan.name,
            flow_run_id=self._ctx.flow_run_id,
            observer=self._ctx.flow_runtime,
            commit_tracker=self._ctx.commit_tracker,
        )


def _apply_into_sink(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> None:
    """Wire an IntoSink terminal node to a Bytewax output operator.

    Looks up the pre-compiled storage sink for the current path and registers
    a ``_StorageDynamicSink`` as the Bytewax output.  The sink is built once
    per worker at dataflow startup via ``IntoSink.build_partition``.

    Observability is threaded from the adapter context into each per-worker
    partition so that every epoch flush appears in the flow's lifecycle events.

    Args:
        stream: Incoming Bytewax stream of ``Message`` items.
        raw:    DSL node, expected to satisfy ``IntoSink``.
        idx:    Position of this node in the process.
        ctx:    Adapter build context.

    Raises:
        UnsupportedNodeError: If *raw* does not satisfy ``IntoSink``.
        RuntimeError:         If no compiled storage sink exists for the path.
    """
    if not isinstance(raw, IntoSink):
        raise UnsupportedNodeError(f"Expected IntoSink, got {type(raw).__name__}.")
    path = ctx.current_path
    compiled = ctx.plan.terminal_storage_sinks.get(path)
    if compiled is None:
        raise RuntimeError(
            f"IntoSink '{type(raw).__name__}' at path {path} has no compiled storage sink; "
            "ensure compile_flow ran before building the dataflow."
        )
    sid = _step_id(f"storage_sink_{idx}", ctx)
    bw_output(sid, stream, _StorageDynamicSink(compiled, ctx))
