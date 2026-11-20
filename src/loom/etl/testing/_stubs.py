"""In-memory stub implementations for ETL testing.

Provides stub I/O and observer implementations that allow step and pipeline
tests to run without real storage dependencies.
"""

from __future__ import annotations

from typing import Any

from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.protocol import LifecycleObserver
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import SourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.lineage._records import EventName, RunStatus
from loom.etl.schema._schema import ColumnSchema, LoomDtype

_UNKNOWN_DTYPE = LoomDtype.NULL


class InMemoryCatalog:
    """In-memory :class:`~loom.etl._io.TableDiscovery` for tests.

    Accepts two optional seed dictionaries:

    * ``tables``  — column name tuples (for quick existence + column checks)
    * ``schemas`` — full :class:`~loom.etl._schema.ColumnSchema` tuples
                    (for type-aware writer tests)

    When ``schemas`` is provided, ``columns`` and ``exists`` are also derived
    from it — no need to populate both.  ``update_schema`` persists changes in
    memory so later steps in the same test see the evolved schema.

    Args:
        tables:  Mapping of ``"schema.table"`` → ``("col1", "col2", ...)``.
        schemas: Mapping of ``"schema.table"`` → ``tuple[ColumnSchema, ...]``.

    Example::

        catalog = InMemoryCatalog(
            schemas={
                "raw.orders": (
                    ColumnSchema("id",     LoomDtype.INT64,   nullable=False),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                    ColumnSchema("year",   LoomDtype.INT32,   nullable=False),
                ),
                "staging.out": (),
            }
        )
        assert catalog.exists(TableRef("raw.orders"))
        assert catalog.schema(TableRef("raw.orders"))[0].name == "id"
    """

    def __init__(
        self,
        tables: dict[str, tuple[str, ...]] | None = None,
        schemas: dict[str, tuple[ColumnSchema, ...]] | None = None,
    ) -> None:
        self._schemas: dict[str, tuple[ColumnSchema, ...]] = dict(schemas or {})
        for ref, cols in (tables or {}).items():
            if ref not in self._schemas:
                self._schemas[ref] = tuple(ColumnSchema(name=c, dtype=_UNKNOWN_DTYPE) for c in cols)

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the ref is registered in this stub catalog."""
        return ref.ref in self._schemas

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return registered column names, or ``()`` for unknown tables."""
        return tuple(c.name for c in self._schemas.get(ref.ref, ()))

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return the full schema, or ``None`` if the table is not yet registered."""
        return self._schemas.get(ref.ref)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist a schema update in memory."""
        self._schemas[ref.ref] = schema


class InMemorySourceReader:
    """In-memory :class:`~loom.etl._io.SourceReader` for tests.

    Returns pre-seeded frames keyed by source alias.  Any alias not found
    returns ``None`` — tests should seed all expected aliases.

    Args:
        frames: Mapping of source alias → frame object.

    Example::

        reader = InMemorySourceReader({"orders": pl.LazyFrame({"id": [1, 2]})})
        frame = reader.read(orders_spec, params)
        assert frame is not None
    """

    def __init__(self, frames: dict[str, Any] | None = None) -> None:
        self._frames: dict[str, Any] = dict(frames or {})

    def read(
        self,
        spec: SourceSpec,
        _params_instance: Any,
        /,
    ) -> Any:
        """Return the pre-seeded frame for ``spec.alias``, or ``None``."""
        return self._frames.get(spec.alias)

    def read_streaming(
        self,
        spec: SourceSpec,
        _params_instance: Any,
        /,
    ) -> Any:
        """Return the same pre-seeded frame as :meth:`read`.

        The stub does not implement true streaming; it just records that the
        streaming code path was taken so tests can exercise it.
        """
        return self._frames.get(spec.alias)

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        """SQL execution is not supported by the generic in-memory stub."""
        _ = frames
        _ = query
        raise NotImplementedError(
            "InMemorySourceReader does not implement SQLExecutor.execute_sql(). "
            "Use backend-specific readers/runners for StepSQL tests."
        )


class InMemoryTargetWriter:
    """In-memory :class:`~loom.etl._io.TargetWriter` for tests.

    Captures all write calls in :attr:`written` for post-execution assertion.

    Attributes:
        written: List of ``(frame, spec)`` pairs in write order.
        streaming_flags: Streaming flag values received for each write call.

    Example::

        writer = InMemoryTargetWriter()
        writer.write(frame, spec, params)
        assert len(writer.written) == 1
        frame_out, spec_out = writer.written[0]
        assert isinstance(spec_out, ReplaceSpec)
    """

    def __init__(self) -> None:
        self.written: list[tuple[Any, TargetSpec]] = []
        self.streaming_flags: list[bool] = []

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        _params_instance: Any,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        """Capture the ``(frame, spec)`` pair for later assertion."""
        _ = write_ctx
        self.written.append((frame, spec))
        self.streaming_flags.append(streaming)


class RecordingRunObserver(LifecycleObserver):
    """In-memory lifecycle observer for tests.

    Captures all lifecycle events as a flat list of ``(event_name, data)``
    tuples.  Helper properties provide quick access to common assertions.

    Attributes:
        events: All events in order — ``(event_name, data_dict)`` tuples.

    Example::

        observer = RecordingRunObserver()
        runtime = Observability([observer])
        executor = ETLExecutor(reader, writer, observability=runtime)
        executor.run_step(plan, params)

        assert observer.step_statuses == ["success"]
        assert "step_start" in observer.event_names
    """

    def __init__(self) -> None:
        self.events: list[tuple[EventName, dict[str, Any]]] = []
        self._synthesized_terminal_events: set[tuple[Scope, str | None]] = set()

    def on_event(self, event: LifecycleEvent) -> None:
        event_key = (event.scope, event.id)
        if event.kind is EventKind.END and event_key in self._synthesized_terminal_events:
            return
        name = _event_name(event)
        data: dict[str, Any] = {
            "scope": event.scope,
            "kind": event.kind,
            "name": event.name,
            "trace_id": event.trace_id,
            "correlation_id": event.correlation_id,
            "id": event.id,
            "status": _run_status(event),
            "duration_ms": event.duration_ms,
            "error": event.error,
            "meta": event.meta,
        }
        if event.scope is Scope.PIPELINE and event.kind is EventKind.START:
            data["pipeline"] = event.name
            data["run_id"] = event.meta.get("run_id")
        if event.scope is Scope.PROCESS and event.kind is EventKind.START:
            data["process"] = event.name
            data["process_run_id"] = event.id
        if event.scope is Scope.STEP:
            data["step"] = event.name
            data["step_run_id"] = event.id
            run_id = event.meta.get("run_id")
            data["run_id"] = run_id if isinstance(run_id, str) else None
        self.events.append((name, data))
        if event.kind is EventKind.ERROR:
            if event.scope is Scope.STEP:
                self.events.append(
                    (
                        EventName.STEP_END,
                        {
                            "step_run_id": event.id,
                            "status": RunStatus.FAILED,
                        },
                    )
                )
                self._synthesized_terminal_events.add(event_key)
            elif event.scope is Scope.PROCESS:
                self.events.append(
                    (
                        EventName.PROCESS_END,
                        {
                            "process_run_id": event.id,
                            "status": RunStatus.FAILED,
                        },
                    )
                )
                self._synthesized_terminal_events.add(event_key)
            elif event.scope is Scope.PIPELINE:
                self.events.append(
                    (
                        EventName.PIPELINE_END,
                        {"run_id": event.id, "status": RunStatus.FAILED},
                    )
                )
                self._synthesized_terminal_events.add(event_key)

    @property
    def event_names(self) -> list[EventName]:
        """Ordered list of event names."""
        return [name for name, _ in self.events]

    @property
    def step_statuses(self) -> list[RunStatus]:
        """Statuses from all ``step_end`` events in order."""
        return [d["status"] for name, d in self.events if name is EventName.STEP_END]

    @property
    def pipeline_statuses(self) -> list[RunStatus]:
        """Statuses from all ``pipeline_end`` events in order."""
        statuses = [d["status"] for name, d in self.events if name is EventName.PIPELINE_END]
        deduped: list[RunStatus] = []
        for status in statuses:
            if not deduped or deduped[-1] is not status:
                deduped.append(status)
        return deduped


def _event_name(event: LifecycleEvent) -> EventName:
    if event.scope is Scope.PIPELINE and event.kind is EventKind.START:
        return EventName.PIPELINE_START
    if event.scope is Scope.PIPELINE and event.kind is not EventKind.START:
        return EventName.PIPELINE_END
    if event.scope is Scope.PROCESS and event.kind is EventKind.START:
        return EventName.PROCESS_START
    if event.scope is Scope.PROCESS and event.kind is not EventKind.START:
        return EventName.PROCESS_END
    if event.scope is Scope.STEP and event.kind is EventKind.START:
        return EventName.STEP_START
    if event.scope is Scope.STEP and event.kind is EventKind.ERROR:
        return EventName.STEP_ERROR
    return EventName.STEP_END


def _run_status(event: LifecycleEvent) -> RunStatus | None:
    if event.status is LifecycleStatus.SUCCESS:
        return RunStatus.SUCCESS
    if event.status is LifecycleStatus.FAILURE:
        return RunStatus.FAILED
    return None
