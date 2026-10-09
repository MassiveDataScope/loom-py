"""ETL executor — internal engine; use :class:`~loom.etl.ETLRunner` instead.

Walks a compiled :class:`~loom.etl.compiler._plan.PipelinePlan` and drives
read → execute → write for each step, emitting lifecycle events through the
shared :class:`~loom.core.observability.runtime.ObservabilityRuntime`.

This class is **not** part of the public API.  It is instantiated exclusively
by :class:`~loom.etl.ETLRunner`, which wires reader, writer, catalog, and the
shared observability runtime from a :data:`~loom.etl.StorageConfig`.

For normal usage::

    from loom.etl import ETLRunner
    from loom.core.observability import ObservabilityRuntime

    runner = ETLRunner.from_yaml(
        "loom.yaml",
        observability=ObservabilityRuntime.noop(),
    )
    runner.run(DailyOrdersPipeline, DailyOrdersParams(run_date=date.today()))

Parallelism
-----------
Sequential nodes run in order.  Nested lists (``ParallelStepGroup``,
``ParallelProcessGroup``) are dispatched through the injected
:class:`~loom.etl.executor.ParallelDispatcher`, defaulting to
:class:`~loom.etl.executor.ThreadDispatcher`.
"""

from __future__ import annotations

import functools
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from loom.core.config import ConfigContext
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.runner import SupportsFlush
from loom.etl.checkpoint import CheckpointCleanupError, CheckpointStore
from loom.etl.compiler._plan import (
    AgentBinding,
    ConfigValueBinding,
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
)
from loom.etl.declarative.source import TempSourceSpec
from loom.etl.declarative.target._client import ClientSpec
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec
from loom.etl.executor._agents import AgentMapper
from loom.etl.executor._dispatcher import ParallelDispatcher, ThreadDispatcher
from loom.etl.lineage._records import RunContext, RunStatus, WriteContext
from loom.etl.pipeline._step_sql import StepSQL
from loom.etl.runtime._config_values import resolve_config_value
from loom.etl.runtime.contracts import (
    AgentBatchRunner,
    ClientCommandExecutor,
    SourceReader,
    SQLExecutor,
    StreamingSourceReader,
    TargetWriter,
)

_log = logging.getLogger(__name__)


class ETLExecutor:
    """Drives execution of compiled ETL plans.

    Responsibilities:

    * Read each source via the injected :class:`~loom.etl._io.SourceReader`.
    * Resolve the step's ``FromConfig`` values from the injected config context.
    * Bind the step's ``WithAgent`` declarations to the injected agent runner.
    * Invoke the step's ``execute()`` with the resulting frames and values.
    * Write the result via the injected :class:`~loom.etl._io.TargetWriter`.
    * Emit lifecycle events to the shared observability runtime.
    * Dispatch parallel groups through the :class:`~loom.etl.executor.ParallelDispatcher`.

    All collaborators are injected — the executor has no dependency on any
    specific backend, broker, or observability system.

    Internal class — use :class:`~loom.etl.ETLRunner` for production code.
    This class is useful for unit tests with stub readers/writers and for
    custom orchestration adapters (Celery, Prefect, Airflow) that need to
    run individual steps outside a full pipeline.

    Args:
        reader:     Source reader implementation.
        writer:     Target writer implementation.
        observability: Shared observability runtime.  Defaults to a no-op runtime.
        dispatcher: Parallel task dispatcher.  Defaults to
                    :class:`~loom.etl.executor.ThreadDispatcher`.
        config_context: Config the ``FromConfig`` values are resolved from,
                    when a step runs.  Required only by steps that
                    declare one.
        agents:     Runner of the agents a ``WithAgent`` declares.  Required
                    only by steps that declare one.
    """

    def __init__(
        self,
        reader: SourceReader,
        writer: TargetWriter,
        observability: ObservabilityRuntime | None = None,
        dispatcher: ParallelDispatcher | None = None,
        checkpoint_store: CheckpointStore | None = None,
        client_executor: ClientCommandExecutor | None = None,
        config_context: ConfigContext | None = None,
        agents: AgentBatchRunner | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._observability = observability or ObservabilityRuntime.noop()
        self._dispatcher: ParallelDispatcher = dispatcher or ThreadDispatcher()
        self._checkpoint_store: CheckpointStore | None = checkpoint_store
        self._client_executor: ClientCommandExecutor | None = client_executor
        self._config_context: ConfigContext | None = config_context
        self._agents: AgentBatchRunner | None = agents

    @property
    def observability(self) -> ObservabilityRuntime:
        """Return the shared observability runtime."""
        return self._observability

    def flush(self) -> None:
        """Flush any buffered executor state exposed by observers."""
        for observer in self._observability.observers:
            if isinstance(observer, SupportsFlush):
                observer.flush()

    def run_pipeline(
        self,
        plan: PipelinePlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a full pipeline plan.

        Processes run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelProcessGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.PipelinePlan`.
            params: Concrete params instance for this run.
            ctx:    Run context carrying ``run_id``, ``correlation_id``, and
                    ``attempt``.  Generated with a fresh UUID4 when omitted.

        Raises:
            Exception: First unhandled exception from any step, after all
                       observers have received the ``pipeline_end(FAILED)`` event.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        _log.info(
            "pipeline start pipeline=%s run_id=%s nodes=%d attempt=%d",
            plan.pipeline_type.__name__,
            ctx.run_id,
            len(plan.nodes),
            ctx.attempt,
        )
        status = RunStatus.SUCCESS
        try:
            with self._observability.span(
                Scope.PIPELINE,
                plan.pipeline_type.__name__,
                trace_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
                id=ctx.run_id,
                run_id=ctx.run_id,
                attempt=ctx.attempt,
                last_attempt=ctx.last_attempt,
                nodes=len(plan.nodes),
                params=_render_params(params),
            ):
                for node in plan.nodes:
                    self._run_pipeline_node(node, params, ctx)
        except Exception:
            status = RunStatus.FAILED
            raise
        finally:
            self._cleanup_temps(ctx, status)

    def run_process(
        self,
        plan: ProcessPlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a single process plan.

        Steps run sequentially unless wrapped in a
        :class:`~loom.etl.compiler.ParallelStepGroup`.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.ProcessPlan`.
            params: Concrete params instance for this run.
            ctx:    Run context from the parent pipeline, or a fresh one.

        Raises:
            Exception: First unhandled exception from any step.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        process_run_id = _new_run_id()
        _log.info(
            "process start process=%s process_run_id=%s nodes=%d",
            plan.process_type.__name__,
            process_run_id,
            len(plan.nodes),
        )
        process_ctx = ctx.for_process(process_run_id)
        with self._observability.span(
            Scope.PROCESS,
            plan.process_type.__name__,
            trace_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
            id=process_run_id,
            run_id=ctx.run_id,
            process_run_id=process_run_id,
            steps=len(plan.nodes),
        ):
            for node in plan.nodes:
                self._run_process_node(node, params, process_ctx)

    def run_step(
        self,
        plan: StepPlan,
        params: Any,
        ctx: RunContext | None = None,
    ) -> None:
        """Execute a single step plan.

        Resolves the step's ``FromConfig`` values, reads all sources, calls
        ``execute()``, writes the target.  Config values are passed to
        ``execute()`` only; they are never logged nor attached to an event.

        Args:
            plan:   Compiled :class:`~loom.etl.compiler.StepPlan`.
            params: Concrete params instance for this run.
            ctx:    Run context from the parent pipeline, or a fresh one.

        Raises:
            ConfigValueError: When a ``FromConfig`` value is missing,
                       unresolvable, or of the wrong type.
            Exception: Any unhandled exception from read, execute, or write —
                       after all observers have received ``on_step_error`` and
                       ``on_step_end(FAILED)``.
        """
        ctx = ctx or RunContext(run_id=_new_run_id())
        step_run_id = _new_run_id()
        _log.info(
            "step start step=%s step_run_id=%s sources=%s",
            plan.step_type.__name__,
            step_run_id,
            [b.alias for b in plan.source_bindings],
        )
        with self._observability.span(
            Scope.STEP,
            plan.step_type.__name__,
            trace_id=ctx.run_id,
            correlation_id=ctx.correlation_id,
            id=step_run_id,
            run_id=ctx.run_id,
            process_run_id=ctx.process_run_id,
            step_run_id=step_run_id,
            sources=[b.alias for b in plan.source_bindings],
            target=_span_target_label(plan.target_binding.spec),
            streaming=plan.streaming,
        ):
            values = {**self._resolve_config_values(plan), **self._bind_agents(plan)}
            if isinstance(plan.target_binding.spec, ClientSpec):
                step = plan.step_type()
                client_exec = self._require_client_executor(step)
                client_exec.command(lambda client: step.execute(params, client=client, **values))
                return

            frames = {
                b.alias: self._read_source(b.spec, params, ctx, streaming=plan.streaming)
                for b in plan.source_bindings
            }
            step = plan.step_type()
            if isinstance(step, StepSQL):
                query = _render_sql_query(step, params)
                sql_reader = self._require_sql_executor(step)
                result = sql_reader.execute_sql(frames, query)
            else:
                result = step.execute(params, **frames, **values)
            write_ctx = WriteContext(
                run_id=ctx.run_id,
                step=plan.step_type.__name__,
                attempt=ctx.attempt,
                process_run_id=ctx.process_run_id,
            )
            self._write_target(
                result,
                plan.target_binding.spec,
                params,
                ctx,
                streaming=plan.streaming,
                write_ctx=write_ctx,
            )

    def _resolve_config_values(self, plan: StepPlan) -> dict[str, Any]:
        """Resolve the plan's ``FromConfig`` values, keyed by ``execute()`` keyword."""
        if not plan.config_bindings:
            return {}
        context = self._require_config_context(plan.step_type, plan.config_bindings)
        return {
            b.alias: resolve_config_value(context, b.key, b.value_type)
            for b in plan.config_bindings
        }

    def _bind_agents(self, plan: StepPlan) -> dict[str, AgentMapper]:
        """Bind the plan's ``WithAgent`` declarations, keyed by ``execute()`` keyword."""
        if not plan.agent_bindings:
            return {}
        runner = self._require_agents(plan.step_type, plan.agent_bindings)
        return {b.alias: AgentMapper(runner, b) for b in plan.agent_bindings}

    def _require_agents(
        self, step_type: type[Any], bindings: tuple[AgentBinding, ...]
    ) -> AgentBatchRunner:
        """Return the agent runner, or raise if unconfigured."""
        if self._agents is not None:
            return self._agents
        raise RuntimeError(
            f"Step {step_type.__qualname__!r} declares WithAgent values "
            f"{[b.alias for b in bindings]} but no agent runner was configured. "
            "Declare an 'ai:' section and build the runner with ETLRunner.from_yaml(), "
            "or pass agents= to ETLRunner / ETLExecutor."
        )

    def _require_config_context(
        self, step_type: type[Any], bindings: tuple[ConfigValueBinding, ...]
    ) -> ConfigContext:
        """Return the config context, or raise if unconfigured."""
        if self._config_context is not None:
            return self._config_context
        raise RuntimeError(
            f"Step {step_type.__qualname__!r} declares FromConfig values "
            f"{[b.alias for b in bindings]} but no config_context was configured. "
            "Build the runner with ETLRunner.from_yaml(), or pass config_context= "
            "to ETLRunner / ETLExecutor."
        )

    def _require_checkpoint_store(self, name: str) -> CheckpointStore:
        """Return the checkpoint store, or raise if unconfigured."""
        if self._checkpoint_store is None:
            raise RuntimeError(
                f"Step uses checkpoint intermediate {name!r} but no checkpoint store is "
                "configured. Set 'checkpoint:' in your storage config."
            )
        return self._checkpoint_store

    def _require_sql_executor(self, step: Any) -> SQLExecutor:
        """Return reader as SQLExecutor, or raise a clear configuration error."""
        if isinstance(self._reader, SQLExecutor):
            return self._reader
        raise TypeError(
            f"Step {type(step).__qualname__!r} is SQL-based but configured reader "
            f"{type(self._reader).__qualname__!r} does not implement SQLExecutor."
        )

    def _require_client_executor(self, step: Any) -> ClientCommandExecutor:
        """Return the injected ClientCommandExecutor, or raise a clear configuration error."""
        if self._client_executor is not None:
            return self._client_executor
        raise TypeError(
            f"Step {type(step).__qualname__!r} is a ClientStep but no "
            "ClientCommandExecutor was configured. "
            "Pass client_executor= to ETLExecutor (or ETLRunner) to enable client steps. "
            "Example: client_executor=ClickHouseClientExecutor(url=...)"
        )

    def _read_source(
        self, spec: Any, params: Any, ctx: RunContext, *, streaming: bool = False
    ) -> Any:
        if isinstance(spec, TempSourceSpec):
            _log.info("read source kind=TEMP name=%s", spec.temp_name)
            return self._require_checkpoint_store(spec.temp_name).get(
                spec.temp_name,
                run_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
            )
        _log.info(
            "read source kind=%s ref=%s streaming=%s",
            spec.kind,
            getattr(spec, "table_ref", None) or getattr(spec, "path", None),
            streaming,
        )
        if streaming:
            if not isinstance(self._reader, StreamingSourceReader):
                raise TypeError(
                    f"Step opts into streaming=True but reader "
                    f"{type(self._reader).__qualname__!r} does not implement "
                    "StreamingSourceReader. Falling back to a non-streaming read "
                    "would risk OOM, so the read is refused."
                )
            return self._reader.read_streaming(spec, params)
        return self._reader.read(spec, params)

    def _write_target(
        self,
        result: Any,
        spec: Any,
        params: Any,
        ctx: RunContext,
        *,
        streaming: bool = False,
        write_ctx: WriteContext | None = None,
    ) -> None:
        if isinstance(spec, (TempSpec, TempFanInSpec)):
            _log.info("write target kind=TEMP name=%s scope=%s", spec.temp_name, spec.temp_scope)
            self._require_checkpoint_store(spec.temp_name).put(
                spec.temp_name,
                run_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
                scope=spec.temp_scope,
                data=result,
                append=isinstance(spec, TempFanInSpec),
            )
        else:
            _log.info(
                "write target ref=%s",
                getattr(spec, "table_ref", None) or getattr(spec, "path", None),
            )
            self._writer.write(result, spec, params, streaming=streaming, write_ctx=write_ctx)

    def _cleanup_temps(self, ctx: RunContext, status: RunStatus) -> None:
        if self._checkpoint_store is None:
            return
        try:
            self._checkpoint_store.cleanup_run(ctx.run_id)
        except CheckpointCleanupError:
            _log.exception(
                "checkpoint RUN cleanup FAILED run_id=%s — temps may accumulate",
                ctx.run_id,
            )
        if ctx.correlation_id is None:
            return
        if status is RunStatus.SUCCESS and ctx.last_attempt:
            try:
                self._checkpoint_store.cleanup_correlation(ctx.correlation_id)
            except CheckpointCleanupError:
                _log.exception(
                    "checkpoint CORRELATION cleanup FAILED correlation_id=%s — "
                    "call cleanup_correlation(%r) manually",
                    ctx.correlation_id,
                    ctx.correlation_id,
                )
        elif status is RunStatus.FAILED and ctx.last_attempt:
            _log.warning(
                "CORRELATION intermediates were NOT cleaned — pipeline failed on last attempt. "
                "Call runner.cleanup_correlation(%r) to reclaim storage.",
                ctx.correlation_id,
            )

    def _dispatch_parallel(
        self,
        run_fn: Callable[..., None],
        plans: tuple[Any, ...],
        params: Any,
        ctx: RunContext,
    ) -> None:
        """Dispatch *plans* concurrently via the configured dispatcher."""
        tasks = [functools.partial(run_fn, p, params, ctx) for p in plans]
        self._dispatcher.run_all(tasks)

    def _run_pipeline_node(self, node: PipelineProcessNode, params: Any, ctx: RunContext) -> None:
        match node:
            case ParallelProcessGroup(plans=plans):
                self._dispatch_parallel(self.run_process, plans, params, ctx)
            case ProcessPlan():
                self.run_process(node, params, ctx)

    def _run_process_node(self, node: ProcessStepNode, params: Any, ctx: RunContext) -> None:
        match node:
            case ParallelStepGroup(plans=plans):
                self._dispatch_parallel(self.run_step, plans, params, ctx)
            case StepPlan():
                self.run_step(node, params, ctx)


def _new_run_id() -> str:
    """Generate a fresh UUID4 run identifier."""
    return str(uuid.uuid4())


def _span_target_label(spec: Any) -> str:
    """Return a human-readable target label for observability spans."""
    if isinstance(spec, ClientSpec):
        return "client"
    return str(getattr(spec, "table_ref", None) or getattr(spec, "path", None))


def _render_params(params: Any) -> dict[str, Any]:
    """Render an ``ETLParams`` instance as a JSON-friendly dict for events.

    Used to attach the resolved invocation parameters to the pipeline START
    event so observers (structlog/JSON sinks, Prefect UI) can show what the
    pipeline was called with. Datetimes are ISO-formatted by ``msgspec``.
    Falls back to ``str(params)`` if msgspec cannot encode the object.
    """
    import msgspec  # noqa: PLC0415

    try:
        rendered = msgspec.to_builtins(params)
    except Exception:  # noqa: BLE001
        return {"repr": str(params)}
    if isinstance(rendered, dict):
        return rendered
    return {"value": rendered}


def _ms(start: float) -> int:
    """Elapsed milliseconds since ``start`` (from ``time.monotonic()``)."""
    return int((time.monotonic() - start) * 1000)


def _render_sql_query(step: Any, params: Any) -> str:
    """Render SQL query from a StepSQL-like step instance."""
    render = getattr(step, "render_sql", None)
    if not callable(render):
        raise TypeError(
            f"{type(step).__qualname__} is marked as SQL step but has no callable render_sql()."
        )
    query = render(params)
    if not isinstance(query, str):
        raise TypeError(
            f"{type(step).__qualname__}.render_sql() must return str, got {type(query)!r}."
        )
    return query
