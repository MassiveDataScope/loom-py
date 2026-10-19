"""StreamingRunner — production Bytewax runtime entry point for Loom flows."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import cli_main

from loom.core.async_bridge import AsyncBridge, build_backend_options
from loom.core.config import (
    ConfigContext,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.model import LoomFrozenStruct
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.runner import shutdown_runner
from loom.core.tracing import generate_trace_id
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown
from loom.streaming.bytewax._errors import RuntimeConfigurationError
from loom.streaming.bytewax._runtime_io import (
    build_commit_tracker,
    build_runtime_error_sinks,
    build_runtime_sink,
    build_runtime_source,
    build_runtime_terminal_sinks,
)
from loom.streaming.bytewax._sink_registry import SinkRegistry
from loom.streaming.compiler import (
    CompiledPlan,
    compile_flow,
    walk_process_nodes,
)
from loom.streaming.compiler._errors import StreamingErrorCode
from loom.streaming.core._errors import ErrorKind
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.nodes._shape import CollectBatch

logger = logging.getLogger(__name__)


@runtime_checkable
class ErrorSink(Protocol):
    """Bytewax sink protocol for ErrorEnvelope items."""

    def build(self, step_id: str, worker_index: int, worker_count: int) -> Any: ...


@dataclass(frozen=True)
class _PreparedStreamingRun:
    """Prepared Bytewax execution bundle.

    Args:
        dataflow: Built Bytewax dataflow ready for execution.
        shutdown: Callback that releases adapter-owned resources.
    """

    dataflow: Dataflow
    shutdown: Callable[[], None]


class BytewaxRecoverySettings(LoomFrozenStruct, frozen=True):
    """Recovery settings for the Bytewax runtime.

    Args:
        db_dir: Local recovery directory shared by this execution.
        backup_interval_ms: Optional snapshot backup interval in milliseconds.
    """

    db_dir: str
    backup_interval_ms: int | None = None


class BytewaxRuntimeConfig(LoomFrozenStruct, frozen=True):
    """Runtime settings for executing a Bytewax dataflow.

    **Delivery guarantee:** By default Kafka offsets are committed by the
    consumer's auto-commit policy and are not tied to downstream success.
    When ``enable.auto.commit`` is disabled in the consumer settings, Loom
    tracks item completion through the adapter and commits offsets only after
    the downstream branch finishes successfully.

    Args:
        workers_per_process: Number of worker threads in this process.
        process_id: Optional cluster process id.
        addresses: Optional cluster address list, including this process.
        epoch_interval_ms: Optional epoch duration in milliseconds.
        recovery: Optional recovery settings.
        async_backend: Anyio backend used for :class:`AsyncBridge` when the
            flow contains :class:`~loom.streaming.WithAsync` nodes.
            Accepted values: ``"asyncio"`` (default) or ``"trio"``.
        use_uvloop: Replace the asyncio event loop with uvloop for lower
            latency async I/O.  Only effective when *async_backend* is
            ``"asyncio"`` and the platform is not Windows.
        force_shutdown_timeout_ms: Maximum milliseconds to wait for in-flight
            async tasks to finish during shutdown.  When exceeded, the portal
            thread is abandoned and a warning is logged.  ``None`` waits
            indefinitely (default).
    """

    workers_per_process: int = 1
    process_id: int | None = None
    addresses: tuple[str, ...] | None = None
    epoch_interval_ms: int | None = None
    recovery: BytewaxRecoverySettings | None = None
    async_backend: Literal["asyncio", "trio"] = "asyncio"
    use_uvloop: bool = False
    force_shutdown_timeout_ms: int | None = None


class StreamingRunner:
    """Wire a :class:`~loom.streaming.StreamFlow` declaration into the real
    Bytewax runtime.

    Args:
        _registry: Optional pre-built SinkRegistry for testing purposes.
    """

    def __init__(self, *, _registry: SinkRegistry | None = None) -> None:
        self._flow: StreamFlow[Any, Any] | None = None
        self._plan: CompiledPlan | None = None
        self._runtime: BytewaxRuntimeConfig = BytewaxRuntimeConfig()
        self._observability_runtime: ObservabilityRuntime = ObservabilityRuntime.noop()
        self._log = logger
        self._shutdown: Callable[[], None] | None = None
        self._sink_registry: SinkRegistry = _registry or SinkRegistry()
        self._resolved_error_sinks: dict[ErrorKind, Any] = {}

    def _configure(
        self,
        flow: StreamFlow[Any, Any],
        plan: CompiledPlan,
        runtime: BytewaxRuntimeConfig,
        observability_runtime: ObservabilityRuntime,
    ) -> None:
        self._flow = flow
        self._plan = plan
        self._runtime = runtime
        self._observability_runtime = observability_runtime

    @classmethod
    def from_context(
        cls,
        flow: StreamFlow[Any, Any],
        *,
        config: ConfigContext,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingRunner:
        """Build a runner from a resolved config context."""
        plan = compile_flow(flow, config=config)
        runtime = config.section_or_default(
            ConfigKey.STREAMING_RUNTIME,
            BytewaxRuntimeConfig,
            BytewaxRuntimeConfig(),
        )
        observability_cfg = config.section_optional(
            ConfigKey.OBSERVABILITY,
            ObservabilityConfig,
        )
        if observability_cfg is None:
            observability_cfg = ObservabilityConfig()
        obs = observability_runtime or ObservabilityRuntime.from_config(observability_cfg)
        runner = cls()
        runner._configure(flow, plan, runtime, obs)
        return runner

    @classmethod
    def from_yaml(
        cls,
        flow: StreamFlow[Any, Any],
        path: str,
        *,
        resolvers: Sequence[ConfigResolver] = (),
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingRunner:
        """Load config from YAML and build a runner.

        Args:
            flow: Flow to compile.
            path: Local path or cloud URI of the YAML config file.
            resolvers: Resolvers for ``${name:key}`` placeholders, registered
                before the built-in ``secrets`` and ``ssm`` defaults.  A
                resolver named like a default replaces it.
            observability_runtime: Optional runtime override.
        """
        return cls.from_context(
            flow,
            config=_load_yaml(path, resolvers),
            observability_runtime=observability_runtime,
        )

    @classmethod
    def from_dict(
        cls,
        flow: StreamFlow[Any, Any],
        config: dict[str, Any],
        *,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingRunner:
        """Build a runner from a plain Python config mapping."""
        return cls.from_context(
            flow,
            config=ConfigContext.from_dict(config),
            observability_runtime=observability_runtime,
        )

    @property
    def _registry(self) -> SinkRegistry:
        """Internal sink registry for inspection in tests."""
        return self._sink_registry

    def register_sink(self, sink_class: type) -> None:
        """Register a sink class to be resolved from YAML config at run time.

        Args:
            sink_class: Class that satisfies the RegisteredSink protocol.

        Raises:
            TypeError: If sink_class does not implement RegisteredSink.
        """
        self._sink_registry.register(sink_class)

    def build_dataflow(self) -> Dataflow:
        """Assemble the Bytewax Dataflow and keep shutdown state."""
        prepared = self.prepare_run()
        return prepared.dataflow

    def prepare_run(
        self,
        *,
        error_sinks: Mapping[ErrorKind, ErrorSink] | None = None,
        runtime: BytewaxRuntimeConfig | None = None,
    ) -> _PreparedStreamingRun:
        """Prepare one executable dataflow and its shutdown callback.

        Releases any resources from a previous ``prepare_run()`` call before
        building the new dataflow, so calling this method twice is safe.

        Args:
            error_sinks: Optional mapping of :class:`~loom.streaming.core._errors.ErrorKind`
                to :class:`ErrorSink` instances.  When provided, these sinks override the
                default error routing built from the compiled plan.
            runtime: Optional runtime override; defaults to the runner's
                config-loaded runtime settings.

        Returns:
            A bundle containing the assembled :class:`~bytewax.dataflow.Dataflow`
            and a shutdown callable that releases adapter-owned resources.
        """
        if self._plan is None:
            raise RuntimeError(
                "StreamingRunner has no compiled plan. "
                "Use run(flow=..., config_path=...) or a factory method "
                "(from_yaml / from_context / from_dict) before calling prepare_run."
            )
        shutdown_runner(self)
        merged_error_sinks: Mapping[ErrorKind, Any] | None
        if error_sinks is not None:
            merged_error_sinks = error_sinks
        else:
            merged_error_sinks = self._resolved_error_sinks or None
        prepared = _prepare_run(
            self._plan,
            observability_runtime=self._observability_runtime,
            runtime=runtime or self._runtime,
            error_sinks=merged_error_sinks,
        )
        self._shutdown = prepared.shutdown
        return prepared

    def run(
        self,
        *,
        flow: StreamFlow[Any, Any] | None = None,
        config: ConfigContext | None = None,
        config_path: str | Path | None = None,
        error_sinks: Mapping[ErrorKind, Any] | None = None,
        runtime: BytewaxRuntimeConfig | None = None,
        resolvers: Sequence[ConfigResolver] = (),
    ) -> None:
        """Execute the compiled dataflow with the real Bytewax runtime.

        Emits ``Scope.POLL_CYCLE`` around the full invocation (including
        preparation) and ``Scope.FLOW`` around the blocking ``cli_main`` call.
        Starts the Prometheus scrape server before execution when configured.

        Args:
            flow: Optional flow override. When provided together with ``config``
                or ``config_path``, the runner is reconfigured before execution.
            config: Optional config context. Used together with ``flow`` to
                reconfigure the runner.
            config_path: Optional path to a YAML config file. Loaded and used
                like ``config`` when provided.
            error_sinks: Optional explicit error sinks. When provided, these
                take precedence over registry-resolved sinks.
            runtime: Optional runtime override. When omitted, uses the runner's
                config-loaded runtime settings.
            resolvers: Resolvers for ``${name:key}`` placeholders in
                ``config_path``, registered before the built-in ``secrets``
                and ``ssm`` defaults.  A resolver named like a default
                replaces it.  Only meaningful together with ``config_path``.

        Raises:
            ValueError: If ``resolvers`` is given together with ``config``
                instead of ``config_path``; ``config`` is already resolved
                and ``resolvers`` would never be applied.
        """
        if config is not None and resolvers:
            raise ValueError(
                "resolvers apply only with config_path, not config: "
                "config is already resolved, so resolvers would never be applied"
            )
        if config_path is not None:
            config = _load_yaml(str(config_path), resolvers)
        if flow is not None and config is not None:
            plan = compile_flow(flow, config=config)
            runtime_cfg = config.section_or_default(
                ConfigKey.STREAMING_RUNTIME, BytewaxRuntimeConfig, BytewaxRuntimeConfig()
            )
            obs_cfg = config.section_optional(ConfigKey.OBSERVABILITY, ObservabilityConfig)
            obs = ObservabilityRuntime.from_config(obs_cfg or ObservabilityConfig())
            self._configure(flow, plan, runtime_cfg, obs)
        if config is not None:
            bindings = self._sink_registry.resolve(config)
            self._resolved_error_sinks = {
                kind: b.sink for b in bindings if b.purpose == "errors" for kind in b.kinds
            }
        if error_sinks is not None:
            self._resolved_error_sinks = error_sinks  # type: ignore[assignment]
        if self._plan is None:
            raise RuntimeError(
                "StreamingRunner has no compiled plan. "
                "Use run(flow=..., config_path=...) or a factory method "
                "(from_yaml / from_context / from_dict) before calling run."
            )
        resolved_runtime = runtime or self._runtime
        try:
            # One span, not an unpaired START/END pair: the run is a unit of
            # work with two ends, so it belongs in a span that also opens the
            # matching OTEL span and drains the exporter when it closes.
            with self._observability_runtime.span(
                Scope.POLL_CYCLE,
                self._plan.name,
                id=generate_trace_id(),
                node_count=len(self._plan.nodes),
            ):
                self._observability_runtime.start_scrape_server()
                prepared = self.prepare_run(runtime=resolved_runtime)
                with self._observability_runtime.span(
                    Scope.FLOW,
                    self._plan.name,
                    node_count=len(self._plan.nodes),
                ):
                    cli_main(prepared.dataflow, **_runtime_kwargs(resolved_runtime))  # type: ignore[no-untyped-call]
        finally:
            shutdown_runner(self)

    def shutdown(self) -> None:
        """Release adapter resources after a run or failed build."""
        if self._shutdown is not None:
            self._log.debug("shutting_down")
            self._shutdown()
            self._shutdown = None


def _load_yaml(path: str, resolvers: Sequence[ConfigResolver]) -> ConfigContext:
    """Load *path* with *resolvers* ahead of the built-in ``secrets``/``ssm`` defaults."""
    return ConfigContext.from_yaml(path, resolvers=with_default_resolvers(resolvers))


def _runtime_kwargs(runtime: BytewaxRuntimeConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "workers_per_process": runtime.workers_per_process,
        "process_id": runtime.process_id,
        "addresses": list(runtime.addresses) if runtime.addresses is not None else None,
        "epoch_interval": _to_timedelta(runtime.epoch_interval_ms),
        "recovery_config": _recovery_config(runtime.recovery),
    }
    return kwargs


def _recovery_config(recovery: BytewaxRecoverySettings | None) -> RecoveryConfig | None:
    if recovery is None:
        return None
    return RecoveryConfig(  # type: ignore[no-untyped-call]
        Path(recovery.db_dir),
        backup_interval=_to_timedelta(recovery.backup_interval_ms),
    )


def _to_timedelta(value_ms: int | None) -> timedelta | None:
    if value_ms is None:
        return None
    return timedelta(milliseconds=value_ms)


def _prepare_run(
    plan: CompiledPlan,
    *,
    observability_runtime: ObservabilityRuntime | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
    runtime: BytewaxRuntimeConfig | None = None,
) -> _PreparedStreamingRun:
    commit_tracker = build_commit_tracker(plan.source)
    _guard_keyed_multiprocess(plan, runtime, commit_tracker)
    if source is None:
        source = build_runtime_source(plan.source, commit_tracker, observability_runtime)
    if sink is None and plan.output is not None:
        sink = build_runtime_sink(plan.output, commit_tracker)
    if terminal_sinks is None:
        terminal_sinks = build_runtime_terminal_sinks(plan.terminal_sinks, commit_tracker)
    if error_sinks is None:
        error_sinks = build_runtime_error_sinks(plan.error_routes, commit_tracker)
    bridge = _create_bridge(plan, runtime or BytewaxRuntimeConfig())
    built = build_dataflow_with_shutdown(
        plan=plan,
        observability_runtime=observability_runtime,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
        bridge=bridge,
        commit_tracker=commit_tracker,
    )
    return _PreparedStreamingRun(dataflow=built.dataflow, shutdown=built.shutdown)


def _guard_keyed_multiprocess(
    plan: CompiledPlan,
    runtime: BytewaxRuntimeConfig | None,
    commit_tracker: object | None,
) -> None:
    """Reject at-least-once + keyed nodes on a multi-process cluster (fail-fast).

    Keyed operators (``CollectBatch``) route records to a primary worker by key
    hash, which in a multi-process cluster may be another process — where
    completions cannot reach the source process's commit tracker, silently
    freezing the watermark. Threads within one process share the tracker and
    remain supported.

    Raises:
        RuntimeConfigurationError: If the plan contains a keyed node and the
            runtime declares more than one process.
    """
    if commit_tracker is None or runtime is None:
        return
    if runtime.addresses is None or len(runtime.addresses) <= 1:
        return
    for node in walk_process_nodes(compiled.node for compiled in plan.nodes):
        if isinstance(node, CollectBatch):
            issue = StreamingErrorCode.DELIVERY_KEYED_MULTIPROCESS.issue(
                component=type(node).__name__
            )
            raise RuntimeConfigurationError([issue])


def _create_bridge(plan: CompiledPlan, runtime: BytewaxRuntimeConfig) -> AsyncBridge | None:
    """Create an AsyncBridge configured from runtime settings, or None if not needed."""
    if not plan.needs_async_bridge:
        return None
    return AsyncBridge(
        backend=runtime.async_backend,
        backend_options=build_backend_options(runtime.async_backend, runtime.use_uvloop),
        shutdown_timeout_ms=runtime.force_shutdown_timeout_ms,
    )


__all__ = [
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "ErrorSink",
    "StreamingRunner",
]
