"""ETL compiler — static validation and plan construction.

The compiler inspects ETL class declarations once at startup and produces
immutable :class:`~loom.etl.compiler._plan.PipelinePlan` /
:class:`~loom.etl.compiler._plan.ProcessPlan` /
:class:`~loom.etl.compiler._plan.StepPlan` objects.  No reflection occurs
after compilation.

Static validation
-----------------
* ``ETLStep[ParamsT]`` generic present
* ``execute()`` first positional param is typed as ``ParamsT``
* every source alias has a matching ``*``-only DataFrame param in ``execute()``
* every ``*``-only param in ``execute()`` has a matching source alias
* every ``FromConfig`` attribute has a matching ``*``-only param in ``execute()``
* ``target`` is declared
* source forms are not mixed (caught earlier in ``__init_subclass__``)
* ``ETLProcess.steps`` entries are valid step types
* ``ETLPipeline.processes`` entries are valid process types
* with a config context, every ``FromConfig`` key resolves and validates
* every ``WithAgent`` has a matching ``*``-only param in ``execute()``
* with a config context or an agent runner, every ``WithAgent`` can be served
"""

from __future__ import annotations

import logging
from typing import Any, cast

from loom.core.config import ConfigContext
from loom.etl.compiler._binding import (
    resolve_agent_bindings,
    resolve_config_bindings,
    resolve_source_bindings,
    resolve_target_binding,
)
from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
)
from loom.etl.compiler._validators import (
    StepCompilationContext,
    validate_params_compat,
    validate_plan_catalog,
    validate_plan_temps,
    validate_process_catalog,
    validate_step,
    validate_step_catalog,
)
from loom.etl.compiler._validators_agents import (
    validate_plan_agents,
    validate_process_agents,
    validate_step_agents,
)
from loom.etl.compiler._validators_config import (
    validate_plan_config,
    validate_process_config,
    validate_step_config,
)
from loom.etl.declarative.target import AppendSpec
from loom.etl.pipeline._pipeline import ETLPipeline
from loom.etl.pipeline._process import ETLProcess
from loom.etl.pipeline._step import ETLStep
from loom.etl.runtime.contracts import AgentBatchRunner, TableDiscovery

_log = logging.getLogger(__name__)


class ETLCompiler:
    """Compiles ETL declarations into immutable execution plans.

    Compilation is idempotent per class — results are cached.  Call
    :meth:`compile` with the top-level :class:`~loom.etl.ETLPipeline`
    subclass to obtain a :class:`~loom.etl.compiler._plan.PipelinePlan`.

    Args:
        catalog: Optional :class:`~loom.etl._io.TableDiscovery` instance.
                 When provided, all ``TABLE`` sources and ``IntoTable`` targets
                 are validated against the catalog at compile time.
        config_context: Optional config the runner was built from.  When
                 provided, every ``FromConfig`` key is resolved and validated
                 at compile time; values are discarded, never stored on a plan.
        agents:  Optional agent runner.  When provided, or when a config
                 context is, every ``WithAgent`` is validated through it at
                 compile time; without one, a declared agent cannot be served.

    Example::

        plan = ETLCompiler().compile(DailyOrdersPipeline)
        plan = ETLCompiler(catalog=HiveCatalog()).compile(DailyOrdersPipeline)
    """

    def __init__(
        self,
        catalog: TableDiscovery | None = None,
        config_context: ConfigContext | None = None,
        agents: AgentBatchRunner | None = None,
    ) -> None:
        self._catalog = catalog
        self._config_context = config_context
        self._agents = agents
        self._step_cache: dict[type[Any], StepPlan] = {}
        self._process_cache: dict[type[Any], ProcessPlan] = {}

    def compile(self, pipeline_type: type[ETLPipeline[Any]]) -> PipelinePlan:
        """Compile a pipeline and return its immutable plan.

        Args:
            pipeline_type: Concrete :class:`~loom.etl.ETLPipeline` subclass.

        Returns:
            Fully validated :class:`~loom.etl.compiler._plan.PipelinePlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated, a
                ``FromConfig`` key does not resolve against the config context,
                or a ``WithAgent`` cannot be served.
        """
        params_type = _require_params_type(pipeline_type, "ETLPipeline")
        _log.debug(
            "compile pipeline=%s processes=%d", pipeline_type.__name__, len(pipeline_type.processes)
        )
        nodes = tuple(
            self._compile_pipeline_item(item, pipeline_type, params_type)
            for item in pipeline_type.processes
        )
        plan = PipelinePlan(pipeline_type=pipeline_type, params_type=params_type, nodes=nodes)
        validate_plan_temps(plan)
        if self._catalog is not None:
            validate_plan_catalog(plan, self._catalog)
        if self._config_context is not None:
            validate_plan_config(plan, self._config_context)
        if self._validates_agents:
            validate_plan_agents(plan, self._agents)
        return plan

    def compile_process(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        """Compile a single process.

        Args:
            process_type: Concrete :class:`~loom.etl.ETLProcess` subclass.

        Returns:
            Validated :class:`~loom.etl.compiler._plan.ProcessPlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated, a
                ``FromConfig`` key does not resolve against the config context,
                or a ``WithAgent`` cannot be served.
        """
        plan = self._get_or_build_process(process_type)
        if self._catalog is not None:
            validate_process_catalog(plan, self._catalog)
        if self._config_context is not None:
            validate_process_config(plan, self._config_context)
        if self._validates_agents:
            validate_process_agents(plan, self._agents)
        return plan

    def compile_step(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        """Compile a single step.

        Args:
            step_type: Concrete :class:`~loom.etl.ETLStep` subclass.

        Returns:
            Validated :class:`~loom.etl.compiler._plan.StepPlan`.

        Raises:
            ETLCompilationError: If any structural constraint is violated, a
                ``FromConfig`` key does not resolve against the config context,
                or a ``WithAgent`` cannot be served.
        """
        plan = self._get_or_build_step(step_type)
        if self._catalog is not None:
            validate_step_catalog(plan, self._catalog)
        if self._config_context is not None:
            validate_step_config(plan, self._config_context)
        if self._validates_agents:
            validate_step_agents(plan, self._agents)
        return plan

    @property
    def _validates_agents(self) -> bool:
        return self._agents is not None or self._config_context is not None

    # ------------------------------------------------------------------
    # Pipeline assembly
    # ------------------------------------------------------------------

    def _compile_pipeline_item(
        self,
        item: Any,
        pipeline_type: type[Any],
        pipeline_params_type: type[Any],
    ) -> PipelineProcessNode:
        if isinstance(item, list):
            return ParallelProcessGroup(
                plans=cast(
                    "tuple[ProcessPlan, ...]",
                    tuple(
                        self._compile_pipeline_item(sub, pipeline_type, pipeline_params_type)
                        for sub in item
                    ),
                )
            )
        if not (isinstance(item, type) and issubclass(item, ETLProcess)):
            raise ETLCompilationError.invalid_process_item(pipeline_type, item)
        plan = self._get_or_build_process(item)
        validate_params_compat(item, plan.params_type, pipeline_params_type)
        return plan

    # ------------------------------------------------------------------
    # Process assembly
    # ------------------------------------------------------------------

    def _build_process_plan(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        params_type = _require_params_type(process_type, "ETLProcess")
        nodes = tuple(
            self._compile_process_item(item, process_type, params_type)
            for item in process_type.steps
        )
        return ProcessPlan(process_type=process_type, params_type=params_type, nodes=nodes)

    def _compile_process_item(
        self,
        item: Any,
        process_type: type[Any],
        process_params_type: type[Any],
    ) -> ProcessStepNode:
        if isinstance(item, list):
            return ParallelStepGroup(
                plans=cast(
                    "tuple[StepPlan, ...]",
                    tuple(
                        self._compile_process_item(sub, process_type, process_params_type)
                        for sub in item
                    ),
                )
            )
        if not (isinstance(item, type) and issubclass(item, ETLStep)):
            raise ETLCompilationError.invalid_step_item(process_type, item)
        plan = self._get_or_build_step(item)
        validate_params_compat(item, plan.params_type, process_params_type)
        return plan

    # ------------------------------------------------------------------
    # Step assembly
    # ------------------------------------------------------------------

    def _build_step_plan(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        params_type = _require_params_type(step_type, "ETLStep")
        source_bindings = resolve_source_bindings(step_type)
        target_binding = resolve_target_binding(step_type)
        config_bindings = resolve_config_bindings(step_type)
        agent_bindings = resolve_agent_bindings(step_type)
        _log.debug(
            "compile step=%s sources=%d",
            step_type.__name__,
            len(source_bindings),
        )
        validate_step(
            StepCompilationContext(
                step_type=step_type,
                params_type=params_type,
                source_bindings=source_bindings,
                target_binding=target_binding,
                config_bindings=config_bindings,
                agent_bindings=agent_bindings,
            )
        )
        _warn_append_target(step_type, target_binding.spec)
        return StepPlan(
            step_type=step_type,
            params_type=params_type,
            source_bindings=source_bindings,
            target_binding=target_binding,
            streaming=step_type.streaming,
            config_bindings=config_bindings,
            agent_bindings=agent_bindings,
        )

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _get_or_build_process(self, process_type: type[ETLProcess[Any]]) -> ProcessPlan:
        if process_type in self._process_cache:
            _log.debug("compile cache hit process=%s", process_type.__name__)
            return self._process_cache[process_type]
        plan = self._build_process_plan(process_type)
        self._process_cache[process_type] = plan
        return plan

    def _get_or_build_step(self, step_type: type[ETLStep[Any]]) -> StepPlan:
        if step_type in self._step_cache:
            _log.debug("compile cache hit step=%s", step_type.__name__)
            return self._step_cache[step_type]
        plan = self._build_step_plan(step_type)
        self._step_cache[step_type] = plan
        return plan


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _require_params_type(cls: type[Any], kind: str) -> type[Any]:
    pt = getattr(cls, "_params_type", None)
    if pt is None:
        raise ETLCompilationError.missing_generic_param(cls, kind)
    return cast(type[Any], pt)


def _warn_append_target(step_type: type[Any], spec: object) -> None:
    if isinstance(spec, AppendSpec):
        _log.warning(
            "compile append target step=%s table=%s schema_mode=%s",
            step_type.__name__,
            spec.table_ref.ref,
            spec.schema_mode.value,
        )
