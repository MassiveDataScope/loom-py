"""Plan filtering: keep the steps and processes a run selects by name.

Internal module — import :func:`filter_plan` and :class:`InvalidStageError`
from :mod:`loom.etl.compiler`.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
    iter_all_steps,
    iter_processes,
)

_Plan = TypeVar("_Plan", ProcessPlan, StepPlan)
_Group = TypeVar("_Group", ParallelProcessGroup, ParallelStepGroup)


class InvalidStageError(ValueError):
    """Raised when a selection names a step or process absent from the compiled plan.

    Args:
        unknown: The selected names that match no step or process.
        include: Deprecated keyword spelling of *unknown*, removed in loom 3.0.

    Attributes:
        unknown: The selected names that match no step or process.

    Raises:
        TypeError: When both or neither of *unknown* and *include* are given.
    """

    def __init__(
        self,
        unknown: frozenset[str] | None = None,
        *,
        include: frozenset[str] | None = None,
    ) -> None:
        self.unknown = _unknown_names(unknown, include)
        super().__init__(
            f"Unknown step or process names: {sorted(self.unknown)!r}. "
            "Check that the names match the class names of your ETLStep or ETLProcess subclasses."
        )

    def __reduce__(self) -> tuple[type[InvalidStageError], tuple[frozenset[str]], dict[str, Any]]:
        return type(self), (self.unknown,), self.__dict__


def _unknown_names(
    unknown: frozenset[str] | None, include: frozenset[str] | None
) -> frozenset[str]:
    if include is None:
        if unknown is None:
            raise TypeError("InvalidStageError() requires the unknown names.")
        return frozenset(unknown)
    if unknown is not None:
        raise TypeError("InvalidStageError() takes unknown= or the deprecated include=, not both.")
    warnings.warn(
        "InvalidStageError(include=...) is deprecated and will be removed in loom 3.0; "
        "use InvalidStageError(unknown=...) instead.",
        DeprecationWarning,
        stacklevel=3,
    )
    return frozenset(include)


def filter_plan(plan: PipelinePlan, include: Iterable[str]) -> PipelinePlan:
    """Return a new plan keeping only the steps and processes named in *include*.

    This is the selection :meth:`~loom.etl.runner.ETLRunner.run` applies for
    ``include=``. A process name keeps the whole process; a step name keeps
    that step inside its process. A process left with no step is dropped, and
    a parallel group left with one member becomes that member. Execution
    order and the remaining parallel groups are preserved.

    Args:
        plan: Compiled pipeline plan, as returned by
            :meth:`~loom.etl.compiler.ETLCompiler.compile`.
        include: Class names of steps or processes to keep. A bare string is
            refused rather than read as its characters.

    Returns:
        A new :class:`PipelinePlan`; *plan* is not modified.

    Raises:
        TypeError: When *include* is a ``str``.
        InvalidStageError: When a name in *include* is neither a step nor a
            process of *plan*.

    Example::

        from loom.etl.compiler import ETLCompiler, filter_plan, iter_all_steps

        plan = filter_plan(ETLCompiler().compile(MyPipeline), {"LoadStep"})
        [step.step_type.__name__ for step in iter_all_steps(plan)]  # ["LoadStep"]
    """
    if isinstance(include, str):
        raise TypeError(
            f"filter_plan() takes an iterable of names, not the string {include!r}; "
            f"wrap it, e.g. include={{{include!r}}}."
        )
    names = frozenset(include)
    unknown = names - _collect_all_names(plan)
    if unknown:
        raise InvalidStageError(unknown)
    nodes = _map_pipeline_nodes(plan.nodes, lambda proc: _filter_process(proc, names))
    return PipelinePlan(
        pipeline_type=plan.pipeline_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_process(plan: ProcessPlan, include: frozenset[str]) -> ProcessPlan | None:
    if plan.process_type.__name__ in include:
        return plan
    nodes = _map_process_nodes(
        plan.nodes,
        lambda step: step if step.step_type.__name__ in include else None,
    )
    if not nodes:
        return None
    return ProcessPlan(
        process_type=plan.process_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _collect_all_names(plan: PipelinePlan) -> frozenset[str]:
    """Collect every step and process class name reachable from *plan*."""
    names = {proc.process_type.__name__ for proc in iter_processes(plan)}
    names.update(step.step_type.__name__ for step in iter_all_steps(plan))
    return frozenset(names)


def _map_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    fn: Callable[[ProcessPlan], ProcessPlan | None],
) -> tuple[PipelineProcessNode, ...]:
    """Map/filter process nodes while preserving parallel-group shape."""
    mapped = (_map_members(_processes_of(node), fn, ParallelProcessGroup) for node in nodes)
    return tuple(node for node in mapped if node is not None)


def _map_process_nodes(
    nodes: tuple[ProcessStepNode, ...],
    fn: Callable[[StepPlan], StepPlan | None],
) -> tuple[ProcessStepNode, ...]:
    """Map/filter step nodes while preserving parallel-group shape."""
    mapped = (_map_members(_steps_of(node), fn, ParallelStepGroup) for node in nodes)
    return tuple(node for node in mapped if node is not None)


def _processes_of(node: PipelineProcessNode) -> tuple[ProcessPlan, ...]:
    match node:
        case ProcessPlan():
            return (node,)
        case ParallelProcessGroup(plans=plans):
            return plans
        case _:
            raise TypeError(f"not a pipeline node: {node!r}")


def _steps_of(node: ProcessStepNode) -> tuple[StepPlan, ...]:
    match node:
        case StepPlan():
            return (node,)
        case ParallelStepGroup(plans=plans):
            return plans
        case _:
            raise TypeError(f"not a process node: {node!r}")


def _map_members(
    members: tuple[_Plan, ...],
    fn: Callable[[_Plan], _Plan | None],
    group: Callable[[tuple[_Plan, ...]], _Group],
) -> _Plan | _Group | None:
    """Map *members*; a single survivor stands alone, several form a *group*."""
    kept = tuple(plan for plan in map(fn, members) if plan is not None)
    if len(kept) == 1:
        return kept[0]
    return group(kept) if kept else None


__all__ = ["InvalidStageError", "filter_plan"]
