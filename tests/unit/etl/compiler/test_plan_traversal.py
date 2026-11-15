"""Unit tests for compiler._plan traversal and map helpers."""

from __future__ import annotations

from typing import Any

from loom.etl.compiler import (
    ETLCompiler,
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_processes,
    iter_steps_in_process,
)
from loom.etl.compiler._filter import _map_pipeline_nodes, _map_process_nodes
from loom.etl.compiler._plan import visit_pipeline_nodes, visit_process_nodes
from loom.etl.declarative.source import FromTable
from loom.etl.declarative.target import IntoTable
from loom.etl.pipeline._pipeline import ETLPipeline, ETLProcess
from loom.etl.pipeline._step import ETLStep
from tests.unit.etl.doubles import (
    OrdersStagingAStep,
    OrdersStagingBStep,
    OrdersStagingCStep,
    RunDateIgnoredParams,
)


class StepD(ETLStep[RunDateIgnoredParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.d").replace()

    def execute(self, params: RunDateIgnoredParams, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class ProcWithParallelSteps(ETLProcess[RunDateIgnoredParams]):
    steps = [OrdersStagingAStep, [OrdersStagingBStep, OrdersStagingCStep]]


class ProcD(ETLProcess[RunDateIgnoredParams]):
    steps = [StepD]


class ProcCOnly(ETLProcess[RunDateIgnoredParams]):
    steps = [OrdersStagingCStep]


class PipelineMixed(ETLPipeline[RunDateIgnoredParams]):
    processes = [ProcWithParallelSteps, [ProcD, ProcCOnly]]


def _compile(plan_type: type[ETLPipeline[RunDateIgnoredParams]]) -> PipelinePlan:
    return ETLCompiler().compile(plan_type)


class TestPlanTraversal:
    def test_iter_processes_flattens_parallel_groups_in_order(self) -> None:
        plan = _compile(PipelineMixed)
        names = tuple(proc.process_type.__name__ for proc in iter_processes(plan))
        assert names == ("ProcWithParallelSteps", "ProcD", "ProcCOnly")

    def test_iter_steps_in_process_flattens_parallel_steps_in_order(self) -> None:
        plan = _compile(PipelineMixed)
        proc0 = plan.nodes[0]
        assert isinstance(proc0, ProcessPlan)
        names = tuple(step.step_type.__name__ for step in iter_steps_in_process(proc0))
        assert names == ("OrdersStagingAStep", "OrdersStagingBStep", "OrdersStagingCStep")

    def test_iter_all_steps_walks_whole_pipeline(self) -> None:
        plan = _compile(PipelineMixed)
        names = tuple(step.step_type.__name__ for step in iter_all_steps(plan))
        assert names == (
            "OrdersStagingAStep",
            "OrdersStagingBStep",
            "OrdersStagingCStep",
            "StepD",
            "OrdersStagingCStep",
        )

    def test_map_process_nodes_filters_and_collapses_single_step_group(self) -> None:
        plan = _compile(PipelineMixed)
        proc0 = plan.nodes[0]
        assert isinstance(proc0, ProcessPlan)

        mapped = _map_process_nodes(
            proc0.nodes,
            lambda step: step if step.step_type.__name__ in {"OrdersStagingCStep"} else None,
        )

        assert len(mapped) == 1
        assert isinstance(mapped[0], StepPlan)
        assert mapped[0].step_type is OrdersStagingCStep

    def test_map_process_nodes_keeps_parallel_group_when_multiple_steps_survive(self) -> None:
        plan = _compile(PipelineMixed)
        proc0 = plan.nodes[0]
        assert isinstance(proc0, ProcessPlan)

        mapped = _map_process_nodes(
            proc0.nodes,
            lambda step: (
                step
                if step.step_type.__name__ in {"OrdersStagingBStep", "OrdersStagingCStep"}
                else None
            ),
        )

        assert len(mapped) == 1
        assert isinstance(mapped[0], ParallelStepGroup)
        assert tuple(step.step_type.__name__ for step in mapped[0].plans) == (
            "OrdersStagingBStep",
            "OrdersStagingCStep",
        )

    def test_map_pipeline_nodes_filters_and_collapses_single_process_group(self) -> None:
        plan = _compile(PipelineMixed)

        mapped = _map_pipeline_nodes(
            plan.nodes,
            lambda proc: proc if proc.process_type.__name__ == "ProcCOnly" else None,
        )

        assert len(mapped) == 1
        assert isinstance(mapped[0], ProcessPlan)
        assert mapped[0].process_type is ProcCOnly

    def test_map_pipeline_nodes_keeps_parallel_group_when_multiple_processes_survive(self) -> None:
        plan = _compile(PipelineMixed)

        mapped = _map_pipeline_nodes(
            plan.nodes,
            lambda proc: proc if proc.process_type.__name__ in {"ProcD", "ProcCOnly"} else None,
        )

        assert len(mapped) == 1
        assert isinstance(mapped[0], ParallelProcessGroup)
        assert tuple(proc.process_type.__name__ for proc in mapped[0].plans) == (
            "ProcD",
            "ProcCOnly",
        )

    def test_visit_pipeline_nodes_flattens_without_parallel_handler(self) -> None:
        plan = _compile(PipelineMixed)
        visited: list[str] = []
        visit_pipeline_nodes(plan.nodes, lambda proc: visited.append(proc.process_type.__name__))
        assert visited == ["ProcWithParallelSteps", "ProcD", "ProcCOnly"]

    def test_visit_pipeline_nodes_uses_parallel_handler_when_provided(self) -> None:
        plan = _compile(PipelineMixed)
        visited: list[str] = []
        groups: list[tuple[str, ...]] = []
        visit_pipeline_nodes(
            plan.nodes,
            lambda proc: visited.append(proc.process_type.__name__),
            on_parallel_group=lambda plans: groups.append(
                tuple(proc.process_type.__name__ for proc in plans)
            ),
        )
        assert visited == ["ProcWithParallelSteps"]
        assert groups == [("ProcD", "ProcCOnly")]

    def test_visit_process_nodes_flattens_without_parallel_handler(self) -> None:
        plan = _compile(PipelineMixed)
        proc0 = plan.nodes[0]
        assert isinstance(proc0, ProcessPlan)
        visited: list[str] = []
        visit_process_nodes(proc0.nodes, lambda step: visited.append(step.step_type.__name__))
        assert visited == ["OrdersStagingAStep", "OrdersStagingBStep", "OrdersStagingCStep"]

    def test_visit_process_nodes_uses_parallel_handler_when_provided(self) -> None:
        plan = _compile(PipelineMixed)
        proc0 = plan.nodes[0]
        assert isinstance(proc0, ProcessPlan)
        visited: list[str] = []
        groups: list[tuple[str, ...]] = []
        visit_process_nodes(
            proc0.nodes,
            lambda step: visited.append(step.step_type.__name__),
            on_parallel_group=lambda plans: groups.append(
                tuple(step.step_type.__name__ for step in plans)
            ),
        )
        assert visited == ["OrdersStagingAStep"]
        assert groups == [("OrdersStagingBStep", "OrdersStagingCStep")]

    def test_iter_all_steps_is_iter_steps_in_process_over_iter_processes(self) -> None:
        plan = ETLCompiler().compile(PipelineMixed)
        nested = [step for proc in iter_processes(plan) for step in iter_steps_in_process(proc)]
        assert list(iter_all_steps(plan)) == nested
