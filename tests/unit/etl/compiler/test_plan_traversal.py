"""Unit tests for compiler._plan traversal and map helpers."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

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
from loom.etl.pipeline._params import ETLParams
from loom.etl.pipeline._pipeline import ETLPipeline
from loom.etl.pipeline._process import ETLProcess
from loom.etl.pipeline._step import ETLStep


class P(ETLParams):  # type: ignore[misc]
    run_date: date


class StepA(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.a").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepB(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.b").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepC(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.c").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepD(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.d").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class ProcWithParallelSteps(ETLProcess[P]):
    steps = [StepA, [StepB, StepC]]


class ProcD(ETLProcess[P]):
    steps = [StepD]


class ProcCOnly(ETLProcess[P]):
    steps = [StepC]


class PipelineMixed(ETLPipeline[P]):
    processes = [ProcWithParallelSteps, [ProcD, ProcCOnly]]


def _compile(plan_type: type[ETLPipeline[P]]) -> PipelinePlan:
    return ETLCompiler().compile(plan_type)


def test_iter_processes_flattens_parallel_groups_in_order() -> None:
    plan = _compile(PipelineMixed)
    names = tuple(proc.process_type.__name__ for proc in iter_processes(plan))
    assert names == ("ProcWithParallelSteps", "ProcD", "ProcCOnly")


def test_iter_steps_in_process_flattens_parallel_steps_in_order() -> None:
    plan = _compile(PipelineMixed)
    proc0 = plan.nodes[0]
    assert isinstance(proc0, ProcessPlan)
    names = tuple(step.step_type.__name__ for step in iter_steps_in_process(proc0))
    assert names == ("StepA", "StepB", "StepC")


def test_iter_all_steps_walks_whole_pipeline() -> None:
    plan = _compile(PipelineMixed)
    names = tuple(step.step_type.__name__ for step in iter_all_steps(plan))
    assert names == ("StepA", "StepB", "StepC", "StepD", "StepC")


def test_map_process_nodes_filters_and_collapses_single_step_group() -> None:
    plan = _compile(PipelineMixed)
    proc0 = plan.nodes[0]
    assert isinstance(proc0, ProcessPlan)

    mapped = _map_process_nodes(
        proc0.nodes,
        lambda step: step if step.step_type.__name__ in {"StepC"} else None,
    )

    assert len(mapped) == 1
    assert isinstance(mapped[0], StepPlan)
    assert mapped[0].step_type is StepC


def test_map_process_nodes_keeps_parallel_group_when_multiple_steps_survive() -> None:
    plan = _compile(PipelineMixed)
    proc0 = plan.nodes[0]
    assert isinstance(proc0, ProcessPlan)

    mapped = _map_process_nodes(
        proc0.nodes,
        lambda step: step if step.step_type.__name__ in {"StepB", "StepC"} else None,
    )

    assert len(mapped) == 1
    assert isinstance(mapped[0], ParallelStepGroup)
    assert tuple(step.step_type.__name__ for step in mapped[0].plans) == ("StepB", "StepC")


def test_map_pipeline_nodes_filters_and_collapses_single_process_group() -> None:
    plan = _compile(PipelineMixed)

    mapped = _map_pipeline_nodes(
        plan.nodes,
        lambda proc: proc if proc.process_type.__name__ == "ProcCOnly" else None,
    )

    assert len(mapped) == 1
    assert isinstance(mapped[0], ProcessPlan)
    assert mapped[0].process_type is ProcCOnly


def test_map_pipeline_nodes_keeps_parallel_group_when_multiple_processes_survive() -> None:
    plan = _compile(PipelineMixed)

    mapped = _map_pipeline_nodes(
        plan.nodes,
        lambda proc: proc if proc.process_type.__name__ in {"ProcD", "ProcCOnly"} else None,
    )

    assert len(mapped) == 1
    assert isinstance(mapped[0], ParallelProcessGroup)
    assert tuple(proc.process_type.__name__ for proc in mapped[0].plans) == ("ProcD", "ProcCOnly")


def test_visit_pipeline_nodes_flattens_without_parallel_handler() -> None:
    plan = _compile(PipelineMixed)
    visited: list[str] = []
    visit_pipeline_nodes(plan.nodes, lambda proc: visited.append(proc.process_type.__name__))
    assert visited == ["ProcWithParallelSteps", "ProcD", "ProcCOnly"]


def test_visit_pipeline_nodes_uses_parallel_handler_when_provided() -> None:
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


def test_visit_process_nodes_flattens_without_parallel_handler() -> None:
    plan = _compile(PipelineMixed)
    proc0 = plan.nodes[0]
    assert isinstance(proc0, ProcessPlan)
    visited: list[str] = []
    visit_process_nodes(proc0.nodes, lambda step: visited.append(step.step_type.__name__))
    assert visited == ["StepA", "StepB", "StepC"]


def test_visit_process_nodes_uses_parallel_handler_when_provided() -> None:
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
    assert visited == ["StepA"]
    assert groups == [("StepB", "StepC")]


def test_iter_all_steps_is_iter_steps_in_process_over_iter_processes() -> None:
    plan = ETLCompiler().compile(PipelineMixed)
    nested = [step for proc in iter_processes(plan) for step in iter_steps_in_process(proc)]
    assert list(iter_all_steps(plan)) == nested


def test_map_pipeline_nodes_refuses_a_node_that_is_neither_a_process_nor_a_group() -> None:
    with pytest.raises(TypeError, match="pipeline node"):
        _map_pipeline_nodes((object(),), lambda proc: proc)  # type: ignore[arg-type]


def test_map_process_nodes_refuses_a_node_that_is_neither_a_step_nor_a_group() -> None:
    with pytest.raises(TypeError, match="process node"):
        _map_process_nodes((object(),), lambda step: step)  # type: ignore[arg-type]
