"""Tests for ETLProcess / ETLPipeline class-level validation and generics."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLPipeline, ETLProcess, ETLStep, IntoTable
from loom.etl.pipeline._generics import _extract_generic_arg
from tests.unit.etl.doubles import RunDateETLParams


class OkNoSourceStep(ETLStep[RunDateETLParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams) -> Any:
        return {"ok": True}


class TestProcessAndPipelineTypes:
    def test_process_extracts_generic_params_type(self) -> None:
        class _Process(ETLProcess[RunDateETLParams]):
            steps = [OkNoSourceStep]

        assert _Process._params_type is RunDateETLParams
        assert _extract_generic_arg(_Process, ETLProcess) is RunDateETLParams
        assert ETLProcess._params_type is None

    def test_pipeline_extracts_generic_params_type(self) -> None:
        class _Process(ETLProcess[RunDateETLParams]):
            steps = [OkNoSourceStep]

        class _Pipeline(ETLPipeline[RunDateETLParams]):
            processes = [_Process]

        assert _Pipeline._params_type is RunDateETLParams
        assert _extract_generic_arg(_Pipeline, ETLPipeline) is RunDateETLParams
        assert ETLPipeline._params_type is None

    def test_process_steps_must_be_a_list(self) -> None:
        with pytest.raises(TypeError, match="'steps' must be a list"):

            class _BadProcess(ETLProcess[RunDateETLParams]):  # NOSONAR
                steps = (OkNoSourceStep,)

    def test_pipeline_processes_must_be_a_list(self) -> None:
        class _Process(ETLProcess[RunDateETLParams]):
            steps = [OkNoSourceStep]

        with pytest.raises(TypeError, match="'processes' must be a list"):

            class _BadPipeline(ETLPipeline[RunDateETLParams]):  # NOSONAR
                processes = (_Process,)

    def test_extract_generic_arg_returns_none_without_matching_origin(self) -> None:
        class _Plain:
            pass

        assert _extract_generic_arg(_Plain, ETLProcess) is None
