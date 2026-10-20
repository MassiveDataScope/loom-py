"""Tests for ETLRunner plan filtering and constructors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import msgspec
import pytest

from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.runner import SupportsFlush
from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler, InvalidStageError, filter_plan, iter_all_steps
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    ProcessPlan,
    StepPlan,
)
from loom.etl.runner import ETLRunner
from loom.etl.storage._config import StorageConfig, StorageDefaults, TablePathConfig, TempConfig
from loom.etl.testing import StubSourceReader, StubTargetWriter
from tests.helpers.version_limited_extras import require_pyspark

RunnerFactory = Callable[..., ETLRunner]


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


class ProcAB(ETLProcess[P]):
    steps = [StepA, StepB]


class ProcC(ETLProcess[P]):
    steps = [StepC]


class ProcParallel(ETLProcess[P]):
    steps = [[StepA, StepB]]


class PipelineAll(ETLPipeline[P]):
    processes = [ProcAB, ProcC]


class PipelineParallelProcs(ETLPipeline[P]):
    processes = [[ProcAB, ProcC]]


class PipelineParallelSteps(ETLPipeline[P]):
    processes = [ProcParallel]


@pytest.fixture
def params() -> P:
    return P(run_date=date(2024, 1, 1))


@pytest.fixture
def runner_factory() -> RunnerFactory:
    def _make(observers: Sequence[Any] | None = None) -> ETLRunner:
        reader = StubSourceReader({"raw.orders": object()})
        return ETLRunner(
            reader,
            StubTargetWriter(),
            observability=ObservabilityRuntime(list(observers or ())),
        )

    return _make


def _compile(pipeline_type: type[ETLPipeline[P]]) -> PipelinePlan:
    return ETLCompiler().compile(pipeline_type)


class TestFilterPlan:
    @pytest.mark.parametrize(
        "include,expected_process,expected_steps",
        [
            (frozenset({"StepA"}), ProcAB, ("StepA",)),
            (frozenset({"StepA", "StepB"}), ProcAB, ("StepA", "StepB")),
            (frozenset({"ProcAB"}), ProcAB, ("StepA", "StepB")),
            (frozenset({"ProcC"}), ProcC, ("StepC",)),
        ],
    )
    def test_filter_regular_pipeline(
        self,
        include: frozenset[str],
        expected_process: type[ETLProcess[P]],
        expected_steps: tuple[str, ...],
    ) -> None:
        filtered = filter_plan(_compile(PipelineAll), include)
        assert len(filtered.nodes) == 1
        proc = filtered.nodes[0]
        assert isinstance(proc, ProcessPlan)
        assert proc.process_type is expected_process
        step_names = tuple(
            node.step_type.__name__ for node in proc.nodes if isinstance(node, StepPlan)
        )
        assert step_names == expected_steps

    @pytest.mark.parametrize(
        "pipeline_type,include,expect_parallel,expected_len,first_step",
        [
            (PipelineParallelSteps, frozenset({"StepA"}), False, 1, "StepA"),
            (PipelineParallelSteps, frozenset({"StepA", "StepB"}), True, 2, None),
            (PipelineParallelProcs, frozenset({"ProcC"}), False, 1, None),
            (PipelineParallelProcs, frozenset({"ProcAB", "ProcC"}), True, 2, None),
        ],
    )
    def test_filter_parallel_shapes(
        self,
        pipeline_type: type[ETLPipeline[P]],
        include: frozenset[str],
        expect_parallel: bool,
        expected_len: int,
        first_step: str | None,
    ) -> None:
        filtered = filter_plan(_compile(pipeline_type), include)
        top_node = filtered.nodes[0]

        if pipeline_type is PipelineParallelSteps:
            assert isinstance(top_node, ProcessPlan)
            node = top_node.nodes[0]
            if expect_parallel:
                assert isinstance(node, ParallelStepGroup)
                assert len(node.plans) == expected_len
            else:
                assert isinstance(node, StepPlan)
                assert node.step_type.__name__ == first_step
            return

        if expect_parallel:
            assert isinstance(top_node, ParallelProcessGroup)
            assert len(top_node.plans) == expected_len
            return

        assert isinstance(top_node, ProcessPlan)
        assert top_node.process_type is ProcC

    def test_filter_no_match_raises(self) -> None:
        plan = _compile(PipelineAll)
        stages = frozenset({"NonExistent"})
        with pytest.raises(InvalidStageError):
            filter_plan(plan, stages)

    def test_filter_reports_every_unknown_name_in_sorted_order(self) -> None:
        plan = _compile(PipelineAll)
        with pytest.raises(InvalidStageError) as exc_info:
            filter_plan(plan, ["StepA", "Phantom", "Ghost"])
        assert exc_info.value.unknown == frozenset({"Ghost", "Phantom"})
        assert "names: ['Ghost', 'Phantom']" in str(exc_info.value)

    def test_filter_refuses_a_bare_string(self) -> None:
        with pytest.raises(TypeError, match=r"not the string 'StepA'.*include=\{'StepA'\}"):
            filter_plan(_compile(PipelineAll), "StepA")

    def test_filter_accepts_any_iterable_of_names(self) -> None:
        plan = _compile(PipelineAll)
        filtered = filter_plan(plan, (name for name in ["StepB", "StepC"]))
        assert [step.step_type.__name__ for step in iter_all_steps(filtered)] == [
            "StepB",
            "StepC",
        ]

    def test_filter_returns_a_new_plan_and_leaves_the_input_untouched(self) -> None:
        plan = _compile(PipelineAll)
        filtered = filter_plan(plan, {"StepC"})
        assert filtered is not plan
        assert filtered.pipeline_type is plan.pipeline_type
        assert filtered.params_type is plan.params_type
        assert [step.step_type.__name__ for step in iter_all_steps(plan)] == [
            "StepA",
            "StepB",
            "StepC",
        ]

    def test_filter_with_an_empty_include_keeps_nothing(self) -> None:
        assert filter_plan(_compile(PipelineAll), ()).nodes == ()


class TestRunnerRun:
    @pytest.mark.parametrize(
        "include,expected_steps",
        [
            (None, 3),
            (["StepA"], 1),
        ],
    )
    def test_runner_run_counts_steps(
        self,
        params: P,
        runner_factory: RunnerFactory,
        include: list[str] | None,
        expected_steps: int,
    ) -> None:
        from loom.etl.testing import StubRunObserver

        observer = StubRunObserver()
        runner_factory([observer]).run(PipelineAll, params, include=include)
        assert len(observer.step_statuses) == expected_steps

    def test_runner_run_invalid_include_raises(
        self,
        params: P,
        runner_factory: RunnerFactory,
    ) -> None:
        factory = runner_factory()
        with pytest.raises(InvalidStageError):
            factory.run(PipelineAll, params, include=["DoesNotExist"])

    def test_runner_run_flushes_prometheus_observers_after_success(
        self,
        params: P,
    ) -> None:
        from loom.core.observability.event import LifecycleEvent

        class _PrometheusSpy:
            def __init__(self) -> None:
                self.events: list[LifecycleEvent] = []
                self.flush_count = 0

            def on_event(self, event: LifecycleEvent) -> None:
                self.events.append(event)

            def flush(self) -> None:
                self.flush_count += 1

        spy = _PrometheusSpy()
        runner = ETLRunner(
            StubSourceReader({"raw.orders": object()}),
            StubTargetWriter(),
            observability=ObservabilityRuntime([spy]),
        )

        assert isinstance(runner, SupportsFlush)
        runner.run(PipelineAll, params)

        assert spy.flush_count == 1
        assert any(event.scope.name == "PIPELINE" for event in spy.events)

    def test_runner_run_flushes_prometheus_observers_after_failure(
        self,
        params: P,
    ) -> None:
        from loom.core.observability.event import LifecycleEvent

        class _PrometheusSpy:
            def __init__(self) -> None:
                self.events: list[LifecycleEvent] = []
                self.flush_count = 0

            def on_event(self, event: LifecycleEvent) -> None:
                self.events.append(event)

            def flush(self) -> None:
                self.flush_count += 1

        class FailingStep(ETLStep[P]):
            orders = FromTable("raw.orders")
            target = IntoTable("staging.fail").replace()

            def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
                raise RuntimeError("boom")

        class FailingProcess(ETLProcess[P]):
            steps = [FailingStep]

        class FailingPipeline(ETLPipeline[P]):
            processes = [FailingProcess]

        spy = _PrometheusSpy()
        runner = ETLRunner(
            StubSourceReader({"raw.orders": object()}),
            StubTargetWriter(),
            observability=ObservabilityRuntime([spy]),
        )

        with pytest.raises(RuntimeError, match="boom"):
            runner.run(FailingPipeline, params)

        assert spy.flush_count == 1
        assert any(event.scope.name == "PIPELINE" for event in spy.events)


class TestRunnerFromConfig:
    def test_from_config_spark_engine_without_spark_raises(self) -> None:
        config = StorageConfig(engine="spark")
        with pytest.raises(ValueError, match="SparkSession"):
            ETLRunner.from_config(config)

    def test_from_config_spark_engine_with_spark_builds_spark_backends(self) -> None:
        require_pyspark()

        from unittest.mock import MagicMock

        from loom.etl.backends.spark import SparkSourceReader

        spark = MagicMock()
        runner = ETLRunner.from_config(StorageConfig(engine="spark"), spark=spark)

        assert isinstance(runner._executor._reader, SparkSourceReader)
        assert runner._compiler._catalog is None

    @pytest.mark.parametrize(
        "checkpoint_enabled,has_checkpoint_store",
        [
            (False, False),
            (True, True),
        ],
    )
    def test_from_config_polars_path_builds_polars_backends(
        self,
        tmp_path: Path,
        checkpoint_enabled: bool,
        has_checkpoint_store: bool,
    ) -> None:
        from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            temp=TempConfig(root="s3://bucket/checkpoints" if checkpoint_enabled else ""),
        )
        runner = ETLRunner.from_config(config)

        assert isinstance(runner._executor._reader, PolarsSourceReader)
        assert isinstance(runner._executor._writer, PolarsTargetWriter)
        assert (runner._checkpoint_store is not None) is has_checkpoint_store

    def test_from_config_with_clickhouse_url_wires_client_executor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from unittest.mock import MagicMock

        from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor
        from loom.etl.storage._config import ClickHouseConfig

        monkeypatch.setattr("loom.etl.io.targets._clickhouse._clickhouse_connect", MagicMock())

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            clickhouse=ClickHouseConfig(url="clickhouse://user:pass@host:8123/db"),
        )
        runner = ETLRunner.from_config(config)

        assert isinstance(runner._executor._client_executor, ClickHouseClientExecutor)

    def test_from_config_without_clickhouse_url_leaves_client_executor_none(
        self, tmp_path: Path
    ) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
        )
        runner = ETLRunner.from_config(config)

        assert runner._executor._client_executor is None


class TestRunnerFromDict:
    def test_from_dict_polars_path_builds_polars_backends(self, tmp_path: Path) -> None:
        from loom.etl.backends.polars import PolarsSourceReader

        runner = ETLRunner.from_dict({"defaults": {"table_path": {"uri": str(tmp_path)}}})
        assert isinstance(runner._executor._reader, PolarsSourceReader)

    @pytest.mark.parametrize(
        "storage,error,match",
        [
            ({"engine": "spark"}, ValueError, "SparkSession"),
            ({"engine": "unknown"}, msgspec.ValidationError, ""),
        ],
    )
    def test_from_dict_errors(
        self, storage: dict[str, Any], error: type[Exception], match: str
    ) -> None:
        if match:
            with pytest.raises(error, match=match):
                ETLRunner.from_dict(storage)
            return
        with pytest.raises(error):
            ETLRunner.from_dict(storage)

    def test_from_dict_invalid_storage_shape_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            ETLRunner.from_dict({"defaults": {"table_path": {"uri": 123}}})

    def test_from_dict_with_observability_dict(self, tmp_path: Path) -> None:
        runner = ETLRunner.from_dict(
            {"defaults": {"table_path": {"uri": str(tmp_path)}}},
            observability={"log": {"enabled": False}},
        )
        assert runner is not None


class TestAuditColumnsEndToEnd:
    """When audit.enabled=True the written frame carries _loom_* columns."""

    def test_audit_columns_written_when_enabled(self, tmp_path: Path) -> None:
        import polars as pl
        from deltalake import write_deltalake

        from loom.etl.storage._config import (
            AuditConfig,
            MissingTablePolicy,
            StorageDefaults,
            TablePathConfig,
        )

        # Seed source table
        src_path = tmp_path / "raw" / "orders"
        src_path.mkdir(parents=True)
        write_deltalake(str(src_path), pl.DataFrame({"id": [1, 2]}), mode="overwrite")

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            missing_table_policy=MissingTablePolicy.CREATE,
            audit=AuditConfig(enabled=True),
        )
        runner = ETLRunner.from_config(config)

        class _P(ETLParams):
            pass

        class _Step(ETLStep[_P]):
            orders = FromTable("raw.orders")
            target = IntoTable("staging.out").replace()

            def execute(self, params: _P, *, orders: Any) -> Any:  # type: ignore[override]
                return orders

        class _Proc(ETLProcess[_P]):
            steps = [_Step]

        class _Pipeline(ETLPipeline[_P]):
            processes = [_Proc]

        runner.run(_Pipeline, _P())

        result = pl.scan_delta(str(tmp_path / "staging" / "out")).collect()
        assert "_loom_run_id" in result.columns
        assert "_loom_step" in result.columns
        assert "_loom_attempt" in result.columns

    def test_audit_columns_not_written_when_disabled(self, tmp_path: Path) -> None:
        import polars as pl
        from deltalake import write_deltalake

        from loom.etl.storage._config import (
            AuditConfig,
            MissingTablePolicy,
            StorageDefaults,
            TablePathConfig,
        )

        src_path = tmp_path / "raw" / "orders"
        src_path.mkdir(parents=True)
        write_deltalake(str(src_path), pl.DataFrame({"id": [1]}), mode="overwrite")

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            missing_table_policy=MissingTablePolicy.CREATE,
            audit=AuditConfig(enabled=False),
        )
        runner = ETLRunner.from_config(config)

        class _P(ETLParams):
            pass

        class _Step(ETLStep[_P]):
            orders = FromTable("raw.orders")
            target = IntoTable("staging.no_audit").replace()

            def execute(self, params: _P, *, orders: Any) -> Any:  # type: ignore[override]
                return orders

        class _Proc(ETLProcess[_P]):
            steps = [_Step]

        class _Pipeline(ETLPipeline[_P]):
            processes = [_Proc]

        runner.run(_Pipeline, _P())

        result = pl.scan_delta(str(tmp_path / "staging" / "no_audit")).collect()
        assert "_loom_run_id" not in result.columns


class TestRunnerCleaner:
    def test_from_config_with_injected_cleaner(self, tmp_path: Path) -> None:
        deleted: list[str] = []

        class SpyCleaner:
            def delete_tree(self, path: str) -> None:
                deleted.append(path)

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            temp=TempConfig(root="s3://bucket/checkpoints"),
        )
        runner = ETLRunner.from_config(config, cleaner=SpyCleaner())

        assert runner._checkpoint_store is not None
        runner._checkpoint_store.cleanup_run("run-xyz")
        assert any("run-xyz" in path for path in deleted)

    def test_run_cleans_up_run_scope_checkpoints_on_success(self, tmp_path: Path) -> None:
        import polars as pl
        from deltalake import write_deltalake

        from loom.etl.storage._config import MissingTablePolicy

        deleted: list[str] = []

        class SpyCleaner:
            def delete_tree(self, path: str) -> None:
                deleted.append(path)

        src = tmp_path / "raw" / "items"
        src.mkdir(parents=True)
        write_deltalake(str(src), pl.DataFrame({"id": [1, 2]}), mode="overwrite")

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            missing_table_policy=MissingTablePolicy.CREATE,
            temp=TempConfig(root="s3://fake-bucket/checkpoints"),
        )
        runner = ETLRunner.from_config(config, cleaner=SpyCleaner())

        class _P(ETLParams):
            pass

        class _Step(ETLStep[_P]):
            items = FromTable("raw.items")
            target = IntoTable("staging.out").replace()

            def execute(self, params: _P, *, items: Any) -> Any:  # type: ignore[override]
                return items

        class _Proc(ETLProcess[_P]):
            steps = [_Step]

        class _Pipeline(ETLPipeline[_P]):
            processes = [_Proc]

        runner.run(_Pipeline, _P(), run_id="test-run-abc")

        assert any("test-run-abc" in path for path in deleted)

    def test_run_cleans_up_run_scope_checkpoints_on_failure(self, tmp_path: Path) -> None:
        import polars as pl
        from deltalake import write_deltalake

        from loom.etl.storage._config import MissingTablePolicy

        deleted: list[str] = []

        class SpyCleaner:
            def delete_tree(self, path: str) -> None:
                deleted.append(path)

        src = tmp_path / "raw" / "items"
        src.mkdir(parents=True)
        write_deltalake(str(src), pl.DataFrame({"id": [1]}), mode="overwrite")

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            missing_table_policy=MissingTablePolicy.CREATE,
            temp=TempConfig(root="s3://fake-bucket/checkpoints"),
        )
        runner = ETLRunner.from_config(config, cleaner=SpyCleaner())

        class _P(ETLParams):
            pass

        class _FailStep(ETLStep[_P]):
            items = FromTable("raw.items")
            target = IntoTable("staging.fail").replace()

            def execute(self, params: _P, *, items: Any) -> Any:  # type: ignore[override]
                raise RuntimeError("intentional failure")

        class _Proc(ETLProcess[_P]):
            steps = [_FailStep]

        class _Pipeline(ETLPipeline[_P]):
            processes = [_Proc]

        params = _P()
        with pytest.raises(RuntimeError, match="intentional failure"):
            runner.run(_Pipeline, params, run_id="fail-run-xyz")

        assert any("fail-run-xyz" in path for path in deleted)
