"""Tests for ETLScenario/StepRunnerProto and in-memory stubs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target._table import ReplaceSpec
from loom.etl.lineage._records import EventName, RunContext, RunStatus
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.testing._scenario import ETLScenario, StepRunnerProto
from loom.etl.testing._stubs import StubCatalog, StubRunObserver, StubSourceReader, StubTargetWriter


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]], list[str]]] = []

    def seed(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
        *,
        schema: Any = None,
    ) -> Any:
        self.calls.append((ref, data, columns))
        return None

    def seed_temp(
        self, name: Any, data: Any = None, columns: Any = None, *, schema: Any = None
    ) -> Any:
        return None

    def run(self, step_cls: type[Any], params: Any) -> Any:
        return None


class TestScenario:
    def test_apply_seeds_all_tables_in_order_and_returns_runner(self) -> None:
        runner = _Runner()
        scenario = (
            ETLScenario()
            .with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
            .with_table("raw.customers", [(1, "alice")], ["id", "name"])
        )

        out = scenario.apply(runner)

        assert out is runner
        assert runner.calls == [
            ("raw.orders", [(1, 10.0)], ["id", "amount"]),
            ("raw.customers", [(1, "alice")], ["id", "name"]),
        ]
        assert "raw.orders" in repr(scenario)
        assert isinstance(runner, StepRunnerProto)


class TestStubCatalog:
    def test_prefers_explicit_schema_over_table_columns(self) -> None:
        schemas: dict[str, tuple[ColumnSchema, ...]] = {
            "raw.orders": (ColumnSchema("id", LoomDtype.INT64),)
        }
        catalog = StubCatalog(tables={"raw.orders": ("legacy",)}, schemas=schemas)

        assert catalog.exists(TableRef("raw.orders")) is True
        assert catalog.columns(TableRef("raw.orders")) == ("id",)
        assert catalog.schema(TableRef("raw.orders")) == schemas["raw.orders"]


class TestStubIO:
    def test_stub_reader_and_writer_capture_data_flow(self) -> None:
        frame = {"id": [1]}
        reader = StubSourceReader({"orders": frame})
        writer = StubTargetWriter()

        src_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
        target_spec = ReplaceSpec(table_ref=TableRef("staging.out"))

        assert reader.read(src_spec, None) is frame
        writer.write(frame, target_spec, _params_instance=None)
        assert writer.written == [(frame, target_spec)]


class TestStubObserver:
    def test_stub_run_observer_records_full_lifecycle_and_status_helpers(self) -> None:
        observer = StubRunObserver()
        ctx = RunContext(
            run_id="run-1",
            correlation_id="corr-1",
            attempt=2,
            last_attempt=False,
        )

        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.PIPELINE,
                name="Pipe",
                id=ctx.run_id,
                correlation_id=ctx.correlation_id,
                meta={
                    "run_id": ctx.run_id,
                    "attempt": ctx.attempt,
                    "last_attempt": ctx.last_attempt,
                },
            )
        )
        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.PROCESS,
                name="Proc",
                id="proc-1",
                correlation_id=ctx.correlation_id,
                meta={"run_id": ctx.run_id, "attempt": ctx.attempt},
            )
        )
        observer.on_event(
            LifecycleEvent.start(
                scope=Scope.STEP,
                name="MyStep",
                id="step-1",
                correlation_id=ctx.correlation_id,
                meta={
                    "run_id": ctx.run_id,
                    "attempt": ctx.attempt,
                    "process_run_id": "proc-1",
                },
            )
        )
        observer.on_event(
            LifecycleEvent.exception(
                scope=Scope.STEP,
                name="MyStep",
                id="step-1",
                correlation_id=ctx.correlation_id,
                error="boom",
                meta={
                    "run_id": ctx.run_id,
                    "attempt": ctx.attempt,
                    "process_run_id": "proc-1",
                },
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.STEP,
                name="MyStep",
                id="step-1",
                correlation_id=ctx.correlation_id,
                duration_ms=5,
                status=LifecycleStatus.FAILURE,
                meta={
                    "run_id": ctx.run_id,
                    "attempt": ctx.attempt,
                    "process_run_id": "proc-1",
                },
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.PROCESS,
                name="Proc",
                id="proc-1",
                correlation_id=ctx.correlation_id,
                duration_ms=6,
                status=LifecycleStatus.FAILURE,
                meta={"run_id": ctx.run_id, "attempt": ctx.attempt},
            )
        )
        observer.on_event(
            LifecycleEvent.end(
                scope=Scope.PIPELINE,
                name="Pipe",
                id=ctx.run_id,
                correlation_id=ctx.correlation_id,
                duration_ms=7,
                status=LifecycleStatus.FAILURE,
                meta={"run_id": ctx.run_id, "attempt": ctx.attempt},
            )
        )

        assert observer.event_names == [
            EventName.PIPELINE_START,
            EventName.PROCESS_START,
            EventName.STEP_START,
            EventName.STEP_ERROR,
            EventName.STEP_END,
            EventName.PROCESS_END,
            EventName.PIPELINE_END,
        ]
        assert observer.step_statuses == [RunStatus.FAILED]
        assert observer.pipeline_statuses == [RunStatus.FAILED]

    def test_run_context_defaults_are_stable(self) -> None:
        ctx = RunContext(run_id="run-2")
        assert ctx.correlation_id is None
        assert ctx.attempt == 1
        assert ctx.last_attempt is True
        assert isinstance(datetime.now(tz=UTC), datetime)
