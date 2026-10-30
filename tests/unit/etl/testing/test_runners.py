"""Tests for testing harness internals: runners and StepResult."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest

from loom.etl import col
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import FileSourceSpec, TableSourceSpec
from loom.etl.declarative.target._table import ReplaceSpec, ReplaceWhereSpec
from loom.etl.executor import ETLExecutor
from loom.etl.schema._schema import LoomDtype
from loom.etl.testing._result import StepResult
from loom.etl.testing._runners import (
    PolarsStepRunner,
    _build_lazy_frame,
    _PolarsCapturingWriter,
    _PolarsStubReader,
)


@dataclass
class _RunParams:
    run_day: int


class TestPolarsRunnerInternals:
    def test_build_lazy_frame_handles_empty_and_rows(self) -> None:
        empty = _build_lazy_frame([], ["id", "amount"]).collect()
        assert empty.is_empty()
        assert empty.columns == ["id", "amount"]

        frame = _build_lazy_frame([(1, 10.0), (2, 20.0)], ["id", "amount"]).collect()
        assert frame.to_dict(as_series=False) == {"id": [1, 2], "amount": [10.0, 20.0]}

    def test_polars_capturing_writer_stores_last_call(self) -> None:
        writer = _PolarsCapturingWriter()
        frame = pl.DataFrame({"id": [1]}).lazy()
        spec = ReplaceSpec(table_ref=TableRef("staging.out"))

        writer.write(frame, spec, {"run_id": "r-1"})

        assert writer.frame is frame
        assert writer.spec is spec
        assert writer._last_params == {"run_id": "r-1"}

    def test_polars_stub_reader_resolves_table_ref_or_alias(self) -> None:
        table_frame = pl.DataFrame({"id": [1]}).lazy()
        file_frame = pl.DataFrame({"id": [2]}).lazy()
        reader = _PolarsStubReader({"raw.orders": table_frame, "events": file_frame})

        table_spec = TableSourceSpec(alias="orders", table_ref=TableRef("raw.orders"))
        file_spec = FileSourceSpec(alias="events", path="s3://bucket/events.csv", format=Format.CSV)

        assert reader.read(table_spec, None) is table_frame
        assert reader.read(file_spec, None) is file_frame


class TestPolarsStepRunner:
    def test_guards_before_run(self) -> None:
        runner = PolarsStepRunner()

        with pytest.raises(RuntimeError, match="No spec"):
            _ = runner.target_spec

        assert runner.resolved_predicate is None

    def test_run_returns_step_result_and_resolved_predicate(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = PolarsStepRunner().seed("raw.orders", [(1, 10.0)], ["id", "amount"])

        class _Plan:
            pass

        def _fake_compile_step(self: ETLCompiler, _step_cls: type[Any]) -> _Plan:
            return _Plan()

        def _fake_run_step(
            self: ETLExecutor, _plan: Any, params_instance: _RunParams, _ctx: Any = None
        ) -> None:
            predicate = col("day") == params_instance.run_day
            spec = ReplaceWhereSpec(table_ref=TableRef("staging.out"), replace_predicate=predicate)
            writer = self._writer
            writer.write(
                pl.DataFrame({"id": [1], "amount": [20.0]}).lazy(),
                spec,
                params_instance,
            )

        monkeypatch.setattr(ETLCompiler, "compile_step", _fake_compile_step)
        monkeypatch.setattr(ETLExecutor, "run_step", _fake_run_step)

        result = runner.run(type("DummyStep", (), {}), _RunParams(run_day=7))
        assert result.to_polars().shape == (1, 2)
        assert isinstance(runner.target_spec, ReplaceWhereSpec)
        resolved = runner.resolved_predicate
        assert resolved is not None
        assert "day" in resolved
        assert "7" in resolved

    def test_run_raises_when_no_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runner = PolarsStepRunner().seed("raw.orders", [(1, 10.0)], ["id", "amount"])

        def _fake_compile_step(self: ETLCompiler, _step_cls: type[Any]) -> object:
            spec = ReplaceSpec(table_ref=TableRef("staging.out"))
            return SimpleNamespace(target_binding=SimpleNamespace(spec=spec))

        def _fake_run_step(
            self: ETLExecutor, _plan: Any, _params_instance: Any, _ctx: Any = None
        ) -> None:
            return None

        monkeypatch.setattr(ETLCompiler, "compile_step", _fake_compile_step)
        monkeypatch.setattr(ETLExecutor, "run_step", _fake_run_step)

        dummy_step = type("DummyStep", (), {})
        params = object()

        with pytest.raises(RuntimeError, match="Step produced no output"):
            runner.run(dummy_step, params)


class TestStepResult:
    def test_assertions_and_show(self, capsys: pytest.CaptureFixture[str]) -> None:
        result = StepResult(pl.DataFrame({"id": [1], "amount": [10.0]}))

        result.assert_count(1)
        result.assert_not_empty()
        result.assert_schema({"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64})
        result.show(1)
        assert "amount" in capsys.readouterr().out

    def test_failures_raise_assertion_error(self) -> None:
        result = StepResult(pl.DataFrame({"id": [1], "amount": [10.0]}))

        with pytest.raises(AssertionError, match="Expected 2 rows"):
            result.assert_count(2)

        with pytest.raises(AssertionError, match="Column 'missing' not found"):
            result.assert_schema({"missing": LoomDtype.INT64})

        with pytest.raises(AssertionError, match="expected String"):
            result.assert_schema({"id": LoomDtype.UTF8})

        empty_result = StepResult(pl.DataFrame({"id": []}))
        with pytest.raises(AssertionError, match="Expected non-empty"):
            empty_result.assert_not_empty()
