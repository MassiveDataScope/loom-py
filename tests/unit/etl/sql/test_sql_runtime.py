"""Tests for SQL runtime helpers: resolve_sql, predicate_to_sql, and StepSQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
import pytest

from loom.etl import col, params
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import FromTable, Sources
from loom.etl.declarative.target import IntoTable
from loom.etl.executor import ETLExecutor
from loom.etl.pipeline._sql import resolve_sql
from loom.etl.pipeline._step_sql import StepSQL, _extract_stepsql_types


@dataclass(frozen=True)
class _SqlParams:
    run_date: date
    countries: tuple[str, ...]
    active: bool


def _params() -> _SqlParams:
    return _SqlParams(run_date=date(2024, 1, 15), countries=("ES", "FR"), active=True)


class TestResolveSql:
    def test_resolve_sql_interpolates_nested_params_as_safe_literals(self) -> None:
        query = (
            "SELECT * FROM orders "
            "WHERE year = {{ params.run_date.year }} "
            "AND country IN ({{ params.countries.0 }})"
        )

        # list index access is intentionally disallowed (non-identifier);
        # only attribute chains are valid by design.
        params = _params()
        with pytest.raises(ValueError, match="invalid expression"):
            resolve_sql(query, params)

        valid = (
            "SELECT * FROM orders "
            "WHERE year = {{ params.run_date.year }} "
            "AND active = {{ params.active }}"
        )
        assert (
            resolve_sql(valid, _params())
            == "SELECT * FROM orders WHERE year = 2024 AND active = TRUE"
        )

    @pytest.mark.parametrize(
        "sql,exc,match",
        [
            ("SELECT {{ config.env }}", ValueError, "only 'params.x.y'"),
            ("SELECT {{ params.run-date }}", ValueError, "invalid expression"),
            ("SELECT {{ params.missing }}", AttributeError, "missing"),
        ],
    )
    def test_resolve_sql_errors(self, sql: str, exc: type[Exception], match: str) -> None:
        params = _params()
        with pytest.raises(exc, match=match):
            resolve_sql(sql, params)


class TestPredicateToSql:
    def test_predicate_to_sql_supports_composition_and_param_resolution(self) -> None:
        node = ((col("year") == params.run_date.year) & (col("active") == params.active)) | (
            col("country").isin(params.countries)
        )
        sql = predicate_to_sql(node, _params())
        assert sql == "((year = 2024) AND (active = TRUE)) OR (country IN ('ES', 'FR'))"

    @pytest.mark.parametrize(
        "node,expected",
        [
            (col("amount") > 10, "amount > 10"),
            (col("amount") >= 10, "amount >= 10"),
            (col("amount") < 10, "amount < 10"),
            (col("amount") <= 10, "amount <= 10"),
            (col("status") != "deleted", "status != 'deleted'"),
            (
                col("updated_at") >= datetime(2026, 5, 25, 0, 0, tzinfo=UTC),
                "updated_at >= '2026-05-25T00:00:00+00:00'",
            ),
            (~(col("active") == True), "NOT (active = TRUE)"),  # noqa: E712
        ],
    )
    def test_predicate_to_sql_binary_and_unary_forms(self, node: Any, expected: str) -> None:
        assert predicate_to_sql(node, _params()) == expected

    def test_predicate_to_sql_raises_on_unsupported_node(self) -> None:
        params = _params()
        with pytest.raises(TypeError, match="unsupported node type"):
            predicate_to_sql(object(), params)  # type: ignore[arg-type]


class _StaticStep(StepSQL[_SqlParams, pl.LazyFrame]):
    sql = "SELECT id, amount FROM orders"


class _DynamicStep(StepSQL[_SqlParams, pl.LazyFrame]):
    @staticmethod
    def sql(p: _SqlParams) -> str:
        return f"SELECT id, {p.run_date.year} AS run_year FROM orders"


class TestStepSqlRender:
    def test_stepsql_renders_query_from_static_and_dynamic_sql(self) -> None:
        assert _StaticStep().render_sql(_params()) == "SELECT id, amount FROM orders"
        assert _DynamicStep().render_sql(_params()) == "SELECT id, 2024 AS run_year FROM orders"


class _ExecReader:
    def __init__(self, frame: Any, result: Any) -> None:
        self._frame = frame
        self._result = result
        self.captured_frames: dict[str, Any] | None = None
        self.captured_query: str | None = None

    def read(self, spec: Any, _params_instance: Any, /) -> Any:
        _ = spec
        return self._frame

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        self.captured_frames = frames
        self.captured_query = query
        return self._result


class _ExecWriter:
    def __init__(self) -> None:
        self.written: Any = None

    def write(
        self,
        frame: Any,
        spec: Any,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        _ = spec
        _ = params_instance
        _ = streaming
        _ = write_ctx
        self.written = frame


class _RouteStep(StepSQL[_SqlParams, object]):
    sources = Sources(frame=FromTable("raw.frame"))
    target = IntoTable("staging.out").replace()
    sql = "SELECT 1 AS value"


class TestStepSqlExecution:
    def test_stepsql_executor_delegates_sql_to_reader(self) -> None:
        plan = ETLCompiler().compile_step(_RouteStep)
        sentinel = object()
        source_frame = object()
        reader = _ExecReader(source_frame, sentinel)
        writer = _ExecWriter()

        ETLExecutor(reader, writer).run_step(plan, _params())

        assert reader.captured_query == "SELECT 1 AS value"
        assert reader.captured_frames == {"frame": source_frame}
        assert writer.written is sentinel

    def test_stepsql_extract_types_on_non_stepsql_class_returns_none(self) -> None:
        class _Plain:
            pass

        assert _extract_stepsql_types(_Plain) == (None, None)

    def test_base_stepsql_execute_raises_not_implemented(self) -> None:
        step_sql = StepSQL()
        params = _params()
        with pytest.raises(
            NotImplementedError, match="executed by ETLExecutor via SQLExecutor.execute_sql"
        ):
            step_sql.execute(params)  # type: ignore[misc]
