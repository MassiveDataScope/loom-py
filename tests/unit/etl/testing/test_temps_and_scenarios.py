"""In-memory runner: temps, typed seeds, immutable scenarios and source finalisation."""

from __future__ import annotations

from typing import Any, ClassVar

import msgspec
import polars as pl
import pytest

from loom.etl import (
    CheckpointScope,
    ETLParams,
    ETLStep,
    FromTable,
    FromTemp,
    IntoTable,
    IntoTemp,
    Sources,
    UpsertDeleteFlagError,
    col,
)
from loom.etl.schema._schema import LoomDtype
from loom.etl.testing import ETLScenario, PolarsStepRunner, StepResult, StepRunnerProto


class P(ETLParams):  # type: ignore[misc]
    min_amount: float = 0.0


class Normalize(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTemp("normalized_orders")
    calls: ClassVar[int] = 0

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        type(self).calls += 1
        return raw.with_columns(pl.col("amount") * 2)


class Enrich(ETLStep[P]):
    orders = FromTemp("normalized_orders")
    target = IntoTable("mart.orders").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders.filter(pl.col("amount").is_not_null())


class EagerIntoTemp(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTemp("eager")

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw.collect()


class PartA(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.a"))
    target = IntoTemp("parts", append=True)

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class PartB(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.b"))
    target = IntoTemp("parts", append=True)

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class KeepAcrossRetries(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTemp("kept", scope=CheckpointScope.CORRELATION)

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class ReadKept(ETLStep[P]):
    kept = FromTemp("kept")
    target = IntoTable("mart.kept").replace()

    def execute(self, params: P, *, kept: Any) -> Any:  # type: ignore[override]
        return kept


class Projected(ETLStep[P]):
    orders = FromTable("raw.orders").where(col("amount") >= 10.0).columns("id", "amount")
    target = IntoTable("mart.big").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class UnknownColumn(ETLStep[P]):
    orders = FromTable("raw.orders").columns("id", "missing")
    target = IntoTable("mart.bad").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class _OrderRow(msgspec.Struct):
    id: int
    amount: float
    note: str | None


class _ReceivedSchema(ETLStep[P]):
    """Return the dtypes the step receives, one row per column."""

    orders = FromTable("raw.orders").with_schema(_OrderRow)
    target = IntoTable("mart.schema").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        schema = orders.collect_schema()
        return pl.LazyFrame({"column": schema.names(), "dtype": [str(d) for d in schema.dtypes()]})


class _Payload(msgspec.Struct):
    kind: str


class DecodedEvents(ETLStep[P]):
    events = FromTable("raw.events").parse_json("payload", _Payload)
    target = IntoTable("mart.events").replace()

    def execute(self, params: P, *, events: Any) -> Any:  # type: ignore[override]
        return events.select(pl.col("payload").struct.field("kind"))


class UpsertWithTombstones(ETLStep[P]):
    sources = Sources(cdc=FromTable("raw.cdc"))
    target = IntoTable("silver.orders").upsert(keys=("id",), delete_flag="_deleted")

    def execute(self, params: P, *, cdc: Any) -> Any:  # type: ignore[override]
        return cdc


class _Passthrough(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.orders"))
    target = IntoTable("mart.out").replace()

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class _PassthroughTemp(ETLStep[P]):
    orders = FromTemp("normalized_orders")
    target = IntoTable("mart.out").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class UpsertDroppingFlag(ETLStep[P]):
    sources = Sources(cdc=FromTable("raw.cdc"))
    target = IntoTable("silver.orders").upsert(keys=("id",), delete_flag="_deleted")

    def execute(self, params: P, *, cdc: Any) -> Any:  # type: ignore[override]
        return cdc.drop("_deleted")


_ORDERS = ETLScenario().with_table(
    "raw.orders",
    [(1, 10.0), (2, 5.0), (3, None)],
    ["id", "amount"],
    schema={"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64},
)


# 1 ---------------------------------------------------------------------------
class TestSeedTemp:
    def test_from_temp_reads_the_temp_seeded_by_name(self) -> None:
        runner = PolarsStepRunner().seed_temp(
            "normalized_orders", [(1, 10.0), (2, None)], ["id", "amount"]
        )

        result = runner.run(Enrich, P())

        assert result.to_polars().to_dict(as_series=False) == {"id": [1], "amount": [10.0]}

    def test_seeding_the_alias_does_not_satisfy_from_temp(self) -> None:
        runner = PolarsStepRunner().seed_temp("orders", [(1, 10.0)], ["id", "amount"])
        runner.seed("orders", [(1, 10.0)], ["id", "amount"])

        with pytest.raises(FileNotFoundError, match="normalized_orders"):
            runner.run(Enrich, P())


# 2 ---------------------------------------------------------------------------
class TestIntoTempResult:
    def test_into_temp_step_returns_the_temp_contents(self) -> None:
        result = PolarsStepRunner(_ORDERS).run(Normalize, P())

        assert result.temp_name == "normalized_orders"
        assert result.to_polars().sort("id")["amount"].to_list() == [20.0, 10.0, None]


# 3 ---------------------------------------------------------------------------
class TestIntoTempRejectsEagerFrames:
    def test_dataframe_from_execute_raises_like_production(self) -> None:
        runner = PolarsStepRunner(_ORDERS)

        with pytest.raises(TypeError, match="expects polars.LazyFrame"):
            runner.run(EagerIntoTemp, P())


# 4 ---------------------------------------------------------------------------
class TestFanIn:
    def test_two_writers_concatenate_and_align_to_the_first_schema(self) -> None:
        runner = (
            PolarsStepRunner()
            .seed("raw.a", [(1, 10.0)], ["id", "amount"])
            .seed("raw.b", [(2, "x")], ["id", "extra"])
        )

        first = runner.run(PartA, P())
        result = runner.run(PartB, P())

        first.assert_count(1)
        assert result.temp_name == "parts"
        frame = result.to_polars().sort("id")
        assert frame.columns == ["id", "amount"]
        assert frame.schema["amount"] == pl.Float64
        assert frame.to_dict(as_series=False) == {"id": [1, 2], "amount": [10.0, None]}


# 5 ---------------------------------------------------------------------------
class TestCorrelationScope:
    def test_correlation_scoped_temp_is_written_and_read(self) -> None:
        runner = PolarsStepRunner(_ORDERS)

        runner.run(KeepAcrossRetries, P()).assert_count(3)
        runner.run(ReadKept, P()).assert_count(3)

    def test_run_returns_the_temp_from_the_scope_the_step_wrote(self) -> None:
        runner = PolarsStepRunner(_ORDERS).seed_temp("kept", [(99, 1.0)], ["id", "amount"])

        result = runner.run(KeepAcrossRetries, P())

        assert sorted(result.to_polars()["id"].to_list()) == [1, 2, 3]

    def test_a_seeded_run_temp_takes_precedence_for_from_temp(self) -> None:
        runner = PolarsStepRunner(_ORDERS).seed_temp("kept", [(99, 1.0)], ["id", "amount"])
        runner.run(KeepAcrossRetries, P())

        result = runner.run(ReadKept, P())

        assert result.to_polars()["id"].to_list() == [99]


# 6 ---------------------------------------------------------------------------
class TestChaining:
    def test_two_steps_chained_through_a_temp_on_one_runner(self) -> None:
        runner = PolarsStepRunner(_ORDERS)

        runner.run(Normalize, P())
        result = runner.run(Enrich, P())

        assert sorted(result.to_polars()["amount"].to_list()) == [10.0, 20.0]

    def test_after_runs_the_upstream_step_once_across_applications(self) -> None:
        Normalize.calls = 0
        normalized = _ORDERS.after(Normalize, P())

        first = PolarsStepRunner(normalized).run(Enrich, P())
        second = PolarsStepRunner(normalized).run(Enrich, P())
        third = normalized.apply(PolarsStepRunner()).run(Enrich, P())

        assert Normalize.calls == 1
        for result in (first, second, third):
            assert sorted(result.to_polars()["amount"].to_list()) == [10.0, 20.0]

    def test_after_ignores_seeds_outside_its_prefix(self) -> None:
        runner = PolarsStepRunner().seed("raw.orders", [(1, 10.0)], ["id", "amount"])

        with pytest.raises(KeyError, match="raw.orders"):
            ETLScenario().after(Normalize, P()).apply(runner)

    def test_composed_scenarios_reuse_the_same_after_result(self) -> None:
        Normalize.calls = 0
        small = ETLScenario().with_table("raw.orders", [(1, 1.0)], ["id", "amount"])
        large = ETLScenario().with_table("raw.orders", [(1, 1.0), (2, 2.0)], ["id", "amount"])
        normalized = _ORDERS.after(Normalize, P())

        from_small = PolarsStepRunner(small + normalized).run(Enrich, P())
        from_large = PolarsStepRunner(large + normalized).run(Enrich, P())

        assert Normalize.calls == 1
        assert from_small.to_polars().equals(from_large.to_polars())

    def test_two_prefixes_with_the_same_after_give_their_own_results(self) -> None:
        Normalize.calls = 0
        small = ETLScenario().with_table("raw.orders", [(1, 1.0)], ["id", "amount"])
        large = ETLScenario().with_table("raw.orders", [(1, 1.0), (2, 2.0)], ["id", "amount"])
        small_after = small.after(Normalize, P())
        large_after = large.after(Normalize, P())

        results = [
            PolarsStepRunner(scenario).run(Enrich, P())
            for scenario in (small_after, large_after, small_after, large_after)
        ]

        assert Normalize.calls == 2
        assert [len(r.to_polars()) for r in results] == [1, 2, 1, 2]

    def test_repr_lists_after_entries_apart_from_temps(self) -> None:
        scenario = _ORDERS.after(Normalize, P())

        assert repr(scenario) == "ETLScenario(tables=['raw.orders'], temps=[], after=['Normalize'])"

    def test_seed_temp_infers_the_name_from_an_into_temp_result(self) -> None:
        upstream = PolarsStepRunner(_ORDERS).run(Normalize, P())

        result = PolarsStepRunner().seed_temp(upstream).run(Enrich, P())

        result.assert_count(2)

    def test_seed_temp_without_a_name_needs_an_into_temp_result(self) -> None:
        not_a_temp = StepResult(pl.DataFrame({"id": [1]}))

        with pytest.raises(ValueError, match="temp name"):
            PolarsStepRunner().seed_temp(not_a_temp)


# 7 ---------------------------------------------------------------------------
class TestTypedSeeds:
    def test_empty_and_all_null_table_seeds_keep_the_declared_dtype(self) -> None:
        schema = {"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64}
        empty = PolarsStepRunner().seed("raw.orders", [], ["id", "amount"], schema=schema)
        nulls = PolarsStepRunner().seed(
            "raw.orders", [(1, None), (2, None)], ["id", "amount"], schema=schema
        )

        for runner, rows in ((empty, 0), (nulls, 2)):
            result = runner.run(_Passthrough, P())
            result.assert_count(rows)
            result.assert_schema(schema)

    def test_empty_and_all_null_temp_seeds_keep_the_declared_dtype(self) -> None:
        schema = {"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64}
        for rows in ([], [(1, None)]):
            runner = PolarsStepRunner().seed_temp(
                "normalized_orders", rows, ["id", "amount"], schema=schema
            )
            result = runner.run(_PassthroughTemp, P())
            result.assert_count(len(rows))
            result.assert_schema(schema)


# 8 ---------------------------------------------------------------------------
class TestScenarioComposition:
    def test_with_methods_return_new_scenarios(self) -> None:
        base = ETLScenario()
        orders = base.with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
        temp = orders.with_temp("normalized_orders", [(1, 10.0)], ["id", "amount"])

        assert base is not orders
        assert orders is not temp
        assert repr(base) == "ETLScenario(tables=[], temps=[], after=[])"
        assert repr(orders) == "ETLScenario(tables=['raw.orders'], temps=[], after=[])"
        assert repr(temp) == (
            "ETLScenario(tables=['raw.orders'], temps=['normalized_orders'], after=[])"
        )

    def test_plus_composes_both_scenarios_in_order(self) -> None:
        orders = ETLScenario().with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
        temps = ETLScenario().with_temp("normalized_orders", [(1, None)], ["id", "amount"])

        full = orders + temps
        runner = PolarsStepRunner(full)

        runner.run(_Passthrough, P()).assert_count(1)
        runner.run(Enrich, P()).assert_count(0)
        assert repr(orders) == "ETLScenario(tables=['raw.orders'], temps=[], after=[])"

    def test_positional_with_table_and_seed_calls_still_work(self) -> None:
        scenario = (
            ETLScenario()
            .with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
            .with_table("raw.customers", [(1, "alice")], ["id", "name"])
        )
        runner = scenario.apply(PolarsStepRunner())
        runner.seed("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])

        assert isinstance(runner, StepRunnerProto)
        runner.run(_Passthrough, P()).assert_count(2)

    def test_runner_accepts_several_scenarios(self) -> None:
        orders = ETLScenario().with_table("raw.orders", [(1, 10.0)], ["id", "amount"])
        temps = ETLScenario().with_temp("normalized_orders", [(1, 10.0)], ["id", "amount"])

        runner = PolarsStepRunner(orders, temps)

        runner.run(_Passthrough, P()).assert_count(1)
        runner.run(Enrich, P()).assert_count(1)


# 9 ---------------------------------------------------------------------------
class TestDeclaredSourceSchema:
    _ROWS: ClassVar[list[tuple[Any, ...]]] = [(1, 10, None), (2, 20, None)]
    _EXPECTED: ClassVar[dict[str, str]] = {"id": "Int64", "amount": "Float64", "note": "String"}

    def test_untyped_seed_reaches_the_step_with_the_declared_dtypes(self) -> None:
        runner = PolarsStepRunner().seed("raw.orders", self._ROWS, ["id", "amount", "note"])

        received = runner.run(_ReceivedSchema, P()).to_polars()

        assert dict(received.iter_rows()) == self._EXPECTED

    def test_untyped_scenario_table_reaches_the_step_with_the_declared_dtypes(self) -> None:
        scenario = ETLScenario().with_table("raw.orders", self._ROWS, ["id", "amount", "note"])

        received = PolarsStepRunner(scenario).run(_ReceivedSchema, P()).to_polars()

        assert dict(received.iter_rows()) == self._EXPECTED


class TestSourceFinalisation:
    def test_columns_project_and_where_filters(self) -> None:
        runner = PolarsStepRunner().seed(
            "raw.orders", [(1, 10.0, "a"), (2, 5.0, "b"), (3, 30.0, "c")], ["id", "amount", "tag"]
        )

        frame = runner.run(Projected, P()).to_polars().sort("id")

        assert frame.columns == ["id", "amount"]
        assert frame["id"].to_list() == [1, 3]

    def test_unknown_column_fails(self) -> None:
        runner = PolarsStepRunner().seed("raw.orders", [(1, 10.0)], ["id", "amount"])

        with pytest.raises(pl.exceptions.ColumnNotFoundError):
            runner.run(UnknownColumn, P())

    def test_parse_json_columns_reach_the_step_decoded(self) -> None:
        runner = PolarsStepRunner().seed("raw.events", [('{"kind": "click"}',)], ["payload"])

        frame = runner.run(DecodedEvents, P()).to_polars()

        assert frame["kind"].to_list() == ["click"]


# deletes / merged in memory ---------------------------------------------------
class TestDeleteMetricsInMemory:
    def test_deletes_counts_flagged_rows_the_step_emitted(self) -> None:
        runner = PolarsStepRunner().seed(
            "raw.cdc", [(1, "open", True), (2, "open", False)], ["id", "status", "_deleted"]
        )

        result = runner.run(UpsertWithTombstones, P())

        assert result.deletes == 1

    def test_deletes_needs_the_flag_column_in_the_output(self) -> None:
        runner = PolarsStepRunner().seed(
            "raw.cdc", [(1, "open", True)], ["id", "status", "_deleted"]
        )

        result = runner.run(UpsertDroppingFlag, P())

        with pytest.raises(UpsertDeleteFlagError, match="is missing from"):
            _ = result.deletes

    def test_deletes_needs_a_boolean_flag_column(self) -> None:
        runner = PolarsStepRunner().seed("raw.cdc", [(1, "open", 1)], ["id", "status", "_deleted"])

        result = runner.run(UpsertWithTombstones, P())

        with pytest.raises(UpsertDeleteFlagError, match="must be a boolean column"):
            _ = result.deletes

    def test_deletes_needs_an_upsert_with_delete_flag(self) -> None:
        result = PolarsStepRunner(_ORDERS).run(_Passthrough, P())

        with pytest.raises(RuntimeError, match="delete_flag"):
            _ = result.deletes

    def test_merged_needs_delta_root(self) -> None:
        runner = PolarsStepRunner().seed(
            "raw.cdc", [(1, "open", True)], ["id", "status", "_deleted"]
        )

        result = runner.run(UpsertWithTombstones, P())

        with pytest.raises(RuntimeError, match=r"table\(ref\)\.assert_count"):
            _ = result.merged

    def test_table_needs_delta_root(self) -> None:
        with pytest.raises(RuntimeError, match="delta_root"):
            PolarsStepRunner().table("raw.orders")
