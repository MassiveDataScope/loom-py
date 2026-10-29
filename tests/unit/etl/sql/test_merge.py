"""Unit tests for shared UPSERT/MERGE helpers in backend utilities."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.etl.backends._merge import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_insert_values,
    _build_join_clause,
    _build_merge_plan,
    _build_partition_predicate,
    _build_update_set,
    _build_upsert_predicate,
    _build_upsert_update_cols,
    _MergePlan,
)
from loom.etl.backends._predicate import sql_literal
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._table import UpsertSpec


def _upsert_spec(
    keys: tuple[str, ...] = ("id",),
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
) -> UpsertSpec:
    return UpsertSpec(
        table_ref=TableRef("test.table"),
        upsert_keys=keys,
        partition_cols=partition_cols,
        upsert_exclude=exclude,
        upsert_include=include,
    )


class TestSqlLiteral:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("hello", "'hello'"),
            ("it's", "'it''s'"),
            (True, "TRUE"),
            (False, "FALSE"),
            (42, "42"),
            (3.14, "3.14"),
            (None, "NULL"),
        ],
    )
    def test_sql_literal(self, value: object, expected: str) -> None:
        assert sql_literal(value) == expected


class TestJoinClause:
    @pytest.mark.parametrize(
        "keys,partition_cols,expected",
        [
            (("id",), (), "t.id = s.id"),
            (
                ("id",),
                ("year", "month"),
                "t.id = s.id AND t.year = s.year AND t.month = s.month",
            ),
            (("order_id", "line_id"), (), "t.order_id = s.order_id AND t.line_id = s.line_id"),
        ],
    )
    def test_build_join_clause(
        self,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        expected: str,
    ) -> None:
        assert _build_join_clause(keys, partition_cols, "t", "s") == expected


class TestPartitionFiltering:
    @pytest.mark.parametrize(
        "rows,partition_cols,alias,expected",
        [
            ([{"year": 2023}], ("year",), "t", "(t.year = 2023)"),
            (
                [{"year": 2023, "month": 1}],
                ("year", "month"),
                "t",
                "(t.year = 2023 AND t.month = 1)",
            ),
            ([{"region": "eu-west"}], ("region",), "t", "(t.region = 'eu-west')"),
            # no alias — replaceWhere style
            ([{"year": 2024, "month": 1}], ("year", "month"), "", "(year = 2024 AND month = 1)"),
        ],
    )
    def test_build_partition_predicate_aliased(
        self,
        rows: list[dict[str, object]],
        partition_cols: tuple[str, ...],
        alias: str,
        expected: str,
    ) -> None:
        assert _build_partition_predicate(rows, partition_cols, alias=alias) == expected

    @pytest.mark.parametrize(
        "rows,partition_cols,expected",
        [
            (
                [{"year": 2023, "month": 1}],
                ("year", "month"),
                "(t.year = 2023 AND t.month = 1)",
            ),
            (
                [{"year": 2023, "month": 1}, {"year": 2024, "month": 3}],
                ("year", "month"),
                "(t.year = 2023 AND t.month = 1) OR (t.year = 2024 AND t.month = 3)",
            ),
        ],
    )
    def test_build_partition_predicate_multi_combo(
        self,
        rows: list[dict[str, object]],
        partition_cols: tuple[str, ...],
        expected: str,
    ) -> None:
        assert _build_partition_predicate(rows, partition_cols, alias="t") == expected


class TestUpsertPredicate:
    @pytest.mark.parametrize(
        "keys,partition_cols,combos,expected",
        [
            (("id",), (), [], "t.id = s.id"),
            (
                ("id",),
                ("year", "month"),
                [{"year": 2023, "month": 1}],
                "((t.year = 2023 AND t.month = 1)) AND "
                "(t.id = s.id AND t.year = s.year AND t.month = s.month)",
            ),
            (("id",), ("year",), [], "t.id = s.id AND t.year = s.year"),
        ],
    )
    def test_build_upsert_predicate(
        self,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...],
        combos: list[dict[str, object]],
        expected: str,
    ) -> None:
        spec = _upsert_spec(keys=keys, partition_cols=partition_cols)
        assert _build_upsert_predicate(combos, spec, TARGET_ALIAS, SOURCE_ALIAS) == expected


class TestUpsertUpdateCols:
    @pytest.mark.parametrize(
        "spec,columns,expected",
        [
            (_upsert_spec(keys=("id",)), ("id", "name", "value"), ("name", "value")),
            (
                _upsert_spec(keys=("id",), partition_cols=("year", "month")),
                ("id", "year", "month", "name"),
                ("name",),
            ),
            (
                _upsert_spec(keys=("id",), exclude=("created_at",)),
                ("id", "name", "created_at", "updated_at"),
                ("name", "updated_at"),
            ),
            (
                _upsert_spec(keys=("id",), include=("name", "value")),
                ("id", "name", "value", "created_at"),
                ("name", "value"),
            ),
            (
                _upsert_spec(keys=("id",), include=("id", "name")),
                ("id", "name", "value"),
                ("name",),
            ),
        ],
    )
    def test_build_upsert_update_cols(
        self,
        spec: UpsertSpec,
        columns: tuple[str, ...],
        expected: tuple[str, ...],
    ) -> None:
        assert _build_upsert_update_cols(columns, spec) == expected


class TestInsertAndUpdateMaps:
    @pytest.mark.parametrize(
        "columns,alias,builder,expected",
        [
            (
                ("name", "value"),
                "s",
                _build_update_set,
                {"name": "s.name", "value": "s.value"},
            ),
            (
                ("id", "name", "year"),
                "s",
                _build_insert_values,
                {"id": "s.id", "name": "s.name", "year": "s.year"},
            ),
        ],
    )
    def test_build_update_or_insert_maps(
        self,
        columns: tuple[str, ...],
        alias: str,
        builder: Callable[[tuple[str, ...], str], dict[str, str]],
        expected: dict[str, str],
    ) -> None:
        assert builder(columns, alias) == expected


def test_merge_plan_without_delete_flag_matches_the_pre_flag_clauses() -> None:
    """Pin the MERGE shape produced when delete_flag is unset."""
    spec = _upsert_spec(keys=("id",), partition_cols=("year",), exclude=("created_at",))
    plan = _build_merge_plan(
        combos=[{"year": 2024}],
        spec=spec,
        df_columns=("id", "year", "name", "created_at"),
        target_alias=TARGET_ALIAS,
        source_alias=SOURCE_ALIAS,
    )

    assert plan == _MergePlan(
        predicate="((t.year = 2024)) AND (t.id = s.id AND t.year = s.year)",
        update_set={"name": "s.name"},
        insert_values={
            "id": "s.id",
            "year": "s.year",
            "name": "s.name",
            "created_at": "s.created_at",
        },
    )


def test_merge_plan_delete_flag_drops_the_column_and_sets_predicates() -> None:
    """A delete flag stays out of the column maps and gates every MERGE action."""
    spec = UpsertSpec(
        table_ref=TableRef("test.table"),
        upsert_keys=("id",),
        delete_flag="_deleted",
    )
    plan = _build_merge_plan(
        combos=[],
        spec=spec,
        df_columns=("id", "name", "_deleted"),
        target_alias=TARGET_ALIAS,
        source_alias=SOURCE_ALIAS,
    )

    assert plan.predicate == "t.id = s.id"
    assert plan.update_set == {"name": "s.name"}
    assert plan.insert_values == {"id": "s.id", "name": "s.name"}
    assert plan.matched_delete_predicate == "coalesce(s._deleted, false)"
    assert plan.matched_update_predicate == "NOT coalesce(s._deleted, false)"
    assert plan.not_matched_insert_predicate == "NOT coalesce(s._deleted, false)"
