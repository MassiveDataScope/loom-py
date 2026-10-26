"""Spark HistorifyBackend implementation."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

from loom.etl.backends._historify._common import log_run_cols, prev_period_value
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifySpec,
    HistoryDateType,
)


def _history_boundary_dtype_sql(spec: HistorifySpec) -> str:
    return "date" if spec.date_type is HistoryDateType.DATE else "timestamp"


def _next_period(col: Column, spec: HistorifySpec) -> Column:
    if spec.date_type is HistoryDateType.DATE:
        return F.date_add(col, 1)
    return col + F.expr("INTERVAL 1 MICROSECOND")


class SparkHistorifyBackend:
    """Spark implementation of HistorifyBackend."""

    def columns(self, frame: DataFrame) -> list[str]:
        return list(frame.columns)

    def non_boolean_dtype(self, frame: DataFrame, col: str) -> str | None:
        dtype = frame.schema[col].dataType
        return None if isinstance(dtype, BooleanType) else dtype.simpleString()

    def history_dtype(self, spec: HistorifySpec) -> str:
        return _history_boundary_dtype_sql(spec)

    def filter_null(self, frame: DataFrame, col: str) -> DataFrame:
        return frame.filter(F.col(col).isNull())

    def filter_not_null(self, frame: DataFrame, col: str) -> DataFrame:
        return frame.filter(F.col(col).isNotNull())

    def filter_eq(self, frame: DataFrame, col: str, value: Any, dtype: Any) -> DataFrame:
        expr = F.lit(value).cast(dtype) if dtype is not None else F.lit(value)
        return frame.filter(F.col(col) == expr)

    def filter_ne(self, frame: DataFrame, col: str, value: Any, dtype: Any) -> DataFrame:
        expr = F.lit(value).cast(dtype) if dtype is not None else F.lit(value)
        return frame.filter(F.col(col) != expr)

    def anti_join(self, left: DataFrame, right: DataFrame, on: list[str]) -> DataFrame:
        return left.join(right, on=self._null_safe_condition(left, right, on), how="left_anti")

    def semi_join(self, left: DataFrame, right: DataFrame, on: list[str]) -> DataFrame:
        return left.join(right, on=self._null_safe_condition(left, right, on), how="left_semi")

    @staticmethod
    def _null_safe_condition(left: DataFrame, right: DataFrame, on: list[str]) -> Any:
        # eqNullSafe: a null in a tracked column must compare equal to itself,
        # not act as SQL NULL (never matches). Otherwise a null-valued row is
        # forever "changed" and reopened on every run. left_semi/left_anti only
        # project the left side, so an explicit condition (vs. `on=list`) does
        # not introduce duplicate key columns here.
        condition = left[on[0]].eqNullSafe(right[on[0]])
        for col in on[1:]:
            condition = condition & left[col].eqNullSafe(right[col])
        return condition

    def union(self, frames: list[DataFrame]) -> DataFrame:
        result = frames[0]
        for f in frames[1:]:
            result = result.unionByName(f, allowMissingColumns=True)
        return result

    def stamp_col(self, frame: DataFrame, name: str, value: Any, dtype: Any) -> DataFrame:
        if dtype is not None:
            return frame.withColumn(name, F.lit(value).cast(dtype))
        return frame.withColumn(name, F.lit(value))

    def null_col(self, frame: DataFrame, name: str, dtype: Any) -> DataFrame:
        return frame.withColumn(name, F.lit(None).cast(dtype))

    def fill_null(self, frame: DataFrame, col: str, value: Any) -> DataFrame:
        return frame.withColumn(col, F.coalesce(F.col(col), F.lit(value)))

    def rename(self, frame: DataFrame, rename_map: dict[str, str]) -> DataFrame:
        result = frame
        for old, new in rename_map.items():
            result = result.withColumnRenamed(old, new)
        return result

    def drop(self, frame: DataFrame, cols: list[str]) -> DataFrame:
        return frame.drop(*cols)

    def dedup_priority(self, frame: DataFrame, subset: list[str], priority_col: str) -> DataFrame:
        # Whole partition, stated explicitly: the max must be taken over every row
        # of the key group. Spark only defaults to that frame while the window
        # stays unordered — adding an orderBy later would silently turn it into a
        # running max and keep the wrong row.
        window = Window.partitionBy(subset).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        return (
            frame.withColumn("__max_priority__", F.max(priority_col).over(window))
            .filter(F.col(priority_col) == F.col("__max_priority__"))
            .drop("__max_priority__")
        )

    def apply_overwrite_cols(
        self,
        unchanged: DataFrame,
        incoming: DataFrame,
        join_key: list[str],
        overwrite: tuple[str, ...],
    ) -> DataFrame:
        # eqNullSafe join_key: same null-safety as anti_join/semi_join, so an
        # entity whose join_key includes a null track column still matches its
        # own overwrite row. An explicit condition (vs. `on=list`) does not
        # dedupe the key columns, so rename incoming's copy and drop it after.
        key_rename = {k: f"__ov_key_{k}__" for k in join_key}
        overwrite_vals = incoming.select(join_key + list(overwrite))
        for old, new in key_rename.items():
            overwrite_vals = overwrite_vals.withColumnRenamed(old, new)

        condition = F.col(join_key[0]).eqNullSafe(F.col(key_rename[join_key[0]]))
        for k in join_key[1:]:
            condition = condition & F.col(k).eqNullSafe(F.col(key_rename[k]))

        joined = unchanged.drop(*overwrite).join(overwrite_vals, condition, how="left")
        return joined.drop(*key_rename.values())

    def rewind_to(
        self,
        frame: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> DataFrame:
        boundary_dtype = _history_boundary_dtype_sql(spec)
        prev = prev_period_value(eff_date, spec)
        null_boundary = F.lit(None).cast(boundary_dtype)

        kept = frame.filter(F.col(spec.valid_from) < F.lit(eff_date).cast(boundary_dtype))
        # Flag before mutating valid_to so deleted_at sees the original values.
        flagged = kept.withColumn(
            "__reopen__", F.col(spec.valid_to) >= F.lit(prev).cast(boundary_dtype)
        )
        result = flagged.withColumn(
            spec.valid_to,
            F.when(F.col("__reopen__"), null_boundary).otherwise(F.col(spec.valid_to)),
        )
        if spec.deleted_at in frame.columns:
            result = result.withColumn(
                spec.deleted_at,
                F.when(F.col("__reopen__"), null_boundary).otherwise(F.col(spec.deleted_at)),
            )
        return result.drop("__reopen__")

    def collect_future_rows(
        self,
        existing: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> list[tuple[object, ...]]:
        boundary_dtype = _history_boundary_dtype_sql(spec)
        collected = (
            existing.filter(F.col(spec.valid_from) > F.lit(eff_date).cast(boundary_dtype))
            .select(*spec.keys, spec.valid_from)
            .collect()
        )
        return [tuple(row) for row in collected]

    def build_log_boundaries(
        self,
        frame: DataFrame,
        spec: HistorifySpec,
    ) -> DataFrame:
        eff_col = str(spec.effective_date)
        entity_key = list(spec.keys)
        boundary_dtype = _history_boundary_dtype_sql(spec)

        collapsed = self._collapse_same_track_runs(frame, spec, entity_key, eff_col)

        # Lead/Lag in Spark do not accept any explicit window frame.
        window = Window.partitionBy(entity_key).orderBy(eff_col)
        next_date = F.lead(eff_col).over(window)

        if spec.date_type is HistoryDateType.DATE:
            valid_to_expr = F.date_sub(next_date, 1)
        else:
            valid_to_expr = next_date - F.expr("INTERVAL 1 MICROSECOND")

        # Clamp so valid_to is never below valid_from: two events sharing an
        # effective instant (e.g. task created and flipped in the same snapshot ms)
        # yield next_eff - offset < valid_from, an inverted interval. Collapse those
        # to a zero-width [T, T] vector instead of a negative one. The last event has
        # a null next_date -> keep valid_to null (open row); greatest alone would skip
        # that null and wrongly close it.
        clamped_valid_to = F.when(next_date.isNull(), F.lit(None)).otherwise(
            F.greatest(F.col(eff_col), valid_to_expr)
        )

        return (
            collapsed.withColumn(spec.valid_from, F.col(eff_col).cast(boundary_dtype))
            .withColumn(spec.valid_to, clamped_valid_to.cast(boundary_dtype))
            .drop(eff_col)
        )

    def closure_events(self, history: DataFrame, spec: HistorifySpec) -> DataFrame:
        entity_key = list(spec.keys)
        # Nulls last: a zero-width vector sorts before the vector sharing its valid_from.
        window = Window.partitionBy(entity_key).orderBy(
            F.col(spec.valid_from), F.col(spec.valid_to).asc_nulls_last()
        )
        closed_at = _next_period(F.col(spec.valid_to), spec)
        next_from = F.col("__next_from__")
        return (
            history.withColumn("__next_from__", F.lead(spec.valid_from).over(window))
            .filter(
                F.col(spec.valid_to).isNotNull() & (next_from.isNull() | (next_from > closed_at))
            )
            .select(
                *entity_key,
                closed_at.cast(_history_boundary_dtype_sql(spec)).alias(str(spec.effective_date)),
            )
        )

    @staticmethod
    def _collapse_same_track_runs(
        frame: DataFrame,
        spec: HistorifySpec,
        entity_key: list[str],
        eff_col: str,
    ) -> DataFrame:
        """Collapse consecutive same-``track`` events per entity into one run.

        valid_from anchors to the first event of the run (``eff_col`` min),
        ``overwrite`` columns refresh to the last observed value, and every other
        payload column freezes at its transition-time (first) value.
        """
        if spec.track is None:
            return frame

        overwrite = list(spec.overwrite or ())
        track_struct = F.struct(*[F.col(c) for c in log_run_cols(spec)])

        order_w = Window.partitionBy(entity_key).orderBy(eff_col)
        prev_struct = F.lag(track_struct).over(order_w)
        is_new_run = F.when(
            prev_struct.isNull() | ~track_struct.eqNullSafe(prev_struct), F.lit(1)
        ).otherwise(F.lit(0))

        run_sum_w = order_w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        with_run = frame.withColumn("__new_run__", is_new_run).withColumn(
            "__run_id__", F.sum("__new_run__").over(run_sum_w)
        )

        run_w = Window.partitionBy([*entity_key, "__run_id__"]).orderBy(eff_col)
        full_run_w = run_w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        result = with_run.withColumn("__rn__", F.row_number().over(run_w))
        for col in overwrite:
            result = result.withColumn(col, F.last(F.col(col)).over(full_run_w))
        return result.filter(F.col("__rn__") == 1).drop("__rn__", "__run_id__", "__new_run__")

    def apply_delete_policy(
        self,
        deleted: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> DataFrame:
        if spec.delete_policy is DeletePolicy.IGNORE:
            return deleted

        boundary_dtype = _history_boundary_dtype_sql(spec)
        prev = prev_period_value(eff_date, spec)
        result = deleted.withColumn(spec.valid_to, F.lit(prev).cast(boundary_dtype))

        if spec.delete_policy is DeletePolicy.SOFT_DELETE:
            result = result.withColumn(spec.deleted_at, F.lit(eff_date).cast(boundary_dtype))

        return result

    def ensure_soft_delete_col(self, result: DataFrame, spec: HistorifySpec) -> DataFrame:
        if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
            return result
        if spec.deleted_at in result.columns:
            return result
        boundary_dtype = _history_boundary_dtype_sql(spec)
        return result.withColumn(spec.deleted_at, F.lit(None).cast(boundary_dtype))

    def duplicate_keys(self, frame: DataFrame, keys: list[str]) -> str | None:
        dupes = (
            frame.groupBy(keys).count().filter(F.col("count") > 1).drop("count").limit(5).collect()
        )
        return str([row.asDict() for row in dupes]) if dupes else None

    def assert_no_date_collisions(
        self,
        frame: DataFrame,
        keys: list[str],
        eff_col: str,
        spec: HistorifySpec,
    ) -> None:
        if spec.date_type is not HistoryDateType.DATE:
            return
        combo_key = keys + [eff_col]
        n_rows = frame.count()
        n_unique = frame.select(combo_key).distinct().count()
        if n_rows == n_unique:
            return
        dupes = (
            frame.groupBy(combo_key)
            .count()
            .filter(F.col("count") > 1)
            .drop("count")
            .limit(5)
            .collect()
        )
        raise HistorifyDateCollisionError(str([row.asDict() for row in dupes]))

    def temporal_conflict_min_date(
        self,
        existing: DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> Any | None:
        boundary_dtype = _history_boundary_dtype_sql(spec)
        conflict_agg = (
            existing.filter(F.col(spec.valid_from) > F.lit(eff_date).cast(boundary_dtype))
            .agg(F.min(spec.valid_from).alias("min_valid_from"))
            .collect()
        )
        min_conflict = conflict_agg[0]["min_valid_from"]
        if min_conflict is None:
            return None
        return min_conflict
