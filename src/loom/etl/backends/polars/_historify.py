"""Polars HistorifyBackend implementation."""

from __future__ import annotations

from typing import Any, TypeVar

import polars as pl

from loom.etl.backends._historify._common import prev_period_value
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistoryDateType,
)

_FrameT = TypeVar("_FrameT", pl.DataFrame, pl.LazyFrame)


def _history_boundary_dtype(spec: HistorifySpec) -> type[pl.Date] | pl.Datetime:
    return pl.Date if spec.date_type is HistoryDateType.DATE else pl.Datetime("us", "UTC")


class PolarsHistorifyBackend:
    """Polars implementation of HistorifyBackend."""

    def columns(self, frame: pl.DataFrame) -> list[str]:
        return list(frame.columns)

    def history_dtype(self, spec: HistorifySpec) -> type[pl.Date] | pl.Datetime:
        return _history_boundary_dtype(spec)

    def filter_null(self, frame: pl.DataFrame, col: str) -> pl.DataFrame:
        return frame.filter(pl.col(col).is_null())

    def filter_not_null(self, frame: pl.DataFrame, col: str) -> pl.DataFrame:
        return frame.filter(pl.col(col).is_not_null())

    def filter_eq(self, frame: pl.DataFrame, col: str, value: Any, dtype: Any) -> pl.DataFrame:
        expr = pl.lit(value).cast(dtype) if dtype is not None else pl.lit(value)
        return frame.filter(pl.col(col) == expr)

    def filter_ne(self, frame: pl.DataFrame, col: str, value: Any, dtype: Any) -> pl.DataFrame:
        expr = pl.lit(value).cast(dtype) if dtype is not None else pl.lit(value)
        return frame.filter(pl.col(col) != expr)

    def anti_join(self, left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame:
        # `on` carries the tracked columns: a null must match itself or the row
        # reads as changed and reopens on every run.
        return left.join(right, on=on, how="anti", nulls_equal=True)

    def semi_join(self, left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> pl.DataFrame:
        return left.join(right, on=on, how="semi", nulls_equal=True)

    def union(self, frames: list[pl.DataFrame]) -> pl.DataFrame:
        return pl.concat([self._utc_datetimes(f) for f in frames], how="diagonal_relaxed")

    @staticmethod
    def _utc_datetimes(frame: pl.DataFrame) -> pl.DataFrame:
        exprs = [
            pl.col(c).dt.replace_time_zone("UTC")
            if dt.time_zone is None
            else pl.col(c).dt.convert_time_zone("UTC")
            for c, dt in frame.schema.items()
            if isinstance(dt, pl.Datetime)
        ]
        return frame.with_columns(exprs) if exprs else frame

    def stamp_col(self, frame: pl.DataFrame, name: str, value: Any, dtype: Any) -> pl.DataFrame:
        if dtype is not None:
            return frame.with_columns(pl.lit(value).cast(dtype).alias(name))
        return frame.with_columns(pl.lit(value).alias(name))

    def null_col(self, frame: pl.DataFrame, name: str, dtype: Any) -> pl.DataFrame:
        return frame.with_columns(pl.lit(None, dtype=dtype).alias(name))

    def rename(self, frame: pl.DataFrame, rename_map: dict[str, str]) -> pl.DataFrame:
        return frame.rename(rename_map)

    def drop(self, frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        return frame.drop(cols)

    def dedup_priority(
        self, frame: pl.DataFrame, subset: list[str], priority_col: str
    ) -> pl.DataFrame:
        return frame.filter(pl.col(priority_col) == pl.col(priority_col).max().over(subset))

    def apply_overwrite_cols(
        self,
        unchanged: pl.DataFrame,
        incoming: pl.DataFrame,
        join_key: list[str],
        overwrite: tuple[str, ...],
    ) -> pl.DataFrame:
        overwrite_vals = incoming.select(join_key + list(overwrite))
        return unchanged.drop(list(overwrite)).join(
            overwrite_vals, on=join_key, how="left", nulls_equal=True
        )

    def rewind_to(
        self,
        frame: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> pl.DataFrame:
        boundary_dtype = _history_boundary_dtype(spec)
        prev = prev_period_value(eff_date, spec)

        kept = frame.filter(pl.col(spec.valid_from) < pl.lit(eff_date).cast(boundary_dtype))
        reopen = pl.col(spec.valid_to) >= pl.lit(prev).cast(boundary_dtype)
        null_boundary = pl.lit(None, dtype=boundary_dtype)

        exprs = [
            pl.when(reopen)
            .then(null_boundary)
            .otherwise(pl.col(spec.valid_to))
            .alias(spec.valid_to)
        ]
        if spec.deleted_at in frame.columns:
            exprs.append(
                pl.when(reopen)
                .then(null_boundary)
                .otherwise(pl.col(spec.deleted_at))
                .alias(spec.deleted_at)
            )
        return kept.with_columns(exprs)

    def collect_future_rows(
        self,
        existing: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> list[tuple[object, ...]]:
        boundary_dtype = _history_boundary_dtype(spec)
        future = existing.filter(pl.col(spec.valid_from) > pl.lit(eff_date).cast(boundary_dtype))
        return list(future.select([*spec.keys, spec.valid_from]).iter_rows())

    def build_log_boundaries(
        self,
        frame: pl.DataFrame,
        spec: HistorifySpec,
    ) -> pl.DataFrame:
        eff_col = str(spec.effective_date)
        entity_key = list(spec.keys)
        boundary_dtype = _history_boundary_dtype(spec)
        offset = (
            pl.duration(days=1)
            if spec.date_type is HistoryDateType.DATE
            else pl.duration(microseconds=1)
        )

        collapsed = self._collapse_same_track_runs(frame, spec, entity_key, eff_col)
        sorted_events = collapsed.sort(entity_key + [eff_col])
        next_date = pl.col(eff_col).shift(-1).over(entity_key)

        valid_from = pl.col(eff_col)
        # Clamp so valid_to is never below valid_from: two events sharing an
        # effective instant (e.g. task created and flipped in the same snapshot ms)
        # yield next_eff - offset < valid_from, an inverted interval. Collapse those
        # to a zero-width [T, T] vector instead of a negative one. The last event has
        # a null next_date -> keep valid_to null (open row); max_horizontal alone
        # would skip that null and wrongly close it.
        clamped_valid_to = (
            pl.when(next_date.is_null())
            .then(pl.lit(None))
            .otherwise(pl.max_horizontal(valid_from, next_date - offset))
        )

        return sorted_events.with_columns(
            valid_from.cast(boundary_dtype).alias(spec.valid_from),
            clamped_valid_to.cast(boundary_dtype).alias(spec.valid_to),
        ).drop(eff_col)

    @staticmethod
    def _collapse_same_track_runs(
        frame: _FrameT,
        spec: HistorifySpec,
        entity_key: list[str],
        eff_col: str,
    ) -> _FrameT:
        """Collapse consecutive same-``track`` events per entity into one run.

        valid_from anchors to the first event of the run (``eff_col`` min),
        ``overwrite`` columns refresh to the last observed value, and every other
        payload column freezes at its transition-time (first) value.
        """
        if spec.track is None:
            return frame

        track = list(spec.track)
        overwrite: list[str] = list(spec.overwrite or ())
        track_struct = pl.struct(track)

        runs = (
            frame.sort(entity_key + [eff_col])
            .with_columns(
                track_struct.ne_missing(track_struct.shift().over(entity_key))
                .fill_null(True)
                .alias("__new_run__")
            )
            .with_columns(pl.col("__new_run__").cum_sum().over(entity_key).alias("__run_id__"))
        )

        reserved = {*entity_key, "__new_run__", "__run_id__", eff_col, *overwrite}
        frozen_cols: list[str] = [c for c in frame.collect_schema().names() if c not in reserved]
        aggs = [
            pl.col(eff_col).min().alias(eff_col),
            *(pl.col(c).last().alias(c) for c in overwrite),
            *(pl.col(c).first().alias(c) for c in frozen_cols),
        ]
        return (
            runs.group_by([*entity_key, "__run_id__"], maintain_order=True)
            .agg(aggs)
            .drop("__run_id__")
        )

    def apply_delete_policy(
        self,
        deleted: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> pl.DataFrame:
        if spec.delete_policy is DeletePolicy.IGNORE:
            return deleted

        boundary_dtype = _history_boundary_dtype(spec)
        prev = prev_period_value(eff_date, spec)
        result = deleted.with_columns(pl.lit(prev).cast(boundary_dtype).alias(spec.valid_to))

        if spec.delete_policy is DeletePolicy.SOFT_DELETE:
            result = result.with_columns(
                pl.lit(eff_date).cast(boundary_dtype).alias(spec.deleted_at)
            )

        return result

    def ensure_soft_delete_col(self, result: pl.DataFrame, spec: HistorifySpec) -> pl.DataFrame:
        if spec.delete_policy is not DeletePolicy.SOFT_DELETE:
            return result
        if spec.deleted_at in result.columns:
            return result
        boundary_dtype = _history_boundary_dtype(spec)
        return result.with_columns(pl.lit(None, dtype=boundary_dtype).alias(spec.deleted_at))

    def assert_unique_keys(self, frame: pl.DataFrame, keys: list[str]) -> None:
        if len(frame) == frame.select(keys).n_unique():
            return
        dupes = frame.filter(frame.select(keys).is_duplicated()).select(keys).head(5)
        raise HistorifyKeyConflictError(str(dupes.to_dicts()))

    def assert_no_date_collisions(
        self,
        frame: pl.DataFrame,
        keys: list[str],
        eff_col: str,
        spec: HistorifySpec,
    ) -> None:
        if spec.date_type is not HistoryDateType.DATE:
            return
        combo_key = keys + [eff_col]
        if len(frame) == frame.select(combo_key).n_unique():
            return
        dupes = frame.filter(frame.select(combo_key).is_duplicated()).select(combo_key).head(5)
        raise HistorifyDateCollisionError(str(dupes.to_dicts()))

    def temporal_conflict_min_date(
        self,
        existing: pl.DataFrame,
        spec: HistorifySpec,
        eff_date: Any,
    ) -> Any | None:
        boundary_dtype = _history_boundary_dtype(spec)
        conflict = existing.filter(pl.col(spec.valid_from) > pl.lit(eff_date).cast(boundary_dtype))
        if conflict.is_empty():
            return None
        return conflict[spec.valid_from].min()
