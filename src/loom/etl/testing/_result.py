"""StepResult — unified assertion interface for step test output."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from loom.etl.backends.polars._writer import _require_boolean_flag_column
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._table import UpsertSpec
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec
from loom.etl.schema._schema import LoomDtype


@dataclass(frozen=True)
class MergeMetrics:
    """Rows a Delta MERGE applied to the target table.

    Read from ``operationMetrics`` of the commit the step wrote.

    Args:
        inserted: Target rows inserted.
        updated: Target rows updated.
        deleted: Target rows deleted.
    """

    inserted: int
    updated: int
    deleted: int


@dataclass(frozen=True)
class StepResult:
    """Output of a :class:`PolarsStepRunner` or :class:`SparkStepRunner` run.

    Provides a backend-agnostic assertion surface.  The internal frame is
    always a materialised ``polars.DataFrame`` — Spark results are converted
    before constructing this object.

    Two counts answer different questions about an upsert with ``delete_flag``:

    * :attr:`deletes` — rows the step *emitted* with the flag set, before any
      MERGE.  Available with or without ``delta_root``.
    * :attr:`merged` — rows the MERGE *applied* to the table.  Only with
      ``delta_root`` and an upsert or update target.

    Args:
        _frame: Materialised output frame.
        target: Target spec of the step that produced the frame, if known.
        merge_metrics: Metrics of the MERGE commit, set by a ``delta_root``
            runner for upsert and update targets.

    Example::

        result = runner.run(MyStep, params)
        result.assert_count(10)
        result.assert_schema({"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64})
        result.show()
    """

    _frame: pl.DataFrame
    target: TargetSpec | None = None
    merge_metrics: MergeMetrics | None = None

    def to_polars(self) -> pl.DataFrame:
        """Return the materialised output as a ``polars.DataFrame``."""
        return self._frame

    @property
    def temp_name(self) -> str | None:
        """Temp name when the step's target is ``IntoTemp``, else ``None``."""
        if isinstance(self.target, (TempSpec, TempFanInSpec)):
            return self.target.temp_name
        return None

    @property
    def delete_flag(self) -> str | None:
        """``delete_flag`` of the step's upsert target, else ``None``."""
        if isinstance(self.target, UpsertSpec):
            return self.target.delete_flag
        return None

    @property
    def deletes(self) -> int:
        """Rows the step emitted with the target's ``delete_flag`` set to true.

        Counts the step output, not what the MERGE did: use :attr:`merged`
        for that.

        Raises:
            RuntimeError: When the target is not an upsert with ``delete_flag``.
            UpsertDeleteFlagError: When the output has no flag column, or it
                is not boolean.
        """
        flag = self.delete_flag
        if flag is None:
            raise RuntimeError(
                "deletes needs a step whose target is IntoTable(...).upsert(delete_flag=...)."
            )
        _require_boolean_flag_column(self._frame.schema, flag)
        return int(self._frame.select(pl.col(flag).fill_null(False).sum()).item())

    @property
    def merged(self) -> MergeMetrics:
        """Rows the MERGE applied to the target table.

        Raises:
            RuntimeError: When the runner has no ``delta_root``, the target is
                not an upsert or update, or the target table did not exist and
                was created by this write instead of merged.
        """
        if self.merge_metrics is None:
            raise RuntimeError(
                "merged is unavailable: no MERGE ran because the runner has no "
                "delta_root, the target is not an upsert or update, "
                "or the target table did not exist and was created by this write. "
                "Check the final table with runner.table(ref).assert_count(n) instead."
            )
        return self.merge_metrics

    def assert_schema(self, expected: dict[str, LoomDtype]) -> None:
        """Assert that the output columns match the expected Loom schema.

        Args:
            expected: Mapping of column name → :class:`~loom.etl._schema.LoomDtype`.

        Raises:
            AssertionError: When a column is missing or has a different dtype.
        """
        from loom.etl.backends.polars._dtype import loom_type_to_polars

        for col_name, loom_dtype in expected.items():
            actual = self._frame.schema.get(col_name)
            if actual is None:
                raise AssertionError(f"Column '{col_name}' not found in result schema")
            expected_polars = loom_type_to_polars(loom_dtype)
            if actual != expected_polars:
                raise AssertionError(
                    f"Column '{col_name}': expected {expected_polars}, got {actual}"
                )

    def assert_count(self, n: int) -> None:
        """Assert the result contains exactly *n* rows.

        Args:
            n: Expected row count.

        Raises:
            AssertionError: When the row count differs.
        """
        actual = len(self._frame)
        if actual != n:
            raise AssertionError(f"Expected {n} rows, got {actual}")

    def assert_not_empty(self) -> None:
        """Assert the result contains at least one row.

        Raises:
            AssertionError: When the result is empty.
        """
        if len(self._frame) <= 0:
            raise AssertionError("Expected non-empty result, got 0 rows")

    def show(self, n: int = 10) -> None:
        """Print the first *n* rows to stdout.

        Useful for visual inspection during ``pytest -s`` runs.

        Args:
            n: Number of rows to display. Defaults to 10.
        """
        print(self._frame.head(n))
