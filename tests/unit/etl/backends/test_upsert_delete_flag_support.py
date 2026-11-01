"""Writers that do not implement upsert delete_flag refuse it."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.schema import SchemaMode
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import UnsupportedUpsertDeleteFlagError
from loom.etl.declarative.target._history import HistorifySpec
from loom.etl.declarative.target._table import UpsertSpec
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget


class _UnsupportedWriter(_WritePolicy[list[int], list[int], dict[str, Any]]):
    """Concrete writer that leaves delete_flag to the base policy."""

    def __init__(self) -> None:
        resolver = MagicMock()
        resolver.resolve.return_value = MagicMock(spec=ResolvedTarget)
        super().__init__(resolver=resolver, missing_table_policy=MissingTablePolicy.SCHEMA_MODE)

    def _physical_schema(self, target: ResolvedTarget) -> dict[str, Any] | None:
        return {"id": "int"}

    def _align(
        self, frame: list[int], existing: dict[str, Any] | None, mode: SchemaMode
    ) -> list[int]:
        return frame

    def _materialize_for_write(self, frame: list[int], streaming: bool) -> list[int]:
        return list(frame)

    def _pin_rows(self, frame: list[int]) -> tuple[list[int], bool]:
        return frame, not frame

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        return ""

    def _create(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        return None

    def _append(self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        return None

    def _replace(
        self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        return None

    def _replace_partitions(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        return None

    def _replace_where(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        return None

    def _upsert(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: dict[str, Any],
    ) -> None:
        return None

    def _update(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        spec: Any,
        existing_schema: dict[str, Any],
    ) -> None:
        return None

    def _read_existing_data(
        self, target: ResolvedTarget, frame: list[int], spec: HistorifySpec
    ) -> list[int] | None:
        return None

    def _historify_backend(self) -> Any:
        return object()

    def _write_file(
        self, frame: list[int], spec: Any, params_instance: Any, /, *, streaming: bool
    ) -> None:
        return None


def test_write_policy_refuses_upsert_delete_flag_when_the_writer_does_not_implement_it() -> None:
    writer = _UnsupportedWriter()
    spec = UpsertSpec(
        table_ref=TableRef("test.orders"),
        upsert_keys=("id",),
        delete_flag="_deleted",
    )
    with pytest.raises(
        UnsupportedUpsertDeleteFlagError, match=r"does not support upsert\(delete_flag='_deleted'\)"
    ):
        writer._do_upsert([1], MagicMock(spec=ResolvedTarget), spec, False)
