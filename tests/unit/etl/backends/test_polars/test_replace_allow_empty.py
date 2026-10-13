"""IntoTable(...).replace(allow_empty=False) refuses to wipe a table with an empty frame."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl import EmptyReplaceError, IntoTable
from loom.etl.backends.polars import PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._locator import TableLocation
from loom.etl.storage.routing import PathTarget

from .conftest import table_path

_REF = TableRef("silver.dim_dealer")
_EMPTY = pl.DataFrame({"id": [], "name": []}, schema={"id": pl.Int64, "name": pl.String})


def _writer(root: Path) -> PolarsTargetWriter:
    return PolarsTargetWriter(root, missing_table_policy=MissingTablePolicy.CREATE)


def _seed(root: Path) -> Path:
    path = table_path(root, _REF)
    path.mkdir(parents=True)
    write_deltalake(str(path), pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    return path


def _rows(path: Path) -> list[dict[str, Any]]:
    return pl.read_delta(str(path)).sort("id").to_dicts()


def _spec(*, allow_empty: bool) -> Any:
    return IntoTable(_REF).replace(allow_empty=allow_empty)._to_spec()


def test_an_empty_frame_leaves_the_existing_table_untouched(tmp_path: Path) -> None:
    path = _seed(tmp_path)
    version = DeltaTable(str(path)).version()

    with pytest.raises(EmptyReplaceError, match="silver.dim_dealer") as exc_info:
        _writer(tmp_path).write(_EMPTY.lazy(), _spec(allow_empty=False), None)

    assert exc_info.value.table_ref == "silver.dim_dealer"
    assert DeltaTable(str(path)).version() == version
    assert _rows(path) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_an_empty_frame_does_not_create_a_missing_table(tmp_path: Path) -> None:
    with pytest.raises(EmptyReplaceError):
        _writer(tmp_path).write(_EMPTY.lazy(), _spec(allow_empty=False), None)

    assert not table_path(tmp_path, _REF).exists()


def test_rows_replace_the_table_as_usual(tmp_path: Path) -> None:
    path = _seed(tmp_path)
    frame = pl.DataFrame({"id": [3], "name": ["c"]}).lazy()

    _writer(tmp_path).write(frame, _spec(allow_empty=False), None)

    assert _rows(path) == [{"id": 3, "name": "c"}]


def test_by_default_an_empty_frame_still_empties_the_table(tmp_path: Path) -> None:
    path = _seed(tmp_path)

    _writer(tmp_path).write(_EMPTY.lazy(), _spec(allow_empty=True), None)

    assert _rows(path) == []


def test_the_check_reads_the_frame_the_write_collects(tmp_path: Path) -> None:
    """The guard costs no second collect: it reads the height of the collected frame."""
    collected: list[int] = []

    class _CountingFrame:
        def collect(self, **_: Any) -> pl.DataFrame:
            collected.append(1)
            return _EMPTY

    target = PathTarget(logical_ref=_REF, location=TableLocation(uri=str(tmp_path)))

    with pytest.raises(EmptyReplaceError):
        _writer(tmp_path)._materialize_checked(
            _CountingFrame(),  # type: ignore[arg-type]
            target,
            streaming=False,
            allow_empty=False,
        )

    assert collected == [1]
