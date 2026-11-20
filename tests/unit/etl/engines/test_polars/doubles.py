"""Shared doubles for Polars backend tests."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.engines.polars import PolarsTargetWriter
from loom.etl.storage._config import MissingTablePolicy

from .conftest import table_path


def read_delta_table(root: Path, ref: str) -> pl.DataFrame:
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


def seed_delta_table(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")


def make_polars_writer(
    root: Path,
    *,
    missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
) -> PolarsTargetWriter:
    return PolarsTargetWriter(str(root), missing_table_policy=missing_table_policy)
