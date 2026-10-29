"""Spark refuses upsert(delete_flag=...) without opening a session."""

from __future__ import annotations

from typing import Any, cast

import pytest

from loom.etl.backends.spark._writer import SparkTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import UnsupportedUpsertDeleteFlagError
from loom.etl.declarative.target._table import UpsertSpec


def _spec() -> UpsertSpec:
    return UpsertSpec(
        table_ref=TableRef("test.orders"),
        upsert_keys=("id",),
        delete_flag="_deleted",
    )


def _writer() -> SparkTargetWriter:
    return SparkTargetWriter.__new__(SparkTargetWriter)


def test_spark_do_upsert_rejects_delete_flag() -> None:
    with pytest.raises(
        UnsupportedUpsertDeleteFlagError,
        match=r"SparkTargetWriter does not support upsert\(delete_flag='_deleted'\)",
    ):
        _writer()._do_upsert(cast(Any, None), cast(Any, None), _spec(), False)


def test_spark_upsert_rejects_delete_flag() -> None:
    with pytest.raises(
        UnsupportedUpsertDeleteFlagError,
        match=r"SparkTargetWriter does not support upsert\(delete_flag='_deleted'\)",
    ):
        _writer()._upsert(
            cast(Any, None),
            cast(Any, None),
            spec=_spec(),
            existing_schema=cast(Any, None),
        )
