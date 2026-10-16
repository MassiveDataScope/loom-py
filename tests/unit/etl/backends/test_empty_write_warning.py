"""An empty write is legal but must never pass silently."""

from __future__ import annotations

import importlib
import logging
from typing import Any

import polars as pl
import pytest

from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.backends.polars._writer import PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.storage._locator import TableLocation
from loom.etl.storage.routing import PathTarget
from tests.helpers.version_limited_extras import require_pyspark

_WARNING_EVENT = "write_produced_no_rows"


def _writer() -> PolarsTargetWriter:
    return PolarsTargetWriter("/tmp/loom-empty-write-test")


def _target() -> PathTarget:
    return PathTarget(
        logical_ref=TableRef("silver.players"),
        location=TableLocation(uri="/tmp/players"),
    )


class TestRowCountIfCheap:
    def test_a_collected_frame_reports_its_height(self) -> None:
        assert _writer()._row_count_if_cheap(pl.DataFrame({"a": [1, 2, 3]})) == 3

    def test_an_empty_frame_reports_zero(self) -> None:
        assert _writer()._row_count_if_cheap(pl.DataFrame({"a": []}, schema={"a": pl.Int64})) == 0


class TestMaterializeChecked:
    def test_an_empty_frame_warns_naming_the_target(self, caplog: pytest.LogCaptureFixture) -> None:
        """Asserted through stdlib logging, which is where a configured app emits.

        Reading stdout instead only worked while structlog was left
        unconfigured and fell back to printing — a pipeline no application
        ever runs.
        """
        writer = _writer()
        empty = pl.DataFrame({"a": []}, schema={"a": pl.Int64}).lazy()

        with caplog.at_level(logging.WARNING):
            writer._materialize_checked(empty, _target(), streaming=False)

        emitted = " ".join(record.getMessage() for record in caplog.records)
        assert _WARNING_EVENT in emitted
        assert "silver.players" in emitted

    def test_a_populated_frame_stays_quiet(self, caplog: pytest.LogCaptureFixture) -> None:
        writer = _writer()
        populated = pl.DataFrame({"a": [1]}).lazy()

        with caplog.at_level(logging.WARNING):
            writer._materialize_checked(populated, _target(), streaming=False)

        assert _WARNING_EVENT not in " ".join(r.getMessage() for r in caplog.records)

    def test_the_frame_is_returned_unchanged(self) -> None:
        writer = _writer()
        frame = pl.DataFrame({"a": [1, 2]}).lazy()

        materialized = writer._materialize_checked(frame, _target(), streaming=False)

        assert materialized.to_dicts() == [{"a": 1}, {"a": 2}]

    def test_the_frame_is_collected_exactly_once(self) -> None:
        """The check reads an already collected frame; it never re-runs the plan."""
        writer = _writer()
        collected: list[int] = []

        class _CountingFrame:
            def collect(self, **_: Any) -> pl.DataFrame:
                collected.append(1)
                return pl.DataFrame({"a": []}, schema={"a": pl.Int64})

        writer._materialize_checked(_CountingFrame(), _target(), streaming=False)  # type: ignore[arg-type]

        assert collected == [1]


class TestBackendsThatCannotCountCheaply:
    def test_the_default_hook_reports_unknown_instead_of_scanning(self) -> None:
        """Spark would need an action to answer, so the base must stay silent."""
        assert _WritePolicy._row_count_if_cheap(object(), object()) is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("module", "writer"),
    [
        ("loom.etl.backends.polars", "PolarsTargetWriter"),
        ("loom.etl.backends.spark", "SparkTargetWriter"),
    ],
)
def test_every_concrete_writer_implements_the_abstract_hooks(module: str, writer: str) -> None:
    if module.endswith("spark"):
        require_pyspark()
    writer_type = getattr(importlib.import_module(module), writer)

    assert issubclass(writer_type, _WritePolicy)
    assert not getattr(writer_type, "__abstractmethods__", frozenset())
