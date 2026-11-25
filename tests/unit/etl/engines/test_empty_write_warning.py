"""An empty write is legal but must never pass silently."""

from __future__ import annotations

import importlib
import logging
from typing import Any

import polars as pl
import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.engines._write_policy import _WritePolicy
from loom.etl.engines.polars._writer import PolarsTargetWriter
from loom.etl.event import EtlEvent
from loom.etl.storage._locator import TableLocation
from loom.etl.storage.routing import PathTarget
from tests.helpers.loggers import CapturedLog
from tests.helpers.version_limited_extras import require_pyspark

_WARNING_EVENT = EtlEvent.WRITE_PRODUCED_NO_ROWS


@pytest.fixture
def writer() -> PolarsTargetWriter:
    return PolarsTargetWriter("/tmp/loom-empty-write-test")


@pytest.fixture
def target() -> PathTarget:
    return PathTarget(
        logical_ref=TableRef("silver.players"),
        location=TableLocation(uri="/tmp/players"),
    )


class TestRowCountIfCheap:
    def test_a_collected_frame_reports_its_height(self, writer: PolarsTargetWriter) -> None:
        assert writer._row_count_if_cheap(pl.DataFrame({"a": [1, 2, 3]})) == 3

    def test_an_empty_frame_reports_zero(self, writer: PolarsTargetWriter) -> None:
        assert writer._row_count_if_cheap(pl.DataFrame({"a": []}, schema={"a": pl.Int64})) == 0


class TestMaterializeChecked:
    def test_an_empty_frame_warns_naming_the_target(
        self, writer: PolarsTargetWriter, target: PathTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Asserted through stdlib logging, which is where a configured app emits.

        Reading stdout instead only worked while structlog was left
        unconfigured and fell back to printing — a pipeline no application
        ever runs.
        """
        empty = pl.DataFrame({"a": []}, schema={"a": pl.Int64}).lazy()

        with caplog.at_level(logging.WARNING):
            writer._materialize_checked(empty, target, streaming=False)

        emitted = CapturedLog(caplog).rendered_text
        assert _WARNING_EVENT in emitted
        assert "silver.players" in emitted

    def test_a_populated_frame_stays_quiet(
        self, writer: PolarsTargetWriter, target: PathTarget, caplog: pytest.LogCaptureFixture
    ) -> None:
        populated = pl.DataFrame({"a": [1]}).lazy()

        with caplog.at_level(logging.WARNING):
            writer._materialize_checked(populated, target, streaming=False)

        assert _WARNING_EVENT not in CapturedLog(caplog).rendered_text

    def test_the_frame_is_returned_unchanged(
        self, writer: PolarsTargetWriter, target: PathTarget
    ) -> None:
        frame = pl.DataFrame({"a": [1, 2]}).lazy()

        materialized = writer._materialize_checked(frame, target, streaming=False)

        assert materialized.to_dicts() == [{"a": 1}, {"a": 2}]

    def test_the_frame_is_collected_exactly_once(
        self, writer: PolarsTargetWriter, target: PathTarget
    ) -> None:
        """The check reads an already collected frame; it never re-runs the plan."""
        collected: list[int] = []

        class _CountingFrame:
            def collect(self, **_: Any) -> pl.DataFrame:
                collected.append(1)
                return pl.DataFrame({"a": []}, schema={"a": pl.Int64})

        writer._materialize_checked(_CountingFrame(), target, streaming=False)  # type: ignore[arg-type]

        assert collected == [1]


class TestBackendsThatCannotCountCheaply:
    def test_the_default_hook_reports_unknown_instead_of_scanning(self) -> None:
        """Spark would need an action to answer, so the base must stay silent."""
        assert _WritePolicy._row_count_if_cheap(object(), object()) is None  # type: ignore[arg-type]


class TestConcreteWriterHooks:
    @pytest.mark.parametrize(
        ("module", "writer"),
        [
            ("loom.etl.engines.polars", "PolarsTargetWriter"),
            ("loom.etl.engines.spark", "SparkTargetWriter"),
        ],
    )
    def test_every_concrete_writer_implements_the_abstract_hooks(
        self, module: str, writer: str
    ) -> None:
        if module.endswith("spark"):
            require_pyspark()
        writer_type = getattr(importlib.import_module(module), writer)

        assert issubclass(writer_type, _WritePolicy)
        assert not getattr(writer_type, "__abstractmethods__", frozenset())
