"""SparkTargetWriter pins the rows of a frame without localCheckpoint() by persisting it.

A Spark Connect DataFrame of pyspark 3.5 raises ``NOT_IMPLEMENTED`` on
``localCheckpoint()``; the fake below stands for it, so no JVM is needed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.storagelevel import StorageLevel

from loom.etl import EmptyReplaceError, IntoTable
from loom.etl.backends.spark import SparkTargetWriter
from loom.etl.declarative.target import SchemaMode
from loom.etl.storage.routing import ResolvedTarget


class _ConnectLikeFrame:
    """Records the calls the writer makes on a frame that cannot be checkpointed."""

    isStreaming = False

    def __init__(self, rows: int) -> None:
        self._rows = rows
        self.calls: list[tuple[str, Any]] = []

    def localCheckpoint(self, eager: bool = True) -> Any:
        raise AssertionError("localCheckpoint() must not be called on this frame")

    def persist(self, storageLevel: StorageLevel) -> _ConnectLikeFrame:
        self.calls.append(("persist", storageLevel))
        return self

    def count(self) -> int:
        self.calls.append(("count", None))
        return self._rows

    def unpersist(self, blocking: bool = False) -> _ConnectLikeFrame:
        self.calls.append(("unpersist", blocking))
        return self


def _writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SparkTargetWriter:
    """A writer whose target table exists and whose alignment keeps the frame."""
    writer = SparkTargetWriter(object(), str(tmp_path))  # type: ignore[arg-type]
    monkeypatch.setattr(writer, "_physical_schema", lambda target: object())
    monkeypatch.setattr(writer, "_align", lambda frame, existing, mode: frame)
    return writer


def _spec() -> Any:
    return IntoTable("silver.dim_dealer").replace(allow_empty=False)._to_spec()


def test_the_frame_is_persisted_counted_written_and_released(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = _writer(tmp_path, monkeypatch)
    frame = _ConnectLikeFrame(rows=2)
    written: list[Any] = []

    def replace(written_frame: Any, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        written.append((written_frame, list(frame.calls)))

    monkeypatch.setattr(writer, "_replace", replace)

    writer.write(frame, _spec(), None)  # type: ignore[arg-type]

    assert written == [(frame, [("persist", StorageLevel.MEMORY_AND_DISK), ("count", None)])]
    assert frame.calls[-1][0] == "unpersist"


def test_an_empty_frame_is_released_before_the_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = _writer(tmp_path, monkeypatch)
    frame = _ConnectLikeFrame(rows=0)
    spec = _spec()

    with pytest.raises(EmptyReplaceError, match="silver.dim_dealer"):
        writer.write(frame, spec, None)  # type: ignore[arg-type]

    assert [name for name, _ in frame.calls] == ["persist", "count", "unpersist"]


def test_the_frame_is_released_when_the_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = _writer(tmp_path, monkeypatch)
    frame = _ConnectLikeFrame(rows=1)

    def fail(written_frame: Any, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        raise RuntimeError("write failed")

    monkeypatch.setattr(writer, "_replace", fail)
    spec = _spec()

    with pytest.raises(RuntimeError, match="write failed"):
        writer.write(frame, spec, None)  # type: ignore[arg-type]

    assert [name for name, _ in frame.calls] == ["persist", "count", "unpersist"]
