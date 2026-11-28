"""Shared schema alignment policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from loom.core.errors import InvalidUse
from loom.etl.declarative.target import SchemaMode
from loom.etl.engines._schema_aligner._aligner import SchemaAligner

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT")


@dataclass(frozen=True)
class AlignmentDecision:
    present: list[tuple[str, Any]]
    missing: list[tuple[str, Any]]
    extra: list[str]
    source_types: dict[str, Any]


class SchemaAlignmentPolicy(Generic[FrameT, SchemaT]):
    def __init__(self, aligner: SchemaAligner[FrameT, SchemaT, Any]) -> None:
        self._aligner = aligner

    def align(self, frame: FrameT, schema: SchemaT | None, mode: SchemaMode) -> FrameT:
        if mode is SchemaMode.OVERWRITE:
            return frame

        if schema is None:
            raise InvalidUse(
                f"No table schema to align the frame to under SchemaMode.{mode.name}. "
                "Pass the table's schema, or use SchemaMode.OVERWRITE, which skips alignment."
            )

        decision = self._analyze(frame, schema)
        return self._apply(frame, decision, mode)

    def _analyze(self, frame: FrameT, schema: SchemaT) -> AlignmentDecision:
        source_types = self._aligner.column_types(frame)
        frame_cols = set(source_types)
        schema_fields = self._aligner.iter_schema(schema)

        present = [(name, dtype) for name, dtype in schema_fields if name in frame_cols]
        missing = [(name, dtype) for name, dtype in schema_fields if name not in frame_cols]
        extra = [c for c in frame_cols if c not in {f[0] for f in schema_fields}]

        return AlignmentDecision(
            present=present, missing=missing, extra=extra, source_types=source_types
        )

    def _apply(self, frame: FrameT, decision: AlignmentDecision, mode: SchemaMode) -> FrameT:
        casts: list[tuple[str, Any]] = []

        for name, dtype in decision.present:
            casts.append(self._aligner.cast_column(name, dtype, decision.source_types[name]))

        for name, dtype in decision.missing:
            casts.append(self._aligner.null_column(name, dtype))

        if casts:
            frame = self._aligner.apply_casts(frame, casts)

        if mode is SchemaMode.STRICT:
            schema_order = [name for name, _ in decision.present + decision.missing]
            frame = self._aligner.select_columns(frame, schema_order)

        return frame
