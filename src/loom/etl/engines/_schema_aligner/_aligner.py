"""Schema alignment protocol."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

FrameT = TypeVar("FrameT")
SchemaT = TypeVar("SchemaT", contravariant=True)
ExprT = TypeVar("ExprT")


class SchemaAligner(Protocol[FrameT, SchemaT, ExprT]):
    def get_frame_columns(self, frame: FrameT) -> set[str]: ...

    def iter_schema(self, schema: SchemaT) -> list[tuple[str, Any]]: ...

    def cast_column(self, name: str, dtype: Any) -> tuple[str, ExprT]: ...

    def null_column(self, name: str, dtype: Any) -> tuple[str, ExprT]: ...

    def apply_casts(self, frame: FrameT, casts: list[tuple[str, ExprT]]) -> FrameT: ...

    def select_columns(self, frame: FrameT, names: list[str]) -> FrameT: ...
