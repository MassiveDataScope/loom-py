"""Compile-time checks of the JSON shapes a step declares."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.declarative._declared_table import ModelBoundSpec
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.schema import FrameType, JsonFit, JsonLeafUnsupported, JsonMismatch


def _declared_shapes(
    sources: Iterable[SourceSpec], target: TargetSpec
) -> Iterator[tuple[str, FrameType]]:
    for source in sources:
        if isinstance(source, TableSourceSpec | FileSourceSpec):
            for json_column in source.json_columns:
                yield f"{source.alias}.{json_column.column}", json_column.frame_type
    if isinstance(target, ModelBoundSpec) and target.model_table is not None:
        for column in target.model_table.nested_columns:
            yield f"target {target.table_ref.ref}.{column.name}", column.dtype


def validate_json_shapes(
    step_type: type[Any], sources: Iterable[SourceSpec], target: TargetSpec
) -> None:
    """Raise when a JSON shape the step declares has a leaf type JSON text cannot hold.

    Checks each ``parse_json`` type of a source and each nested column of a model target.

    Raises:
        ETLCompilationError: A leaf has no JSON rule, naming the column and the path of the leaf.
    """
    for where, frame_type in _declared_shapes(sources, target):
        try:
            JsonFit.of(frame_type, JsonMismatch.FAIL)
        except JsonLeafUnsupported as unsupported:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.JSON_LEAF_UNSUPPORTED.issue(
                    component=step_type,
                    where=where,
                    leaf=unsupported.leaf,
                    path=unsupported.path,
                )
            ) from unsupported
