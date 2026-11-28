"""Compile-time checks of the JSON shapes a step declares."""

from __future__ import annotations

from typing import Any

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import DeclaredJsonColumn, SourceBinding, TargetBinding
from loom.etl.schema import JsonFit, JsonLeafUnsupported, JsonMismatch


def validate_json_shapes(
    step_type: type[Any], source_bindings: tuple[SourceBinding, ...], target_binding: TargetBinding
) -> None:
    """Raise when a JSON shape the step declares has a leaf type JSON text cannot hold.

    Checks each ``parse_json`` type of a source and each nested column of a model target.

    Raises:
        ETLCompilationError: A leaf has no JSON rule, naming the column and the path of the leaf.
    """
    for declared in DeclaredJsonColumn.of_step(source_bindings, target_binding):
        try:
            JsonFit.of(declared.frame_type, JsonMismatch.FAIL)
        except JsonLeafUnsupported as unsupported:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.JSON_LEAF_UNSUPPORTED.issue(
                    component=step_type,
                    where=declared.where,
                    reason=str(unsupported),
                    path=unsupported.path,
                )
            ) from unsupported
