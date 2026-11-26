"""Compile-time checks of a target declared with a model."""

from __future__ import annotations

import logging
from typing import Any

from loom.core.schema import SchemaMode
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.declarative.target import (
    AppendSpec,
    HistorifySpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    TargetSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._history._enums import HistoryDateType
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import ModelTable

_log = logging.getLogger(__name__)

_TableSpec = (
    AppendSpec
    | ReplaceSpec
    | ReplacePartitionsSpec
    | ReplaceWhereSpec
    | UpsertSpec
    | UpdateSpec
    | HistorifySpec
)


class ModelTargetCheck:
    """What a model target must satisfy before it compiles.

    Args:
        step_type: The step class being compiled, named in every message.
        spec: A table spec declared with a model.
        table: The lake table of that model.
    """

    def __init__(self, step_type: type[Any], spec: _TableSpec, table: ModelTable) -> None:
        self._step_type = step_type
        self._spec = spec
        self._table = table
        self._table_ref = spec.table_ref.ref

    def run(self) -> None:
        """Raise the first refusal, else log that the model resolved for the step."""
        self._check_schema_mode()
        self._check_declared_columns()
        self._check_merge_keys()
        self._check_history_date_type()
        _log.debug(EtlEvent.MODEL_RESOLVED, extra=self._log_fields())

    def _check_schema_mode(self) -> None:
        mode = self._spec.schema_mode
        if mode is not SchemaMode.STRICT:
            raise self._refusal(
                ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED, "schema", mode=mode.name
            )

    def _check_declared_columns(self) -> None:
        for role, names in self._spec.declared_columns.items():
            unknown = self._table.unknown_columns(names)
            if unknown:
                raise self._refusal(
                    ETLErrorCode.MODEL_COLUMN_UNKNOWN,
                    unknown[0],
                    role=role,
                    column=unknown[0],
                    known=self._table.column_names,
                )

    def _check_merge_keys(self) -> None:
        if not isinstance(self._spec, UpsertSpec | UpdateSpec) or self._spec.upsert_keys:
            return
        problem = self._table.key_problem()
        if problem is not None:
            raise self._refusal(
                ETLErrorCode.MODEL_KEY_NOT_IN_FRAME,
                problem.field,
                field=problem.field,
                reason=problem.reason,
            )

    def _check_history_date_type(self) -> None:
        spec = self._spec
        if not isinstance(spec, HistorifySpec):
            return
        column_type = self._table.column_type(spec.valid_from)
        actual = HistoryDateType.of_column(column_type)
        if actual is None:
            raise self._refusal(
                ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL,
                spec.valid_from,
                column=spec.valid_from,
                actual=column_type,
            )
        if actual is not spec.date_type:
            raise self._refusal(
                ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
                "date_type",
                declared=spec.date_type.value,
                column=spec.valid_from,
                actual=actual.value,
            )

    def _refusal(self, code: ETLErrorCode, blamed: str, **fields: object) -> ETLCompilationError:
        _log.warning(
            EtlEvent.MODEL_REFUSED,
            extra={**self._log_fields(), EtlKey.FIELD: blamed, EtlKey.REASON: code.value},
        )
        issue = code.issue(
            component=self._step_type,
            model=self._table.model,
            table=self._table_ref,
            **fields,
        )
        return ETLCompilationError.from_issue(issue)

    def _log_fields(self) -> dict[str, str]:
        return {
            EtlKey.STEP: self._step_type.__qualname__,
            EtlKey.MODEL: self._table.model.__qualname__,
            EtlKey.TABLE: self._table_ref,
        }


def validate_model_target(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate a target declared with a model; any other target is skipped."""
    if isinstance(spec, _TableSpec) and spec.model_table is not None:
        ModelTargetCheck(step_type, spec, spec.model_table).run()
