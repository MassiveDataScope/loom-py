"""Compile-time checks of a source or target declared with a model."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from loom.core.errors import InvalidType
from loom.core.schema import SchemaMode
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.declarative._declared_table import DeclaredTable
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target import (
    HistorifySpec,
    TableSpec,
    TargetSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import FrameKey, LakeRefusal, ModelTable

_log = logging.getLogger(__name__)


class _ModelDeclaring(Protocol):
    @property
    def model(self) -> type | None: ...

    @property
    def table_ref(self) -> TableRef: ...

    @property
    def declared_columns(self) -> Mapping[str, tuple[str, ...]]: ...


class ModelCheck:
    """What a source or target declared with a model must satisfy before it compiles.

    Args:
        step_type: The step class being compiled, named in every message.
        model: The model the spec was declared with.
        spec: The source or target spec.
    """

    def __init__(self, step_type: type[Any], model: type, spec: _ModelDeclaring) -> None:
        self._step_type = step_type
        self._spec = spec
        self._model = model
        self._table_ref = spec.table_ref.ref

    def run(self) -> None:
        """Raise the first refusal, else log that the model resolved for the step."""
        table = self._translated()
        self._check_declared_columns(table)
        self._check_target()
        _log.debug(EtlEvent.MODEL_RESOLVED, extra=self._log_fields())

    def _check_target(self) -> None:
        """Checks only a target has; a source has none."""

    def _translated(self) -> ModelTable:
        try:
            return ModelTable.from_model(self._model)
        except LakeRefusal as refusal:
            raise self._refusal(
                ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
                refusal.field,
                field=refusal.field,
                reason=str(refusal),
            ) from refusal
        except InvalidType as unresolvable:
            raise self._refusal(
                ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED,
                "model",
                field="model",
                reason=str(unresolvable),
            ) from unresolvable

    def _check_declared_columns(self, table: ModelTable) -> None:
        for role, names in self._spec.declared_columns.items():
            unknown = table.unknown_columns(names)
            if unknown:
                raise self._refusal(
                    ETLErrorCode.MODEL_COLUMN_UNKNOWN,
                    unknown[0],
                    role=role,
                    column=unknown[0],
                    known=table.column_names,
                )

    def _refusal(self, code: ETLErrorCode, blamed: str, **fields: object) -> ETLCompilationError:
        _log.warning(
            EtlEvent.MODEL_REFUSED,
            extra={**self._log_fields(), EtlKey.FIELD: blamed, EtlKey.REASON: code.value},
        )
        issue = code.issue(
            component=self._step_type,
            model=self._model,
            table=self._table_ref,
            **fields,
        )
        return ETLCompilationError.from_issue(issue)

    def _log_fields(self) -> dict[str, str]:
        return {
            EtlKey.STEP: self._step_type.__qualname__,
            EtlKey.MODEL: self._model.__qualname__,
            EtlKey.TABLE: self._table_ref,
        }


class ModelTargetCheck(ModelCheck):
    """What a model target must satisfy beyond its columns: schema mode, keys and history."""

    def __init__(self, step_type: type[Any], model: type, spec: TableSpec) -> None:
        super().__init__(step_type, model, spec)
        self._target = spec

    def _check_target(self) -> None:
        self._check_schema_mode()
        self._check_merge_keys()
        self._check_history_date_type()

    def _check_schema_mode(self) -> None:
        mode = self._target.schema_mode
        if mode is not SchemaMode.STRICT:
            raise self._refusal(
                ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED, "schema", mode=mode.name
            )

    def _check_merge_keys(self) -> None:
        if not isinstance(self._target, UpsertSpec | UpdateSpec) or self._target.upsert_keys:
            return
        problem = FrameKey(self._model).problem
        if problem is not None:
            raise self._refusal(
                ETLErrorCode.MODEL_KEY_NOT_IN_FRAME,
                problem.field,
                field=problem.field,
                reason=problem.reason,
            )

    def _check_history_date_type(self) -> None:
        spec = self._target
        if not isinstance(spec, HistorifySpec):
            return
        actual = DeclaredTable(spec.table_ref, self._model).boundary_type(spec.valid_from)
        if actual is None:
            raise self._refusal(
                ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL,
                spec.valid_from,
                column=spec.valid_from,
            )
        if actual is not spec.date_type:
            raise self._refusal(
                ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
                "date_type",
                declared=spec.date_type.value,
                column=spec.valid_from,
                actual=actual.value,
            )


def validate_model_sources(step_type: type[Any], sources: Iterable[Any]) -> None:
    """Validate every table source declared with a model; any other source is skipped."""
    for spec in sources:
        if isinstance(spec, TableSourceSpec) and spec.model is not None:
            ModelCheck(step_type, spec.model, spec).run()


def validate_model_target(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate a target declared with a model; any other target is skipped."""
    if isinstance(spec, TableSpec) and spec.model is not None:
        ModelTargetCheck(step_type, spec.model, spec).run()
