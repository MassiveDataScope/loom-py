"""Compile-time checks of a source or target declared with a model."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from loom.core.errors import InvalidType
from loom.core.model.introspection import ModelKeys
from loom.core.schema import SchemaMode
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.declarative._declared_table import ModelBoundSpec
from loom.etl.declarative.source import SourceSpec, TableSourceSpec
from loom.etl.declarative.target import (
    HistorifySpec,
    TableSpec,
    TargetSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import LakeRefusal, ModelTable

_log = logging.getLogger(__name__)


class ModelCheck:
    """What a source or target declared with a model must satisfy before it compiles.

    A target must also be written in a schema mode a model supports, have keys to merge
    on and, for history, a temporal boundary column.

    Args:
        step_type: The step class being compiled, named in every message.
        spec: The source or target spec, declared with a model.
    """

    def __init__(self, step_type: type[Any], spec: ModelBoundSpec) -> None:
        assert spec.model is not None
        self._step_type = step_type
        self._spec = spec
        self._model = spec.model

    def run(self) -> None:
        """Raise the first refusal, logging it once, else log that the model resolved."""
        try:
            self._check()
        except ETLCompilationError as refusal:
            _log.warning(
                EtlEvent.MODEL_REFUSED,
                extra={
                    **self._log_fields(),
                    EtlKey.FIELD: refusal.field,
                    EtlKey.REASON: refusal.code.value,
                },
            )
            raise
        _log.debug(EtlEvent.MODEL_RESOLVED, extra=self._log_fields())

    def _check(self) -> None:
        self._check_declared_columns(self._model_table())
        if isinstance(self._spec, TableSpec):
            self._check_schema_mode(self._spec)
            self._check_merge_keys(self._spec)
            self._check_history_date_type(self._spec)

    def _model_table(self) -> ModelTable:
        try:
            return ModelTable.from_model(self._model)
        except (LakeRefusal, InvalidType) as refusal:
            field = refusal.field if isinstance(refusal, LakeRefusal) else "model"
            raise self._refusal(
                ETLErrorCode.MODEL_COLUMN_TYPE_REFUSED, field, reason=str(refusal)
            ) from refusal

    def _check_declared_columns(self, table: ModelTable) -> None:
        for role, names in self._spec.declared_columns.items():
            unknown = table.unknown_columns(names)
            if unknown:
                raise self._refusal(
                    ETLErrorCode.MODEL_COLUMN_UNKNOWN,
                    unknown[0],
                    role=role,
                    known=table.column_names,
                )

    def _check_schema_mode(self, spec: TableSpec) -> None:
        if spec.schema_mode is not SchemaMode.STRICT:
            raise self._refusal(
                ETLErrorCode.MODEL_SCHEMA_MODE_UNSUPPORTED, "schema", mode=spec.schema_mode.name
            )

    def _check_merge_keys(self, spec: TableSpec) -> None:
        if not isinstance(spec, UpsertSpec | UpdateSpec):
            return
        if spec.declared_keys is not None:
            return
        keys = ModelKeys.of(self._model)
        if keys.frame_supplied:
            return
        if not keys.conflict:
            field, reason = "keys", "declares neither a business key nor a primary key"
        else:
            field, fill = next(iter(keys.store_filled.items()))
            reason = f"primary key {field!r} is {fill.description}"
        raise self._refusal(ETLErrorCode.MODEL_KEY_NOT_IN_FRAME, field, reason=reason)

    def _check_history_date_type(self, spec: TableSpec) -> None:
        if not isinstance(spec, HistorifySpec):
            return
        actual = spec.model_boundary_type
        if actual is None:
            raise self._refusal(ETLErrorCode.MODEL_BOUNDARY_NOT_TEMPORAL, spec.valid_from)
        if spec.declared_date_type is not None and actual is not spec.declared_date_type:
            raise self._refusal(
                ETLErrorCode.MODEL_DATE_TYPE_MISMATCH,
                "date_type",
                declared=spec.declared_date_type.value,
                column=spec.valid_from,
                actual=actual.value,
            )

    def _refusal(self, code: ETLErrorCode, blamed: str, **details: object) -> ETLCompilationError:
        issue = code.issue(
            component=self._step_type,
            model=self._model,
            table=self._spec.table_ref.ref,
            field=blamed,
            **details,
        )
        return ETLCompilationError.from_issue(issue)

    def _log_fields(self) -> dict[str, str]:
        return {
            EtlKey.STEP: self._step_type.__qualname__,
            EtlKey.MODEL: self._model.__qualname__,
            EtlKey.TABLE: self._spec.table_ref.ref,
        }


def validate_model_sources(step_type: type[Any], sources: Iterable[SourceSpec]) -> None:
    """Validate every table source declared with a model; any other source is skipped."""
    for spec in sources:
        if isinstance(spec, TableSourceSpec) and spec.model is not None:
            ModelCheck(step_type, spec).run()


def validate_model_target(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate a target declared with a model; any other target is skipped."""
    if isinstance(spec, TableSpec) and spec.model is not None:
        ModelCheck(step_type, spec).run()
