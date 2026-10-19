"""Unit tests for compiler._structural — execute signature and params compat."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding
from loom.etl.compiler._validators import (
    validate_execute_signature,
    validate_params_compat,
)

# ---------------------------------------------------------------------------
# Params fixtures
# ---------------------------------------------------------------------------


class BaseParams(ETLParams):
    run_date: date


class ExtendedParams(ETLParams):
    run_date: date
    country: str


class UnrelatedParams(ETLParams):
    only_field: str


# ---------------------------------------------------------------------------
# validate_params_compat
# ---------------------------------------------------------------------------


def test_params_compat_same_type_passes() -> None:
    validate_params_compat(object, BaseParams, BaseParams)


def test_params_compat_subclass_passes() -> None:
    # ExtendedParams is structurally compatible — has all BaseParams fields
    validate_params_compat(object, BaseParams, ExtendedParams)


def test_params_compat_missing_field_raises() -> None:
    with pytest.raises(ETLCompilationError, match="run_date") as exc_info:
        validate_params_compat(object, BaseParams, UnrelatedParams)

    assert exc_info.value.code is ETLErrorCode.MISSING_PARAMS_FIELDS


def test_params_compat_error_includes_type_name() -> None:
    with pytest.raises(ETLCompilationError, match="UnrelatedParams"):
        validate_params_compat(object, BaseParams, UnrelatedParams)


# ---------------------------------------------------------------------------
# validate_execute_signature — valid cases
# ---------------------------------------------------------------------------


class _ValidStep(ETLStep[BaseParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace()

    def execute(self, params: BaseParams, *, orders: Any) -> Any:
        return orders


def _bindings_for(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Build minimal source bindings by reading inline sources from the step."""
    return tuple(
        SourceBinding(alias=alias, spec=src._to_spec(alias))
        for alias, src in step_type._inline_sources.items()
    )


def test_valid_execute_signature_passes() -> None:
    validate_execute_signature(_ValidStep, BaseParams, _bindings_for(_ValidStep))


class _NoSourceStep(ETLStep[BaseParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: BaseParams) -> Any:
        return None


def test_no_source_step_passes() -> None:
    validate_execute_signature(_NoSourceStep, BaseParams, ())


# ---------------------------------------------------------------------------
# validate_execute_signature — error cases
# ---------------------------------------------------------------------------


class _MissingFrameStep(ETLStep[BaseParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace()

    def execute(self, params: BaseParams) -> Any:  # missing *, orders
        return None


def test_missing_frame_raises() -> None:
    bindings = _bindings_for(_MissingFrameStep)
    with pytest.raises(ETLCompilationError, match="orders") as exc_info:
        validate_execute_signature(_MissingFrameStep, BaseParams, bindings)

    assert exc_info.value.code is ETLErrorCode.MISSING_SOURCE_PARAMS


class _ExtraFrameStep(ETLStep[BaseParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: BaseParams, *, ghost: Any) -> Any:  # no source named ghost
        return None


def test_extra_frame_raises() -> None:
    with pytest.raises(ETLCompilationError, match="ghost") as exc_info:
        validate_execute_signature(_ExtraFrameStep, BaseParams, ())

    assert exc_info.value.code is ETLErrorCode.EXTRA_SOURCE_PARAMS


class _WrongParamNameStep(ETLStep[BaseParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, context: BaseParams) -> Any:  # named 'context' not 'params'
        return None


def test_wrong_params_name_raises() -> None:
    with pytest.raises(ETLCompilationError, match="named 'params'") as exc_info:
        validate_execute_signature(_WrongParamNameStep, BaseParams, ())

    assert exc_info.value.code is ETLErrorCode.INVALID_PARAMS_NAME
