"""Unit tests for compiler._structural — execute signature and params compat."""

from __future__ import annotations

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
from tests.unit.etl.compiler.doubles import RunDateCountryETLParams
from tests.unit.etl.doubles import RunDateETLParams

# ---------------------------------------------------------------------------
# Params fixtures
# ---------------------------------------------------------------------------


class OnlyFieldParams(ETLParams):
    only_field: str


class TestParamsCompat:
    # ---------------------------------------------------------------------------
    # validate_params_compat
    # ---------------------------------------------------------------------------

    def test_params_compat_same_type_passes(self) -> None:
        validate_params_compat(object, RunDateETLParams, RunDateETLParams)

    def test_params_compat_subclass_passes(self) -> None:
        # RunDateCountryETLParams is structurally compatible — has all RunDateETLParams fields
        validate_params_compat(object, RunDateETLParams, RunDateCountryETLParams)

    def test_params_compat_missing_field_raises_naming_the_field_and_the_type(self) -> None:
        with pytest.raises(ETLCompilationError, match="run_date") as exc_info:
            validate_params_compat(object, RunDateETLParams, OnlyFieldParams)

        assert exc_info.value.code is ETLErrorCode.MISSING_PARAMS_FIELDS
        assert "OnlyFieldParams" in str(exc_info.value)


# ---------------------------------------------------------------------------
# validate_execute_signature — valid cases
# ---------------------------------------------------------------------------


class _ValidStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


def _bindings_for(step_type: type[ETLStep[Any]]) -> tuple[SourceBinding, ...]:
    """Build minimal source bindings by reading inline sources from the step."""
    return tuple(
        SourceBinding(alias=alias, spec=src._to_spec(alias))
        for alias, src in step_type._inline_sources.items()
    )


class TestValidExecuteSignature:
    def test_valid_execute_signature_passes(self) -> None:
        validate_execute_signature(_ValidStep, RunDateETLParams, _bindings_for(_ValidStep))


class EmptyNoSourceStep(ETLStep[RunDateETLParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams) -> Any:
        return None


class TestNoSourceExecuteSignature:
    def test_no_source_step_passes(self) -> None:
        validate_execute_signature(EmptyNoSourceStep, RunDateETLParams, ())


# ---------------------------------------------------------------------------
# validate_execute_signature — error cases
# ---------------------------------------------------------------------------


class _MissingFrameStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams) -> Any:  # missing *, orders
        return None


class TestMissingSourceParam:
    def test_missing_frame_raises(self) -> None:
        bindings = _bindings_for(_MissingFrameStep)
        with pytest.raises(ETLCompilationError, match="orders") as exc_info:
            validate_execute_signature(_MissingFrameStep, RunDateETLParams, bindings)

        assert exc_info.value.code is ETLErrorCode.MISSING_SOURCE_PARAMS


class _ExtraFrameStep(ETLStep[RunDateETLParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams, *, ghost: Any) -> Any:  # no source named ghost
        return None


class TestExtraSourceParam:
    def test_extra_frame_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="ghost") as exc_info:
            validate_execute_signature(_ExtraFrameStep, RunDateETLParams, ())

        assert exc_info.value.code is ETLErrorCode.EXTRA_SOURCE_PARAMS


class _WrongParamNameStep(ETLStep[RunDateETLParams]):
    target = IntoTable("staging.out").replace()

    def execute(self, context: RunDateETLParams) -> Any:  # named 'context' not 'params'
        return None


class TestWrongParamsName:
    def test_wrong_params_name_raises(self) -> None:
        with pytest.raises(ETLCompilationError, match="named 'params'") as exc_info:
            validate_execute_signature(_WrongParamNameStep, RunDateETLParams, ())

        assert exc_info.value.code is ETLErrorCode.INVALID_PARAMS_NAME
