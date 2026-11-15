"""Tests for ETLStep declaration and __init_subclass__ validation."""

from __future__ import annotations

from datetime import date
from typing import Any, ClassVar, get_args, get_origin, get_type_hints

import pytest

from loom.etl import (
    ETLParams,
    ETLStep,
    FromTable,
    IntoTable,
    Sources,
    SourceSet,
)
from loom.etl.declarative.target import IntoHistory
from loom.etl.pipeline._step import _SourceForm


class RunDateCountriesStepParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


class StepForm2(ETLStep[RunDateCountriesStepParams]):
    sources = Sources(
        orders=FromTable("raw.orders"),
        customers=FromTable("raw.customers"),
    )
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateCountriesStepParams, *, orders: Any, customers: Any) -> Any:
        return orders


class StepForm1(ETLStep[RunDateCountriesStepParams]):
    orders = FromTable("raw.orders")
    customers = FromTable("raw.customers")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateCountriesStepParams, *, orders: Any, customers: Any) -> Any:
        return orders


class CalendarNoSourceStep(ETLStep[RunDateCountriesStepParams]):
    target = IntoTable("staging.calendar").replace()

    def execute(self, params: RunDateCountriesStepParams) -> Any:
        return None


class OrderSources(SourceSet[RunDateCountriesStepParams]):
    orders = FromTable("raw.orders")


class StepForm3(ETLStep[RunDateCountriesStepParams]):
    sources = OrderSources.extended(customers=FromTable("raw.customers"))
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateCountriesStepParams, *, orders: Any, customers: Any) -> Any:
        return orders


class TestStepDeclaration:
    def test_form2_detected_as_grouped(self) -> None:
        assert StepForm2._source_form is _SourceForm.GROUPED

    def test_form1_detected_as_inline(self) -> None:
        assert StepForm1._source_form is _SourceForm.INLINE

    def test_form1_inline_sources_collected(self) -> None:
        assert set(StepForm1._inline_sources) == {"orders", "customers"}

    def test_form3_detected_as_grouped(self) -> None:
        assert StepForm3._source_form is _SourceForm.GROUPED

    def test_no_sources_form_is_none(self) -> None:
        assert CalendarNoSourceStep._source_form is _SourceForm.NONE

    def test_params_type_extracted(self) -> None:
        assert StepForm2._params_type is RunDateCountriesStepParams

    def test_params_type_none_for_base_class(self) -> None:
        assert ETLStep._params_type is None

    def test_mixing_inline_and_grouped_raises(self) -> None:
        inline_source = FromTable("raw.orders")
        grouped_sources = Sources(customers=FromTable("raw.customers"))
        out_target = IntoTable("staging.out").replace()

        with pytest.raises(TypeError, match="cannot mix inline source attributes"):

            class _BadStep(ETLStep[RunDateCountriesStepParams]):  # NOSONAR
                orders = inline_source
                sources = grouped_sources
                target = out_target

                def execute(
                    self, params: RunDateCountriesStepParams, *, orders: Any, customers: Any
                ) -> Any:
                    return orders

    def test_execute_raises_not_implemented_on_base(self) -> None:
        class Bare(ETLStep[RunDateCountriesStepParams]):
            target = IntoTable("staging.x").replace()

        bare = Bare()
        params = RunDateCountriesStepParams(run_date=date(2024, 1, 1), countries=())
        with pytest.raises(NotImplementedError):
            bare.execute(params)

    def test_streaming_flag_must_be_bool(self) -> None:
        out_target = IntoTable("staging.out").replace()

        with pytest.raises(TypeError, match="'streaming' must be bool"):

            class _BadStreamingType(ETLStep[RunDateCountriesStepParams]):  # NOSONAR
                streaming = "yes"
                target = out_target

                def execute(self, params: RunDateCountriesStepParams) -> Any:
                    return None

    def test_streaming_name_is_not_treated_as_inline_source(self) -> None:
        orders_source = FromTable("raw.orders")
        out_target = IntoTable("staging.out").replace()

        with pytest.raises(TypeError, match="'streaming' must be bool"):

            class _BadStreamingSource(ETLStep[RunDateCountriesStepParams]):  # NOSONAR
                streaming = orders_source
                target = out_target

                def execute(self, params: RunDateCountriesStepParams) -> Any:
                    return None

    def test_target_annotation_admits_into_history(self) -> None:
        """IntoHistory is part of the declared target union, not just runtime-tolerated."""
        hints = get_type_hints(ETLStep)
        assert get_origin(hints["target"]) is ClassVar  # ClassVar wrapper intact
        (target_union,) = get_args(hints["target"])
        assert IntoHistory in get_args(target_union)
