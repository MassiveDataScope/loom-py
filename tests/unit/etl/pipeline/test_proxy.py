"""Tests for the params proxy and ParamExpr."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pytest

from loom.etl.declarative.expr._params import ParamExpr, _ParamProxy, params, resolve_param_expr
from loom.etl.declarative.expr._predicate import EqPred, InPred
from tests.unit.etl.doubles import RunDateCountriesParams


@pytest.fixture
def run_params() -> RunDateCountriesParams:
    return RunDateCountriesParams(run_date=date(2024, 1, 5), countries=("ES", "FR"))


class TestParamsProxy:
    def test_params_singleton_proxy(self) -> None:
        assert isinstance(params, _ParamProxy)

    @pytest.mark.parametrize(
        "expr,expected",
        [
            (params.run_date, ("run_date",)),
            (params.run_date.year, ("run_date", "year")),
            (params.a.b.c.d, ("a", "b", "c", "d")),
        ],
    )
    def test_attr_chain_builds_path(self, expr: ParamExpr, expected: tuple[str, ...]) -> None:
        assert isinstance(expr, ParamExpr)
        assert expr.path == expected

    @pytest.mark.parametrize(
        "expr,expected_type",
        [
            (params.run_date.year == 2024, EqPred),
            (params.countries.isin(("ES", "FR")), InPred),
        ],
    )
    def test_predicate_builders(self, expr: object, expected_type: type[object]) -> None:
        assert isinstance(expr, expected_type)

    def test_repr_and_hash_contract(self) -> None:
        assert repr(params.run_date.year) == "params.run_date.year"
        e1 = ParamExpr(("run_date", "year"))
        e2 = ParamExpr(("run_date", "year"))
        assert hash(e1) == hash(e2)

    @pytest.mark.parametrize(
        "accessor",
        [
            lambda: params._private,
            lambda: ParamExpr(("run_date",))._private,
        ],
    )
    def test_private_attr_raises(self, accessor: Callable[[], object]) -> None:
        with pytest.raises(AttributeError):
            _ = accessor()


class TestResolveParamExpr:
    @pytest.mark.parametrize(
        "expr,expected",
        [
            (ParamExpr(("run_date",)), date(2024, 1, 5)),
            (ParamExpr(("run_date", "year")), 2024),
            (ParamExpr(("run_date", "month")), 1),
            (ParamExpr(("run_date", "day")), 5),
            (ParamExpr(("countries",)), ("ES", "FR")),
        ],
    )
    def test_resolve_success(
        self,
        run_params: RunDateCountriesParams,
        expr: ParamExpr,
        expected: object,
    ) -> None:
        assert resolve_param_expr(expr, run_params) == expected

    def test_resolve_missing_field_raises(self, run_params: RunDateCountriesParams) -> None:
        expr = ParamExpr(("nonexistent",))
        with pytest.raises(AttributeError):
            resolve_param_expr(expr, run_params)
