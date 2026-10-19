"""Compiler-time validation tests for UPSERT target specs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode


class _Params(ETLParams):  # type: ignore[misc]
    pass


def _compiler() -> ETLCompiler:
    return ETLCompiler()


class _ValidUpsert(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(keys=("order_id",))

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _UpsertWithPartitions(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        partition_cols=("year", "month"),
    )

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _UpsertWithExclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("created_at",),
    )

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _UpsertWithInclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        include=("status", "updated_at"),
    )

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _EmptyKeys(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(keys=())

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _ExcludeAndInclude(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("id",),
        exclude=("created_at",),
        include=("status",),
    )

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class _ExcludeOverlapsKeys(ETLStep[_Params]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("order_id", "created_at"),
    )

    def execute(self, params: _Params) -> Any:  # type: ignore[override]
        return None


class TestCompileUpsertSpecs:
    @pytest.mark.parametrize(
        "step_type,assertion",
        [
            (
                _ValidUpsert,
                lambda spec: spec.upsert_keys == ("order_id",),
            ),
            (
                _UpsertWithPartitions,
                lambda spec: (
                    spec.upsert_keys == ("order_id",) and spec.partition_cols == ("year", "month")
                ),
            ),
            (
                _UpsertWithExclude,
                lambda spec: spec.upsert_exclude == ("created_at",),
            ),
            (
                _UpsertWithInclude,
                lambda spec: spec.upsert_include == ("status", "updated_at"),
            ),
        ],
    )
    def test_valid_specs_compile(
        self,
        step_type: type[ETLStep[_Params]],
        assertion: Callable[[Any], bool],
    ) -> None:
        spec = _compiler().compile_step(step_type).target_binding.spec
        assert assertion(spec)

    @pytest.mark.parametrize(
        "step_type,error,code",
        [
            (_EmptyKeys, "requires at least one key", ETLErrorCode.UPSERT_NO_KEYS),
            (
                _ExcludeAndInclude,
                "mutually exclusive",
                ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT,
            ),
            (_ExcludeOverlapsKeys, "overlaps with upsert keys", ETLErrorCode.UPSERT_KEY_IN_EXCLUDE),
        ],
    )
    def test_invalid_specs_raise(
        self,
        step_type: type[ETLStep[_Params]],
        error: str,
        code: ETLErrorCode,
    ) -> None:
        compiler = _compiler()
        with pytest.raises(ETLCompilationError, match=error) as exc_info:
            compiler.compile_step(step_type)

        assert exc_info.value.code is code
