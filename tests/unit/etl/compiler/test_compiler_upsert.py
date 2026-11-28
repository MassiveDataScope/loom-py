"""Compiler-time validation tests for UPSERT target specs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import ETLParams, ETLStep, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode


class EmptyIgnoredParams(ETLParams):  # type: ignore[misc]
    pass


def _compiler() -> ETLCompiler:
    return ETLCompiler()


class OrdersValidUpsert(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(keys=("order_id",))

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class _UpsertWithPartitions(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        partition_cols=("year", "month"),
    )

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class OrdersUpsertExcludingCreatedAt(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("created_at",),
    )

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class OrdersUpsertIncludingStatus(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        include=("status", "updated_at"),
    )

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class _EmptyKeys(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(keys=())

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class _ExcludeAndInclude(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(
        keys=("id",),
        exclude=("created_at",),
        include=("status",),
    )

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class _ExcludeOverlapsKeys(ETLStep[EmptyIgnoredParams]):
    target = IntoTable("test.orders").upsert(
        keys=("order_id",),
        exclude=("order_id", "created_at"),
    )

    def execute(self, params: EmptyIgnoredParams) -> Any:  # type: ignore[override]
        return None


class TestCompileUpsertSpecs:
    @pytest.mark.parametrize(
        "step_type,assertion",
        [
            (
                OrdersValidUpsert,
                lambda spec: spec.keys == ("order_id",),
            ),
            (
                _UpsertWithPartitions,
                lambda spec: (
                    spec.keys == ("order_id",) and spec.partition_cols == ("year", "month")
                ),
            ),
            (
                OrdersUpsertExcludingCreatedAt,
                lambda spec: spec.exclude == ("created_at",),
            ),
            (
                OrdersUpsertIncludingStatus,
                lambda spec: spec.include == ("status", "updated_at"),
            ),
        ],
    )
    def test_valid_specs_compile(
        self,
        step_type: type[ETLStep[EmptyIgnoredParams]],
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
        step_type: type[ETLStep[EmptyIgnoredParams]],
        error: str,
        code: ETLErrorCode,
    ) -> None:
        compiler = _compiler()
        with pytest.raises(ETLCompilationError, match=error) as exc_info:
            compiler.compile_step(step_type)

        assert exc_info.value.code is code
