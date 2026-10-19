"""The ``Query()`` marker: one declared, strict query struct per ``execute``."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.engine.compiler import CompilationError, UseCaseCompiler
from loom.core.engine.executor import ParameterBindingError, RuntimeExecutor
from loom.core.engine.plan import ParamBinding, QueryBinding
from loom.core.job.job import Job
from loom.core.model import UnsupportedBoundaryType
from loom.core.use_case.markers import Query
from loom.core.use_case.use_case import UseCase


class GrantQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    limit: int = 50


class LaxQuery(msgspec.Struct, frozen=True):
    limit: int = 50


class ListGrants(UseCase[Any, int]):
    async def execute(self, query: GrantQuery = Query()) -> int:
        return query.limit


class ListGrantsJob(Job[int]):
    async def execute(self, query: GrantQuery = Query()) -> int:  # type: ignore[override]
        return query.limit


class TwoQueries(UseCase[Any, int]):
    async def execute(self, first: GrantQuery = Query(), second: GrantQuery = Query()) -> int:
        return first.limit


class LaxQueryUseCase(UseCase[Any, int]):
    async def execute(self, query: LaxQuery = Query()) -> int:
        return query.limit


class DictQueryUseCase(UseCase[Any, int]):
    async def execute(self, query: dict[str, str] = Query()) -> int:
        return len(query)


def test_query_compiles_to_a_param_binding_and_a_query_binding() -> None:
    plan = UseCaseCompiler().compile(ListGrants)

    assert plan.param_bindings == (ParamBinding(name="query", annotation=GrantQuery),)
    assert plan.query_binding == QueryBinding(name="query", query_type=GrantQuery)


def test_two_query_parameters_are_refused() -> None:
    compiler = UseCaseCompiler()

    with pytest.raises(CompilationError, match="only one Query"):
        compiler.compile(TwoQueries)


@pytest.mark.parametrize("use_case", [LaxQueryUseCase, DictQueryUseCase])
def test_a_query_type_that_is_not_a_strict_model_is_refused(
    use_case: type[UseCase[Any, int]],
) -> None:
    compiler = UseCaseCompiler()

    with pytest.raises(CompilationError) as excinfo:
        compiler.compile(use_case)
    assert f"{use_case.__qualname__}.execute" in str(excinfo.value)
    assert "'query'" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, UnsupportedBoundaryType)


async def test_a_query_that_is_not_an_instance_of_its_type_is_refused() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(ListGrants)
    executor = RuntimeExecutor(compiler)
    use_case = ListGrants()

    with pytest.raises(ParameterBindingError) as excinfo:
        await executor.execute(use_case, params={"query": 424242})
    assert "'query'" in str(excinfo.value)
    assert "GrantQuery" in str(excinfo.value)
    assert "424242" not in str(excinfo.value)


def test_a_job_that_declares_query_is_refused() -> None:
    compiler = UseCaseCompiler()

    with pytest.raises(CompilationError, match=r"ListGrantsJob\.execute: a Job cannot"):
        compiler.compile(ListGrantsJob)
