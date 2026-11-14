"""Audit F06: a subclass must never execute under its parent's compiled plan."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.plan import ExecutionPlan
from loom.core.use_case.use_case import UseCase


def _fresh_hierarchy() -> tuple[type[UseCase[Any, str]], type[UseCase[Any, str]]]:
    class Parent(UseCase[Any, str]):
        async def execute(self, value: str) -> str:
            return f"parent:{value}"

    class Child(Parent):
        async def execute(self, value: str, extra: str) -> str:  # type: ignore[override]
            return f"child:{value}:{extra}"

    return Parent, Child


def _own_plan(uc_type: type[Any]) -> ExecutionPlan | None:
    return uc_type.__dict__.get("__execution_plan__")


@pytest.fixture
def compiler() -> UseCaseCompiler:
    return UseCaseCompiler()


@pytest.fixture
def executor(compiler: UseCaseCompiler) -> RuntimeExecutor:
    return RuntimeExecutor(compiler)


class TestSubclassPlanResolution:
    async def test_parent_compiled_first_child_executes_with_its_own_plan(
        self, compiler: UseCaseCompiler, executor: RuntimeExecutor
    ) -> None:
        parent, child = _fresh_hierarchy()
        compiler.compile(parent)
        assert _own_plan(child) is None

        result = await executor.execute(child(), params={"value": "v", "extra": "e"})

        assert result == "child:v:e"
        child_plan = _own_plan(child)
        assert child_plan is not None
        assert child_plan.use_case_type is child
        assert child_plan is not _own_plan(parent)

    async def test_child_compiled_first_parent_executes_with_its_own_plan(
        self, compiler: UseCaseCompiler, executor: RuntimeExecutor
    ) -> None:
        parent, child = _fresh_hierarchy()
        compiler.compile(child)
        assert _own_plan(parent) is None

        assert await executor.execute(parent(), params={"value": "v"}) == "parent:v"
        assert await executor.execute(child(), params={"value": "v", "extra": "e"}) == "child:v:e"
        parent_plan = _own_plan(parent)
        assert parent_plan is not None
        assert parent_plan.use_case_type is parent

    async def test_both_compiled_explicitly(
        self, compiler: UseCaseCompiler, executor: RuntimeExecutor
    ) -> None:
        parent, child = _fresh_hierarchy()
        compiler.compile(parent)
        compiler.compile(child)

        assert await executor.execute(child(), params={"value": "v", "extra": "e"}) == "child:v:e"
        assert await executor.execute(parent(), params={"value": "v"}) == "parent:v"

    async def test_parent_only_params_bind_against_the_child_plan(
        self, compiler: UseCaseCompiler, executor: RuntimeExecutor
    ) -> None:
        parent, child = _fresh_hierarchy()
        compiler.compile(parent)

        use_case = child()
        with pytest.raises(ValueError, match="missing required parameter 'extra'"):
            await executor.execute(use_case, params={"value": "v"})

        child_plan = _own_plan(child)
        assert child_plan is not None
        assert child_plan.use_case_type is child
