"""Unit tests for golden testing utilities."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest

from loom.core.command import Command
from loom.core.engine.plan import (
    ComputeStep,
    ExecutionPlan,
    InputBinding,
    LoadStep,
    ParamBinding,
    RuleStep,
)
from loom.core.model import LoomStruct
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind
from loom.core.use_case.use_case import UseCase
from loom.testing.golden import GoldenHarness, _ErrorProxy, _serialize_result, serialize_plan

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Product(LoomStruct):
    id: int
    name: str


class CreateProductCmd(Command, frozen=True):
    name: str


def _normalize(cmd: Any, fields: frozenset[str]) -> Any:
    return cmd


def _price_rule(cmd: Any, fields: frozenset[str]) -> None:
    pass


def _rule_step_fn(
    command: Command,
    fields_set: frozenset[str],
    context: dict[str, object] | None = None,
) -> None:
    _price_rule(command, fields_set)


class IProductRepo:
    pass


class FakeProductRepo:
    async def create(self, cmd: Any) -> Product:
        return Product(id=1, name=cmd.name)

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> Product | None:
        return Product(id=int(obj_id), name="existing")

    async def list_paginated(self, *args: Any, **kwargs: Any) -> Any:
        return []

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> Product | None:
        return Product(id=1, name="existing")

    async def exists_by(self, field: str, value: Any) -> bool:
        await asyncio.sleep(0)
        return True

    async def update(self, obj_id: Any, data: Any) -> Product | None:
        return Product(id=int(obj_id), name="updated")

    async def delete(self, obj_id: Any) -> bool:
        return True


class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProductCmd = ...) -> Product:  # type: ignore[assignment]
        return cast(Product, await self.main_repo.create(cmd))


class SimpleUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# serialize_plan
# ---------------------------------------------------------------------------


class TestSerializePlan:
    def _make_plan(self) -> ExecutionPlan:
        return ExecutionPlan(
            use_case_type=SimpleUseCase,
            param_bindings=(ParamBinding("user_id", int),),
            input_binding=InputBinding("cmd", CreateProductCmd),
            load_steps=(
                LoadStep(
                    "product",
                    Product,
                    source_kind=SourceKind.PARAM,
                    source_name="user_id",
                    lookup_kind=LookupKind.BY_ID,
                    against="id",
                    profile="detail",
                    on_missing=OnMissing.RAISE,
                ),
            ),
            exists_steps=(),
            compute_steps=(ComputeStep(fn=_normalize),),
            rule_steps=(RuleStep(fn=_rule_step_fn),),
        )

    def test_returns_dict(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        assert isinstance(result, dict)

    def test_use_case_is_qualified_name(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        assert "SimpleUseCase" in result["use_case"]
        assert "." in result["use_case"]

    def test_param_bindings_serialized(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        assert len(result["param_bindings"]) == 1
        assert result["param_bindings"][0]["name"] == "user_id"
        assert "int" in result["param_bindings"][0]["annotation"]

    def test_input_binding_serialized(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        assert result["input_binding"] is not None
        assert result["input_binding"]["name"] == "cmd"
        assert "CreateProductCmd" in result["input_binding"]["command_type"]

    def test_load_step_serialized(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        ls = result["load_steps"][0]
        assert ls["source_name"] == "user_id"
        assert ls["lookup_kind"] == "by_id"
        assert ls["profile"] == "detail"
        assert "Product" in ls["entity_type"]

    @pytest.mark.parametrize(
        ("expected", "steps"),
        [
            pytest.param("_normalize", "compute_steps", id="compute_step_has_fn_qualname"),
            pytest.param("_rule_step_fn", "rule_steps", id="rule_step_has_fn_qualname"),
        ],
    )
    def test_step_has_fn_qualname(self, expected: str, steps: str) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        assert expected in result[steps][0]["fn"]

    def test_no_input_binding_is_none(self) -> None:
        plan = ExecutionPlan(
            use_case_type=SimpleUseCase,
            param_bindings=(),
            input_binding=None,
            load_steps=(),
            exists_steps=(),
            compute_steps=(),
            rule_steps=(),
        )
        result = serialize_plan(plan)
        assert result["input_binding"] is None

    def test_deterministic_same_plan_same_output(self) -> None:
        first = serialize_plan(self._make_plan())
        second = serialize_plan(self._make_plan())
        assert first == second

    def test_keys_are_sorted(self) -> None:
        plan = self._make_plan()
        result = serialize_plan(plan)
        keys = list(result.keys())
        assert keys == sorted(keys)


# ---------------------------------------------------------------------------
# _ErrorProxy
# ---------------------------------------------------------------------------


class TestErrorProxy:
    def test_proxies_normal_attribute(self) -> None:
        repo = FakeProductRepo()
        proxy = _ErrorProxy(repo, {})
        assert getattr(proxy.create, "__func__", None) is getattr(repo.create, "__func__", None)

    @pytest.mark.asyncio
    async def test_raises_configured_error_on_method(self) -> None:
        repo = FakeProductRepo()
        err = RuntimeError("forced")
        proxy = _ErrorProxy(repo, {"create": err})

        with pytest.raises(RuntimeError, match="forced"):
            await proxy.create("anything")

    @pytest.mark.asyncio
    async def test_unaffected_method_still_works(self) -> None:
        repo = FakeProductRepo()
        proxy = _ErrorProxy(repo, {"create": RuntimeError("forced")})
        result = await proxy.get_by_id(1)
        assert result is not None


# ---------------------------------------------------------------------------
# GoldenHarness — inject_repo + run
# ---------------------------------------------------------------------------


class TestGoldenHarnessRun:
    @pytest.mark.asyncio
    async def test_run_simple_headless_use_case(self) -> None:
        harness = GoldenHarness()
        result = await harness.run(SimpleUseCase)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_run_injects_main_repo(self) -> None:
        harness = GoldenHarness()
        harness.inject_repo(IProductRepo, FakeProductRepo(), model=Product)

        class _GetUseCase(UseCase[Product, Product | None]):
            async def execute(self, product_id: int) -> Product | None:
                return await self.main_repo.get_by_id(product_id)

        result = await harness.run(_GetUseCase, params={"product_id": 42})
        assert result is not None
        assert result.id == 42

    @pytest.mark.asyncio
    async def test_each_run_uses_fresh_container(self) -> None:
        harness = GoldenHarness()
        r1 = await harness.run(SimpleUseCase)
        r2 = await harness.run(SimpleUseCase)
        assert r1 == r2


# ---------------------------------------------------------------------------
# GoldenHarness — force_error / simulate_system_error
# ---------------------------------------------------------------------------


class TestGoldenHarnessErrors:
    @pytest.mark.asyncio
    async def test_force_error_raises_on_method(self) -> None:
        from loom.core.errors import Conflict

        harness = GoldenHarness()
        fake = FakeProductRepo()
        harness.inject_repo(IProductRepo, fake, model=Product)
        harness.force_error(IProductRepo, "get_by_id", Conflict("dup"))

        class _UC(UseCase[Product, Product | None]):
            async def execute(self, product_id: int) -> Product | None:
                return await self.main_repo.get_by_id(product_id)

        with pytest.raises(Conflict):
            await harness.run(_UC, params={"product_id": 1})

    @pytest.mark.asyncio
    async def test_simulate_system_error(self) -> None:
        harness = GoldenHarness()
        harness.inject_repo(IProductRepo, FakeProductRepo(), model=Product)
        harness.simulate_system_error(IProductRepo, "get_by_id")

        class _UC(UseCase[Product, Product | None]):
            async def execute(self, product_id: int) -> Product | None:
                return await self.main_repo.get_by_id(product_id)

        with pytest.raises(SystemError):
            await harness.run(_UC, params={"product_id": 1})


# ---------------------------------------------------------------------------
# GoldenHarness — run_with_baseline
# ---------------------------------------------------------------------------


class TestGoldenHarnessBaseline:
    @pytest.mark.asyncio
    async def test_within_limit_passes(self, tmp_path: Path) -> None:
        harness = GoldenHarness()
        result = await harness.run_with_baseline(
            SimpleUseCase,
            name="simple_ok",
            max_ms=5000.0,
            baseline_dir=tmp_path,
        )
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_writes_baseline_file(self, tmp_path: Path) -> None:
        harness = GoldenHarness()
        await harness.run_with_baseline(
            SimpleUseCase,
            name="simple_baseline",
            max_ms=5000.0,
            baseline_dir=tmp_path,
        )
        baseline_file = tmp_path / "simple_baseline.json"
        assert baseline_file.exists()
        data = baseline_file.read_text()
        assert "elapsed_ms" in data
        assert "max_ms" in data

    @pytest.mark.asyncio
    async def test_exceeds_limit_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import time as _time_module

        call_count = 0

        def _fake_perf_counter() -> float:
            nonlocal call_count
            call_count += 1
            return 0.0 if call_count == 1 else 10.0

        monkeypatch.setattr(_time_module, "perf_counter", _fake_perf_counter)

        harness = GoldenHarness()
        with pytest.raises(AssertionError, match="baseline exceeded"):
            await harness.run_with_baseline(
                SimpleUseCase,
                name="slow",
                max_ms=1.0,
                baseline_dir=tmp_path,
            )


# ---------------------------------------------------------------------------
# _serialize_result
# ---------------------------------------------------------------------------


class TestSerializeResult:
    def test_struct_converted_to_builtins(self) -> None:
        p = Product(id=1, name="x")
        assert _serialize_result(p) == {"id": 1, "name": "x"}

    def test_plain_value_returned_as_is(self) -> None:
        assert _serialize_result("hello") == "hello"
        assert _serialize_result(42) == 42

    def test_dict_returned_as_is(self) -> None:
        d = {"a": 1}
        assert _serialize_result(d) is d
