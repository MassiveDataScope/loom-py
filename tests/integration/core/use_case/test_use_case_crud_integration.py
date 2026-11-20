from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import msgspec
import pytest

from loom.core.bootstrap import create_kernel
from loom.core.command import Command, Patch
from loom.core.config.loader import load_config, section
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import RuleViolationsError
from loom.core.use_case import Compute, F, Input, Rule
from loom.core.use_case.use_case import UseCase
from loom.testing import RepositoryIntegrationHarness
from tests.integration.fake_repo.product.model import Product


class _Cfg:
    env: str = "test"


class _CacheSettings(msgspec.Struct, kw_only=True):
    backend: str = "memory"
    alias: str = "default"
    ttl_seconds: int = 60


class _ProductUseCaseSettings(msgspec.Struct, kw_only=True):
    timeout_ms: int = 200
    pagination_mode: str = "cursor"


class _UseCasesSettings(msgspec.Struct, kw_only=True):
    products_list: _ProductUseCaseSettings = msgspec.field(default_factory=_ProductUseCaseSettings)


class _IntegrationAppConfig(msgspec.Struct, kw_only=True):
    env: str = "test"
    cache: _CacheSettings = msgspec.field(default_factory=_CacheSettings)
    use_cases: _UseCasesSettings = msgspec.field(default_factory=_UseCasesSettings)


class UseCaseConfigSnapshot(Command, frozen=True):
    env: str
    cache_store: str
    timeout_ms: int
    pagination_mode: str


class CreateProductCmd(Command, frozen=True):
    name: str
    price: float


class UpdateProductCmd(Command, frozen=True):
    name: Patch[str] = None
    price: Patch[float] = None


class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProductCmd = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class GetProductUseCase(UseCase[Product, Product | None]):
    async def execute(self, product_id: int) -> Product | None:
        return await self.main_repo.get_by_id(product_id)


class UpdateProductUseCase(UseCase[Product, Product | None]):
    async def execute(
        self,
        product_id: int,
        cmd: UpdateProductCmd = Input(),
    ) -> Product | None:
        return await self.main_repo.update(product_id, cmd)


class DeleteProductUseCase(UseCase[Product, bool]):
    async def execute(self, product_id: int) -> bool:
        return await self.main_repo.delete(product_id)


class CreateProductWithRulesCmd(Command, frozen=True):
    name: str
    price: float


def _normalize_product_name(name: str) -> str:
    return name.strip().upper()


def _price_must_be_positive(price: float) -> str | None:
    if price <= 0:
        return "price must be positive"
    return None


NORMALIZE_NAME = Compute.set(F(CreateProductWithRulesCmd).name).from_command(
    F(CreateProductWithRulesCmd).name,
    via=_normalize_product_name,
)

PRICE_RULE = Rule.check(
    F(CreateProductWithRulesCmd).price,
    via=_price_must_be_positive,
)


class CreateProductWithComputeAndRulesUseCase(UseCase[Product, Product]):
    computes = [NORMALIZE_NAME]
    rules = [PRICE_RULE]

    async def execute(self, cmd: CreateProductWithRulesCmd = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class ReadTypedConfigUseCase(UseCase[object, UseCaseConfigSnapshot]):
    def __init__(self, settings: _IntegrationAppConfig) -> None:
        self._settings = settings

    async def execute(self) -> UseCaseConfigSnapshot:
        products_list = self._settings.use_cases.products_list
        return UseCaseConfigSnapshot(
            env=self._settings.env,
            cache_store=self._settings.cache.backend,
            timeout_ms=products_list.timeout_ms,
            pagination_mode=products_list.pagination_mode,
        )


def _repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    repo = integration_context.product.repository
    repo_interface = type(repo)

    def register(container: LoomContainer) -> None:
        container.register(repo_interface, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(Product, repo_interface)

    return register


class TestUseCaseCrudIntegration:
    @pytest.mark.asyncio
    async def test_use_case_crud_flow_with_main_repo_injected(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = create_kernel(
            config=_Cfg(),
            use_cases=[
                CreateProductUseCase,
                GetProductUseCase,
                UpdateProductUseCase,
                DeleteProductUseCase,
            ],
            modules=[_repo_module(integration_context)],
        )

        executor = RuntimeExecutor(result.compiler)

        create_uc = result.factory.build(CreateProductUseCase)
        created = await executor.execute(
            create_uc,
            payload={"name": "headphones", "price": 55.0},
        )
        assert created.name == "headphones"
        assert created.id == 1

        get_uc = result.factory.build(GetProductUseCase)
        loaded = await executor.execute(get_uc, params={"product_id": 1})
        assert loaded is not None
        assert loaded.name == "headphones"

        update_uc = result.factory.build(UpdateProductUseCase)
        updated = await executor.execute(
            update_uc,
            params={"product_id": 1},
            payload={"price": 49.9},
        )
        assert updated is not None
        assert float(updated.price) == pytest.approx(49.9)

        delete_uc = result.factory.build(DeleteProductUseCase)
        deleted = await executor.execute(delete_uc, params={"product_id": 1})
        assert deleted is True

        missing = await executor.execute(get_uc, params={"product_id": 1})
        assert missing is None

    @pytest.mark.asyncio
    async def test_use_case_compute_and_rules_success_path(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = create_kernel(
            config=_Cfg(),
            use_cases=[CreateProductWithComputeAndRulesUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)
        create_uc = result.factory.build(CreateProductWithComputeAndRulesUseCase)

        created = await executor.execute(
            create_uc,
            payload={"name": "  headset  ", "price": 42.0},
        )

        assert created.name == "HEADSET"
        assert float(created.price) == pytest.approx(42.0)

    @pytest.mark.asyncio
    async def test_use_case_compute_and_rules_rejects_invalid_input(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = create_kernel(
            config=_Cfg(),
            use_cases=[CreateProductWithComputeAndRulesUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)
        create_uc = result.factory.build(CreateProductWithComputeAndRulesUseCase)

        with pytest.raises(RuleViolationsError):
            await executor.execute(
                create_uc,
                payload={"name": "bad", "price": -1.0},
            )

    @pytest.mark.asyncio
    async def test_use_case_can_resolve_typed_config_from_bootstrap(self) -> None:
        config = _IntegrationAppConfig(
            env="integration",
            use_cases=_UseCasesSettings(
                products_list=_ProductUseCaseSettings(
                    timeout_ms=750,
                    pagination_mode="offset",
                )
            ),
            cache=_CacheSettings(backend="redis", ttl_seconds=120),
        )
        result = create_kernel(config=config, use_cases=[ReadTypedConfigUseCase])
        executor = RuntimeExecutor(result.compiler)

        use_case = result.factory.build(ReadTypedConfigUseCase)
        snapshot = await executor.execute(use_case)

        assert snapshot.env == "integration"
        assert snapshot.cache_store == "redis"
        assert snapshot.timeout_ms == 750
        assert snapshot.pagination_mode == "offset"

    @pytest.mark.asyncio
    async def test_use_case_can_resolve_typed_config_loaded_from_yaml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("CACHE_TTL", "300")
        config_file = tmp_path / "integration-config.yaml"
        config_file.write_text(
            "\n".join(
                [
                    "app:",
                    "  env: test-yaml",
                    "  cache:",
                    "    backend: memory",
                    "    ttl_seconds: ${oc.decode:${oc.env:CACHE_TTL}}",
                    "  use_cases:",
                    "    products_list:",
                    "      timeout_ms: 900",
                    "      pagination_mode: cursor",
                ]
            )
        )

        raw_cfg = load_config(str(config_file))
        typed_cfg = section(raw_cfg, "app", _IntegrationAppConfig)

        result = create_kernel(config=typed_cfg, use_cases=[ReadTypedConfigUseCase])
        executor = RuntimeExecutor(result.compiler)
        use_case = result.factory.build(ReadTypedConfigUseCase)
        snapshot = await executor.execute(use_case)

        assert snapshot.env == "test-yaml"
        assert snapshot.cache_store == "memory"
        assert snapshot.timeout_ms == 900
        assert snapshot.pagination_mode == "cursor"
