"""Integration tests — bootstrap and transaction lifecycle.

Verifies key framework behaviors that require a real async stack but no
external services:

  - ``LoadById`` marker raises ``NotFound`` when the entity is absent.
  - A use-case that raises an exception rolls back the transaction: no data
    is persisted.
  - A successful use-case commits: data is queryable after the call.
  - Multiple use-cases share the same repository instance within a session.

Infrastructure:
    - SQLite ``:memory:`` via ``aiosqlite``
    - ``build_sqlalchemy_repository_registration_module``
    - ``SQLAlchemyUnitOfWorkFactory`` for transaction lifecycle
    - ``create_kernel`` to wire everything as a production-equivalent runtime
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.bootstrap.kernel import create_kernel
from loom.core.errors import NotFound
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.use_case import Input
from loom.core.use_case.markers import LoadById
from loom.core.use_case.use_case import UseCase
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.schemas import CreateProduct


class _Cfg:
    pass


class _CreateProductUC(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class _GetProductUC(UseCase[Product, Product | None]):
    async def execute(self, product_id: int) -> Product | None:
        return await self.main_repo.get_by_id(product_id)


class _FailAfterCreateUC(UseCase[Product, None]):
    """Creates a product but then raises — transaction must be rolled back."""

    async def execute(self, cmd: CreateProduct = Input()) -> None:
        await self.main_repo.create(cmd)
        raise RuntimeError("simulated failure after write")


class _LoadByIdUC(UseCase[Product, str]):
    async def execute(
        self,
        product_id: int,
        product: Product = LoadById(Product, by="product_id"),
    ) -> str:
        return product.name


@pytest.fixture
async def sa_env(tmp_path: Path):  # type: ignore[no-untyped-def]
    """Yield a (session_manager, repo_module) for SQLite in-memory tests."""
    reset_registry()
    compile_all(Product)
    db_path = tmp_path / "bootstrap_test.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"

    manager = SessionManager(
        database_url,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().create_all)

    repo_module = build_sqlalchemy_repository_registration_module(
        manager, (Product,), cursor_codec=CursorCodec(signing="ephemeral")
    )

    try:
        yield manager, repo_module
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.dispose()
        reset_registry()
        if db_path.exists():
            db_path.unlink()


# ---------------------------------------------------------------------------
# Transaction commit: successful use-case persists data
# ---------------------------------------------------------------------------


class TestTransactionCommit:
    @pytest.mark.asyncio
    async def test_successful_use_case_persists_entity(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_CreateProductUC, _GetProductUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        await runtime.app.invoke(_CreateProductUC, payload={"name": "keyboard", "price": 120.0})

        found = await runtime.app.invoke(_GetProductUC, params={"product_id": 1})
        assert found is not None
        assert found.name == "keyboard"

    @pytest.mark.asyncio
    async def test_data_survives_across_separate_executor_calls(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_CreateProductUC, _GetProductUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        await runtime.app.invoke(_CreateProductUC, payload={"name": "mouse", "price": 25.0})
        await runtime.app.invoke(_CreateProductUC, payload={"name": "monitor", "price": 300.0})

        found = await runtime.app.invoke(_GetProductUC, params={"product_id": 2})
        assert found is not None
        assert found.name == "monitor"


# ---------------------------------------------------------------------------
# Transaction rollback: failed use-case leaves database unchanged
# ---------------------------------------------------------------------------


class TestTransactionRollback:
    @pytest.mark.asyncio
    async def test_exception_in_use_case_rolls_back_write(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_FailAfterCreateUC, _GetProductUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        with pytest.raises(RuntimeError, match="simulated failure after write"):
            await runtime.app.invoke(
                _FailAfterCreateUC,
                payload={"name": "ghost", "price": 1.0},
            )

        # Entity must NOT be visible — the transaction was rolled back.
        found = await runtime.app.invoke(_GetProductUC, params={"product_id": 1})
        assert found is None

    @pytest.mark.asyncio
    async def test_rollback_does_not_affect_prior_committed_rows(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_CreateProductUC, _FailAfterCreateUC, _GetProductUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        # Commit one product successfully.
        await runtime.app.invoke(_CreateProductUC, payload={"name": "safe", "price": 10.0})

        # Roll back a second write.
        with pytest.raises(RuntimeError):
            await runtime.app.invoke(
                _FailAfterCreateUC,
                payload={"name": "ghost", "price": 99.0},
            )

        # The committed product is still there.
        first = await runtime.app.invoke(_GetProductUC, params={"product_id": 1})
        assert first is not None
        assert first.name == "safe"

        # The rolled-back product is absent.
        second = await runtime.app.invoke(_GetProductUC, params={"product_id": 2})
        assert second is None


# ---------------------------------------------------------------------------
# LoadById marker: raises NotFound when entity is absent
# ---------------------------------------------------------------------------


class TestLoadByIdMarker:
    @pytest.mark.asyncio
    async def test_load_by_id_raises_not_found_for_missing_entity(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_LoadByIdUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        with pytest.raises(NotFound) as exc_info:
            await runtime.app.invoke(_LoadByIdUC, params={"product_id": 99})

        assert exc_info.value.entity == "Product"
        assert exc_info.value.id == 99

    @pytest.mark.asyncio
    async def test_load_by_id_returns_entity_when_present(self, sa_env) -> None:  # type: ignore[no-untyped-def]
        manager, repo_module = sa_env
        uow_factory = SQLAlchemyUnitOfWorkFactory(manager)
        runtime = create_kernel(
            config=_Cfg(),
            use_cases=[_CreateProductUC, _LoadByIdUC],
            modules=[repo_module],
            uow_factory=uow_factory,
        )

        await runtime.app.invoke(_CreateProductUC, payload={"name": "desk", "price": 200.0})

        name = await runtime.app.invoke(_LoadByIdUC, params={"product_id": 1})
        assert name == "desk"
