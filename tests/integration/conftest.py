from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.testing import RepositoryIntegrationHarness, build_repository_harness
from tests.integration.fake_repo.product.category.model import Category
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.relations import ProductCategoryLink
from tests.integration.fake_repo.product.review.model import ProductReview


@pytest.fixture
async def integration_context(tmp_path: Path) -> AsyncGenerator[RepositoryIntegrationHarness, None]:
    reset_registry()
    compile_all(Category, ProductReview, ProductCategoryLink, Product)

    db_path = tmp_path / "integration.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"

    manager = SessionManager(
        database_url,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )

    integration_context = build_repository_harness(
        session_manager=manager,
        cursor_codec=CursorCodec(signing="ephemeral"),
        models={
            "product": Product,
            "category": Category,
            "category_link": ProductCategoryLink,
            "review": ProductReview,
        },
        load_order=("product", "category", "category_link", "review"),
    )
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().create_all)
    try:
        yield integration_context
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.dispose()
        reset_registry()
        if db_path.exists():
            db_path.unlink()
