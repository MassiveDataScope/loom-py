from __future__ import annotations

from typing import Any, cast

import msgspec
from sqlalchemy import func, select

from loom.core.repository import repository_for
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy, with_session_scope
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository_contract import ProductRepo


@repository_for(Product)
class ProductRepository(RepositorySQLAlchemy[Product, int], ProductRepo):
    """Custom Product repository used as the automatic main repository."""

    async def create(self, data: msgspec.Struct) -> Product:
        """Normalize the product name before delegating to the base create flow."""
        payload = msgspec.to_builtins(data)
        if not isinstance(payload, dict):
            raise TypeError("Struct payload must serialize to dict")
        name = payload.get("name")
        if isinstance(name, str):
            payload["name"] = name.strip()
        return await super().create(cast(Any, payload))

    @with_session_scope(write=False)
    async def get_by_name(self, session: Any, name: str) -> Product | None:
        """Lookup a product by normalized name."""
        stmt = select(self._effective_sa_model).where(
            func.lower(self._column_for_field("name")) == name.strip().lower()
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return cast(Product, self._to_output(row))
