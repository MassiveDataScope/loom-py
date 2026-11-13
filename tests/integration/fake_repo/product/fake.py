"""An in-memory ``ProductRepo`` for tests that serve products without a database."""

from __future__ import annotations

from typing import Any

from loom.core.repository.abc.query import CursorResult, PageResult, QuerySpec
from loom.testing.in_memory import InMemoryRepository
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository_contract import ProductRepo


# ProductRepo is a declared base, as on ProductRepository, so the harness binds
# this fake under it; InMemoryRepository's paging and create signatures differ
# from RepoFor's, which no harness route relies on.
class InMemoryProductRepository(  # pyright: ignore[reportIncompatibleMethodOverride]
    InMemoryRepository[Product], ProductRepo
):
    """Products in memory, found by name as ``ProductRepository`` finds them."""

    def __init__(self) -> None:
        super().__init__(Product)

    async def get_by_name(self, name: str) -> Product | None:
        """The product whose name matches *name*, ignoring case and surrounding blanks."""
        wanted = name.strip().lower()
        return next((p for p in self._store.values() if p.name.lower() == wanted), None)

    async def get_by(self, field: str, value: Any, profile: str = "default") -> Product | None:
        del profile
        return next((p for p in self._store.values() if getattr(p, field) == value), None)

    async def exists_by(self, field: str, value: Any) -> bool:
        return await self.get_by(field, value) is not None

    async def count(self) -> int:
        return len(self._store)

    async def list_with_query(
        self, query: QuerySpec, profile: str = "default"
    ) -> PageResult[Product] | CursorResult[Product]:
        del query, profile
        raise NotImplementedError("InMemoryProductRepository does not run query specs")
