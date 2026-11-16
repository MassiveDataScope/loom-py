from __future__ import annotations

from typing import Protocol

from loom.core.stores.repository import RepoFor
from tests.integration.fake_repo.product.model import Product


class ProductRepo(RepoFor[Product], Protocol):
    """Custom repository contract for Product use cases and jobs."""

    async def get_by_name(self, name: str) -> Product | None:
        """Return a product matching ``name`` after repository normalization."""
        ...
