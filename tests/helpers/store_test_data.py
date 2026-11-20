"""The ``stores`` section a test declares, built one store at a time."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from loom.core.config import ConfigContext
from loom.core.stores.catalog import StoreCatalog, load_catalog

_MONGO_DEFAULTS: Mapping[str, Any] = {"url": "mongodb://localhost", "database": "test"}


@dataclass(frozen=True)
class StoreTestData:
    """A configuration whose ``stores`` section a test grows with ``with_*`` copies.

    Args:
        stores: The section of each declared store, by store key.
    """

    stores: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def with_store(self, name: str, backend: str, **section: Any) -> StoreTestData:
        """Return a copy that also declares *name*, served by *backend*."""
        return replace(self, stores={**self.stores, name: {"backend": backend, **section}})

    def with_mongo_store(self, name: str, **section: Any) -> StoreTestData:
        """Return a copy that also declares *name* as a Mongo store with a url and a database."""
        return self.with_store(name, "mongo", **{**_MONGO_DEFAULTS, **section})

    def context(self) -> ConfigContext:
        """The configuration holding the declared stores."""
        return ConfigContext.from_dict({"stores": dict(self.stores)})

    def catalog(self) -> StoreCatalog:
        """The catalog the declared stores load into."""
        return load_catalog(self.context())
