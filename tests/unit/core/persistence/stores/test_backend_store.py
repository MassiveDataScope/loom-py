"""Every built-in backend serves the store the host names in ``runtime.store`` (ADR 0008)."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.stores import resolve_backend
from loom.core.stores.catalog import StoreKey, load_catalog
from loom.core.stores.profile import SERVER_PROFILE, BackendRuntime


class TestBackendStoreLookup:
    @pytest.mark.parametrize("name", ["sqlalchemy", "mongo", "dynamodb"])
    def test_a_backend_asks_the_catalog_for_the_store_it_was_handed(self, name: str) -> None:
        ctx = ConfigContext.from_dict({"stores": {"default": {"backend": name}}})
        runtime = BackendRuntime(
            SERVER_PROFILE, catalog=load_catalog(ctx), store=StoreKey("events")
        )
        backend = resolve_backend(name)

        with pytest.raises(ConfigError, match=rf"the '{name}' backend needs stores\.events"):
            backend.build(ctx, (), runtime)
