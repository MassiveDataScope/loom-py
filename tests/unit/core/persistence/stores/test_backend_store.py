"""Every built-in backend serves the store the host names in ``runtime.store`` (ADR 0008)."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.persistence import resolve_backend
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.persistence.stores import StoreKey, load_catalog


@pytest.mark.parametrize("name", ["sqlalchemy", "mongo", "dynamodb"])
def test_a_backend_asks_the_catalog_for_the_store_it_was_handed(name: str) -> None:
    ctx = ConfigContext.from_dict({"stores": {"default": {"backend": name}}})
    runtime = BackendRuntime(SERVER_PROFILE, catalog=load_catalog(ctx), store=StoreKey("events"))

    with pytest.raises(ConfigError, match=rf"the '{name}' backend needs stores\.events"):
        resolve_backend(name).build(ctx, (), runtime)
