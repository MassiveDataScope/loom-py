"""Simulate an optional extra that is absent, or replaced by a stub, for one test.

An island imports its SDK at the top and is cached in ``sys.modules`` once
loaded, so blocking the SDK alone would not affect a test that runs after the
island was imported. Both helpers therefore evict the island as well: the next
``import_optional`` re-imports it against the blocked or stubbed SDK.
``sys.modules[name] = None`` is what makes ``import name`` raise
``ImportError``; ``monkeypatch`` restores every entry afterwards.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping

import pytest

BOTO3_ISLAND = "loom.connections.aws.client"
MONGO_ISLAND = "loom.connections.mongo.client"
CLICKHOUSE_ISLAND = "loom.connections.clickhouse.client"
ENGINE_EXTRAS = "loom.ai.engines.pydantic_ai.extras"
ENGINE_PROVIDERS = "loom.ai.engines.pydantic_ai.providers"


def without_extra(
    monkeypatch: pytest.MonkeyPatch, sdk_modules: Iterable[str], islands: Iterable[str]
) -> None:
    """Make *sdk_modules* unimportable and evict *islands* so they re-import against that."""
    for name in sdk_modules:
        monkeypatch.setitem(sys.modules, name, None)
    for island in islands:
        monkeypatch.setitem(sys.modules, island, None)


def with_stubbed_extra(
    monkeypatch: pytest.MonkeyPatch, stubs: Mapping[str, object], islands: Iterable[str]
) -> None:
    """Serve *stubs* as the SDK modules and evict *islands* so they import the stubs."""
    for name, stub in stubs.items():
        monkeypatch.setitem(sys.modules, name, stub)
    for island in islands:
        monkeypatch.delitem(sys.modules, island, raising=False)
