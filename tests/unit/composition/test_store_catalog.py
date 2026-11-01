"""One store catalog per composition: loaded once, shared through DI, each store resolved once.

ADR 0008: the composition root loads ``stores`` once; the backend, the
container and every other reader take that catalog instead of reading the
configuration again, and a store's section resolves its placeholders once.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pytest

import loom.core.persistence.stores as stores_module
import loom.core.repository.mongo.backend as mongo_backend_module
from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, build_application, compose_application
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, StoreSpec
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory, composition_source
from tests.helpers.persistence import record_catalogs
from tests.unit.core.repository.mongo._fake import FakeMongoClient
from tests.unit.rest._fixture_app import UUID4_ID_FIELD

_CELERY = {"broker_url": "memory://", "result_backend": "cache+memory://"}


class _PingJob(Job[int]):
    def execute(self) -> int:
        return 1


class _CountingResolver:
    """Serves ``${count:<key>}`` from *values*, counting each lookup."""

    name = "count"

    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.calls: Counter[str] = Counter()

    def resolve(self, key: str) -> object:
        self.calls[key] += 1
        return self.values[key]


@pytest.fixture
def catalogs(monkeypatch: pytest.MonkeyPatch) -> list[StoreCatalog]:
    return record_catalogs(monkeypatch)


@pytest.fixture
def store_resolutions(monkeypatch: pytest.MonkeyPatch) -> Counter[str]:
    """Count each resolution of a store's section, by the store's path."""
    resolutions: Counter[str] = Counter()
    resolve = ConfigContext.section

    def counted(self: ConfigContext, key: Any, target: type[Any]) -> Any:
        if str(key).startswith("stores.") and str(key).count(".") == 1:
            resolutions[str(key)] += 1
        return resolve(self, key, target)

    monkeypatch.setattr(ConfigContext, "section", counted)
    return resolutions


def _worker_source(stores: dict[str, Any]) -> Any:
    return composition_source(
        ConfigContext.from_dict({"stores": stores, "celery": _CELERY}),
        app=AppConfig(name="worker"),
    )


def test_the_api_builds_its_catalog_once_and_binds_that_one(
    fixture_project: ProjectFactory, catalogs: list[StoreCatalog]
) -> None:
    application = build_application(
        fixture_project("catalog_api"), adapter=FastAPIAdapter(), environment="test"
    )

    assert len(catalogs) == 1
    assert application.kernel.container.resolve(StoreCatalog) is catalogs[0]


def test_the_worker_builds_its_catalog_once_and_binds_that_one(
    catalogs: list[StoreCatalog],
) -> None:
    source = _worker_source(
        {DEFAULT_STORE: {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}}
    )

    application = compose_application(source, CeleryAdapter(jobs=[_PingJob]))

    assert len(catalogs) == 1
    assert application.kernel.container.resolve(StoreCatalog) is source.catalog is catalogs[0]


def test_a_connection_only_default_store_fails_the_composition() -> None:
    source = _worker_source({DEFAULT_STORE: {"backend": "clickhouse", "url": "clickhouse://ch"}})

    adapter = CeleryAdapter(jobs=[_PingJob])
    with pytest.raises(ConfigError, match=r"stores\.default\.backend is 'clickhouse'"):
        compose_application(source, adapter)


def test_a_sqlalchemy_store_resolves_its_section_once_per_composition(
    fixture_project: ProjectFactory, store_resolutions: Counter[str]
) -> None:
    counter = _CountingResolver({"url": "sqlite+aiosqlite:///"})
    path = fixture_project(
        "resolved_sqlalchemy", store={"backend": "sqlalchemy", "url": "${count:url}"}
    )

    build_application(path, adapter=FastAPIAdapter(), environment="test", resolvers=[counter])

    assert counter.calls == Counter({"url": 1})
    assert store_resolutions == Counter({"stores.default": 1})


def test_a_mongo_store_resolves_its_section_once_per_composition(
    fixture_project: ProjectFactory,
    store_resolutions: Counter[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeMongoClient()
    monkeypatch.setattr(
        mongo_backend_module, "build_async_mongo_client", lambda _cfg, **_extra: client
    )
    counter = _CountingResolver({"url": "mongodb://localhost:27017", "database": "demo"})
    path = fixture_project(
        "resolved_mongo",
        store={"backend": "mongo", "url": "${count:url}", "database": "${count:database}"},
        id_field=UUID4_ID_FIELD,
    )

    build_application(path, adapter=FastAPIAdapter(), environment="test", resolvers=[counter])

    assert counter.calls == Counter({"url": 1, "database": 1})
    # settings() for the retired keys and decode() share the one resolution.
    assert store_resolutions == Counter({"stores.default": 1})


def test_a_store_spec_resolves_once_for_settings_and_decode(
    store_resolutions: Counter[str],
) -> None:
    ctx = ConfigContext.from_dict({"stores": {"default": {"backend": "none", "url": "u"}}})
    spec = StoreSpec(key=DEFAULT_STORE, backend="none", config=ctx)

    settings = spec.settings()
    settings["url"] = "changed"

    assert spec.decode(dict) == {"url": "u"}
    assert store_resolutions == Counter({"stores.default": 1})


def test_the_catalog_reads_the_registered_backends_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scans: list[str] = []
    scan = stores_module.registered_names

    def counted(group: str) -> tuple[str, ...]:
        scans.append(group)
        return scan(group)

    monkeypatch.setattr(stores_module, "registered_names", counted)
    stores = {key: {"backend": "sqlalchemy"} for key in ("default", "audit", "archive")}

    stores_module.load_catalog(ConfigContext.from_dict({"stores": stores}))

    assert scans == ["loom.persistence.backends"]
