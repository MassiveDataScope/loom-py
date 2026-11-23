"""Connection-pool metrics: gauges at scrape time, checkout timing and timeouts."""

from __future__ import annotations

import gc
from pathlib import Path

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.pool import AsyncAdaptedQueuePool

from loom.core.environment import Environment
from loom.core.model.redaction import Dsn
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.database import DatabaseConfig, PoolConfig
from loom.core.stores.profile import SERVER_PROFILE
from loom.prometheus.db_pool import PrometheusPoolMetrics
from loom.rest.fastapi.auto import create_app
from loom.stores.sqlalchemy.session_manager import SessionManager, build_session_manager
from tests.unit.rest._fixture_app import write_project

_LABELS = {"application": "shop", "entrypoint": "server", "database": "default"}


def _manager(
    tmp_path: Path, registry: CollectorRegistry | None, *, size: int = 1
) -> SessionManager:
    config = DatabaseConfig(
        url=Dsn(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"),
        pool=PoolConfig(size=size, max_overflow=0, timeout=1),
    )
    sink = (
        PrometheusPoolMetrics(registry, application="shop", entrypoint="server")
        if registry is not None
        else None
    )
    return build_session_manager(config, SERVER_PROFILE, pool_metrics=sink, database=DEFAULT_STORE)


def _sample(registry: CollectorRegistry, name: str) -> float | None:
    return registry.get_sample_value(name, _LABELS)


class TestDbPoolMetrics:
    async def test_gauges_reflect_the_pool_when_scraped(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        manager = _manager(tmp_path, registry, size=2)
        try:
            assert _sample(registry, "loom_db_pool_checked_out") == 0
            async with manager.engine.connect():
                assert _sample(registry, "loom_db_pool_checked_out") == 1
                assert _sample(registry, "loom_db_pool_size") == 2
                assert _sample(registry, "loom_db_pool_overflow") == 0
        finally:
            await manager.close()

    async def test_the_instrumented_pool_logs_under_sqlalchemys_namespace(
        self, tmp_path: Path
    ) -> None:
        manager = _manager(tmp_path, CollectorRegistry())
        try:
            assert not manager.engine.sync_engine.pool.logger.name.startswith("loom.")
        finally:
            await manager.close()

    async def test_each_checkout_is_timed(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        manager = _manager(tmp_path, registry)
        try:
            for _ in range(3):
                async with manager.engine.connect():
                    pass
        finally:
            await manager.close()

        assert _sample(registry, "loom_db_pool_checkout_seconds_count") == 3

    async def test_a_checkout_that_runs_out_of_time_is_counted(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        manager = _manager(tmp_path, registry)
        try:
            async with manager.engine.connect():
                with pytest.raises(PoolTimeoutError):
                    async with manager.engine.connect():
                        pass
        finally:
            await manager.close()

        assert _sample(registry, "loom_db_pool_checkout_timeouts_total") == 1

    async def test_without_a_sink_the_pool_is_the_dialect_default(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        manager = _manager(tmp_path, None)
        try:
            assert type(manager.engine.sync_engine.pool) is AsyncAdaptedQueuePool
        finally:
            await manager.close()

        assert not [m for m in registry.collect() if m.name.startswith("loom_db_pool")]

    def test_several_sinks_can_share_one_registry(self) -> None:
        registry = CollectorRegistry()

        PrometheusPoolMetrics(registry, application="shop", entrypoint="server")
        PrometheusPoolMetrics(registry, application="shop", entrypoint="worker")

    async def test_a_collected_engine_stops_being_reported(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        manager = _manager(tmp_path, registry)
        await manager.close()
        del manager
        gc.collect()

        assert _sample(registry, "loom_db_pool_size") is None

    def test_create_app_with_prometheus_enabled_reports_its_pool(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        config_path = write_project(
            tmp_path,
            observability={"prometheus": {"enabled": True}},
            store={"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"},
        )

        create_app(config_path, metrics_registry=registry, environment=Environment.TEST)

        labels = {"application": "config-demo", "entrypoint": "server", "database": "default"}
        assert registry.get_sample_value("loom_db_pool_size", labels) == 5

    def test_create_app_without_prometheus_reports_no_pool(self, tmp_path: Path) -> None:
        registry = CollectorRegistry()
        config_path = write_project(
            tmp_path,
            store={"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"},
        )

        create_app(config_path, metrics_registry=registry, environment=Environment.TEST)

        assert not [m for m in registry.collect() if m.name.startswith("loom_db_pool")]

    async def test_a_rebuilt_engine_replaces_the_series_of_the_previous_one(
        self, tmp_path: Path
    ) -> None:
        registry = CollectorRegistry()
        first = _manager(tmp_path, registry, size=1)
        second = _manager(tmp_path, registry, size=3)
        try:
            sizes = [
                sample.value
                for metric in registry.collect()
                if metric.name == "loom_db_pool_size"
                for sample in metric.samples
            ]
            assert sizes == [3]
        finally:
            await first.close()
            await second.close()
