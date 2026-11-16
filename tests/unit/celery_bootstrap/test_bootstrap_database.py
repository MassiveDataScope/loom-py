"""The worker reads ``stores.default`` through the shared contract and the worker profile."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("celery")

from loom.celery.adapter import CeleryAdapter  # noqa: E402
from loom.composition import AppConfig  # noqa: E402
from loom.core.config import ConfigContext, ConfigError  # noqa: E402
from loom.core.job.job import Job  # noqa: E402
from loom.core.stores.uow.abc import UnitOfWorkFactory  # noqa: E402
from tests.helpers.composition import composition_project, uow_factory  # noqa: E402

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


class _PingJob(Job[int]):
    def execute(self) -> int:
        return 1


def _resolve_uow_factory(config: dict[str, Any]) -> UnitOfWorkFactory | None:
    """Compose a worker from *config* and return the unit-of-work factory it runs jobs with."""
    project = composition_project(
        ConfigContext.from_dict(
            {"celery": {"broker_url": "memory://", "result_backend": "cache+memory://"}, **config}
        ),
        app=AppConfig(name="worker"),
    )
    return uow_factory(project.compose(CeleryAdapter(jobs=[_PingJob])))


def _pool(config: dict[str, Any]) -> Any:
    factory: Any = _resolve_uow_factory(config)
    assert factory is not None
    return factory._session_manager.engine.sync_engine.pool


class TestBootstrapDatabase:
    def test_without_a_database_section_the_worker_runs_without_a_unit_of_work(self) -> None:
        assert _resolve_uow_factory({}) is None

    def test_named_streaming_databases_alone_give_the_worker_no_unit_of_work(self) -> None:
        config = {"stores": {"orders": {"backend": "sqlalchemy", "url": _POSTGRES}}}

        assert _resolve_uow_factory(config) is None

    def test_the_worker_profile_sizes_an_undeclared_pool(self) -> None:
        pool = _pool({"stores": {"default": {"backend": "sqlalchemy", "url": _POSTGRES}}})

        assert (pool.size(), pool._max_overflow, pool._timeout, pool._recycle) == (2, 0, 30, 1800)

    def test_a_declared_pool_key_wins_over_the_worker_profile(self) -> None:
        pool = _pool(
            {
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": _POSTGRES, "pool": {"size": 4}}
                }
            }
        )

        assert (pool.size(), pool._max_overflow) == (4, 0)

    @pytest.mark.parametrize(
        "database",
        [
            {"pool": {"size": 5}},
            {"uri": _POSTGRES, "pool": {"size": 5}},
            {"URL": _POSTGRES},
            {"url": _POSTGRES, "pool": {"sise": 1}},
            {"url": _POSTGRES, "pool_size": 3},
            {"url": _POSTGRES, "pool": {"size": 0}},
        ],
    )
    def test_an_invalid_database_section_fails_the_worker_instead_of_dropping_the_database(
        self,
        database: dict[str, Any],
    ) -> None:
        with pytest.raises(ConfigError):
            _resolve_uow_factory({"stores": {"default": {"backend": "sqlalchemy", **database}}})
