"""The worker reads ``database`` through the shared contract and the worker profile."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("celery")

from loom.celery.bootstrap import _resolve_uow_factory  # noqa: E402
from loom.core.config import ConfigContext, ConfigError  # noqa: E402

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


def _pool(config: dict[str, Any]) -> Any:
    _factory, manager = _resolve_uow_factory(ConfigContext.from_dict(config))
    assert manager is not None
    return manager.engine.sync_engine.pool


def test_without_a_database_section_the_worker_runs_without_a_unit_of_work() -> None:
    assert _resolve_uow_factory(ConfigContext.from_dict({})) == (None, None)


def test_named_streaming_databases_alone_give_the_worker_no_unit_of_work() -> None:
    config = {"database": {"orders": {"url": _POSTGRES}}}

    assert _resolve_uow_factory(ConfigContext.from_dict(config)) == (None, None)


def test_the_worker_profile_sizes_an_undeclared_pool() -> None:
    pool = _pool({"database": {"url": _POSTGRES}})

    assert (pool.size(), pool._max_overflow, pool._timeout, pool._recycle) == (2, 0, 30, 1800)


def test_a_declared_pool_key_wins_over_the_worker_profile() -> None:
    pool = _pool({"database": {"url": _POSTGRES, "pool": {"size": 4}}})

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
    database: dict[str, Any],
) -> None:
    with pytest.raises(ConfigError):
        _resolve_uow_factory(ConfigContext.from_dict({"database": database}))
