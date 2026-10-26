"""``bootstrap_worker`` wires the ``cache:`` section (AC5 of spec 009).

A worker whose YAML carries ``database:`` and ``cache:`` serves the marked
model through :class:`~loom.core.cache.CachedRepository`; without the section
the same model resolves to its bare repository.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from loom.celery.bootstrap import bootstrap_worker
from loom.core.cache import CachedRepository, cached
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import repository_for
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy
from tests.unit.core.cache._doubles import SERIALIZED_BACKEND


class _NoopJob(Job[None]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self) -> None:
        return None


class _CachedWorkerModel(BaseModel):
    __tablename__ = "cached_worker_model_bootstrap_test"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


@cached
@repository_for(_CachedWorkerModel)
class _CachedWorkerRepository(RepositorySQLAlchemy[_CachedWorkerModel, int]):
    """Marked repository the worker must serve wrapped when ``cache:`` is present."""


def _bootstrap(tmp_path: Path, cache: dict[str, Any] | None) -> Any:
    cfg: dict[str, Any] = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "database": {"url": f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        "app": {"discovery": {"mode": "modules", "modules": {"include": [__name__]}}},
    }
    if cache is not None:
        cfg["cache"] = cache
    config_path = tmp_path / "worker.yaml"
    config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return bootstrap_worker(str(config_path))


def test_the_marked_model_is_served_wrapped_with_the_section(tmp_path: Path) -> None:
    result = _bootstrap(tmp_path, cache={"aiocache_config": {"default": dict(SERIALIZED_BACKEND)}})

    assert isinstance(result.container.resolve_repo(_CachedWorkerModel), CachedRepository)


def test_the_marked_model_is_served_bare_without_the_section(tmp_path: Path) -> None:
    result = _bootstrap(tmp_path, cache=None)

    assert isinstance(result.container.resolve_repo(_CachedWorkerModel), _CachedWorkerRepository)
