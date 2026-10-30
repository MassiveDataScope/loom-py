"""A worker serves repositories for the models it declares, and refuses at start-up otherwise.

A job or use case that reads a model the worker never discovered fails
``bootstrap_worker`` with :class:`~loom.core.use_case.errors.UnservedModelError`
(raised by ``factory.verify``, as in the API) instead of failing its first
task. Declared, the model is served in full, cursor pagination included.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from celery.signals import worker_init, worker_shutdown

from loom.celery.bootstrap import bootstrap_worker
from loom.celery.constants import TASK_JOB_PREFIX
from loom.core.backend.sqlalchemy import reset_registry
from loom.core.command import Command
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc.query import CursorResult, PaginationMode, QuerySpec
from loom.core.use_case.errors import UnservedModelError, UseCaseErrorCode
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.markers import LoadById
from loom.core.use_case.use_case import UseCase


class _Order(BaseModel):
    __tablename__ = "worker_models_orders"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    label: str = ColumnField(length=40)


class _CreateOrder(Command, frozen=True):
    label: str


class _LoadOrderJob(Job[str]):
    async def execute(self, order_id: int, order: _Order = LoadById(_Order, by="order_id")) -> str:
        return order.label


class _CountOrders(UseCase[_Order, int]):
    async def execute(self) -> int:
        return 0


class _PageThroughOrders(UseCase[_Order, list[list[str]]]):
    """Create three orders, then read them back two at a time by cursor."""

    async def execute(self) -> list[list[str]]:
        for label in ("a", "b", "c"):
            await self.main_repo.create(_CreateOrder(label=label))
        pages: list[list[str]] = []
        cursor: str | None = None
        while True:
            query = QuerySpec(pagination=PaginationMode.CURSOR, limit=2, cursor=cursor)
            page = cast(CursorResult[_Order], await self.main_repo.list_with_query(query))
            pages.append([order.label for order in page.items])
            if not page.has_next:
                return pages
            cursor = page.next_cursor


class _PageThroughOrdersJob(Job[list[list[str]]]):
    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> list[list[str]]:
        return list(await self._app.invoke(_PageThroughOrders))


class _SoloWorker:
    pool_cls = "solo"


_SIGNALS = (worker_init, worker_shutdown)


@pytest.fixture(autouse=True)
def _isolated() -> Iterator[None]:
    saved = [list(cast(list[Any], signal.receivers)) for signal in _SIGNALS]
    yield
    for signal, receivers in zip(_SIGNALS, saved, strict=True):
        cast(list[Any], signal.receivers)[:] = receivers
    reset_registry()


def _config(tmp_path: Path, *, discover: bool) -> str:
    document: dict[str, Any] = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "database": {"url": f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    if discover:
        document["app"] = {"discovery": {"mode": "modules", "modules": {"include": [__name__]}}}
    path = tmp_path / "worker.yaml"
    path.write_text(yaml.safe_dump(document))
    return str(path)


def test_a_job_loading_an_undeclared_model_fails_the_start_up(tmp_path: Path) -> None:
    with pytest.raises(UnservedModelError) as caught:
        bootstrap_worker(
            _config(tmp_path, discover=False), jobs=[_LoadOrderJob], environment="test"
        )

    issue = caught.value.issue
    assert issue.code is UseCaseErrorCode.UNSERVED_MODEL
    assert (issue.component, issue.field) == ("_LoadOrderJob", "order")
    assert caught.value.model is _Order
    assert "app.discovery" in issue.message


def test_a_main_repository_over_an_undeclared_model_fails_the_start_up(tmp_path: Path) -> None:
    with pytest.raises(UnservedModelError) as caught:
        bootstrap_worker(
            _config(tmp_path, discover=False), use_cases=[_CountOrders], environment="test"
        )

    issue = caught.value.issue
    assert issue.code is UseCaseErrorCode.UNSERVED_MODEL
    assert (issue.component, issue.field) == ("_CountOrders", "main_repo")
    assert caught.value.model is _Order


def test_a_declared_model_serves_cursor_pagination_to_a_job(tmp_path: Path) -> None:
    result = bootstrap_worker(_config(tmp_path, discover=True), environment="test")
    task = result.celery_app.tasks[f"{TASK_JOB_PREFIX}.{_PageThroughOrdersJob.__qualname__}"]

    worker_init.send(sender=_SoloWorker())
    try:
        pages = task.apply().get()
    finally:
        worker_shutdown.send(sender=None)

    assert pages == [["a", "b"], ["c"]]
