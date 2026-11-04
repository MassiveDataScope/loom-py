"""DynamoDB calls run on the backend's executor, sized by ``pool.size``."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

import loom.core.repository.dynamodb.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from tests.helpers.persistence import build_backend

from .conftest import FakeClient, Product

_BLOCK_SECONDS = 0.2
_REQUEST: contextvars.ContextVar[str] = contextvars.ContextVar("request", default="")


class _SlowClient:
    """Blocks like a network call and records the context it ran in."""

    def __init__(self) -> None:
        self.seen_requests: list[str] = []

    def get_item(self, **kwargs: Any) -> dict[str, Any]:  # noqa: ARG002 - boto3 shape
        self.seen_requests.append(_REQUEST.get())
        time.sleep(_BLOCK_SECONDS)
        return {}


async def test_sixteen_calls_progress_in_parallel_on_a_sixteen_thread_executor() -> None:
    client = _SlowClient()
    with ThreadPoolExecutor(max_workers=16) as executor:
        repository: RepositoryDynamoDB[Any, Any] = RepositoryDynamoDB(
            client, "products", Product, executor=executor
        )

        started = time.perf_counter()
        await asyncio.gather(*(repository.get_by_id(i) for i in range(16)))
        elapsed = time.perf_counter() - started

    assert elapsed < _BLOCK_SECONDS * 3


async def test_the_caller_context_is_visible_inside_the_executor_thread() -> None:
    client = _SlowClient()
    with ThreadPoolExecutor(max_workers=2) as executor:
        repository: RepositoryDynamoDB[Any, Any] = RepositoryDynamoDB(
            client, "products", Product, executor=executor
        )
        _REQUEST.set("req-42")

        await repository.get_by_id(1)

    assert client.seen_requests == ["req-42"]


def _wiring(monkeypatch: pytest.MonkeyPatch, client: FakeClient) -> PersistenceWiring:
    monkeypatch.setattr(backend_module, "build_aws_client", lambda _service, _cfg: client)
    ctx = ConfigContext.from_dict(
        {
            "stores": {
                "default": {
                    "backend": "dynamodb",
                    "region": "eu-west-1",
                    "table": "products",
                    "pool": {"size": 16},
                }
            }
        }
    )
    return build_backend(DynamoDBBackend(), ctx, (), SERVER_PROFILE)


@pytest.fixture
def created(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Sizes of the thread pools the backend creates."""
    sizes: list[int] = []
    real = backend_module.ThreadPoolExecutor

    def _executor(*, max_workers: int, thread_name_prefix: str) -> ThreadPoolExecutor:
        sizes.append(max_workers)
        return real(max_workers=max_workers, thread_name_prefix=thread_name_prefix)

    monkeypatch.setattr(backend_module, "ThreadPoolExecutor", _executor)
    return sizes


async def test_each_lifespan_runs_on_a_new_pool_sized_by_the_pool_size(
    monkeypatch: pytest.MonkeyPatch, fake_client: FakeClient, created: list[int]
) -> None:
    wiring = _wiring(monkeypatch, fake_client)
    assert wiring.readiness is not None
    answers: list[bool] = []

    for _ in range(2):
        async with wiring.lifespan_init():
            answers.append(await wiring.readiness())

    assert answers == [True, True]
    assert created == [16, 16]


async def test_a_closed_pool_refuses_work_until_the_next_lifespan(
    monkeypatch: pytest.MonkeyPatch,
    fake_client: FakeClient,
    created: list[int],
    caplog: pytest.LogCaptureFixture,
) -> None:
    wiring = _wiring(monkeypatch, fake_client)
    assert wiring.readiness is not None
    async with wiring.lifespan_init():
        pass

    with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
        ready = await wiring.readiness()

    assert ready is False
    [record] = caplog.records
    assert record.exc_info is not None
    assert record.exc_info[0] is RuntimeError
    assert created == [16]
