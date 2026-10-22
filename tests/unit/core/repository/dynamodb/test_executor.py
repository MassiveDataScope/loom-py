"""DynamoDB calls run on the backend's executor, sized by ``pool.size``."""

from __future__ import annotations

import asyncio
import contextvars
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

import loom.core.repository.dynamodb.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB

from .conftest import Product

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


def test_the_backend_sizes_its_executor_by_the_pool_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[int] = []
    real = backend_module.ThreadPoolExecutor

    def _executor(*, max_workers: int, thread_name_prefix: str) -> ThreadPoolExecutor:
        created.append(max_workers)
        return real(max_workers=max_workers, thread_name_prefix=thread_name_prefix)

    monkeypatch.setattr(backend_module, "ThreadPoolExecutor", _executor)
    monkeypatch.setattr(backend_module, "build_aws_client", lambda _service, _cfg: _SlowClient())
    ctx = ConfigContext.from_dict(
        {
            "persistence": {
                "backend": "dynamodb",
                "dynamodb": {"region": "eu-west-1", "table": "t", "pool": {"size": 16}},
            }
        }
    )

    DynamoDBBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))

    assert created == [16]
