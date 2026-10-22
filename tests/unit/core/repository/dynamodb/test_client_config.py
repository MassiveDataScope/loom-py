"""``persistence.dynamodb``: timeouts, retries and parallelism reach the boto3 client."""

from __future__ import annotations

import time
from typing import Any

import msgspec
import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.connections.aws.client import build_aws_client
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.dynamodb.backend import DynamoDBBackend, DynamoDBRepositoryConfig
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from tests.helpers.silent_endpoint import silent_endpoint  # noqa: F401 - pytest fixture

from .conftest import Product

_REQUIRED = {"region": "eu-west-1", "table": "products"}


def _build(monkeypatch: pytest.MonkeyPatch, section: dict[str, Any]) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    class _Session:
        def __init__(self, **kwargs: Any) -> None:
            seen["session"] = kwargs

        def client(self, service: str, **kwargs: Any) -> object:
            seen.update(kwargs, service=service)
            return object()

    import boto3  # type: ignore[import-untyped]

    monkeypatch.setattr(boto3, "Session", _Session)
    ctx = ConfigContext.from_dict(
        {"persistence": {"backend": "dynamodb", "dynamodb": {**_REQUIRED, **section}}}
    )
    DynamoDBBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))
    return seen


def test_bounded_defaults_reach_the_client(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build(monkeypatch, {})["config"]

    assert config.connect_timeout == 2
    assert config.read_timeout == 5
    assert config.retries == {"total_max_attempts": 3, "mode": "standard"}
    assert config.max_pool_connections == 32


def test_declared_values_reach_the_client(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build(
        monkeypatch,
        {
            "connect_timeout": 1,
            "read_timeout": 3,
            "max_attempts": 5,
            "retry_mode": "adaptive",
            "pool": {"size": 8},
        },
    )["config"]

    assert (config.connect_timeout, config.read_timeout, config.max_pool_connections) == (1, 3, 8)
    assert config.retries == {"total_max_attempts": 5, "mode": "adaptive"}


@pytest.mark.parametrize(
    ("section", "named"),
    [
        ({"read_timout": 1}, "read_timout"),
        ({"retry_mode": "legacy"}, "retry_mode"),
        ({"connect_timeout": 0}, "connect_timeout"),
        ({"read_timeout": 0}, "read_timeout"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"pool": {"size": 0}}, "pool.size"),
    ],
)
def test_an_invalid_key_is_refused_naming_it(section: dict[str, Any], named: str) -> None:
    with pytest.raises(msgspec.ValidationError, match=named):
        msgspec.convert({**_REQUIRED, **section}, DynamoDBRepositoryConfig)


def test_a_missing_section_still_names_the_section() -> None:
    ctx = ConfigContext.from_dict({"persistence": {"backend": "dynamodb"}})

    with pytest.raises(ConfigError, match="persistence.dynamodb"):
        DynamoDBBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))


async def test_a_silent_service_fails_within_the_declared_bounds(
    monkeypatch: pytest.MonkeyPatch,
    silent_endpoint: str,  # noqa: F811
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    cfg = msgspec.convert(
        {
            **_REQUIRED,
            "url": silent_endpoint,
            "connect_timeout": 1,
            "read_timeout": 1,
            "max_attempts": 2,
        },
        DynamoDBRepositoryConfig,
    )
    repository: RepositoryDynamoDB[Any, Any] = RepositoryDynamoDB(
        build_aws_client("dynamodb", cfg), "products", Product
    )

    started = time.perf_counter()
    with pytest.raises(Exception) as raised:
        await repository.get_by_id(1)
    elapsed = time.perf_counter() - started

    # (connect + read) x attempts plus at most one second of standard backoff jitter.
    assert elapsed < (1 + 1) * 2 + 1
    assert "timeout" in type(raised.value).__name__.lower() or "timed out" in str(raised.value)
