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
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

BOTO3_ISLAND = "loom.core.connections.aws.client"
MONGO_ISLAND = "loom.core.connections.mongo.client"
CLICKHOUSE_ISLAND = "loom.core.connections.clickhouse.client"
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


def without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``config-ssm`` extra is absent."""
    without_extra(monkeypatch, ["boto3"], [BOTO3_ISLAND])


def with_boto3(monkeypatch: pytest.MonkeyPatch, stub: object) -> None:
    """*stub* stands in for boto3 (the AWS island calls ``stub.Session(...).client(...)``)."""
    with_stubbed_extra(monkeypatch, {"boto3": stub}, [BOTO3_ISLAND])


class AwsClientFactory:
    """Stands in for boto3 behind the AWS island: hands out *client*, records what was asked.

    Keeps the assertions of a patched ``boto3.client``: ``call_count``,
    ``assert_called_once_with(service, region_name=...)`` and ``assert_not_called()``.
    """

    def __init__(self, client: Any) -> None:
        self.client = client
        self.calls: list[tuple[str, str | None]] = []
        self.options: list[dict[str, Any]] = []

    def session(self, *, profile_name: str | None = None) -> Any:
        factory = self

        class _Session:
            def client(self, service: str, **options: Any) -> Any:
                factory.calls.append((service, options.get("region_name")))
                factory.options.append(options)
                return factory.client

        return _Session()

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def assert_called_once_with(self, service: str, *, region_name: str | None) -> None:
        assert self.calls == [(service, region_name)], self.calls

    def assert_not_called(self) -> None:
        assert self.calls == [], self.calls


@contextmanager
def patched_aws_clients(client: Any = None) -> Iterator[AwsClientFactory]:
    """Make every client the AWS island builds *client* (a ``MagicMock`` by default)."""
    factory = AwsClientFactory(client if client is not None else MagicMock())
    with patch("boto3.Session", side_effect=factory.session):
        yield factory


class FakeSdkError(Exception):
    """The error a :class:`FakeAwsIsland` reports as the SDK's own."""


class FakeAwsIsland:
    """Stands in for the AWS island behind :class:`AwsClients`: a new mock client per build.

    Args:
        build_seconds: How long each build takes, to widen a race between threads.
        setup: Called with each client built, to script what it answers.

    Attributes:
        sessions: The profile of every session opened.
        built: The service and settings of every client built, in order.
        created: Every client built, in order.
    """

    def __init__(
        self, build_seconds: float = 0.0, setup: Callable[[MagicMock], None] | None = None
    ) -> None:
        self._build_seconds = build_seconds
        self._setup = setup
        self.sessions: list[str | None] = []
        self.built: list[tuple[str, Any]] = []
        self.created: list[MagicMock] = []

    def open_session(self, profile: str | None) -> object:
        self.sessions.append(profile)
        return object()

    def build_aws_client(self, session: object, service: str, config: Any) -> MagicMock:
        time.sleep(self._build_seconds)
        self.built.append((service, config))
        client = MagicMock(name=f"{service}-client")
        self.created.append(client)
        if self._setup is not None:
            self._setup(client)
        return client

    def sdk_errors(self) -> tuple[type[Exception], ...]:
        return (FakeSdkError,)


def fake_aws_island(
    monkeypatch: pytest.MonkeyPatch,
    build_seconds: float = 0.0,
    setup: Callable[[MagicMock], None] | None = None,
) -> FakeAwsIsland:
    """Make :class:`AwsClients` and the Lambda consumer use a :class:`FakeAwsIsland`."""
    island = FakeAwsIsland(build_seconds, setup)
    monkeypatch.setattr("loom.core.connections.aws.clients.aws_island", lambda: island)
    monkeypatch.setattr("loom.serverless.aws.lambda_._jobs.aws_island", lambda: island)
    return island
