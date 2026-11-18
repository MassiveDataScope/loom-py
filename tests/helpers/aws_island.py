"""The in-memory AWS island: a double for the port loom's AWS clients are built through."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tests.helpers.extras import BOTO3_ISLAND


class FakeSdkError(Exception):
    """The error a :class:`FakeAwsIsland` reports as the SDK's own."""


@dataclass(frozen=True)
class FakeSession:
    """The session a :class:`FakeAwsIsland` opens: it keeps the profile it was opened for."""

    profile_name: str | None


class FakeAwsIsland:
    """Stands in for the AWS island behind :class:`AwsClients`: a new mock client per build.

    Args:
        build_seconds: How long each build takes, to widen a race between threads.
        setup: Called with each client built, to script what it answers.
        serving: Gives the client to hand out for the settings of a build, in place of a mock.
            A served client may be a real one, so the island then reports botocore's errors
            as the SDK's own, as the real island does.

    Attributes:
        sessions: The profile of every session opened.
        built: The service and settings of every client built, in order.
        created: Every client built, in order.
    """

    def __init__(
        self,
        build_seconds: float = 0.0,
        setup: Callable[[MagicMock], None] | None = None,
        serving: Callable[[Any], Any] | None = None,
    ) -> None:
        self._build_seconds = build_seconds
        self._setup = setup
        self._serving = serving
        self.sessions: list[str | None] = []
        self.built: list[tuple[str, Any]] = []
        self.created: list[MagicMock] = []

    def open_session(self, profile: str | None) -> FakeSession:
        self.sessions.append(profile)
        return FakeSession(profile)

    def build_aws_client(self, session: object, service: str, config: Any) -> Any:
        time.sleep(self._build_seconds)
        self.built.append((service, config))
        if self._serving is not None:
            return self._serving(config)
        client = MagicMock(name=f"{service}-client")
        self.created.append(client)
        if self._setup is not None:
            self._setup(client)
        return client

    def sdk_errors(self) -> tuple[type[Exception], ...]:
        if self._serving is None:
            return (FakeSdkError,)
        return (FakeSdkError, BotoCoreError, ClientError)


def fake_aws_island(
    monkeypatch: pytest.MonkeyPatch,
    build_seconds: float = 0.0,
    setup: Callable[[MagicMock], None] | None = None,
    serving: Callable[[Any], Any] | None = None,
) -> FakeAwsIsland:
    """Make every ``aws_island()`` of loom answer a :class:`FakeAwsIsland`."""
    island = FakeAwsIsland(build_seconds, setup, serving)
    monkeypatch.setitem(sys.modules, BOTO3_ISLAND, island)
    return island
