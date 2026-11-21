"""The in-memory AWS island: a double for the port loom's AWS clients are built through."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from tests.helpers.extras import BOTO3_ISLAND


class FakeSdkError(Exception):
    """The error a :class:`FakeAwsIsland` reports as the SDK's own."""


@dataclass(frozen=True)
class FakeAwsSession:
    """The session a :class:`FakeAwsIsland` opens: it keeps the profile it was opened for."""

    profile_name: str | None


class FakeAwsIsland:
    """Stands in for the AWS island behind :class:`AwsClients`.

    Every build hands out *client*, or a new ``MagicMock`` when there is none.

    Args:
        client: The one client every build hands out.
        setup: Called with each mock client built, to script what it answers.
        build_seconds: How long each build takes, to widen a race between threads.
        make_client: Called with the service and settings of each build to make
            its client, for a test that needs one fake per build.

    Attributes:
        sessions: The profile of every session opened.
        built: The service and settings of every client built, in order.
        created: Every mock client built, in order.
    """

    def __init__(
        self,
        client: Any = None,
        setup: Callable[[MagicMock], None] | None = None,
        build_seconds: float = 0.0,
        make_client: Callable[[str, Any], Any] | None = None,
    ) -> None:
        self._client = client
        self._setup = setup
        self._make_client = make_client
        self._build_seconds = build_seconds
        self.sessions: list[str | None] = []
        self.built: list[tuple[str, Any]] = []
        self.created: list[Any] = []

    @property
    def regions_built(self) -> list[tuple[str, str | None]]:
        """The service and region of every client built, in order."""
        return [(service, config.region) for service, config in self.built]

    def created_by_max_attempts(self) -> dict[int, Any]:
        """The client built for each ``max_attempts`` of the settings it was built with."""
        return {
            config.max_attempts: client
            for (_, config), client in zip(self.built, self.created, strict=True)
        }

    def install(self, monkeypatch: pytest.MonkeyPatch) -> FakeAwsIsland:
        """Make every ``aws_island()`` of loom answer this island, and return it."""
        monkeypatch.setitem(sys.modules, BOTO3_ISLAND, self)
        return self

    def open_session(self, profile: str | None) -> FakeAwsSession:
        self.sessions.append(profile)
        return FakeAwsSession(profile)

    def build_aws_client(self, session: object, service: str, config: Any) -> Any:
        time.sleep(self._build_seconds)
        self.built.append((service, config))
        return self._new_client(service, config)

    def sdk_errors(self) -> tuple[type[Exception], ...]:
        return (FakeSdkError,)

    def _new_client(self, service: str, config: Any) -> Any:
        if self._client is not None:
            return self._client
        if self._make_client is not None:
            made = self._make_client(service, config)
            self.created.append(made)
            return made
        client = MagicMock(name=f"{service}-client")
        self.created.append(client)
        if self._setup is not None:
            self._setup(client)
        return client
