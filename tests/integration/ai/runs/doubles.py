"""Doubles shared by the agent-run execution tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from loom.ai.abc import AgentEvent, HealthStatus
from loom.core.execution import current_execution, execution_identity
from loom.core.identity import Identity, reset_identity, set_identity
from tests.helpers.ai import ScriptedEngine

AGENT = "acme-analyst"
SERVER = "acme-crm"
TOOL = "find_orders"
PREFIX = "/agents"

ALICE = Identity(subject="acme-alice", roles=("analyst",), mechanism="test")
BOB = Identity(subject="acme-bob", roles=("analyst",), mechanism="test")


class AcmeActingEngine(ScriptedEngine):
    """A scripted engine recording who acts while each event is produced.

    Args:
        during: Awaited before each event, inside the run, when given.
        script: Events to replay, terminal event last.
        delays_ms: Delay before each event.
    """

    def __init__(
        self,
        *,
        during: Callable[[], Awaitable[object]] | None = None,
        script: Sequence[AgentEvent] | None = None,
        delays_ms: Sequence[int] = (),
    ) -> None:
        super().__init__(script=script, delays_ms=delays_ms)
        self.during = during
        self.acting: list[Identity] = []
        self.in_execution: list[bool] = []

    async def _iterate(self) -> AsyncGenerator[AgentEvent, None]:
        async for event in super()._iterate():
            self.acting.append(execution_identity())
            self.in_execution.append(current_execution() is not None)
            if self.during is not None:
                await self.during()
            yield event


@dataclass(frozen=True, slots=True)
class AcmeNativeAccess:
    """What the native engine double hands back: its guard."""

    guard: Callable[[], AbstractAsyncContextManager[None]]


class AcmeNativeEngine:
    """An engine publishing ``native()`` and nothing else."""

    def native(
        self,
        *,
        identity: Identity,
        state: object | None = None,
        guard: Callable[[], AbstractAsyncContextManager[None]],
    ) -> AcmeNativeAccess:
        del identity, state
        return AcmeNativeAccess(guard=guard)

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


class AcmeAuthenticator:
    """Authenticates every caller as Alice."""

    name = "acme"
    provides_roles = True

    async def authenticate(self, credentials: object) -> Identity | None:
        del credentials
        return ALICE


class AcmeIdentityMiddleware:
    """Publishes Alice for one request, as the authentication middleware does."""

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        token = set_identity(ALICE)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


async def abandon_stream_after_first_frame(app: FastAPI) -> None:
    """POST one ``/stream`` request over raw ASGI and disconnect once a frame reached the wire."""
    frame_sent = asyncio.Event()
    pending: list[dict[str, Any]] = [
        {"type": "http.request", "body": b'{"prompt": "p"}', "more_body": False}
    ]

    async def receive() -> dict[str, Any]:
        if pending:
            return pending.pop(0)
        await frame_sent.wait()
        return {"type": "http.disconnect"}

    async def send(message: Any) -> None:
        if message["type"] == "http.response.body" and message.get("body"):
            frame_sent.set()

    path = f"{PREFIX}/{AGENT}/stream"
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/json")],
        "client": ("testclient", 50000),
        "server": ("agents.test", 80),
    }
    await app(scope, receive, send)
