"""Phase A on a real Lambda adapter: an ``http`` function and a ``jobs`` SQS batch (T06).

A use case reaches an MCP server through an ``Mcp()`` marker, or through an
``Agent()`` marker whose agent a scripted model drives; both go through the
connection's one process session. The server is a real streamable-HTTP FastMCP
server on a local port behind a TCP proxy the test controls
(:mod:`tests.integration.ai.freeze.server`). A Lambda freeze is simulated by
moving the wall clock the connection pool reads -- and nothing else -- forward;
the proxy plays the server that closed the frozen function's idle connection.

The ``http`` function is driven with API Gateway events through
:class:`~loom.testing.LambdaTestClient`; the ``jobs`` function with SQS batches.
The handshake era is chosen by the server (see the server module): the client
is loom's, unmodified.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import threading
import time
import urllib.request
from collections import Counter
from collections.abc import AsyncIterator, Callable, Generator, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

import pytest

# fastmcp lives in the optional 'mcp-tests' group; skip before anything that imports it
# (the pool module, the server module), so a lane without the group collects no error.
pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

import httpx2  # noqa: E402
import jwt  # noqa: E402
import msgspec  # noqa: E402
import yaml  # noqa: E402
from pydantic_ai.messages import (  # noqa: E402
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import (  # noqa: E402
    AgentInfo,
    DeltaToolCall,
    DeltaToolCalls,
    FunctionModel,
)

from loom.ai import registry, remote_auth  # noqa: E402
from loom.ai.abc import AgentHandle, McpHandle  # noqa: E402
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, _mcp  # noqa: E402
from loom.ai.engines.pydantic_ai import _transport as _transport  # noqa: E402
from loom.ai.engines.pydantic_ai._transport import EvictableTransport  # noqa: E402
from loom.ai.errors import AgentRunError, AgentRunErrorCode  # noqa: E402
from loom.composition import create_app  # noqa: E402
from loom.core.command import Command  # noqa: E402
from loom.core.identity import Identity  # noqa: E402
from loom.core.identity.wire import encode_identity  # noqa: E402
from loom.core.job.job import Job  # noqa: E402
from loom.core.plugins import entrypoints as entrypoints_module  # noqa: E402
from loom.core.use_case import Agent, Input, Mcp, UseCase  # noqa: E402
from loom.rest.model import RestInterface, RestRoute  # noqa: E402
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHandler  # noqa: E402
from loom.testing import LambdaEvent, LambdaTestClient  # noqa: E402
from tests.integration.ai._entrypoints import fake_entry_points  # noqa: E402
from tests.integration.ai.freeze.server import Era, LiveMcpServer, serving  # noqa: E402

# Not marked 'integration': that marker means a real external service, and this needs
# only loopback sockets, so it runs in the fast lane, which installs 'mcp-tests'.
pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
    pytest.mark.filterwarnings("ignore:app.job_service is not set"),
]

Trigger = Literal["http", "jobs"]
Path_ = Literal["marker", "agent"]

_SERVER = "orders"
_AGENT = "clerk"
_TOOLS = ("echo", "book", "slow")
_MANIFEST = "loom_t06_lambda_manifest"
_TOKEN_KIND = "t06-fetched-token"
_HANDSHAKE = ("server/discover", "initialize", "notifications/initialized", "tools/list")
_SECRET = "t06-lambda-suite-secret-of-at-least-32-bytes"
_AUDIENCE = "loom-t06"
_BEARER = {
    "authorization": "Bearer "
    + jwt.encode(
        {"sub": "tester", "aud": _AUDIENCE, "exp": int(time.time()) + 3600},
        _SECRET,
        algorithm="HS256",
    )
}
_CALLER = encode_identity(Identity(subject="tester"))
_SETTLE_S = 5.0
_GAP_TAKEN = threading.Event()
"""Set by a test once it moved the wall clock while a ``slow`` call is in flight."""
_RAISED: list[AgentRunErrorCode] = []
"""The code of every ``AgentRunError`` a use case or job saw, noted before it propagated."""

_POST_CONNECTS: dict[str, Callable[[int], int]] = {
    "modern": lambda posts: 0,
    "legacy": lambda posts: posts,
}
"""TCP connects *posts* tool calls open after Init, per era (measured, fastmcp 4.1.6 / mcp 2.3.0).

In the handshake era a ``POST`` is answered with an event stream, and the mcp
client closes that connection once it read the response (it closes first, the
server second, at the proxy), so every call opens a connection of its own: there
is no warm connection to keep, and none to go stale. The modern era answers
with plain JSON and keeps the one connection Init opened.
"""

_TRIGGERS = pytest.mark.parametrize("trigger", ["http", "jobs"])
_ERAS = pytest.mark.parametrize("era", ["modern", "legacy"])
_PATHS = pytest.mark.parametrize("path", ["marker", "agent"])


# ---------------------------------------------------------------------------
# The application: each marker, from a use case (http) and from a job (SQS)
# ---------------------------------------------------------------------------


class Say(Command, frozen=True):
    """Which tool to call, and with what."""

    tool: str = "echo"
    text: str = "marker"


def _noted(error: AgentRunError) -> AgentRunError:
    _RAISED.append(error.code)
    return error


class CallTool(UseCase[object, dict[str, Any]]):
    """Calls one tool through an ``Mcp()`` marker: no agent, no model."""

    async def execute(
        self, cmd: Say = Input(), orders: McpHandle = Mcp(_SERVER, include=_TOOLS)
    ) -> dict[str, Any]:
        try:
            return dict(await orders.call_untyped(cmd.tool, {"text": cmd.text}))
        except AgentRunError as error:
            raise _noted(error) from None


class CallTwice(UseCase[object, int]):
    """Calls ``echo`` twice at once through an ``Mcp()`` marker."""

    async def execute(self, orders: McpHandle = Mcp(_SERVER, include=_TOOLS)) -> int:
        calls = (orders.call_untyped("echo", {"text": str(n)}) for n in range(2))
        return len(await asyncio.gather(*calls))


class EchoDuringSlow(UseCase[object, int]):
    """Calls ``echo`` while a ``slow`` call is in flight, once the test moved the clock."""

    async def execute(self, orders: McpHandle = Mcp(_SERVER, include=_TOOLS)) -> int:
        slow = asyncio.ensure_future(orders.call_untyped("slow", {"text": "slow"}))
        while not _GAP_TAKEN.is_set():
            await asyncio.sleep(0.01)
        await orders.call_untyped("echo", {"text": "during"})
        await slow
        return 2


class AskClerk(UseCase[object, str]):
    """Runs the agent, whose model calls the tool the prompt names."""

    async def execute(self, cmd: Say = Input(), clerk: AgentHandle = Agent(_AGENT)) -> str:
        try:
            await clerk.run(cmd.tool)
        except AgentRunError as error:
            raise _noted(error) from None
        return cmd.tool


class ToolsInterface(RestInterface[object]):
    prefix = "/mcp"
    routes = (
        RestRoute(use_case=CallTool, method="POST", path="/call"),
        RestRoute(use_case=CallTwice, method="POST", path="/twice"),
        RestRoute(use_case=EchoDuringSlow, method="POST", path="/inflight"),
    )


class ClerkInterface(RestInterface[object]):
    prefix = "/mcp"
    routes = (RestRoute(use_case=AskClerk, method="POST", path="/ask"),)


class CallToolJob(Job[None]):
    """The ``Mcp()`` marker, from a job of an SQS batch."""

    async def execute(
        self, tool: str, text: str, orders: McpHandle = Mcp(_SERVER, include=_TOOLS)
    ) -> None:
        try:
            await orders.call_untyped(tool, {"text": text})
        except AgentRunError as error:
            raise _noted(error) from None


class CallTwiceJob(Job[None]):
    """Two ``echo`` calls at once, from a job of an SQS batch."""

    async def execute(self, orders: McpHandle = Mcp(_SERVER, include=_TOOLS)) -> None:
        await asyncio.gather(*(orders.call_untyped("echo", {"text": str(n)}) for n in range(2)))


class AskClerkJob(Job[None]):
    """The agent, from a job of an SQS batch."""

    async def execute(self, tool: str, clerk: AgentHandle = Agent(_AGENT)) -> None:
        try:
            await clerk.run(tool)
        except AgentRunError as error:
            raise _noted(error) from None


# ---------------------------------------------------------------------------
# The model, the credential, the clock
# ---------------------------------------------------------------------------


@dataclass
class _Model:
    """Calls the tool the prompt names once, then answers with what came back.

    Attributes:
        requests: Model requests made, over every run.
        retries: ``ModelRetry`` prompts the model was handed.
    """

    requests: int = 0
    retries: int = 0

    def model(self, target: object) -> FunctionModel:
        del target
        return FunctionModel(self._respond, stream_function=self._stream)

    def _next(self, messages: list[ModelMessage], info: AgentInfo) -> tuple[str, str]:
        self.requests += 1
        last = messages[-1].parts
        self.retries += sum(isinstance(part, RetryPromptPart) for part in last)
        returned = [part for part in last if isinstance(part, ToolReturnPart)]
        if returned:
            answer = msgspec.json.encode({"answer": returned[0].model_response_str()})
            return info.output_tools[0].name, answer.decode()
        prompt = next(part for part in messages[0].parts if isinstance(part, UserPromptPart))
        return str(prompt.content), json.dumps({"text": "agent"})

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        name, arguments = self._next(messages, info)
        return ModelResponse(parts=[ToolCallPart(tool_name=name, args=arguments)])

    async def _stream(
        self, messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        name, arguments = self._next(messages, info)
        yield {0: DeltaToolCall(name=name, json_args=arguments, tool_call_id=f"c{self.requests}")}


class FetchedToken(httpx2.Auth):
    """A credential fetched on its first request, then presented on every request.

    Stands in for a token exchange; :attr:`fetches` counts the exchanges.
    """

    fetches = 0

    def __init__(self) -> None:
        self._token: str | None = None

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response]:
        if self._token is None:
            type(self).fetches += 1
            self._token = f"token-{type(self).fetches}"
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


class _TokenEntryPoint:
    """Registers :class:`FetchedToken` as an auth strategy, as a distribution would."""

    name = _TOKEN_KIND
    group = remote_auth.REMOTE_AUTH_ENTRY_POINT_GROUP

    class dist:  # noqa: N801 - mirrors importlib.metadata.EntryPoint.dist
        name = "loom-t06-tests"

    def load(self) -> object:
        return FetchedToken


class _WallClock:
    """Stands in for the pool's ``time`` module: a wall clock a test moves, no ``monotonic``."""

    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


# ---------------------------------------------------------------------------
# The world: a server, a Lambda function over it, and what to count
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Mark:
    log: int
    connects: int
    builds: int
    drops: int
    model: int
    runs: Counter[str]


@dataclass
class _World:
    """A live server, one Lambda function over it, and every counter the tests read.

    Counts are read relative to :meth:`mark`, taken once Init is done.
    """

    server: LiveMcpServer
    handler: LambdaHandler
    model: _Model
    clock: _WallClock
    builds: list[object]
    drops: list[EvictableTransport]
    trigger: Trigger
    _mark: _Mark | None = None
    _ids: Iterator[int] = field(default_factory=lambda: iter(range(1, 1_000)))

    def call(self, tool: str = "echo") -> bool:
        """One invocation calling *tool* through the ``Mcp()`` marker; whether it succeeded."""
        return self._invoke("call", CallToolJob, {"tool": tool, "text": "marker"})

    def ask(self, tool: str = "echo") -> bool:
        """One invocation running the agent, whose model calls *tool*; whether it succeeded."""
        return self._invoke("ask", AskClerkJob, {"tool": tool})

    def by(self, path: Path_, tool: str = "echo") -> bool:
        """One invocation calling *tool* through *path*; whether it succeeded."""
        return self.call(tool) if path == "marker" else self.ask(tool)

    def twice(self) -> bool:
        """One invocation making two concurrent ``echo`` calls; whether it succeeded."""
        return self._invoke("twice", CallTwiceJob, {})

    def echo_during_slow(self) -> bool:
        """One ``http`` invocation calling ``echo`` while its ``slow`` call is in flight."""
        return self._invoke("inflight", CallTwiceJob, {})

    def freeze(self, seconds: float) -> None:
        """Advance the wall clock the pool reads, as a thawed Lambda sees it."""
        self.clock.now += seconds

    def mark(self) -> None:
        """Count from here on, once the legacy era's event stream is open."""
        if self.server.era == "legacy":
            self.settle(streams=1)
        self._mark = _Mark(
            len(self.server.log),
            self.server.connects,
            len(self.builds),
            len(self.drops),
            self.model.requests,
            Counter(self.server.runs),
        )

    def settle(self, *, streams: int) -> None:
        """Wait until the server logged *streams* GET event streams in all (legacy era).

        The client sends a GET while the function's loop runs, in Init or an
        invocation; this only waits for the server's thread to log it.
        """
        deadline = time.monotonic() + _SETTLE_S
        while self.streams() < streams:
            assert time.monotonic() < deadline, f"the server logged {self.streams()} GET streams"
            time.sleep(0.01)

    def streams(self) -> int:
        """GET event streams the client opened in all."""
        return sum(request.verb == "GET" for request in self.server.log)

    @property
    def since(self) -> _Mark:
        assert self._mark is not None
        return self._mark

    def methods(self) -> list[str]:
        """The JSON-RPC methods sent since the mark, in order."""
        return [request.method for request in self.server.log[self.since.log :] if request.method]

    def count(self, *methods: str) -> int:
        return sum(method in methods for method in self.methods())

    def connections(self) -> set[int]:
        """The TCP connections the ``POST`` requests since the mark came on."""
        return {
            request.connection
            for request in self.server.log[self.since.log :]
            if request.verb == "POST"
        }

    def connects(self) -> int:
        return self.server.connects - self.since.connects

    def rebuilds(self) -> int:
        return len(self.builds) - self.since.builds

    def evictions(self) -> int:
        return len(self.drops) - self.since.drops

    def runs(self) -> Counter[str]:
        """How often each tool ran since the mark."""
        return self.server.runs - self.since.runs

    def model_requests(self) -> int:
        return self.model.requests - self.since.model

    def _invoke(self, route: str, job: type[Job[None]], params: dict[str, Any]) -> bool:
        if self.trigger == "http":
            body = {key: value for key, value in params.items() if key in ("tool", "text")}
            client = LambdaTestClient(self.handler)  # type: ignore[arg-type]
            return client.post(f"/mcp/{route}", json=body, headers=_BEARER).status_code == 200
        envelope = {"job": job.__qualname__, "params": params, "payload": {}, "identity": _CALLER}
        event = LambdaEvent.sqs_record(json.dumps(envelope), message_id=str(next(self._ids)))
        return self.handler(event, None) == {"batchItemFailures": []}


def _write_project(tmp_path: Path, url: str, trigger: Trigger, *, agents: bool) -> str:
    """Write the function's configuration and its agent; return the configuration's path."""
    manifest = ModuleType(_MANIFEST)
    if trigger == "http":
        manifest.USE_CASES = [CallTool, CallTwice, EchoDuringSlow, *([AskClerk] if agents else [])]  # type: ignore[attr-defined]
        manifest.INTERFACES = [ToolsInterface, *([ClerkInterface] if agents else [])]  # type: ignore[attr-defined]
    else:
        manifest.JOBS = [CallToolJob, CallTwiceJob, *([AskClerkJob] if agents else [])]  # type: ignore[attr-defined]
    if agents:
        manifest.AGENTS = ["agents/*.yaml"]  # type: ignore[attr-defined]
        _write_agent(tmp_path / "agents")
    sys.modules[_MANIFEST] = manifest
    server = {"url": url, "timeout_ms": 5000, "auth": {"kind": _TOKEN_KIND}}
    config: dict[str, Any] = {
        "app": {
            "name": "t06",
            "code_path": str(tmp_path),
            "discovery": {"mode": "manifest", "manifest": {"module": _MANIFEST}},
        },
        "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        "ai": {
            "engine": "pydantic-ai",
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "mcp_servers": {_SERVER: server},
        },
    }
    if trigger == "http":
        secret = tmp_path / "jwt.key"
        secret.write_text(_SECRET, encoding="utf-8")
        jwt_auth = {"secret_path": str(secret), "algorithms": ["HS256"], "audience": _AUDIENCE}
        config["app"]["rest"] = {"auth": {"jwt": jwt_auth}}
    else:
        config["lambda"] = {"functions": {"worker": {"trigger": {"source": "jobs"}}}}
    path = tmp_path / "app.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(path)


def _write_agent(directory: Path) -> None:
    directory.mkdir()
    spec = {
        "spec_version": 1,
        "name": _AGENT,
        "description": "Calls the order server's tools.",
        "instructions": "Call the tool you are asked for.",
        "output": {
            "kind": "json_schema",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["answer"],
                "properties": {"answer": {"type": "string"}},
            },
        },
        "capabilities": [{"kind": "mcp", "server": _SERVER, "include": list(_TOOLS)}],
    }
    (directory / f"{_AGENT}.yaml").write_text(yaml.safe_dump(spec), encoding="utf-8")


@pytest.fixture(autouse=True)
def _no_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """A proxy on the machine would make the toolset keep fastmcp's stock transport."""
    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    monkeypatch.setattr(urllib.request, "getproxies", urllib.request.getproxies_environment)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """One credential, one manifest and one list of raised codes per test."""
    remote_auth._STRATEGIES._by_endpoint.clear()
    FetchedToken.fetches = 0
    _RAISED.clear()
    _GAP_TAKEN.clear()
    monkeypatch.setattr(
        entrypoints_module,
        "entry_points",
        fake_entry_points(remote_auth.REMOTE_AUTH_ENTRY_POINT_GROUP, (_TokenEntryPoint(),)),
    )
    yield
    remote_auth._STRATEGIES._by_endpoint.clear()
    sys.modules.pop(_MANIFEST, None)


def _counted(monkeypatch: pytest.MonkeyPatch) -> tuple[list[object], list[EvictableTransport]]:
    """Every toolset built and every pool dropped, from here on."""
    builds: list[object] = []
    drops: list[EvictableTransport] = []
    build, drop = _mcp.build_mcp_toolset, EvictableTransport.drop

    def counting_build(*args: Any, **kwargs: Any) -> Any:
        builds.append(build(*args, **kwargs))
        return builds[-1]

    async def counting_drop(self: EvictableTransport) -> None:
        drops.append(self)
        await drop(self)

    monkeypatch.setattr(_mcp, "build_mcp_toolset", counting_build)
    monkeypatch.setattr(EvictableTransport, "drop", counting_drop)
    return builds, drops


World = Callable[..., _World]


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[World]:
    """Builds a server of one era and a Lambda function of one trigger over it, at Init.

    The pool's wall clock is patched before anything is built, so every pool
    reads it; the counters are marked once Init is done.
    """
    with contextlib.ExitStack() as stack:

        def build(era: Era, trigger: Trigger, *, agents: bool = True) -> _World:
            server = stack.enter_context(serving(era))
            clock = _WallClock()
            monkeypatch.setattr(_transport, "time", clock)
            model = _Model()
            provider = PydanticAIEngineProvider(model_resolver=model.model)
            monkeypatch.setattr(registry, "resolve_engine_provider", lambda name: provider)
            builds, drops = _counted(monkeypatch)
            config = _write_project(tmp_path, server.url, trigger, agents=agents)
            function = "worker" if trigger == "jobs" else None
            adapter = LambdaAdapter(function=function)
            handler = create_app(config, adapter=adapter, environment="test")
            stack.callback(handler.close)
            built = _World(server, handler, model, clock, builds, drops, trigger)
            built.mark()
            return built

        yield build


def _warmed(world: World, era: Era, trigger: Trigger) -> _World:
    """A function that served one marker call and one agent run, marked after them."""
    built = world(era, trigger)
    assert built.call()
    assert built.ask()
    built.mark()
    return built


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTheProcessSession:
    @_TRIGGERS
    @_ERAS
    def test_the_era_is_the_one_the_server_chose(
        self, world: World, era: Era, trigger: Trigger
    ) -> None:
        built = world(era, trigger)

        init = [request.method for request in built.server.log if request.method]
        handshake = ["server/discover", "tools/list"]
        if era == "legacy":
            handshake = ["server/discover", "initialize", "notifications/initialized", "tools/list"]
        assert init == handshake

    @_TRIGGERS
    @_ERAS
    def test_a_use_case_with_only_an_mcp_marker_runs_over_it(
        self, world: World, era: Era, trigger: Trigger
    ) -> None:
        built = world(era, trigger, agents=False)

        assert [built.call() for _ in range(3)] == [True] * 3

        assert built.methods() == ["tools/call"] * 3
        assert built.runs()["echo"] == 3
        assert len(built.builds) == 1  # the toolset Init built, never another
        assert built.rebuilds() == 0
        assert built.model.requests == 0
        assert built.connects() == _POST_CONNECTS[era](3)


class TestWarmInvocations:
    @_TRIGGERS
    @_ERAS
    def test_send_only_their_tool_calls(self, world: World, era: Era, trigger: Trigger) -> None:
        """Modern era: over the one connection Init opened. Legacy era: see ``_POST_CONNECTS``."""
        built = world(era, trigger)

        results = []
        for n in range(20):
            built.freeze(1.0)  # a warm gap, under the 5 s expiry
            results.append(built.call() if n % 2 else built.ask())

        assert results == [True] * 20
        assert built.methods() == ["tools/call"] * 20
        assert built.runs()["echo"] == 20
        assert built.connects() == _POST_CONNECTS[era](20)
        assert len(built.connections()) == max(1, _POST_CONNECTS[era](20))
        assert built.evictions() == 0
        assert built.rebuilds() == 0
        assert FetchedToken.fetches == 1
        assert {request.authorization for request in built.server.log} == {"Bearer token-1"}


class TestAThawWithTheSessionStillValid:
    @_PATHS
    @_TRIGGERS
    @_ERAS
    def test_the_first_call_evicts_once_and_keeps_the_session(
        self, world: World, era: Era, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, era, trigger)
        built.server.stale()
        built.freeze(6.0)

        assert built.by(path)

        assert built.evictions() == 1
        assert built.methods() == ["tools/call"]
        assert built.count(*_HANDSHAKE) == 0
        assert built.rebuilds() == 0
        assert built.runs()["echo"] == 1
        assert built.connects() == 1
        assert FetchedToken.fetches == 1
        assert _RAISED == []

    @_TRIGGERS
    def test_two_concurrent_calls_evict_once(self, world: World, trigger: Trigger) -> None:
        """The gap is stamped at check time, so the second call sees none."""
        built = _warmed(world, "modern", trigger)
        built.server.stale()
        built.freeze(6.0)

        assert built.twice()

        assert built.evictions() == 1
        assert built.rebuilds() == 0
        assert built.methods() == ["tools/call", "tools/call"]
        assert built.runs()["echo"] == 2
        assert _RAISED == []

    def test_a_call_in_flight_across_the_gap_is_not_cut(self, world: World) -> None:
        """A call made after the gap while another is in flight drops nothing; both answer."""
        built = _warmed(world, "modern", "http")

        with ThreadPoolExecutor(1) as invocation:
            served = invocation.submit(built.echo_during_slow)
            try:
                assert built.server.started.wait(_SETTLE_S)
                built.freeze(6.0)
                _GAP_TAKEN.set()
                deadline = time.monotonic() + _SETTLE_S
                while built.runs()["echo"] < 1:
                    assert time.monotonic() < deadline, "the call after the gap never ran"
                    time.sleep(0.01)
            finally:
                # Whatever failed above, let the worker finish so the executor can exit.
                _GAP_TAKEN.set()
                built.server.release.set()
            assert served.result(_SETTLE_S)

        assert built.evictions() == 0
        assert built.runs() == Counter({"slow": 1, "echo": 1})
        assert (built.rebuilds(), _RAISED) == (0, [])

    def test_the_legacy_event_stream_cut_by_the_eviction_reopens_off_the_call_path(
        self, world: World
    ) -> None:
        """The stream's reconnect waits 1 s of loop time, which runs only within an invocation."""
        built = _warmed(world, "legacy", "http")
        built.server.stale()
        built.freeze(6.0)
        assert built.call()
        assert built.streams() == 1

        time.sleep(1.1)  # the mcp client's reconnection delay, DEFAULT_RECONNECTION_DELAY_MS
        built.freeze(1.0)
        assert built.call()

        built.settle(streams=2)
        assert built.methods() == ["tools/call", "tools/call"]
        assert (built.evictions(), built.rebuilds()) == (1, 0)

    @_ERAS
    def test_without_the_gap_a_stale_connection_is_reused_only_in_the_modern_era(
        self, world: World, era: Era
    ) -> None:
        """The control: the proxy's stale connection bites when nothing evicts it.

        Modern era: the call is written to the dead connection, so it may have
        run (``UNSAFE``) although the server never saw it (R18's residual). The
        handshake era opens a connection per ``POST`` anyway (``_POST_CONNECTS``),
        so there is no stale connection for a call to reuse.
        """
        built = _warmed(world, era, "http")
        built.server.stale()
        built.freeze(1.0)

        served = built.call()

        assert built.evictions() == 0
        if era == "legacy":
            assert served
            assert (built.runs()["echo"], built.rebuilds()) == (1, 0)
            return
        assert not served
        assert _RAISED == [AgentRunErrorCode.MCP_CALL_INTERRUPTED]
        assert built.runs()["echo"] == 0
        assert built.call()
        assert built.rebuilds() == 1


class TestAFreezeWhereTheServerForgotTheSession:
    @_PATHS
    @_TRIGGERS
    def test_the_first_call_succeeds_through_one_rebuild(
        self, world: World, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, "legacy", trigger)
        built.server.stale()
        built.server.forget()
        built.freeze(6.0)

        assert built.by(path)

        assert built.evictions() == 1
        assert built.rebuilds() == 1
        assert built.methods() == [
            "tools/call",  # answered 404 Session not found: nothing ran
            "server/discover",
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call",
        ]
        assert built.runs()["echo"] == 1
        assert FetchedToken.fetches == 1
        assert _RAISED == []


class TestACallCutMidResponse:
    @_PATHS
    @_TRIGGERS
    @_ERAS
    def test_raises_once_never_runs_again_and_the_next_call_works(
        self, world: World, era: Era, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, era, trigger)
        built.server.cut_next_call()

        assert not built.by(path, "book")

        assert _RAISED == [AgentRunErrorCode.MCP_CALL_INTERRUPTED]
        assert built.runs()["book"] == 1
        assert built.methods() == ["tools/call"]
        if path == "agent":
            assert (built.model_requests(), built.model.retries) == (1, 0)
        assert built.rebuilds() == 0

        assert built.by(path)

        assert built.runs() == {"book": 1, "echo": 1}
        assert built.rebuilds() == 1
        assert _RAISED == [AgentRunErrorCode.MCP_CALL_INTERRUPTED]


class TestTwoConcurrentCallsAfterAnExpiry:
    @_TRIGGERS
    def test_rebuild_once(self, world: World, trigger: Trigger) -> None:
        built = _warmed(world, "legacy", trigger)
        built.server.forget()

        assert built.twice()

        assert built.rebuilds() == 1
        assert built.count("initialize") == 1
        assert built.count("tools/call") == 4  # two answered 404, two served
        assert built.runs()["echo"] == 2
        assert _RAISED == []
