"""Inbound A2A surface: card, JSON-RPC methods and streaming parity (T135, T136).

Drives ``bind_a2a_endpoints`` over a live :class:`AgentRuntime` backed by
scripted engines, asserting ``specs/001-ai-agent-layer/contracts/a2a.md``:
the published card validates against the A2A SDK, the streamed events are the
same union the HTTP surface projects (FR-039a), the capability wiring never
reaches the wire (FR-030a), unsupported methods answer a named JSON-RPC error,
and only the card is anonymous (FR-041b).

The application is wired with the real
:class:`~loom.rest.auth.middleware.AuthenticationMiddleware`, so the exclusion
``bind_a2a_endpoints`` registers is exercised end to end instead of simulated.
Nothing here touches the network: engines replay a script and every request
goes through an in-process ASGI transport.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
import pytest
from a2a.compat.v0_3.types import AgentCard
from fastapi import FastAPI

from loom.ai.a2a.card import card_path
from loom.ai.a2a.server import bind_a2a_endpoints
from loom.ai.abc import (
    AgentEvent,
    Conversation,
    DepsFactory,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler._plan import AgentPlan
from loom.ai.config import A2AConfig, AgentEndpointConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunErrorCode
from loom.ai.fastapi.endpoints import bind_agent_endpoints
from loom.ai.runtime import AgentRuntime
from loom.core.config.errors import ConfigError
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model import loom_type
from loom.core.model.redaction import Dsn
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.middleware import AuthenticationMiddleware
from tests.integration.ai.conftest import (
    DEFAULT_OUTPUT,
    DEFAULT_USAGE,
    OUTPUT_TWINS,
    ConversationRecorder,
    CountingEngineProvider,
    RecordingDepsFactory,
    RecordingObserver,
    ScriptedEngine,
    StubDepsFactory,
    conversational_plan,
    error_script,
    make_ai_config,
    make_endpoint,
    make_plan,
    make_state_shape,
    plan_with_output,
)

_AGENT = "analyst"
_PREFIX = "/a2a"
_AGENTS_PREFIX = "/agents"
_BASE_URL = "https://api.example.com"
_TOKEN = "let-me-in"


# Distinctive strings the redaction assertions look for: none of them may ever
# reach an external caller (FR-030a).
_SECRET_TOOL = "reporting_warehouse_query"
_SECRET_ARGUMENT = "SELECT margin FROM internal_pricing"
_SECRET_SUMMARY = "17 rows from internal_pricing"

# Failure texts an external caller must never see: the first is the shape
# ``AgentRunError`` carries (capability key plus budget), the second the shape
# an unhandled exception carries (file path and DSN).
_FAILING_CODE = AgentRunErrorCode.TOOL_TIMEOUT
_RUN_CANARY = "tool 'usecase_pricing.recalculate' exceeded tool_timeout_ms (5000)"
_CATCH_ALL_CANARY = "/srv/loom/secrets.yaml: clickhouse://root:hunter2@analytics.internal"


class ExplodingEngine:
    """Engine double whose stream fails with an exception nothing anticipates.

    Its message carries the shapes an unhandled exception typically leaks, so
    the catch-all's redaction is observable end to end rather than asserted on
    a stub error string.
    """

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> Any:
        """Return a stream whose entry raises before any event exists."""
        del prompt, identity, conversation, state
        return _FailingStream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> object:
        """Fail the same way the stream does; the runtime drives the stream."""
        del prompt, identity, conversation, state
        raise RuntimeError(_CATCH_ALL_CANARY)

    async def health(self) -> HealthStatus:
        """Report a healthy engine: the failure is per run, not per probe."""
        return HealthStatus(status="ok")


class _FailingStream:
    """Engine session whose entry raises, like a broken provider connection."""

    async def __aenter__(self) -> AsyncIterator[AgentEvent]:
        raise RuntimeError(_CATCH_ALL_CANARY)

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class HeaderAuthenticator:
    """Authenticator accepting exactly one bearer token, refusing everything else."""

    name = "jwt"
    provides_roles = True

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        """Return the fixed caller when the expected bearer token is present."""
        if credentials.header("authorization") != f"Bearer {_TOKEN}":
            return None
        return Identity(subject="stub-user", roles=("analyst",), mechanism=self.name)


def tool_script() -> tuple[AgentEvent, ...]:
    """Return a run exercising every member of the event union."""
    return (
        TextDeltaEvent(text="Demand rose "),
        ToolCallEvent(tool=_SECRET_TOOL, call_id="call-1", arguments={"query": _SECRET_ARGUMENT}),
        ToolResultEvent(call_id="call-1", ok=True, summary=_SECRET_SUMMARY),
        FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
    )


@asynccontextmanager
async def _serving(
    *,
    deps: DepsFactory,
    container: LoomContainer,
    plans: Sequence[AgentPlan] | None = None,
    engines: Mapping[str, ScriptedEngine] | None = None,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    a2a: A2AConfig | None = None,
    mounted_exclusions: Sequence[str] = (),
    declared_exclusions: Sequence[str] = (),
    max_prompt_bytes: int = 65536,
    with_http_endpoints: bool = False,
    observability_runtime: ObservabilityRuntime | None = None,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    """Serve an entered runtime behind the real authentication middleware.

    ``mounted_exclusions`` are the exclusions the deployment really mounts on
    the middleware; ``declared_exclusions`` are the ones the caller repeats in
    the ``exclude_paths`` argument of ``bind_a2a_endpoints``. They are separate
    on purpose: the guard must fail on the mounted list alone.
    """
    plans = list(plans if plans is not None else (make_plan(_AGENT),))
    config = make_ai_config(
        endpoints=dict(endpoints if endpoints is not None else {_AGENT: make_endpoint()}),
        a2a=a2a if a2a is not None else A2AConfig(base_url=Dsn(_BASE_URL), expose=(_AGENT,)),
        max_prompt_bytes=max_prompt_bytes,
    )
    runtime = AgentRuntime(
        plans=plans,
        config=config,
        engine_provider=CountingEngineProvider(engines=engines),  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )
    authenticator = HeaderAuthenticator()
    async with runtime:
        app = FastAPI()
        app.add_middleware(
            AuthenticationMiddleware,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(mounted_exclusions),
        )
        if with_http_endpoints:
            bind_agent_endpoints(
                app,
                runtime=runtime,
                config=config,
                authenticator=authenticator,  # type: ignore[arg-type]
                prefix=_AGENTS_PREFIX,
            )
        bind_a2a_endpoints(
            app,
            runtime=runtime,
            config=config,
            plans=plans,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(declared_exclusions),
            observability_runtime=observability_runtime,
            prefix=_PREFIX,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield app, client


def _runtime(
    *,
    deps: DepsFactory,
    container: LoomContainer,
    config: Any,
    plans: Sequence[AgentPlan],
) -> AgentRuntime:
    """Build a runtime over scripted engines for one configuration."""
    return AgentRuntime(
        plans=list(plans),
        config=config,
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


async def _bind_a2a(
    *,
    deps: DepsFactory,
    container: LoomContainer,
    mounted_exclusions: Sequence[str] = (),
    agents_prefix: str,
) -> None:
    """Bind the A2A surface of one agent under a chosen HTTP agents prefix."""
    config = make_ai_config(
        endpoints={_AGENT: make_endpoint()},
        a2a=A2AConfig(base_url=Dsn(_BASE_URL), expose=(_AGENT,)),
    )
    plans = [make_plan(_AGENT)]
    authenticator = HeaderAuthenticator()
    runtime = _runtime(deps=deps, container=container, config=config, plans=plans)
    async with runtime:
        app = FastAPI()
        app.add_middleware(
            AuthenticationMiddleware,
            authenticator=authenticator,  # type: ignore[arg-type]
            exclude_paths=tuple(mounted_exclusions),
        )
        bind_a2a_endpoints(
            app,
            runtime=runtime,
            config=config,
            plans=plans,
            authenticator=authenticator,  # type: ignore[arg-type]
            prefix=_PREFIX,
            agents_prefix=agents_prefix,
        )


@asynccontextmanager
async def _serving_anonymously(
    *,
    deps: DepsFactory,
    container: LoomContainer,
    endpoints: Mapping[str, AgentEndpointConfig],
) -> AsyncIterator[httpx.AsyncClient]:
    """Serve the A2A surface of an application configuring no authentication.

    No authenticator means no middleware either, which is the only wiring where
    an anonymous caller reaches the endpoint at all: with one mounted, the
    middleware refuses the request before any agent code runs.
    """
    config = make_ai_config(
        endpoints=dict(endpoints),
        a2a=A2AConfig(base_url=Dsn(_BASE_URL), expose=(_AGENT,)),
    )
    plans = [make_plan(_AGENT)]
    runtime = _runtime(deps=deps, container=container, config=config, plans=plans)
    async with runtime:
        app = FastAPI()
        bind_a2a_endpoints(app, runtime=runtime, config=config, plans=plans, prefix=_PREFIX)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield client


def _rpc(
    method: str,
    *,
    prompt: str = "hola",
    request_id: int | str = 7,
    context_id: str | int | None = None,
    task_id: str | int | None = None,
) -> dict[str, Any]:
    """Build one JSON-RPC request body carrying a single text part.

    ``context_id`` and ``task_id`` are added to the message when not ``None``,
    so a test can send an empty string, a wrong type or nothing at all.
    """
    message: dict[str, Any] = {
        "role": "user",
        "kind": "message",
        "messageId": "m-1",
        "parts": [{"kind": "text", "text": prompt}],
    }
    if context_id is not None:
        message["contextId"] = context_id
    if task_id is not None:
        message["taskId"] = task_id
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": {"message": message}}


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {_TOKEN}"}


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


def _stream_results(payload: str) -> list[dict[str, Any]]:
    """Return the ``result`` object of every JSON-RPC frame in an SSE payload."""
    return [
        json.loads(line[len("data: ") :])["result"]
        for line in payload.splitlines()
        if line.startswith("data: ")
    ]


def _sse_names(payload: str) -> list[str]:
    """Return the event names of an HTTP SSE payload, ignoring comment frames."""
    return [line[len("event: ") :] for line in payload.splitlines() if line.startswith("event: ")]


class TestCard:
    """The published card is a valid A2A card and says nothing about the wiring."""

    async def test_validates_against_the_sdk_when_it_publishes_the_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """T135: the served card validates against the A2A SDK's own model.

        ``a2a.compat.v0_3.types.AgentCard`` is the pydantic model of the SDK's
        JSON representation, validating the exact camelCase document served on
        the wire.  ``a2a.utils.parse_agent_card`` is deliberately not used: it
        goes through protobuf and rewrites legacy 0.3 fields, so a card could
        "pass" only because the parser edited it first.
        """
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        card = AgentCard.model_validate(response.json())
        assert card.name == _AGENT
        assert card.url == f"{_BASE_URL}{_PREFIX}/{_AGENT}"
        assert card.capabilities.streaming is True

    async def test_does_not_publish_the_wiring_when_it_publishes_the_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Instructions and capability wiring never appear in the card."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        assert "answer" not in response.text
        assert "fake-model" not in response.text

    async def test_serves_the_card_without_credentials_when_authentication_is_enabled(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The card path is the sole authentication exclusion (FR-041b)."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.get(card_path(_AGENT, prefix=_PREFIX))

        assert response.status_code == 200

    async def test_responds_401_when_invoked_without_credentials(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The invocation path is never excluded, whatever the card allows."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"))

        assert response.status_code == 401


class TestOptionality:
    """With no ``ai.a2a`` section nothing at all is published (T149, FR-041)."""

    async def test_mounts_no_routes_when_there_is_no_a2a_section(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """No section means no card route and no invocation route."""
        config = make_ai_config(endpoints={_AGENT: make_endpoint()})
        plans = [make_plan(_AGENT)]
        runtime = AgentRuntime(
            plans=plans,
            config=config,
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )

        async with runtime:
            app = FastAPI()
            bind_a2a_endpoints(app, runtime=runtime, config=config, plans=plans)

        assert not [path for path in _route_paths(app) if path.startswith(_PREFIX)]

    async def test_does_not_publish_an_agent_without_a_compiled_plan(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A name in ``expose`` with no compiled agent is not published."""
        a2a = A2AConfig(base_url=Dsn(_BASE_URL), expose=(_AGENT, "ghost"))
        async with _serving(deps=deps, container=container, a2a=a2a) as (app, _client):
            paths = _route_paths(app)

        assert f"{_PREFIX}/ghost" not in paths
        assert f"{_PREFIX}/{_AGENT}" in paths


class TestStateSurfaceRefusal:
    """A stateful artifact and A2A exposure are refused together (T306, FR-014, AC-012)."""

    async def test_fails_to_start_when_a_stateful_artifact_is_exposed_over_a2a(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Both halves of the conflict are named: the artifact and the surface."""
        plans = [make_plan(_AGENT, state=make_state_shape())]
        with pytest.raises(AgentCompilationError) as raised:
            async with _serving(deps=deps, container=container, plans=plans) as (
                _app,
                _client,
            ):
                pass  # pragma: no cover - binding fails before the body runs

        issue = raised.value.issues[0]
        assert issue.code is AgentErrorCode.STATE_SURFACE_UNSUPPORTED
        assert _AGENT in issue.message
        assert "a2a" in issue.message

    async def test_starts_when_a_stateful_artifact_is_not_exposed_over_a2a(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The same declared state boots when ``ai.a2a.expose`` does not name it."""
        plans = [make_plan(_AGENT, state=make_state_shape())]
        a2a = A2AConfig(base_url=Dsn(_BASE_URL), expose=("other",))
        async with _serving(deps=deps, container=container, plans=plans, a2a=a2a) as (
            app,
            _client,
        ):
            paths = _route_paths(app)

        assert f"{_PREFIX}/{_AGENT}" not in paths

    async def test_starts_when_a_stateless_artifact_is_exposed_over_a2a(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An artifact declaring no state is published over A2A normally."""
        plans = [make_plan(_AGENT)]
        async with _serving(deps=deps, container=container, plans=plans) as (app, _client):
            paths = _route_paths(app)

        assert f"{_PREFIX}/{_AGENT}" in paths


class TestMethods:
    """``message/send`` runs the agent; every other method answers explicitly."""

    async def test_returns_a_terminal_task_when_it_sends_a_message(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``message/send`` answers a task already in a terminal state."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        body = response.json()
        assert body["id"] == 7
        assert body["result"]["status"]["state"] == "completed"
        assert body["result"]["artifacts"][0]["parts"][0]["data"] == DEFAULT_OUTPUT

    @pytest.mark.parametrize(
        "method",
        [
            "tasks/get",
            "tasks/list",
            "tasks/cancel",
            "tasks/resubscribe",
            "tasks/pushNotificationConfig/set",
        ],
    )
    async def test_responds_an_explicit_error_when_the_method_is_unsupported(
        self, deps: StubDepsFactory, container: LoomContainer, method: str
    ) -> None:
        """Unsupported methods are HTTP 200 with a ``-32004`` naming the method."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc(method), headers=_auth())

        body = response.json()
        assert response.status_code == 200
        assert body["error"]["code"] == -32004
        assert body["error"]["data"]["method"] == method
        assert body["id"] == 7

    async def test_responds_method_not_found_when_the_method_is_unknown(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An unknown method is ``-32601``, echoing the request id."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("agent/teleport"), headers=_auth()
            )

        assert response.json()["error"]["code"] == -32601

    async def test_responds_parse_error_when_the_body_is_not_json(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An unreadable body is ``-32700``."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", content=b"{not json", headers=_auth()
            )

        assert response.json()["error"]["code"] == -32700

    async def test_responds_invalid_request_when_the_method_is_missing(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A JSON body that is not a JSON-RPC request is ``-32600``."""
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json={"jsonrpc": "2.0", "id": 3}, headers=_auth()
            )

        body = response.json()
        assert body["error"]["code"] == -32600
        assert body["id"] == 3

    async def test_responds_invalid_params_when_there_is_no_text_part(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A message with no text part is ``-32602``."""
        request = _rpc("message/send")
        request["params"]["message"]["parts"] = [{"kind": "data", "data": {"a": 1}}]
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=request, headers=_auth())

        assert response.json()["error"]["code"] == -32602


def _output_artifact_data(results: list[dict[str, Any]]) -> object:
    """Return the ``data`` part of the single ``output`` artifact update."""
    for result in results:
        artifact = result.get("artifact")
        if isinstance(artifact, dict) and artifact.get("artifactId") == "output":
            return artifact["parts"][0]["data"]
    raise AssertionError("no output artifact-update in the stream")


@pytest.mark.parametrize(("library", "cls", "answer"), OUTPUT_TWINS)
class TestOutputProjection:
    """Both A2A paths carry the answer as builtins (SC-001)."""

    async def test_send_message_body_carries_the_hook_payload(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        library: str,
        cls: type[Any],
        answer: object,
    ) -> None:
        """``message/send``'s data part is a JSON object equal to the hook payload."""
        del library
        plan = plan_with_output(cls, name=_AGENT)
        engines = {_AGENT: ScriptedEngine(script=(FinalEvent(output=answer, usage=DEFAULT_USAGE),))}
        async with _serving(deps=deps, container=container, plans=(plan,), engines=engines) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        body = response.json()
        assert body["result"]["artifacts"][0]["parts"][0]["data"] == loom_type(cls).to_builtins(
            answer
        )

    async def test_stream_output_artifact_carries_the_hook_payload(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        library: str,
        cls: type[Any],
        answer: object,
    ) -> None:
        """``message/stream``'s output artifact is a JSON object equal to the hook payload."""
        del library
        plan = plan_with_output(cls, name=_AGENT)
        engines = {_AGENT: ScriptedEngine(script=(FinalEvent(output=answer, usage=DEFAULT_USAGE),))}
        async with _serving(deps=deps, container=container, plans=(plan,), engines=engines) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        results = _stream_results(response.text)
        assert _output_artifact_data(results) == loom_type(cls).to_builtins(answer)


class TestStreaming:
    """``message/stream`` projects the same union the HTTP surface projects."""

    async def test_emits_the_initial_task_and_the_projected_events(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The stream opens with the task and closes on a final status update."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        results = _stream_results(response.text)
        assert [result["kind"] for result in results] == [
            "task",
            "artifact-update",
            "status-update",
            "status-update",
            "artifact-update",
            "status-update",
        ]
        assert results[-1]["final"] is True

    async def test_does_not_publish_the_capability_when_the_run_uses_a_tool(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Neither the capability key, its arguments nor the summary reach the wire."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        assert _SECRET_TOOL not in response.text
        assert _SECRET_ARGUMENT not in response.text
        assert _SECRET_SUMMARY not in response.text
        assert "call-1" not in response.text

    async def test_publishes_only_the_opaque_ordinal_when_the_run_uses_a_tool(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A tool call publishes ``step n/m`` and nothing else."""
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        working = _stream_results(response.text)[2]
        assert working["status"]["message"]["parts"] == [{"kind": "text", "text": "step 1/8"}]

    async def test_projects_the_same_events_as_the_http_surface(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """T136: both surfaces are projections of one union (FR-039a).

        The same scripted run is served over ``/agents/{name}/stream`` and over
        ``message/stream``; the events correspond one for one, with the single
        documented difference that ``final`` projects to two A2A events.
        """
        engines = {_AGENT: ScriptedEngine(script=tool_script())}
        async with _serving(
            deps=deps, container=container, engines=engines, with_http_endpoints=True
        ) as (_app, client):
            http = await client.post(
                f"{_AGENTS_PREFIX}/{_AGENT}/stream", json={"prompt": "hola"}, headers=_auth()
            )
            a2a = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/stream"), headers=_auth()
            )

        # The leading frame is the A2A task envelope, which has no HTTP twin.
        projected = [result["kind"] for result in _stream_results(a2a.text)[1:]]
        assert _sse_names(http.text) == ["text_delta", "tool_call", "tool_result", "final"]
        assert projected == [
            "artifact-update",
            "status-update",
            "status-update",
            "artifact-update",
            "status-update",
        ]


class TestUsageOnTheAgentSpan:
    """A2A publishes what a run spent, exactly as the HTTP surface does.

    Both surfaces close a ``Scope.AGENT`` span named ``agent_run``, so an
    operator querying the cost across agent spans must not get an answer that
    depends on which surface the caller happened to use.
    """

    @staticmethod
    async def _closing_meta(
        *, deps: StubDepsFactory, container: LoomContainer
    ) -> Mapping[str, object]:
        recorder = RecordingObserver()
        async with _serving(
            deps=deps,
            container=container,
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )
            assert response.status_code == 200
        closing = next(
            event
            for event in recorder.events
            if event.scope is Scope.AGENT and event.kind is not EventKind.START
        )
        return closing.meta

    async def test_publishes_the_run_cost_when_it_sends_a_message(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``message/send`` closes its span with the counters of the run."""
        meta = await self._closing_meta(deps=deps, container=container)

        assert meta["gen_ai.usage.requests"] == DEFAULT_USAGE.requests
        assert meta["gen_ai.usage.tool_calls"] == DEFAULT_USAGE.tool_calls
        assert meta["gen_ai.usage.cost"] == pytest.approx(0.0021)
        assert meta["gen_ai.usage.cost_known"] is True


class TestFailureRedaction:
    """A failed run publishes its stable code and nothing else (T151 B1, B2)."""

    async def test_does_not_return_the_failure_message_when_the_run_fails(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``message/send`` never echoes ``AgentRunError.message``.

        The failure text names the capability key and its budget — the very
        wiring the card and the stream redact — so publishing it on the other
        method of the same endpoint would be a leak beside a guarantee.
        """
        engines = {_AGENT: ScriptedEngine(script=error_script(_FAILING_CODE, _RUN_CANARY))}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        assert _RUN_CANARY not in response.text
        assert "usecase_pricing" not in response.text

    async def test_returns_the_stable_code_when_the_run_fails(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The caller still gets the code, plus a fixed catalogue detail."""
        engines = {_AGENT: ScriptedEngine(script=error_script(_FAILING_CODE, _RUN_CANARY))}
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        error = response.json()["error"]
        assert error["code"] == -32603
        assert error["data"] == {
            "code": str(_FAILING_CODE),
            "detail": "a capability call exceeded its time limit",
        }

    async def test_does_not_return_the_exception_text_when_the_failure_is_unexpected(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The catch-all answers a fixed message, like the HTTP surface does.

        An unanticipated exception carries file paths, DSNs and credential
        references in its text; none of it may reach an external caller.
        """
        engines = cast("Mapping[str, ScriptedEngine]", {_AGENT: ExplodingEngine()})
        async with _serving(deps=deps, container=container, engines=engines) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"), headers=_auth()
            )

        assert _CATCH_ALL_CANARY not in response.text
        assert response.json()["error"]["data"]["detail"] == "An unexpected error occurred"


class TestConversations:
    """``contextId`` is the ``conversation_id``, on both methods (008 AC1-AC5, AC9)."""

    @staticmethod
    def _agent_spans(recorder: RecordingObserver) -> list[object]:
        return [event for event in recorder.events if event.scope is Scope.AGENT]

    async def test_delivers_the_context_id_to_the_hook_when_the_client_sends_it(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
    ) -> None:
        """AC1: the sent ``contextId`` is echoed on the task and reaches the hook."""
        async with _serving(
            deps=conversation_deps, container=container, plans=(conversational_plan(_AGENT),)
        ) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}",
                json=_rpc("message/send", context_id="c-42"),
                headers=_auth(),
            )

        assert response.json()["result"]["contextId"] == "c-42"
        assert [cmd.conversation_id for cmd in conversation_recorder.commands] == ["c-42"]

    @pytest.mark.parametrize("context_id", [None, ""])
    async def test_mints_a_context_id_when_the_client_does_not_send_one(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        context_id: str | None,
    ) -> None:
        """AC2: absent or empty, loom mints one and the hook gets the same id, never ``None``."""
        async with _serving(
            deps=conversation_deps, container=container, plans=(conversational_plan(_AGENT),)
        ) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}",
                json=_rpc("message/send", context_id=context_id),
                headers=_auth(),
            )

        minted = response.json()["result"]["contextId"]
        assert re.fullmatch(r"[0-9a-f]{32}", minted)
        assert [cmd.conversation_id for cmd in conversation_recorder.commands] == [minted]

    async def test_stamps_the_context_id_on_every_frame_when_it_streams(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
    ) -> None:
        """AC3: every frame, the initial ``submitted`` task included, carries the sent id."""
        async with _serving(
            deps=conversation_deps, container=container, plans=(conversational_plan(_AGENT),)
        ) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}",
                json=_rpc("message/stream", context_id="c-42"),
                headers=_auth(),
            )

        results = _stream_results(response.text)
        assert results[0]["kind"] == "task"
        assert results[0]["status"]["state"] == "submitted"
        assert [result["contextId"] for result in results] == ["c-42"] * len(results)
        assert [cmd.conversation_id for cmd in conversation_recorder.commands] == ["c-42"]

    async def test_delivers_the_context_id_to_the_loader_and_the_engine_over_a_conversational_plan(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
    ) -> None:
        """AC3: over a conversational plan, the loader and the engine see the sent id."""
        engine = ScriptedEngine()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, loader=True),),
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}",
                json=_rpc("message/stream", context_id="c-42"),
                headers=_auth(),
            )

        assert {result["contextId"] for result in _stream_results(response.text)} == {"c-42"}
        assert [cmd.conversation_id for cmd in conversation_recorder.load_commands] == ["c-42"]
        assert engine.conversations == [Conversation(conversation_id="c-42")]

    @pytest.mark.parametrize("method", ["message/send", "message/stream"])
    @pytest.mark.parametrize("context_id", ["x" * 129, 7])
    async def test_refuses_with_32602_when_the_context_id_is_malformed(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        method: str,
        context_id: str | int,
    ) -> None:
        """AC4, AC9: a malformed ``contextId`` is refused before any run or span, unechoed."""
        engine = ScriptedEngine()
        observer = RecordingObserver()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([observer]),
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc(method, context_id=context_id), headers=_auth()
            )

        error = response.json()["error"]
        assert error["code"] == -32602
        assert "contextId" in error["data"]["reason"]
        assert "x" * 129 not in response.text
        assert conversation_recorder.commands == []
        assert engine.stream_count == 0
        assert self._agent_spans(observer) == []

    @pytest.mark.parametrize("method", ["message/send", "message/stream"])
    async def test_refuses_with_32001_when_the_message_continues_a_task(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        method: str,
    ) -> None:
        """AC5: loom retains no task, so a non-empty ``taskId`` names one that does not exist."""
        engine = ScriptedEngine()
        observer = RecordingObserver()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([observer]),
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc(method, task_id="t-1"), headers=_auth()
            )

        error = response.json()["error"]
        assert error["code"] == -32001
        assert "taskId" in error["data"]["reason"]
        assert conversation_recorder.commands == []
        assert engine.stream_count == 0
        assert self._agent_spans(observer) == []

    async def test_refuses_with_32602_when_the_task_id_is_not_a_string(
        self, conversation_deps: RecordingDepsFactory, container: LoomContainer
    ) -> None:
        """A malformed ``taskId`` is a shape error, like a malformed ``contextId``."""
        engine = ScriptedEngine()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send", task_id=7), headers=_auth()
            )

        error = response.json()["error"]
        assert error["code"] == -32602
        assert "taskId" in error["data"]["reason"]
        assert engine.stream_count == 0

    async def test_refuses_with_32001_before_reading_the_parts_when_continuing_a_task(
        self, conversation_deps: RecordingDepsFactory, container: LoomContainer
    ) -> None:
        """A message continuing a task is refused on that ground even without a text part."""
        engine = ScriptedEngine()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            engines={_AGENT: engine},
        ) as (_app, client):
            body = _rpc("message/send", task_id="t-1")
            body["params"]["message"]["parts"] = []
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=body, headers=_auth())

        error = response.json()["error"]
        assert error["code"] == -32001
        assert "taskId" in error["data"]["reason"]
        assert engine.stream_count == 0

    async def test_ignores_the_task_id_when_it_is_empty(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """AC5: an empty ``taskId`` counts as absent and the run proceeds."""
        engine = ScriptedEngine()
        async with _serving(deps=deps, container=container, engines={_AGENT: engine}) as (
            _app,
            client,
        ):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}", json=_rpc("message/send", task_id=""), headers=_auth()
            )

        assert response.json()["result"]["status"] == {"state": "completed"}
        assert engine.stream_count == 1


class TestEffectiveExclusions:
    """The FR-041b guard reads the exclusions actually mounted (T151 B3, M1)."""

    async def test_fails_to_start_when_the_mounted_exclusion_opens_the_invocation(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A mounted exclusion aborts start-up even if the caller never declares it.

        ``exclude_paths`` defaults to empty, so validating only the argument
        would let a deployment open the invocation path by simply not repeating
        it here.
        """
        excluded = f"{_PREFIX}/{_AGENT}"
        with pytest.raises(AgentCompilationError) as raised:
            async with _serving(deps=deps, container=container, mounted_exclusions=(excluded,)) as (
                _app,
                _client,
            ):
                pass  # pragma: no cover - binding fails before the body runs

        issue = raised.value.issues[0]
        assert issue.code is AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS
        assert excluded in issue.message

    async def test_fails_to_start_when_the_exclusion_covers_another_agents_prefix(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The HTTP prefix under guard is the one the deployment really mounts."""
        excluded = "/bots/analyst/run"
        with pytest.raises(AgentCompilationError):
            await _bind_a2a(
                deps=deps,
                container=container,
                mounted_exclusions=(excluded,),
                agents_prefix="/bots",
            )

    async def test_starts_when_the_exclusion_covers_the_unused_default_prefix(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """With the agents mounted elsewhere, ``/agents`` is nobody's invocation path."""
        await _bind_a2a(
            deps=deps,
            container=container,
            mounted_exclusions=("/agents/analyst/run",),
            agents_prefix="/bots",
        )


class TestAnonymousOptOut:
    """Anonymity is granted by an active stanza only (T151 I1, I2)."""

    async def test_invokes_without_credentials_when_the_active_stanza_allows_it(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An active stanza with ``allow_anonymous`` serves an unauthenticated caller."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=True)}
        async with _serving_anonymously(
            deps=deps, container=container, endpoints=endpoints
        ) as client:
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"))

        assert response.json()["result"]["status"]["state"] == "completed"

    async def test_fails_to_start_when_a_disabled_stanza_declares_anonymous(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A disabled HTTP stanza grants nothing, so its opt-out is not inherited.

        Otherwise switching the HTTP surface off while leaving
        ``allow_anonymous: true`` behind would keep anonymous invocation alive
        on the one surface published to the internet.
        """
        endpoints = {_AGENT: make_endpoint(enabled=False, allow_anonymous=True)}
        with pytest.raises(ConfigError):
            async with _serving_anonymously(
                deps=deps, container=container, endpoints=endpoints
            ) as _client:
                pass  # pragma: no cover - binding fails before the body runs

    async def test_fails_to_start_when_publishing_without_a_usable_authenticator(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Publishing with no authenticator and no opt-out is a dead route, not a posture."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=False)}
        with pytest.raises(ConfigError) as raised:
            async with _serving_anonymously(
                deps=deps, container=container, endpoints=endpoints
            ) as _client:
                pass  # pragma: no cover - binding fails before the body runs

        assert _AGENT in str(raised.value)


class TestPublicationAnnouncement:
    """Start-up says exactly which agents are reachable from outside."""

    @staticmethod
    async def _publication_warning(
        *,
        deps: DepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
        endpoints: Mapping[str, AgentEndpointConfig] | None = None,
        plans: Sequence[AgentPlan] | None = None,
    ) -> str:
        """Return the WARNING text emitted while publishing one agent."""
        with caplog.at_level(logging.WARNING):
            async with _serving(
                deps=deps, container=container, endpoints=endpoints, plans=plans
            ) as (_app, _client):
                pass
        return caplog.text

    async def test_names_the_agent_when_publishing_it(
        self, deps: StubDepsFactory, container: LoomContainer, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The announcement names the agent and the URL a stranger reaches."""
        text = await self._publication_warning(deps=deps, container=container, caplog=caplog)

        assert _AGENT in text
        assert f"{_BASE_URL}{_PREFIX}/{_AGENT}" in text
        assert card_path(_AGENT, prefix=_PREFIX) in text

    async def test_does_not_announce_anonymity_when_the_stanza_is_disabled(
        self, deps: StubDepsFactory, container: LoomContainer, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A disabled stanza is announced as what it is: not an anonymous mount."""
        endpoints = {_AGENT: make_endpoint(enabled=False, allow_anonymous=True)}
        text = await self._publication_warning(
            deps=deps, container=container, caplog=caplog, endpoints=endpoints
        )

        assert "allow_anonymous=False" in text
        assert "NOT authenticated" not in text

    async def test_warns_the_context_id_is_the_credential_when_an_anonymous_mount_converses(
        self,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """AC6: an anonymous mount of a conversational agent says the id is the credential."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=True)}
        text = await self._publication_warning(
            deps=conversation_deps,
            container=container,
            caplog=caplog,
            endpoints=endpoints,
            plans=(conversational_plan(_AGENT, loader=True),),
        )

        assert "allow_anonymous=True" in text
        assert "contextId" in text
        assert "credential" in text


class TestBodyLimit:
    """The prompt cap applies to the JSON-RPC route too (FR-033a)."""

    async def test_refuses_the_prompt_when_it_exceeds_the_maximum(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A prompt over ``ai.max_prompt_bytes`` is ``-32602``, never a run."""
        request = _rpc("message/send", prompt="x" * 2048)
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=request, headers=_auth())

        assert response.json()["error"]["code"] == -32602

    async def test_refuses_the_json_rpc_body_when_it_exceeds_the_cap(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An oversized body answers ``-32602``, like the oversized prompt.

        The body cap and the prompt cap are the same logical refusal caught one
        layer apart. Answering the outer one with a flat ``{"code","message"}``
        413 handed a JSON-RPC client a body it cannot parse, and made the shape
        of the answer depend on where the size was noticed.
        """
        body = b'{"jsonrpc":"2.0","id":7,"method":"message/send","padding":"' + b"x" * 200_000
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", content=body, headers=_auth())

        assert response.status_code == 200
        assert response.json()["error"]["code"] == -32602

    async def test_the_cap_does_not_leak_as_a_flat_response_when_it_is_exceeded(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Nothing past authentication may answer in the flat REST shape."""
        body = b'{"jsonrpc":"2.0","id":7,"method":"message/send","padding":"' + b"x" * 200_000
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", content=body, headers=_auth())

        payload = response.json()
        assert payload["jsonrpc"] == "2.0"
        assert "code" not in payload


class TestResponseProtocol:
    """Where the protocol boundary sits: authentication outside, the rest inside.

    ``require_caller`` runs before JSON-RPC exists for this request, so an
    unverified caller is refused at the transport (HTTP 401, flat body).
    Everything after it — body read, envelope decode, method dispatch, the run
    itself — is inside the protocol and answers HTTP 200 with a JSON-RPC error
    object, whatever the failure.
    """

    async def test_responds_a_flat_401_when_the_verified_caller_is_missing(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Authentication precedes the protocol, so it answers outside it.

        In this wiring the mounted middleware refuses first; ``require_caller``
        is the same refusal one layer in, for the deployments that mount none.
        Either way the answer is an HTTP status, never a JSON-RPC envelope.
        """
        async with _serving(deps=deps, container=container) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}", json=_rpc("message/send"))

        assert response.status_code == 401
        assert "jsonrpc" not in response.text

    @pytest.mark.parametrize(
        ("body", "expected_code"),
        [
            pytest.param(b"not json at all", -32700, id="non-json-body"),
            pytest.param(b'{"jsonrpc":"2.0","id":7}', -32600, id="no-method"),
            pytest.param(
                b'{"jsonrpc":"2.0","id":7,"method":"message/unknown","params":{}}',
                -32601,
                id="unknown-method",
            ),
            pytest.param(
                b'{"jsonrpc":"2.0","id":7,"method":"message/send","params":{}}',
                -32602,
                id="no-text-part",
            ),
            pytest.param(
                b'{"jsonrpc":"2.0","id":7,"method":"message/send","padding":"' + b"x" * 200_000,
                -32602,
                id="oversized-body",
            ),
        ],
    )
    async def test_responds_json_rpc_when_the_failure_happens_after_require_caller(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        body: bytes,
        expected_code: int,
    ) -> None:
        async with _serving(deps=deps, container=container, max_prompt_bytes=1024) as (
            _app,
            client,
        ):
            response = await client.post(f"{_PREFIX}/{_AGENT}", content=body, headers=_auth())

        assert response.status_code == 200
        payload = response.json()
        assert payload["jsonrpc"] == "2.0"
        assert payload["error"]["code"] == expected_code
