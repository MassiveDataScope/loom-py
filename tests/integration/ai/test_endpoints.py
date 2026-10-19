"""HTTP surface of the agent runtime (T086-T095, T098).

Drives ``bind_agent_endpoints`` over a live :class:`AgentRuntime` backed by
scripted engines, asserting the wire contract in
``specs/001-ai-agent-layer/contracts/http-sse.md``: double opt-in mounting,
401 before 404, the body cap enforced while reading, the run-error status
table, the SSE surface, the redacted health projection, and the import
containment that keeps ``loom.ai`` out of an application with no ``ai:``
section.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import msgspec
import pytest
import yaml
from fastapi import FastAPI

from loom.ai.a2a.card import card_path
from loom.ai.a2a.server import bind_a2a_endpoints
from loom.ai.abc import (
    AgentEvent,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    StateShape,
    TextDeltaEvent,
)
from loom.ai.compiler._plan import AgentPlan
from loom.ai.config import A2AConfig, AgentEndpointConfig
from loom.ai.errors import (
    CONVERSATION_LOAD_FAILED_MESSAGE,
    CONVERSATION_LOAD_TIMEOUT_MESSAGE,
    AgentCompilationError,
    AgentErrorCode,
    AgentRunErrorCode,
)
from loom.ai.fastapi.endpoints import bind_agent_endpoints
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._hooks import HOOK_FAILED_MESSAGE
from loom.core.config.errors import ConfigError
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden
from loom.core.identity import ANONYMOUS, Identity, reset_identity, set_identity
from loom.core.model import loom_type, msgspec_type
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.auth.middleware import AuthenticationMiddleware
from tests.integration.ai.conftest import (
    DEFAULT_OUTPUT,
    DEFAULT_USAGE,
    OUTPUT_TWINS,
    ConversationRecorder,
    CountingEngineProvider,
    RecordingDepsFactory,
    RecordingMcpSession,
    RecordingObserver,
    RecordingScriptedEngine,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    conversational_plan,
    make_ai_config,
    make_endpoint,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
    plan_with_output,
)

_AGENT = "analyst"
_PREFIX = "/agents"
_MCP_SERVER = "tools"
_SRC = Path(__file__).resolve().parents[3] / "src"
_INTERACTION_ID_LENGTH = 32
"""Length of the ``uuid4().hex`` the runtime mints per admitted run."""
_NEW_MESSAGES = b'[{"kind": "request", "conversation_id": "c-42"}]'
_RESULT_KEYS = {"output", "usage", "interaction_id", "hook_result"}
"""The only keys a completed run publishes, on ``/run`` and on the ``final`` frame."""


class StubAuthenticator:
    """Authentication mechanism standing in for any configured one."""

    name = "stub"
    provides_roles = True

    async def authenticate(self, credentials: object) -> Identity | None:
        """Authenticate every caller as the same fixed subject."""
        del credentials
        return Identity(subject="stub-user", mechanism=self.name)


class _IdentityMiddleware:
    """Publishes a fixed identity for the duration of one request."""

    def __init__(self, app: Any, *, identity: Identity | None) -> None:
        self._app = app
        self._identity = identity

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """Set the identity around the downstream ASGI call."""
        if scope["type"] != "http" or self._identity is None:
            await self._app(scope, receive, send)
            return
        token = set_identity(self._identity)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


@asynccontextmanager
async def _serving(
    *,
    deps: DepsFactory,
    container: LoomContainer,
    plans: Sequence[AgentPlan] | None = None,
    engines: Mapping[str, ScriptedEngine] | None = None,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    identity: Identity | None = None,
    authenticator: object | None = None,
    max_prompt_bytes: int = 65536,
    max_state_bytes: int = 65536,
    health_cache_ttl_ms: int = 20,
    max_concurrent_runs: int = 8,
    observability_runtime: ObservabilityRuntime | None = None,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    """Serve an entered runtime over an in-process ASGI client."""
    config = make_ai_config(
        endpoints=dict(endpoints if endpoints is not None else {_AGENT: make_endpoint()}),
        mcp_servers=make_mcp_servers(_MCP_SERVER),
        max_prompt_bytes=max_prompt_bytes,
        max_state_bytes=max_state_bytes,
        health_cache_ttl_ms=health_cache_ttl_ms,
        max_concurrent_runs=max_concurrent_runs,
    )
    runtime = AgentRuntime(
        plans=list(plans if plans is not None else (make_plan(_AGENT),)),
        config=config,
        engine_provider=CountingEngineProvider(engines=engines),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(
            {_MCP_SERVER: StubMcpClient(label="mcp", session=RecordingMcpSession(), log=[])}
        ),  # type: ignore[arg-type]
    )
    async with runtime:
        app = FastAPI()
        bind_agent_endpoints(
            app,
            runtime=runtime,
            config=config,
            authenticator=authenticator if authenticator is not None else StubAuthenticator(),  # type: ignore[arg-type]
            observability_runtime=observability_runtime,
            prefix=_PREFIX,
        )
        app.add_middleware(_IdentityMiddleware, identity=identity)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://agents.test") as client:
            yield app, client


def _route_paths(app: FastAPI) -> set[str]:
    """Return every path the application mounted."""
    return {str(getattr(route, "path", "")) for route in app.routes}


def _sse_names(payload: str) -> list[str]:
    """Return the event names of an SSE payload, ignoring comment frames."""
    return [line[len("event: ") :] for line in payload.splitlines() if line.startswith("event: ")]


async def _asgi_post(
    app: FastAPI,
    path: str,
    chunks: Sequence[bytes],
    headers: Sequence[tuple[bytes, bytes]],
    *,
    on_body: Callable[[bytes], None] | None = None,
) -> int:
    """POST raw body chunks over ASGI and return the response status.

    Args:
        app: Application under test.
        path: Request path.
        chunks: Body chunks, sent one ASGI message each.
        headers: Raw request headers.
        on_body: Called with every non-empty response body chunk the moment it
            reaches the wire, so a test can order it against other work.
    """
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": chunk, "more_body": True} for chunk in chunks
    ]
    messages.append({"type": "http.request", "body": b"", "more_body": False})
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        if messages:
            return messages.pop(0)
        # A streaming response listens for the disconnect concurrently with
        # the body: a receive that never suspends would starve the frames.
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def send(message: Any) -> None:
        sent.append(dict(message))
        if on_body is not None and message["type"] == "http.response.body" and message.get("body"):
            on_body(message["body"])

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
        "headers": list(headers),
        "client": ("testclient", 50000),
        "server": ("agents.test", 80),
    }
    await app(scope, receive, send)
    return int(next(m["status"] for m in sent if m["type"] == "http.response.start"))


def _script_with_messages() -> tuple[AgentEvent, ...]:
    """A success script whose ``final`` carries the run's new messages."""
    return (
        TextDeltaEvent(text="ok"),
        FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE, messages=_NEW_MESSAGES),
    )


async def _abandon_stream_when(app: FastAPI, path: str, signal: asyncio.Event) -> None:
    """Drive one ``/stream`` request over ASGI and disconnect once *signal* is set.

    ``httpx.ASGITransport`` buffers the whole response, so it cannot express a
    client that walks away; the raw ASGI call can.
    """
    pending: list[dict[str, Any]] = [
        {"type": "http.request", "body": b'{"prompt": "p"}', "more_body": False}
    ]

    async def receive() -> dict[str, Any]:
        if pending:
            return pending.pop(0)
        await signal.wait()
        return {"type": "http.disconnect"}

    async def send(message: Any) -> None:
        del message

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


def _final_frame(payload: str) -> dict[str, Any]:
    """Return the decoded data of the single ``final`` frame of an SSE payload."""
    return _frame(payload, "final")


def _frame(payload: str, name: str) -> dict[str, Any]:
    """Return the decoded data of the single ``name`` frame of an SSE payload."""
    lines = payload.splitlines()
    (position,) = [index for index, line in enumerate(lines) if line == f"event: {name}"]
    return dict(msgspec.json.decode(lines[position + 1][len("data: ") :]))


class TestMounting:
    """Double opt-in: only ``enabled`` agents with a named ``auth`` mount (T095)."""

    async def test_mounts_no_route_when_the_agent_is_not_in_endpoints(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An agent absent from ``ai.endpoints`` exposes no route at all."""
        async with _serving(
            deps=deps,
            container=container,
            plans=(make_plan(_AGENT), make_plan("hidden")),
            identity=identity,
        ) as (app, _client):
            assert f"{_PREFIX}/hidden/run" not in _route_paths(app)

    async def test_responds_404_when_the_agent_is_not_in_endpoints(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A compiled but unexposed agent is unreachable for a verified caller."""
        async with _serving(
            deps=deps,
            container=container,
            plans=(make_plan(_AGENT), make_plan("hidden")),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/hidden/run", json={"prompt": "p"})

            assert response.status_code == 404

    async def test_responds_a_null_interaction_id_when_it_fails_before_admission(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A refusal before any run exists still carries the key, as ``null``."""
        async with _serving(
            deps=deps,
            container=container,
            plans=(make_plan(_AGENT), make_plan("hidden")),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/hidden/run", json={"prompt": "p"})

            assert response.json() == {
                "code": "AGENT_NOT_FOUND",
                "message": "no agent named 'hidden' is exposed",
                "interaction_id": None,
            }

    async def test_mounts_no_route_when_the_agent_is_disabled(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """``enabled: false`` is an opt-out even with a named authentication."""
        async with _serving(
            deps=deps,
            container=container,
            endpoints={_AGENT: make_endpoint(enabled=False)},
            identity=identity,
        ) as (app, _client):
            assert f"{_PREFIX}/{_AGENT}/run" not in _route_paths(app)

    async def test_fails_to_start_when_no_usable_authenticator_exists(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Opting in without an authenticator and without anonymous aborts start-up."""
        config = make_ai_config(endpoints={_AGENT: make_endpoint(allow_anonymous=False)})
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT)],
            config=config,
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )

        app = FastAPI()

        async with runtime:
            with pytest.raises(ConfigError):
                bind_agent_endpoints(
                    app, runtime=runtime, config=config, authenticator=None, prefix=_PREFIX
                )

    async def test_warns_when_mounting_an_agent(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Every mount announces its security state at WARNING level."""
        with caplog.at_level(logging.WARNING):
            async with _serving(deps=deps, container=container) as (_app, _client):
                pass

        assert _AGENT in caplog.text

    @staticmethod
    async def _mount_warning(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
        allow_anonymous: bool,
    ) -> str:
        """Return the mount WARNING emitted for one endpoint configuration."""
        endpoints = {_AGENT: make_endpoint(allow_anonymous=allow_anonymous)}
        with caplog.at_level(logging.WARNING):
            async with _serving(deps=deps, container=container, endpoints=endpoints) as (
                _app,
                _client,
            ):
                pass
        return caplog.text

    async def test_does_not_claim_a_verified_identity_when_the_mount_is_anonymous(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """``allow_anonymous`` removes the identity: the warning must not claim one."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=True
        )

        assert "runs as that verified identity" not in text
        assert "NOT authenticated" in text

    async def test_warns_about_spend_without_identity_when_the_mount_is_anonymous(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An anonymous mount spends model tokens for an unidentified caller: say so."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=True
        )

        assert "no verified identity" in text
        assert "max_concurrent_runs" in text
        assert "rate limit" in text

    async def test_claims_a_verified_identity_when_the_mount_requires_auth(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An authenticated mount keeps the sentence that is true for it."""
        text = await self._mount_warning(
            deps=deps, container=container, caplog=caplog, allow_anonymous=False
        )

        assert "runs as that verified identity" in text


class TestAuthenticationBeforeExistence:
    """401 precedes 404 so the surface cannot enumerate agents (T089)."""

    async def test_responds_401_when_the_agent_does_not_exist_and_the_caller_is_anonymous(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous probe for an unknown agent gets 401, never 404 (FR-029b)."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/does-not-exist/run", json={"prompt": "p"})

            assert response.status_code == 401

    async def test_responds_401_when_the_agent_exists_and_the_caller_is_anonymous(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous caller is refused before the agent is even resolved."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 401


class TestRun:
    """``POST /run`` returns the decoded output and its usage (T087)."""

    async def test_responds_200_when_the_identity_is_valid(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A verified caller drives the agent to completion."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 200

    async def test_returns_output_and_usage_when_the_identity_is_valid(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The body carries the decoded output plus the run's usage, encoded once."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            body = response.json()
            assert len(body.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert body == {
                "output": dict(DEFAULT_OUTPUT),
                "usage": {
                    "input_tokens": DEFAULT_USAGE.input_tokens,
                    "output_tokens": DEFAULT_USAGE.output_tokens,
                    "requests": DEFAULT_USAGE.requests,
                    "duration_ms": DEFAULT_USAGE.duration_ms,
                    "cache_read_tokens": DEFAULT_USAGE.cache_read_tokens,
                    "cache_write_tokens": DEFAULT_USAGE.cache_write_tokens,
                    "tool_calls": DEFAULT_USAGE.tool_calls,
                    "cost": str(DEFAULT_USAGE.cost),
                    "details": dict(DEFAULT_USAGE.details),
                },
                "hook_result": None,
            }

    async def test_responds_422_when_the_body_carries_an_unknown_key(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The body accepts nothing beyond the documented keys."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "history": []}
            )

            assert response.status_code == 422
            assert response.json()["code"] == "INVALID_REQUEST"


class TestConversationId:
    """``conversation_id`` is an opaque body field copied into the hook's command (AC12)."""

    async def test_delivers_the_conversation_id_to_the_command_when_it_is_given(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """The value on the body reaches the command untouched."""
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert response.status_code == 200
            assert [command.conversation_id for command in conversation_recorder.commands] == [
                "c-42"
            ]

    async def test_delivers_none_when_no_conversation_id_is_given(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A body without the field delivers ``None``."""
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert [command.conversation_id for command in conversation_recorder.commands] == [None]

    @pytest.mark.parametrize("value", ["x" * 129, ""])
    async def test_responds_422_when_the_conversation_id_is_out_of_bounds(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity, value: str
    ) -> None:
        """Out-of-bound values are refused at decode, before any run is admitted."""
        async with _serving(deps=deps, container=container, identity=identity) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": value}
            )

            assert response.status_code == 422
            assert response.json()["code"] == "INVALID_REQUEST"
            assert response.json()["interaction_id"] is None


class TestHookOverHttp:
    """The output hook seen from the wire: ids, results and failures (AC13)."""

    async def test_returns_the_interaction_id_and_the_hook_result_when_the_hook_completes(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """The ``/run`` body carries the id the hook saw and what the hook returned."""
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            body = response.json()
            assert len(body["interaction_id"]) == _INTERACTION_ID_LENGTH
            assert body["hook_result"] == {"triage_id": body["interaction_id"]}
            assert [command.interaction_id for command in conversation_recorder.commands] == [
                body["interaction_id"]
            ]

    async def test_emits_final_with_the_hooks_result_when_it_streams(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """The ``final`` frame carries the id and the hook's result, and the hook ran first."""
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (app, _client):
            chunks: list[bytes] = []

            def _on_body(chunk: bytes) -> None:
                chunks.append(chunk)
                if b"event: final" in chunk:
                    conversation_recorder.timeline.append("final")

            status = await _asgi_post(
                app,
                f"{_PREFIX}/{_AGENT}/stream",
                [b'{"prompt": "p"}'],
                [(b"content-type", b"application/json")],
                on_body=_on_body,
            )

            assert status == 200
            assert conversation_recorder.timeline == ["hook", "final"]
            final = _final_frame(b"".join(chunks).decode("utf-8"))
            assert len(final["interaction_id"]) == _INTERACTION_ID_LENGTH
            assert final["hook_result"] == {"triage_id": final["interaction_id"]}

    async def test_responds_500_hook_failed_with_the_interaction_id_when_the_hook_raises(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """The body is the fixed text plus the id; the exception detail stays server-side."""
        conversation_recorder.failure = ValueError("secret detail")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 500
            body = response.json()
            assert len(body.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert body == {"code": "HOOK_FAILED", "message": HOOK_FAILED_MESSAGE}
            assert "secret detail" not in response.text

    async def test_emits_an_error_with_the_interaction_id_when_the_hook_raises_in_stream(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """On ``/stream`` the same failure is one ``error`` frame carrying the id."""
        conversation_recorder.failure = ValueError("secret detail")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _sse_names(response.text) == ["text_delta", "error"]
            error = _frame(response.text, "error")
            assert len(error.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert error == {"code": "HOOK_FAILED", "message": HOOK_FAILED_MESSAGE}
            assert "secret detail" not in response.text

    async def test_commits_once_and_releases_the_slot_when_the_client_abandons_during_the_hook(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A disconnect mid-hook neither cuts the record nor closes a running generator."""
        conversation_recorder.gate = asyncio.Event()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
            max_concurrent_runs=1,
        ) as (app, client):
            with caplog.at_level(logging.WARNING):
                request = asyncio.ensure_future(
                    _abandon_stream_when(
                        app, f"{_PREFIX}/{_AGENT}/stream", conversation_recorder.entered
                    )
                )
                await conversation_recorder.entered.wait()
                await asyncio.sleep(0.05)  # the disconnect reaches the stream

                refused = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

                conversation_recorder.gate.set()
                await request
                admitted = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

        assert refused.status_code == 429
        assert refused.json()["code"] == AgentRunErrorCode.TOO_MANY_RUNS
        assert admitted.status_code == 200
        assert conversation_deps.uow.log == ["begin", "commit", "begin", "commit"]
        assert "aclose" not in caplog.text

    async def test_responds_403_when_the_hooks_rules_refuse(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A ``Forbidden`` raised by the hook keeps its meaning on the wire."""
        conversation_recorder.failure = Forbidden("triage of INC-1 is restricted")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == 403
            body = response.json()
            assert body["code"] == "UNAUTHORIZED"
            assert len(body["interaction_id"]) == _INTERACTION_ID_LENGTH


class TestNoHistoryOverTheWire:
    """A run's new messages never reach the wire (006 AC13)."""

    async def test_responds_with_only_the_four_keys_when_the_final_carries_messages(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """``/run`` projects the result explicitly: ``messages`` cannot leak as base64."""
        engine = ScriptedEngine(script=_script_with_messages())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert response.status_code == 200
            assert set(response.json()) == _RESULT_KEYS

    async def test_emits_final_with_the_four_keys_when_the_final_carries_messages(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The ``final`` frame keeps its published shape whatever the event carries."""
        engine = ScriptedEngine(script=_script_with_messages())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert _sse_names(response.text) == ["text_delta", "final"]
            assert set(_final_frame(response.text)) == _RESULT_KEYS


class TestLoaderOverHttp:
    """The conversation loader seen from the wire: failures and the caller (006 AC13)."""

    async def test_responds_500_with_the_fixed_text_when_the_loader_fails(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A failed load is a coded 500 with the fixed text and the run's id, nothing else."""
        conversation_recorder.load_failure = ValueError("secret detail")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, hook=False, loader=True),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert response.status_code == 500
            body = response.json()
            assert len(body.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert body == {
                "code": "CONVERSATION_LOAD_FAILED",
                "message": CONVERSATION_LOAD_FAILED_MESSAGE,
            }

    async def test_emits_a_single_error_when_the_loader_fails_in_stream(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """On ``/stream`` the same failure is the single frame, with the same fields."""
        conversation_recorder.load_failure = ValueError("secret detail")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, hook=False, loader=True),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert _sse_names(response.text) == ["error"]
            frame = _frame(response.text, "error")
            assert len(frame.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert frame == {
                "code": "CONVERSATION_LOAD_FAILED",
                "message": CONVERSATION_LOAD_FAILED_MESSAGE,
            }

    async def test_emits_a_single_error_when_the_loader_times_out_in_stream(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A loader timeout is the single ``error`` frame with its own code and no ``final``."""
        conversation_recorder.load_failure = TimeoutError()
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, hook=False, loader=True),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert _sse_names(response.text) == ["error"]
            frame = _frame(response.text, "error")
            assert len(frame.pop("interaction_id")) == _INTERACTION_ID_LENGTH
            assert frame == {
                "code": "CONVERSATION_LOAD_TIMEOUT",
                "message": CONVERSATION_LOAD_TIMEOUT_MESSAGE,
            }

    async def test_responds_403_when_the_loaders_rules_refuse(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """A ``Forbidden`` raised by the loader keeps its meaning on the wire."""
        conversation_recorder.load_failure = Forbidden("thread c-42 belongs to another subject")
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, hook=False, loader=True),),
            identity=identity,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert response.status_code == 403
            body = response.json()
            assert body["code"] == "UNAUTHORIZED"
            assert len(body["interaction_id"]) == _INTERACTION_ID_LENGTH

    async def test_loads_as_anonymous_when_the_endpoint_allows_anonymous(
        self,
        conversation_deps: RecordingDepsFactory,
        conversation_recorder: ConversationRecorder,
        container: LoomContainer,
    ) -> None:
        """An anonymous mount runs the loader as the anonymous identity, by value."""
        async with _serving(
            deps=conversation_deps,
            container=container,
            plans=(conversational_plan(_AGENT, hook=False, loader=True),),
            endpoints={_AGENT: make_endpoint(allow_anonymous=True)},
            identity=None,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p", "conversation_id": "c-42"}
            )

            assert response.status_code == 200
            assert conversation_recorder.load_callers == [ANONYMOUS]


class TestBodyCap:
    """The prompt cap is enforced while reading, never from the header (T088)."""

    async def test_responds_413_when_the_body_exceeds_the_cap(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An oversized honest body is rejected with 413."""
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "x" * 200_000})

            assert response.status_code == 413

    async def test_codes_prompt_too_large_when_the_body_exceeds_the_cap(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The 413 body carries the stable ``PROMPT_TOO_LARGE`` code."""
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "x" * 200_000})

            assert response.json()["code"] == "PROMPT_TOO_LARGE"

    async def test_responds_413_when_the_content_length_lies(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A body larger than its declared length is still rejected while reading."""
        chunk = b'{"prompt":"' + b"x" * 100_000 + b'"}'
        async with _serving(
            deps=deps, container=container, identity=identity, max_prompt_bytes=32
        ) as (app, _client):
            status = await _asgi_post(
                app,
                f"{_PREFIX}/{_AGENT}/run",
                [chunk, chunk, chunk],
                [
                    (b"host", b"agents.test"),
                    (b"content-type", b"application/json"),
                    (b"content-length", b"12"),
                ],
            )

            assert status == 413


class _StateWithDefault(msgspec.Struct, forbid_unknown_fields=True):
    """A state shape with one required field and one declared default."""

    marca: str
    km: int = 0


_SCHEMA_STATE = StateShape(schema={"type": "object"}, loom_type=msgspec_type(_StateWithDefault))
_OPEN_STATE = StateShape(schema=None, loom_type=None)


class TestState:
    """``state``'s single decode, its own cap and the normalised mapping (T304)."""

    async def test_omitted_defaults_reach_the_engine_normalised_and_never_echo_back(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        engine = RecordingScriptedEngine()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=[make_plan(_AGENT, state=_SCHEMA_STATE)],
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run",
                json={"prompt": "assess", "state": {"marca": "civic"}},
            )

        assert response.status_code == 200
        assert "state" not in response.json()
        assert engine.states == [{"marca": "civic", "km": 0}]

    async def test_state_against_no_declared_shape_is_422_naming_the_field(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """AC-009: a stateless artefact refuses a non-empty ``state``, naming itself."""
        async with _serving(
            deps=deps, container=container, identity=identity, plans=[make_plan(_AGENT)]
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run",
                json={"prompt": "assess", "state": {"marca": "civic"}},
            )

        body = response.json()
        assert response.status_code == 422
        assert body["code"] == "STATE_NOT_DECLARED"
        assert _AGENT in body["message"]

    async def test_a_state_missing_a_required_field_is_422(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """AC-008: a malformed state never reaches the engine — no run, no usage."""
        engine = RecordingScriptedEngine()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=[make_plan(_AGENT, state=_SCHEMA_STATE)],
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run", json={"prompt": "assess", "state": {}}
            )

        assert response.status_code == 422
        assert engine.states == []
        assert engine.stream_count == 0
        assert response.json()["code"] == "INVALID_STATE"

    async def test_a_large_state_is_413_naming_state_with_a_legal_prompt(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """AC-013: ``state``'s own cap, distinct from the prompt's."""
        # Small enough to stay under the *total* body cap
        # (max_prompt_bytes + max_state_bytes + BODY_OVERHEAD_BYTES), so the
        # generic body-size guard never fires first — only 'state''s own cap.
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=[make_plan(_AGENT, state=_OPEN_STATE)],
            max_state_bytes=32,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run",
                json={"prompt": "assess", "state": {"marca": "x" * 5_000}},
            )

        assert response.status_code == 413
        assert response.json()["code"] == "STATE_TOO_LARGE"

    async def test_a_large_prompt_still_gives_its_own_413_with_a_legal_state(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """AC-013: ``max_prompt_bytes`` keeps measuring the prompt alone (FR-015)."""
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=[make_plan(_AGENT, state=_OPEN_STATE)],
            max_prompt_bytes=32,
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/run",
                json={"prompt": "x" * 200_000, "state": {"marca": "civic"}},
            )

        assert response.status_code == 413
        assert response.json()["code"] == "PROMPT_TOO_LARGE"

    async def test_stream_resolves_state_the_same_way_as_run(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        engine = RecordingScriptedEngine()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=[make_plan(_AGENT, state=_SCHEMA_STATE)],
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(
                f"{_PREFIX}/{_AGENT}/stream",
                json={"prompt": "assess", "state": {"marca": "civic"}},
            )

        assert response.status_code == 200
        assert "state" not in _final_frame(response.text)
        assert engine.states == [{"marca": "civic", "km": 0}]


class TestErrorMapping:
    """Run-error codes map to the published status codes (contract table)."""

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            (AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION, 422),
            (AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED, 422),
            (AgentRunErrorCode.USAGE_LIMIT_EXCEEDED, 422),
            (AgentRunErrorCode.PROVIDER_UNAVAILABLE, 503),
            (AgentRunErrorCode.TOOL_UNAVAILABLE, 503),
            (AgentRunErrorCode.TOOL_TIMEOUT, 504),
            (AgentRunErrorCode.RUN_TIMEOUT, 504),
            (AgentRunErrorCode.TOO_MANY_RUNS, 429),
            (AgentRunErrorCode.UNAUTHORIZED, 403),
            (AgentRunErrorCode.HOOK_FAILED, 500),
            (AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT, 504),
            (AgentRunErrorCode.COST_NOT_MEASURABLE, 500),
        ],
    )
    async def test_maps_the_status_when_the_run_fails(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
        code: AgentRunErrorCode,
        status: int,
    ) -> None:
        """Each run-error code produces its documented HTTP status."""
        engine = ScriptedEngine(script=(ErrorEvent(code=code, message="scripted"),))
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.status_code == status


class TestStream:
    """``POST /stream`` is SSE, with pre-stream failures as status codes (T090)."""

    @staticmethod
    def _script() -> tuple[AgentEvent, ...]:
        return (
            TextDeltaEvent(text="Demand rose "),
            FinalEvent(output={"answer": "42"}, usage=DEFAULT_USAGE),
        )

    async def test_responds_text_event_stream_when_the_identity_is_valid(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The streaming endpoint advertises the SSE media type."""
        engine = ScriptedEngine(script=self._script())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert response.headers["content-type"].startswith("text/event-stream")

    async def test_emits_the_frames_in_order_when_the_identity_is_valid(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Frames arrive in script order and end in the single terminal event."""
        engine = ScriptedEngine(script=self._script())
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _sse_names(response.text) == ["text_delta", "final"]

    async def test_responds_401_when_it_fails_before_the_first_byte(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A pre-stream failure is a status code, never an SSE frame."""
        async with _serving(deps=deps, container=container, identity=None) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert response.status_code == 401

    async def test_emits_an_in_band_error_when_it_fails_after_the_first_byte(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A failure after the first byte travels in-band as an ``error`` frame."""
        engine = ScriptedEngine(
            script=(
                TextDeltaEvent(text="Demand rose "),
                ErrorEvent(code=AgentRunErrorCode.PROVIDER_RATE_LIMITED, message="rate limited"),
            )
        )
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _sse_names(response.text) == ["text_delta", "error"]


@pytest.mark.parametrize(("library", "cls", "answer"), OUTPUT_TWINS)
class TestOutputProjection:
    """``/run`` and the SSE ``final`` frame carry the answer as builtins (SC-001)."""

    async def test_run_body_carries_the_hook_payload(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
        library: str,
        cls: type[Any],
        answer: object,
    ) -> None:
        """The ``/run`` body's ``output`` is a JSON object equal to the hook payload."""
        del library
        plan = plan_with_output(cls, name=_AGENT)
        engine = ScriptedEngine(script=(FinalEvent(output=answer, usage=DEFAULT_USAGE),))
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=(plan,),
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})

            assert response.json()["output"] == loom_type(cls).to_builtins(answer)

    async def test_sse_final_carries_the_hook_payload(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
        library: str,
        cls: type[Any],
        answer: object,
    ) -> None:
        """The SSE ``final`` frame's ``output`` is a JSON object equal to the hook payload."""
        del library
        plan = plan_with_output(cls, name=_AGENT)
        engine = ScriptedEngine(script=(FinalEvent(output=answer, usage=DEFAULT_USAGE),))
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            plans=(plan,),
            engines={_AGENT: engine},
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})

            assert _final_frame(response.text)["output"] == loom_type(cls).to_builtins(answer)


class TestStreamTracing:
    """``POST /stream`` is attributed: exactly one agent span, never one per delta."""

    @staticmethod
    def _script() -> tuple[AgentEvent, ...]:
        return (
            TextDeltaEvent(text="Demand "),
            TextDeltaEvent(text="rose "),
            TextDeltaEvent(text="12%"),
            FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
        )

    @staticmethod
    async def _stream_with_recorder(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> list[LifecycleEvent]:
        """Drive one full ``/stream`` request and return the events it emitted."""
        recorder = RecordingObserver()
        engine = ScriptedEngine(script=TestStreamTracing._script())
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/stream", json={"prompt": "p"})
            assert _sse_names(response.text) == [
                "text_delta",
                "text_delta",
                "text_delta",
                "final",
            ]
        return recorder.events

    async def test_emits_a_single_agent_span_when_serving_a_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """One run is one span: three deltas do not become three spans."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        starts = [
            event
            for event in events
            if event.scope is Scope.AGENT and event.kind is EventKind.START
        ]
        assert len(starts) == 1

    async def test_closes_the_span_when_the_stream_ends(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The span closes when the generator is exhausted, not when the handler returns."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        ends = [
            event for event in events if event.scope is Scope.AGENT and event.kind is EventKind.END
        ]
        assert len(ends) == 1

    @staticmethod
    async def _abandon_stream_after_first_frame(app: FastAPI, path: str) -> None:
        """Drive one ``/stream`` request over ASGI and disconnect mid-run.

        ``httpx.ASGITransport`` buffers the whole response, so it cannot express
        a client that walks away; the raw ASGI call can. The disconnect is
        published only once a frame reached the wire, so the span is provably
        open when the stream is abandoned.
        """
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

    async def test_closes_the_span_when_the_client_abandons_the_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A disconnect terminates the span: an abandoned stream leaks no open span."""
        recorder = RecordingObserver()
        # The second delta never arrives, so the run is still in flight — and
        # its span still open — when the client disconnects after the first.
        engine = ScriptedEngine(
            script=(
                TextDeltaEvent(text="Demand "),
                TextDeltaEvent(text="rose "),
                FinalEvent(output=DEFAULT_OUTPUT, usage=DEFAULT_USAGE),
            ),
            delays_ms=(0, 60_000, 0),
        )
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (app, _client):
            await self._abandon_stream_after_first_frame(app, f"{_PREFIX}/{_AGENT}/stream")

        agent_kinds = [event.kind for event in recorder.events if event.scope is Scope.AGENT]
        assert agent_kinds[0] is EventKind.START
        assert agent_kinds[-1] in (EventKind.END, EventKind.ERROR)

    async def test_attributes_the_subject_when_serving_a_stream(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The span carries who ran what, over which route: an audit needs all of it."""
        events = await self._stream_with_recorder(deps=deps, container=container, identity=identity)

        start = next(
            event
            for event in events
            if event.scope is Scope.AGENT and event.kind is EventKind.START
        )
        assert start.meta["subject"] == identity.subject
        assert start.meta["agent"] == _AGENT
        assert start.meta["mechanism"] == identity.mechanism
        assert start.meta["route"] == f"{_PREFIX}/{{name}}/stream"
        assert start.meta["method"] == "POST"


class TestUsageOnTheAgentSpan:
    """The closing attributes of the agent span carry what the run spent.

    The response has carried the usage from the start; the span had none, so
    an operator comparing two models on cost or on model round trips had to
    correlate provider-side invocation logs by time window.
    """

    @staticmethod
    async def _agent_events(
        suffix: str,
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
        engine: ScriptedEngine | None = None,
    ) -> list[LifecycleEvent]:
        """Drive one successful request and return the agent-scope events."""
        recorder = RecordingObserver()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine if engine is not None else ScriptedEngine()},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/{suffix}", json={"prompt": "p"})
            assert response.status_code == 200
        return [event for event in recorder.events if event.scope is Scope.AGENT]

    @staticmethod
    def _closing_usage(events: Sequence[LifecycleEvent]) -> Mapping[str, object]:
        """Return the usage attributes of the single closing event."""
        end = next(event for event in events if event.kind is EventKind.END)
        return {key: value for key, value in end.meta.items() if key.startswith("gen_ai.usage.")}

    async def test_publishes_every_counter_when_a_run_completes(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Cost and model round trips are readable off the span, not off provider logs."""
        events = await self._agent_events("run", deps=deps, container=container, identity=identity)

        assert self._closing_usage(events) == {
            "gen_ai.usage.input_tokens": DEFAULT_USAGE.input_tokens,
            "gen_ai.usage.output_tokens": DEFAULT_USAGE.output_tokens,
            "gen_ai.usage.cache_read.input_tokens": DEFAULT_USAGE.cache_read_tokens,
            "gen_ai.usage.cache_creation.input_tokens": DEFAULT_USAGE.cache_write_tokens,
            "gen_ai.usage.requests": DEFAULT_USAGE.requests,
            "gen_ai.usage.tool_calls": DEFAULT_USAGE.tool_calls,
            "gen_ai.usage.cost": pytest.approx(0.0021),
            "gen_ai.usage.cost_known": True,
            "gen_ai.usage.details.reasoning_tokens": 5,
        }

    async def test_keeps_the_usage_off_the_opening_attributes(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Nothing is spent yet when the span opens, so nothing is claimed there."""
        events = await self._agent_events("run", deps=deps, container=container, identity=identity)

        start = next(event for event in events if event.kind is EventKind.START)
        assert not [key for key in start.meta if key.startswith("gen_ai.usage.")]

    async def test_publishes_no_cost_when_the_model_has_no_price(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An absent cost publishes no attribute: a zero would read as a free run."""
        engine = ScriptedEngine(
            script=(
                FinalEvent(
                    output=DEFAULT_OUTPUT, usage=msgspec.structs.replace(DEFAULT_USAGE, cost=None)
                ),
            )
        )
        events = await self._agent_events(
            "run", deps=deps, container=container, identity=identity, engine=engine
        )

        usage = self._closing_usage(events)
        assert "gen_ai.usage.cost" not in usage
        # Published as unknown rather than omitted in silence: a dashboard
        # summing the cost must be able to tell a total from a lower bound.
        assert usage["gen_ai.usage.cost_known"] is False

    async def test_publishes_what_a_failed_run_burned(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A run that spends and then fails reports the spend: it was not free."""
        recorder = RecordingObserver()
        engine = ScriptedEngine(
            script=(
                ErrorEvent(
                    code=AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION,
                    message="the model answered outside the declared schema",
                    usage=DEFAULT_USAGE,
                ),
            )
        )
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            observability_runtime=ObservabilityRuntime([recorder]),
        ) as (_app, client):
            response = await client.post(f"{_PREFIX}/{_AGENT}/run", json={"prompt": "p"})
            assert response.status_code == 422

        closing = next(
            event
            for event in recorder.events
            if event.scope is Scope.AGENT and event.kind is not EventKind.START
        )
        assert closing.meta["gen_ai.usage.requests"] == DEFAULT_USAGE.requests
        assert closing.meta["gen_ai.usage.cost"] == pytest.approx(0.0021)

    async def test_publishes_the_same_counters_when_it_streams(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A streamed run is attributed like a complete one: same run, same span."""
        completed = await self._agent_events(
            "run", deps=deps, container=container, identity=identity
        )
        streamed = await self._agent_events(
            "stream", deps=deps, container=container, identity=identity
        )

        assert self._closing_usage(streamed) == self._closing_usage(completed)


class TestHealth:
    """``GET /health`` is cached, redacted for anonymous callers (T093/T094)."""

    @staticmethod
    async def _wait_for_status(
        client: httpx.AsyncClient, path: str, expected: str, *, budget_s: float = 0.5
    ) -> httpx.Response:
        """Poll the cached health until the background probe reports ``expected``."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_s
        response = await client.get(path)
        while loop.time() < deadline and response.json().get("status") != expected:
            await asyncio.sleep(0.005)
            response = await client.get(path)
        return response

    async def test_omits_checks_when_the_caller_is_anonymous(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """An anonymous caller gets the aggregate only (FR-029c)."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=None) as (
            _app,
            client,
        ):
            response = await client.get(f"{_PREFIX}/{_AGENT}/health")

            assert "checks" not in response.json()

    async def test_reveals_no_dependency_when_the_caller_is_anonymous(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """No dependency identifier appears anywhere in the anonymous body."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=None) as (
            _app,
            client,
        ):
            body = (await client.get(f"{_PREFIX}/{_AGENT}/health")).text

            assert "tools.internal" not in body
            assert "mcp:" not in body

    async def test_returns_checks_when_the_caller_is_authenticated(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """A verified caller gets the per-dependency breakdown."""
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_MCP_SERVER),))
        async with _serving(deps=deps, container=container, plans=(plan,), identity=identity) as (
            _app,
            client,
        ):
            response = await self._wait_for_status(client, f"{_PREFIX}/{_AGENT}/health", "ok")

            assert "checks" in response.json()

    async def test_responds_503_when_the_agent_is_unavailable(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """An ``unavailable`` aggregate is reported with a 503 status."""
        engine = ScriptedEngine(health_status="unavailable")
        async with _serving(
            deps=deps, container=container, identity=identity, engines={_AGENT: engine}
        ) as (_app, client):
            response = await self._wait_for_status(
                client, f"{_PREFIX}/{_AGENT}/health", "unavailable"
            )

            assert response.status_code == 503

    async def test_reports_probing_when_the_first_probe_has_not_finished(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """Before the first probe completes the status is ``degraded``/``probing``."""
        engine = ScriptedEngine(health_gate=asyncio.Event())
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            health_cache_ttl_ms=5000,
        ) as (_app, client):
            body = (await client.get(f"{_PREFIX}/{_AGENT}/health")).json()

            assert (body["status"], body["detail"]) == ("degraded", "probing")

    async def test_does_not_block_when_the_first_probe_has_not_finished(
        self, deps: StubDepsFactory, container: LoomContainer, identity: Identity
    ) -> None:
        """The scrape never waits on the probe: it answers from the cache."""
        engine = ScriptedEngine(health_gate=asyncio.Event())
        loop = asyncio.get_running_loop()
        async with _serving(
            deps=deps,
            container=container,
            identity=identity,
            engines={_AGENT: engine},
            health_cache_ttl_ms=5000,
        ) as (_app, client):
            started = loop.time()
            await client.get(f"{_PREFIX}/{_AGENT}/health")
            elapsed = loop.time() - started

            assert elapsed < 0.050, f"the health scrape blocked for {elapsed:.3f}s"


_APP_MODULE = "loom_ai_containment_fixture_app"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the ``loom.ai`` import-containment test."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class ContainmentRecord(BaseModel):
    __tablename__ = "ai_containment_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class ContainmentPingUseCase(UseCase[ContainmentRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class ContainmentPingInterface(RestInterface[str]):
    prefix = "/containment-ping"
    routes = (RestRoute(use_case=ContainmentPingUseCase, method="GET", path="/"),)
'''

_CONTAINMENT_SCRIPT = """
import sys

from loom.rest.fastapi.auto import create_app, describe_fastapi_app

app = create_app(sys.argv[1])
description = describe_fastapi_app(app)
if "agents" in description:
    raise SystemExit("an app with no ai: section described an 'agents' section")
leaked = sorted(name for name in sys.modules if name == "loom.ai" or name.startswith("loom.ai."))
if leaked:
    raise SystemExit("loom.ai leaked into an app with no ai: section: " + ", ".join(leaked))
"""


def test_does_not_import_loom_ai_when_the_config_has_no_ai_section(tmp_path: Path) -> None:
    """Neither building nor describing an app without ``ai:`` imports ``loom.ai`` (T098, SC-013).

    Covers both routes into the pillar — ``create_app`` and the
    post-construction ``describe_fastapi_app`` — and runs in a clean
    interpreter so modules imported by unrelated suites inside the pytest
    process cannot mask the leak.
    """
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    config = {
        "app": {
            "name": "ai-containment-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(_SRC), str(tmp_path)])}

    result = subprocess.run(
        [sys.executable, "-c", _CONTAINMENT_SCRIPT, str(config_path)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        cwd=str(tmp_path),
    )

    assert result.returncode == 0, result.stderr


_A2A_PREFIX = "/a2a"
_A2A_BASE_URL = "https://api.example.com"


class TestAuthenticationExclusions:
    """Only the card may be excluded from authentication (T141, FR-041b)."""

    @staticmethod
    async def _bind(
        *,
        deps: StubDepsFactory,
        container: LoomContainer,
        exclude_paths: Sequence[str],
    ) -> None:
        """Bind the A2A surface of one agent with the given exclusion list."""
        config = make_ai_config(
            endpoints={_AGENT: make_endpoint()},
            a2a=A2AConfig(base_url=_A2A_BASE_URL, expose=(_AGENT,)),
        )
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
            app.add_middleware(
                AuthenticationMiddleware,
                authenticator=StubAuthenticator(),  # type: ignore[arg-type]
                exclude_paths=tuple(exclude_paths),
            )
            bind_a2a_endpoints(
                app,
                runtime=runtime,
                config=config,
                plans=plans,
                authenticator=StubAuthenticator(),  # type: ignore[arg-type]
                exclude_paths=tuple(exclude_paths),
                prefix=_A2A_PREFIX,
            )

    @pytest.mark.parametrize(
        "excluded",
        [_A2A_PREFIX, f"{_A2A_PREFIX}/{_AGENT}", _PREFIX, f"{_PREFIX}/{_AGENT}/run"],
    )
    async def test_fails_to_start_when_the_exclusion_covers_an_invocation(
        self, deps: StubDepsFactory, container: LoomContainer, excluded: str
    ) -> None:
        """Any exclusion under the A2A or agents prefix other than a card aborts start-up."""
        with pytest.raises(AgentCompilationError) as raised:
            await self._bind(deps=deps, container=container, exclude_paths=(excluded,))

        assert raised.value.issues[0].code is AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS
        assert excluded in raised.value.issues[0].message

    async def test_starts_when_the_only_exclusion_is_the_card(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The card path is the one exclusion this surface accepts."""
        await self._bind(
            deps=deps,
            container=container,
            exclude_paths=(card_path(_AGENT, prefix=_A2A_PREFIX),),
        )

    async def test_starts_when_the_exclusion_does_not_touch_the_agents(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Exclusions outside both prefixes are none of this surface's business."""
        await self._bind(deps=deps, container=container, exclude_paths=("/health", "/docs"))
