"""What a Lambda function refuses of the ``ai`` section, and the agent runtime it hosts.

A Lambda function hosts agents for its use cases, reached through ``Agent()``
and ``Mcp()`` markers, never over HTTP endpoints. It refuses only what it
cannot run: an MCP server spoken to over stdio, a subprocess a frozen
function cannot keep.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any

import pytest
import yaml

from loom.ai import registry
from loom.ai.runtime import AgentRuntime
from loom.composition import AppConfig, CompositionSource, compose_application, create_app
from loom.composition.build import load_source
from loom.core.config import ConfigContext
from loom.core.job.job import Job
from loom.rest.fastapi import FastAPIAdapter
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaUnservedConfigError
from loom.serverless.aws.lambda_.triggers.http import LambdaHTTPHandler
from tests.helpers.ai import CountingEngineProvider
from tests.helpers.composition import composition_source
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import WORKER, make_record, with_worker
from tests.unit.serverless.aws.lambda_.http._clients import lambda_client
from tests.unit.serverless.aws.lambda_.http._contract_app import write_contract_project

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
]

_TEST = "test"
_AGENT = "triage"
_AGENTS_GLOB = "agents/*.yaml"
_AGENT_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "name": _AGENT,
    "description": "Sorts an incoming ticket into a queue.",
    "instructions": "Answer with the queue the ticket belongs to.",
    "output": {
        "kind": "json_schema",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["answer"],
            "properties": {"answer": {"type": "string", "description": "The queue."}},
        },
    },
}
_MODELS = {"default": {"provider": "fake", "model": "fake-model"}}
_AI: dict[str, Any] = {"engine": "fake", "models": _MODELS}
_STDIO_AI: dict[str, Any] = {
    **_AI,
    "mcp_servers": {"x": {"transport": "stdio", "command": "mcp-server"}},
}
_HTTP_AI: dict[str, Any] = {
    **_AI,
    "mcp_servers": {"x": {"transport": "http", "url": "https://mcp.example.com/mcp"}},
}
_MISSING_MANIFEST = "tests.unit.serverless.aws.lambda_.ai._no_such_manifest"
_UNDISCOVERABLE_APP = {
    "name": "lambda-ai",
    "discovery": {"mode": "interfaces", "interfaces": {"modules": ["no_such_ai_module"]}},
}

ProbeFixture = tuple[type[Job[None]], Probe]


def _jobs_source(tmp_path: Path, manifest: str, ai: dict[str, Any]) -> CompositionSource:
    """A ``jobs`` function discovering from *manifest*, with the ``ai`` section *ai*."""
    discovery = {"mode": "manifest", "manifest": {"module": manifest}}
    document = with_worker({"app": {"name": "lambda-ai", "discovery": discovery}, "ai": ai})
    return composition_source(
        ConfigContext.from_dict(document),
        app=AppConfig(name="lambda-ai", entry=WORKER),
        code_path=tmp_path,
        config_root=tmp_path,
    )


def _http_source(ai: dict[str, Any]) -> CompositionSource:
    """An ``http`` function whose discovery would fail, with the ``ai`` section *ai*."""
    return composition_source(ConfigContext.from_dict({"app": _UNDISCOVERABLE_APP, "ai": ai}))


def _write_agent(code_path: Path) -> None:
    agents = code_path / "agents"
    agents.mkdir(exist_ok=True)
    (agents / f"{_AGENT}.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")


@pytest.fixture
def jobs_manifest(probe: ProbeFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> str:
    """A manifest declaring the probe job and the agents under ``agents/``; its module name."""
    manifest = ModuleType("tests.unit.serverless.aws.lambda_.ai._agents_manifest")
    manifest.JOBS = [probe[0]]  # type: ignore[attr-defined]
    manifest.AGENTS = [_AGENTS_GLOB]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, manifest.__name__, manifest)
    _write_agent(tmp_path)
    return manifest.__name__


@pytest.fixture
def lifetime(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every entry into and exit from an :class:`AgentRuntime`, in order.

    The engine provider is an in-process double, so the runtime opens no
    network client.
    """
    events: list[str] = []
    enter, exit_ = AgentRuntime.__aenter__, AgentRuntime.__aexit__

    async def entered(self: AgentRuntime) -> AgentRuntime:
        events.append("enter")
        return await enter(self)

    async def exited(
        self: AgentRuntime,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        events.append("exit")
        await exit_(self, exc_type, exc, tb)

    monkeypatch.setattr(registry, "resolve_engine_provider", lambda _: CountingEngineProvider())
    monkeypatch.setattr(AgentRuntime, "__aenter__", entered)
    monkeypatch.setattr(AgentRuntime, "__aexit__", exited)
    return events


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(lambda tmp_path: _http_source(_STDIO_AI), id="http"),
        pytest.param(
            lambda tmp_path: _jobs_source(tmp_path, _MISSING_MANIFEST, _STDIO_AI), id="jobs"
        ),
    ],
)
def test_a_stdio_mcp_server_is_refused_before_discovery(
    tmp_path: Path, source: Callable[[Path], CompositionSource]
) -> None:
    adapter = LambdaAdapter()

    with pytest.raises(LambdaUnservedConfigError) as raised:
        adapter.recipe(source(tmp_path))

    assert raised.value.key == "ai.mcp_servers.x.transport"
    assert "stdio" in str(raised.value)


def test_a_jobs_function_hosts_agents_without_their_endpoints(
    tmp_path: Path, jobs_manifest: str
) -> None:
    adapter = LambdaAdapter()

    recipe = adapter.recipe(_jobs_source(tmp_path, jobs_manifest, _HTTP_AI))

    assert (recipe.agents, recipe.agent_endpoints) == (True, False)
    assert recipe.discovered.agent_specs == (_AGENTS_GLOB,)


@pytest.mark.parametrize(
    ("adapter", "endpoints"),
    [
        pytest.param(LambdaAdapter, False, id="lambda"),
        pytest.param(FastAPIAdapter, True, id="fastapi"),
    ],
)
def test_only_fastapi_serves_agent_endpoints(
    tmp_path: Path, adapter: Callable[[], Any], endpoints: bool
) -> None:
    _write_agent(tmp_path)
    config = write_contract_project(tmp_path, agents=(_AGENTS_GLOB,), ai=_AI)
    source = load_source(
        config, config_dir=None, environment=_TEST, code_path=None, resolvers=(), caller=_TEST
    )

    recipe = adapter().recipe(source)

    assert (recipe.agents, recipe.agent_endpoints) == (True, endpoints)
    assert recipe.discovered.agent_specs == (_AGENTS_GLOB,)


def test_a_jobs_function_enters_the_agent_runtime_once_at_init(
    tmp_path: Path, jobs_manifest: str, probe: ProbeFixture, lifetime: list[str]
) -> None:
    adapter = LambdaAdapter()
    application = compose_application(_jobs_source(tmp_path, jobs_manifest, _AI), adapter)

    handler = adapter.bind(application)
    assert lifetime == ["enter"]
    for message_id in ("1", "2"):
        record = make_record(message_id, "a", job_name=probe[0].__qualname__)
        assert handler({"Records": [record]}, None) == {"batchItemFailures": []}
    assert lifetime == ["enter"]
    adapter.close()

    assert lifetime == ["enter", "exit"]
    assert [seen[0] for seen in probe[1].seen] == ["a", "a"]


def test_an_http_function_enters_the_agent_runtime_once_at_init(
    tmp_path: Path, lifetime: list[str]
) -> None:
    _write_agent(tmp_path)
    config = write_contract_project(tmp_path, agents=(_AGENTS_GLOB,), ai=_AI)

    handler = create_app(config, adapter=LambdaAdapter(), environment=_TEST)
    assert isinstance(handler, LambdaHTTPHandler)
    assert lifetime == ["enter"]
    client = lambda_client(handler)
    statuses = [client("GET", "/items/1").status for _ in range(2)]
    assert lifetime == ["enter"]
    handler.close()

    assert lifetime == ["enter", "exit"]
    assert statuses == [401, 401]
