"""Compose an application whose AI declarations live on adapters, with or without agents.

The engine provider is an in-process double counting the engines it builds,
and its MCP client factory counts the sessions it opens: nothing reaches the
network.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai import registry
from loom.composition import AppConfig, Application, Project, Recipe
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.callback import JobCallback
from loom.core.stores import SERVER_PROFILE
from loom.serverless.aws.lambda_ import LambdaAdapter
from tests.helpers.ai import CountingEngineProvider
from tests.helpers.composition import WithOverrides, composition_project
from tests.helpers.extras import patched_aws_clients
from tests.helpers.lambda_jobs import jobs_queue, jobs_trigger
from tests.integration.ai.adapters.doubles import (
    AGENT,
    SERVER,
    UNUSED_AGENT,
    AcmeAdvice,
    AcmeAnalystAdvice,
    AcmeGatewayQuotes,
    AcmeMarket,
    AcmeQuotes,
)
from tests.integration.ai.conftest import (
    CountingMcpClientFactory,
    RecordingMcpSession,
    StubMcpClient,
)

WORKER = "worker"
Compose = Callable[..., Application]


def _agent_spec(name: str) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "name": name,
        "description": f"The {name} of the acme market.",
        "instructions": "Answer with one word.",
        "output": {
            "kind": "json_schema",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["answer"],
                "properties": {"answer": {"type": "string", "description": "The word."}},
            },
        },
    }


class AgentlessHost:
    """A host that runs the given use cases and callbacks and hosts no agents, as a worker does."""

    def __init__(
        self, use_cases: tuple[type, ...], callbacks: tuple[type[JobCallback], ...] = ()
    ) -> None:
        self.use_cases, self.callbacks = use_cases, callbacks

    def recipe(self, project: Project) -> Recipe:
        del project
        discovered = DiscoveryResult(
            models=(),
            use_cases=self.use_cases,
            interfaces=(),
            jobs=(),
            callbacks=self.callbacks,
        )
        return Recipe(profile=SERVER_PROFILE, discovered=discovered, agents=False)

    def bind(self, application: Application) -> Application:
        return application


@pytest.fixture
def session() -> RecordingMcpSession:
    """The one ``pricing`` server session, publishing ``quote`` and ``history``."""
    return RecordingMcpSession(label=SERVER, tools=("quote", "history"))


@pytest.fixture
def sessions(session: RecordingMcpSession, lifecycle_log: list[str]) -> CountingMcpClientFactory:
    """The MCP client factory: each call opens one session, recorded by server name."""
    return CountingMcpClientFactory(
        {SERVER: StubMcpClient(label=SERVER, session=session, log=lifecycle_log)}
    )


@pytest.fixture
def engines(
    sessions: CountingMcpClientFactory, monkeypatch: pytest.MonkeyPatch
) -> CountingEngineProvider:
    """The engine provider ``ai.engine`` resolves to; ``calls`` names each agent built."""
    provider = CountingEngineProvider()
    provider.mcp_client_factory = sessions  # type: ignore[assignment]
    monkeypatch.setattr(registry, "resolve_engine_provider", lambda _: provider)
    return provider


@pytest.fixture
def project(tmp_path: Path) -> Iterator[Project]:
    """A Lambda jobs function ``worker`` with ``pricing`` configured and two agent artefacts.

    Its SQS client is a stand-in for the test's duration.
    """
    agents = tmp_path / "agents"
    agents.mkdir()
    for name in (AGENT, UNUSED_AGENT):
        (agents / f"{name}.yaml").write_text(yaml.safe_dump(_agent_spec(name)), encoding="utf-8")
    document = {
        "app": {"name": "acme-adapters"},
        "ai": {
            "engine": "fake",
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "specs": ["agents/*.yaml"],
            "mcp_servers": {SERVER: {"url": f"https://{SERVER}.example.com/mcp"}},
            "startup_timeout_ms": 2000,
            "health_cache_ttl_ms": 5,
        },
        **jobs_queue(),
        "lambda": {"functions": {WORKER: {"trigger": jobs_trigger()}}},
    }
    with patched_aws_clients():
        yield composition_project(
            ConfigContext.from_dict(document),
            app=AppConfig(name="acme-adapters", entry=WORKER, job_service="inline"),
            code_path=tmp_path,
            config_root=tmp_path,
        )


def binding(port: type, adapter: object) -> Callable[[LoomContainer], None]:
    """Bind *port* to *adapter*: a class to build, or an instance to hand out as is."""
    if isinstance(adapter, type):
        return lambda container: container.register(port, adapter)
    return lambda container: container.register_instance(port, adapter)


@pytest.fixture
def compose(project: Project, engines: CountingEngineProvider) -> Compose:
    """Compose the Lambda function ``worker`` (or *host*) over *use_cases* with the ports bound.

    ``quotes`` and ``advice`` default to the adapters declaring ``Mcp('pricing')``
    and ``Agent('analyst')``.
    """
    del engines

    def build(
        *use_cases: type,
        quotes: object = AcmeGatewayQuotes,
        advice: object = AcmeAnalystAdvice,
        host: object | None = None,
    ) -> Application:
        cases = use_cases or (AcmeMarket,)
        return project.compose(
            WithOverrides(
                host or LambdaAdapter(use_cases=cases),
                binding(AcmeQuotes, quotes),
                binding(AcmeAdvice, advice),
            )
        )

    return build
