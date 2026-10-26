"""The artifact names an endpoint; the deployment says how it authenticates.

This module pins the property that makes the whole design worth having: the
same artifact compiles unchanged whether the MCP server or the remote A2A agent
it names needs no credential, a fixed header, or a named strategy. Only the
plan differs, because only the deployment differs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.ai.compiler import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledMcpCapability,
    CompiledRemoteAuth,
)
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.declarative import A2ACapability, AgentSpecV1, McpCapability
from loom.core.model.redaction import Dsn, RedactedMap, Secret


def _remote_settings(settings: dict[str, Any]) -> dict[str, Any]:
    """Wrap a header reference and an auth block before the struct stores them."""
    prepared = dict(settings)
    headers_ref = prepared.get("headers_ref")
    if isinstance(headers_ref, str):
        prepared["headers_ref"] = Secret(headers_ref)
    auth = prepared.get("auth")
    if isinstance(auth, dict):
        prepared["auth"] = RedactedMap(auth)
    return prepared

_SERVER = "knowledge"
_URL = "https://knowledge.example.com/mcp"
_AGENT = "market"
_AGENT_URL = "https://market.example.com/a2a"


def _mcp_capability(plan: AgentPlan) -> CompiledMcpCapability:
    """Return the single MCP grant of a compiled plan."""
    capability = next(c for c in plan.capabilities if c.kind == "mcp")
    assert isinstance(capability, CompiledMcpCapability)
    return capability


@pytest.fixture
def artifact(spec_factory: Callable[..., AgentSpecV1]) -> AgentSpecV1:
    """One artifact, reused across every deployment: it names the server only."""
    return spec_factory(capabilities=(McpCapability(server=_SERVER),))


@pytest.fixture
def config_naming(ai_config_factory: Callable[..., AiConfig]) -> Callable[..., AiConfig]:
    """Build a deployment whose single MCP server is configured as asked."""

    def _make(**server_settings: Any) -> AiConfig:
        return ai_config_factory(
            mcp_servers={
                _SERVER: McpServerConfig(url=Dsn(_URL), **_remote_settings(server_settings))
            }
        )

    return _make


class TestTheArtifactDoesNotChangeAcrossEnvironments:
    """Criterion: one artifact, three deployments, three clean compilations."""

    @pytest.mark.parametrize(
        "server_settings",
        [
            {},
            {"headers_ref": "X-API-Key=abc123"},
            {"auth": {"kind": "oauth"}},
        ],
        ids=["no_credential", "headers_ref", "strategy"],
    )
    def test_compiles_the_same_artifact_regardless_of_the_credential(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
        server_settings: dict[str, Any],
    ) -> None:
        plan = plan_for(artifact, config=config_naming(**server_settings))

        assert _mcp_capability(plan).server == _SERVER


class TestThePlanCarriesTheResolvedCredential:
    """The name dies at compile: the plan carries the strategy, not the lookup."""

    def test_carries_no_auth_when_the_server_declares_none(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(artifact, config=config_naming())

        assert _mcp_capability(plan).auth is None

    def test_separates_kind_from_its_settings_when_the_server_declares_a_strategy(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        """The engine must never have to dig ``kind`` out of the settings again."""
        config = config_naming(
            auth={"kind": "static", "headers_ref": "X-API-Key=abc123"},
        )

        plan = plan_for(artifact, config=config)

        assert _mcp_capability(plan).auth == CompiledRemoteAuth(
            kind="static", settings=(("headers_ref", "X-API-Key=abc123"),)
        )

    def test_carries_headers_ref_when_the_server_declares_it(
        self,
        artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(artifact, config=config_naming(headers_ref="X-API-Key=abc123"))

        assert _mcp_capability(plan).headers_ref == "X-API-Key=abc123"


@pytest.fixture
def a2a_artifact(spec_factory: Callable[..., AgentSpecV1]) -> AgentSpecV1:
    """One artifact, reused across every deployment: it names the remote agent only."""
    return spec_factory(capabilities=(A2ACapability(agent=_AGENT),))


@pytest.fixture
def config_naming_agent(ai_config_factory: Callable[..., AiConfig]) -> Callable[..., AiConfig]:
    """Build a deployment whose single remote agent is configured as asked."""

    def _make(**agent_settings: Any) -> AiConfig:
        return ai_config_factory(
            a2a_agents={
                _AGENT: A2AAgentConfig(url=Dsn(_AGENT_URL), **_remote_settings(agent_settings))
            }
        )

    return _make


def _a2a_capability(plan: AgentPlan) -> CompiledA2ACapability:
    """Return the single A2A grant of a compiled plan."""
    capability = next(c for c in plan.capabilities if c.kind == "a2a")
    assert isinstance(capability, CompiledA2ACapability)
    return capability


class TestTheA2AArtifactDoesNotChangeAcrossEnvironments:
    """Criterion 8: one artifact, three deployments, three clean compilations."""

    @pytest.mark.parametrize(
        "agent_settings",
        [
            {},
            {"headers_ref": "X-API-Key=abc123"},
            {"auth": {"kind": "bearer", "token_ref": "a.b-c_1"}},
        ],
        ids=["no_credential", "headers_ref", "strategy"],
    )
    def test_compiles_the_same_artifact_regardless_of_the_credential(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
        agent_settings: dict[str, Any],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent(**agent_settings))

        assert _a2a_capability(plan).agent == _AGENT


class TestTheA2APlanCarriesTheResolvedCredential:
    """The strategy name dies at compile: the engine never re-reads configuration."""

    def test_carries_no_auth_when_the_agent_declares_none(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent())

        assert _a2a_capability(plan).auth is None

    def test_separates_kind_from_its_settings_when_the_agent_declares_a_strategy(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        config = config_naming_agent(auth={"kind": "bearer", "token_ref": "a.b-c_1"})

        plan = plan_for(a2a_artifact, config=config)

        assert _a2a_capability(plan).auth == CompiledRemoteAuth(
            kind="bearer", settings=(("token_ref", "a.b-c_1"),)
        )

    def test_carries_headers_ref_when_the_agent_declares_it(
        self,
        a2a_artifact: AgentSpecV1,
        plan_for: Callable[..., AgentPlan],
        config_naming_agent: Callable[..., AiConfig],
    ) -> None:
        plan = plan_for(a2a_artifact, config=config_naming_agent(headers_ref="X-API-Key=abc123"))

        assert _a2a_capability(plan).headers_ref == "X-API-Key=abc123"
