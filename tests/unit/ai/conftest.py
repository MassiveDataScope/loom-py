"""Shared fixtures for the ``loom.ai`` unit tests."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from loom.ai.config import A2AAgentConfig, AgentEndpointConfig, AiConfig, McpServerConfig
from loom.ai.inference import InferenceTarget
from loom.core.model.redaction import Dsn, RedactedMap, Secret
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.use_case.registry import UseCaseRegistry


@pytest.fixture(autouse=True)
def _event_loop_before_socket_block(request: pytest.FixtureRequest) -> None:
    """Create the pytest-asyncio runner before module-level autouse fixtures.

    ``test_fake_engine.py`` blocks all socket creation with an autouse
    fixture, but the asyncio event loop itself opens one socketpair (its
    self-pipe) when it is created.  Conftest autouse fixtures resolve before
    module ones, so requesting the runner here creates the loop first and the
    network block then applies only to the code under test.
    """
    if "_function_scoped_runner" in request.fixturenames:
        request.getfixturevalue("_function_scoped_runner")


# ---------------------------------------------------------------------------
# Compiler-phase environment (US1).  Deliberately no module-level import of
# ``loom.ai.compiler``: a conftest ImportError would abort the whole session,
# while the red state must come from each test module's own import.
# ---------------------------------------------------------------------------

FAKE_PKGS_DIR = Path(__file__).parent / "fixtures" / "fake_pkgs"
CORPUS_DIR = Path(__file__).parent / "fixtures" / "corpus_v1"
SKILLS_ROOT_DIR = Path(__file__).parent / "fixtures" / "skills_root"

CORPUS_PATTERN = "*/agent.yaml"
"""Glob matching every corpus artifact: one directory per agent, always."""

_CORPUS_USECASE_KEYS: tuple[str, ...] = (
    "orders.get_order_status",
    "orders.request_refund",
    "customers.get_profile",
    "incidents.get_incident",
    "incidents.append_timeline_entry",
)


@pytest.fixture(scope="session")
def fake_myapp_path() -> Iterator[Path]:
    """Make the fake ``myapp`` package importable for corpus references."""
    path = str(FAKE_PKGS_DIR)
    sys.path.insert(0, path)
    try:
        yield FAKE_PKGS_DIR
    finally:
        sys.path.remove(path)
        for name in [mod for mod in sys.modules if mod.split(".")[0] == "myapp"]:
            del sys.modules[name]


_CORPUS_MCP_SERVERS: tuple[str, ...] = ("knowledge", "runbooks")
_CORPUS_A2A_AGENTS: tuple[str, ...] = ("translations", "oncall")

STDIO_MCP_SERVER = "filesystem"
"""The one ``transport: stdio`` server of the compiler environment."""


def _inference_target() -> InferenceTarget:
    """Offline-safe model binding: nothing in the provider requires settings."""
    return InferenceTarget(provider="fake", model="fake-model")


@pytest.fixture
def ai_config_factory() -> Callable[..., AiConfig]:
    """Build an ``AiConfig`` satisfying every corpus reference by default."""

    def _make(
        *,
        skills_root: str | None = str(SKILLS_ROOT_DIR),
        endpoints: dict[str, AgentEndpointConfig] | None = None,
        mcp_servers: dict[str, McpServerConfig] | None = None,
        a2a_agents: dict[str, A2AAgentConfig] | None = None,
    ) -> AiConfig:
        return AiConfig(
            engine="fake",
            specs=("ai/agents/*/agent.yaml",),
            models={"default": _inference_target(), "reasoning": _inference_target()},
            skills_root=skills_root,
            mcp_servers=mcp_servers if mcp_servers is not None else _corpus_mcp_servers(),
            a2a_agents=a2a_agents if a2a_agents is not None else _corpus_a2a_agents(),
            endpoints=endpoints if endpoints is not None else {},
        )

    return _make


def _corpus_mcp_servers() -> dict[str, McpServerConfig]:
    """Locate every MCP server the corpus names, plus one subprocess server.

    Artifacts carry no URL or command.  ``env`` is declared out of key order
    on purpose: the plan must carry it sorted, whatever the operator wrote.
    """
    servers = {
        name: McpServerConfig(
            url=Dsn(f"https://{name}.example.com/mcp"),
            headers_ref=Secret(f"{name}_server_headers"),
        )
        for name in _CORPUS_MCP_SERVERS
    }
    servers[STDIO_MCP_SERVER] = McpServerConfig(
        transport="stdio",
        command="npx",
        args=("-y", "@modelcontextprotocol/server-filesystem", "/srv/data"),
        env=RedactedMap({"ROOT_DIR": "/srv/data", "LOG_LEVEL": "warn"}),
    )
    return servers


def _corpus_a2a_agents() -> dict[str, A2AAgentConfig]:
    """Locate every remote agent the corpus names; artifacts carry no URL."""
    return {
        name: A2AAgentConfig(
            url=Dsn(f"https://{name}.example.com/a2a"),
            headers_ref=Secret(f"{name}_agent_headers"),
        )
        for name in _CORPUS_A2A_AGENTS
    }


@pytest.fixture
def compiler_env_config(ai_config_factory: Callable[..., AiConfig]) -> AiConfig:
    """Default AI config for compiler tests (roles ``default`` and ``reasoning``)."""
    return ai_config_factory()


def _connection(
    *,
    readonly: bool = True,
    allowed_roles: tuple[str, ...] = (),
) -> SqlConnectionConfig:
    return SqlConnectionConfig(
        backend="clickhouse",
        url=Dsn("clickhouse://reports.internal:8123/reporting"),
        allowed_roles=allowed_roles,
        readonly=readonly,
    )


@pytest.fixture
def compiler_env_sql() -> SqlConfig:
    """SQL config with the corpus connections plus deliberately unsafe ones."""
    return SqlConfig(
        connections={
            "reporting_readonly": _connection(),
            "observability_readonly": _connection(),
            "writable": _connection(readonly=False),
            "roles_menu": _connection(allowed_roles=("analyst", "auditor")),
        }
    )


@pytest.fixture
def compiler_env_registry() -> UseCaseRegistry:
    """Registry resolving every use-case key the corpus grants."""
    by_name: dict[str, type] = {
        key: type(f"UseCase{index}", (), {}) for index, key in enumerate(_CORPUS_USECASE_KEYS)
    }
    return UseCaseRegistry(by_name, {value: key for key, value in by_name.items()})
