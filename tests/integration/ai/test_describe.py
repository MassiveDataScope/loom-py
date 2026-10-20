"""Self-description of a live application (US7, T152-T153).

Drives :func:`loom.rest.fastapi.auto.create_app` over a temporary project
whose ``ai:`` section declares three agents, then asks the resulting app to
describe itself through ``describe_fastapi_app``.  Two properties are under
test:

* completeness — every declared agent appears with its published projection
  and the capabilities it was actually granted (T152);
* containment — no secret-bearing value of the deployment configuration nor
  any private field of the artifact reaches the description (T153, FR-054,
  SC-013).

The engine entry point is faked in-process and the app lifespan is never
entered: no provider SDK, no network, no credential.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi import FastAPI

from loom.ai.describe import AGENTS_CONTRIBUTOR, AGENTS_SECTION, describe_agents
from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import (
    _AGENTS_CONTRIBUTOR,
    _AGENTS_SECTION,
    create_app,
    describe_fastapi_app,
)
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.conftest import CountingEngineProvider

_APP_MODULE = "loom_aidescribe_fixture_app"
_ENGINE_NAME = "aidescribe-fake"
_GROUP = "loom.ai.engines"

_USECASE_KEY = "describe.ping"
_MCP_SERVER = "knowledge"

_AGENT_USECASE = "describe-usecase-agent"
_AGENT_MCP = "describe-mcp-agent"
_AGENT_PLAIN = "describe-plain-agent"
_AGENTS: tuple[str, ...] = (_AGENT_MCP, _AGENT_PLAIN, _AGENT_USECASE)

_APP_NAME = "aidescribe-demo"
_APP_VERSION = "9.9.9"

# ---------------------------------------------------------------------------
# Canaries: unlikely, unique strings seeded in every place the description
# must never reach.  A canary in the serialised output is a leak, full stop.
# ---------------------------------------------------------------------------

_CANARY_INSTRUCTIONS = "canary-instructions-zq7x"
_CANARY_METADATA = "canary-metadata-zq7x"
_CANARY_PROVIDER = "canaryprovider-zq7x"
_CANARY_MODEL = "canary-model-zq7x"
_CANARY_REGION = "canary-region-zq7x"
_CANARY_ENDPOINT = "https://canary-endpoint-zq7x.internal/v1"
_CANARY_CREDENTIALS_REF = "canary-credentials-ref-zq7x"
_CANARY_MCP_URL = "https://canary-mcp-url-zq7x.internal/mcp"
_CANARY_MCP_HEADERS_REF = "canary-mcp-headers-ref-zq7x"
_CANARY_A2A_URL = "https://canary-a2a-url-zq7x.internal/a2a"
_CANARY_A2A_HEADERS_REF = "canary-a2a-headers-ref-zq7x"

_CANARIES: tuple[str, ...] = (
    _CANARY_INSTRUCTIONS,
    _CANARY_METADATA,
    _CANARY_PROVIDER,
    _CANARY_MODEL,
    _CANARY_REGION,
    _CANARY_ENDPOINT,
    _CANARY_CREDENTIALS_REF,
    _CANARY_MCP_URL,
    _CANARY_MCP_HEADERS_REF,
    _CANARY_A2A_URL,
    _CANARY_A2A_HEADERS_REF,
)

_FORBIDDEN_KEYS: tuple[str, ...] = (
    "instructions",
    "inference",
    "credentials_ref",
    "headers_ref",
    "url",
    "decoder",
    "metadata",
)

_APP_SOURCE = '''\
"""Minimal discoverable app used by the self-description tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class DescribeRecord(BaseModel):
    __tablename__ = "aidescribe_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


@use_case_key("describe.ping")
class DescribePingUseCase(UseCase[DescribeRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class DescribePingInterface(RestInterface[str]):
    prefix = "/aidescribe-ping"
    routes = (RestRoute(use_case=DescribePingUseCase, method="GET", path="/"),)
'''


def _output_schema(field: str) -> dict[str, Any]:
    """Build a JSON schema whose property names never collide with a forbidden key."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [field],
        "properties": {field: {"type": "string", "description": "Short answer."}},
    }


_AGENT_SPECS: Mapping[str, dict[str, Any]] = {
    _AGENT_USECASE: {
        "spec_version": 1,
        "name": _AGENT_USECASE,
        "description": "Runs the granted business operation and reports its outcome.",
        "instructions": f"Follow the internal playbook {_CANARY_INSTRUCTIONS} verbatim.",
        "output": {"kind": "json_schema", "schema": _output_schema("resolution")},
        "capabilities": [{"kind": "usecase", "keys": [_USECASE_KEY]}],
        "metadata": {"owner": _CANARY_METADATA},
    },
    _AGENT_MCP: {
        "spec_version": 1,
        "name": _AGENT_MCP,
        "description": "Enriches a ticket with material from the knowledge server.",
        "instructions": f"Search only what {_CANARY_INSTRUCTIONS} allows.",
        "output": {"kind": "json_schema", "schema": _output_schema("summary")},
        "capabilities": [
            {
                "kind": "mcp",
                "server": _MCP_SERVER,
                "include": ["search_*"],
                "exclude": ["delete_*"],
            }
        ],
        "metadata": {"owner": _CANARY_METADATA},
    },
    _AGENT_PLAIN: {
        "spec_version": 1,
        "name": _AGENT_PLAIN,
        "description": "Answers plain product questions with a short summary.",
        "instructions": f"Answer using the conversation only, per {_CANARY_INSTRUCTIONS}.",
        "output": {"kind": "json_schema", "schema": _output_schema("answer")},
        "policies": {
            "retries": 2,
            "tool_timeout_ms": 1500,
            "max_iterations": 5,
            "run_timeout_ms": 9000,
        },
        "metadata": {"owner": _CANARY_METADATA},
    },
}

_PLAIN_POLICIES: Mapping[str, int | str | None] = {
    "retries": 2,
    "tool_timeout_ms": 1500,
    "max_iterations": 5,
    "run_timeout_ms": 9000,
    "max_history_bytes": 1048576,
    "max_usd": None,
    "max_total_tokens": None,
    "max_input_tokens_per_request": None,
    "max_tool_calls": None,
    "max_requests": 50,
    "on_unpriced_spend": "serve",
}


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Entry point resolving the fake engine provider of these tests."""

    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-aidescribe-tests")

    def load(self) -> object:
        """Return the provider class the registry instantiates."""
        return CountingEngineProvider


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to."""
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )


def _write_app_module(tmp_path: Path) -> None:
    """Write the discoverable fixture application module."""
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")


def _base_config(tmp_path: Path) -> dict[str, Any]:
    """Build the configuration shared by the described and undescribed apps."""
    return {
        "app": {
            "name": _APP_NAME,
            "code_path": str(tmp_path),
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
            "rest": {"version": _APP_VERSION},
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }


def _write_project(tmp_path: Path) -> str:
    """Write the fixture app, three agent artifacts and the YAML config.

    Every secret-bearing value of the ``ai:`` section is a canary, so a leak
    into the self-description is detectable by a plain substring search.

    Returns:
        Path of the written configuration file.
    """
    _write_app_module(tmp_path)
    for name, spec in _AGENT_SPECS.items():
        agent_dir = tmp_path / "ai" / "agents" / name
        agent_dir.mkdir(parents=True)
        (agent_dir / "agent.yaml").write_text(yaml.safe_dump(spec), encoding="utf-8")
    config = _base_config(tmp_path)
    config["ai"] = {
        "engine": _ENGINE_NAME,
        "specs": ["ai/agents/*/agent.yaml"],
        "models": {
            "default": {
                "provider": _CANARY_PROVIDER,
                "model": _CANARY_MODEL,
                "region": _CANARY_REGION,
                "endpoint": _CANARY_ENDPOINT,
                "credentials_ref": _CANARY_CREDENTIALS_REF,
            }
        },
        "mcp_servers": {
            _MCP_SERVER: {
                "url": _CANARY_MCP_URL,
                "headers_ref": _CANARY_MCP_HEADERS_REF,
            }
        },
        "a2a_agents": {
            "oncall": {
                "url": _CANARY_A2A_URL,
                "headers_ref": _CANARY_A2A_HEADERS_REF,
            }
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _write_project_without_ai(tmp_path: Path) -> str:
    """Write the same application with no ``ai:`` section at all."""
    _write_app_module(tmp_path)
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(_base_config(tmp_path)), encoding="utf-8")
    return str(config_path)


def _agents_by_name(description: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Index the described agents by name."""
    agents: Sequence[Mapping[str, Any]] = description["agents"]
    return {str(agent["name"]): agent for agent in agents}


def _capability(agent: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    """Return the single described capability of ``agent`` with the given kind."""
    matches = [item for item in agent["capabilities"] if item["kind"] == kind]
    if len(matches) != 1:
        raise AssertionError(f"expected exactly one {kind!r} capability, got {len(matches)}")
    return matches[0]


def _normalise(value: Any) -> Any:
    """Normalise sequences to tuples so list/tuple projections compare equal."""
    if isinstance(value, Mapping):
        return {key: _normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_normalise(item) for item in value)
    return value


def _walk_keys(value: Any) -> Iterator[str]:
    """Yield every mapping key appearing anywhere inside ``value``."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _walk_keys(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_keys(item)


@pytest.fixture
def described_app(tmp_path: Path, fake_engine: None) -> FastAPI:
    """Build the three-agent application whose description is under test."""
    del fake_engine
    return create_app(_write_project(tmp_path))


@pytest.fixture
def description(described_app: FastAPI) -> dict[str, Any]:
    """Self-description of the three-agent application."""
    return describe_fastapi_app(described_app)


class TestFullDescription:
    """Every declared agent is described with what it was really granted (T152)."""

    def test_publishes_the_apps_identity_when_there_are_agents(
        self, description: dict[str, Any]
    ) -> None:
        """The ``app`` section carries the configured name and REST version."""
        assert description["app"] == {"name": _APP_NAME, "version": _APP_VERSION}

    def test_lists_all_three_agents_when_all_three_are_declared(
        self, description: dict[str, Any]
    ) -> None:
        """No declared artifact is missing from the description."""
        assert sorted(_agents_by_name(description)) == sorted(_AGENTS)

    def test_publishes_the_declared_description_when_describing_an_agent(
        self, description: dict[str, Any]
    ) -> None:
        """``description`` is the author's public sentence, published verbatim."""
        agent = _agents_by_name(description)[_AGENT_PLAIN]

        assert agent["description"] == _AGENT_SPECS[_AGENT_PLAIN]["description"]

    def test_publishes_the_spec_version_when_describing_an_agent(
        self, description: dict[str, Any]
    ) -> None:
        """``spec_version`` is the artifact format each agent compiled from."""
        agents = _agents_by_name(description)

        assert [agents[name]["spec_version"] for name in sorted(_AGENTS)] == [1, 1, 1]

    def test_publishes_the_output_schema_when_describing_an_agent(
        self, description: dict[str, Any]
    ) -> None:
        """``output_schema`` is the contract callers code against."""
        agent = _agents_by_name(description)[_AGENT_USECASE]

        assert _normalise(agent["output_schema"]) == _normalise(_output_schema("resolution"))

    def test_publishes_the_policies_when_the_artifact_declares_them(
        self, description: dict[str, Any]
    ) -> None:
        """The declared execution limits are published as they compiled."""
        agent = _agents_by_name(description)[_AGENT_PLAIN]

        assert dict(agent["policies"]) == dict(_PLAIN_POLICIES)

    def test_publishes_the_provenance_when_the_artifact_comes_from_a_file(
        self, description: dict[str, Any]
    ) -> None:
        """``source_path`` names the artifact the agent was compiled from."""
        agent = _agents_by_name(description)[_AGENT_MCP]

        assert str(agent["source_path"]).endswith(f"{_AGENT_MCP}/agent.yaml")

    def test_publishes_the_granted_keys_when_the_capability_is_usecase(
        self, description: dict[str, Any]
    ) -> None:
        """A ``usecase`` grant publishes the keys, never the resolved types."""
        capability = _capability(_agents_by_name(description)[_AGENT_USECASE], "usecase")

        assert _normalise(capability["settings"]) == {"keys": (_USECASE_KEY,)}

    def test_publishes_the_server_and_filter_when_the_capability_is_mcp(
        self, description: dict[str, Any]
    ) -> None:
        """An ``mcp`` grant publishes the server name and its filter, never the URL."""
        capability = _capability(_agents_by_name(description)[_AGENT_MCP], "mcp")

        assert _normalise(capability["settings"]) == {
            "server": _MCP_SERVER,
            "transport": "http",
            "include": ("search_*",),
            "exclude": ("delete_*",),
            "timeout_ms": 20000,
        }

    def test_lists_no_capabilities_when_the_agent_declares_none(
        self, description: dict[str, Any]
    ) -> None:
        """An agent with no grant is described with an empty capability list."""
        agent = _agents_by_name(description)[_AGENT_PLAIN]

        assert tuple(agent["capabilities"]) == ()


class TestSecretsContainment:
    """The description leaks neither configuration secrets nor private fields."""

    def test_leaks_no_canary_when_the_description_is_serialized(
        self, description: dict[str, Any]
    ) -> None:
        """Not one seeded canary survives into the serialised description."""
        payload = json.dumps(description, default=str)

        leaked = [canary for canary in _CANARIES if canary in payload]
        assert leaked == []

    def test_exposes_no_private_key_when_the_description_is_walked(
        self, description: dict[str, Any]
    ) -> None:
        """No excluded key appears at any depth of the description."""
        present = set(_walk_keys(description))

        assert sorted(present & set(_FORBIDDEN_KEYS)) == []


class TestWiringIsReachable:
    """The entry point is reachable from the pillar and works without agents."""

    def test_is_exported_from_the_rest_pillar_when_loom_rest_fastapi_is_imported(self) -> None:
        """``describe_fastapi_app`` is public API of ``loom.rest.fastapi``."""
        import loom.rest.fastapi as rest_fastapi

        assert rest_fastapi.describe_fastapi_app is describe_fastapi_app

    def test_is_declared_in_all_when_loom_rest_fastapi_is_imported(self) -> None:
        """The re-export is declared, not merely reachable."""
        import loom.rest.fastapi as rest_fastapi

        assert "describe_fastapi_app" in rest_fastapi.__all__

    def test_describes_the_app_when_there_is_no_ai_section(self, tmp_path: Path) -> None:
        """An application without agents still describes its identity."""
        app = create_app(_write_project_without_ai(tmp_path))

        assert describe_fastapi_app(app) == {"app": {"name": _APP_NAME, "version": _APP_VERSION}}

    def test_publishes_no_agents_when_there_is_no_ai_section(self, tmp_path: Path) -> None:
        """The ``agents`` section is absent, not empty, when no pillar is wired."""
        app = create_app(_write_project_without_ai(tmp_path))

        assert "agents" not in describe_fastapi_app(app)


class TestContributorContract:
    """The wiring literals of ``auto`` and the pillar's own must not drift."""

    # 'auto' restates these literals instead of importing them: importing
    # 'loom.ai.describe' there would pull the AI pillar into every app (FR-050).

    def test_the_section_declared_by_auto_matches_the_pillars_own(self) -> None:
        """Both sides name the same document section."""
        assert _AGENTS_SECTION == AGENTS_SECTION

    def test_the_contributor_declared_by_auto_matches_the_pillars_own(self) -> None:
        """Both sides name the same ``module:callable`` reference."""
        assert _AGENTS_CONTRIBUTOR == AGENTS_CONTRIBUTOR

    def test_resolves_to_describe_agents_when_the_reference_is_imported(self) -> None:
        """The reference resolves to the pillar's contributor, not to a stale name."""
        module_name, _, attribute = _AGENTS_CONTRIBUTOR.partition(":")

        assert getattr(import_module(module_name), attribute) is describe_agents
