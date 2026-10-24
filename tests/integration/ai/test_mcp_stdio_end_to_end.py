"""End-to-end MCP over stdio: a real child process, filtered, called and reaped.

The HTTPS module proves the protocol; this one proves the transport. Both drive
the real compiler, the real provider and the real client factory, so what is
under test is loom's wiring and not a stub of it.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, McpCapability
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model.redaction import RedactedMap
from loom.core.use_case.registry import UseCaseRegistry
from tests.integration.ai.conftest import CANARY, CapabilityDepsFactory, ScriptedToolModel, _until

pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

_AGENT = "order-clerk"
_SERVER = "orders"
_PROMPT = "What has acme ordered?"
_SCRIPT = str(Path(__file__).parent / "mcp_stdio_server.py")

_ANSWER_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}


def _config(
    pid_file: Path,
    *,
    command: str = sys.executable,
    env: dict[str, str] | None = None,
    remote_clients: str = "required",
) -> AiConfig:
    """Deployment configuration registering the child process under its name."""
    return AiConfig(
        engine="pydantic-ai",
        specs=(),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        mcp_servers={
            _SERVER: McpServerConfig(
                transport="stdio",
                command=command,
                args=(_SCRIPT, str(pid_file), CANARY),
                env=None if env is None else RedactedMap(env),
            )
        },
        remote_clients=remote_clients,
        startup_timeout_ms=30000,
        health_cache_ttl_ms=5000,
    )


def _compile(config: AiConfig, capability: McpCapability) -> AgentPlan:
    """Compile one artifact through the real compiler and the real provider kinds."""
    spec = AgentSpecV1(
        spec_version=1,
        name=_AGENT,
        description="Reads the order server on behalf of the caller.",
        instructions="Answer using only the granted tools.",
        output=JsonSchemaOutput(schema=dict(_ANSWER_SCHEMA)),
        capabilities=(capability,),
    )
    compiler = AgentCompiler(
        config=config,
        registry=UseCaseRegistry.build([]),
        supported_kinds=PydanticAIEngineProvider().supported_capability_kinds(),
    )
    return compiler.compile(spec, source_path=f"ai/agents/{_AGENT}/agent.yaml")


def _runtime(config: AiConfig, plan: AgentPlan, model: ScriptedToolModel) -> Any:
    """Build the runtime the composition root builds, with the real MCP factory."""
    from loom.ai.runtime import AgentRuntime

    provider = PydanticAIEngineProvider(model_resolver=lambda target: model.as_model())
    return AgentRuntime(
        plans=[plan],
        config=config,
        engine_provider=provider,
        deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
        container=LoomContainer(),
        # Read off the provider, as ``create_app`` does: the start-up session
        # and the run's toolset are then one shared connection per server.
        mcp_client_factory=provider.mcp_client_factory,
    )


def _wait_until_gone(pid: int, *, timeout_s: float = 5.0) -> bool:
    """Return whether the process is gone before the deadline."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


class TestStdioServer:
    """The declared command is spawned, filtered, called and reaped."""

    async def test_only_the_included_tool_reaches_the_model_when_the_grant_filters(
        self, tmp_path: Path, caller: Identity
    ) -> None:
        """``include: [read_*]`` leaves the writing tool unreachable by the model."""
        config = _config(tmp_path / "pid")
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plan = _compile(config, McpCapability(server=_SERVER, include=("read_*",)))

        async with _runtime(config, plan, model) as runtime:
            await runtime.run(_AGENT, _PROMPT, identity=caller)

        assert model.offered_tools == ("read_orders",)
        assert any(CANARY in returned for returned in model.tool_returns)

    async def test_the_subprocess_dies_when_the_runtime_exits(
        self, tmp_path: Path, caller: Identity
    ) -> None:
        """No server outlives the runtime that spawned it."""
        pid_file = tmp_path / "pid"
        config = _config(pid_file)
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plan = _compile(config, McpCapability(server=_SERVER))

        async with _runtime(config, plan, model) as runtime:
            await runtime.run(_AGENT, _PROMPT, identity=caller)
            pid = int(pid_file.read_text(encoding="utf-8"))

        assert _wait_until_gone(pid)

    async def test_the_child_receives_only_the_declared_environment(
        self, tmp_path: Path, caller: Identity, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The declared variables arrive; the worker's own do not."""
        monkeypatch.setenv("LOOM_SECRET_PROBE", "must-not-leak")
        config = _config(tmp_path / "pid", env={"DECLARED": "hola"})
        model = ScriptedToolModel(
            calls=(
                ("echo_env", {"name": "DECLARED"}),
                ("echo_env", {"name": "LOOM_SECRET_PROBE"}),
            )
        )
        plan = _compile(config, McpCapability(server=_SERVER))

        async with _runtime(config, plan, model) as runtime:
            await runtime.run(_AGENT, _PROMPT, identity=caller)

        assert "hola" in model.tool_returns
        assert "must-not-leak" not in model.tool_returns


class TestStartupTolerance:
    """``remote_clients`` decides whether an unreachable command aborts start-up."""

    async def test_starts_and_marks_unavailable_when_it_is_optional(self, tmp_path: Path) -> None:
        """An optional server that cannot be spawned is dropped, not fatal."""
        config = _config(
            tmp_path / "pid", command="/nonexistent/mcp-binary", remote_clients="optional"
        )
        model = ScriptedToolModel()
        plan = _compile(config, McpCapability(server=_SERVER))

        async with _runtime(config, plan, model) as runtime:
            await _until(lambda: runtime._health.get(_AGENT) is not None)  # noqa: SLF001
            health = await runtime.health(_AGENT)

        assert health.checks[f"mcp:{_SERVER}"] == "unavailable"

    async def test_aborts_naming_the_server_when_it_is_required(self, tmp_path: Path) -> None:
        """A required server that cannot be spawned aborts start-up."""
        config = _config(tmp_path / "pid", command="/nonexistent/mcp-binary")
        model = ScriptedToolModel()
        plan = _compile(config, McpCapability(server=_SERVER))

        with pytest.raises(AgentCompilationError) as failure:
            async with _runtime(config, plan, model):
                pass

        issues = failure.value.issues
        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in {issue.code for issue in issues}
        assert any(_SERVER in issue.message for issue in issues)
