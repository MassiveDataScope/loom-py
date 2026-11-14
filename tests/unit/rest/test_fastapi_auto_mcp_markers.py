"""Start-up verification of the ``Mcp()`` marker (spec 015, T202).

White-box unit tests of ``verify_mcp_markers``, the private helper
``create_app`` calls immediately before ``resolve_ai`` — never in the
``verify_agent_markers`` slot after it, so an unknown server name dies
before the shared MCP capability compiler could ever be asked to report it
for a use case.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from loom.ai.abc import McpHandle
from loom.ai.config import McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition._ai import compile_use_case_mcp, verify_mcp_markers
from loom.core.config import ConfigContext
from loom.core.engine.plan import McpBinding
from loom.core.model.redaction import Dsn
from loom.core.use_case import Mcp, UseCase
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.mcp_markers import DeclaringMcpBindings
from loom.core.use_case.registry import UseCaseRegistry
from tests.unit.rest.doubles import compile_use_cases

_KNOWN_SERVER = "knowledge"

_SRC = Path(__file__).resolve().parents[3] / "src"

_CONTAINMENT_SCRIPT = """
import json
import sys

from loom.core.config import ConfigContext
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from loom.composition._ai import verify_mcp_markers


class AcmeUnmarkedMcpUseCase(UseCase):
    async def execute(self):
        return None


compiler = UseCaseCompiler()
compiler.compile(AcmeUnmarkedMcpUseCase)
registry = UseCaseRegistry.build([AcmeUnmarkedMcpUseCase])
ctx = ConfigContext.from_dict({})

verify_mcp_markers((AcmeUnmarkedMcpUseCase,), compiler, registry, ctx)

leaked = [name for name in sys.modules if name == "loom.ai" or name.startswith("loom.ai.")]
print(json.dumps(sorted(leaked)))
"""


class KnownServerUseCase(UseCase[object, object]):
    """Declares a server this deployment configures."""

    async def execute(
        self, gateway: McpHandle = Mcp(_KNOWN_SERVER, include=["search_*"])
    ) -> object:
        return gateway


class UnknownServerUseCase(UseCase[object, object]):
    """Names a server no deployment in this test configures."""

    async def execute(self, gateway: McpHandle = Mcp("does-not-exist", include=["*"])) -> object:
        return gateway


class AcmeMcpUseCaseWithoutMarker(UseCase[object, object]):
    """Declares no Mcp() marker at all — must never be inspected."""

    async def execute(self) -> object:
        return None


def _ctx_with_server(server: str = _KNOWN_SERVER) -> ConfigContext:
    return ConfigContext.from_dict(
        {
            "ai": {
                "engine": "fake",
                "models": {"default": {"provider": "fake", "model": "fake-model"}},
                "mcp_servers": {server: {"url": f"https://{server}.example.com/mcp"}},
            }
        }
    )


def _ctx_without_ai() -> ConfigContext:
    return ConfigContext.from_dict({})


class TestUseCaseWithoutAnMcpMarker:
    def test_no_ai_section_and_no_markers_does_not_fail(self) -> None:
        compiler, registry = compile_use_cases(AcmeMcpUseCaseWithoutMarker)
        ctx = _ctx_without_ai()

        result = verify_mcp_markers((AcmeMcpUseCaseWithoutMarker,), compiler, registry, ctx)

        assert result == []

    def test_does_not_import_loom_ai_in_a_clean_interpreter(self) -> None:
        """Run in a clean subprocess: the pytest interpreter already imported
        'loom.ai' for other suites, so an in-process check here could not
        distinguish this call's own behaviour from that prior import."""
        env = {**os.environ, "PYTHONPATH": str(_SRC)}
        result = subprocess.run(
            [sys.executable, "-c", _CONTAINMENT_SCRIPT],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        assert result.returncode == 0, result.stderr

        leaked = json.loads(result.stdout.strip().splitlines()[-1])
        assert leaked == []


class TestKnownMcpServer:
    def test_a_configured_server_name_returns_the_binding(self) -> None:
        compiler, registry = compile_use_cases(KnownServerUseCase)
        ctx = _ctx_with_server()

        result = verify_mcp_markers((KnownServerUseCase,), compiler, registry, ctx)

        [(uc_type, bindings)] = result
        assert uc_type is KnownServerUseCase
        assert [binding.server for binding in bindings] == [_KNOWN_SERVER]


class TestUnknownMcpServer:
    def test_a_typo_aborts_naming_the_use_case_parameter_server_and_the_configured_servers(
        self,
    ) -> None:
        compiler, registry = compile_use_cases(UnknownServerUseCase)
        ctx = _ctx_with_server()

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_mcp_markers((UnknownServerUseCase,), compiler, registry, ctx)

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        message = str(excinfo.value)
        assert "does-not-exist" in message
        assert "gateway" in message
        assert _KNOWN_SERVER in message

    def test_without_an_ai_section_the_message_says_there_are_no_servers(self) -> None:
        compiler, registry = compile_use_cases(UnknownServerUseCase)
        ctx = _ctx_without_ai()

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_mcp_markers((UnknownServerUseCase,), compiler, registry, ctx)

        assert "none" in str(excinfo.value)


class TestCompileUseCaseMcpKeepsTheInclude:
    """H2: the ``include`` declared on the marker reaches the grant unchanged.

    ``compile_use_case_mcp`` is the glue between the verified binding and
    ``UseCaseMcpGrant``. No other test calls this function directly, so
    replacing the real ``include`` with ``()`` in the call to
    ``compile_mcp_capability`` would leave the whole suite green without this case.
    """

    def test_the_binding_include_reaches_the_compiled_capability(self) -> None:
        registry = UseCaseRegistry.build([KnownServerUseCase])
        servers = {
            _KNOWN_SERVER: McpServerConfig(url=Dsn(f"https://{_KNOWN_SERVER}.example.com/mcp"))
        }
        binding = McpBinding(name="gateway", server=_KNOWN_SERVER, include=("search_*", "fetch"))
        declaring: DeclaringMcpBindings = [(KnownServerUseCase, (binding,))]

        (grant,) = compile_use_case_mcp(declaring, registry, servers)

        assert grant.capability.include == ("search_*", "fetch")
        assert grant.parameter == "gateway"

    def test_the_grant_carries_the_registered_key_not_the_qualname(self) -> None:
        """Without this case, ``registry.key_for(uc_type) or uc_type.__qualname__``
        collapsing to ``uc_type.__qualname__`` survives: the rest of the suite
        only exercises classes with no registered key, where both branches
        agree."""

        @use_case_key("known.gateway")
        class RegisteredGatewayUseCase(UseCase[object, object]):
            async def execute(
                self, gateway: McpHandle = Mcp(_KNOWN_SERVER, include=["search_*"])
            ) -> object:
                return gateway

        registry = UseCaseRegistry.build([RegisteredGatewayUseCase])
        servers = {
            _KNOWN_SERVER: McpServerConfig(url=Dsn(f"https://{_KNOWN_SERVER}.example.com/mcp"))
        }
        binding = McpBinding(name="gateway", server=_KNOWN_SERVER, include=("search_*",))
        declaring: DeclaringMcpBindings = [(RegisteredGatewayUseCase, (binding,))]

        (grant,) = compile_use_case_mcp(declaring, registry, servers)

        assert grant.usecase == "known.gateway"
