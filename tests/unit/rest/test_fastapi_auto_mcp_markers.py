"""Start-up verification of the ``Mcp()`` marker (spec 015, T202).

White-box unit tests of ``_verify_mcp_markers``, the private helper
``create_app`` calls immediately before ``_resolve_ai`` — never in the
``_verify_agent_markers`` slot after it, so an unknown server name dies
before the shared MCP capability compiler could ever be asked to report it
for a use case.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from loom.ai.abc import McpHandle
from loom.ai.config import McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.config import ConfigContext
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.plan import McpBinding
from loom.core.model.redaction import Dsn
from loom.core.use_case import Mcp, UseCase
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.mcp_markers import DeclaringMcpBindings
from loom.core.use_case.registry import UseCaseRegistry
from loom.rest.fastapi.auto import _compile_use_case_mcp, _verify_mcp_markers

_KNOWN_SERVER = "knowledge"

_SRC = Path(__file__).resolve().parents[3] / "src"

_CONTAINMENT_SCRIPT = """
import json
import sys

from loom.core.config import ConfigContext
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi.auto import _verify_mcp_markers


class NoMarkerUseCase(UseCase):
    async def execute(self):
        return None


compiler = UseCaseCompiler()
compiler.compile(NoMarkerUseCase)
registry = UseCaseRegistry.build([NoMarkerUseCase])
ctx = ConfigContext.from_dict({})

_verify_mcp_markers((NoMarkerUseCase,), compiler, registry, ctx)

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


class NoMarkerUseCase(UseCase[object, object]):
    """Declares no Mcp() marker at all — must never be inspected."""

    async def execute(self) -> object:
        return None


def _compiled(*use_case_types: type[UseCase[Any, Any]]) -> tuple[UseCaseCompiler, UseCaseRegistry]:
    compiler = UseCaseCompiler()
    for uc in use_case_types:
        compiler.compile(uc)
    registry = UseCaseRegistry.build(list(use_case_types))
    return compiler, registry


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


class TestUnUsoSinMarcadorNuncaSeInspecciona:
    def test_ninguna_seccion_ai_y_sin_marcadores_no_falla(self) -> None:
        compiler, registry = _compiled(NoMarkerUseCase)
        ctx = _ctx_without_ai()

        result = _verify_mcp_markers((NoMarkerUseCase,), compiler, registry, ctx)

        assert result == []

    def test_ni_importa_loom_ai_en_un_interprete_limpio(self) -> None:
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


class TestServidorConocido:
    def test_un_nombre_configurado_no_falla_y_devuelve_el_binding(self) -> None:
        compiler, registry = _compiled(KnownServerUseCase)
        ctx = _ctx_with_server()

        result = _verify_mcp_markers((KnownServerUseCase,), compiler, registry, ctx)

        [(uc_type, bindings)] = result
        assert uc_type is KnownServerUseCase
        assert [binding.server for binding in bindings] == [_KNOWN_SERVER]


class TestServidorDesconocido:
    def test_un_typo_aborta_nombrando_caso_de_uso_parametro_servidor_y_configurados(self) -> None:
        compiler, registry = _compiled(UnknownServerUseCase)
        ctx = _ctx_with_server()

        with pytest.raises(AgentCompilationError) as excinfo:
            _verify_mcp_markers((UnknownServerUseCase,), compiler, registry, ctx)

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        message = str(excinfo.value)
        assert "does-not-exist" in message
        assert "gateway" in message
        assert _KNOWN_SERVER in message

    def test_sin_seccion_ai_el_mensaje_dice_que_no_hay_servidores(self) -> None:
        compiler, registry = _compiled(UnknownServerUseCase)
        ctx = _ctx_without_ai()

        with pytest.raises(AgentCompilationError) as excinfo:
            _verify_mcp_markers((UnknownServerUseCase,), compiler, registry, ctx)

        assert "none" in str(excinfo.value)


class TestCompileUseCaseMcpPreservaElInclude:
    """H2: el ``include`` declarado en el marcador debe llegar intacto al grant.

    ``_compile_use_case_mcp`` es el pegamento entre el binding verificado y
    ``UseCaseMcpGrant``; ningún otro test ejercita esta función directamente,
    así que sustituir el ``include`` real por ``()`` en la llamada a
    ``compile_mcp_capability`` dejaría la suite entera verde sin este caso.
    """

    def test_el_include_del_binding_llega_intacto_a_la_capacidad_compilada(self) -> None:
        registry = UseCaseRegistry.build([KnownServerUseCase])
        servers = {
            _KNOWN_SERVER: McpServerConfig(url=Dsn(f"https://{_KNOWN_SERVER}.example.com/mcp"))
        }
        binding = McpBinding(name="gateway", server=_KNOWN_SERVER, include=("search_*", "fetch"))
        declaring: DeclaringMcpBindings = [(KnownServerUseCase, (binding,))]

        (grant,) = _compile_use_case_mcp(declaring, registry, servers)

        assert grant.capability.include == ("search_*", "fetch")
        assert grant.parameter == "gateway"

    def test_el_grant_lleva_la_clave_registrada_no_el_qualname(self) -> None:
        """Sin este caso, ``registry.key_for(uc_type) or uc_type.__qualname__``
        colapsando a ``uc_type.__qualname__`` sobrevive: el resto de la suite
        solo ejercita clases sin clave registrada, donde ambas ramas
        coinciden."""

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

        (grant,) = _compile_use_case_mcp(declaring, registry, servers)

        assert grant.usecase == "known.gateway"
