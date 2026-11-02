"""Start-up verification of the ``Agent()`` marker (spec 014, T202).

White-box unit tests of the two private helpers ``create_app`` calls between
``resolve_ai`` and ``result.factory.verify()``: ``verify_agent_markers``
aborts start-up on an unknown agent or a mismatched output type, and
``bind_agent_resolver`` wires the executor's resolver once (and only once)
an AI runtime exists.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.ai.compiler._plan import AgentPlan, CompiledInstruction, CompiledOutput
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.composition._ai import (
    AiWiring,
    bind_agent_resolver,
    bind_mcp_resolver,
    verify_agent_markers,
)
from loom.core.bootstrap import KernelRuntime
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.model import msgspec_type
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.service import NullSqlQueryService, SqlQueryService
from loom.core.use_case import Agent
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase


def _plan(name: str, *, output_type: type[Any] = dict) -> AgentPlan:
    """Build a minimal compiled plan naming its own declared output type."""
    return AgentPlan(
        name=name,
        description=f"{name} test agent",
        instructions=(CompiledInstruction(text="answer"),),
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(
            schema={"type": "object"},
            loom_type=msgspec_type(output_type),
        ),
        capabilities=(),
        policies=PolicySpec(),
        metadata={},
    )


class _Severity(msgspec.Struct):
    level: int


class KnownAgentUseCase(UseCase[object, object]):
    """Declares an agent whose plan will exist, with a matching output type."""

    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class MismatchedOutputUseCase(UseCase[object, object]):
    """The annotation names a type the artefact does not declare (dict)."""

    async def execute(self, triage: AgentHandle[_Severity] = Agent("triage")) -> object:
        return triage


class UnknownAgentUseCase(UseCase[object, object]):
    """Names an agent no plan in this deployment compiles."""

    async def execute(self, triage: AgentHandle[dict] = Agent("does-not-exist")) -> object:
        return triage


class NoMarkerUseCase(UseCase[object, object]):
    """Declares no Agent() marker at all — must never be inspected."""

    async def execute(self) -> object:
        return None


def _compiled(*use_case_types: type[UseCase[Any, Any]]) -> tuple[UseCaseCompiler, UseCaseRegistry]:
    compiler = UseCaseCompiler()
    for uc in use_case_types:
        compiler.compile(uc)
    registry = UseCaseRegistry.build(list(use_case_types))
    return compiler, registry


class TestUnUsoSinMarcadorNuncaSeInspecciona:
    def test_ninguna_seccion_ai_y_sin_marcadores_no_falla(self) -> None:
        compiler, registry = _compiled(NoMarkerUseCase)
        ai = AiWiring(config=None, runtime=None)

        verify_agent_markers((NoMarkerUseCase,), compiler, registry, ai)  # no raise


class TestAgenteDesconocido:
    def test_un_nombre_que_ningun_plan_compila_aborta_el_arranque(self) -> None:
        compiler, registry = _compiled(UnknownAgentUseCase)
        ai = AiWiring(config=None, runtime=None, plans=(_plan("triage"),))

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_agent_markers((UnknownAgentUseCase,), compiler, registry, ai)

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.AGENT_MARKER_UNKNOWN}
        message = str(excinfo.value)
        assert "does-not-exist" in message
        assert "triage" in message

    def test_sin_seccion_ai_el_mensaje_dice_que_no_hay_agentes(self) -> None:
        compiler, registry = _compiled(UnknownAgentUseCase)
        ai = AiWiring(config=None, runtime=None)

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_agent_markers((UnknownAgentUseCase,), compiler, registry, ai)

        assert "none" in str(excinfo.value)


class TestTipoDeSalidaDesajustado:
    def test_la_anotacion_que_no_coincide_con_el_output_declarado_aborta(self) -> None:
        compiler, registry = _compiled(MismatchedOutputUseCase)
        ai = AiWiring(config=None, runtime=None, plans=(_plan("triage", output_type=dict),))

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_agent_markers((MismatchedOutputUseCase,), compiler, registry, ai)

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH}

    def test_una_anotacion_coincidente_no_falla(self) -> None:
        compiler, registry = _compiled(KnownAgentUseCase)
        ai = AiWiring(config=None, runtime=None, plans=(_plan("triage", output_type=dict),))

        verify_agent_markers((KnownAgentUseCase,), compiler, registry, ai)  # no raise


class TestAgregacionDeProblemas:
    def test_dos_problemas_distintos_se_reportan_juntos(self) -> None:
        compiler, registry = _compiled(UnknownAgentUseCase, MismatchedOutputUseCase)
        ai = AiWiring(config=None, runtime=None, plans=(_plan("triage", output_type=dict),))

        with pytest.raises(AgentCompilationError) as excinfo:
            verify_agent_markers(
                (UnknownAgentUseCase, MismatchedOutputUseCase), compiler, registry, ai
            )

        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {
            AgentErrorCode.AGENT_MARKER_UNKNOWN,
            AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH,
        }


def _kernel_runtime() -> KernelRuntime:
    """Build a real, minimal ``KernelRuntime`` — every collaborator genuine, none stubbed.

    ``SqlQueryService`` is registered the same way ``create_app`` always
    registers it, before ``bind_agent_resolver`` ever runs: that
    function resolves it unconditionally now, so an unregistered container
    here would fail every test in this class for a reason unrelated to what
    each one is pinning.
    """
    container = LoomContainer()
    container.register_instance(SqlQueryService, NullSqlQueryService())
    compiler = UseCaseCompiler()
    factory = UseCaseFactory(container)
    executor = RuntimeExecutor(compiler)
    registry = UseCaseRegistry.build([])
    app = AppInvoker(factory=factory, executor=executor, registry=registry)
    return KernelRuntime(
        container=container,
        compiler=compiler,
        factory=factory,
        executor=executor,
        registry=registry,
        app=app,
    )


def _fake_agent_runtime() -> Any:
    """An unentered ``AgentRuntime``: enough to close over, never called here.

    ``bind_agent_resolver`` only reads ``ai.runtime is None`` and hands the
    value on to ``agent_marker_resolver``, which stores it in a closure and
    never calls any of its methods until the handle it builds actually runs.
    So a real ``AgentRuntime`` never entered as a context manager is a
    faithful double for this seam — no fake class needed.
    """
    from loom.ai.runtime import AgentRuntime

    return object.__new__(AgentRuntime)


class TestElResolverSeLigaSoloConRuntimeDeIa:
    def test_sin_seccion_ai_el_executor_no_recibe_resolver(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=None)

        bind_agent_resolver(result, ai)

        assert result.executor._agent_resolver is None  # noqa: SLF001 - white-box wiring test

    def test_con_runtime_de_ia_el_resolver_queda_ligado(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=_fake_agent_runtime())

        bind_agent_resolver(result, ai)

        assert result.executor._agent_resolver is not None  # noqa: SLF001
        with pytest.raises(RuntimeError, match="more than once"):
            result.executor.bind_agent_resolver(lambda name, identity: object())

    def test_resuelve_la_observabilidad_registrada_en_el_contenedor(self) -> None:
        result = _kernel_runtime()
        runtime = ObservabilityRuntime([])
        result.container.register_instance(ObservabilityRuntime, runtime)
        ai = AiWiring(config=None, runtime=_fake_agent_runtime())

        bind_agent_resolver(result, ai)

        resolver = result.executor._agent_resolver  # noqa: SLF001
        assert resolver is not None
        handle = resolver("triage", Identity(subject="ada", mechanism="test"))
        assert handle._observability is runtime  # noqa: SLF001

    def test_sin_runtime_de_observabilidad_registrado_el_mango_no_abre_span(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=_fake_agent_runtime())

        bind_agent_resolver(result, ai)

        resolver = result.executor._agent_resolver  # noqa: SLF001
        assert resolver is not None
        handle = resolver("triage", Identity(subject="ada", mechanism="test"))
        assert handle._observability is None  # noqa: SLF001


class TestElResolverDeMcpSeLigaSoloConRuntimeDeIa:
    """Mirrors ``TestElResolverSeLigaSoloConRuntimeDeIa`` for ``bind_mcp_resolver`` (S7).

    ``bind_mcp_resolver`` is a second, differently-typed wiring call, never
    exercised by any test above: deleting it entirely still passes every
    ``Agent()`` test in this module and every start-up check, and leaves the
    ``Mcp()`` marker dead in production (H4).
    """

    def test_sin_seccion_ai_el_executor_no_recibe_resolver_mcp(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=None)

        bind_mcp_resolver(result, ai)

        assert result.executor._mcp_resolver is None  # noqa: SLF001 - white-box wiring test

    def test_con_runtime_de_ia_el_resolver_de_mcp_queda_ligado(self) -> None:
        result = _kernel_runtime()
        ai = AiWiring(config=None, runtime=_fake_agent_runtime())

        bind_mcp_resolver(result, ai)

        assert result.executor._mcp_resolver is not None  # noqa: SLF001
        with pytest.raises(RuntimeError, match="more than once"):
            result.executor.bind_mcp_resolver(lambda server, include, identity: object())
