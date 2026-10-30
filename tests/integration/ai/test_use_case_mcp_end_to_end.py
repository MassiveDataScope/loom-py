"""The real path, end to end, for the ``Mcp()`` marker (spec 015, T501).

Every test here drives a real :class:`~loom.core.engine.compiler.UseCaseCompiler`,
a real :class:`~loom.core.engine.executor.RuntimeExecutor` and a real
:class:`~loom.ai.runtime.AgentRuntime`, wired through the same production glue
``create_app`` uses — :func:`~loom.core.use_case.mcp_markers.declaring_mcp_bindings`
and ``loom.composition._ai.compile_use_case_mcp`` — rather than hand-built
``UseCaseMcpGrant`` instances. The seam faked is the MCP client itself, at the
edge, exactly as ``test_agent_marker_grants_integration.py`` fakes it for the
``Agent()`` marker.

``test_use_case_mcp_runtime.py`` (T203) and ``test_use_case_mcp_resolver.py``
(T302) already cover the runtime's own opening/listing rules and the
resolver's own filtering rule in isolation; this module is the one that
proves the whole chain — compiler, executor, runtime, resolver — agrees, and
adds the measurements those modules deliberately stop short of: a real
client-factory call counter (not a log reading), a use case reaching what an
agent's own filter excludes in one test, two use cases with disjoint
``include``s each admitting only its own over one connection and one
listing, the double and the real path agreeing on a refusal, and the two
aborts only this path can reach — a declared marker with no ``ai:`` section
at all, and an ``include`` matching nothing on the capability the compiler
actually built.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import agent_marker_resolver, mcp_marker_resolver
from loom.composition._ai import compile_use_case_mcp
from loom.core.di import LoomContainer
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.sql.service import NullSqlQueryService
from loom.core.use_case import Agent, Mcp, UseCase
from loom.core.use_case.mcp_markers import declaring_mcp_bindings
from loom.core.use_case.registry import UseCaseRegistry
from loom.rest.fastapi.auto import create_app
from loom.testing.runner import McpHandleDouble, UseCaseTest
from tests.integration.ai.conftest import (
    AgentPlan,
    CountingEngineProvider,
    CountingMcpClientFactory,
    RecordingMcpSession,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_SERVER = "alpha-tools"
_AGENT = "triage"


class _GatewayUseCase(UseCase[object, dict[str, object]]):
    """The example this module runs both live and offline: one ``Mcp()`` call."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("search",))
    ) -> dict[str, object]:
        return dict(await gateway.call_untyped("search", {"q": "widgets"}))


class _NarrowGatewayUseCase(UseCase[object, dict[str, object]]):
    """Declares ``search`` only, then calls a published tool outside it."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("search",))
    ) -> dict[str, object]:
        return dict(await gateway.call_untyped("delete", {}))


class _WideningGatewayUseCase(UseCase[object, dict[str, object]]):
    """Declares ``delete`` — a server-published tool the agent's own grant excludes."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("delete",))
    ) -> dict[str, object]:
        return dict(await gateway.call_untyped("delete", {}))


class _MissingToolGatewayUseCase(UseCase[object, dict[str, object]]):
    """Declares an ``include`` entry the server never publishes."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("nope",))
    ) -> dict[str, object]:
        return dict(await gateway.call_untyped("search", {}))


class _AgentToolUseCase(UseCase[object, tuple[str, ...]]):
    """Reaches the shared server through the agent's own, narrower grant."""

    async def execute(self, triage: AgentHandle[dict[str, Any]] = Agent(_AGENT)) -> tuple[str, ...]:
        return triage.mcp(_SERVER).tools()


class _FirstOrdersUseCase(UseCase[object, tuple[str, ...]]):
    """One of two use cases naming the same server with a disjoint ``include``."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("search",))
    ) -> tuple[str, ...]:
        return gateway.tools()


class _SecondOrdersUseCase(UseCase[object, tuple[str, ...]]):
    """The other of the two use cases, admitting the tool the first excludes."""

    async def execute(
        self, gateway: McpHandle = Mcp(_SERVER, include=("delete",))
    ) -> tuple[str, ...]:
        return gateway.tools()


def _mcp_wired(
    *,
    use_case_types: Sequence[type[Compilable]],
    clients: Mapping[str, StubMcpClient],
    factory: CountingMcpClientFactory | None = None,
    deps: StubDepsFactory,
    container: LoomContainer,
    plans: Sequence[AgentPlan] = (),
) -> tuple[RuntimeExecutor, AgentRuntime]:
    """Wire a real executor and a real runtime, exactly as ``create_app`` does.

    Reuses the same production glue ``auto.py`` calls between compiling and
    constructing ``AgentRuntime`` (``:1453-1458``):
    :func:`declaring_mcp_bindings` finds the compiled ``Mcp()`` bindings, and
    ``compile_use_case_mcp`` turns them into ``UseCaseMcpGrant``s. Building
    that tuple by hand, as ``test_use_case_mcp_runtime.py`` does for its own
    narrower purpose, would skip exercising this glue at all.

    The agent resolver is bound only when *plans* is non-empty: a runtime
    with zero compiled agent plans has no artefact for it to serve, and
    ``create_app`` never binds one either in that deployment shape.

    Args:
        use_case_types: Every use case to compile; some may declare no
            marker at all.
        clients: Stub MCP client per server name.
        factory: Overrides the plain ``mcp_client_factory(clients)`` — used
            to pass a :class:`CountingMcpClientFactory`.
        deps: Per-invocation dependency factory.
        container: Application container.
        plans: Compiled agent plans this runtime also serves.

    Returns:
        The bound executor and the not-yet-entered runtime.
    """
    compiler = UseCaseCompiler()
    for use_case_type in use_case_types:
        compiler.compile(use_case_type)
    registry = UseCaseRegistry.build(list(use_case_types))
    declaring = declaring_mcp_bindings(list(use_case_types), compiler)
    servers = make_mcp_servers(*clients)
    use_case_mcp = compile_use_case_mcp(declaring, registry, servers)

    executor = RuntimeExecutor(compiler)
    runtime = AgentRuntime(
        plans=list(plans),
        config=make_ai_config(mcp_servers=servers),
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=factory or mcp_client_factory(dict(clients)),  # type: ignore[arg-type]
        use_case_mcp=use_case_mcp,
    )
    executor.bind_mcp_resolver(mcp_marker_resolver(runtime, observability=None))
    if plans:
        executor.bind_agent_resolver(
            agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=None
            )
        )
    return executor, runtime


class TestTheRealPathCompilesStartsResolvesAndCallsWithZeroAgentPlans:
    """T501 items 1 and 8: one call over the worker's shared session,
    proven with zero compiled agent plans naming the server at all."""

    async def test_the_result_arrives_over_the_session_startup_opened(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search",), results={"search": {"hits": 5}})
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        executor, runtime = _mcp_wired(
            use_case_types=(_GatewayUseCase,), clients=clients, deps=deps, container=container
        )

        async with runtime:
            result = await executor.execute(_GatewayUseCase(), identity=identity)

        assert result == {"hits": 5}
        assert lifecycle_log.count("open:a") == 1
        assert session.calls == [("search", {"q": "widgets"})]


class TestAServerSharedByAnAgentAndAUseCase:
    """T501 item 2: measured with a real factory call counter, not a log."""

    async def test_the_factory_and_the_listing_are_invoked_only_once(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        factory = CountingMcpClientFactory(clients)
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER, include=("search",)),))
        executor, runtime = _mcp_wired(
            use_case_types=(_AgentToolUseCase, _GatewayUseCase),
            clients=clients,
            factory=factory,
            deps=deps,
            container=container,
            plans=(plan,),
        )

        async with runtime:
            await executor.execute(_GatewayUseCase(), identity=identity)

        assert factory.calls == [_SERVER]
        assert session.listed == 1


class TestTheUseCaseReachesWhatTheAgentExcludes:
    """T501 item 3: pinned in one test, over the real chain both ways."""

    async def test_the_use_case_reaches_it_while_the_agents_view_still_does_not(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"), results={"delete": {"ok": True}})
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(_AGENT, capabilities=(make_mcp_capability(_SERVER, include=("search",)),))
        executor, runtime = _mcp_wired(
            use_case_types=(_AgentToolUseCase, _WideningGatewayUseCase),
            clients=clients,
            deps=deps,
            container=container,
            plans=(plan,),
        )

        async with runtime:
            agent_tools = await executor.execute(_AgentToolUseCase(), identity=identity)
            use_case_result = await executor.execute(_WideningGatewayUseCase(), identity=identity)

        assert agent_tools == ("search",)
        assert use_case_result == {"ok": True}


class TestAnIncludeMatchingNothingAbortsThroughTheRealPath:
    """T501 item 4: an ``include`` matching nothing aborts start-up through
    ``compile_use_case_mcp`` itself, not through a hand-built
    ``UseCaseMcpGrant`` — ``test_use_case_mcp_runtime.py`` pins this same
    outcome, but its ``_grant()`` helper builds the grant by hand and never
    calls ``compile_use_case_mcp``, so it cannot catch a regression in the
    glue that carries ``binding.include`` into the compiled capability.
    """

    async def test_an_include_matching_nothing_aborts_with_tool_filter_matches_nothing(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        _, runtime = _mcp_wired(
            use_case_types=(_MissingToolGatewayUseCase,),
            clients=clients,
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        codes = {issue.code for issue in failure.value.issues}
        assert AgentErrorCode.USECASE_TOOL_FILTER_MATCHES_NOTHING in codes
        # The other start-up abort must not fire here, and this is the only
        # file that can tell: `MCP_SERVER_UNKNOWN` has one producer,
        # `compile_mcp_capability`, which only this path reaches. Asserted
        # anywhere that hand-builds the grant, it could never fail.
        assert AgentErrorCode.MCP_SERVER_UNKNOWN not in codes


class TestTwoUseCasesWithDisjointIncludesOverOneSession:
    """T501 item 9: measured on the shared session, not only on ``tools()``."""

    async def test_each_one_admits_only_its_own_over_the_same_session(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        executor, runtime = _mcp_wired(
            use_case_types=(_FirstOrdersUseCase, _SecondOrdersUseCase),
            clients=clients,
            deps=deps,
            container=container,
        )

        async with runtime:
            first = await executor.execute(_FirstOrdersUseCase(), identity=identity)
            second = await executor.execute(_SecondOrdersUseCase(), identity=identity)

        assert first == ("search",)
        assert second == ("delete",)
        # Same-session identity across two disjoint 'include's is
        # 'test_use_case_mcp_resolver.py's own assertion (FR-15); here the
        # observable proxy is what a shared session buys: one connection,
        # one listing, for both bindings.
        assert lifecycle_log.count("open:a") == 1
        assert session.listed == 1


class TestTheFullExampleRunsWithoutNetworkOrMcpServer:
    """T501 item 7: the same use case item 1 runs live, now offline."""

    async def test_the_result_reflects_what_was_scripted_on_the_double(
        self, identity: Identity
    ) -> None:
        gateway = McpHandleDouble(_SERVER).with_tools("search")
        gateway.on_call_untyped("search", {"hits": 2})

        result = await (
            UseCaseTest(_GatewayUseCase()).with_caller(identity).with_mcp(_SERVER, gateway).run()
        )

        assert result == {"hits": 2}


class TestTheDoubleAndTheRealPathAgreeOnTheRefusal:
    """T501 item 10: the same call, refused the same way, both paths."""

    async def test_both_paths_refuse_the_same_call_outside_include(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"), results={"delete": {"ok": True}})
        clients = {_SERVER: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        executor, runtime = _mcp_wired(
            use_case_types=(_NarrowGatewayUseCase,),
            clients=clients,
            deps=deps,
            container=container,
        )

        use_case = _NarrowGatewayUseCase()
        async with runtime:
            with pytest.raises(AgentRunError) as real_failure:
                await executor.execute(use_case, identity=identity)

        double = McpHandleDouble(_SERVER).with_tools("search", "delete")
        double.on_call_untyped("delete", {"ok": True})
        runner = (
            UseCaseTest(_NarrowGatewayUseCase()).with_caller(identity).with_mcp(_SERVER, double)
        )
        with pytest.raises(AgentRunError) as double_failure:
            await runner.run()

        assert real_failure.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert double_failure.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        # Session-level refusal: production never reached the network either.
        assert session.calls == []


_NO_AI_APP_SOURCE = '''\
"""Minimal discoverable app: one use case declaring Mcp(), no ai: section."""

from __future__ import annotations

from loom.ai.abc import McpHandle
from loom.core.use_case import Mcp
from loom.core.use_case.use_case import UseCase


class LookUpKnowledgeUseCase(UseCase[object, str]):
    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> str:
        return "ok"
'''

_NO_AI_MANIFEST_SOURCE = """\
from {app_module} import LookUpKnowledgeUseCase

USE_CASES = [LookUpKnowledgeUseCase]
"""


class TestWithNoAiSectionStartupAbortsIfAUseCaseDeclaresTheMarker:
    """T501 item 6 (the ``create_app`` half): no unit test drives this —
    ``test_fastapi_auto_mcp_markers.py`` calls ``verify_mcp_markers`` directly."""

    def test_create_app_aborts_naming_the_use_case_and_the_parameter(self, tmp_path: Path) -> None:
        app_module = "loom_noai_mcp_fixture_app"
        manifest_module = "loom_noai_mcp_fixture_manifest"
        sys.modules.pop(manifest_module, None)
        sys.modules.pop(app_module, None)
        (tmp_path / f"{app_module}.py").write_text(_NO_AI_APP_SOURCE, encoding="utf-8")
        (tmp_path / f"{manifest_module}.py").write_text(
            _NO_AI_MANIFEST_SOURCE.format(app_module=app_module), encoding="utf-8"
        )
        config: dict[str, Any] = {
            "app": {
                "name": "noai-mcp-demo",
                "code_path": str(tmp_path),
                "discovery": {"mode": "manifest", "manifest": {"module": manifest_module}},
            },
            "database": {"url": "sqlite+aiosqlite:///"},
            "persistence": {"cursors": {"signing": "ephemeral"}},
            # Deliberately no 'ai:' section: FR-08 / T501 item 6, the case
            # 'ctx.has(ConfigKey.AI)' being false must still abort a
            # deployment whose use case declares 'Mcp()'.
        }
        config_path = tmp_path / "app.yaml"
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

        try:
            with pytest.raises(AgentCompilationError) as failure:
                create_app(str(config_path))
        finally:
            sys.modules.pop(manifest_module, None)
            sys.modules.pop(app_module, None)

        codes = {issue.code for issue in failure.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        message = str(failure.value)
        assert "LookUpKnowledgeUseCase" in message
        assert "gateway" in message
        assert "knowledge" in message
