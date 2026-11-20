"""``McpMarkerResolver`` (T302) and its degradation (T303).

Builds real ``AgentHost`` instances over stub MCP clients — the same
shape ``test_use_case_mcp_runtime.py`` (T203) uses — and resolves through
:class:`~loom.ai.execution._handle.McpMarkerResolver`, the resolver
``UseCaseExecutor.bind_mcp_resolver`` takes, instead of constructing
``McpGrantView`` by hand. This is the seam T203's suite deliberately stops
short of: the resolving *binding's own* ``include``, not the shared
substrate grant's, must be what a resolved view enforces.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.ai.compiler._plan import CompiledMcpCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentHost
from loom.ai.execution._handle import McpMarkerResolver
from loom.ai.execution._lifecycle import UseCaseMcpGrant
from loom.core.di import LoomContainer
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingMcpSession,
    RecordingObserver,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    mcp_client_factory,
    mcp_server_url,
)

_SERVER_A = "alpha-tools"


def _grant(
    server: str,
    *,
    include: tuple[str, ...],
    usecase: str = "orders.get_order_status",
    parameter: str = "gateway",
    timeout_ms: int | None = None,
) -> UseCaseMcpGrant:
    capability = (
        make_mcp_capability(server, include=include)
        if timeout_ms is None
        else CompiledMcpCapability(
            server=server, url=mcp_server_url(server), include=tuple(include), timeout_ms=timeout_ms
        )
    )
    return UseCaseMcpGrant(capability=capability, usecase=usecase, parameter=parameter)


def _host(
    *,
    clients: dict[str, StubMcpClient],
    use_case_mcp: tuple[UseCaseMcpGrant, ...],
    deps: StubDepsFactory,
    container: LoomContainer,
    remote_clients: str = "required",
) -> AgentHost:
    return AgentHost(
        plans=[],
        config=make_ai_config(
            mcp_servers=make_mcp_servers(*clients), remote_clients=remote_clients
        ),
        engine_plugin=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        use_case_mcp=use_case_mcp,
    )


class TestTheResolverAppliesTheResolvingBindingsFilter:
    """The authorization failure PR2 measured cannot return."""

    async def test_admits_exactly_whats_declared_and_refuses_the_rest(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(
            tools=("search", "delete"), schemas=("search",), results={"search": {"hits": 1}}
        )
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        host = _host(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with host:
            resolve = McpMarkerResolver(host, observability=None)
            handle = resolve(_SERVER_A, ("search",), identity)

            with ExecutionScope(identity):
                assert handle.tools() == ("search",)
                result = await handle.call_untyped("search", {})
            assert result == {"hits": 1}

            with ExecutionScope(identity), pytest.raises(AgentRunError) as excinfo:
                await handle.call_untyped("delete", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN

    async def test_two_use_cases_over_one_server_do_not_collide(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        host = _host(
            clients=clients,
            use_case_mcp=(
                _grant(_SERVER_A, include=("search",), usecase="orders.first"),
                _grant(_SERVER_A, include=("delete",), usecase="orders.second"),
            ),
            deps=deps,
            container=container,
        )

        async with host:
            resolve = McpMarkerResolver(host, observability=None)
            first = resolve(_SERVER_A, ("search",), identity)
            second = resolve(_SERVER_A, ("delete",), identity)

            with ExecutionScope(identity):
                assert first.tools() == ("search",)
                assert second.tools() == ("delete",)
                await first.call_untyped("search", {})
                await second.call_untyped("delete", {})
            # Both bindings call through the very same session (FR-15):
            # neither a second connection nor a second listing served either one.
            assert [name for name, _ in session.calls] == ["search", "delete"]
            assert session.listed == 1

    async def test_reading_the_shared_grants_include_instead_of_the_bindings_own_fails_this(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """Pins the exact mutation the plan's corrected S7 note exists to forbid.

        The shared substrate grant's own capability always carries
        ``include=()`` (explicitly empty, T203's ``_build_use_case_grants``).
        A resolver reading *that* field back instead of the resolving
        binding's own ``include`` would see an empty filter here and admit
        every tool — this test is the one that goes red if that regresses.
        """
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        host = _host(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with host:
            grant = host.use_case_mcp_grant(_SERVER_A)
            assert grant is not None
            # The shared substrate carries no filter of its own (H1, T203).
            assert grant.capability.include == ()

            resolve = McpMarkerResolver(host, observability=None)
            handle = resolve(_SERVER_A, ("search",), identity)

            with ExecutionScope(identity):
                assert handle.tools() == ("search",)
            with ExecutionScope(identity), pytest.raises(AgentRunError) as excinfo:
                await handle.call_untyped("delete", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN


class TestTheSpanDistinguishesTheUseCasePath:
    async def test_the_span_names_the_mcp_server_and_not_the_agent(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(tools=("search",), results={"search": {"ok": True}})
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        host = _host(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )
        observer = RecordingObserver()
        observability = Observability(observers=[observer])

        async with host:
            resolve = McpMarkerResolver(host, observability=observability)
            handle = resolve(_SERVER_A, ("search",), identity)
            with ExecutionScope(identity):
                await handle.call_untyped("search", {})

        tool_spans = [e for e in observer.events if e.scope is Scope.TOOL]
        assert tool_spans
        for event in tool_spans:
            meta = event.meta or {}
            assert meta.get("mcp_server") == "alpha-tools"
            assert "agent" not in meta
            # FR-09, and the only place the binding is observable: the span is
            # where this path's identity surfaces, so without this the resolver
            # could hand the view an anonymous identity and nothing would fail.
            assert meta.get("subject") == identity.subject


class TestTimeoutComesFromTheServersOwnDeadline:
    """H1: the marker path has no plan to read ``tool_timeout_ms`` from (T302).

    Deleting the ``/ 1000`` at the resolver's ``timeout_s=`` call site turns
    a 20ms deadline into 20000s, which this test would then wait forever
    for — this is the one place that stated conversion is exercised.
    """

    async def test_a_short_server_deadline_times_out_and_names_it(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        class SlowSession(RecordingMcpSession):
            async def call_tool(self, name: str, arguments: dict) -> object:  # type: ignore[override]
                await asyncio.sleep(0.5)
                return await super().call_tool(name, arguments)

        session = SlowSession(tools=("search",), results={"search": {"ok": 1}})
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        host = _host(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",), timeout_ms=40),),
            deps=deps,
            container=container,
        )

        async with host:
            resolve = McpMarkerResolver(host, observability=None)
            handle = resolve(_SERVER_A, ("search",), identity)
            with ExecutionScope(identity), pytest.raises(AgentRunError) as excinfo:
                await handle.call_untyped("search", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_TIMEOUT
        assert "0.040s" in str(excinfo.value), str(excinfo.value)
        assert _SERVER_A in str(excinfo.value), str(excinfo.value)


class TestUnreachableServerTolerated:
    """FR-07/T303: degradation on call, not on startup."""

    async def test_startup_passes_and_the_first_call_is_tool_unavailable(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        client = StubMcpClient(label="a", session=None, log=lifecycle_log, connect_error="refused")
        host = _host(
            clients={_SERVER_A: client},
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with host:
            resolve = McpMarkerResolver(host, observability=None)
            handle = resolve(_SERVER_A, ("search",), identity)

            with ExecutionScope(identity):
                assert handle.tools() == ()

            with ExecutionScope(identity), pytest.raises(AgentRunError) as excinfo:
                await handle.call_untyped("search", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE

            # ``call`` is the Protocol's typed default and the one a use case
            # writes; degrading only on the untyped path would return ``None``
            # where the signature promises ``expect``.
            with ExecutionScope(identity), pytest.raises(AgentRunError) as typed:
                await handle.call("search", {}, expect=dict)
            assert typed.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE

    async def test_a_runtime_never_entered_raises_runtime_error_not_tool_unavailable(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """Distinguishes 'never entered' (a wiring bug) from 'tolerated down'."""
        client = StubMcpClient(label="a", session=None, log=lifecycle_log)
        host = _host(
            clients={_SERVER_A: client},
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )
        handle = McpMarkerResolver(host, observability=None)(_SERVER_A, ("search",), identity)

        with (
            ExecutionScope(identity),
            pytest.raises(RuntimeError, match="must be entered before use"),
        ):
            handle.tools()
