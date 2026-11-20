"""Lifecycle, start-up validation and shared-session tests of ``AgentHost``.

Covers T072 (open/close ordering and per-worker client sharing), T073 (tool
filters applied against the tools the server actually offers), T077 (single
exit stack, task affinity, bounded concurrent start-up), T078 (a shared MCP
session is never poisoned by a cancelled neighbour) and T079 (read-only drift
re-verified against live SQL configuration).

Every dependency is a local stub: no network, no credential, no token.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.execution import AgentHost, SharedMcpSession
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    InterleavingSensitiveSession,
    RecordingMcpSession,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_sql_capability,
    make_sql_config,
    mcp_client_factory,
    mcp_server_url,
)

_SERVER_A = "alpha-tools"
_SERVER_B = "beta-tools"
_SERVER_C = "gamma-tools"


def _codes(error: AgentCompilationError) -> set[AgentErrorCode]:
    """Return the distinct issue codes an aggregated compilation error carries."""
    return {issue.code for issue in error.issues}


def _build_runtime(
    *,
    plans: tuple[object, ...],
    clients: dict[str, StubMcpClient],
    provider: CountingEngineProvider,
    deps: StubDepsFactory,
    container: LoomContainer,
    config_kwargs: dict[str, Any] | None = None,
    sql_config: object | None = None,
) -> AgentHost:
    """Assemble an ``AgentHost`` over local stubs only."""
    return AgentHost(
        plans=list(plans),  # type: ignore[arg-type]
        config=make_ai_config(mcp_servers=make_mcp_servers(*clients), **(config_kwargs or {})),
        engine_plugin=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
        sql_config=sql_config,  # type: ignore[arg-type]
        mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
    )


class TestLifecycle:
    """Opening and closing the live clients through one exit stack (T072/T077)."""

    async def test_opens_every_client_when_the_runtime_is_entered(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``__aenter__`` opens every MCP client the plans declare."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log),
            _SERVER_B: StubMcpClient(label="b", session=RecordingMcpSession(), log=lifecycle_log),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        host = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            assert sorted(lifecycle_log) == ["open:a", "open:b"]

    async def test_closes_in_reverse_order_when_the_runtime_exits(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Clients close in strict reverse order of the order they opened in."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log),
            _SERVER_B: StubMcpClient(
                label="b",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                connect_delay_ms=20,
            ),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        host = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            pass

        assert lifecycle_log == ["open:a", "open:b", "close:b", "close:a"]

    async def test_builds_a_single_engine_across_multiple_invocations(
        self,
        lifecycle_log: list[str],
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Engines are built once per plan, never per request (FR-026)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        provider = CountingEngineProvider()
        host = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=provider,
            deps=deps,
            container=container,
        )

        async with host:
            await host.run("analyst", "p1", identity=identity)
            await host.run("analyst", "p2", identity=identity)
            async with host.run_stream("analyst", "p3", identity=identity) as stream:
                async for _ in stream:
                    pass

        assert provider.calls == ["analyst"]

    async def test_opens_the_client_once_across_multiple_invocations(
        self,
        lifecycle_log: list[str],
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The MCP client is shared per worker, not opened per call (FR-026)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        host = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            await host.run("analyst", "p1", identity=identity)
            await host.run("analyst", "p2", identity=identity)
            async with host.run_stream("analyst", "p3", identity=identity) as stream:
                async for _ in stream:
                    pass

        assert lifecycle_log.count("open:a") == 1

    async def test_refuses_the_close_when_exiting_from_another_task(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Exiting the stack from another task is refused, not silently done (T077)."""
        clients = {
            _SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=lifecycle_log)
        }
        host = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )
        await host.__aenter__()

        async def _exit_elsewhere() -> None:
            await host.__aexit__(None, None, None)

        try:
            task = asyncio.create_task(_exit_elsewhere())
            with pytest.raises(RuntimeError):
                await task
        finally:
            await host.__aexit__(None, None, None)


class TestConcurrentStartup:
    """Start-up is concurrent, bounded and never hangs (T077)."""

    async def test_starts_in_parallel_across_multiple_clients(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Two 40 ms connections must not cost 80 ms: start-up is concurrent."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a", session=RecordingMcpSession(), log=lifecycle_log, connect_delay_ms=40
            ),
            _SERVER_B: StubMcpClient(
                label="b", session=RecordingMcpSession(), log=lifecycle_log, connect_delay_ms=40
            ),
        }
        plan = make_plan(
            capabilities=(make_mcp_capability(_SERVER_A), make_mcp_capability(_SERVER_B)),
        )
        host = _build_runtime(
            plans=(plan,),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 1000},
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        async with host:
            elapsed = loop.time() - started

        assert elapsed < 0.070, f"start-up took {elapsed:.3f}s; the two clients ran in sequence"

    async def test_aborts_naming_the_server_when_it_does_not_connect(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An unreachable server aborts start-up naming its registered server."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                never_connects=True,
            )
        }
        host = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 50},
        )

        # The outer timeout is twice the declared budget: if start-up hangs the
        # test fails on TimeoutError instead of blocking the suite.
        budget = asyncio.timeout(0.1)

        with pytest.raises(AgentCompilationError) as failure:
            async with budget:
                await host.__aenter__()

        assert _SERVER_A in str(failure.value)

    async def test_reports_mcp_server_unreachable_when_it_does_not_connect(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The abort carries the stable ``MCP_SERVER_UNREACHABLE`` code."""
        clients = {
            _SERVER_A: StubMcpClient(
                label="a",
                session=RecordingMcpSession(),
                log=lifecycle_log,
                never_connects=True,
            )
        }
        host = _build_runtime(
            plans=(make_plan(capabilities=(make_mcp_capability(_SERVER_A),)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 50},
        )

        budget = asyncio.timeout(0.1)

        with pytest.raises(AgentCompilationError) as failure:
            async with budget:
                await host.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)


class TestStartupBudget:
    """``startup_timeout_ms`` bounds the whole of start-up exactly once."""

    @staticmethod
    def _slow_listing_runtime(
        *,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> AgentHost:
        """Three servers whose tool listings alone outlast a single budget."""
        servers = (_SERVER_A, _SERVER_B, _SERVER_C)
        clients = {
            server: StubMcpClient(
                label=server,
                session=RecordingMcpSession(tools=("alpha",), list_delay_ms=50),
                log=lifecycle_log,
            )
            for server in servers
        }
        plans = tuple(
            make_plan(
                f"agent-{index}",
                capabilities=(make_mcp_capability(server, include=("alpha",)),),
            )
            for index, server in enumerate(servers)
        )
        return _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            config_kwargs={"startup_timeout_ms": 80},
        )

    async def test_does_not_exceed_the_budget_across_multiple_mcp_capabilities(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Three 50 ms listings share one 80 ms budget, they do not each get one."""
        host = self._slow_listing_runtime(
            lifecycle_log=lifecycle_log, deps=deps, container=container
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(AgentCompilationError):
            await host.__aenter__()
        elapsed = loop.time() - started

        assert elapsed < 0.130, (
            f"start-up took {elapsed:.3f}s: the budget was spent once per capability"
        )

    async def test_names_the_server_when_the_budget_expires(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The expired budget is a coded issue naming the server, not a bare timeout."""
        host = self._slow_listing_runtime(
            lifecycle_log=lifecycle_log, deps=deps, container=container
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)
        assert _SERVER_C in str(failure.value)

    async def test_lists_the_tools_once_when_two_plans_share_a_server(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """One shared session means one ``list_tools`` round trip, not one per plan."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plans = (
            make_plan("first", capabilities=(make_mcp_capability(_SERVER_A, include=("alpha",)),)),
            make_plan("second", capabilities=(make_mcp_capability(_SERVER_A, include=("beta",)),)),
        )
        host = _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            assert session.listed == 1

    async def test_applies_each_plans_own_filter_when_they_share_a_server(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The single listing is still checked against every plan's own filter."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plans = (
            make_plan("first", capabilities=(make_mcp_capability(_SERVER_A, include=("alpha",)),)),
            make_plan("second", capabilities=(make_mcp_capability(_SERVER_A, include=("gamma",)),)),
        )
        host = _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)
        assert "second" in str(failure.value)


class TestToolFilter:
    """Declared filters are applied against the real tool list (T073)."""

    async def test_aborts_when_the_filter_matches_no_tool(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A filter matching none of the offered tools fails start-up (FR-025)."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("gamma",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_starts_when_the_filter_matches_a_tool(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A filter keeping at least one offered tool starts up normally."""
        session = RecordingMcpSession(tools=("alpha", "beta"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("alpha",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            assert host.has_agent("analyst")

    async def test_starts_when_a_glob_matches_part_of_the_tools(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``include`` is glob-matched, so a pattern selects a family of tools."""
        session = RecordingMcpSession(tools=("search_web", "search_docs", "delete_index"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("search_*",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        async with host:
            assert host.has_agent("analyst")

    async def test_aborts_when_the_glob_matches_no_tool(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A glob is matched, not compared literally: an unmatched one still fails."""
        session = RecordingMcpSession(tools=("search_web", "delete_index"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("write_*",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_aborts_when_exclude_empties_what_the_glob_includes(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``exclude`` is applied after ``include`` and can empty the selection."""
        session = RecordingMcpSession(tools=("search_web", "search_docs"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("search_*",), exclude=("search_*",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)

    async def test_names_the_registered_server_and_not_its_url_when_the_filter_fails(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The issue names the registered server: a URL would publish the topology."""
        session = RecordingMcpSession(tools=("alpha",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        capability = make_mcp_capability(_SERVER_A, include=("gamma",))
        host = _build_runtime(
            plans=(make_plan(capabilities=(capability,)),),
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        message = str(failure.value)
        assert _SERVER_A in message
        assert mcp_server_url(_SERVER_A) not in message


class TestReadOnlyDrift:
    """Live SQL configuration is re-verified at start-up (T079, FR-046)."""

    async def test_aborts_when_the_connection_is_no_longer_read_only(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A plan compiled against a read-only connection refuses a writable one."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        host = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=False),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert AgentErrorCode.SQL_READONLY_DRIFT in _codes(failure.value)

    async def test_names_the_connection_when_there_is_drift(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The drift issue names the connection the operator must fix."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        host = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=False),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert "reporting" in str(failure.value)

    async def test_starts_when_the_connection_is_still_read_only(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """No drift means start-up proceeds with the SQL capability live."""
        plan = make_plan(capabilities=(make_sql_capability("reporting", readonly=True),))
        host = _build_runtime(
            plans=(plan,),
            clients={},
            provider=CountingEngineProvider(engines={"analyst": ScriptedEngine()}),
            deps=deps,
            container=container,
            sql_config=make_sql_config("reporting", readonly=True),
        )

        async with host:
            assert host.agent_names() == ("analyst",)


class TestSharedMcpSession:
    """A cancelled caller must not poison a shared JSON-RPC session (T078)."""

    async def test_completes_the_in_flight_call_when_the_caller_is_cancelled(self) -> None:
        """The shielded in-flight call runs to completion despite cancellation."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        assert session.completed == ["a"], "the in-flight JSON-RPC call was abandoned mid-frame"

    async def test_returns_its_own_result_when_the_neighbour_is_cancelled(self) -> None:
        """The surviving caller gets its own answer, never the cancelled one's."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        second = asyncio.create_task(shared.call_tool("echo", {"token": "b"}))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        result = await second
        assert result.structured == "b"

    async def test_does_not_interleave_calls_when_they_share_the_session(self) -> None:
        """Two concurrent runs over one session are serialised, never interleaved."""
        session = InterleavingSensitiveSession(delay_ms=20)
        shared = SharedMcpSession(session, label="alpha")  # type: ignore[arg-type]

        first = asyncio.create_task(shared.call_tool("echo", {"token": "a"}))
        while not session.started:
            await asyncio.sleep(0)
        second = asyncio.create_task(shared.call_tool("echo", {"token": "b"}))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await second

        assert session.interleaved is False


class TestOneConnectionPerServerName:
    """One name, one connection: the worker shares a client between agents."""

    def _host(
        self,
        plans: tuple[object, ...],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> AgentHost:
        clients = {_SERVER_A: StubMcpClient(label="a", session=RecordingMcpSession(), log=[])}
        return _build_runtime(
            plans=plans,
            clients=clients,
            provider=CountingEngineProvider(),
            deps=deps,
            container=container,
        )

    async def test_aborts_naming_both_agents_when_two_grants_of_one_name_disagree(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A divergent connection fact is a boot failure, not a silent swap.

        The de-duplication keeps the first grant of a name, so the second agent
        would otherwise run against the first agent's URL and credential.
        """
        first = make_plan("reader", capabilities=(make_mcp_capability(_SERVER_A),))
        second = make_plan(
            "writer",
            capabilities=(
                CompiledMcpCapability(server=_SERVER_A, url="https://elsewhere.internal/mcp"),
            ),
        )
        host = self._host((first, second), deps, container)

        with pytest.raises(AgentCompilationError) as failure:
            await host.__aenter__()

        assert _codes(failure.value) == {AgentErrorCode.MCP_CONNECTION_CONFLICT}
        message = str(failure.value)
        assert "reader" in message
        assert "writer" in message
        assert _SERVER_A in message

    async def test_starts_when_two_grants_of_one_name_differ_only_in_their_filter(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``include``/``exclude`` are per-agent views, not connection facts."""
        plans = (
            make_plan("reader", capabilities=(make_mcp_capability(_SERVER_A, include=("alpha",)),)),
            make_plan("writer", capabilities=(make_mcp_capability(_SERVER_A, exclude=("alpha",)),)),
        )
        host = self._host(plans, deps, container)

        async with host:
            assert host.agent_names() == ("reader", "writer")
