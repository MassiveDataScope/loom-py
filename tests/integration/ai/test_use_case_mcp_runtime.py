"""``AgentRuntime`` opening and validating a use-case-only MCP server (T203).

A ``Mcp()`` marker's server never appears in any compiled agent plan, so it
would be invisible to ``_open_clients``/``_verify_tool_filters`` unless its
own capability is folded into their inputs. This module pins that folding,
both of its early-return traps (``_open_clients``'s ``if not mcp and not
a2a``, ``_verify_tool_filters``'s ``if not targets``), the standalone
``include`` check, and the grant a resolved handle would read from.

Every dependency is a local stub: no network, no credential, no token.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._grants import McpGrantView
from loom.ai.runtime._lifecycle import UseCaseMcpGrant
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingMcpSession,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_SERVER_A = "alpha-tools"
_SERVER_B = "beta-tools"
_USECASE = "orders.get_order_status"
_PARAMETER = "gateway"


def _codes(error: AgentCompilationError) -> set[AgentErrorCode]:
    return {issue.code for issue in error.issues}


def _grant(
    server: str,
    *,
    include: tuple[str, ...],
    usecase: str = _USECASE,
    parameter: str = _PARAMETER,
) -> UseCaseMcpGrant:
    return UseCaseMcpGrant(
        capability=make_mcp_capability(server, include=include),
        usecase=usecase,
        parameter=parameter,
    )


def _runtime(
    *,
    plans: tuple[object, ...] = (),
    clients: dict[str, StubMcpClient],
    use_case_mcp: tuple[UseCaseMcpGrant, ...],
    deps: StubDepsFactory,
    container: LoomContainer,
    remote_clients: str = "required",
) -> AgentRuntime:
    return AgentRuntime(
        plans=list(plans),  # type: ignore[arg-type]
        config=make_ai_config(
            mcp_servers=make_mcp_servers(*clients), remote_clients=remote_clients
        ),
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        use_case_mcp=use_case_mcp,
    )


class TestServerDeclaredOnlyByAUseCase:
    """No agent plan names it; only the ``Mcp()`` binding does."""

    async def test_opens_and_lists_even_though_no_agent_declares_it(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]
            assert session.listed == 1
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        assert grant is not None
        assert grant.capability.server == _SERVER_A

    async def test_zero_agent_plans_still_starts_verifies_include_and_the_call_works(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        """FR-04/AC2/AC8a of spec 015: zero agent plans, use-case-only server."""
        session = RecordingMcpSession(
            tools=("search", "delete"),
            schemas=("search",),
            results={"search": {"hits": 3}},
        )
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        binding = _grant(_SERVER_A, include=("search",))
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(binding,),
            deps=deps,
            container=container,
        )

        async with runtime:
            grant = runtime.use_case_mcp_grant(_SERVER_A)
            assert grant is not None
            # The caller's own filter comes from its own binding (S6), never
            # from the shared substrate grant this runtime hands back — see
            # 'plan.md's "Where the caller's include comes from" note.
            view = McpGrantView(
                span_attributes={"mcp_server": _SERVER_A},
                capability=binding.capability,
                include=binding.capability.include,
                exclude=binding.capability.exclude,
                session=grant.session,
                catalogue=grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert view.tools() == ("search",)
            result = await view.call_untyped("search", {})
            assert result == {"hits": 3}
            with pytest.raises(AgentRunError) as excinfo:
                await view.call_untyped("delete", {})
            assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN

    async def test_an_include_matching_nothing_aborts_naming_the_use_case_parameter_and_server(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        issue = next(
            issue
            for issue in failure.value.issues
            if issue.code is AgentErrorCode.USECASE_TOOL_FILTER_MATCHES_NOTHING
        )
        assert (issue.component, issue.field) == (_USECASE, f"parameters.{_PARAMETER}")
        assert _USECASE in issue.message
        assert _PARAMETER in issue.message
        assert _SERVER_A in issue.message

    async def test_the_listings_expiry_still_aborts(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",), list_delay_ms=50)
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = AgentRuntime(
            plans=[],
            config=make_ai_config(mcp_servers=make_mcp_servers(_SERVER_A), startup_timeout_ms=10),
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.MCP_SERVER_UNREACHABLE in _codes(failure.value)


class TestServerSharedWithAnAgent:
    """An agent and a use case declare the same server."""

    async def test_the_client_and_the_listing_happen_only_once(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("delete",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]
            assert session.listed == 1
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        assert grant is not None

    async def test_the_use_cases_include_reaches_what_the_agent_excludes(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
        identity: Identity,
    ) -> None:
        session = RecordingMcpSession(
            tools=("search", "delete"), schemas=("delete",), results={"delete": {"ok": True}}
        )
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        binding = _grant(_SERVER_A, include=("delete",))
        runtime = _runtime(
            plans=(plan,),
            clients=clients,
            use_case_mcp=(binding,),
            deps=deps,
            container=container,
        )

        async with runtime:
            agent_grant = runtime.grants("analyst").mcp[_SERVER_A]
            agent_view = McpGrantView(
                span_attributes={"agent": "analyst"},
                capability=agent_grant.capability,
                include=agent_grant.capability.include,
                exclude=agent_grant.capability.exclude,
                session=agent_grant.session,
                catalogue=agent_grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert agent_view.tools() == ("search",)

            use_case_grant = runtime.use_case_mcp_grant(_SERVER_A)
            assert use_case_grant is not None
            # Same note as above: the caller's own filter is its own
            # binding's 'include', not the shared substrate grant's.
            use_case_view = McpGrantView(
                span_attributes={"mcp_server": _SERVER_A},
                capability=binding.capability,
                include=binding.capability.include,
                exclude=binding.capability.exclude,
                session=use_case_grant.session,
                catalogue=use_case_grant.catalogue,
                timeout_s=1.0,
                identity=identity,
                observability=None,
            )
            assert use_case_view.tools() == ("delete",)
            result = await use_case_view.call_untyped("delete", {})
            assert result == {"ok": True}


class TestTwoUseCasesOverTheSameServer:
    """FR-15/AC3a: each with its own ``include``, without colliding."""

    async def test_each_lookup_is_unambiguous(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search", "delete"))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(
                _grant(_SERVER_A, include=("search",), usecase="orders.first", parameter="a"),
                _grant(_SERVER_A, include=("delete",), usecase="orders.second", parameter="b"),
            ),
            deps=deps,
            container=container,
        )

        async with runtime:
            grant = runtime.use_case_mcp_grant(_SERVER_A)

        # Both bindings resolve the same server-keyed grant (FR-15): the
        # 'include' a caller sees comes from its own binding at resolution
        # time (PR3), not from this shared grant's catalogue.
        assert grant is not None
        assert {tool.name for tool in grant.catalogue} == {"search", "delete"}
        # H1: the shared substrate never collapses onto either binding's own
        # 'include' — it carries none at all, explicitly.
        assert grant.capability.include == ()
        assert grant.capability.exclude == ()


class TestEarlyReturnGuards:
    """The two guards T203 must pass through (S6, `_lifecycle.py`)."""

    async def test_zero_agent_plans_still_opens_the_server(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Mutating step 4 (contributing under the guard) must fail this test."""
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert lifecycle_log == ["open:a"]

    async def test_zero_agent_plans_still_lists_and_verifies_the_include(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Mutating step 5 (contributing under the guard) must fail this test:
        the catalogue would stay empty and the call would fail with
        TOOL_UNKNOWN against a cleanly opened server, instead of aborting
        start-up."""
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        assert AgentErrorCode.USECASE_TOOL_FILTER_MATCHES_NOTHING in _codes(failure.value)


class TestUseCaseMcpGrantBeforeEntering:
    """H4: ``use_case_mcp_grant`` requires an entered runtime, like ``_require_slot``."""

    def test_raises_runtime_error_if_never_entered(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = _runtime(
            clients={},
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        with pytest.raises(RuntimeError, match="must be entered before use"):
            runtime.use_case_mcp_grant(_SERVER_A)


class TestUseCaseGrantsAreClearedOnExit:
    """H5: ``__aexit__`` empties ``_use_case_grants`` (T203), not only ``_grants``.

    White-box: ``use_case_mcp_grant`` never observes this through its own
    guard (``_stack is None``), so the private dict is read directly, exactly
    as ``test_runtime_remote_clients.py`` already reads ``runtime._health``.
    Without this clearing, a re-entered runtime would hand out, during the
    window between ``__aexit__`` and a subsequent ``__aenter__`` that failed
    before rebuilding it, grants pointing at already-closed sessions.
    """

    async def test_the_grants_dict_is_empty_after_exit(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        runtime = _runtime(
            clients=clients,
            use_case_mcp=(_grant(_SERVER_A, include=("search",)),),
            deps=deps,
            container=container,
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is not None

        assert runtime._use_case_grants == {}


class TestToleratedUnreachableServerDoesNotTriggerTheFilter:
    """H6: the ``if catalogue is None: continue`` guard in ``_use_case_filter_issues``.

    Under ``remote_clients: optional`` a server whose connection was
    tolerated was never listed, so its ``include`` has nothing to compare
    against: this server's outage is reported by the connection check, not
    by the filter. Without this guard, an ``include`` that would in theory
    match nothing would raise ``USECASE_TOOL_FILTER_MATCHES_NOTHING`` on top of a
    server already reported unreachable, duplicating the failure with the
    wrong code.
    """

    async def test_starts_tolerating_the_failure_without_raising_filter_matches_nothing(
        self, lifecycle_log: list[str], deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        client = StubMcpClient(label="a", session=None, log=lifecycle_log, connect_error="refused")
        runtime = _runtime(
            clients={_SERVER_A: client},
            use_case_mcp=(_grant(_SERVER_A, include=("no-such-tool",)),),
            deps=deps,
            container=container,
            remote_clients="optional",
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is None


class TestAgentsOnlyDeployment:
    """A deployment without ``Mcp()`` does not change behaviour (``use_case_mcp=()``)."""

    async def test_startup_does_not_change_when_there_is_no_marker(
        self,
        lifecycle_log: list[str],
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        session = RecordingMcpSession(tools=("search",))
        clients = {_SERVER_A: StubMcpClient(label="a", session=session, log=lifecycle_log)}
        plan = make_plan(capabilities=(make_mcp_capability(_SERVER_A, include=("search",)),))
        runtime = _runtime(
            plans=(plan,), clients=clients, use_case_mcp=(), deps=deps, container=container
        )

        async with runtime:
            assert runtime.use_case_mcp_grant(_SERVER_A) is None
            assert runtime.has_agent("analyst")
