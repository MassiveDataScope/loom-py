"""A standalone kernel takes the legacy agent and MCP resolvers, as ``RuntimeExecutor`` does."""

from __future__ import annotations

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.core.bootstrap import BootstrapError, create_kernel
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionRequired, ExecutionScope
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case import Agent, Mcp


class AcmeResolverConfig:
    pass


class AcmeCrm:
    def __init__(
        self,
        crm: McpHandle = Mcp("acme-crm", include=["find_*"]),
        triage: AgentHandle[str] = Agent("acme-triage"),
    ) -> None:
        self.crm, self.triage = crm, triage


def _mcp(server: str, include: tuple[str, ...], identity: Identity) -> object:
    return ("mcp", server, include, identity)


def _agent(name: str, identity: Identity) -> object:
    return ("agent", name, identity)


def _binds(scope: Scope) -> object:
    def module(container: LoomContainer) -> None:
        container.register(AcmeCrm, AcmeCrm, scope=scope)

    return module


class TestKernelLegacyResolvers:
    def test_a_per_execution_adapter_gets_handles_from_the_kernel_resolvers(self) -> None:
        kernel = create_kernel(
            config=AcmeResolverConfig(),
            use_cases=(),
            modules=(_binds(Scope.REQUEST),),  # type: ignore[arg-type]
            agent_resolver=_agent,
            mcp_resolver=_mcp,
        )

        with ExecutionScope(ANONYMOUS):
            crm = kernel.container.resolve(AcmeCrm)

        assert crm.crm == ("mcp", "acme-crm", ("find_*",), ANONYMOUS)
        assert crm.triage == ("agent", "acme-triage", ANONYMOUS)

    def test_until_call_time_handles_a_singleton_holding_them_cannot_be_built(self) -> None:
        with pytest.raises(BootstrapError) as caught:
            create_kernel(
                config=AcmeResolverConfig(),
                use_cases=(),
                modules=(_binds(Scope.APPLICATION),),  # type: ignore[arg-type]
                agent_resolver=_agent,
                mcp_resolver=_mcp,
            )

        cause = caught.value.__cause__
        assert isinstance(cause, ResolutionError)
        assert isinstance(cause.__cause__, ExecutionRequired)
