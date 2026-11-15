"""Singletons see no execution: a resolver reading the identity fails closed while building one."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import ExecutionRequired, ExecutionScope
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case import Agent
from loom.core.use_case.kinds import declare_kinds


class AcmeTriage:
    """Holds an agent handle; bound as a singleton."""

    def __init__(self, agent: Any = Agent("acme-triage")) -> None:
        self.agent = agent


def _legacy(name: str, identity: Identity) -> tuple[str, Identity]:
    """A legacy agent resolver: it takes the caller, which the shim reads per resolve."""
    return name, identity


def _bound(scope: Scope) -> LoomContainer:
    container = LoomContainer()
    declare_kinds(container.kinds)
    RuntimeExecutor(UseCaseCompiler(), resolvers=container.kinds, agent_resolver=_legacy)
    container.register(AcmeTriage, AcmeTriage, scope=scope)
    return container


class TestResolutionOutsideAnExecution:
    def test_a_singleton_first_resolved_inside_an_execution_through_a_legacy_resolver_fails(
        self,
    ) -> None:
        container = _bound(Scope.APPLICATION)

        with ExecutionScope(ANONYMOUS), pytest.raises(ExecutionRequired):
            container.resolve(AcmeTriage)

    def test_a_per_execution_adapter_resolves_its_handle_as_the_caller(self) -> None:
        container = _bound(Scope.REQUEST)

        with ExecutionScope(ANONYMOUS):
            triage = container.resolve(AcmeTriage)

        assert triage.agent == ("acme-triage", ANONYMOUS)

    def test_a_per_execution_adapter_resolved_outside_an_execution_fails_closed(self) -> None:
        container = _bound(Scope.REQUEST)

        with pytest.raises(ExecutionRequired):
            container.resolve(AcmeTriage)
