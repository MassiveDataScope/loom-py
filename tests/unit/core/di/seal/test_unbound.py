"""Refusal 7 (F6): a binding seal builds may not declare a kind no resolver is bound for."""

from __future__ import annotations

from typing import Any

import pytest

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition import Project
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.errors import ContainerSealed, DependencyWarning
from loom.core.di.graph import (
    ClassNode,
    DependencyGraphError,
    refuse_unbound,
)
from loom.core.di.scope import Scope
from loom.core.execution import Kind
from loom.core.use_case import Mcp, UseCase
from tests.helpers.composition import Serving, composition_project
from tests.unit.composition.phases.components import AcmeMcpUser, AcmeRecord
from tests.unit.composition.phases.paths import SQLITE
from tests.unit.core.di.graph import acme

ACME_CRM = Kind("acme-crm")


class AcmeCrmMarker:
    """A constructor marker of :data:`ACME_CRM`."""


class AcmeCrmAdapter:
    def __init__(self, crm: object = AcmeCrmMarker()) -> None:
        self.crm = crm


class AcmeUsesCrm:
    def __init__(self, adapter: AcmeCrmAdapter) -> None:
        self.adapter = adapter


def _bound(scope: Scope) -> LoomContainer:
    container = acme.container()
    container.kinds.declare_type(AcmeCrmMarker, ACME_CRM)
    container.register(AcmeCrmAdapter, AcmeCrmAdapter, scope=scope)
    return container


def _refused(container: LoomContainer, *skip: Kind) -> None:
    rule = refuse_unbound(container.kinds, skip)
    container.graph.analyse([ClassNode(AcmeUsesCrm)], (rule,))


class TestUnboundKind:
    def test_a_singleton_declaring_an_unbound_kind_is_refused_with_its_path(self) -> None:
        container = _bound(Scope.APPLICATION)

        with pytest.raises(
            DependencyGraphError,
            match=(
                "AcmeUsesCrm -> adapter: AcmeCrmAdapter: crm: no resolver is bound for 'acme-crm'"
            ),
        ):
            _refused(container)

    def test_a_skipped_kind_is_not_refused(self) -> None:
        _refused(_bound(Scope.APPLICATION), ACME_CRM)

    def test_a_per_execution_adapter_is_not_refused(self) -> None:
        _refused(_bound(Scope.REQUEST))

    def test_a_bound_kind_is_not_refused(self) -> None:
        container = _bound(Scope.APPLICATION)
        container.kinds.register(ACME_CRM, acme_resolver())

        _refused(container)

    def test_seal_refuses_a_reached_singleton_declaring_an_unbound_kind(self) -> None:
        container = _bound(Scope.APPLICATION)

        with pytest.raises(DependencyGraphError, match="crm: no resolver is bound for 'acme-crm'"):
            container.seal(acme.analysed(container, AcmeUsesCrm))

    def test_seal_defers_an_unbound_kind_it_is_told_to_and_refuses_a_later_resolver(self) -> None:
        container = _bound(Scope.APPLICATION)

        with pytest.warns(DependencyWarning, match="AcmeCrmAdapter is not built"):
            container.seal(acme.analysed(container, AcmeUsesCrm), deferred=(ACME_CRM,))
        with pytest.raises(ResolutionError, match="no resolver is bound for 'acme-crm'"):
            container.resolve(AcmeCrmAdapter)
        resolver = acme_resolver()
        with pytest.raises(ContainerSealed, match="the container is sealed"):
            container.kinds.register(ACME_CRM, resolver)


class AcmeCrmPort:
    """What the use case asks for."""


class AcmeMcpCrm(AcmeCrmPort):
    def __init__(self, crm: McpHandle = Mcp("acme-crm", include=["find_*"])) -> None:
        self.crm = crm


class AcmeUsesCrmPort(UseCase[AcmeRecord, None]):
    def __init__(self, crm: AcmeCrmPort) -> None:
        super().__init__()
        self.crm = crm

    async def execute(self) -> None:
        return None


def _binds_crm(container: LoomContainer) -> None:
    container.register(AcmeCrmPort, AcmeMcpCrm, scope=Scope.APPLICATION)


def _project() -> Project:
    return composition_project(
        ConfigContext.from_dict(
            {
                "app": {"name": "acme", "job_service": "inline"},
                "stores": {"default": SQLITE, "settings": {"cursors": {"signing": "ephemeral"}}},
            }
        )
    )


class TestUnboundMcpAdapter:
    def test_an_agentless_composition_refuses_the_adapter_in_the_ai_phase_before_seal(
        self,
    ) -> None:
        """No MCP resolver is ever bound without agents: the AI phase refuses first (T11)."""
        adapter = Serving(AcmeRecord, use_cases=(AcmeUsesCrmPort,), overrides=(_binds_crm,))
        project = _project()

        with pytest.raises(AgentCompilationError) as caught:
            project.compose(adapter)

        assert {issue.code for issue in caught.value.issues} == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        assert "AcmeUsesCrmPort -> crm: AcmeCrmPort: crm" in str(caught.value)

    def test_a_lenient_composition_does_not_refuse_an_unbound_mcp_adapter(self) -> None:
        adapter = Serving(
            AcmeRecord, use_cases=(AcmeUsesCrmPort,), lenient=True, overrides=(_binds_crm,)
        )
        project = _project()

        with pytest.warns(DependencyWarning, match="AcmeCrmPort is not built"):
            application = project.compose(adapter)

        with pytest.raises(ResolutionError, match="no resolver is bound for 'mcp'"):
            application.kernel.container.resolve(AcmeCrmPort)

    def test_an_execute_marker_with_no_resolver_is_not_refused_at_seal(self) -> None:
        adapter = Serving(AcmeRecord, use_cases=(AcmeMcpUser,), lenient=True)

        application = _project().compose(adapter)

        assert application.kernel.compiler.get_plan(AcmeMcpUser) is not None


def acme_resolver() -> Any:
    class Resolver:
        def resolve(self, declaration: object, annotation: object) -> object:
            return "acme-crm-handle"

    return Resolver()


class TestUnbuiltSingletonWarning:
    def test_the_unbuilt_singleton_warning_points_at_the_caller_of_seal(self) -> None:
        container = _bound(Scope.APPLICATION)

        with pytest.warns(DependencyWarning, match="AcmeCrmAdapter is not built") as caught:
            container.seal(acme.analysed(container, AcmeUsesCrm), deferred=(ACME_CRM,))

        assert caught[0].filename == __file__
