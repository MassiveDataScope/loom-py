"""``build(type)`` runs the class's compiled plan, in the execution in progress if any."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionRequired, ExecutionScope
from loom.core.identity import ANONYMOUS, Identity
from loom.core.repository.dependency import classify_key
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.kinds import CALLER, CallerResolver, use_case_kinds
from loom.core.use_case.markers import Caller


class AcmeBuildSession:
    pass


class AcmeSessionOrders:
    def __init__(self, session: AcmeBuildSession) -> None:
        self.session = session


class AcmeInvoices:
    def __init__(self, session: AcmeBuildSession) -> None:
        self.session = session


class AcmeBilling:
    def __init__(self, orders: AcmeSessionOrders, invoices: AcmeInvoices) -> None:
        self.orders, self.invoices = orders, invoices


class AcmeAsCaller:
    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller


def _factory() -> UseCaseFactory:
    container = LoomContainer(classify_key=classify_key)
    for key in (AcmeBuildSession, AcmeSessionOrders, AcmeInvoices):
        container.register(key, key, scope=Scope.REQUEST)
    return UseCaseFactory(container)


class TestConstructionBuild:
    def test_a_build_outside_an_execution_builds_each_per_execution_dependency_fresh(self) -> None:
        billing = _factory().build(AcmeBilling)

        assert billing.orders.session is not billing.invoices.session

    def test_builds_inside_an_execution_share_its_per_execution_dependencies(self) -> None:
        factory = _factory()

        with ExecutionScope(ANONYMOUS):
            first, second = factory.build(AcmeBilling), factory.build(AcmeBilling)

        assert first.orders.session is first.invoices.session is second.orders.session

    def test_a_constructor_caller_needs_an_execution_to_build(self) -> None:
        container = LoomContainer(classify_key=classify_key)
        use_case_kinds(container.kinds)
        container.kinds.bind(CALLER, CallerResolver())
        factory = UseCaseFactory(container)

        with pytest.raises(ExecutionRequired):
            factory.build(AcmeAsCaller)
        with ExecutionScope(ANONYMOUS):
            built: Any = factory.build(AcmeAsCaller)

        assert built.caller is ANONYMOUS
