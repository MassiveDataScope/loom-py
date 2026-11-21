"""``LoomContainer.compiled(type)``: the plan a class loom builds is built by, compiled once."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di.compiler import SupplierCompiler
from loom.core.di.container import LoomContainer
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository


class AcmePlanLedger:
    pass


class AcmePlanOtherLedger(AcmePlanLedger):
    pass


class AcmePosting:
    def __init__(self, ledger: AcmePlanLedger) -> None:
        self.ledger = ledger


class AcmeTagged:
    def __init__(self, ledger: AcmePlanLedger, tags: tuple[str, ...]) -> None:
        self.ledger, self.tags = ledger, tags


def _factory() -> UseCaseFactory:
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    container.register(AcmePlanLedger, AcmePlanLedger, scope=Scope.APPLICATION)
    return UseCaseFactory(container)


def _counting_compiles(monkeypatch: pytest.MonkeyPatch) -> list[type[Any]]:
    compiled: list[type[Any]] = []
    plan = SupplierCompiler.plan

    def counting(compiler: SupplierCompiler, cls: type[Any]) -> Any:
        compiled.append(cls)
        return plan(compiler, cls)

    monkeypatch.setattr(SupplierCompiler, "plan", counting)
    return compiled


class TestConstructionPlans:
    def test_the_plan_builds_a_fresh_instance_with_its_dependencies(self) -> None:
        factory = _factory()
        plan = factory._container.compiled(AcmePosting)

        first, second = plan(), plan()

        assert isinstance(first, AcmePosting)
        assert first is not second
        assert first.ledger is second.ledger

    def test_a_plan_is_compiled_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        factory = _factory()
        compiles = _counting_compiles(monkeypatch)

        assert factory._container.compiled(AcmePosting) is factory._container.compiled(AcmePosting)
        factory.build(AcmePosting)

        assert compiles == [AcmePosting]

    def test_a_registration_compiles_the_plan_again_with_the_new_binding(self) -> None:
        factory = _factory()
        factory.build(AcmePosting)

        factory._container.register(AcmePlanLedger, AcmePlanOtherLedger, scope=Scope.APPLICATION)

        assert isinstance(factory.build(AcmePosting).ledger, AcmePlanOtherLedger)

    def test_a_root_with_a_parameter_loom_cannot_provide_fails_at_registration(self) -> None:
        factory = _factory()

        with pytest.raises(DependencyGraphError) as caught:
            factory.register(AcmeTagged)

        assert str(caught.value) == (
            "AcmeTagged: tags: tuple[str, ...] names nothing the container can serve"
        )

    def test_a_root_whose_binding_comes_later_registers(self) -> None:
        factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))

        factory.register(AcmePosting)
