"""``compiled(type)``: the closure a class loom builds is built by, compiled once."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di import container as container_module
from loom.core.di.container import LoomContainer
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from loom.core.repository.dependency import classify_key
from loom.core.use_case.factory import UseCaseFactory


class AcmeLedger:
    pass


class AcmeOtherLedger(AcmeLedger):
    pass


class AcmePosting:
    def __init__(self, ledger: AcmeLedger) -> None:
        self.ledger = ledger


class AcmeTagged:
    def __init__(self, ledger: AcmeLedger, tags: tuple[str, ...]) -> None:
        self.ledger, self.tags = ledger, tags


def _factory() -> UseCaseFactory:
    container = LoomContainer(classify_key=classify_key)
    container.register(AcmeLedger, AcmeLedger, scope=Scope.APPLICATION)
    return UseCaseFactory(container)


def _counting_compiles(monkeypatch: pytest.MonkeyPatch) -> list[type[Any]]:
    compiled: list[type[Any]] = []
    compile_class = container_module.compile_class

    def counting(container: LoomContainer, cls: type[Any]) -> Any:
        compiled.append(cls)
        return compile_class(container, cls)

    monkeypatch.setattr(container_module, "compile_class", counting)
    return compiled


def test_the_plan_builds_a_fresh_instance_with_its_dependencies() -> None:
    factory = _factory()
    plan = factory.compiled(AcmePosting)

    first, second = plan(), plan()

    assert isinstance(first, AcmePosting)
    assert first is not second
    assert first.ledger is second.ledger


def test_a_plan_is_compiled_once(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = _factory()
    compiles = _counting_compiles(monkeypatch)

    assert factory.compiled(AcmePosting) is factory.compiled(AcmePosting)
    factory.build(AcmePosting)

    assert compiles == [AcmePosting]


def test_a_registration_compiles_the_plan_again_with_the_new_binding() -> None:
    factory = _factory()
    factory.build(AcmePosting)

    factory._container.register(AcmeLedger, AcmeOtherLedger, scope=Scope.APPLICATION)

    assert isinstance(factory.build(AcmePosting).ledger, AcmeOtherLedger)


def test_a_root_with_a_parameter_loom_cannot_provide_fails_at_registration() -> None:
    factory = _factory()

    with pytest.raises(DependencyGraphError) as caught:
        factory.register(AcmeTagged)

    assert str(caught.value) == (
        "AcmeTagged: tags: tuple[str, ...] names nothing the container can serve"
    )


def test_a_root_whose_binding_comes_later_registers() -> None:
    factory = UseCaseFactory(LoomContainer(classify_key=classify_key))

    factory.register(AcmePosting)
