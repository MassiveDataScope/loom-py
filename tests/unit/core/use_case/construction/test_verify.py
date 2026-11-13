"""``verify()`` keeps its checks and compiles every registered class; ``build`` compiles none."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.di import container as container_module
from loom.core.di.container import LoomContainer
from loom.core.di.graph import DependencyGraphError
from loom.core.di.scope import Scope
from loom.core.model import LoomStruct
from loom.core.repository.abc import Listable
from loom.core.repository.dependency import classify_key
from loom.core.use_case.errors import UnservedModelError
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase


class AcmeItem(LoomStruct):
    id: int


class AcmeItemRepo:
    pass


class AcmeClock:
    pass


class AcmeTick:
    def __init__(self, clock: AcmeClock) -> None:
        self.clock = clock


class AcmeListItems(UseCase[AcmeItem, str, Listable[AcmeItem]]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def _container() -> LoomContainer:
    container = LoomContainer(classify_key=classify_key)
    container.register(AcmeClock, AcmeClock, scope=Scope.APPLICATION)
    container.register(AcmeItemRepo, AcmeItemRepo, scope=Scope.APPLICATION)
    container.register_repo(AcmeItem, AcmeItemRepo)
    container.register(Listable[AcmeItem], AcmeItemRepo, scope=Scope.APPLICATION)
    return container


def test_no_build_compiles_after_verify(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = UseCaseFactory(_container())
    factory.register(AcmeTick)
    factory.register(AcmeListItems)
    compiles: list[type[Any]] = []
    compile_class = container_module.compile_class

    def counting(container: LoomContainer, cls: type[Any]) -> Any:
        compiles.append(cls)
        return compile_class(container, cls)

    monkeypatch.setattr(container_module, "compile_class", counting)

    factory.verify()
    for _ in range(3):
        factory.build(AcmeTick)
        factory.build(AcmeListItems)

    assert compiles == [AcmeTick, AcmeListItems]


def test_verify_names_the_path_to_a_missing_key() -> None:
    factory = UseCaseFactory(LoomContainer(classify_key=classify_key))
    factory.register(AcmeTick)

    with pytest.raises(DependencyGraphError) as caught:
        factory.verify()

    assert str(caught.value) == "AcmeTick -> clock: AcmeClock: no binding is registered for it"


def test_verify_names_a_main_repository_lacking_its_capability() -> None:
    container = LoomContainer(classify_key=classify_key)
    container.register(AcmeItemRepo, AcmeItemRepo, scope=Scope.APPLICATION)
    container.register_repo(AcmeItem, AcmeItemRepo)
    factory = UseCaseFactory(container)
    factory.register(AcmeListItems)

    with pytest.raises(DependencyGraphError) as caught:
        factory.verify()

    assert str(caught.value) == (
        "AcmeListItems -> main_repo: Listable[AcmeItem]: no binding is registered for it"
    )


def test_verify_refuses_an_unmapped_main_repository_before_compiling() -> None:
    factory = UseCaseFactory(LoomContainer(classify_key=classify_key))
    factory.register(AcmeListItems)

    with pytest.raises(UnservedModelError):
        factory.verify()


def test_a_main_repository_lacking_its_capability_still_builds_unverified() -> None:
    container = LoomContainer(classify_key=classify_key)
    container.register(AcmeItemRepo, AcmeItemRepo, scope=Scope.APPLICATION)
    container.register_repo(AcmeItem, AcmeItemRepo)

    built = UseCaseFactory(container).build(AcmeListItems)

    assert isinstance(built.main_repo, AcmeItemRepo)
