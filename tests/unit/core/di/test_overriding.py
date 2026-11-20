"""An alias serves its target's own singleton, and a replaced singleton is put back."""

from __future__ import annotations

from dataclasses import dataclass
from types import FunctionType

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope


class AcmeStore:
    pass


class AcmeReadable:
    pass


@dataclass(frozen=True)
class AcmeLabel:
    text: str


class AcmeConsumer:
    def __init__(self, readable: AcmeReadable) -> None:
        self.readable = readable


class CountedBuilds:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> AcmeStore:
        self.calls += 1
        return AcmeStore()


@pytest.fixture
def builds() -> CountedBuilds:
    return CountedBuilds()


@pytest.fixture
def container(builds: CountedBuilds) -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeStore, builds, scope=Scope.APPLICATION)
    container.register_alias(AcmeReadable, AcmeStore)
    container.register(AcmeConsumer, AcmeConsumer, scope=Scope.REQUEST)
    container.seal()
    return container


class TestAlias:
    def test_serves_the_instance_of_its_target_on_every_resolve(
        self, container: LoomContainer, builds: CountedBuilds
    ) -> None:
        resolved = {id(container.resolve(key)) for key in (AcmeReadable, AcmeStore) * 50}

        assert len(resolved) == 1
        assert builds.calls == 1

    def test_is_served_as_a_constant_once_the_container_is_sealed(
        self, container: LoomContainer
    ) -> None:
        container.resolve(AcmeReadable)
        container.resolve(AcmeStore)

        assert not any(
            isinstance(container._served[key], FunctionType) for key in (AcmeReadable, AcmeStore)
        )

    def test_is_served_as_a_constant_once_resolving_it_built_its_target(
        self, builds: CountedBuilds
    ) -> None:
        container = LoomContainer()
        container.register(AcmeStore, builds, scope=Scope.APPLICATION)
        container.register_alias(AcmeReadable, AcmeStore)

        container.resolve(AcmeReadable)

        assert not any(
            isinstance(container._served[key], FunctionType) for key in (AcmeReadable, AcmeStore)
        )

    def test_is_injected_as_the_instance_of_its_target(self, container: LoomContainer) -> None:
        assert container.compiled(AcmeConsumer)().readable is container.resolve(AcmeStore)


class TestOverriding:
    def test_serves_the_replacement_to_the_key_its_aliases_and_a_cached_plan(
        self, container: LoomContainer
    ) -> None:
        original, double = container.resolve(AcmeStore), AcmeStore()
        plan = container.compiled(AcmeConsumer)
        assert plan().readable is original

        with container.overriding(AcmeStore, double):
            assert container.resolve(AcmeStore) is double
            assert container.resolve(AcmeReadable) is double
            assert container.compiled(AcmeConsumer)().readable is double

        assert container.resolve(AcmeReadable) is original
        assert container.compiled(AcmeConsumer)().readable is original

    def test_puts_the_original_back_when_the_block_raises(
        self, container: LoomContainer, builds: CountedBuilds
    ) -> None:
        original = container.resolve(AcmeStore)

        with pytest.raises(RuntimeError), container.overriding(AcmeStore, AcmeStore()):
            raise RuntimeError("boom")

        assert container.resolve(AcmeReadable) is original
        assert builds.calls == 1

    @pytest.mark.parametrize(
        ("key", "message"),
        [
            (AcmeReadable, "alias of AcmeStore.*Override.repository"),
            (AcmeConsumer, "bound per execution"),
            (int, "unbound"),
        ],
    )
    def test_refuses_what_cannot_be_replaced_saying_why(
        self, container: LoomContainer, key: type, message: str
    ) -> None:
        with pytest.raises(ResolutionError, match=message), container.overriding(key, object()):
            pass

    def test_refuses_an_exit_out_of_order_and_leaves_no_double_behind(
        self, container: LoomContainer
    ) -> None:
        original = container.resolve(AcmeStore)
        first, second = AcmeStore(), AcmeStore()
        outer = container.overriding(AcmeStore, first)
        inner = container.overriding(AcmeStore, second)
        outer.__enter__()
        inner.__enter__()

        with pytest.raises(ResolutionError, match="out of order"):
            outer.__exit__(None, None, None)
        assert container.resolve(AcmeStore) is second
        inner.__exit__(None, None, None)

        assert container.resolve(AcmeStore) is original
        assert container.resolve(AcmeReadable) is original

    def test_exits_the_override_entered_by_identity_when_two_are_equal(
        self, container: LoomContainer
    ) -> None:
        original = container.resolve(AcmeStore)
        first, second = AcmeLabel("same"), AcmeLabel("same")
        outer = container.overriding(AcmeStore, first)
        inner = container.overriding(AcmeStore, second)
        outer.__enter__()
        inner.__enter__()

        with pytest.raises(ResolutionError, match="out of order"):
            outer.__exit__(None, None, None)
        assert container.resolve(AcmeStore) is second
        inner.__exit__(None, None, None)

        assert container.resolve(AcmeStore) is original


class TestRegisterAlias:
    def test_refuses_an_unbound_target(self) -> None:
        with pytest.raises(ResolutionError, match="unbound"):
            LoomContainer().register_alias(AcmeReadable, AcmeStore)

    def test_follows_the_scope_its_target_has_when_validated(self, builds: CountedBuilds) -> None:
        container = LoomContainer()
        container.register(AcmeStore, builds, scope=Scope.APPLICATION)
        container.register_alias(AcmeReadable, AcmeStore)
        container.register(AcmeStore, builds, scope=Scope.REQUEST)

        container.validate()

        assert builds.calls == 0

    def test_takes_the_scope_of_its_target(self) -> None:
        container = LoomContainer()
        container.register(AcmeStore, AcmeStore, scope=Scope.REQUEST)
        container.register_alias(AcmeReadable, AcmeStore)

        assert container.resolve(AcmeReadable) is not container.resolve(AcmeReadable)
