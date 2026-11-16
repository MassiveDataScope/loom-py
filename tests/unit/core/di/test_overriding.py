"""An alias serves its target's own singleton, and a replaced singleton is put back."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope


class AcmeStore:
    pass


class AcmeReadable:
    pass


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

    def test_is_served_by_the_supplier_of_its_target(self, container: LoomContainer) -> None:
        container.resolve(AcmeReadable)

        assert container._served[AcmeReadable] is container._served[AcmeStore]

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

    @pytest.mark.parametrize("key", [AcmeReadable, AcmeConsumer, int])
    def test_refuses_what_is_not_an_application_binding(
        self, container: LoomContainer, key: type
    ) -> None:
        with (
            pytest.raises(ResolutionError, match="can be replaced"),
            container.overriding(key, object()),
        ):
            pass
