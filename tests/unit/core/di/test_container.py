"""Unit tests for LoomContainer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope


class IService:
    pass


class ConcreteService(IService):
    def __init__(self) -> None:
        self.created = True


class AnotherService:
    pass


class Product:
    pass


class ProductRepo:
    pass


@pytest.fixture
def container() -> LoomContainer:
    return LoomContainer()


class TestRegister:
    def test_register_class_provider(self, container: LoomContainer) -> None:
        container.register(IService, ConcreteService, scope=Scope.REQUEST)
        assert container.is_registered(IService)

    def test_register_factory_provider(self, container: LoomContainer) -> None:
        container.register(IService, lambda: ConcreteService(), scope=Scope.REQUEST)
        assert container.is_registered(IService)

    def test_register_overrides_previous(self, container: LoomContainer) -> None:
        class AltService(IService):
            pass

        container.register(IService, ConcreteService, scope=Scope.REQUEST)
        container.register(IService, AltService, scope=Scope.REQUEST)

        instance = container.resolve(IService)
        assert isinstance(instance, AltService)


class TestRegisterDefault:
    def test_registers_when_the_key_is_absent(self, container: LoomContainer) -> None:
        container.register_default(IService, ConcreteService, scope=Scope.APPLICATION)

        assert isinstance(container.resolve(IService), ConcreteService)

    def test_keeps_a_binding_already_registered(self, container: LoomContainer) -> None:
        class AltService(IService):
            pass

        container.register(IService, AltService, scope=Scope.REQUEST)
        container.register_default(IService, ConcreteService, scope=Scope.APPLICATION)

        assert isinstance(container.resolve(IService), AltService)
        assert container.resolve(IService) is not container.resolve(IService)

    def test_keeps_a_registered_instance(self, container: LoomContainer) -> None:
        own = ConcreteService()
        container.register_instance(IService, own)
        container.register_default(IService, lambda: ConcreteService(), scope=Scope.APPLICATION)

        assert container.resolve(IService) is own

    def test_a_kept_binding_never_calls_the_default_provider(
        self, container: LoomContainer
    ) -> None:
        calls: list[int] = []
        container.register(IService, ConcreteService)

        container.register_default(IService, lambda: calls.append(1), scope=Scope.APPLICATION)
        container.validate()

        assert calls == []


class TestResolve:
    @pytest.mark.parametrize("scope", [Scope.REQUEST, Scope.TRANSACTION])
    def test_resolve_non_singleton_scope_creates_new_instance(
        self,
        container: LoomContainer,
        scope: Scope,
    ) -> None:
        container.register(IService, ConcreteService, scope=scope)

        a = container.resolve(IService)
        b = container.resolve(IService)
        assert a is not b

    def test_resolve_application_scope_returns_same_instance(
        self,
        container: LoomContainer,
    ) -> None:
        container.register(IService, ConcreteService, scope=Scope.APPLICATION)

        a = container.resolve(IService)
        b = container.resolve(IService)
        assert a is b

    def test_resolve_unregistered_raises(self, container: LoomContainer) -> None:
        with pytest.raises(ResolutionError, match="IService"):
            container.resolve(IService)

    def test_resolve_lambda_provider(self, container: LoomContainer) -> None:
        sentinel = ConcreteService()
        container.register(IService, lambda: sentinel, scope=Scope.REQUEST)

        result = container.resolve(IService)
        assert result is sentinel

    def test_is_registered_true(self, container: LoomContainer) -> None:
        container.register(IService, ConcreteService)
        assert container.is_registered(IService) is True

    def test_is_registered_false(self, container: LoomContainer) -> None:
        assert container.is_registered(IService) is False


class TestValidate:
    def test_validate_instantiates_application_scope(
        self,
        container: LoomContainer,
    ) -> None:
        container.register(IService, ConcreteService, scope=Scope.APPLICATION)
        container.validate()
        resolved = cast(ConcreteService, container.resolve(IService))
        assert resolved.created is True

    def test_validate_does_not_instantiate_request_scope(
        self,
        container: LoomContainer,
    ) -> None:
        call_count = 0

        def provider() -> ConcreteService:
            nonlocal call_count
            call_count += 1
            return ConcreteService()

        container.register(IService, provider, scope=Scope.REQUEST)
        container.validate()
        assert call_count == 0

    def test_validate_raises_on_failing_application_provider(
        self,
        container: LoomContainer,
    ) -> None:
        def bad_provider() -> IService:
            raise RuntimeError("database unreachable")

        container.register(IService, bad_provider, scope=Scope.APPLICATION)

        with pytest.raises(ResolutionError, match="IService"):
            container.validate()

    def test_validate_multiple_application_bindings(
        self,
        container: LoomContainer,
    ) -> None:
        container.register(IService, ConcreteService, scope=Scope.APPLICATION)
        container.register(AnotherService, AnotherService, scope=Scope.APPLICATION)
        container.validate()


class TestRepoResolution:
    def test_register_repo_and_resolve_repo(self, container: LoomContainer) -> None:
        repo = ProductRepo()
        container.register(ProductRepo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(Product, ProductRepo)

        resolved = container.resolve_repo(Product)
        assert resolved is repo

    def test_resolve_repo_missing_mapping_raises(
        self,
        container: LoomContainer,
    ) -> None:
        with pytest.raises(ResolutionError, match="Product"):
            container.resolve_repo(Product)


class TestTokenResolution:
    def test_resolve_with_non_type_token(self, container: LoomContainer) -> None:
        @dataclass(frozen=True)
        class _Token:
            name: str

        token = _Token("repo:product")
        repo = ProductRepo()
        container.register(token, lambda: repo, scope=Scope.APPLICATION)

        resolved = container.resolve(token)
        assert resolved is repo
