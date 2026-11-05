"""Capability detection and default DI keys of the repository registration module."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import msgspec

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.abc import (
    BulkCreatable,
    Countable,
    Creatable,
    Deletable,
    Listable,
    Readable,
    Updatable,
)
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.registration import (
    RepositoryDecorator,
    build_repository_registration_module,
    capabilities_of,
)
from loom.core.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryRegistration,
    RepositoryToken,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy


class Widget(BaseModel):
    __tablename__ = "registration_widgets_fixture"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=50)


class _BulkWidgetRepository(Creatable[Widget], BulkCreatable[Widget]):
    async def create(self, data: msgspec.Struct) -> Widget:  # pragma: no cover - never invoked
        raise AssertionError("not exercised")

    async def create_many(
        self, data: Sequence[msgspec.Struct]
    ) -> tuple[Widget, ...]:  # pragma: no cover - never invoked
        raise AssertionError("not exercised")


def _unused_builder(
    context: RepositoryBuildContext, registration: RepositoryRegistration
) -> Any:  # pragma: no cover - never invoked
    raise AssertionError("not exercised")


def _container(default_repository_type: type | None) -> LoomContainer:
    container = LoomContainer()
    build_repository_registration_module(
        models=(Widget,),
        build_registered_repository=_unused_builder,
        default_repository_type=default_repository_type,
        default_builder=_default_builder,
        store=DEFAULT_STORE,
    )(container)
    return container


class TestCapabilitiesOf:
    def test_dynamodb_declares_only_the_key_operations(self) -> None:
        assert set(capabilities_of(RepositoryDynamoDB)) == {
            Readable,
            Creatable,
            Updatable,
            Deletable,
        }

    def test_detects_bulk_creatable(self) -> None:
        assert set(capabilities_of(_BulkWidgetRepository)) == {Creatable, BulkCreatable}

    def test_keeps_the_declaration_order(self) -> None:
        assert capabilities_of(RepositorySQLAlchemy) == (
            Readable,
            Creatable,
            BulkCreatable,
            Updatable,
            Deletable,
            Listable,
            Countable,
        )


class TestDefaultKeys:
    def test_derived_from_the_default_repository_type(self) -> None:
        container = _container(_BulkWidgetRepository)

        assert container.is_registered(Creatable[Widget])
        assert container.is_registered(BulkCreatable[Widget])
        assert not container.is_registered(Listable[Widget])
        assert not container.is_registered(Readable[Widget])

    def test_no_default_type_binds_no_capability(self) -> None:
        container = _container(None)

        assert container.is_registered(RepositoryToken(Widget))
        assert not container.is_registered(Readable[Widget])
        assert not container.is_registered(Creatable[Widget])


class _Wrapped:
    def __init__(self, repository: Any) -> None:
        self.repository = repository


class _CountingDecorator:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, repository: Any) -> Any:
        self.calls += 1
        return _Wrapped(repository)


def _default_builder(context: RepositoryBuildContext) -> Any:
    return _BulkWidgetRepository()


def _container_with_default_builder() -> LoomContainer:
    container = _container(_BulkWidgetRepository)
    container.register(DefaultRepositoryBuilder, lambda: _default_builder, scope=Scope.APPLICATION)
    return container


class TestProviderMemo:
    def test_every_key_of_the_model_resolves_the_same_instance(self) -> None:
        container = _container_with_default_builder()

        primary = container.resolve(RepositoryToken(Widget))

        assert container.resolve(Creatable[Widget]) is primary
        assert container.resolve(BulkCreatable[Widget]) is primary

    def test_a_registered_decorator_is_applied_once_per_model(self) -> None:
        container = _container_with_default_builder()
        decorator = _CountingDecorator()
        container.register_instance(RepositoryDecorator, decorator)

        primary = container.resolve(RepositoryToken(Widget))
        container.resolve(Creatable[Widget])
        container.resolve(BulkCreatable[Widget])

        assert isinstance(primary, _Wrapped)
        assert isinstance(primary.repository, _BulkWidgetRepository)
        assert decorator.calls == 1

    def test_the_repository_is_untouched_without_a_decorator(self) -> None:
        container = _container_with_default_builder()

        assert isinstance(container.resolve(RepositoryToken(Widget)), _BulkWidgetRepository)
