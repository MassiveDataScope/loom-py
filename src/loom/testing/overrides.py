"""Test substitutions for composed applications."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import LoomStruct
from loom.core.stores.repository.registration import (
    capability_keys,
    register_repository_binding,
)
from loom.core.stores.repository.registry import (
    ReplaceableSource,
    RepositorySourceToken,
    RepositoryToken,
)

if TYPE_CHECKING:
    from loom.composition.application import Application
    from loom.composition.project import Project
    from loom.composition.recipe import Adapter, Recipe


class OverrideKind(StrEnum):
    """The kind of test substitution."""

    REPOSITORY = "repository"
    INSTANCE = "instance"


@dataclass(frozen=True)
class InstanceSourceToken:
    """DI key for a replaceable test instance."""

    key: object


@dataclass(frozen=True)
class Override:
    """A repository or instance substitution for a test harness."""

    kind: OverrideKind
    key: object
    value: Any

    @classmethod
    def repository(cls, model: type[LoomStruct], double: Any) -> Override:
        """Substitute the repository served for a model."""
        return cls(OverrideKind.REPOSITORY, model, double)

    @classmethod
    def instance(cls, key: object, value: Any) -> Override:
        """Substitute one dependency instance."""
        return cls(OverrideKind.INSTANCE, key, value)

    def install(self, container: LoomContainer) -> None:
        """Register this substitution during composition."""
        source_key = self._source_token
        container.register_instance(source_key, ReplaceableSource(lambda: self.value))
        if self.kind is OverrideKind.INSTANCE:
            container.register(
                self.key,
                lambda: container.resolve(source_key).get(),
                scope=Scope.REQUEST,
            )
            return
        model = cast(type[LoomStruct], self.key)
        register_repository_binding(
            container=container,
            model=model,
            capability_keys=capability_keys(type(self.value), model),
            binding_key=RepositoryToken(model),
            provider=lambda: self.value,
        )

    @contextmanager
    def applied(self, container: LoomContainer) -> Iterator[None]:
        """Substitute this value in a composed application until the block exits."""
        source = container.resolve(self._source_token)
        previous = source.swap(self.value)
        try:
            yield
        finally:
            source.restore(previous)

    @classmethod
    @contextmanager
    def applying(cls, container: LoomContainer, overrides: Sequence[Override]) -> Iterator[None]:
        """Substitute every one of *overrides* in a composed application until the block exits."""
        with ExitStack() as stack:
            for override in overrides:
                stack.enter_context(override.applied(container))
            yield

    @property
    def _source_token(self) -> object:
        if self.kind is OverrideKind.REPOSITORY:
            return RepositorySourceToken(cast(type[LoomStruct], self.key))
        return InstanceSourceToken(self.key)


class OverrideAdapter:
    """Apply test overrides to an adapter recipe during composition."""

    def __init__(self, adapter: Adapter[Any], overrides: tuple[Override, ...]) -> None:
        self._adapter = adapter
        self._overrides = overrides

    def recipe(self, project: Project) -> Recipe:
        """Return the wrapped recipe with the test overrides."""
        recipe = self._adapter.recipe(project)
        return replace(
            recipe,
            overrides=(*recipe.overrides, *(override.install for override in self._overrides)),
        )

    def bind(self, application: Application) -> Any:
        """Bind the wrapped adapter to the composed application."""
        return self._adapter.bind(application)
