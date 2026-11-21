"""Test substitutions for composed applications."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

from loom.connections.aws import AwsClients
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.model import LoomStruct
from loom.core.stores.repository.registration import (
    capability_keys,
    register_repository_binding,
)
from loom.core.stores.repository.registry import (
    RepositoryToken,
)

if TYPE_CHECKING:
    from loom.composition.application import Application
    from loom.composition.project import Project
    from loom.composition.recipe import Recipe, Runtime


class OverrideKind(StrEnum):
    """The kind of test substitution."""

    REPOSITORY = "repository"
    INSTANCE = "instance"


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
        if self.kind is OverrideKind.INSTANCE:
            container.register_instance(self.key, self.value)
            return
        model = cast(type[LoomStruct], self.key)
        register_repository_binding(
            container=container,
            model=model,
            capability_keys=capability_keys(type(self.value), model),
            binding_key=self.binding_key,
            provider=lambda: self.value,
        )

    @contextmanager
    def apply(self, container: LoomContainer) -> Iterator[None]:
        """Substitute this value in a composed application until the block exits.

        Raises:
            ResolutionError: When nothing is bound to substitute, as
                :meth:`~loom.core.di.container.LoomContainer.overriding` raises it.
        """
        if self.kind is OverrideKind.REPOSITORY and not container.is_registered(self.binding_key):
            model = cast(type[LoomStruct], self.key)
            raise ResolutionError(f"No store serves a repository for {model.__qualname__}")
        with container.overriding(self.binding_key, self.value):
            yield

    @classmethod
    @contextmanager
    def apply_all(cls, container: LoomContainer, overrides: Sequence[Override]) -> Iterator[None]:
        """Substitute every one of *overrides* in a composed application until the block exits."""
        with ExitStack() as stack:
            for override in overrides:
                stack.enter_context(override.apply(container))
            yield

    @property
    def binding_key(self) -> object:
        """The container key whose binding this substitution replaces."""
        if self.kind is OverrideKind.REPOSITORY:
            return RepositoryToken(cast(type[LoomStruct], self.key))
        return self.key


class OverrideRuntime:
    """Apply test overrides, and optionally the application's AWS clients, to a runtime recipe.

    Args:
        runtime: The runtime whose recipe is substituted.
        overrides: The substitutions run right after the discovered installers.
        aws: The recording or fake AWS clients the stores, the job transports and
            the container all use; the recipe's own without it. ``AwsClients``
            is not an :class:`Override` key: composition owns it once.
    """

    def __init__(
        self,
        runtime: Runtime[Any],
        overrides: tuple[Override, ...],
        *,
        aws: AwsClients | None = None,
    ) -> None:
        self._runtime = runtime
        self._overrides = overrides
        self._aws = aws

    def recipe(self, project: Project) -> Recipe:
        """Return the wrapped recipe with the test overrides and AWS clients."""
        recipe = self._runtime.recipe(project)
        return replace(
            recipe,
            aws=recipe.aws if self._aws is None else self._aws,
            overrides=(*recipe.overrides, *(override.install for override in self._overrides)),
        )

    def mount(self, application: Application) -> Any:
        """Bind the wrapped runtime to the composed application."""
        return self._runtime.mount(application)
