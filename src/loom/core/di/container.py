"""Minimal dependency injection container.

Provides key-based registration and resolution with application and request
scopes. The resolution graph is validated at startup
(via :meth:`LoomContainer.validate`) so missing bindings are caught before the
first request.

No reflection occurs after :meth:`LoomContainer.validate` - all providers are
resolved eagerly for ``APPLICATION`` scope during validation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from loom.core.di.scope import Scope

T = TypeVar("T")
BindingKey = object


class ResolutionError(Exception):
    """Raised when a dependency cannot be resolved from the container.

    Args:
        message: Human-readable description of the resolution failure.

    Example::

        raise ResolutionError("No binding registered for: UserRepository")
    """


@dataclass
class _Binding:
    """Internal binding record."""

    provider: Callable[[], Any]
    scope: Scope
    _instance: Any | None = field(default=None, repr=False)


def _describe_key(interface: BindingKey) -> str:
    """Return a stable human-readable name for a DI binding key."""
    if isinstance(interface, type):
        return interface.__qualname__
    return repr(interface)


class LoomContainer:
    """Lightweight key-based dependency injection container.

    Dependencies are registered at startup and resolved at runtime without
    dynamic reflection per request. Application-scope instances are
    singletons; request-scope instances are created fresh per
    :meth:`resolve` call.

    Example::

        container = LoomContainer()
        container.register(UserRepository, SQLAlchemyUserRepository, scope=Scope.REQUEST)
        container.register(AppConfig, lambda: cfg, scope=Scope.APPLICATION)
        container.validate()

        repo = container.resolve(UserRepository)
    """

    def __init__(self) -> None:
        self._bindings: dict[BindingKey, _Binding] = {}
        self._repo_bindings: dict[type[Any], BindingKey] = {}

    def register(
        self,
        interface: BindingKey,
        provider: Callable[[], Any] | type[Any],
        scope: Scope = Scope.REQUEST,
    ) -> None:
        """Register a provider for ``interface``.

        Args:
            interface: A hashable DI key (typically an abstract type, protocol,
                or concrete class) used as the resolution key.
            provider: A zero-argument callable that returns an instance of
                ``interface``, or a concrete class to instantiate directly.
            scope: Lifetime scope. Defaults to :attr:`~Scope.REQUEST`.

        Example::

            container.register(UserRepository, SQLAlchemyUserRepository)
            container.register(AppConfig, lambda: load_config(MyCfg), scope=Scope.APPLICATION)
        """
        if isinstance(provider, type):

            def _factory() -> Any:
                return provider()

            self._bindings[interface] = _Binding(provider=_factory, scope=scope)
            return

        factory = provider
        self._bindings[interface] = _Binding(provider=factory, scope=scope)

    def register_default(
        self,
        interface: BindingKey,
        provider: Callable[[], Any] | type[Any],
        scope: Scope = Scope.REQUEST,
    ) -> None:
        """Register a provider for ``interface`` unless one is already bound.

        :meth:`register` replaces a binding silently. A framework default
        registered after the caller's modules uses this instead, so the
        caller's own binding — through :meth:`register` or
        :meth:`register_instance` — is kept and *provider* is never called.

        Args:
            interface: A hashable DI key used as the resolution key.
            provider: As in :meth:`register`.
            scope: Lifetime scope of the default. Defaults to
                :attr:`~Scope.REQUEST`; ignored when a binding is kept.

        Example::

            container.register(JobService, MyJobService, scope=Scope.APPLICATION)
            container.register_default(JobService, InlineJobService)
            assert isinstance(container.resolve(JobService), MyJobService)
        """
        if interface in self._bindings:
            return
        self.register(interface, provider, scope=scope)

    def register_instance(self, interface: BindingKey, instance: Any) -> None:
        """Register an already-constructed singleton instance.

        Equivalent to ``register(interface, lambda: instance,
        scope=Scope.APPLICATION)`` but skips the factory indirection — the
        instance is stored directly and returned on every :meth:`resolve` call.

        Use this when the caller owns the lifecycle of the object (e.g. a
        ``SessionManager`` created by the bootstrap) and the container should
        treat it as an application-scope singleton without constructing it.

        Args:
            interface: A hashable DI key used as the resolution key.
            instance: The singleton instance to bind.

        Example::

            container.register_instance(SessionManager, session_manager)
            assert container.resolve(SessionManager) is session_manager
        """
        self._bindings[interface] = _Binding(
            provider=lambda: instance,
            scope=Scope.APPLICATION,
            _instance=instance,
        )

    def resolve(self, interface: BindingKey) -> Any:
        """Return an instance bound to ``interface``.

        For ``APPLICATION`` scope, the same instance is returned on every call.
        For ``REQUEST`` (and ``TRANSACTION``) scope, a new instance is created
        each time.

        Args:
            interface: The DI key to resolve.

        Returns:
            An instance satisfying ``interface``.

        Raises:
            ResolutionError: If no binding is registered for ``interface``.
        """
        binding = self._bindings.get(interface)
        if binding is None:
            raise ResolutionError(f"No binding registered for: {_describe_key(interface)}")
        if binding.scope is Scope.APPLICATION:
            if binding._instance is None:
                binding._instance = binding.provider()
            return binding._instance
        return binding.provider()

    def validate(self) -> None:
        """Eagerly validate and instantiate all ``APPLICATION`` scope bindings.

        Ensures that application-scope singletons can be created without error
        at startup (fail-fast). Request-scope bindings are verified to have a
        registered provider but are not instantiated.

        Raises:
            ResolutionError: If an ``APPLICATION`` scope binding fails to
                instantiate.
        """
        for interface, binding in self._bindings.items():
            if binding.scope is Scope.APPLICATION:
                try:
                    self.resolve(interface)
                except ResolutionError:
                    raise
                except Exception as exc:
                    raise ResolutionError(
                        f"Failed to instantiate APPLICATION-scope binding "
                        f"for {_describe_key(interface)}: {exc}"
                    ) from exc

    def is_registered(self, interface: BindingKey) -> bool:
        """Return ``True`` if ``interface`` has a registered binding.

        Args:
            interface: The DI key to check.
        """
        return interface in self._bindings

    def register_repo(self, model: type[Any], interface: BindingKey) -> None:
        """Register a model-to-repository interface mapping.

        Args:
            model: Domain/struct model type.
            interface: DI key previously bound via :meth:`register`.
        """
        self._repo_bindings[model] = interface

    def resolve_repo(self, model: type[Any]) -> Any:
        """Resolve a repository instance by model type.

        Args:
            model: Domain/struct model type used as lookup key.

        Returns:
            Repository instance registered for ``model``.

        Raises:
            ResolutionError: If no repository mapping exists for ``model``.
        """
        interface = self._repo_bindings.get(model)
        if interface is None:
            raise ResolutionError(
                f"No repository mapping registered for model: {model.__qualname__}"
            )
        return self.resolve(interface)

    def has_repo_mapping(self, model: type[Any]) -> bool:
        """Return ``True`` when a model-to-repository mapping exists."""
        return model in self._repo_bindings
