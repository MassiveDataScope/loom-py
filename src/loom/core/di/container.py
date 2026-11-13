"""Minimal dependency injection container.

Provides key-based registration and resolution with application and
per-execution scopes. A class binding's required constructor parameters are
resolved from the container (autowiring), through a closure compiled once per
binding (:mod:`loom.core.di.compiler`), so serving a binding reflects on
nothing.

An application seals its container once it has started
(:meth:`LoomContainer.seal`): its singletons are built then, and registering
into it afterwards still works but warns with
:class:`~loom.core.di.errors.ContainerSealedWarning`.
"""

from __future__ import annotations

import threading
import warnings
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeVar

from loom.core.di.compiler import (
    COLD_PATH,
    UNBUILT,
    Closure,
    compile_key,
)
from loom.core.di.errors import (
    ContainerSealedWarning,
    DependencyWarning,
    ResolutionError,
    describe_key,
)
from loom.core.di.graph import (
    UNREACHED_REFUSALS,
    DependencyGraph,
    DependencyGraphError,
    GraphAnalysis,
    KeyNode,
    Node,
    Rule,
    analyse,
    refuse_unbound,
)
from loom.core.di.introspection import Constructor, KeyClassifier, plain_key
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionKinds, Kind

T = TypeVar("T")
BindingKey = object

__all__ = ["BindingKey", "LoomContainer", "ResolutionError"]


@dataclass(eq=False, slots=True)
class _Binding:
    """One registration: compared by identity, so it also keys an execution's cache.

    A singleton is built once under :attr:`lock`, its own: building it waits
    on no other binding, and a provider resolving another on the same thread
    recurses. :attr:`refused` is why seal refused building it, for good: it
    is raised wherever the binding is compiled.
    """

    provider: Callable[[], Any]
    scope: Scope
    instance: object = field(default=UNBUILT, repr=False)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    refused: str | None = field(default=None, repr=False)


class LoomContainer:
    """Lightweight key-based dependency injection container.

    Dependencies are registered at startup and resolved at runtime without
    dynamic reflection per request. Application-scope instances are
    singletons, built outside any execution; request-scope instances are
    built once per execution, and fresh on each :meth:`resolve` outside one.
    A class bound as a provider gets its required constructor parameters
    from the container.

    Args:
        classify_key: The :data:`~loom.core.di.introspection.KeyClassifier`
            the dependency graph reads constructors with; defaults to
            :func:`~loom.core.di.introspection.plain_key`.

    Attributes:
        kinds: The execution kinds of this application, shared with its
            executor; empty until the kernel declares them.

    Example::

        container = LoomContainer()
        container.register(UserRepository, SQLAlchemyUserRepository, scope=Scope.REQUEST)
        container.register(AppConfig, lambda: cfg, scope=Scope.APPLICATION)
        container.validate()

        repo = container.resolve(UserRepository)
    """

    def __init__(self, *, classify_key: KeyClassifier = plain_key) -> None:
        self._bindings: dict[BindingKey, _Binding] = {}
        self._repo_bindings: dict[type[Any], BindingKey] = {}
        self._compiled: dict[BindingKey, Closure] = {}
        self._sealed = False
        self.kinds = ExecutionKinds()
        self._graph = DependencyGraph(self, classify_key=classify_key)
        self._reading: tuple[KeyClassifier, Callable[[type[Any]], Constructor]] | None = None

    @property
    def graph(self) -> DependencyGraph:
        """The dependency graph the bindings form, the one :meth:`seal` takes the analysis of."""
        return self._graph

    def read_with(
        self, *, classify_key: KeyClassifier, implicit: Callable[[type[Any]], Constructor]
    ) -> DependencyGraph:
        """Read constructors with *classify_key* and *implicit* from now on; return :attr:`graph`.

        What loom builds reads as the graph does, so every use-case factory
        on this container calls this, and they all get the one graph: a
        ``register*`` forgets what it read for its key, and :meth:`seal`
        refuses the analysis of any other graph. The first call compiles
        again anything compiled before; a built singleton is kept.

        Args:
            classify_key: How a constructor annotation becomes a key.
            implicit: The dependencies loom injects into a class it builds
                beyond its signature, such as a use case's main repository.

        Raises:
            ValueError: When constructors are already read another way.
        """
        reading = (classify_key, implicit)
        with COLD_PATH:
            if self._reading is None:
                self._reading = reading
                self._graph = DependencyGraph(self, classify_key=classify_key, implicit=implicit)
                self._compiled.clear()
            elif self._reading != reading:
                raise ValueError("the container already reads constructors another way")
        return self._graph

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
        self._bind(interface, provider, scope)

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
        After :meth:`seal` it warns only when it binds.

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
        self._bind(interface, provider, scope)

    def register_instance(self, interface: BindingKey, instance: Any) -> None:
        """Register an already-constructed singleton instance.

        Equivalent to ``register(interface, lambda: instance,
        scope=Scope.APPLICATION)``: the container never constructs it, and
        returns it on every :meth:`resolve` call.

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
        self._bind(interface, lambda: instance, Scope.APPLICATION)

    def resolve(self, interface: BindingKey) -> Any:
        """Return an instance bound to ``interface``.

        For ``APPLICATION`` scope, the same instance is returned on every call.
        For ``REQUEST`` (and ``TRANSACTION``) scope, one instance per
        execution, and a new one on each call outside any execution.

        Args:
            interface: The DI key to resolve.

        Returns:
            An instance satisfying ``interface``.

        Raises:
            ResolutionError: If no binding is registered for ``interface``.
            DependencyGraphError: If a binding it needs is missing or
                refused, or they form a cycle; the message holds the path.
        """
        try:
            closure = self._compiled[interface]
        except KeyError:
            closure = compile_key(self, interface)
        return closure()

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
                self._instantiate(interface)

    def seal(
        self,
        analysis: GraphAnalysis | None = None,
        *,
        deferred: Collection[Kind] = (),
        strict: bool = True,
    ) -> None:
        """Mark the container as complete and build its singletons, outside any execution.

        Compiles what *analysis* reaches, then builds every ``APPLICATION``
        binding as :meth:`validate` does, the reachable ones first, holding no
        container lock while a provider runs. A *strict* seal raises a
        reached binding that does not compile (a key registered again since
        the analysis); otherwise it fails at its first resolve. Refusal 7
        is one policy over the singletons *analysis* reaches: one declaring
        a kind with no resolver bound is refused, unless the kind is in
        *deferred* (a lenient composition defers the agent and MCP kinds);
        then it warns with :class:`~loom.core.di.errors.DependencyWarning`
        and is not built, but keeps its binding, so a resolver bound later
        serves it. A singleton no root reaches whose construction a refusal
        2-5 refuses, or that declares an execution kind, warns the same way
        and fails with the reason whenever resolved. Without *analysis*, no
        binding is reached.

        Every ``register*`` afterwards warns with
        :class:`~loom.core.di.errors.ContainerSealedWarning` naming the key,
        then replaces that key's binding; every binding is compiled again at
        its next resolve. Only that key's built instance is dropped: every
        other built instance, and whatever already holds one, is kept.

        Raises:
            ValueError: When *analysis* analysed another dependency graph
                than :attr:`graph`.
            DependencyGraphError: Refusal 7, with the path from its root.
            ResolutionError: If an ``APPLICATION`` binding fails to
                instantiate, worded as :meth:`validate` words it.
        """
        with COLD_PATH:
            reached = frozenset[Node]()
            if analysis is not None:
                reached = self._reached(analysis, deferred)
            self._sealed = True
            self._compile_reached(reached, strict=strict)
            buildable, unbuilt = self._buildable(reached)
        for message in unbuilt:
            warnings.warn(DependencyWarning(message), stacklevel=2)
        for key in buildable:
            self._instantiate(key)

    def _reached(self, analysis: GraphAnalysis, deferred: Collection[Kind]) -> frozenset[Node]:
        """What *analysis* reaches, once refusal 7 passed from its roots, *deferred* skipped."""
        if analysis.graph is not self._graph:
            raise ValueError("seal() was given the analysis of another dependency graph")
        analyse(self._graph, analysis.roots, (refuse_unbound(self.kinds, skip=deferred),))
        return analysis.reachable

    def _compile_reached(self, reached: frozenset[Node], *, strict: bool) -> None:
        """Compile each reached binding; a failure is raised only when *strict*.

        A strict analysis refused whatever would not compile, so a failure
        here comes from a key registered again since. A lenient one let it
        through: that binding fails at its first resolve instead.
        """
        for node in reached:
            if isinstance(node, KeyNode) and node.key in self._bindings:
                try:
                    compile_key(self, node.key)
                except ResolutionError:
                    if strict:
                        raise

    def _buildable(self, reached: frozenset[Node]) -> tuple[list[BindingKey], list[str]]:
        """The singletons seal builds, the reached ones first, and why each other one is not.

        A reached one is left unbuilt only for a deferred kind and keeps its
        binding; one no root reaches is refused for good (:attr:`_Binding.refused`).
        """
        singletons = [k for k, b in self._bindings.items() if b.scope is Scope.APPLICATION]
        reached_rules = (refuse_unbound(self.kinds),)
        buildable: list[BindingKey] = []
        unbuilt: list[str] = []
        for key in sorted(singletons, key=lambda k: KeyNode(k) not in reached):  # reached first
            is_reached = KeyNode(key) in reached
            refused = self._refusal(key, reached_rules if is_reached else UNREACHED_REFUSALS)
            if refused is None:
                buildable.append(key)
                continue
            unbuilt.append(
                f"{describe_key(key)} is not built at start-up: {refused}; "
                "it fails when first resolved"
            )
            if not is_reached:
                self._bindings[key].refused = refused
                self._compiled.clear()  # a closure compiled before may hold it
        return buildable, unbuilt

    def _refusal(self, key: BindingKey, rules: tuple[Rule, ...]) -> str | None:
        """Why one of *rules* refuses building the singleton under *key*; else ``None``.

        A provider declares nothing, so no rule can refuse it: it is not analysed.
        """
        node = KeyNode(key)
        if not self._graph.constructor(node):
            return None
        try:
            analyse(self._graph, (node,), rules)
        except DependencyGraphError as refused:
            return str(refused)
        return None

    def _instantiate(self, interface: BindingKey) -> None:
        """Resolve *interface*; a failure other than a resolution error is wrapped in one."""
        try:
            self.resolve(interface)
        except ResolutionError:
            raise
        except Exception as exc:
            raise ResolutionError(
                f"Failed to instantiate APPLICATION-scope binding "
                f"for {describe_key(interface)}: {exc}"
            ) from exc

    def _bind(
        self, interface: BindingKey, provider: Callable[[], Any] | type[Any], scope: Scope
    ) -> None:
        """Bind *provider* under *interface*, replacing its binding and its built instance.

        Every compiled closure is stale then, so all are dropped and each is
        compiled again at its next resolve. Built instances live on their
        bindings and per-execution ones are keyed by binding, so they stay.

        Every ``register*`` but :meth:`register_repo` binds through here, so
        the sealed-container warning names the caller of that ``register*``.
        """
        self._warn_if_sealed(interface, stacklevel=4)
        with COLD_PATH:
            self._bindings[interface] = _Binding(provider, scope)
            self._graph.forget(interface)
            self._compiled.clear()

    def _warn_if_sealed(self, interface: BindingKey, *, stacklevel: int) -> None:
        """Warn that *interface* is registered into a sealed container, *stacklevel* frames up."""
        if self._sealed:
            warnings.warn(
                ContainerSealedWarning(
                    f"{describe_key(interface)} registered into a sealed container: only "
                    "that key changes, and this becomes an error in the next major "
                    "version; bind it in a module passed to the application instead"
                ),
                stacklevel=stacklevel,
            )

    def bindings(self) -> Mapping[BindingKey, object]:
        """Every bound key, as a read-only view that follows later registrations.

        Each value is an opaque token standing for the key's binding, only
        meant to be compared by identity: :meth:`register` and
        :meth:`register_instance` give a key a new one, so a key whose token
        changed between two reads was bound or replaced in between.
        """
        return MappingProxyType(self._bindings)

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
        self._warn_if_sealed(model, stacklevel=3)
        with COLD_PATH:
            self._repo_bindings[model] = interface
            self._graph.forget_edges()
            self._compiled.clear()

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
