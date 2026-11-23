"""Minimal dependency injection container.

Provides key-based registration and resolution with application and
per-execution scopes. A class binding's required constructor parameters are
resolved from the container (autowiring), through a supplier compiled once
per binding (:mod:`loom.core.di.compiler`), so serving a binding reflects on
nothing.

An application seals its container once it has started
(:meth:`LoomContainer.seal`): its singletons are built then, and registering
into it, or declaring an execution kind or binding its resolver, afterwards
raises :class:`~loom.core.di.errors.ContainerSealed`.
"""

from __future__ import annotations

import threading
import warnings
from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeVar

from loom.core.di.compiler import STRUCTURE_LOCK, UNBUILT, Supplier, SupplierCompiler
from loom.core.di.errors import (
    ContainerSealed,
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
    Refusal,
    refuse_unbound,
)
from loom.core.di.introspection import (
    Constructor,
    KeyClassifier,
    SignatureReader,
    no_implicit_dependencies,
    plain_key,
)
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionKinds, Kind, NoExecution
from loom.core.logger.field import ErrorSummary

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

    provider: Callable[..., Any]
    scope: Scope
    instance: object = field(default=UNBUILT, repr=False)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    refused: str | None = field(default=None, repr=False)
    target: object = field(default=None, repr=False)
    overridden: list[object] = field(default_factory=list, repr=False)

    def build(self, make: Supplier) -> object:
        """Return the instance, built once by *make* under :attr:`lock`, outside any execution."""
        with self.lock:
            instance = self.instance
            if instance is UNBUILT:
                with NoExecution():
                    instance = make()
                self.instance = instance
        return instance


class LoomContainer:
    """Lightweight key-based dependency injection container.

    Dependencies are registered at startup and resolved at runtime without
    dynamic reflection per request. Application-scope instances are
    singletons, built outside any execution; request-scope instances are
    built once per execution, and fresh on each :meth:`resolve` outside one.
    A class bound as a provider gets its required constructor parameters
    from the container, and so does a factory function its own parameters.

    Args:
        classify_key: The :data:`~loom.core.di.introspection.KeyClassifier`
            the dependency graph reads constructors with; defaults to
            :func:`~loom.core.di.introspection.plain_key`.
        implicit: The dependencies loom injects into a class it builds
            beyond its signature, such as a use case's main repository; none
            by default.

    Attributes:
        kinds: The execution kinds of this application, shared with its
            executor; empty until the kernel declares them, frozen by :meth:`seal`.

    Example::

        container = LoomContainer()
        container.register(UserRepository, SQLAlchemyUserRepository, scope=Scope.REQUEST)
        container.register(AppConfig, lambda: cfg, scope=Scope.APPLICATION)
        container.validate()

        repo = container.resolve(UserRepository)
    """

    def __init__(
        self,
        *,
        classify_key: KeyClassifier = plain_key,
        implicit: Callable[[type[Any]], Constructor] = no_implicit_dependencies,
    ) -> None:
        self._bindings: dict[BindingKey, _Binding] = {}
        self._repo_bindings: dict[type[Any], BindingKey] = {}
        self._served: dict[BindingKey, Supplier] = {}
        self._plans: dict[type[Any], Callable[[], Any]] = {}
        self._sealed = False
        self.kinds = ExecutionKinds()
        self._install_reader(SignatureReader(self.kinds, classify_key, implicit))

    @property
    def graph(self) -> DependencyGraph:
        """The dependency graph the bindings form, the one :meth:`seal` takes the analysis of."""
        return self._graph

    def _install_reader(self, reader: SignatureReader) -> None:
        """Read constructors with *reader*: a new graph, and a compiler over it."""
        self._graph = DependencyGraph(self._bindings, self._repo_bindings, reader)
        self._compiler = SupplierCompiler(self._graph, self.kinds, self._served, self._plans)

    def _adopt_reading(
        self, classify_key: KeyClassifier, implicit: Callable[[type[Any]], Constructor]
    ) -> bool:
        """Read constructors with *classify_key* and *implicit*; ``True`` when adopted now.

        Only a container still on the default reading adopts another one, and
        only while unsealed; anything compiled before compiles again, and a
        built singleton is kept.

        Raises:
            ValueError: When constructors are already read another way.
            ContainerSealed: When the container is sealed on the default reading.
        """
        with STRUCTURE_LOCK:
            reader = self._graph.reader
            if reader.classify_key is classify_key and reader.implicit is implicit:
                return False
            if reader != SignatureReader(self.kinds):
                raise ValueError("the container already reads constructors another way")
            self._refuse_if_sealed(DependencyGraph)
            self._install_reader(SignatureReader(self.kinds, classify_key, implicit))
            self._drop_compiled()
            return True

    def register(
        self,
        interface: BindingKey,
        provider: Callable[..., Any],
        scope: Scope = Scope.REQUEST,
    ) -> None:
        """Register a provider for ``interface``.

        Args:
            interface: A hashable DI key (typically an abstract type, protocol,
                or concrete class) used as the resolution key.
            provider: A concrete class to instantiate, or a function
                returning an instance of ``interface``. The required
                parameters of either are resolved from the container, so a
                factory's parameters are dependency-graph edges like a
                constructor's, refused at start-up with their path. A
                zero-argument function (``lambda: ...``) and any other
                callable are called with no arguments and are opaque to the
                graph.
            scope: Lifetime scope. Defaults to :attr:`~Scope.REQUEST`.

        Raises:
            ContainerSealed: When the container is sealed.

        Example::

            def gateway(client: HttpClient) -> Gateway:
                return Gateway(client, retries=3)

            container.register(UserRepository, SQLAlchemyUserRepository)
            container.register(Gateway, gateway)
            container.register(AppConfig, lambda: load_config(MyCfg), scope=Scope.APPLICATION)
        """
        with STRUCTURE_LOCK:
            self._bind(interface, provider, scope)

    def register_default(
        self,
        interface: BindingKey,
        provider: Callable[..., Any],
        scope: Scope = Scope.REQUEST,
    ) -> None:
        """Register a provider for ``interface`` unless one is already bound.

        :meth:`register` replaces a binding silently. A framework default
        registered after the caller's modules uses this instead, so the
        caller's own binding — through :meth:`register` or
        :meth:`register_instance` — is kept and *provider* is never called.
        The check and the binding are one operation under the container's
        lock, so two threads never both bind a default.

        Args:
            interface: A hashable DI key used as the resolution key.
            provider: As in :meth:`register`.
            scope: Lifetime scope of the default. Defaults to
                :attr:`~Scope.REQUEST`; ignored when a binding is kept.

        Raises:
            ContainerSealed: When the container is sealed, even if a
                binding is already there.

        Example::

            container.register(JobService, MyJobService, scope=Scope.APPLICATION)
            container.register_default(JobService, InlineJobTransport)
            assert isinstance(container.resolve(JobService), MyJobService)
        """
        with STRUCTURE_LOCK:
            self._refuse_if_sealed(interface)
            if interface not in self._bindings:
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

        Raises:
            ContainerSealed: When the container is sealed.

        Example::

            container.register_instance(SessionManager, session_manager)
            assert container.resolve(SessionManager) is session_manager
        """
        with STRUCTURE_LOCK:
            self._bind(interface, lambda: instance, Scope.APPLICATION)

    def register_alias(self, interface: BindingKey, target: BindingKey) -> None:
        """Serve ``interface`` from whatever is bound under *target*.

        The alias is served by *target*'s own supplier, so both keys return the
        same instance, and a change of *target* reaches ``interface`` with it.
        The alias has no scope of its own: it follows *target*'s.

        Args:
            interface: The key that resolves through *target*.
            target: The key whose binding serves it.

        Raises:
            ContainerSealed: When the container is sealed.
            ResolutionError: When *target* is unbound.
        """
        with STRUCTURE_LOCK:
            served = self._bindings.get(target)
            if served is None:
                raise ResolutionError(
                    f"{describe_key(interface)} aliases {describe_key(target)}, which is unbound"
                )
            self._bind(interface, served.provider, served.scope)
            self._bindings[interface].target = target

    def is_alias_of(self, interface: BindingKey, target: BindingKey) -> bool:
        """Return ``True`` when ``interface`` is an alias of *target*."""
        binding = self._bindings.get(interface)
        return binding is not None and binding.target == target

    @contextmanager
    def overriding(self, interface: BindingKey, instance: Any) -> Iterator[None]:
        """Serve *instance* for an ``APPLICATION`` binding until the block exits.

        The bound singleton is swapped for *instance* and put back on exit,
        also when the block raises. Nothing is registered again: every key
        that resolves through ``interface``, and every class plan, is
        compiled again at its next use. A singleton already built from the
        old value keeps it.

        Args:
            interface: The key of an ``APPLICATION`` binding, not an alias.
            instance: What it serves meanwhile.

        Raises:
            ResolutionError: When ``interface`` is unbound, is an alias of
                another key, or is not ``APPLICATION`` scoped; or, on exit,
                when another override of the same key was entered after this
                one and has not exited. This one is removed all the same and
                the original is back once every override has exited.
        """
        with STRUCTURE_LOCK:
            binding = self._replaceable(interface)
            if not binding.overridden:
                binding.overridden.append(binding.instance)
            binding.overridden.append(instance)
            binding.instance = instance
            self._drop_compiled()
        try:
            yield
        finally:
            with STRUCTURE_LOCK:
                exited_in_order = binding.overridden[-1] is instance
                last = max(i for i, held in enumerate(binding.overridden) if held is instance)
                del binding.overridden[last]
                binding.instance = binding.overridden[-1]
                if len(binding.overridden) == 1:
                    binding.overridden.clear()
                self._drop_compiled()
            if not exited_in_order:
                raise ResolutionError(
                    f"Overrides of {describe_key(interface)} exited out of order: exit the "
                    "last one entered first. This one is removed; the others stay until they exit"
                )

    def _replaceable(self, interface: BindingKey) -> _Binding:
        """The binding of *interface*, which :meth:`overriding` may swap."""
        binding = self._bindings.get(interface)
        if binding is None:
            raise ResolutionError(
                f"{describe_key(interface)} is unbound: there is nothing to override"
            )
        if binding.target is not None:
            raise ResolutionError(
                f"{describe_key(interface)} is an alias of {describe_key(binding.target)}: "
                "override that key instead; for a repository, use Override.repository"
            )
        if binding.scope is not Scope.APPLICATION:
            raise ResolutionError(
                f"{describe_key(interface)} is bound per execution: only an APPLICATION "
                "binding can be overridden"
            )
        return binding

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
            supplier = self._served[interface]
        except KeyError:
            supplier = self._compiler.serving(interface)
        return supplier()

    def compiled(self, cls: type[T]) -> Callable[[], T]:
        """Return the plan of *cls*, a class loom builds itself such as a use case.

        Each call builds a new *cls*, its constructor resolved as a class
        binding's is. Compiled on first use and again after any ``register*``.

        Raises:
            DependencyGraphError: When a dependency is missing, unmapped,
                refused or part of a construction cycle, with the path.
        """
        try:
            plan = self._plans[cls]
        except KeyError:
            plan = self._compiler.plan(cls)
        return plan

    def validate(self) -> None:
        """Eagerly validate and instantiate all ``APPLICATION`` scope bindings.

        Ensures that application-scope singletons can be created without error
        at startup (fail-fast). Request-scope bindings are verified to have a
        registered provider but are not instantiated.

        Raises:
            ResolutionError: If an ``APPLICATION`` scope binding fails to
                instantiate.
        """
        for interface in self._bindings:
            if self._graph.scope(KeyNode(interface)) is Scope.APPLICATION:
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
        the analysis); otherwise it fails at its first resolve. A singleton
        *analysis* reaches that declares a kind with no resolver bound is
        refused, unless the kind is in *deferred*; then it warns with
        :class:`~loom.core.di.errors.DependencyWarning`, is not built and
        fails at its first resolve. A singleton
        no root reaches whose construction a construction refusal refuses,
        or that declares an execution kind, warns the same way and fails with
        the reason whenever resolved. Without *analysis*, no binding is
        reached.

        Every ``register*`` afterwards raises
        :class:`~loom.core.di.errors.ContainerSealed` naming the key and
        changes nothing: to reconfigure, build a new container. So does
        declaring a kind on :attr:`kinds` or binding its resolver.

        Raises:
            ValueError: When *analysis* analysed another dependency graph
                than :attr:`graph`.
            DependencyGraphError: A reached singleton declaring an unbound
                kind, with the path from its root.
            ResolutionError: If an ``APPLICATION`` binding fails to
                instantiate, worded as :meth:`validate` words it.
        """
        with STRUCTURE_LOCK:
            reached = frozenset[Node]()
            if analysis is not None:
                reached = self._reached(analysis, deferred)
            self._sealed = True
            self.kinds.freeze(
                "the container is sealed. Declare execution kinds and bind their resolvers "
                "before the container is sealed: pass agent_resolver= and mcp_resolver= to "
                "create_kernel, or bind them before the application seals it"
            )
            self._compile_reached(reached, strict=strict)
            buildable, unbuilt = self._buildable(reached)
        for message in unbuilt:
            warnings.warn(DependencyWarning(message), stacklevel=2)
        for key in buildable:
            self._instantiate(key)

    def _reached(self, analysis: GraphAnalysis, deferred: Collection[Kind]) -> frozenset[Node]:
        """What *analysis* reaches from its roots once every kind is bound, *deferred* skipped."""
        if analysis.graph is not self._graph:
            raise ValueError("seal() was given the analysis of another dependency graph")
        self._graph.analyse(analysis.roots, (refuse_unbound(self.kinds, skip=deferred),))
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
                    self._compiler.serving(node.key)
                except ResolutionError:
                    if strict:
                        raise

    def _buildable(self, reached: frozenset[Node]) -> tuple[list[BindingKey], list[str]]:
        """The singletons seal builds, the reached ones first, and why each other one is not.

        A reached one is left unbuilt only for a deferred kind and keeps its
        binding; one no root reaches is refused for good (:attr:`_Binding.refused`).
        """
        singletons = [
            k for k in self._bindings if self._graph.scope(KeyNode(k)) is Scope.APPLICATION
        ]
        reached_refusals = (refuse_unbound(self.kinds),)
        buildable: list[BindingKey] = []
        unbuilt: list[str] = []
        for key in sorted(singletons, key=lambda k: KeyNode(k) not in reached):
            is_reached = KeyNode(key) in reached
            refused = self._refusal(key, reached_refusals if is_reached else UNREACHED_REFUSALS)
            if refused is None:
                buildable.append(key)
                continue
            unbuilt.append(
                f"{describe_key(key)} is not built at start-up: {refused}; "
                "it fails when first resolved"
            )
            if not is_reached:
                self._bindings[key].refused = refused
                self._drop_compiled()
        return buildable, unbuilt

    def _refusal(self, key: BindingKey, refusals: tuple[Refusal, ...]) -> str | None:
        """Why one of *refusals* refuses building the singleton under *key*; else ``None``.

        An opaque provider declares nothing, so nothing can refuse it: it is not analysed.
        """
        node = KeyNode(key)
        if not self._graph.constructor(node):
            return None
        try:
            self._graph.analyse((node,), refusals)
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
                f"for {describe_key(interface)}: {ErrorSummary.cite(exc)}"
            ) from exc

    def _bind(self, interface: BindingKey, provider: Callable[..., Any], scope: Scope) -> None:
        """Bind *provider* under *interface*, replacing its binding and its built instance.

        Called under :data:`~loom.core.di.compiler.STRUCTURE_LOCK`. Every
        compiled supplier is stale then, so all are dropped and each is
        compiled again at its next resolve. Built instances live on their
        bindings and per-execution ones are keyed by binding, so they stay.
        """
        self._refuse_if_sealed(interface)
        self._bindings[interface] = _Binding(provider, scope)
        self._graph.invalidate(interface)
        self._drop_compiled()

    def _drop_compiled(self) -> None:
        """Every compiled supplier and plan is stale: each compiles again on its next use."""
        self._served.clear()
        self._plans.clear()

    def _refuse_if_sealed(self, key: object) -> None:
        """Refuse changing *key* once the container is sealed; it is described only then."""
        if self._sealed:
            raise ContainerSealed(
                f"{describe_key(key)} cannot change: the container is sealed. Bind it in a module "
                "passed to the application, or build a new container to reconfigure"
            )

    def bindings(self) -> Mapping[BindingKey, object]:
        """Every bound key, as a read-only view that follows later registrations.

        Each value is an opaque token standing for the key's binding, only
        meant to be compared by identity: :meth:`register` and
        :meth:`register_instance` give a key a new one, so a key whose token
        changed between two reads was bound or replaced in between.
        """
        return MappingProxyType(self._bindings)

    def resolve_or_none(self, interface: BindingKey) -> Any:
        """Return the instance bound to ``interface``, or ``None`` when nothing is bound.

        Args:
            interface: The DI key to resolve.

        Returns:
            What :meth:`resolve` returns, or ``None``.

        Raises:
            DependencyGraphError: As :meth:`resolve`, for a bound key.
        """
        return self.resolve(interface) if interface in self._bindings else None

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

        Raises:
            ContainerSealed: When the container is sealed.
        """
        with STRUCTURE_LOCK:
            self._refuse_if_sealed(model)
            self._repo_bindings[model] = interface
            self._graph.invalidate_edges()
            self._drop_compiled()

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
