"""UseCase factory for declarative infrastructure injection.

Inspects ``UseCase.__init__`` type hints **once at startup** and caches the
dependency list.  At request time, it resolves each dependency from
:class:`~loom.core.di.container.LoomContainer` and constructs the instance —
zero per-request reflection.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, NamedTuple, TypeVar

from loom.core.di.container import LoomContainer, ResolutionError, _describe_key
from loom.core.di.introspection import KeyDependency, RepoDependency, constructor_dependencies
from loom.core.repository.abc import RepoFor
from loom.core.repository.dependency import classify_key
from loom.core.repository.registration import is_standard_capability
from loom.core.use_case.errors import UnservedModelError
from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")
_T = TypeVar("_T")


class ModelRead(NamedTuple):
    """A model a use case or job reads: through which parameter, and which model.

    Args:
        component: The use case or job.
        parameter: The parameter that reads it.
        model: The model read.
    """

    component: type[object]
    parameter: str
    model: type[object]


@dataclass(frozen=True, slots=True)
class _MainRepoDep:
    """Model-centric repository dependency resolved through the repo mapping.

    ``contract`` is the capability the use case declared for its main
    repository (``RepoFor[Any]`` when it declared none).  A standard
    capability such as ``Listable[Order]`` is verified against the container
    as well as the model mapping.
    """

    model: type[Any]
    contract: object = RepoFor[Any]

    @property
    def capability_key(self) -> object | None:
        """The standard capability DI key the contract requires, if any."""
        return self.contract if is_standard_capability(self.contract) else None


DepToken = object
"""A container key, or a :class:`_MainRepoDep` resolved through the repository mapping."""


class UseCaseFactory:
    """Builds UseCase instances by resolving constructor dependencies from the container.

    The factory inspects ``UseCase.__init__`` type hints once (at startup or
    on first use for each class) and stores the resolved dependency list.
    Every subsequent :meth:`build` call resolves instances from the container
    without further reflection. :meth:`verify`, called once the host has
    finished wiring, surfaces every dependency the container cannot provide
    before the first request.

    Args:
        container: The :class:`~loom.core.di.container.LoomContainer` to
            resolve dependencies from.

    Example::

        class CreateOrderUseCase(UseCase[Order, OrderResponse]):
            def __init__(self, order_repo: OrderRepository) -> None:
                self._repo = order_repo
            ...

        factory = UseCaseFactory(container)
        use_case = factory.build(CreateOrderUseCase)
    """

    def __init__(self, container: LoomContainer) -> None:
        self._container = container
        # Maps use_case_type -> list of (param_name, resolved_token) pairs.
        # The token is either:
        # - a concrete dependency type (resolved via container.resolve)
        # - a _MainRepoDep for model-centric repo resolution
        self._dep_cache: dict[type[Any], list[tuple[str, DepToken]]] = {}

    def build(self, use_case_type: type[_T]) -> _T:
        """Construct an instance with dependencies injected from the container.

        Accepts any class whose constructor dependencies are resolvable from
        the container — both :class:`~loom.core.use_case.use_case.UseCase`
        and :class:`~loom.core.job.job.Job` subclasses are valid inputs.

        Args:
            use_case_type: Concrete class to instantiate.

        Returns:
            A fully constructed instance of ``use_case_type``.

        Raises:
            ~loom.core.di.container.ResolutionError: If a required dependency
                is not registered in the container.
        """
        deps = self._get_deps(use_case_type)
        kwargs: dict[str, Any] = {}
        for name, dep in deps:
            if isinstance(dep, _MainRepoDep):
                kwargs[name] = self._container.resolve_repo(dep.model)
                continue
            kwargs[name] = self._container.resolve(dep)
        return use_case_type(**kwargs)

    def register(self, use_case_type: type[Any]) -> None:
        """Pre-warm the dependency cache for ``use_case_type`` at startup.

        Only inspects and caches: a dependency the host registers later (a job
        service, the application invoker) is legitimate here. :meth:`verify`
        checks the cached dependencies once wiring is complete.

        Args:
            use_case_type: Class to inspect and cache. Supports both
                ``UseCase`` and ``Job`` subclasses.
        """
        self._get_deps(use_case_type)

    def verify(self, *, reads: Iterable[ModelRead] = ()) -> None:
        """Check every registered class against the container's bindings.

        Call once the host has finished wiring, so that a missing binding
        fails the bootstrap instead of the first request.  A main repository
        declared with a standard capability contract (``Listable[Order]``)
        must be mapped for its model *and* registered under that capability.

        Args:
            reads: Models the registered classes read besides their main
                repository (the ``LoadById``, ``Load`` and ``Exists`` markers
                of their compiled plans); each must have a repository too.

        Raises:
            UnservedModelError: When a main repository or one of *reads*
                names a model no repository is registered for.
            ~loom.core.di.container.ResolutionError: Naming the class, the
                constructor parameter and the key the container lacks.
        """
        for component, parameter, model in (*self._main_repository_reads(), *reads):
            if not self._container.has_repo_mapping(model):
                raise UnservedModelError.for_parameter(component, parameter, model)
        for use_case_type, deps in self._dep_cache.items():
            for name, dep in deps:
                missing = self._missing_key(dep)
                if missing is not None:
                    raise ResolutionError(
                        f"{use_case_type.__qualname__} injects {name}: {missing}, "
                        "which is not registered in the container."
                    )

    def _main_repository_reads(self) -> list[ModelRead]:
        return [
            ModelRead(use_case_type, name, dep.model)
            for use_case_type, deps in self._dep_cache.items()
            for name, dep in deps
            if isinstance(dep, _MainRepoDep)
        ]

    def _missing_key(self, dep: DepToken) -> str | None:
        """Describe the key the container lacks for ``dep``; ``None`` when provided."""
        if isinstance(dep, _MainRepoDep):
            return self._missing_main_repo_key(dep)
        return None if self._container.is_registered(dep) else _describe_key(dep)

    def _missing_main_repo_key(self, dep: _MainRepoDep) -> str | None:
        """The capability key a mapped main repository lacks; the mapping is checked first."""
        key = dep.capability_key
        if key is not None and not self._container.is_registered(key):
            return f"{_describe_key(key)} (main repository capability)"
        return None

    def _get_deps(self, use_case_type: type[Any]) -> list[tuple[str, DepToken]]:
        """Return the cached (or freshly computed) dependency list for a UseCase.

        The constructor is read by
        :func:`~loom.core.di.introspection.constructor_dependencies` with
        :func:`~loom.core.repository.dependency.classify_key`; only its keys
        and repositories are injected.

        Args:
            use_case_type: UseCase subclass to inspect.

        Returns:
            List of ``(param_name, token)`` pairs from ``__init__``.
        """
        if use_case_type in self._dep_cache:
            return self._dep_cache[use_case_type]

        init = use_case_type.__init__
        if init is UseCase.__init__:
            deps = _default_init_deps(use_case_type)
        else:
            deps = self._explicit_init_deps(use_case_type)
        self._dep_cache[use_case_type] = deps
        return deps

    def _explicit_init_deps(self, use_case_type: type[Any]) -> list[tuple[str, DepToken]]:
        """Return the keys and repositories a user-defined ``__init__`` injects."""
        deps: list[tuple[str, DepToken]] = []
        kinds = self._container.kinds
        for name, dependency in constructor_dependencies(use_case_type, kinds, classify_key):
            match dependency:
                case RepoDependency(model):
                    deps.append((name, _MainRepoDep(model)))
                case KeyDependency(key):
                    deps.append((name, key))
        return deps


def _default_init_deps(use_case_type: type[Any]) -> list[tuple[str, DepToken]]:
    """Return the main-repository dependency the ``UseCase`` generics declare."""
    main_model = getattr(use_case_type, "__loom_main_model__", None)
    if main_model is None:
        return []
    contract = getattr(use_case_type, "__loom_main_repo_contract__", RepoFor[Any])
    return [("main_repo", _MainRepoDep(main_model, contract))]
