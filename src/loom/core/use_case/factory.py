"""UseCase factory for declarative infrastructure injection.

Reads a use case's or a job's constructor **once**, from the container's
dependency graph, and compiles it into a *plan*: a closure that builds the
instance with every dependency resolved from
:class:`~loom.core.di.container.LoomContainer`, with zero per-request
reflection.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable
from typing import Any, NamedTuple, TypeVar

from loom.core.di.container import LoomContainer
from loom.core.di.graph import BINDING_REFUSALS, ClassNode, DependencyGraphError
from loom.core.di.introspection import Constructor, RepoDependency, no_implicit_dependencies
from loom.core.repository.registration import classify_key, is_standard_capability
from loom.core.use_case.errors import UnservedModelError
from loom.core.use_case.kinds import declare_kinds
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


def main_repository(cls: type[Any]) -> Constructor:
    """The main repository a use case built through ``UseCase.__init__`` gets injected.

    Its generics name the model; a standard capability declared for it
    (``Listable[Order]``) is the key the repository must also be bound under.
    A use case with its own ``__init__``, or no main model, gets none.
    """
    model = getattr(cls, "__loom_main_model__", None)
    if model is None or cls.__init__ is not UseCase.__init__:
        return no_implicit_dependencies(cls)
    contract = getattr(cls, "__loom_main_repo_contract__", None)
    capability = contract if is_standard_capability(contract) else None
    return (("main_repo", RepoDependency(model, capability)),)


class UseCaseFactory:
    """Builds UseCase instances by resolving constructor dependencies from the container.

    Each class is compiled once into a plan
    (:meth:`~loom.core.di.container.LoomContainer.compiled`), the supplier
    every :meth:`build` runs, so building reflects on nothing. A plan is
    compiled again after any registration into the container. :meth:`verify`,
    called once the host has finished wiring, compiles every registered class
    and surfaces each dependency the container cannot provide before the
    first request.

    Args:
        container: The :class:`~loom.core.di.container.LoomContainer` to
            resolve dependencies from. One still on the default reading is
            deprecated: it adopts the use-case reading once, with a
            :class:`DeprecationWarning`.

    Raises:
        ValueError: When *container* reads constructors another way.
        ContainerSealed: When *container* is sealed on the default reading.

    Example::

        class CreateOrderUseCase(UseCase[Order, OrderResponse]):
            def __init__(self, order_repo: OrderRepository) -> None:
                self._repo = order_repo
            ...

        factory = UseCaseFactory(container)
        use_case = factory.build(CreateOrderUseCase)

    Attributes:
        graph: The container's dependency graph, read as loom reads a use
            case or a job: constructor annotations classified with
            :func:`~loom.core.repository.registration.classify_key`, a use
            case's main repository included (:func:`main_repository`).
    """

    def __init__(self, container: LoomContainer) -> None:
        if container._adopt_reading(classify_key, main_repository):
            declare_kinds(container.kinds)
            warnings.warn(
                "UseCaseFactory over a container that does not read constructors as loom "
                "builds a use case is deprecated; build it with create_kernel or "
                "loom.composition.create_app",
                DeprecationWarning,
                stacklevel=2,
            )
        self._container = container
        self.graph = container.graph
        self._registered: dict[type[Any], None] = {}

    def build(self, use_case_type: type[_T]) -> _T:
        """Construct an instance with dependencies injected from the container.

        Runs the plan of ``use_case_type``
        (:meth:`~loom.core.di.container.LoomContainer.compiled`); outside an
        execution each per-execution dependency is built fresh. Accepts any
        class whose constructor dependencies are resolvable from the
        container — both :class:`~loom.core.use_case.use_case.UseCase`
        and :class:`~loom.core.job.job.Job` subclasses are valid inputs.

        Args:
            use_case_type: Concrete class to instantiate.

        Returns:
            A fully constructed instance of ``use_case_type``.

        Raises:
            ~loom.core.di.graph.DependencyGraphError: Naming the path to a
                dependency the container cannot provide.
        """
        return self._container.compiled(use_case_type)()

    def register(self, use_case_type: type[Any]) -> None:
        """Record ``use_case_type`` for :meth:`verify`, refusing a parameter loom cannot provide.

        A dependency the host registers later (a job service, the application
        invoker) is legitimate here; :meth:`verify` checks them once wiring
        is complete.

        Args:
            use_case_type: Class to record. Supports both ``UseCase`` and
                ``Job`` subclasses.

        Raises:
            ~loom.core.di.graph.DependencyGraphError: When a required
                parameter names nothing the container can serve, naming the
                class and the parameter.
        """
        root = ClassNode(use_case_type)
        reason = next(self.graph.unresolvable(root), None)
        if reason is not None:
            raise DependencyGraphError(f"{root}: {reason}")
        self._registered[use_case_type] = None

    def verify(self, *, reads: Iterable[ModelRead] = ()) -> None:
        """Check every registered class against the container's bindings and compile it.

        Call once the host has finished wiring, so that a missing binding
        fails the bootstrap instead of the first request, and no build
        compiles afterwards. A main repository declared with a standard
        capability contract (``Listable[Order]``) must be mapped for its
        model *and* registered under that capability.

        Args:
            reads: Models the registered classes read besides their main
                repository (the ``LoadById``, ``Load`` and ``Exists`` markers
                of their compiled plans); each must have a repository too.

        Raises:
            UnservedModelError: When a main repository or one of *reads*
                names a model no repository is registered for.
            ~loom.core.di.graph.DependencyGraphError: Naming the path from
                the class to the key the container lacks, or to a cycle.
        """
        for component, parameter, model in (*self._main_repository_reads(), *reads):
            if not self._container.has_repo_mapping(model):
                raise UnservedModelError.for_parameter(component, parameter, model)
        self.graph.analyse(map(ClassNode, self._registered), BINDING_REFUSALS)
        for use_case_type in self._registered:
            self._container.compiled(use_case_type)

    def _main_repository_reads(self) -> list[ModelRead]:
        return [
            ModelRead(use_case_type, name, dependency.model)
            for use_case_type in self._registered
            for name, dependency in self.graph.constructor(ClassNode(use_case_type))
            if isinstance(dependency, RepoDependency)
        ]
