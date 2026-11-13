"""Verify store reachability and the compiled marker reads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from loom.composition.pipeline.persistence import Persisted
from loom.composition.pipeline.registration import Registered
from loom.composition.recipe import Recipe
from loom.composition.source import CompositionSource
from loom.core.bootstrap import KernelRuntime
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import ExistsStep, LoadStep
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, StoreKey
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.use_case.errors import JoinedAutocommitStore, UncomposedWriteStore, UseCaseErrorCode
from loom.core.use_case.factory import ModelRead


def seal(
    source: CompositionSource, recipe: Recipe, persisted: Persisted, registered: Registered
) -> None:
    kernel, discovered = registered.kernel, recipe.discovered
    _refuse_unservable_stores(
        kernel, (*discovered.use_cases, *discovered.jobs), source.catalog, persisted.factories
    )
    kernel.factory.verify(reads=_marker_reads(kernel, discovered.use_cases))


def _marker_reads(
    kernel: KernelRuntime, use_cases: Sequence[type[Compilable]]
) -> tuple[ModelRead, ...]:
    """The models the compiled plans read through ``LoadById``, ``Load`` or ``Exists``."""
    reads: list[ModelRead] = []
    for use_case in use_cases:
        plan = kernel.compiler.get_plan(use_case)
        if plan is None:
            continue
        steps: tuple[LoadStep | ExistsStep, ...] = (*plan.load_steps, *plan.exists_steps)
        reads.extend(ModelRead(use_case, step.name, step.entity_type) for step in steps)
    return tuple(reads)


def _refuse_unservable_stores(
    kernel: KernelRuntime,
    components: Sequence[type[Compilable]],
    catalog: StoreCatalog,
    factories: Mapping[StoreKey, UnitOfWorkFactory],
) -> None:
    """Refuse a store a component writes to that its invocation could not commit.

    Compiles each use case or job, as its first execution would. A read-only
    one writes nowhere; without any unit of work every execution runs bare.

    Raises:
        CompilationError: When a component does not compile.
        ConfigError: When a primary or joined store is not declared.
        JoinedAutocommitStore: When a joined store is not transactional.
        UncomposedWriteStore: When a store written to has no unit of work.
    """
    for component in components:
        plan = kernel.compiler.compile(component)
        if plan.primary != DEFAULT_STORE:
            catalog.require(plan.primary, purpose=f"the __store__ of {component.__qualname__}")
        for key in sorted(plan.joined):
            spec = catalog.require(key, purpose=f"Joined in {component.__qualname__}")
            if not spec.capabilities.transactional:
                issue = UseCaseErrorCode.JOINED_AUTOCOMMIT_STORE.issue(
                    component=component, store=key
                )
                raise JoinedAutocommitStore(issue)
        uncomposed = sorted({plan.primary, *plan.joined} - factories.keys())
        if factories and not plan.read_only and uncomposed:
            issue = UseCaseErrorCode.UNCOMPOSED_WRITE_STORE.issue(
                component=component, store=uncomposed[0]
            )
            raise UncomposedWriteStore(issue)
