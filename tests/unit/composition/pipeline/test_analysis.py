"""The analysis that ends registration: its roots, its lenient rules and its warnings."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from loom.composition import Recipe, compose_application, pipeline
from loom.composition.source import CompositionSource
from loom.core.bootstrap import BootstrapError
from loom.core.di.container import LoomContainer
from loom.core.di.errors import DependencyUnresolvable
from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode
from loom.core.logger import get_logger
from loom.core.repository.abc import Listable, RepoFor
from loom.core.use_case import UnservedModelError, UseCase
from tests.helpers.composition import Serving
from tests.unit.composition.pipeline.components import (
    AcmeJob,
    AcmeNeedsCycle,
    AcmeNeedsService,
    AcmePing,
    AcmeRecord,
    AcmeUnregistered,
    AcmeUnserved,
    binds_cycle,
)


class AcmeAuditTrail:
    pass


class AcmeAuditCallback:
    """A job callback injecting :class:`AcmeAuditTrail`."""

    def __init__(self, trail: AcmeAuditTrail) -> None:
        self.trail = trail

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context


class AcmeNamedTrail:
    """Bound, with a required parameter no binding serves."""

    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names


class AcmeAuditNames(AcmeAuditCallback):
    def __init__(self, trail: AcmeNamedTrail) -> None:
        self.named = trail


class AcmeUnservedCallback(AcmeAuditCallback):
    """Injects the repository of a model no store serves."""

    def __init__(self, unserved: RepoFor[AcmeUnserved]) -> None:
        self.unserved = unserved


class AcmeInjectsUnserved(UseCase[AcmeRecord, None]):
    def __init__(self, unserved: RepoFor[AcmeUnserved]) -> None:
        self.unserved = unserved

    async def execute(self) -> None:
        return None


class _WithCallbacks(Serving):
    """:class:`Serving`, declaring *callbacks* in its recipe."""

    def __init__(self, *callbacks: type, **options: Any) -> None:
        super().__init__(AcmeRecord, use_cases=(AcmePing,), jobs=(AcmeJob,), **options)
        self.callbacks = callbacks

    def recipe(self, source: CompositionSource) -> Recipe:
        return replace(super().recipe(source), callbacks=self.callbacks)


def _binds(key: type, provider: type) -> Any:
    def module(container: LoomContainer) -> None:
        container.register(key, provider)

    return module


def test_the_roots_are_the_use_cases_jobs_and_callbacks(source: CompositionSource) -> None:
    planned = pipeline.plan(source, _WithCallbacks(AcmeAuditCallback))
    registered = pipeline.register(
        pipeline.persist(planned),
        modules=(_binds(AcmeAuditTrail, AcmeAuditTrail),),
        logger=get_logger("tests.acme"),
    )

    analysis = registered.analysis
    assert analysis.roots == tuple(map(ClassNode, (AcmePing, AcmeJob, AcmeAuditCallback)))
    assert KeyNode(AcmeAuditTrail) in analysis.reachable
    assert registered.kernel.compiler.get_plan(cast(Any, AcmeAuditCallback)) is None


def test_a_callback_is_refused_like_any_root(source: CompositionSource) -> None:
    with pytest.raises(BootstrapError, match="AcmeAuditCallback -> trail: AcmeAuditTrail"):
        compose_application(source, _WithCallbacks(AcmeAuditCallback))


def test_a_callback_injecting_an_unmapped_repository_is_refused_at_startup(
    source: CompositionSource,
) -> None:
    with pytest.raises(BootstrapError) as caught:
        compose_application(source, _WithCallbacks(AcmeUnservedCallback))

    assert str(caught.value.__cause__) == (
        "AcmeUnservedCallback: unserved: no repository mapping is registered for AcmeUnserved"
    )


def test_a_lenient_recipe_leaves_an_unmapped_callback_repository_to_the_first_build(
    source: CompositionSource,
) -> None:
    application = compose_application(source, _WithCallbacks(AcmeUnservedCallback, lenient=True))

    assert not application.kernel.container.has_repo_mapping(AcmeUnserved)


def test_a_use_case_injecting_an_unmapped_repository_keeps_the_unserved_model_error(
    source: CompositionSource,
) -> None:
    adapter = Serving(AcmeRecord, use_cases=(AcmeInjectsUnserved,))

    with pytest.raises(UnservedModelError, match="AcmeInjectsUnserved injects unserved"):
        compose_application(source, adapter)


def test_a_lenient_recipe_leaves_a_missing_binding_to_the_first_build(
    source: CompositionSource,
) -> None:
    adapter = Serving(AcmeRecord, use_cases=(AcmeNeedsService,), lenient=True)

    application = compose_application(source, adapter)

    assert not application.kernel.container.is_registered(AcmeUnregistered)


def test_a_lenient_recipe_still_refuses_the_other_structural_refusals(
    source: CompositionSource,
) -> None:
    adapter = Serving(AcmeRecord, use_cases=(AcmeNeedsCycle,), lenient=True)

    with pytest.raises(BootstrapError, match="dependency cycle") as caught:
        compose_application(source, adapter, modules=(binds_cycle,))

    assert isinstance(caught.value.__cause__, DependencyGraphError)


def test_an_unresolvable_parameter_of_a_binding_warns(source: CompositionSource) -> None:
    adapter = _WithCallbacks(AcmeAuditNames)

    with pytest.warns(DependencyUnresolvable, match="AcmeNamedTrail -> names"):
        compose_application(source, adapter, modules=(_binds(AcmeNamedTrail, AcmeNamedTrail),))


class AcmeListsRecords(UseCase[AcmeRecord, int, Listable[AcmeRecord]]):
    """Declares its main repository as listable."""

    async def execute(self) -> int:
        return 0


def test_a_use_case_reaches_its_main_repository_and_capability(source: CompositionSource) -> None:
    planned = pipeline.plan(source, Serving(AcmeRecord, use_cases=(AcmeListsRecords,)))
    registered = pipeline.register(
        pipeline.persist(planned), modules=(), logger=get_logger("tests.acme")
    )

    container, analysis = registered.kernel.container, registered.analysis
    repository = KeyNode(container._repo_bindings[AcmeRecord])  # noqa: SLF001 - the mapped key
    assert set(analysis.graph.edges(ClassNode(AcmeListsRecords))) == {
        Edge(repository, "main_repo"),
        Edge(KeyNode(Listable[AcmeRecord]), "main_repo"),
    }
    assert {repository, KeyNode(Listable[AcmeRecord])} <= analysis.reachable
    assert analysis.graph is registered.kernel.factory.graph
