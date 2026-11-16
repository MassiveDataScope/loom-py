"""The analysis that ends registration: its roots, its lenient rules and its warnings."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from loom.composition import Project, Recipe
from loom.core.bootstrap import BootstrapError
from loom.core.di.container import LoomContainer
from loom.core.di.errors import DependencyUnresolvable
from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, GraphAnalysis, KeyNode
from loom.core.stores.repository import Listable, RepoFor
from loom.core.use_case import UnservedModelError, UseCase
from tests.helpers.composition import Serving
from tests.unit.composition.phases.components import (
    AcmeJob,
    AcmeNeedsCycle,
    AcmeNeedsService,
    AcmePing,
    AcmeRecord,
    AcmeUnregistered,
    AcmeUnserved,
    binds_cycle,
)


class AcmeCallbackTrail:
    pass


class AcmeAuditCallback:
    """A job callback injecting :class:`AcmeCallbackTrail`."""

    def __init__(self, trail: AcmeCallbackTrail) -> None:
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

    def recipe(self, project: Project) -> Recipe:
        return replace(super().recipe(project), callbacks=self.callbacks)


def _binds(key: type, provider: type) -> Any:
    def module(container: LoomContainer) -> None:
        container.register(key, provider)

    return module


class AcmeListsRecords(UseCase[AcmeRecord, int, Listable[AcmeRecord]]):
    """Declares its main repository as listable."""

    async def execute(self) -> int:
        return 0


@pytest.fixture
def analyses(monkeypatch: pytest.MonkeyPatch) -> list[GraphAnalysis]:
    """Every analysis the composition makes, in order."""
    made: list[GraphAnalysis] = []
    analysed = Recipe.analysed

    def recording(recipe: Recipe, graph: Any) -> GraphAnalysis:
        made.append(analysed(recipe, graph))
        return made[-1]

    monkeypatch.setattr(Recipe, "analysed", recording)
    return made


class TestRegistrationAnalysis:
    def test_the_roots_are_the_use_cases_jobs_and_callbacks(
        self, project: Project, analyses: list[GraphAnalysis]
    ) -> None:
        application = project.compose(
            _WithCallbacks(
                AcmeAuditCallback,
                overrides=(_binds(AcmeCallbackTrail, AcmeCallbackTrail),),
            ),
        )

        (analysis,) = analyses
        assert analysis.roots == tuple(map(ClassNode, (AcmePing, AcmeJob, AcmeAuditCallback)))
        assert KeyNode(AcmeCallbackTrail) in analysis.reachable
        assert application.kernel.compiler.get_plan(cast(Any, AcmeAuditCallback)) is None

    def test_a_callback_is_refused_like_any_root(self, project: Project) -> None:
        adapter = _WithCallbacks(AcmeAuditCallback)

        with pytest.raises(BootstrapError, match="AcmeAuditCallback -> trail: AcmeCallbackTrail"):
            project.compose(adapter)

    def test_a_callback_injecting_an_unmapped_repository_is_refused_at_startup(
        self,
        project: Project,
    ) -> None:
        adapter = _WithCallbacks(AcmeUnservedCallback)

        with pytest.raises(BootstrapError) as caught:
            project.compose(adapter)

        assert str(caught.value.__cause__) == (
            "AcmeUnservedCallback: unserved: no repository mapping is registered for AcmeUnserved"
        )

    def test_a_lenient_recipe_leaves_an_unmapped_callback_repository_to_the_first_build(
        self,
        project: Project,
    ) -> None:
        application = project.compose(_WithCallbacks(AcmeUnservedCallback, lenient=True))

        assert not application.kernel.container.has_repo_mapping(AcmeUnserved)

    def test_a_use_case_injecting_an_unmapped_repository_keeps_the_unserved_model_error(
        self,
        project: Project,
    ) -> None:
        adapter = Serving(AcmeRecord, use_cases=(AcmeInjectsUnserved,))

        with pytest.raises(UnservedModelError, match="AcmeInjectsUnserved injects unserved"):
            project.compose(adapter)

    def test_a_lenient_recipe_leaves_a_missing_binding_to_the_first_build(
        self,
        project: Project,
    ) -> None:
        adapter = Serving(AcmeRecord, use_cases=(AcmeNeedsService,), lenient=True)

        application = project.compose(adapter)

        assert not application.kernel.container.is_registered(AcmeUnregistered)

    def test_a_lenient_recipe_still_refuses_the_other_structural_refusals(
        self,
        project: Project,
    ) -> None:
        adapter = Serving(
            AcmeRecord, use_cases=(AcmeNeedsCycle,), lenient=True, overrides=(binds_cycle,)
        )

        with pytest.raises(BootstrapError, match="dependency cycle") as caught:
            project.compose(adapter)

        assert isinstance(caught.value.__cause__, DependencyGraphError)

    def test_an_unresolvable_parameter_of_a_reached_binding_is_refused(
        self,
        project: Project,
    ) -> None:
        modules = (_binds(AcmeNamedTrail, AcmeNamedTrail),)
        adapter = _WithCallbacks(AcmeAuditNames, overrides=modules)

        with pytest.raises(BootstrapError, match="AcmeNamedTrail: names: tuple") as caught:
            project.compose(adapter)

        assert isinstance(caught.value.__cause__, DependencyGraphError)

    def test_a_lenient_recipe_only_warns_of_an_unresolvable_parameter_of_a_binding(
        self,
        project: Project,
    ) -> None:
        modules = (_binds(AcmeNamedTrail, AcmeNamedTrail),)
        adapter = _WithCallbacks(AcmeAuditNames, lenient=True, overrides=modules)

        with pytest.warns(DependencyUnresolvable, match="AcmeNamedTrail -> names"):
            project.compose(adapter)

    def test_a_use_case_reaches_its_main_repository_and_capability(
        self, project: Project, analyses: list[GraphAnalysis]
    ) -> None:
        application = project.compose(Serving(AcmeRecord, use_cases=(AcmeListsRecords,)))

        (analysis,) = analyses
        kernel = application.kernel
        container = kernel.container
        repository = KeyNode(container._repo_bindings[AcmeRecord])  # noqa: SLF001 - the mapped key
        assert set(analysis.graph.edges(ClassNode(AcmeListsRecords))) == {
            Edge(repository, "main_repo"),
            Edge(KeyNode(Listable[AcmeRecord]), "main_repo"),
        }
        assert {repository, KeyNode(Listable[AcmeRecord])} <= analysis.reachable
        assert analysis.graph is kernel.factory.graph
