"""Analyse the registered dependency graph from the recipe's use cases, jobs and callbacks."""

from __future__ import annotations

import warnings

from loom.composition.recipe import Recipe
from loom.core.bootstrap import BootstrapError
from loom.core.di.container import LoomContainer
from loom.core.di.graph import (
    LENIENT_REFUSALS,
    REFUSALS,
    ClassNode,
    DependencyGraph,
    DependencyGraphError,
    GraphAnalysis,
    analyse,
    refuse_unmapped_repository,
)


def analysed(container: LoomContainer, recipe: Recipe) -> GraphAnalysis:
    """The analysis of *container*'s graph, rooted at the classes loom builds for *recipe*.

    Each :class:`~loom.core.di.errors.DependencyUnresolvable` is emitted as a
    warning.

    Raises:
        BootstrapError: Wrapping the :class:`~loom.core.di.graph.DependencyGraphError`
            of the first structural refusal, cause chained.
    """
    discovered = recipe.discovered
    roots = tuple(map(ClassNode, (*discovered.use_cases, *discovered.jobs, *recipe.callbacks)))
    callbacks = frozenset(map(ClassNode, recipe.callbacks))
    # A lenient recipe leaves a missing binding to the first build, as it skips factory.verify.
    # factory.verify reports a use case's or a job's unmapped repository; nothing does a callback's.
    rules = (
        LENIENT_REFUSALS if recipe.lenient else (*REFUSALS, refuse_unmapped_repository(callbacks))
    )
    try:
        analysis = analyse(DependencyGraph(container), roots, rules)
    except DependencyGraphError as exc:
        raise BootstrapError(f"Bootstrap failed: {exc}") from exc
    for warning in analysis.warnings:
        warnings.warn(warning, stacklevel=2)
    return analysis
