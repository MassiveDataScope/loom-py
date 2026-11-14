"""Take the adapter's recipe and run the deploy checks on it."""

from __future__ import annotations

from dataclasses import dataclass

from loom.composition.recipe import Adapter, Recipe
from loom.composition.source import CompositionSource
from loom.core.checks import enforce_deploy_checks


@dataclass(frozen=True, slots=True, kw_only=True)
class Planned:
    """What to compose: the loaded source and the recipe its adapter declared."""

    source: CompositionSource
    recipe: Recipe


def plan(source: CompositionSource, adapter: Adapter[object]) -> Planned:
    """Ask *adapter* for its recipe, then refuse a failing deploy check.

    A lenient recipe runs no deploy check.

    Raises:
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
    """
    recipe = adapter.recipe(source)
    if not recipe.lenient:
        enforce_deploy_checks(source.config, source.environment, skip=recipe.skip_checks)
    return Planned(source=source, recipe=recipe)
