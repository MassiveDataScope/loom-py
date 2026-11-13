"""Run deploy checks after the adapter has discovered its recipe."""

from __future__ import annotations

from loom.composition.recipe import Adapter, Recipe
from loom.composition.source import CompositionSource
from loom.core.checks import enforce_deploy_checks


def plan(source: CompositionSource, adapter: Adapter[object]) -> Recipe:
    recipe = adapter.recipe(source)
    # After the code path and discovery: checks.modules may name the
    # application's modules, and no resource exists yet.
    enforce_deploy_checks(source.config, source.environment, skip=recipe.skip_checks)
    return recipe
