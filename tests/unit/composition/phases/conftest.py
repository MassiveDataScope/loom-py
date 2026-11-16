"""Fixtures of the characterisation: the executors a path builds, and an in-memory composition."""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pytest

from loom.composition import Application, Project
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from tests.helpers.composition import Serving, composition_source, executors, fixture_project
from tests.unit.composition.phases.components import AcmeJob, AcmePing, AcmeRecord
from tests.unit.composition.phases.paths import SQLITE

__all__ = ["executors", "fixture_project", "overridden"]

Module = Callable[[LoomContainer], None]


@pytest.fixture
def source() -> Project:
    """An in-memory source: the ``acme`` application over SQLite, jobs inline."""
    return composition_source(
        ConfigContext.from_dict(
            {
                "app": {"name": "acme", "job_service": "inline"},
                "stores": {"default": SQLITE, "settings": {"cursors": {"signing": "ephemeral"}}},
            }
        )
    )


@pytest.fixture
def overridden(source: Project) -> Callable[..., Application]:
    """Compose ``AcmeRecord``, ``AcmePing`` and ``AcmeJob`` over SQLite with these overrides."""

    def compose(*overrides: Module, modules: Sequence[Module] = ()) -> Application:
        adapter = Serving(AcmeRecord, use_cases=(AcmePing,), jobs=(AcmeJob,), overrides=overrides)
        return source.compose(adapter, modules=modules)

    return compose
