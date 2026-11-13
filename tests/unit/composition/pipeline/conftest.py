"""Fixtures of the characterisation: the executors a path builds, and an in-memory composition."""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pytest

from loom.composition import Application, compose_application
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from tests.helpers.composition import Serving, composition_source, executors, fixture_project
from tests.unit.composition.pipeline.components import AcmeJob, AcmePing, AcmeRecord
from tests.unit.composition.pipeline.paths import SQLITE

__all__ = ["executors", "fixture_project", "overridden"]

Module = Callable[[LoomContainer], None]


@pytest.fixture
def overridden() -> Callable[..., Application]:
    """Compose ``AcmeRecord``, ``AcmePing`` and ``AcmeJob`` over SQLite with these overrides."""
    source = composition_source(
        ConfigContext.from_dict(
            {
                "app": {"name": "acme", "job_service": "inline"},
                "stores": {"default": SQLITE},
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        )
    )

    def compose(*overrides: Module, modules: Sequence[Module] = ()) -> Application:
        adapter = Serving(AcmeRecord, use_cases=(AcmePing,), jobs=(AcmeJob,), overrides=overrides)
        return compose_application(source, adapter, modules=modules)

    return compose
